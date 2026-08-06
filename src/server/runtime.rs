use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::address::{DEFAULT_TOWS_PORT, parse_target};
use crate::multiplex::{FlowWriter, WsWriter, spawn_writer};
use crate::network::{accept_websocket, server_handshake};
use crate::protocol::{Frame, FrameType, MAX_DATA_LEN, MAX_TUNNELS};
use crate::{APP_VERSION, init_tracing};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_QUEUE_FRAMES: usize = 16;
const HTTP_PROBE_RESPONSE: &[u8] =
    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

pub async fn run_cli() -> Result<()> {
    init_tracing();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        println!("用法: tows [port]\n默认端口: {DEFAULT_TOWS_PORT}");
        return Ok(());
    }
    if args.len() > 1 {
        bail!("参数过多；用法: tows [port]");
    }
    let port = args
        .first()
        .map(|value| value.parse::<u16>().context("无效监听端口"))
        .transpose()?
        .unwrap_or(DEFAULT_TOWS_PORT);
    if port == 0 {
        bail!("监听端口必须在 1..=65535 范围内");
    }
    let listener = TcpListener::bind(("0.0.0.0", port))
        .await
        .with_context(|| format!("无法监听 0.0.0.0:{port}"))?;
    tracing::info!("tows v{APP_VERSION} 正在监听 {}", listener.local_addr()?);
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = stop_tx.send(true);
    });
    serve(listener, stop_rx).await
}

/// 在给定监听器上运行服务端。公开此入口以便不接触 WebVPN 的本地集成测试。
pub async fn serve(listener: TcpListener, mut stop: watch::Receiver<bool>) -> Result<()> {
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    tracing::info!("tows 正在停止");
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("接受连接失败")?;
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, peer).await {
                        tracing::warn!("连接 {peer} 已结束: {error:#}");
                    }
                });
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, peer: SocketAddr) -> Result<()> {
    if !is_websocket_upgrade(&stream).await? {
        stream.write_all(HTTP_PROBE_RESPONSE).await?;
        stream.shutdown().await?;
        return Ok(());
    }

    let (mut websocket, path) = accept_websocket(stream).await?;
    let version = server_handshake(&mut websocket, &format!("tows {APP_VERSION}")).await?;
    tracing::info!("{peer} 已连接，路径={path}，协议=v{version}");

    let (sink, mut source) = websocket.split();
    let (writer, writer_task) = spawn_writer(sink);
    let (event_tx, mut event_rx) = mpsc::channel::<TunnelEvent>(256);
    let mut tunnels = HashMap::<u16, Tunnel>::new();

    let result = loop {
        tokio::select! {
            message = source.next() => {
                match message {
                    Some(Ok(Message::Close(_))) => break Ok(()),
                    Some(Ok(message)) => {
                        if let Err(error) = handle_ws_message(
                            message,
                            &writer,
                            &event_tx,
                            &mut tunnels,
                        ).await {
                            writer.protocol_close(error.to_string()).await;
                            break Err(error);
                        }
                    }
                    Some(Err(error)) => break Err(anyhow!(error).context("读取 WebSocket 失败")),
                    None => break Ok(()),
                }
            }
            event = event_rx.recv() => {
                let Some(event) = event else { break Ok(()) };
                if let Err(error) = handle_tunnel_event(event, &writer, &event_tx, &mut tunnels).await {
                    writer.protocol_close(error.to_string()).await;
                    break Err(error);
                }
            }
        }
    };

    close_all(&writer, &mut tunnels).await;
    writer.normal_close().await;
    let _ = writer_task.await;
    result
}

enum Tunnel {
    Opening,
    Open(OpenTunnel),
}

struct OpenTunnel {
    tcp_sender: mpsc::Sender<TcpCommand>,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    local_eof_sent: bool,
    remote_eof_seen: bool,
    tcp_writer_done: bool,
}

enum TcpCommand {
    Data(Vec<u8>),
    Eof,
}

enum TunnelEvent {
    Connected(u16, std::result::Result<TcpStream, String>),
    LocalEof(u16),
    TcpWriterDone(u16),
    TcpError(u16, String),
}

async fn handle_ws_message(
    message: Message,
    writer: &WsWriter,
    events: &mpsc::Sender<TunnelEvent>,
    tunnels: &mut HashMap<u16, Tunnel>,
) -> Result<()> {
    match message {
        Message::Binary(bytes) => {
            let frame = Frame::decode(&bytes)?;
            frame.validate_client_to_server(true)?;
            handle_frame(frame, writer, events, tunnels).await
        }
        Message::Ping(payload) => {
            writer.raw(Message::Pong(payload)).await;
            Ok(())
        }
        Message::Pong(_) => Ok(()),
        Message::Close(_) => bail!("客户端关闭了 WebSocket"),
        Message::Text(_) => bail!("协议只接受 WebSocket Binary 消息"),
        Message::Frame(_) => Ok(()),
    }
}

async fn handle_frame(
    frame: Frame,
    writer: &WsWriter,
    events: &mpsc::Sender<TunnelEvent>,
    tunnels: &mut HashMap<u16, Tunnel>,
) -> Result<()> {
    match frame.kind {
        FrameType::Open => {
            if tunnels.contains_key(&frame.tunnel_id) {
                bail!("重复使用 tunnel_id {}", frame.tunnel_id);
            }
            if tunnels.len() >= MAX_TUNNELS {
                let reason = "当前 WebSocket 已达到 64 条隧道上限".as_bytes().to_vec();
                writer
                    .send(Frame::new(FrameType::OpenFail, frame.tunnel_id, reason)?)
                    .await?;
                return Ok(());
            }
            let target_text = std::str::from_utf8(&frame.payload)?.to_string();
            let target = match parse_target(&target_text) {
                Ok(target) => target,
                Err(error) => {
                    writer
                        .send(Frame::new(
                            FrameType::OpenFail,
                            frame.tunnel_id,
                            safe_error(&format!("目标地址无效: {error}")),
                        )?)
                        .await?;
                    return Ok(());
                }
            };
            tunnels.insert(frame.tunnel_id, Tunnel::Opening);
            let id = frame.tunnel_id;
            let events = events.clone();
            tokio::spawn(async move {
                let result = match tokio::time::timeout(
                    CONNECT_TIMEOUT,
                    TcpStream::connect(target.to_string()),
                )
                .await
                {
                    Ok(Ok(stream)) => Ok(stream),
                    Ok(Err(error)) => Err(public_connect_error(&error)),
                    Err(_) => Err("连接目标超时（10 秒）".to_string()),
                };
                let _ = events.send(TunnelEvent::Connected(id, result)).await;
            });
            Ok(())
        }
        FrameType::Data => {
            let tunnel = open_tunnel_mut(tunnels, frame.tunnel_id)?;
            if tunnel.remote_eof_seen {
                bail!("隧道 {} 在 EOF 后又收到 DATA", frame.tunnel_id);
            }
            tunnel
                .tcp_sender
                .send(TcpCommand::Data(frame.payload))
                .await
                .map_err(|_| anyhow!("隧道 {} 的 TCP 写任务已停止", frame.tunnel_id))
        }
        FrameType::Eof => {
            let tunnel = open_tunnel_mut(tunnels, frame.tunnel_id)?;
            if tunnel.remote_eof_seen {
                bail!("隧道 {} 收到重复 EOF", frame.tunnel_id);
            }
            tunnel.remote_eof_seen = true;
            tunnel
                .tcp_sender
                .send(TcpCommand::Eof)
                .await
                .map_err(|_| anyhow!("隧道 {} 的 TCP 写任务已停止", frame.tunnel_id))?;
            maybe_finish(frame.tunnel_id, writer, tunnels).await
        }
        FrameType::Close => {
            if tunnels.contains_key(&frame.tunnel_id) {
                remove_tunnel(frame.tunnel_id, writer, tunnels).await;
            }
            Ok(())
        }
        FrameType::Ping => Ok(()),
        _ => bail!("客户端发送了不允许的 {:?} 帧", frame.kind),
    }
}

async fn handle_tunnel_event(
    event: TunnelEvent,
    writer: &WsWriter,
    events: &mpsc::Sender<TunnelEvent>,
    tunnels: &mut HashMap<u16, Tunnel>,
) -> Result<()> {
    match event {
        TunnelEvent::Connected(id, result) => {
            if !matches!(tunnels.get(&id), Some(Tunnel::Opening)) {
                return Ok(());
            }
            match result {
                Ok(stream) => {
                    stream
                        .set_nodelay(true)
                        .context("无法为目标 TCP 启用 TCP_NODELAY")?;
                    let flow = writer.register(id).await?;
                    writer
                        .send(Frame::new(FrameType::OpenOk, id, Vec::new())?)
                        .await?;
                    let tunnel = spawn_tcp_tasks(id, stream, flow, events.clone());
                    tunnels.insert(id, Tunnel::Open(tunnel));
                }
                Err(reason) => {
                    tunnels.remove(&id);
                    writer
                        .send(Frame::new(FrameType::OpenFail, id, safe_error(&reason))?)
                        .await?;
                }
            }
        }
        TunnelEvent::LocalEof(id) => {
            if let Some(Tunnel::Open(tunnel)) = tunnels.get_mut(&id) {
                tunnel.local_eof_sent = true;
                maybe_finish(id, writer, tunnels).await?;
            }
        }
        TunnelEvent::TcpWriterDone(id) => {
            if let Some(Tunnel::Open(tunnel)) = tunnels.get_mut(&id) {
                tunnel.tcp_writer_done = true;
                maybe_finish(id, writer, tunnels).await?;
            }
        }
        TunnelEvent::TcpError(id, reason) => {
            tracing::warn!("隧道 {id} TCP 错误: {reason}");
            if tunnels.contains_key(&id) {
                writer
                    .send(Frame::new(FrameType::Close, id, Vec::new())?)
                    .await?;
                remove_tunnel(id, writer, tunnels).await;
            }
        }
    }
    Ok(())
}

fn spawn_tcp_tasks(
    id: u16,
    stream: TcpStream,
    flow: FlowWriter,
    events: mpsc::Sender<TunnelEvent>,
) -> OpenTunnel {
    let (reader, writer) = stream.into_split();
    let (tcp_sender, tcp_receiver) = mpsc::channel(TCP_QUEUE_FRAMES);
    let read_events = events.clone();
    let reader_task = tokio::spawn(read_tcp(id, reader, flow, read_events));
    let writer_task = tokio::spawn(write_tcp(id, writer, tcp_receiver, events));
    OpenTunnel {
        tcp_sender,
        reader_task,
        writer_task,
        local_eof_sent: false,
        remote_eof_seen: false,
        tcp_writer_done: false,
    }
}

async fn read_tcp(
    id: u16,
    mut reader: OwnedReadHalf,
    flow: FlowWriter,
    events: mpsc::Sender<TunnelEvent>,
) {
    let mut buffer = vec![0_u8; MAX_DATA_LEN];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => {
                let eof = Frame::new(FrameType::Eof, id, Vec::new()).expect("固定 EOF 帧合法");
                if flow.send_flushed(eof).await.is_ok() {
                    let _ = events.send(TunnelEvent::LocalEof(id)).await;
                }
                return;
            }
            Ok(size) => {
                let frame = Frame::new(FrameType::Data, id, buffer[..size].to_vec())
                    .expect("TCP 分片不会超过协议上限");
                if flow.send(frame).await.is_err() {
                    return;
                }
            }
            Err(error) if is_normal_close(&error) => {
                let eof = Frame::new(FrameType::Eof, id, Vec::new()).expect("固定 EOF 帧合法");
                if flow.send_flushed(eof).await.is_ok() {
                    let _ = events.send(TunnelEvent::LocalEof(id)).await;
                }
                return;
            }
            Err(error) => {
                let _ = events
                    .send(TunnelEvent::TcpError(id, error.to_string()))
                    .await;
                return;
            }
        }
    }
}

async fn write_tcp(
    id: u16,
    mut writer: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<TcpCommand>,
    events: mpsc::Sender<TunnelEvent>,
) {
    while let Some(command) = receiver.recv().await {
        let result = match command {
            TcpCommand::Data(data) => writer.write_all(&data).await,
            TcpCommand::Eof => {
                let result = writer.shutdown().await;
                if result.is_ok() {
                    let _ = events.send(TunnelEvent::TcpWriterDone(id)).await;
                }
                return;
            }
        };
        if let Err(error) = result {
            let _ = events
                .send(TunnelEvent::TcpError(id, error.to_string()))
                .await;
            return;
        }
    }
}

fn open_tunnel_mut(tunnels: &mut HashMap<u16, Tunnel>, id: u16) -> Result<&mut OpenTunnel> {
    match tunnels.get_mut(&id) {
        Some(Tunnel::Open(tunnel)) => Ok(tunnel),
        Some(Tunnel::Opening) => bail!("隧道 {id} 尚未 OPEN_OK 就收到数据"),
        None => bail!("帧指向未知 tunnel_id {id}"),
    }
}

async fn maybe_finish(
    id: u16,
    writer: &WsWriter,
    tunnels: &mut HashMap<u16, Tunnel>,
) -> Result<()> {
    let finished = matches!(
        tunnels.get(&id),
        Some(Tunnel::Open(tunnel))
            if tunnel.local_eof_sent && tunnel.remote_eof_seen && tunnel.tcp_writer_done
    );
    if finished {
        writer
            .send(Frame::new(FrameType::Close, id, Vec::new())?)
            .await?;
        remove_tunnel(id, writer, tunnels).await;
    }
    Ok(())
}

async fn remove_tunnel(id: u16, writer: &WsWriter, tunnels: &mut HashMap<u16, Tunnel>) {
    if let Some(Tunnel::Open(tunnel)) = tunnels.remove(&id) {
        tunnel.reader_task.abort();
        tunnel.writer_task.abort();
    }
    writer.remove(id).await;
}

async fn close_all(writer: &WsWriter, tunnels: &mut HashMap<u16, Tunnel>) {
    let ids: Vec<u16> = tunnels.keys().copied().collect();
    for id in ids {
        remove_tunnel(id, writer, tunnels).await;
    }
}

fn safe_error(reason: &str) -> Vec<u8> {
    let mut text = reason.replace(['\r', '\n'], " ");
    while text.len() > 256 {
        text.pop();
    }
    text.into_bytes()
}

fn public_connect_error(error: &io::Error) -> String {
    match error.kind() {
        io::ErrorKind::ConnectionRefused => "目标服务拒绝连接".to_string(),
        io::ErrorKind::TimedOut => "目标连接超时".to_string(),
        io::ErrorKind::NotFound | io::ErrorKind::AddrNotAvailable => "目标地址不可用".to_string(),
        io::ErrorKind::PermissionDenied => "tows 无权连接目标".to_string(),
        _ => "目标无法从 tows 所在机器访问".to_string(),
    }
}

fn is_normal_close(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

async fn is_websocket_upgrade(stream: &TcpStream) -> Result<bool> {
    let mut buffer = [0_u8; 2048];
    let size = stream
        .peek(&mut buffer)
        .await
        .context("检查 HTTP 请求失败")?;
    let request = String::from_utf8_lossy(&buffer[..size]);
    let mut connection = false;
    let mut upgrade = false;
    for line in request.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("connection")
            && value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("upgrade"))
        {
            connection = true;
        }
        if name.trim().eq_ignore_ascii_case("upgrade")
            && value.trim().eq_ignore_ascii_case("websocket")
        {
            upgrade = true;
        }
    }
    Ok(connection && upgrade)
}
