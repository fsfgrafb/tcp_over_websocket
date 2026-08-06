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
    init_tracing("tows");
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        println!("Usage: tows [port]\nDefault port: {DEFAULT_TOWS_PORT}");
        return Ok(());
    }
    if args.len() > 1 {
        bail!("too many arguments; usage: tows [port]");
    }
    let port = args
        .first()
        .map(|value| value.parse::<u16>().context("invalid listen port"))
        .transpose()?
        .unwrap_or(DEFAULT_TOWS_PORT);
    if port == 0 {
        bail!("listen port must be in the range 1..=65535");
    }
    let listener = TcpListener::bind(("0.0.0.0", port))
        .await
        .with_context(|| format!("failed to listen on 0.0.0.0:{port}"))?;
    tracing::info!(target: "tows", "tows v{APP_VERSION} listening on {}", listener.local_addr()?);
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
                    tracing::info!(target: "tows", "stopping");
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("failed to accept connection")?;
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, peer).await {
                        tracing::warn!(target: "tunnel", "connection {peer} ended: {error:#}");
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
    tracing::info!(target: "tunnel", "peer={peer} path={path} protocol=v{version}");

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
                    Some(Err(error)) => break Err(anyhow!(error).context("failed to read WebSocket")),
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
        Message::Close(_) => bail!("client closed the WebSocket"),
        Message::Text(_) => bail!("the protocol only accepts WebSocket Binary messages"),
        Message::Frame(_) => Ok(()),
    }
}

async fn handle_frame(
    frame: Frame,
    writer: &WsWriter,
    events: &mpsc::Sender<TunnelEvent>,
    tunnels: &mut HashMap<u16, Tunnel>,
) -> Result<()> {
    if matches!(frame.kind, FrameType::Data | FrameType::Eof)
        && !tunnels.contains_key(&frame.tunnel_id)
    {
        let mut active_ids = tunnels.keys().copied().collect::<Vec<_>>();
        active_ids.sort_unstable();
        bail!(
            "{:?} frame refers to unknown tunnel_id {}; active_ids={active_ids:?}",
            frame.kind,
            frame.tunnel_id
        );
    }
    match frame.kind {
        FrameType::Open => {
            if tunnels.contains_key(&frame.tunnel_id) {
                bail!("duplicate tunnel_id {}", frame.tunnel_id);
            }
            if tunnels.len() >= MAX_TUNNELS {
                let reason = "this WebSocket has reached the 64-tunnel limit"
                    .as_bytes()
                    .to_vec();
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
                            safe_error(&format!("invalid target address: {error}")),
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
                    Err(_) => Err("target connection timed out after 10 seconds".to_string()),
                };
                let _ = events.send(TunnelEvent::Connected(id, result)).await;
            });
            Ok(())
        }
        FrameType::Data => {
            let tunnel = open_tunnel_mut(tunnels, frame.tunnel_id)?;
            if tunnel.remote_eof_seen {
                bail!("tunnel {} received DATA after EOF", frame.tunnel_id);
            }
            tunnel
                .tcp_sender
                .send(TcpCommand::Data(frame.payload))
                .await
                .map_err(|_| anyhow!("TCP writer for tunnel {} has stopped", frame.tunnel_id))
        }
        FrameType::Eof => {
            tracing::info!(
                target: "tunnel",
                "diagnostic: received EOF for active tunnel {}",
                frame.tunnel_id
            );
            let tunnel = open_tunnel_mut(tunnels, frame.tunnel_id)?;
            if tunnel.remote_eof_seen {
                bail!("tunnel {} received duplicate EOF", frame.tunnel_id);
            }
            tunnel.remote_eof_seen = true;
            tunnel
                .tcp_sender
                .send(TcpCommand::Eof)
                .await
                .map_err(|_| anyhow!("TCP writer for tunnel {} has stopped", frame.tunnel_id))?;
            maybe_finish(frame.tunnel_id, writer, tunnels).await
        }
        FrameType::Close => {
            tracing::info!(
                target: "tunnel",
                "diagnostic: received CLOSE for tunnel {}; active={}",
                frame.tunnel_id,
                tunnels.contains_key(&frame.tunnel_id)
            );
            if tunnels.contains_key(&frame.tunnel_id) {
                remove_tunnel(frame.tunnel_id, writer, tunnels).await;
            }
            Ok(())
        }
        FrameType::Ping => Ok(()),
        _ => bail!("client sent a disallowed {:?} frame", frame.kind),
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
                        .context("failed to enable TCP_NODELAY on target TCP stream")?;
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
            tracing::warn!(target: "tunnel", "tunnel {id} TCP error: {reason}");
            if tunnels.contains_key(&id) {
                tracing::info!(
                    target: "tunnel",
                    "diagnostic: sending CLOSE for tunnel {id} after target TCP error"
                );
                writer
                    .send_and_remove(Frame::new(FrameType::Close, id, Vec::new())?)
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
                let eof =
                    Frame::new(FrameType::Eof, id, Vec::new()).expect("static EOF frame is valid");
                if flow.send_flushed(eof).await.is_ok() {
                    tracing::info!(
                        target: "tunnel",
                        "diagnostic: sent EOF for tunnel {id} after target TCP EOF"
                    );
                    let _ = events.send(TunnelEvent::LocalEof(id)).await;
                }
                return;
            }
            Ok(size) => {
                let frame = Frame::new(FrameType::Data, id, buffer[..size].to_vec())
                    .expect("TCP chunk does not exceed the protocol limit");
                if flow.send(frame).await.is_err() {
                    return;
                }
            }
            Err(error) if is_normal_close(&error) => {
                let eof =
                    Frame::new(FrameType::Eof, id, Vec::new()).expect("static EOF frame is valid");
                if flow.send_flushed(eof).await.is_ok() {
                    tracing::info!(
                        target: "tunnel",
                        "diagnostic: sent EOF for tunnel {id} after normal target TCP close"
                    );
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
        Some(Tunnel::Opening) => bail!("tunnel {id} received data before OPEN_OK"),
        None => bail!("frame refers to unknown tunnel_id {id}"),
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
        tracing::info!(
            target: "tunnel",
            "diagnostic: sending CLOSE for tunnel {id} after bidirectional EOF"
        );
        writer
            .send_and_remove(Frame::new(FrameType::Close, id, Vec::new())?)
            .await?;
        remove_tunnel(id, writer, tunnels).await;
    }
    Ok(())
}

async fn remove_tunnel(id: u16, writer: &WsWriter, tunnels: &mut HashMap<u16, Tunnel>) {
    if let Some(tunnel) = tunnels.remove(&id) {
        let state = match &tunnel {
            Tunnel::Opening => "opening",
            Tunnel::Open(_) => "open",
        };
        tracing::info!(
            target: "tunnel",
            "diagnostic: removed tunnel {id}; previous_state={state}"
        );
        if let Tunnel::Open(tunnel) = tunnel {
            tunnel.reader_task.abort();
            tunnel.writer_task.abort();
        }
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
        io::ErrorKind::ConnectionRefused => "target service refused the connection".to_string(),
        io::ErrorKind::TimedOut => "target connection timed out".to_string(),
        io::ErrorKind::NotFound | io::ErrorKind::AddrNotAvailable => {
            "target address is unavailable".to_string()
        }
        io::ErrorKind::PermissionDenied => {
            "tows is not allowed to connect to the target".to_string()
        }
        _ => "target is unreachable from the tows host".to_string(),
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
        .context("failed to inspect HTTP request")?;
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
