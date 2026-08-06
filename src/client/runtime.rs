use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::address::Endpoint;
use crate::multiplex::{FlowWriter, WsWriter, spawn_writer};
use crate::network::{
    HEARTBEAT_INTERVAL, build_webvpn_ws_url, client_handshake, connect_websocket,
};
use crate::protocol::{Frame, FrameType, MAX_DATA_LEN, MAX_TUNNELS};
use crate::{APP_VERSION, init_tracing};

use super::auth::{AuthPrompt, SessionCookie, login_or_restore, refresh_ticket};
use super::config::{ParsedArgs, parse_args, prompt_interactive};

const OPEN_TIMEOUT: Duration = Duration::from_secs(15);
const COOKIE_REFRESH_INTERVAL: Duration = Duration::from_secs(600);
const TCP_QUEUE_FRAMES: usize = 16;

#[derive(Debug, Clone)]
pub struct ForwardRule {
    pub name: String,
    pub target: Endpoint,
    pub listen: Endpoint,
}

pub trait ClientObserver: Send + Sync {
    fn status(&self, message: &str);
    fn tunnel_status(&self, name: &str, message: &str);
}

struct TerminalUi;

impl AuthPrompt for TerminalUi {
    fn status(&self, message: &str) {
        tracing::info!("{message}");
    }

    fn show_qr(&self, image: Vec<u8>) -> Result<()> {
        super::qr::print(&image)
    }

    fn request_code(&self, label: &str) -> Result<String> {
        use std::io::Write;
        print!("请输入{label}验证码: ");
        std::io::stdout().flush()?;
        let mut code = String::new();
        std::io::stdin().read_line(&mut code)?;
        Ok(code.trim().to_string())
    }
}

impl ClientObserver for TerminalUi {
    fn status(&self, message: &str) {
        tracing::info!("{message}");
    }

    fn tunnel_status(&self, name: &str, message: &str) {
        tracing::info!("[{name}] {message}");
    }
}

pub async fn run_cli() -> Result<()> {
    init_tracing();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = match parse_args(&args)? {
        ParsedArgs::Help => {
            print_help();
            return Ok(());
        }
        ParsedArgs::Interactive => prompt_interactive()?,
        ParsedArgs::Run(config) => config,
    };

    if !config.listen.is_loopback() {
        tracing::warn!(
            "监听 {} 不是回环地址，将向局域网暴露本地端口",
            config.listen
        );
    }
    let ui = Arc::new(TerminalUi);
    let auth: Arc<dyn AuthPrompt> = ui.clone();
    let observer: Arc<dyn ClientObserver> = ui;
    let cookie = login_or_restore(auth, config.login).await?;
    let rule = ForwardRule {
        name: "towc".to_string(),
        target: config.target,
        listen: config.listen,
    };
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = stop_tx.send(true);
    });
    run_tunnels(config.server, vec![rule], cookie, stop_rx, observer).await
}

pub async fn run_tunnels(
    server: Endpoint,
    rules: Vec<ForwardRule>,
    cookie: SessionCookie,
    stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) -> Result<()> {
    if rules.is_empty() {
        bail!("没有启用的隧道");
    }
    if rules.len() > MAX_TUNNELS {
        bail!("单条 WebSocket 最多配置 {MAX_TUNNELS} 条隧道");
    }
    let url = build_webvpn_ws_url(&server)?;
    run_tunnels_to_url(url, server, rules, cookie, stop, observer).await
}

async fn run_tunnels_to_url(
    url: String,
    server: Endpoint,
    rules: Vec<ForwardRule>,
    cookie: SessionCookie,
    mut stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) -> Result<()> {
    // 登录完成后先绑定全部端口；任何冲突都在建立 WS 前清晰报出。
    let mut listeners = Vec::new();
    for rule in rules {
        let address = rule.listen.resolve().await?;
        let listener = TcpListener::bind(address)
            .await
            .with_context(|| format!("无法监听 {}（端口可能已占用）", rule.listen))?;
        listeners.push((rule, listener));
    }

    observer.status("正在连接 WebVPN WebSocket");
    let mut websocket = connect_websocket(&url, &cookie.snapshot())
        .await
        .map_err(|error| anyhow!(error))?;
    client_handshake(&mut websocket, &format!("towc {APP_VERSION}")).await?;
    observer.status(&format!("已连接 tows {server}"));

    let (sink, mut source) = websocket.split();
    let (writer, mut writer_task) = spawn_writer(sink);
    let (open_tx, mut open_rx) = mpsc::channel::<LocalOpen>(MAX_TUNNELS * 2);
    let (event_tx, mut event_rx) = mpsc::channel::<TunnelEvent>(256);
    let mut accept_tasks = Vec::new();
    for (rule, listener) in listeners {
        let sender = open_tx.clone();
        let observer = Arc::clone(&observer);
        let task_stop = stop.clone();
        observer.tunnel_status(
            &rule.name,
            &format!("ready: {} -> {} -> {}", rule.listen, server, rule.target),
        );
        accept_tasks.push(tokio::spawn(accept_loop(
            rule, listener, sender, task_stop, observer,
        )));
    }
    drop(open_tx);

    let keepalive_events = event_tx.clone();
    let keepalive_cookie = cookie.clone();
    let cookie_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + COOKIE_REFRESH_INTERVAL,
            COOKIE_REFRESH_INTERVAL,
        );
        loop {
            interval.tick().await;
            if let Err(error) = refresh_ticket(&keepalive_cookie).await {
                let _ = keepalive_events
                    .send(TunnelEvent::SessionError(format!(
                        "WebVPN Cookie 保活失败: {error:#}"
                    )))
                    .await;
                return;
            }
        }
    });

    let mut tunnels = HashMap::<u16, Tunnel>::new();
    let mut next_id = 1_u16;
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
        HEARTBEAT_INTERVAL,
    );

    let result = loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break Ok(());
                }
            }
            _ = heartbeat.tick() => {
                writer.send(Frame::new(FrameType::Ping, 0, Vec::new())?).await?;
            }
            local = open_rx.recv() => {
                let Some(local) = local else { break Ok(()) };
                if tunnels.len() >= MAX_TUNNELS {
                    observer.tunnel_status(&local.name, "连接被拒绝：已达到 64 条并发流上限");
                    continue;
                }
                let id = allocate_id(&tunnels, &mut next_id)?;
                writer.send(Frame::new(FrameType::Open, id, local.target.to_string().into_bytes())?).await?;
                observer.tunnel_status(&local.name, &format!("正在建立流 {id}"));
                tunnels.insert(id, Tunnel::Opening(OpeningTunnel {
                    stream: Some(local.stream),
                    name: local.name,
                }));
                let timeout_events = event_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(OPEN_TIMEOUT).await;
                    let _ = timeout_events.send(TunnelEvent::OpenTimeout(id)).await;
                });
            }
            message = source.next() => {
                match message {
                    Some(Ok(message)) => {
                        if let Err(error) = handle_ws_message(message, &writer, &event_tx, &observer, &mut tunnels).await {
                            writer.protocol_close(error.to_string()).await;
                            break Err(error);
                        }
                    }
                    Some(Err(error)) => break Err(anyhow!(error).context("WebSocket 读取失败；请重新启动并登录")),
                    None => break Err(anyhow!("WebSocket 已断开；请重新启动并登录")),
                }
            }
            event = event_rx.recv() => {
                let Some(event) = event else { break Ok(()) };
                if let Err(error) = handle_tunnel_event(event, &writer, &event_tx, &observer, &mut tunnels).await {
                    break Err(error);
                }
            }
            writer_result = &mut writer_task => {
                break match writer_result {
                    Ok(Ok(())) => Err(anyhow!("WebSocket 写任务已停止")),
                    Ok(Err(error)) => Err(error.context("WebSocket 写任务失败")),
                    Err(error) => Err(anyhow!(error).context("WebSocket 写任务异常结束")),
                };
            }
        }
    };

    for task in &accept_tasks {
        task.abort();
    }
    for task in accept_tasks {
        let _ = task.await;
    }
    cookie_task.abort();
    close_all(&writer, &mut tunnels).await;
    writer.normal_close().await;
    if !writer_task.is_finished() {
        let _ = writer_task.await;
    }
    result
}

struct LocalOpen {
    stream: TcpStream,
    target: Endpoint,
    name: String,
}

enum Tunnel {
    Opening(OpeningTunnel),
    Open(OpenTunnel),
}

struct OpeningTunnel {
    stream: Option<TcpStream>,
    name: String,
}

struct OpenTunnel {
    name: String,
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
    OpenTimeout(u16),
    LocalEof(u16),
    TcpWriterDone(u16),
    TcpError(u16, String),
    SessionError(String),
}

async fn accept_loop(
    rule: ForwardRule,
    listener: TcpListener,
    sender: mpsc::Sender<LocalOpen>,
    mut stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) {
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() { return; }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        observer.tunnel_status(&rule.name, &format!("本地连接 {peer}"));
                        if sender.send(LocalOpen {
                            stream,
                            target: rule.target.clone(),
                            name: rule.name.clone(),
                        }).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        observer.tunnel_status(&rule.name, &format!("监听失败: {error}"));
                        return;
                    }
                }
            }
        }
    }
}

async fn handle_ws_message(
    message: Message,
    writer: &WsWriter,
    events: &mpsc::Sender<TunnelEvent>,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
) -> Result<()> {
    match message {
        Message::Binary(bytes) => {
            let frame = Frame::decode(&bytes)?;
            frame.validate_server_to_client(true)?;
            handle_frame(frame, writer, events, observer, tunnels).await
        }
        Message::Ping(payload) => {
            writer.raw(Message::Pong(payload)).await;
            Ok(())
        }
        Message::Pong(_) => Ok(()),
        Message::Close(frame) => bail!("服务端关闭 WebSocket: {frame:?}"),
        Message::Text(text) if text.as_str() == "连接成功" => {
            bail!("检测到旧版 tows 文本协议；请升级服务端")
        }
        Message::Text(_) => bail!("协议只接受 WebSocket Binary 消息"),
        Message::Frame(_) => Ok(()),
    }
}

async fn handle_frame(
    frame: Frame,
    writer: &WsWriter,
    events: &mpsc::Sender<TunnelEvent>,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
) -> Result<()> {
    match frame.kind {
        FrameType::OpenOk => {
            let Some(Tunnel::Opening(opening)) = tunnels.get_mut(&frame.tunnel_id) else {
                bail!("OPEN_OK 指向未知或非 opening 流 {}", frame.tunnel_id);
            };
            let stream = opening.stream.take().context("本地 TCP 已被取走")?;
            let name = opening.name.clone();
            stream
                .set_nodelay(true)
                .context("无法启用本地 TCP_NODELAY")?;
            let flow = writer.register(frame.tunnel_id).await?;
            let tunnel =
                spawn_tcp_tasks(frame.tunnel_id, name.clone(), stream, flow, events.clone());
            tunnels.insert(frame.tunnel_id, Tunnel::Open(tunnel));
            observer.tunnel_status(&name, &format!("流 {} 已建立", frame.tunnel_id));
            Ok(())
        }
        FrameType::OpenFail => {
            let Some(Tunnel::Opening(opening)) = tunnels.remove(&frame.tunnel_id) else {
                bail!("OPEN_FAIL 指向未知或非 opening 流 {}", frame.tunnel_id);
            };
            let reason = std::str::from_utf8(&frame.payload)?;
            observer.tunnel_status(&opening.name, &format!("建立失败: {reason}"));
            Ok(())
        }
        FrameType::Data => {
            let tunnel = open_tunnel_mut(tunnels, frame.tunnel_id)?;
            if tunnel.remote_eof_seen {
                bail!("流 {} 在 EOF 后又收到 DATA", frame.tunnel_id);
            }
            tunnel
                .tcp_sender
                .send(TcpCommand::Data(frame.payload))
                .await
                .map_err(|_| anyhow!("流 {} 的本地 TCP 写任务已停止", frame.tunnel_id))
        }
        FrameType::Eof => {
            let tunnel = open_tunnel_mut(tunnels, frame.tunnel_id)?;
            if tunnel.remote_eof_seen {
                bail!("流 {} 收到重复 EOF", frame.tunnel_id);
            }
            tunnel.remote_eof_seen = true;
            tunnel
                .tcp_sender
                .send(TcpCommand::Eof)
                .await
                .map_err(|_| anyhow!("流 {} 的本地 TCP 写任务已停止", frame.tunnel_id))?;
            maybe_finish(frame.tunnel_id, writer, observer, tunnels).await
        }
        FrameType::Close => {
            if tunnels.contains_key(&frame.tunnel_id) {
                remove_tunnel(frame.tunnel_id, writer, observer, tunnels, "对端关闭").await;
            }
            Ok(())
        }
        _ => bail!("服务端发送了不允许的 {:?} 帧", frame.kind),
    }
}

async fn handle_tunnel_event(
    event: TunnelEvent,
    writer: &WsWriter,
    _events: &mpsc::Sender<TunnelEvent>,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
) -> Result<()> {
    match event {
        TunnelEvent::OpenTimeout(id) => {
            if let Some(Tunnel::Opening(opening)) = tunnels.remove(&id) {
                observer.tunnel_status(&opening.name, "OPEN 等待超过 15 秒");
                writer
                    .send(Frame::new(FrameType::Close, id, Vec::new())?)
                    .await?;
            }
        }
        TunnelEvent::LocalEof(id) => {
            if let Some(Tunnel::Open(tunnel)) = tunnels.get_mut(&id) {
                tunnel.local_eof_sent = true;
                maybe_finish(id, writer, observer, tunnels).await?;
            }
        }
        TunnelEvent::TcpWriterDone(id) => {
            if let Some(Tunnel::Open(tunnel)) = tunnels.get_mut(&id) {
                tunnel.tcp_writer_done = true;
                maybe_finish(id, writer, observer, tunnels).await?;
            }
        }
        TunnelEvent::TcpError(id, reason) => {
            if tunnels.contains_key(&id) {
                writer
                    .send(Frame::new(FrameType::Close, id, Vec::new())?)
                    .await?;
                remove_tunnel(
                    id,
                    writer,
                    observer,
                    tunnels,
                    &format!("TCP 错误: {reason}"),
                )
                .await;
            }
        }
        TunnelEvent::SessionError(reason) => return Err(anyhow!(reason)),
    }
    Ok(())
}

fn spawn_tcp_tasks(
    id: u16,
    name: String,
    stream: TcpStream,
    flow: FlowWriter,
    events: mpsc::Sender<TunnelEvent>,
) -> OpenTunnel {
    let (reader, writer) = stream.into_split();
    let (tcp_sender, receiver) = mpsc::channel(TCP_QUEUE_FRAMES);
    let read_events = events.clone();
    let reader_task = tokio::spawn(read_tcp(id, reader, flow, read_events));
    let writer_task = tokio::spawn(write_tcp(id, writer, receiver, events));
    OpenTunnel {
        name,
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
                let eof = Frame::new(FrameType::Eof, id, Vec::new()).expect("固定 EOF 合法");
                if flow.send_flushed(eof).await.is_ok() {
                    let _ = events.send(TunnelEvent::LocalEof(id)).await;
                }
                return;
            }
            Ok(size) => {
                let frame = Frame::new(FrameType::Data, id, buffer[..size].to_vec())
                    .expect("TCP 分片不超过协议上限");
                if flow.send(frame).await.is_err() {
                    return;
                }
            }
            Err(error) if is_normal_close(&error) => {
                let eof = Frame::new(FrameType::Eof, id, Vec::new()).expect("固定 EOF 合法");
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
        match command {
            TcpCommand::Data(data) => {
                if let Err(error) = writer.write_all(&data).await {
                    let _ = events
                        .send(TunnelEvent::TcpError(id, error.to_string()))
                        .await;
                    return;
                }
            }
            TcpCommand::Eof => {
                if let Err(error) = writer.shutdown().await {
                    let _ = events
                        .send(TunnelEvent::TcpError(id, error.to_string()))
                        .await;
                } else {
                    let _ = events.send(TunnelEvent::TcpWriterDone(id)).await;
                }
                return;
            }
        }
    }
}

fn open_tunnel_mut(tunnels: &mut HashMap<u16, Tunnel>, id: u16) -> Result<&mut OpenTunnel> {
    match tunnels.get_mut(&id) {
        Some(Tunnel::Open(tunnel)) => Ok(tunnel),
        Some(Tunnel::Opening(_)) => bail!("流 {id} 尚未 OPEN_OK 就收到数据"),
        None => bail!("帧指向未知 tunnel_id {id}"),
    }
}

async fn maybe_finish(
    id: u16,
    writer: &WsWriter,
    observer: &Arc<dyn ClientObserver>,
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
        remove_tunnel(id, writer, observer, tunnels, "双向 EOF").await;
    }
    Ok(())
}

async fn remove_tunnel(
    id: u16,
    writer: &WsWriter,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
    reason: &str,
) {
    if let Some(tunnel) = tunnels.remove(&id) {
        match tunnel {
            Tunnel::Opening(opening) => observer.tunnel_status(&opening.name, reason),
            Tunnel::Open(open) => {
                open.reader_task.abort();
                open.writer_task.abort();
                observer.tunnel_status(&open.name, reason);
            }
        }
    }
    writer.remove(id).await;
}

async fn close_all(writer: &WsWriter, tunnels: &mut HashMap<u16, Tunnel>) {
    let ids: Vec<u16> = tunnels.keys().copied().collect();
    for (_, tunnel) in tunnels.drain() {
        if let Tunnel::Open(open) = tunnel {
            open.reader_task.abort();
            open.writer_task.abort();
        }
    }
    for id in ids {
        writer.remove(id).await;
    }
}

fn allocate_id(tunnels: &HashMap<u16, Tunnel>, next: &mut u16) -> Result<u16> {
    for _ in 0..u16::MAX - 1 {
        if *next == 0 || *next == u16::MAX {
            *next = 1;
        }
        let candidate = *next;
        *next = next.saturating_add(1);
        if !tunnels.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    bail!("没有可用的 tunnel_id")
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

fn print_help() {
    println!(
        "用法:\n  towc\n  towc <tows-host[:port]> [--target <host:port|port>] [--listen <host:port|port>] [--login <手机号|邮箱>]\n\n默认值:\n  tows 端口 4489\n  --target 127.0.0.1:22\n  --listen 127.0.0.1:14489\n  未指定 --login 时使用微信扫码；有效缓存始终优先复用。"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    use std::sync::Mutex;
    use tokio_tungstenite::tungstenite::Message;

    struct TestObserver;

    impl ClientObserver for TestObserver {
        fn status(&self, _message: &str) {}
        fn tunnel_status(&self, _name: &str, _message: &str) {}
    }

    #[test]
    fn tunnel_ids_skip_reserved_values_and_active_ids() {
        let mut tunnels = HashMap::new();
        tunnels.insert(
            1,
            Tunnel::Opening(OpeningTunnel {
                stream: None,
                name: "test".to_string(),
            }),
        );
        let mut next = 1;
        assert_eq!(allocate_id(&tunnels, &mut next).unwrap(), 2);
        next = u16::MAX;
        assert_eq!(allocate_id(&tunnels, &mut next).unwrap(), 2);
    }

    #[test]
    fn server_address_parser_remains_available_for_gui() {
        assert_eq!(
            crate::address::parse_tows("example.test").unwrap().port(),
            4489
        );
    }

    #[tokio::test]
    async fn websocket_failure_stops_listeners_and_allows_manual_restart() {
        let local_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_port = local_probe.local_addr().unwrap().port();
        drop(local_probe);

        for _ in 0..2 {
            let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws_address = ws_listener.local_addr().unwrap();
            tokio::spawn(async move {
                let (stream, _) = ws_listener.accept().await.unwrap();
                let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                crate::network::server_handshake(&mut websocket, "test-tows")
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                websocket.send(Message::Close(None)).await.unwrap();
            });

            let rule = ForwardRule {
                name: "restart-test".to_string(),
                target: crate::address::parse_target("22").unwrap(),
                listen: crate::address::parse_listen(&local_port.to_string()).unwrap(),
            };
            let cookie = SessionCookie(Arc::new(Mutex::new(format!(
                "wengine_vpn_ticketwebvpn_szut_edu_cn=wrdvpn1-{}",
                "0".repeat(32)
            ))));
            let (_stop_tx, stop_rx) = watch::channel(false);
            let result = run_tunnels_to_url(
                format!("ws://{ws_address}/"),
                crate::address::parse_tows("127.0.0.1").unwrap(),
                vec![rule],
                cookie,
                stop_rx,
                Arc::new(TestObserver),
            )
            .await;
            assert!(result.is_err());
            assert!(TcpStream::connect(("127.0.0.1", local_port)).await.is_err());
        }
    }
}
