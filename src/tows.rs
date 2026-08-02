use crate::{
    DEFAULT_SERVER_PORT, SERVER_LISTEN_ADDR, SERVER_LISTEN_HOST, TOWS_READY_MESSAGE,
    TOWS_TARGET_CONNECT_FAILURE_PREFIX, WEBVPN_KEEPALIVE_PATH, WebVpnHeartbeatRole,
    accept_websocket_with_path, log_error, log_info, log_success,
    parse_socket_addr_with_default_host, parse_tcp_target_path, relay_stream,
    run_webvpn_heartbeat_websocket,
};
use anyhow::{Context, Result};
use futures_util::SinkExt;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const HTTP_PROBE_RESPONSE: &[u8] =
    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const MAX_WEBSOCKET_CLOSE_REASON_BYTES: usize = 123;
const MAX_HTTP_REQUEST_HEAD_BYTES: usize = 16 * 1024;
const HTTP_REQUEST_HEAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const TARGET_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Observable lifecycle state of an embedded server.
pub enum TowsServerState {
    /// No listener task is active.
    Stopped,
    /// The listener is being bound.
    Starting,
    /// The listener is accepting connections.
    Running {
        /// Actual bound address, including an OS-assigned port when requested.
        listen_addr: SocketAddr,
    },
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Classified purpose of an accepted server connection.
pub enum TowsConnectionKind {
    /// Plain HTTP health probe.
    HttpProbe,
    /// Heartbeat-only compatibility WebSocket.
    Keepalive,
    /// Data tunnel connected to a TCP target.
    Tunnel {
        /// Actual local endpoint through which this connection reached `tows`.
        server: SocketAddr,
        /// Normalized target address.
        target: String,
    },
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Structured events emitted by [`TowsServer`].
pub enum TowsEvent {
    /// Server lifecycle state changed.
    StateChanged(TowsServerState),
    /// A TCP connection was accepted.
    ConnectionOpened {
        /// Unique connection identifier.
        connection_id: u64,
        /// Remote peer address.
        peer: String,
    },
    /// The connection was classified and is ready.
    ConnectionReady {
        /// Unique connection identifier.
        connection_id: u64,
        /// Classified connection purpose.
        kind: TowsConnectionKind,
    },
    /// A previously opened connection ended.
    ConnectionClosed {
        /// Unique connection identifier.
        connection_id: u64,
        /// Remote peer address.
        peer: String,
    },
    /// Listener-level or connection-level failure.
    Error {
        /// Connection identifier, or `None` for listener failures.
        connection_id: Option<u64>,
        /// Human-readable error chain.
        detail: String,
    },
}

/// Receives structured server events.
///
/// `emit` runs synchronously on server tasks and should return quickly.
pub trait TowsEventSink: Send + Sync {
    /// Receives the next server event.
    fn emit(&self, event: TowsEvent);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Runtime configuration for a server listener.
pub struct TowsServerConfig {
    /// Address on which incoming HTTP and WebSocket connections are accepted.
    pub listen_addr: SocketAddr,
}

impl Default for TowsServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([0, 0, 0, 0], DEFAULT_SERVER_PORT)),
        }
    }
}

#[derive(Clone)]
/// Reusable, observable TCP-over-WebSocket server.
pub struct TowsServer {
    inner: Arc<TowsServerInner>,
}

struct TowsServerInner {
    state: Mutex<TowsServerState>,
    state_tx: watch::Sender<TowsServerState>,
    running: AtomicBool,
    next_connection_id: AtomicU64,
    events: Arc<dyn TowsEventSink>,
}

struct ServerRunGuard {
    inner: Arc<TowsServerInner>,
}

impl Drop for ServerRunGuard {
    fn drop(&mut self) {
        self.inner.running.store(false, Ordering::Release);
        let state = TowsServerState::Stopped;
        *self.inner.state.lock().expect("tows server state poisoned") = state;
        self.inner.state_tx.send_replace(state);
        self.inner.events.emit(TowsEvent::StateChanged(state));
    }
}

impl TowsServer {
    /// Creates a stopped server using `events` for callbacks.
    pub fn new(events: Arc<dyn TowsEventSink>) -> Self {
        let initial = TowsServerState::Stopped;
        let (state_tx, _) = watch::channel(initial);
        Self {
            inner: Arc::new(TowsServerInner {
                state: Mutex::new(initial),
                state_tx,
                running: AtomicBool::new(false),
                next_connection_id: AtomicU64::new(1),
                events,
            }),
        }
    }

    /// Returns a snapshot of the current server state.
    pub fn state(&self) -> TowsServerState {
        *self.inner.state.lock().expect("tows server state poisoned")
    }

    /// Subscribes to server state changes.
    pub fn subscribe(&self) -> watch::Receiver<TowsServerState> {
        self.inner.state_tx.subscribe()
    }

    /// Binds and runs the server until `shutdown` becomes true or closes.
    pub async fn run(
        &self,
        config: TowsServerConfig,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if self
            .inner
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            anyhow::bail!("tows server is already running");
        }
        let _run_guard = ServerRunGuard {
            inner: Arc::clone(&self.inner),
        };
        self.set_state(TowsServerState::Starting);
        let result = self.run_inner(config, shutdown).await;
        if let Err(err) = &result {
            self.emit_error(None, format!("{err:#}"));
        }
        result
    }

    async fn run_inner(
        &self,
        config: TowsServerConfig,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let listener = TcpListener::bind(config.listen_addr)
            .await
            .with_context(|| format!("failed to bind server on {}", config.listen_addr))?;
        let listen_addr = listener
            .local_addr()
            .context("failed to read bound server address")?;
        self.set_state(TowsServerState::Running { listen_addr });

        let mut connections = JoinSet::new();
        let mut peers = HashMap::<u64, String>::new();
        if *shutdown.borrow() {
            return Ok(());
        }
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, peer_addr) = accepted.context("failed to accept incoming connection")?;
                    let connection_id = self
                        .inner
                        .next_connection_id
                        .fetch_add(1, Ordering::Relaxed);
                    let peer = peer_addr.to_string();
                    peers.insert(connection_id, peer.clone());
                    self.inner.events.emit(TowsEvent::ConnectionOpened {
                        connection_id,
                        peer: peer.clone(),
                    });
                    let events = Arc::clone(&self.inner.events);
                    connections.spawn(async move {
                        let result = handle_connection(connection_id, stream, events).await;
                        (connection_id, peer, result)
                    });
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    let Some(completed) = completed else {
                        continue;
                    };
                    if let Ok((connection_id, peer, result)) = completed {
                        peers.remove(&connection_id);
                        if let Err(err) = result {
                            self.emit_error(Some(connection_id), format!("{err:#}"));
                        }
                        self.inner.events.emit(TowsEvent::ConnectionClosed {
                            connection_id,
                            peer,
                        });
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        connections.abort_all();
                        for (connection_id, peer) in peers.drain() {
                            self.inner.events.emit(TowsEvent::ConnectionClosed {
                                connection_id,
                                peer,
                            });
                        }
                        return Ok(());
                    }
                }
            }
        }
    }

    fn set_state(&self, state: TowsServerState) {
        *self.inner.state.lock().expect("tows server state poisoned") = state;
        self.inner.state_tx.send_replace(state);
        self.inner.events.emit(TowsEvent::StateChanged(state));
    }

    fn emit_error(&self, connection_id: Option<u64>, detail: String) {
        self.inner.events.emit(TowsEvent::Error {
            connection_id,
            detail,
        });
    }
}

/// Convenience wrapper that constructs and runs one [`TowsServer`].
pub async fn run_tows_server(
    config: TowsServerConfig,
    events: Arc<dyn TowsEventSink>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    TowsServer::new(events).run(config, shutdown).await
}

struct TerminalEventSink;

fn format_tunnel_ready(server: SocketAddr, target: &str) -> String {
    format!("ready: WebVPN -> tows {server} -> target {target}")
}

impl TowsEventSink for TerminalEventSink {
    fn emit(&self, event: TowsEvent) {
        match event {
            TowsEvent::StateChanged(TowsServerState::Running { listen_addr }) => {
                log_success("server", format!("listening on {listen_addr}"));
            }
            TowsEvent::ConnectionReady {
                kind: TowsConnectionKind::Tunnel { server, target },
                ..
            } => log_success("tunnel", format_tunnel_ready(server, &target)),
            TowsEvent::Error { detail, .. } => log_error("tunnel", detail),
            _ => {}
        }
    }
}

/// Runs the `tows` command-line interface.
pub async fn run_cli() -> Result<()> {
    log_info("server", format!("tows v{}", env!("CARGO_PKG_VERSION")));
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_usage();
        return Ok(());
    }
    if args.len() > 1 {
        anyhow::bail!("too many arguments; use tows [port]");
    }

    let listen_addr = args
        .first()
        .cloned()
        .unwrap_or_else(|| SERVER_LISTEN_ADDR.to_string());
    let listen_addr = parse_socket_addr_with_default_host(&listen_addr, SERVER_LISTEN_HOST)?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    let events: Arc<dyn TowsEventSink> = Arc::new(TerminalEventSink);
    run_tows_server(TowsServerConfig { listen_addr }, events, shutdown_rx).await?;
    log_info("server", "shutting down");
    Ok(())
}

fn print_usage() {
    println!("Usage: tows [port]");
    println!("       default port: {DEFAULT_SERVER_PORT}");
}

async fn handle_connection(
    connection_id: u64,
    stream: TcpStream,
    events: Arc<dyn TowsEventSink>,
) -> Result<()> {
    let server_addr = stream
        .local_addr()
        .context("failed to read accepted connection local address")?;
    if !is_websocket_upgrade_request(&stream).await? {
        events.emit(TowsEvent::ConnectionReady {
            connection_id,
            kind: TowsConnectionKind::HttpProbe,
        });
        return respond_http_probe(stream).await;
    }

    let (mut websocket, path) = accept_websocket_with_path(stream).await?;
    if path == WEBVPN_KEEPALIVE_PATH {
        events.emit(TowsEvent::ConnectionReady {
            connection_id,
            kind: TowsConnectionKind::Keepalive,
        });
        return run_webvpn_heartbeat_websocket(websocket, WebVpnHeartbeatRole::Server).await;
    }

    let target_addr = parse_tcp_target_path(&path)?;
    let target_result = match tokio::time::timeout(
        TARGET_CONNECT_TIMEOUT,
        TcpStream::connect(&target_addr),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "target connection timed out after {}s",
                TARGET_CONNECT_TIMEOUT.as_secs()
            ),
        )),
    };
    let target = match target_result {
        Ok(target) => target,
        Err(err) => {
            let reason = target_connect_failure_close_reason(&target_addr, &err);
            let _ = websocket
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Error,
                    reason: reason.into(),
                })))
                .await;
            anyhow::bail!(
                "target connect failed: {path} -> {target_addr}: {err}; diagnosis: {}",
                target_connect_failure_diagnosis(&err)
            );
        }
    };
    websocket
        .send(Message::Text(TOWS_READY_MESSAGE.into()))
        .await
        .context("failed to acknowledge tunnel readiness")?;
    events.emit(TowsEvent::ConnectionReady {
        connection_id,
        kind: TowsConnectionKind::Tunnel {
            server: server_addr,
            target: target_addr,
        },
    });

    relay_stream(websocket, target, WebVpnHeartbeatRole::Server).await
}

fn target_connect_failure_close_reason(target_addr: &str, err: &io::Error) -> String {
    truncate_websocket_close_reason(&format!(
        "{TOWS_TARGET_CONNECT_FAILURE_PREFIX}: {target_addr}: {err}"
    ))
}

fn target_connect_failure_diagnosis(err: &io::Error) -> &'static str {
    match err.kind() {
        io::ErrorKind::ConnectionRefused => {
            "target service is not listening or refused the connection"
        }
        io::ErrorKind::TimedOut => "target host or firewall did not answer before timeout",
        io::ErrorKind::NotFound | io::ErrorKind::AddrNotAvailable => {
            "target address is not available on the tows host"
        }
        io::ErrorKind::PermissionDenied => {
            "tows does not have permission to connect to the target endpoint"
        }
        _ => "target endpoint is unreachable from the tows host",
    }
}

fn truncate_websocket_close_reason(reason: &str) -> String {
    if reason.len() <= MAX_WEBSOCKET_CLOSE_REASON_BYTES {
        return reason.to_string();
    }

    let mut end = MAX_WEBSOCKET_CLOSE_REASON_BYTES.saturating_sub(3);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &reason[..end])
}

async fn is_websocket_upgrade_request(stream: &TcpStream) -> Result<bool> {
    let mut buffer = vec![0_u8; MAX_HTTP_REQUEST_HEAD_BYTES];
    tokio::time::timeout(HTTP_REQUEST_HEAD_TIMEOUT, async {
        loop {
            let read_size = stream
                .peek(&mut buffer)
                .await
                .context("failed to inspect incoming request")?;
            let head = &buffer[..read_size];
            if head.windows(4).any(|window| window == b"\r\n\r\n") {
                let request = String::from_utf8_lossy(head);
                return Ok(has_websocket_upgrade_headers(&request));
            }
            if read_size == buffer.len() {
                anyhow::bail!(
                    "incoming HTTP request headers exceed {MAX_HTTP_REQUEST_HEAD_BYTES} bytes"
                );
            }
            if read_size == 0 {
                anyhow::bail!("connection closed before HTTP request headers were complete");
            }

            // MSG_PEEK leaves the bytes readable, so wait briefly for the next TCP
            // segment instead of immediately spinning on the same prefix.
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .context("timed out waiting for complete HTTP request headers")?
}

fn has_websocket_upgrade_headers(request: &str) -> bool {
    let mut has_upgrade_header = false;
    let mut has_websocket_header = false;

    for line in request.lines() {
        let line = line.trim();
        if line.is_empty() {
            break;
        }

        let Some((name, value)) = line.split_once(':') else {
            continue;
        };

        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_ascii_lowercase();

        if name == "connection" && value.split(',').any(|part| part.trim() == "upgrade") {
            has_upgrade_header = true;
        }

        if name == "upgrade" && value == "websocket" {
            has_websocket_header = true;
        }
    }

    has_upgrade_header && has_websocket_header
}

async fn respond_http_probe(mut stream: TcpStream) -> Result<()> {
    stream
        .write_all(HTTP_PROBE_RESPONSE)
        .await
        .context("failed to write http probe response")?;
    stream
        .shutdown()
        .await
        .context("failed to close http probe connection")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<TowsEvent>>,
    }

    impl TowsEventSink for RecordingSink {
        fn emit(&self, event: TowsEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[tokio::test]
    async fn embedded_server_reports_state_and_handles_http_probe() {
        let sink = Arc::new(RecordingSink::default());
        let events: Arc<dyn TowsEventSink> = sink.clone();
        let server = TowsServer::new(events);
        let mut state = server.subscribe();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running_server = server.clone();
        let task = tokio::spawn(async move {
            running_server
                .run(
                    TowsServerConfig {
                        listen_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
                    },
                    shutdown_rx,
                )
                .await
        });

        let listen_addr = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let TowsServerState::Running { listen_addr } = *state.borrow() {
                    break listen_addr;
                }
                state.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let mut stream = TcpStream::connect(listen_addr).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 204 No Content"));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if sink.events.lock().unwrap().iter().any(|event| {
                    matches!(
                        event,
                        TowsEvent::ConnectionReady {
                            kind: TowsConnectionKind::HttpProbe,
                            ..
                        }
                    )
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(server.state(), TowsServerState::Stopped);
    }

    #[tokio::test]
    async fn websocket_upgrade_detection_waits_for_fragmented_headers() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(listen_addr).await.unwrap();
            stream
                .write_all(b"GET /tcp HTTP/1.1\r\nHost: test\r\nConnec")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            stream
                .write_all(b"tion: Upgrade\r\nUpgrade: websocket\r\n\r\n")
                .await
                .unwrap();
        });
        let (stream, _) = listener.accept().await.unwrap();

        assert!(is_websocket_upgrade_request(&stream).await.unwrap());
        client.await.unwrap();
    }

    #[tokio::test]
    async fn tunnel_acknowledges_readiness_after_target_connects() {
        let target_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        let ws_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let sink: Arc<dyn TowsEventSink> = Arc::new(RecordingSink::default());

        let handler = tokio::spawn(async move {
            let (stream, _) = ws_listener.accept().await.unwrap();
            handle_connection(1, stream, sink).await
        });
        let target_accept = tokio::spawn(async move { target_listener.accept().await.unwrap() });

        let (mut websocket, _) =
            tokio_tungstenite::connect_async(format!("ws://{ws_addr}/tcp/{target_addr}"))
                .await
                .unwrap();
        let message = websocket.next().await.unwrap().unwrap();
        assert_eq!(message, Message::Text(TOWS_READY_MESSAGE.into()));

        websocket.close(None).await.unwrap();
        target_accept.await.unwrap();
        handler.await.unwrap().unwrap();
    }

    #[test]
    fn target_connect_close_reason_stays_within_websocket_limit() {
        let err = io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused");
        let target_addr = format!("127.0.0.1:{}", "5".repeat(200));

        let reason = target_connect_failure_close_reason(&target_addr, &err);

        assert!(reason.len() <= MAX_WEBSOCKET_CLOSE_REASON_BYTES);
        assert!(reason.starts_with(TOWS_TARGET_CONNECT_FAILURE_PREFIX));
        assert!(reason.ends_with("..."));
    }

    #[test]
    fn target_connect_diagnosis_names_refused_connections() {
        let err = io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused");

        assert_eq!(
            target_connect_failure_diagnosis(&err),
            "target service is not listening or refused the connection"
        );
    }

    #[test]
    fn terminal_ready_message_contains_the_complete_server_path() {
        assert_eq!(
            format_tunnel_ready(SocketAddr::from(([10, 18, 47, 77], 4489)), "127.0.0.1:22"),
            "ready: WebVPN -> tows 10.18.47.77:4489 -> target 127.0.0.1:22"
        );
    }
}
