#![warn(missing_docs)]

//! TCP-over-WebSocket client and server components for SZUT WebVPN.
//!
//! Enable the `client` feature for login and local tunnel management, `server`
//! for the remote forwarding server, and `cli` for terminal integration. All
//! three are enabled by default for compatibility with the command-line package.

#[cfg(feature = "client")]
use aes::Aes128;
#[cfg(feature = "client")]
use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
#[cfg(feature = "client")]
use num_bigint::BigUint;
#[cfg(feature = "client")]
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, MissedTickBehavior};
#[cfg(feature = "client")]
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
#[cfg(feature = "client")]
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::Request as ServerRequest;
#[cfg(feature = "client")]
use tokio_tungstenite::tungstenite::http::header::LOCATION;
#[cfg(feature = "client")]
use tokio_tungstenite::tungstenite::http::header::{COOKIE, HeaderValue};
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

#[cfg(feature = "client")]
/// Client session, login, and local tunnel management.
pub mod towc;
#[cfg(feature = "server")]
/// Remote TCP forwarding server.
pub mod tows;

/// Default address used by the server CLI.
pub const SERVER_LISTEN_ADDR: &str = "0.0.0.0:4489";
/// Default host used when the server CLI receives only a port.
pub const SERVER_LISTEN_HOST: &str = "0.0.0.0";
/// Default remote server port.
pub const DEFAULT_SERVER_PORT: u16 = 4489;
/// Default local client listener port.
pub const DEFAULT_LOCAL_LISTEN_PORT: u16 = 14489;
/// Default local client listener address.
pub const DEFAULT_LOCAL_LISTEN_ADDR: &str = "127.0.0.1:14489";
/// Default target host on the server.
pub const DEFAULT_TARGET_HOST: &str = "127.0.0.1";
/// Default target port on the server.
pub const DEFAULT_TARGET_PORT: u16 = 22;
/// Default target address on the server.
pub const DEFAULT_TARGET_ADDR: &str = "127.0.0.1:22";
/// SZUT WebVPN gateway host used by client URL builders.
pub const DEFAULT_WEBVPN_WS_HOST: &str = "webvpn.szut.edu.cn";
/// Prefix used in WebSocket close reasons for target connection failures.
pub const TOWS_TARGET_CONNECT_FAILURE_PREFIX: &str = "tows target connect failed";
/// WebSocket path reserved for compatibility keepalive connections.
pub const WEBVPN_KEEPALIVE_PATH: &str = "/webvpn-keepalive";
/// Text frame used as the WebVPN application heartbeat.
pub const WEBVPN_HEARTBEAT_MESSAGE: &str = "连接成功";
/// Control frame sent by `tows` after the requested target TCP connection succeeds.
///
/// It intentionally reuses the legacy heartbeat value so older clients consume
/// the frame instead of forwarding it into the local TCP stream.
pub const TOWS_READY_MESSAGE: &str = WEBVPN_HEARTBEAT_MESSAGE;
/// Control frame indicating that one TCP write direction reached EOF.
pub const TOWS_TCP_EOF_MESSAGE: &str = "tows-tcp-eof";
/// Interval between session-level WebVPN keepalive frames.
pub const WEBVPN_HEARTBEAT_INTERVAL_SECS: u64 = 210;
/// Interval between heartbeat frames on active data WebSockets.
pub const WEBVPN_DATA_HEARTBEAT_INTERVAL_SECS: u64 = 60;
/// Maximum duration allowed for a client WebSocket handshake.
pub const WEBSOCKET_CONNECT_TIMEOUT_SECS: u64 = 8;

const RELAY_BUFFER_SIZE: usize = 64 * 1024;

#[cfg(feature = "client")]
const WEBVPN_AES_KEY: &[u8; 16] = b"wrdvpnisthebest!";
#[cfg(feature = "client")]
const WEBVPN_ENCRYPTED_PREFIX: &str = "77726476706e69737468656265737421";
#[cfg(feature = "client")]
const RSA_CHUNK_SIZE: usize = 62;

const RESET: &str = "\x1b[0m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogLevel {
    Info,
    Success,
    Warning,
    Error,
}

impl LogLevel {
    const fn color(self) -> &'static str {
        match self {
            Self::Info => CYAN,
            Self::Success => GREEN,
            Self::Warning => YELLOW,
            Self::Error => RED,
        }
    }
}

#[cfg(feature = "client")]
#[non_exhaustive]
#[derive(Debug)]
/// Failure categories produced while opening a WebVPN WebSocket.
pub enum ConnectFailure {
    /// The gateway redirected to login because the session expired.
    CookieExpired {
        /// Redirect location returned by the gateway.
        location: String,
    },
    /// The WebVPN gateway rejected or could not reach the tunnel endpoint.
    WebVpnFailed {
        /// Failure location returned by the gateway.
        location: String,
    },
    /// Any transport, protocol, or request construction error.
    Other(anyhow::Error),
}

#[cfg(feature = "client")]
impl fmt::Display for ConnectFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CookieExpired { location } => {
                write!(
                    formatter,
                    "cookie expired, please log in again; location: {location}"
                )
            }
            Self::WebVpnFailed { location } => {
                write!(formatter, "WebVPN returned failed; location: {location}")
            }
            Self::Other(err) => write!(formatter, "{err}"),
        }
    }
}

#[cfg(feature = "client")]
impl std::error::Error for ConnectFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Other(err) => Some(err.as_ref()),
            Self::CookieExpired { .. } | Self::WebVpnFailed { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Determines which side sends or echoes the WebVPN application heartbeat.
pub enum WebVpnHeartbeatRole {
    /// Sends heartbeat frames.
    Client,
    /// Echoes heartbeat frames.
    Server,
}

impl WebVpnHeartbeatRole {
    fn sends_heartbeat(self) -> bool {
        matches!(self, Self::Client)
    }

    fn echoes_heartbeat(self) -> bool {
        matches!(self, Self::Server)
    }
}

fn webvpn_heartbeat_interval(period_secs: u64) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(Duration::from_secs(period_secs));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval
}

/// Converts a strict `/tcp` or `/tcp/...` path into a TCP target address.
pub fn parse_tcp_target_path(path: &str) -> Result<String> {
    let target = match path {
        "/tcp" => "",
        _ => path
            .strip_prefix("/tcp/")
            .ok_or_else(|| anyhow!("unsupported path: {path}"))?,
    };

    if target.is_empty() {
        return Ok(DEFAULT_TARGET_ADDR.to_string());
    }

    if let Ok(port) = parse_port(target) {
        return Ok(format!("{DEFAULT_TARGET_HOST}:{port}"));
    }

    let Some((host, port)) = target.rsplit_once(':') else {
        return Err(anyhow!("invalid tcp target in path: {path}"));
    };
    let port = parse_port(port)?;
    if host.trim().is_empty() {
        return Ok(format!("{DEFAULT_TARGET_HOST}:{port}"));
    }

    Ok(format!("{}:{port}", host.trim()))
}

#[cfg(feature = "client")]
/// Builds the WebVPN WebSocket URL for a data tunnel.
pub fn build_webvpn_ws_url(server: &str, target: Option<&str>) -> Result<String> {
    let server = parse_host_port(server, DEFAULT_SERVER_PORT, DEFAULT_TARGET_HOST, "server")?;
    let target_addr = normalize_tcp_target_arg(target)?;
    let target_path = tcp_target_url_path_from_addr(&target_addr)?;
    let encrypted_host = encrypt_webvpn_host(&server.host)?;

    Ok(format!(
        "wss://{DEFAULT_WEBVPN_WS_HOST}/ws-{}/{WEBVPN_ENCRYPTED_PREFIX}{encrypted_host}{target_path}",
        server.port
    ))
}

#[cfg(feature = "client")]
/// Builds the WebVPN WebSocket URL for a compatibility keepalive connection.
pub fn build_webvpn_keepalive_ws_url(server: &str) -> Result<String> {
    let server = parse_host_port(server, DEFAULT_SERVER_PORT, DEFAULT_TARGET_HOST, "server")?;
    let encrypted_host = encrypt_webvpn_host(&server.host)?;

    Ok(format!(
        "wss://{DEFAULT_WEBVPN_WS_HOST}/ws-{}/{WEBVPN_ENCRYPTED_PREFIX}{encrypted_host}{WEBVPN_KEEPALIVE_PATH}",
        server.port
    ))
}

#[cfg(feature = "client")]
/// Normalizes a server host or host-port value with the default server port.
pub fn normalize_server_addr(value: &str) -> Result<String> {
    let server = parse_host_port(value, DEFAULT_SERVER_PORT, DEFAULT_TARGET_HOST, "server")?;
    Ok(format!("{}:{}", server.host, server.port))
}

#[cfg(feature = "client")]
/// Normalizes an optional TCP target, accepting a port-only shorthand.
pub fn normalize_tcp_target_arg(value: Option<&str>) -> Result<String> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(DEFAULT_TARGET_ADDR.to_string());
    };

    if let Ok(port) = parse_port(value) {
        return Ok(format!("{DEFAULT_TARGET_HOST}:{port}"));
    }

    let target = parse_host_port(value, DEFAULT_TARGET_PORT, DEFAULT_TARGET_HOST, "target")?;
    Ok(format!("{}:{}", target.host, target.port))
}

#[cfg(feature = "client")]
/// Performs the RSA encoding required by the SZUT WebVPN login endpoint.
pub fn rsa_encrypt(plain: &str, modulus_hex: &str, exponent_hex: &str) -> Result<String> {
    let modulus = BigUint::parse_bytes(modulus_hex.as_bytes(), 16)
        .ok_or_else(|| anyhow!("invalid RSA modulus hex"))?;
    let exponent = BigUint::parse_bytes(exponent_hex.as_bytes(), 16)
        .ok_or_else(|| anyhow!("invalid RSA exponent hex"))?;

    let mut codes: Vec<u16> = plain.encode_utf16().collect();
    let padded_len = codes.len().div_ceil(RSA_CHUNK_SIZE) * RSA_CHUNK_SIZE;
    codes.resize(padded_len, 0);

    let mut parts = Vec::new();
    for chunk in codes.chunks(RSA_CHUNK_SIZE) {
        let mut bytes = Vec::with_capacity(RSA_CHUNK_SIZE);
        for pair in chunk.chunks(2) {
            let high = pair.get(1).copied().unwrap_or_default();
            let digit = u32::from(pair[0]) | (u32::from(high) << 8);
            bytes.push((digit & 0xff) as u8);
            bytes.push(((digit >> 8) & 0xff) as u8);
        }

        let block = BigUint::from_bytes_le(&bytes);
        let encrypted = block.modpow(&exponent, &modulus);
        parts.push(encrypted.to_str_radix(16));
    }

    Ok(parts.join(" "))
}

#[cfg(feature = "client")]
fn tcp_target_url_path_from_addr(target_addr: &str) -> Result<String> {
    if target_addr == DEFAULT_TARGET_ADDR {
        return Ok("/tcp".to_string());
    }

    let Some((host, port)) = target_addr.rsplit_once(':') else {
        return Err(anyhow!("invalid tcp target: {target_addr}"));
    };

    if host == DEFAULT_TARGET_HOST {
        return Ok(format!("/tcp/{port}"));
    }

    Ok(format!("/tcp/{host}:{port}"))
}

#[cfg(feature = "client")]
fn encrypt_webvpn_host(host: &str) -> Result<String> {
    let ciphertext = aes_128_cfb_encrypt(host.as_bytes(), WEBVPN_AES_KEY, WEBVPN_AES_KEY)?;
    Ok(hex_encode(&ciphertext))
}

#[cfg(feature = "client")]
fn aes_128_cfb_encrypt(plaintext: &[u8], key: &[u8; 16], iv: &[u8; 16]) -> Result<Vec<u8>> {
    let cipher = Aes128::new_from_slice(key).context("failed to initialize AES-128 cipher")?;
    let mut feedback = *iv;
    let mut ciphertext = Vec::with_capacity(plaintext.len());

    for chunk in plaintext.chunks(16) {
        let mut block = GenericArray::clone_from_slice(&feedback);
        cipher.encrypt_block(&mut block);

        let offset = ciphertext.len();
        ciphertext.extend(
            chunk
                .iter()
                .zip(block.iter())
                .map(|(plain_byte, key_byte)| plain_byte ^ key_byte),
        );

        let encrypted_chunk = &ciphertext[offset..];
        if encrypted_chunk.len() == 16 {
            feedback.copy_from_slice(encrypted_chunk);
        } else {
            feedback[..encrypted_chunk.len()].copy_from_slice(encrypted_chunk);
        }
    }

    Ok(ciphertext)
}

#[cfg(feature = "client")]
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(feature = "client")]
struct HostPort {
    host: String,
    port: u16,
}

#[cfg(feature = "client")]
fn parse_host_port(
    value: &str,
    default_port: u16,
    default_host: &str,
    label: &str,
) -> Result<HostPort> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("{label} address cannot be empty"));
    }

    let (host, port) = match value.rsplit_once(':') {
        Some((host, port)) => {
            let host = host.trim();
            let port = if port.trim().is_empty() {
                default_port
            } else {
                parse_port(port.trim())?
            };
            (host, port)
        }
        None => (value, default_port),
    };

    let host = if host.is_empty() { default_host } else { host };

    if host.contains('/') || host.contains('?') || host.contains('#') {
        return Err(anyhow!("invalid {label} host: {host}"));
    }

    Ok(HostPort {
        host: host.to_string(),
        port,
    })
}

/// Parses a concrete IP socket address.
pub fn parse_socket_addr(value: &str) -> Result<SocketAddr> {
    value
        .parse::<SocketAddr>()
        .with_context(|| format!("invalid socket address: {value}"))
}

/// Parses a socket address and expands a port-only value with `default_host`.
pub fn parse_socket_addr_with_default_host(value: &str, default_host: &str) -> Result<SocketAddr> {
    if let Ok(port) = parse_port(value) {
        return parse_socket_addr(&format!("{default_host}:{port}"));
    }

    parse_socket_addr(value)
}

fn parse_port(value: &str) -> Result<u16> {
    let port = value
        .parse::<u16>()
        .with_context(|| format!("invalid port: {value}"))?;

    if port == 0 {
        return Err(anyhow!("port must be greater than zero"));
    }

    Ok(port)
}

/// Writes an informational terminal log message.
pub fn log_info(scope: &str, message: impl AsRef<str>) {
    log_message(LogLevel::Info, scope, message.as_ref());
}

/// Writes a successful-operation terminal log message.
pub fn log_success(scope: &str, message: impl AsRef<str>) {
    log_message(LogLevel::Success, scope, message.as_ref());
}

/// Writes a warning terminal log message.
pub fn log_warn(scope: &str, message: impl AsRef<str>) {
    log_message(LogLevel::Warning, scope, message.as_ref());
}

/// Writes an error terminal log message.
pub fn log_error(scope: &str, message: impl AsRef<str>) {
    log_message(LogLevel::Error, scope, message.as_ref());
}

fn log_message(level: LogLevel, scope: &str, message: &str) {
    eprintln!("{}", format_log_message(level, scope, message));
}

fn format_log_message(level: LogLevel, scope: &str, message: &str) -> String {
    format!("{}[{scope}]{RESET} {message}", level.color())
}

/// Relays TCP bytes and WebSocket frames until either side closes.
pub async fn relay_stream<S>(
    websocket: WebSocketStream<S>,
    tcp: TcpStream,
    heartbeat_role: WebVpnHeartbeatRole,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tcp.set_nodelay(true)
        .context("failed to enable TCP_NODELAY on relay stream")?;
    let (mut ws_sink, mut ws_stream) = websocket.split();
    let (mut tcp_read, mut tcp_write) = tcp.into_split();
    let mut buffer = Vec::with_capacity(RELAY_BUFFER_SIZE);
    let mut heartbeat_interval = webvpn_heartbeat_interval(WEBVPN_DATA_HEARTBEAT_INTERVAL_SECS);
    let mut tcp_read_open = true;
    let mut remote_tcp_eof = false;

    loop {
        tokio::select! {
            _ = heartbeat_interval.tick(), if heartbeat_role.sends_heartbeat() => {
                if let Err(err) = ws_sink
                    .send(Message::Text(WEBVPN_HEARTBEAT_MESSAGE.into()))
                    .await
                {
                    if is_normal_websocket_close(&err) {
                        break;
                    }
                    return Err(err).context("failed to send WebVPN heartbeat");
                }
            }
            read_result = tcp_read.read_buf(&mut buffer), if tcp_read_open => {
                let read_size = match read_result {
                    Ok(read_size) => read_size,
                    Err(err) if is_normal_connection_close(&err) => 0,
                    Err(err) => return Err(err).context("failed to read from tcp stream"),
                };
                if read_size == 0 {
                    tcp_read_open = false;
                    if let Err(err) = ws_sink
                        .send(Message::Text(TOWS_TCP_EOF_MESSAGE.into()))
                        .await
                    {
                        if is_normal_websocket_close(&err) {
                            break;
                        }
                        return Err(err).context("failed to send tcp EOF control frame");
                    }
                    if remote_tcp_eof {
                        let _ = ws_sink.send(Message::Close(None)).await;
                        break;
                    }
                    continue;
                }

                let payload = std::mem::replace(
                    &mut buffer,
                    Vec::with_capacity(RELAY_BUFFER_SIZE),
                );
                if let Err(err) = ws_sink.send(Message::Binary(payload.into())).await {
                    if is_normal_websocket_close(&err) {
                        break;
                    }
                    return Err(err).context("failed to send websocket data");
                }
            }
            message_result = ws_stream.next() => {
                match message_result {
                    Some(Ok(Message::Binary(data))) => {
                        if let Err(err) = tcp_write.write_all(&data).await {
                            if is_normal_connection_close(&err) {
                                break;
                            }
                            return Err(err)
                                .context("failed to write websocket binary payload to tcp");
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        if heartbeat_role == WebVpnHeartbeatRole::Client
                            && text.as_str() == TOWS_READY_MESSAGE
                        {
                            continue;
                        }
                        if text.as_str() == TOWS_TCP_EOF_MESSAGE {
                            if let Err(err) = tcp_write.shutdown().await
                                && !is_normal_connection_close(&err)
                            {
                                return Err(err).context("failed to apply remote tcp EOF");
                            }
                            remote_tcp_eof = true;
                            if !tcp_read_open {
                                let _ = ws_sink.send(Message::Close(None)).await;
                                break;
                            }
                            continue;
                        }
                        if text.as_str() == WEBVPN_HEARTBEAT_MESSAGE {
                            if heartbeat_role.echoes_heartbeat()
                                && let Err(err) = ws_sink.send(Message::Text(text)).await
                            {
                                if is_normal_websocket_close(&err) {
                                    break;
                                }
                                return Err(err).context("failed to echo WebVPN heartbeat");
                            }
                            continue;
                        }

                        if let Err(err) = tcp_write.write_all(text.as_bytes()).await {
                            if is_normal_connection_close(&err) {
                                break;
                            }
                            return Err(err)
                                .context("failed to write websocket text payload to tcp");
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if let Err(err) = ws_sink.send(Message::Pong(payload)).await {
                            if is_normal_websocket_close(&err) {
                                break;
                            }
                            return Err(err).context("failed to reply to websocket ping");
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        let _ = tcp_write.shutdown().await;
                        break;
                    }
                    Some(Err(err)) if is_normal_websocket_close(&err) => break,
                    Some(Err(err)) => return Err(err.into()),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

/// Runs a heartbeat-only WebSocket connection until it closes.
pub async fn run_webvpn_heartbeat_websocket<S>(
    websocket: WebSocketStream<S>,
    heartbeat_role: WebVpnHeartbeatRole,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut ws_sink, mut ws_stream) = websocket.split();
    let mut heartbeat_interval = webvpn_heartbeat_interval(WEBVPN_HEARTBEAT_INTERVAL_SECS);

    loop {
        tokio::select! {
            _ = heartbeat_interval.tick(), if heartbeat_role.sends_heartbeat() => {
                if let Err(err) = ws_sink
                    .send(Message::Text(WEBVPN_HEARTBEAT_MESSAGE.into()))
                    .await
                {
                    if is_normal_websocket_close(&err) {
                        break;
                    }
                    return Err(err).context("failed to send WebVPN heartbeat");
                }
            }
            message_result = ws_stream.next() => {
                match message_result {
                    Some(Ok(Message::Text(text))) => {
                        if text.as_str() == WEBVPN_HEARTBEAT_MESSAGE
                            && heartbeat_role.echoes_heartbeat()
                            && let Err(err) = ws_sink.send(Message::Text(text)).await
                        {
                            if is_normal_websocket_close(&err) {
                                break;
                            }
                            return Err(err).context("failed to echo WebVPN heartbeat");
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if let Err(err) = ws_sink.send(Message::Pong(payload)).await {
                            if is_normal_websocket_close(&err) {
                                break;
                            }
                            return Err(err).context("failed to reply to websocket ping");
                        }
                    }
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Err(err)) if is_normal_websocket_close(&err) => break,
                    Some(Err(err)) => return Err(err.into()),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

fn is_normal_connection_close(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

fn is_normal_websocket_close(err: &WebSocketError) -> bool {
    matches!(
        err,
        WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed
    ) || matches!(err, WebSocketError::Io(err) if is_normal_connection_close(err))
}

#[cfg(feature = "client")]
/// Opens an authenticated WebVPN WebSocket using the supplied Cookie header.
pub async fn connect_websocket(
    url: &str,
    cookie: &str,
) -> std::result::Result<
    WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    ConnectFailure,
> {
    let mut request = url.into_client_request().map_err(|err| {
        ConnectFailure::Other(anyhow!(err).context("failed to build websocket request"))
    })?;

    request.headers_mut().insert(
        COOKIE,
        HeaderValue::from_str(cookie).map_err(|err| {
            ConnectFailure::Other(anyhow!(err).context("invalid cookie header value"))
        })?,
    );

    let connection = tokio::time::timeout(
        Duration::from_secs(WEBSOCKET_CONNECT_TIMEOUT_SECS),
        connect_async_with_config(request, None, true),
    )
    .await
    .map_err(|_| {
        ConnectFailure::Other(anyhow!(
            "websocket handshake timed out after {WEBSOCKET_CONNECT_TIMEOUT_SECS}s: {url}"
        ))
    })?;

    let (websocket, _) = match connection {
        Ok(result) => result,
        Err(WebSocketError::Http(response)) => {
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("<none>");

            if location == "/wengine-vpn/failed" {
                return Err(ConnectFailure::WebVpnFailed {
                    location: location.to_string(),
                });
            }

            if location.contains("webvpn.szut.edu.cn/login") {
                return Err(ConnectFailure::CookieExpired {
                    location: location.to_string(),
                });
            }

            return Err(ConnectFailure::Other(anyhow!(
                "failed to connect websocket: {url}: HTTP error: {} {}; location: {location}",
                response.status().as_u16(),
                response.status().canonical_reason().unwrap_or("")
            )));
        }
        Err(err) => {
            return Err(ConnectFailure::Other(
                anyhow!(err).context(format!("failed to connect websocket: {url}")),
            ));
        }
    };

    Ok(websocket)
}

#[allow(clippy::result_large_err)]
/// Accepts a WebSocket and returns it together with the requested URL path.
pub async fn accept_websocket_with_path(
    stream: TcpStream,
) -> Result<(WebSocketStream<TcpStream>, String)> {
    stream
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY on websocket stream")?;
    let requested_path = Arc::new(Mutex::new(None::<String>));
    let requested_path_for_callback = Arc::clone(&requested_path);

    let websocket = accept_hdr_async(stream, move |request: &ServerRequest, response| {
        let mut guard = requested_path_for_callback
            .lock()
            .expect("request path mutex poisoned");
        *guard = Some(request.uri().path().to_string());
        Ok(response)
    })
    .await
    .context("websocket handshake failed")?;

    let path = requested_path
        .lock()
        .expect("request path mutex poisoned")
        .take()
        .ok_or_else(|| anyhow!("missing websocket request path"))?;

    Ok((websocket, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "client")]
    fn builds_webvpn_ws_url_from_server_and_target() {
        let url = build_webvpn_ws_url("192.0.2.10:4489", Some("3389")).unwrap();

        assert!(
            url.starts_with("wss://webvpn.szut.edu.cn/ws-4489/77726476706e69737468656265737421")
        );
        assert!(url.ends_with("/tcp/3389"));
    }

    #[test]
    #[cfg(feature = "client")]
    fn builds_webvpn_keepalive_ws_url_from_server() {
        let url = build_webvpn_keepalive_ws_url("192.0.2.10:4489").unwrap();

        assert!(
            url.starts_with("wss://webvpn.szut.edu.cn/ws-4489/77726476706e69737468656265737421")
        );
        assert!(url.ends_with(WEBVPN_KEEPALIVE_PATH));
    }

    #[test]
    #[cfg(feature = "client")]
    fn target_path_uses_documented_defaults() {
        assert_eq!(
            tcp_target_url_path_from_addr(DEFAULT_TARGET_ADDR).unwrap(),
            "/tcp"
        );
        assert_eq!(normalize_tcp_target_arg(None).unwrap(), DEFAULT_TARGET_ADDR);
        assert_eq!(
            tcp_target_url_path_from_addr("127.0.0.1:3389").unwrap(),
            "/tcp/3389"
        );
        assert_eq!(
            tcp_target_url_path_from_addr("10.0.0.2:9999").unwrap(),
            "/tcp/10.0.0.2:9999"
        );
        assert_eq!(
            tcp_target_url_path_from_addr("127.0.0.1:2222").unwrap(),
            "/tcp/2222"
        );
        assert_eq!(
            normalize_tcp_target_arg(Some(":2222")).unwrap(),
            "127.0.0.1:2222"
        );
    }

    #[test]
    fn tcp_target_path_requires_a_route_boundary() {
        assert!(parse_tcp_target_path("/tcpfoo:22").is_err());
        assert!(parse_tcp_target_path("/tcpx/22").is_err());
        assert_eq!(parse_tcp_target_path("/tcp").unwrap(), DEFAULT_TARGET_ADDR);
        assert_eq!(parse_tcp_target_path("/tcp/").unwrap(), DEFAULT_TARGET_ADDR);
        assert_eq!(
            parse_tcp_target_path("/tcp/2222").unwrap(),
            "127.0.0.1:2222"
        );
    }

    #[test]
    #[cfg(feature = "client")]
    fn rsa_encrypt_matches_webvpn_rsa_utils() {
        let encrypted = rsa_encrypt(
            "654321",
            "91c28b7f794d9aa0e73078c8f9ef68270154fbecdbc455c06afb4fe922fa433218e785e1e90402c0ab120c04296472ff310da4237339e1d15c506694add53d4b",
            "10001",
        )
        .unwrap();

        assert_eq!(
            encrypted,
            "1aa6cdb463265bdf0927564d3ca7160be772ebcbc71d96eb74c18bb0c2955f361c49be02c908f8387736a845214217e0a6b67c5a8b56caf2bfcec4645b49eecd"
        );
    }

    #[test]
    fn log_levels_color_the_prefix_by_semantics() {
        assert_eq!(
            format_log_message(LogLevel::Success, "client", "ready"),
            "\x1b[32m[client]\x1b[0m ready"
        );
        assert_eq!(
            format_log_message(LogLevel::Info, "client", "starting"),
            "\x1b[36m[client]\x1b[0m starting"
        );
        assert_eq!(
            format_log_message(LogLevel::Warning, "client", "retrying"),
            "\x1b[33m[client]\x1b[0m retrying"
        );
        assert_eq!(
            format_log_message(LogLevel::Error, "client", "failed"),
            "\x1b[31m[client]\x1b[0m failed"
        );
    }

    #[test]
    #[cfg(feature = "client")]
    fn connect_failure_display_does_not_expand_the_same_source_twice() {
        let failure = ConnectFailure::Other(anyhow!("rustls detail").context("IO error"));

        assert_eq!(failure.to_string(), "IO error");
        assert!(std::error::Error::source(&failure).is_some());
    }

    #[test]
    fn websocket_unexpected_eof_is_a_normal_connection_close() {
        let error = WebSocketError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "peer closed connection without sending TLS close_notify",
        ));

        assert!(is_normal_websocket_close(&error));
    }

    #[tokio::test]
    async fn heartbeat_matches_v04_immediate_then_210_second_schedule() {
        let mut interval = webvpn_heartbeat_interval(WEBVPN_HEARTBEAT_INTERVAL_SECS);

        assert_eq!(interval.period(), Duration::from_secs(210));
        tokio::time::timeout(Duration::from_millis(50), interval.tick())
            .await
            .expect("the first v0.4-compatible heartbeat must be immediate");
    }

    #[tokio::test]
    async fn data_heartbeat_is_immediate_then_every_60_seconds() {
        let mut interval = webvpn_heartbeat_interval(WEBVPN_DATA_HEARTBEAT_INTERVAL_SECS);

        assert_eq!(interval.period(), Duration::from_secs(60));
        tokio::time::timeout(Duration::from_millis(50), interval.tick())
            .await
            .expect("the first data heartbeat must be immediate");
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn relay_moves_binary_data_in_both_directions() {
        let tcp_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let tcp_addr = tcp_listener.local_addr().unwrap();
        let mut target_peer = TcpStream::connect(tcp_addr).await.unwrap();
        let (target_stream, _) = tcp_listener.accept().await.unwrap();

        let ws_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let relay_task = tokio::spawn(async move {
            let (transport, _) = ws_listener.accept().await.unwrap();
            let (websocket, path) = accept_websocket_with_path(transport).await.unwrap();
            assert_eq!(path, "/tcp");
            relay_stream(websocket, target_stream, WebVpnHeartbeatRole::Server)
                .await
                .unwrap();
        });

        let (mut websocket, _) = tokio_tungstenite::connect_async(format!("ws://{ws_addr}/tcp"))
            .await
            .unwrap();
        websocket
            .send(Message::Text(WEBVPN_HEARTBEAT_MESSAGE.into()))
            .await
            .unwrap();
        assert_eq!(
            websocket.next().await.unwrap().unwrap(),
            Message::Text(WEBVPN_HEARTBEAT_MESSAGE.into())
        );
        websocket
            .send(Message::Binary(b"client-to-target".to_vec().into()))
            .await
            .unwrap();
        let mut received = [0_u8; 16];
        target_peer.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"client-to-target");

        target_peer.write_all(b"target-to-client").await.unwrap();
        let message = websocket.next().await.unwrap().unwrap();
        assert_eq!(message.into_data(), b"target-to-client".as_slice());

        websocket.close(None).await.unwrap();
        relay_task.await.unwrap();
    }

    #[tokio::test]
    async fn client_relay_consumes_tows_ready_control_frame() {
        let tcp_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let tcp_addr = tcp_listener.local_addr().unwrap();
        let relay_tcp = TcpStream::connect(tcp_addr).await.unwrap();
        let (mut local_peer, _) = tcp_listener.accept().await.unwrap();

        let ws_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (transport, _) = ws_listener.accept().await.unwrap();
            let (mut websocket, _) = accept_websocket_with_path(transport).await.unwrap();
            websocket
                .send(Message::Text(TOWS_READY_MESSAGE.into()))
                .await
                .unwrap();
            websocket
                .send(Message::Binary(b"payload".to_vec().into()))
                .await
                .unwrap();
            websocket.close(None).await.unwrap();
        });

        let (websocket, _) = tokio_tungstenite::connect_async(format!("ws://{ws_addr}/tcp"))
            .await
            .unwrap();
        let relay = tokio::spawn(relay_stream(
            websocket,
            relay_tcp,
            WebVpnHeartbeatRole::Client,
        ));
        let mut received = [0_u8; 7];
        local_peer.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"payload");

        server.await.unwrap();
        relay.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn relay_preserves_tcp_half_close_for_the_response_direction() {
        let tcp_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let tcp_addr = tcp_listener.local_addr().unwrap();
        let mut target_peer = TcpStream::connect(tcp_addr).await.unwrap();
        let (target_stream, _) = tcp_listener.accept().await.unwrap();

        let ws_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let relay = tokio::spawn(async move {
            let (transport, _) = ws_listener.accept().await.unwrap();
            let (websocket, _) = accept_websocket_with_path(transport).await.unwrap();
            relay_stream(websocket, target_stream, WebVpnHeartbeatRole::Server).await
        });
        let (mut websocket, _) = tokio_tungstenite::connect_async(format!("ws://{ws_addr}/tcp"))
            .await
            .unwrap();

        target_peer.shutdown().await.unwrap();
        assert_eq!(
            websocket.next().await.unwrap().unwrap(),
            Message::Text(TOWS_TCP_EOF_MESSAGE.into())
        );
        websocket
            .send(Message::Binary(b"response".to_vec().into()))
            .await
            .unwrap();
        let mut response = [0_u8; 8];
        target_peer.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"response");

        websocket
            .send(Message::Text(TOWS_TCP_EOF_MESSAGE.into()))
            .await
            .unwrap();
        relay.await.unwrap().unwrap();
    }
}
