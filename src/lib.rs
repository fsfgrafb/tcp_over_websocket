use aes::Aes128;
use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use num_bigint::BigUint;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::Request as ServerRequest;
use tokio_tungstenite::tungstenite::http::header::{COOKIE, HeaderValue};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, http::header::LOCATION};
use tokio_tungstenite::{WebSocketStream, accept_hdr_async, connect_async};

pub const SERVER_LISTEN_ADDR: &str = "0.0.0.0:4489";
pub const SERVER_LISTEN_HOST: &str = "0.0.0.0";
pub const DEFAULT_SERVER_PORT: u16 = 4489;
pub const DEFAULT_LOCAL_LISTEN_PORT: u16 = 14489;
pub const DEFAULT_LOCAL_LISTEN_ADDR: &str = "127.0.0.1:14489";
pub const DEFAULT_TARGET_HOST: &str = "127.0.0.1";
pub const DEFAULT_TARGET_PORT: u16 = 22;
pub const DEFAULT_TARGET_ADDR: &str = "127.0.0.1:22";
pub const DEFAULT_WEBVPN_WS_HOST: &str = "webvpn.szut.edu.cn";
pub const TOWS_TARGET_CONNECT_FAILURE_PREFIX: &str = "tows target connect failed";
pub const WEBVPN_KEEPALIVE_PATH: &str = "/webvpn-keepalive";
pub const WEBVPN_HEARTBEAT_MESSAGE: &str = "连接成功";
pub const WEBVPN_HEARTBEAT_INTERVAL_SECS: u64 = 210;

const WEBVPN_AES_KEY: &[u8; 16] = b"wrdvpnisthebest!";
const WEBVPN_ENCRYPTED_PREFIX: &str = "77726476706e69737468656265737421";
const RSA_CHUNK_SIZE: usize = 62;

const RESET: &str = "\x1b[0m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";

#[derive(Debug)]
pub enum ConnectFailure {
    CookieExpired { location: String },
    WebVpnFailed { location: String },
    Other(anyhow::Error),
}

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
            Self::Other(err) => write!(formatter, "{err:#}"),
        }
    }
}

impl std::error::Error for ConnectFailure {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebVpnHeartbeatRole {
    Client,
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

pub fn parse_tcp_target_path(path: &str) -> Result<String> {
    let target = path
        .strip_prefix("/tcp")
        .ok_or_else(|| anyhow!("unsupported path: {path}"))?
        .trim_start_matches('/');

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

pub fn build_webvpn_keepalive_ws_url(server: &str) -> Result<String> {
    let server = parse_host_port(server, DEFAULT_SERVER_PORT, DEFAULT_TARGET_HOST, "server")?;
    let encrypted_host = encrypt_webvpn_host(&server.host)?;

    Ok(format!(
        "wss://{DEFAULT_WEBVPN_WS_HOST}/ws-{}/{WEBVPN_ENCRYPTED_PREFIX}{encrypted_host}{WEBVPN_KEEPALIVE_PATH}",
        server.port
    ))
}

pub fn normalize_server_addr(value: &str) -> Result<String> {
    let server = parse_host_port(value, DEFAULT_SERVER_PORT, DEFAULT_TARGET_HOST, "server")?;
    Ok(format!("{}:{}", server.host, server.port))
}

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

fn encrypt_webvpn_host(host: &str) -> Result<String> {
    let ciphertext = aes_128_cfb_encrypt(host.as_bytes(), WEBVPN_AES_KEY, WEBVPN_AES_KEY)?;
    Ok(hex_encode(&ciphertext))
}

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

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

struct HostPort {
    host: String,
    port: u16,
}

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

pub fn parse_socket_addr(value: &str) -> Result<SocketAddr> {
    value
        .parse::<SocketAddr>()
        .with_context(|| format!("invalid socket address: {value}"))
}

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

pub fn log_info(scope: &str, message: impl AsRef<str>) {
    eprintln!("{CYAN}[{scope}]{RESET} {}", message.as_ref());
}

pub fn log_success(scope: &str, message: impl AsRef<str>) {
    eprintln!("{GREEN}[{scope}]{RESET} {}", message.as_ref());
}

pub fn log_warn(scope: &str, message: impl AsRef<str>) {
    eprintln!("{YELLOW}[{scope}]{RESET} {}", message.as_ref());
}

pub fn log_error(scope: &str, message: impl AsRef<str>) {
    eprintln!("{RED}[{scope}]{RESET} {}", message.as_ref());
}

pub async fn relay_stream<S>(websocket: WebSocketStream<S>, tcp: TcpStream) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    relay_stream_inner(websocket, tcp, None).await
}

pub async fn relay_stream_with_webvpn_heartbeat<S>(
    websocket: WebSocketStream<S>,
    tcp: TcpStream,
    heartbeat_role: WebVpnHeartbeatRole,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    relay_stream_inner(websocket, tcp, Some(heartbeat_role)).await
}

async fn relay_stream_inner<S>(
    websocket: WebSocketStream<S>,
    tcp: TcpStream,
    heartbeat_role: Option<WebVpnHeartbeatRole>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut ws_sink, mut ws_stream) = websocket.split();
    let (mut tcp_read, mut tcp_write) = tcp.into_split();
    let mut buffer = vec![0_u8; 16 * 1024];
    let mut heartbeat_interval =
        tokio::time::interval(Duration::from_secs(WEBVPN_HEARTBEAT_INTERVAL_SECS));
    heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = heartbeat_interval.tick(), if heartbeat_role.is_some_and(WebVpnHeartbeatRole::sends_heartbeat) => {
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
            read_result = tcp_read.read(&mut buffer) => {
                let read_size = match read_result {
                    Ok(read_size) => read_size,
                    Err(err) if is_normal_connection_close(&err) => break,
                    Err(err) => return Err(err).context("failed to read from tcp stream"),
                };
                if read_size == 0 {
                    let _ = ws_sink.send(Message::Close(None)).await;
                    break;
                }

                if let Err(err) = ws_sink
                    .send(Message::Binary(buffer[..read_size].to_vec().into()))
                    .await
                {
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
                        if text.as_str() == WEBVPN_HEARTBEAT_MESSAGE {
                            if heartbeat_role.is_some_and(WebVpnHeartbeatRole::echoes_heartbeat)
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
                    Some(Err(err)) => return Err(err.into()),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

pub async fn run_webvpn_heartbeat_websocket<S>(
    websocket: WebSocketStream<S>,
    heartbeat_role: WebVpnHeartbeatRole,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut ws_sink, mut ws_stream) = websocket.split();
    let mut heartbeat_interval =
        tokio::time::interval(Duration::from_secs(WEBVPN_HEARTBEAT_INTERVAL_SECS));
    heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

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
    )
}

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

    let (websocket, _) = match connect_async(request).await {
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
pub async fn accept_websocket_with_path(
    stream: TcpStream,
) -> Result<(WebSocketStream<TcpStream>, String)> {
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
    fn builds_webvpn_ws_url_from_server_and_target() {
        let url = build_webvpn_ws_url("192.0.2.10:4489", Some("3389")).unwrap();

        assert!(
            url.starts_with("wss://webvpn.szut.edu.cn/ws-4489/77726476706e69737468656265737421")
        );
        assert!(url.ends_with("/tcp/3389"));
    }

    #[test]
    fn builds_webvpn_keepalive_ws_url_from_server() {
        let url = build_webvpn_keepalive_ws_url("192.0.2.10:4489").unwrap();

        assert!(
            url.starts_with("wss://webvpn.szut.edu.cn/ws-4489/77726476706e69737468656265737421")
        );
        assert!(url.ends_with(WEBVPN_KEEPALIVE_PATH));
    }

    #[test]
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
}
