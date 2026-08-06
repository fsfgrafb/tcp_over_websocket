#[cfg(feature = "client")]
use aes::Aes128;
#[cfg(feature = "client")]
use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
#[cfg(feature = "client")]
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
#[cfg(feature = "client")]
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::Request as ServerRequest;
#[cfg(feature = "client")]
use tokio_tungstenite::tungstenite::http::HeaderValue;
#[cfg(feature = "client")]
use tokio_tungstenite::tungstenite::http::header::{COOKIE, LOCATION};
#[cfg(feature = "client")]
use tokio_tungstenite::{MaybeTlsStream, connect_async};
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

#[cfg(feature = "client")]
use crate::address::Endpoint;
use crate::protocol::{Frame, PROTOCOL_VERSION};

pub const WEBVPN_HOST: &str = "webvpn.szut.edu.cn";
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
pub const OLD_TOWS_MESSAGE: &str = "连接成功";

#[cfg(feature = "client")]
const WEBVPN_AES_KEY: &[u8; 16] = b"wrdvpnisthebest!";
#[cfg(feature = "client")]
const WEBVPN_ENCRYPTED_PREFIX: &str = "77726476706e69737468656265737421";

#[cfg(feature = "client")]
pub type ClientWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
pub enum ConnectFailure {
    CookieExpired { location: String },
    WebVpnFailed { location: String },
    Other(anyhow::Error),
}

impl fmt::Display for ConnectFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CookieExpired { location } => write!(
                formatter,
                "WebVPN login has expired; restart and sign in again (location: {location})"
            ),
            Self::WebVpnFailed { location } => write!(
                formatter,
                "WebVPN could not reach tows; check the address, port, and service (location: {location})"
            ),
            Self::Other(error) => write!(formatter, "{error:#}"),
        }
    }
}

impl std::error::Error for ConnectFailure {}

#[cfg(feature = "client")]
pub fn build_webvpn_ws_url(server: &Endpoint) -> Result<String> {
    let encrypted = encrypt_webvpn_host(server.host())?;
    Ok(format!(
        "wss://{WEBVPN_HOST}/ws-{}/{WEBVPN_ENCRYPTED_PREFIX}{encrypted}/",
        server.port()
    ))
}

#[cfg(feature = "client")]
fn encrypt_webvpn_host(host: &str) -> Result<String> {
    let cipher = Aes128::new_from_slice(WEBVPN_AES_KEY)
        .map_err(|_| anyhow!("failed to initialize AES-128"))?;
    let mut feedback = *WEBVPN_AES_KEY;
    let mut ciphertext = Vec::with_capacity(host.len());

    for chunk in host.as_bytes().chunks(16) {
        let mut block = GenericArray::clone_from_slice(&feedback);
        cipher.encrypt_block(&mut block);
        let offset = ciphertext.len();
        ciphertext.extend(
            chunk
                .iter()
                .zip(block.iter())
                .map(|(plain, key)| plain ^ key),
        );
        let encrypted = &ciphertext[offset..];
        feedback[..encrypted.len()].copy_from_slice(encrypted);
    }

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(ciphertext.len() * 2);
    for byte in ciphertext {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(output)
}

#[cfg(feature = "client")]
pub async fn connect_websocket(
    url: &str,
    cookie: &str,
) -> std::result::Result<ClientWebSocket, ConnectFailure> {
    install_crypto_provider();
    let mut request = url.into_client_request().map_err(|error| {
        ConnectFailure::Other(anyhow!(error).context("failed to build WebSocket request"))
    })?;
    request.headers_mut().insert(
        COOKIE,
        HeaderValue::from_str(cookie).map_err(|error| {
            ConnectFailure::Other(anyhow!(error).context("cached cookie is invalid"))
        })?,
    );

    match connect_async(request).await {
        Ok((websocket, _)) => Ok(websocket),
        Err(WebSocketError::Http(response)) => {
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("<none>")
                .to_string();
            if location == "/wengine-vpn/failed" {
                Err(ConnectFailure::WebVpnFailed { location })
            } else if location.contains("/login") {
                Err(ConnectFailure::CookieExpired { location })
            } else {
                Err(ConnectFailure::Other(anyhow!(
                    "WebSocket handshake returned HTTP {} (location: {location})",
                    response.status()
                )))
            }
        }
        Err(error) => Err(ConnectFailure::Other(
            anyhow!(error).context("WebSocket connection failed"),
        )),
    }
}

#[cfg(feature = "client")]
fn install_crypto_provider() {
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// 客户端 HELLO/HELLO_ACK 握手。旧版文本提示会得到明确诊断。
pub async fn client_handshake<S>(websocket: &mut WebSocketStream<S>, program: &str) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let hello = Frame::hello(program)?;
    websocket
        .send(Message::Binary(hello.encode().into()))
        .await
        .context("failed to send HELLO")?;

    let message = tokio::time::timeout(HANDSHAKE_TIMEOUT, websocket.next())
        .await
        .map_err(|_| anyhow!("timed out waiting for HELLO_ACK; upgrade tows to v0.5.1 or later"))?
        .context("tows closed the connection before HELLO_ACK")??;
    let Message::Binary(bytes) = message else {
        if matches!(&message, Message::Text(text) if text.as_str() == OLD_TOWS_MESSAGE) {
            return Err(anyhow!(
                "legacy text-based tows protocol detected; upgrade the server"
            ));
        }
        return Err(anyhow!("HELLO_ACK must be a WebSocket Binary message"));
    };
    let ack = Frame::decode(&bytes)?;
    ack.validate_server_to_client(false)?;
    let (version, server_program) = ack.version()?;
    if version != PROTOCOL_VERSION {
        return Err(anyhow!(
            "protocol version mismatch: client v{PROTOCOL_VERSION}, server v{version} ({server_program}); upgrade tows"
        ));
    }
    tracing::info!(target: "tunnel", "protocol handshake complete; peer={server_program}");
    Ok(())
}

/// 服务端等待首帧 HELLO，并且即使版本不同也回 HELLO_ACK。
pub async fn server_handshake<S>(websocket: &mut WebSocketStream<S>, program: &str) -> Result<u16>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = tokio::time::timeout(HANDSHAKE_TIMEOUT, websocket.next())
        .await
        .map_err(|_| anyhow!("timed out waiting for HELLO"))?
        .context("client closed the connection before HELLO")??;
    let Message::Binary(bytes) = message else {
        return Err(anyhow!("first connection frame is not a Binary HELLO"));
    };
    let hello = Frame::decode(&bytes)?;
    hello.validate_client_to_server(false)?;
    let (version, client_program) = hello.version()?;
    websocket
        .send(Message::Binary(Frame::hello_ack(program)?.encode().into()))
        .await
        .context("failed to send HELLO_ACK")?;
    tracing::info!(target: "tunnel", "received protocol v{version} HELLO from {client_program}");
    Ok(version)
}

/// 接受 WebSocket；记录路径仅用于诊断，新协议不从路径读取目标。
#[allow(clippy::result_large_err)]
pub async fn accept_websocket(stream: TcpStream) -> Result<(WebSocketStream<TcpStream>, String)> {
    stream
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY")?;
    let path = Arc::new(Mutex::new(None::<String>));
    let captured = Arc::clone(&path);
    let websocket = accept_hdr_async(stream, move |request: &ServerRequest, response| {
        *captured.lock().expect("path mutex poisoned") = Some(request.uri().path().to_string());
        Ok(response)
    })
    .await
    .context("WebSocket handshake failed")?;
    let requested = path
        .lock()
        .expect("path mutex poisoned")
        .take()
        .unwrap_or_else(|| "/".to_string());
    Ok((websocket, requested))
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "client")]
    use super::*;
    #[cfg(feature = "client")]
    use crate::address::parse_tows;

    #[cfg(feature = "client")]
    #[test]
    fn webvpn_url_uses_fixed_root_path_and_known_aes_encoding() {
        let url = build_webvpn_ws_url(&parse_tows("10.18.47.77:4489").unwrap()).unwrap();
        assert_eq!(
            url,
            "wss://webvpn.szut.edu.cn/ws-4489/77726476706e69737468656265737421a1a70fcd7f7e3c07305fde/"
        );
    }
}
