use anyhow::{Context, Result};
use futures_util::SinkExt;
use std::io;
use tcp_over_websocket::{
    DEFAULT_SERVER_PORT, SERVER_LISTEN_ADDR, SERVER_LISTEN_HOST,
    TOWS_TARGET_CONNECT_FAILURE_PREFIX, accept_websocket_with_path, log_error, log_info,
    log_success, parse_socket_addr_with_default_host, parse_tcp_target_path, relay_stream,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const HTTP_PROBE_RESPONSE: &[u8] =
    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const MAX_WEBSOCKET_CLOSE_REASON_BYTES: usize = 123;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        log_error("server", format!("{err:#}"));
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
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

    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind server on {listen_addr}"))?;
    log_success("server", format!("listening on {listen_addr}"));

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted.context("failed to accept incoming connection")?;

                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream).await {
                        log_error("server", format!("{peer_addr}: {err:#}"));
                    }
                });
            }
            _ = &mut shutdown => {
                log_info("server", "shutting down");
                return Ok(());
            }
        }
    }
}

fn print_usage() {
    eprintln!("Usage: tows [port]");
    eprintln!("       default port: {DEFAULT_SERVER_PORT}");
}

async fn handle_connection(stream: TcpStream) -> Result<()> {
    if !is_websocket_upgrade_request(&stream).await? {
        return respond_http_probe(stream).await;
    }

    let (mut websocket, path) = accept_websocket_with_path(stream).await?;
    let target_addr = parse_tcp_target_path(&path)?;
    let target = match TcpStream::connect(&target_addr).await {
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
    log_info("server", format!("{path} -> {target_addr}"));

    relay_stream(websocket, target).await
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
    let mut buffer = [0_u8; 1024];
    let read_size = stream
        .peek(&mut buffer)
        .await
        .context("failed to inspect incoming request")?;

    let request = String::from_utf8_lossy(&buffer[..read_size]);
    Ok(has_websocket_upgrade_headers(&request))
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
}
