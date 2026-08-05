use crate::{
    SERVER_LISTEN_ADDR, SERVER_LISTEN_HOST, TOWS_READY_MESSAGE, WebVpnHeartbeatRole,
    accept_websocket_with_path, log_error, log_info, log_success,
    parse_socket_addr_with_default_host, parse_tcp_target_path, relay_stream,
};
use anyhow::{Context, Result};
use futures_util::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

/// Starts the remote WebSocket-to-TCP forwarding server.
pub async fn run_cli() -> Result<()> {
    let address = parse_listen_address()?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to listen on {address}"))?;
    log_success("server", format!("listening on {}", listener.local_addr()?));

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to wait for Ctrl+C")?;
                log_info("server", "shutting down");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("failed to accept connection")?;
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream).await {
                        log_error("server", format!("connection from {peer} failed: {err:#}"));
                    }
                });
            }
        }
    }
}

fn parse_listen_address() -> Result<std::net::SocketAddr> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => parse_socket_addr_with_default_host(SERVER_LISTEN_ADDR, SERVER_LISTEN_HOST),
        [value] if value != "--help" && value != "-h" => {
            parse_socket_addr_with_default_host(value, SERVER_LISTEN_HOST)
        }
        _ => {
            println!("Usage: tows [port|host:port]");
            std::process::exit(0);
        }
    }
}

async fn handle_connection(stream: TcpStream) -> Result<()> {
    let (mut websocket, path) = accept_websocket_with_path(stream).await?;
    let target = parse_tcp_target_path(&path)?;
    let tcp = TcpStream::connect(&target)
        .await
        .with_context(|| format!("failed to connect to target {target}"))?;
    websocket
        .send(Message::Text(TOWS_READY_MESSAGE.into()))
        .await
        .context("failed to confirm target readiness")?;
    relay_stream(websocket, tcp, WebVpnHeartbeatRole::Server).await
}
