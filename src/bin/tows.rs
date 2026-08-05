#[tokio::main]
async fn main() {
    if let Err(err) = tcp_over_websocket::server::run_cli().await {
        eprintln!("[server] {err:#}");
        std::process::exit(1);
    }
}
