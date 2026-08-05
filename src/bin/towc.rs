#[tokio::main]
async fn main() {
    if let Err(err) = tcp_over_websocket::client::run_cli().await {
        eprintln!("[client] {err:#}");
        std::process::exit(1);
    }
}
