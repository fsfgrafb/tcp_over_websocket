#[tokio::main]
async fn main() {
    if let Err(error) = tcp_over_websocket::client::run_cli().await {
        eprintln!("[towc] {error:#}");
        std::process::exit(1);
    }
}
