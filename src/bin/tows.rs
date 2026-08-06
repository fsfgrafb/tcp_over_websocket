#[tokio::main]
async fn main() {
    if let Err(error) = tcp_over_websocket::server::run_cli().await {
        eprintln!("[tows] {error:#}");
        std::process::exit(1);
    }
}
