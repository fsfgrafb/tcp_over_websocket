#[tokio::main]
async fn main() {
    if let Err(err) = tcp_over_websocket::towc::run_cli().await {
        tcp_over_websocket::log_error("client", format!("{err:#}"));
        std::process::exit(1);
    }
}
