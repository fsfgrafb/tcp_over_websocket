#[tokio::main]
async fn main() {
    if let Err(err) = tcp_over_websocket::tows::run_cli().await {
        tcp_over_websocket::log_error("server", format!("{err:#}"));
        std::process::exit(1);
    }
}
