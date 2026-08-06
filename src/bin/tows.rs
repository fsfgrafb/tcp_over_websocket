#[tokio::main]
async fn main() {
    if let Err(error) = tcp_over_websocket::server::run_cli().await {
        tracing::error!(target: "tows", "{error:#}");
        std::process::exit(1);
    }
}
