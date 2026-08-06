#[tokio::main]
async fn main() {
    if let Err(error) = tcp_over_websocket::client::run_cli().await {
        tracing::error!(target: "towc", "{error:#}");
        std::process::exit(1);
    }
}
