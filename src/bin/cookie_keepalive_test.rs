#[tokio::main]
async fn main() {
    if let Err(err) = tcp_over_websocket::client::run_cookie_keepalive_test().await {
        eprintln!("[cookie-test] {err:#}");
        std::process::exit(1);
    }
}
