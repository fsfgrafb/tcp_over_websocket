#[cfg(windows)]
fn main() {
    tcp_over_websocket::init_tracing("towc");
    if let Err(error) = tcp_over_websocket::gui::run() {
        tracing::error!(target: "towc", "{error:#}");
        std::process::exit(1);
    }
}

#[cfg(not(windows))]
fn main() {
    tcp_over_websocket::init_tracing("towc");
    tracing::error!(target: "towc", "towc_gui is only supported on Windows");
}
