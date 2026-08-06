#[cfg(windows)]
fn main() {
    if let Err(error) = tcp_over_websocket::gui::run() {
        eprintln!("[towc_gui] {error:#}");
        std::process::exit(1);
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("towc_gui 仅支持 Windows");
}
