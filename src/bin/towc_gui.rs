#![cfg_attr(windows, windows_subsystem = "windows")]

#[cfg(windows)]
fn main() -> anyhow::Result<()> {
    tcp_over_websocket::gui::run()
}

#[cfg(not(windows))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("towc_gui is only supported on Windows")
}
