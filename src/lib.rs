//! tcp_over_websocket 的共享实现。
//!
//! 项目刻意把地址、协议、网络调度、登录和界面拆开，便于学习与测试。

pub mod address;
pub mod protocol;
pub mod storage;

mod multiplex;
pub mod network;

#[cfg(feature = "client")]
pub mod client;
#[cfg(all(feature = "gui", windows))]
pub mod gui;
#[cfg(feature = "server")]
pub mod server;

pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

/// 初始化命令行日志。重复调用不会报错。
pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .try_init();
}
