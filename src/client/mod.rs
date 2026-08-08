//! 外网客户端：认证、配置和本地监听。

mod auth;
mod config;
mod qr;
mod runtime;

pub use auth::{
    AuthPrompt, LoginPreference, SessionCookie, clear_cached_ticket, login_or_restore,
    login_with_preference, restore_valid_cached_ticket,
};
pub use config::{
    ClientConfig, InteractiveDefaults, ParsedArgs, parse_args, prompt_interactive, prompt_login,
};
#[cfg(all(feature = "gui", windows))]
pub(crate) use runtime::authentication_expired;
pub use runtime::{
    ClientObserver, ForwardRule, ServerGroup, run_cli, run_dynamic_server_groups,
    run_server_groups, run_tunnels,
};
