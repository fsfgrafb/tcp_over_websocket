//! 外网客户端：认证、配置和本地监听。

mod auth;
mod config;
mod qr;
mod runtime;

pub use auth::{
    AuthPrompt, LoginPreference, SessionCookie, login_or_restore, login_or_restore_for_server,
    login_with_preference, restore_valid_cached_ticket_for_server,
};
pub use config::{
    ClientConfig, InteractiveDefaults, ParsedArgs, parse_args, prompt_interactive, prompt_login,
};
pub use runtime::{
    ClientObserver, ForwardRule, ServerGroup, run_cli, run_dynamic_server_groups,
    run_server_groups, run_tunnels,
};
