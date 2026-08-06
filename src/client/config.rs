use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Write};

use crate::address::{
    DEFAULT_LISTEN, DEFAULT_TARGET, Endpoint, parse_listen, parse_target, parse_tows,
};
use crate::network::build_webvpn_ws_url;
use crate::storage::{data_file, write_json};

use super::auth::LoginPreference;

const DEFAULTS_FILE: &str = "interactive.defaults";

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub server: Endpoint,
    pub target: Endpoint,
    pub listen: Endpoint,
    pub login: LoginPreference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractiveDefaults {
    pub version: u32,
    pub server: String,
    pub target: String,
    pub listen_addr: String,
}

#[derive(Debug, Clone)]
pub enum ParsedArgs {
    Help,
    Interactive,
    Run(ClientConfig),
}

pub fn parse_args(args: &[String]) -> Result<ParsedArgs> {
    if args.is_empty() {
        return Ok(ParsedArgs::Interactive);
    }
    if is_help(&args[0]) {
        return Ok(ParsedArgs::Help);
    }
    if args[0].starts_with('-') {
        bail!("the only positional argument <tows-host[:port]> must come first");
    }

    let server = parse_tows(&args[0]).context("invalid tows address")?;
    let mut target = parse_target(DEFAULT_TARGET).expect("built-in target is valid");
    let mut listen = parse_listen(DEFAULT_LISTEN).expect("built-in listen address is valid");
    let mut login = LoginPreference::Wechat;
    let mut seen_target = false;
    let mut seen_listen = false;
    let mut seen_login = false;
    let mut index = 1;

    while index < args.len() {
        let flag = &args[index];
        if is_help(flag) {
            return Ok(ParsedArgs::Help);
        }
        index += 1;
        let value = args
            .get(index)
            .with_context(|| format!("{flag} requires a value"))?;
        if value.starts_with('-') {
            bail!("{flag} requires a value");
        }
        match flag.as_str() {
            "--target" if !seen_target => {
                target = parse_target(value).context("invalid --target")?;
                seen_target = true;
            }
            "--listen" if !seen_listen => {
                listen = parse_listen(value).context("invalid --listen")?;
                seen_listen = true;
            }
            "--login" if !seen_login => {
                login = LoginPreference::from_identity(value)?;
                seen_login = true;
            }
            "--target" | "--listen" | "--login" => bail!("{flag} may only appear once"),
            _ if flag.starts_with('-') => bail!("unknown option: {flag}"),
            _ => bail!("unexpected positional argument: {flag}"),
        }
        index += 1;
    }

    Ok(ParsedArgs::Run(ClientConfig {
        server,
        target,
        listen,
        login,
    }))
}

pub fn prompt_interactive() -> Result<ClientConfig> {
    let cached = read_defaults();
    let server = loop {
        let prompt = cached.as_ref().map_or_else(
            || "tows address <host[:port]>: ".to_string(),
            |defaults| {
                format!(
                    "tows address <host[:port]> (default: {}): ",
                    defaults.server
                )
            },
        );
        let value = prompt_line(&prompt)?;
        let value = if value.is_empty() {
            cached
                .as_ref()
                .map(|defaults| defaults.server.clone())
                .unwrap_or_default()
        } else {
            value
        };
        match parse_tows(&value) {
            Ok(server) => break server,
            Err(error) => tracing::warn!(target: "towc", "invalid input: {error}"),
        }
    };

    let location = reqwest::Url::parse(&build_webvpn_ws_url(&server)?)?
        .path()
        .to_string();
    tracing::info!(target: "towc", "WebVPN location: {location}");

    let target_default = cached
        .as_ref()
        .map(|defaults| defaults.target.as_str())
        .unwrap_or("22");
    let target = prompt_endpoint(
        &format!("target address/port (default: {target_default}): "),
        target_default,
        parse_target,
    )?;

    let listen_default = cached
        .as_ref()
        .map(|defaults| defaults.listen_addr.as_str())
        .unwrap_or("14489");
    let listen = prompt_endpoint(
        &format!("listen address/port (default: {listen_default}): "),
        listen_default,
        parse_listen,
    )?;

    let login = loop {
        let value = prompt_line("login mobile/email (default: WeChat QR): ")?;
        if value.is_empty() {
            break LoginPreference::Wechat;
        }
        match LoginPreference::from_identity(&value) {
            Ok(login) => break login,
            Err(error) => tracing::warn!(target: "towc", "invalid input: {error}"),
        }
    };

    write_defaults(&InteractiveDefaults {
        version: 1,
        server: server.to_string(),
        target: target.to_string(),
        listen_addr: listen.to_string(),
    });

    Ok(ClientConfig {
        server,
        target,
        listen,
        login,
    })
}

fn prompt_endpoint(
    prompt: &str,
    default: &str,
    parser: fn(&str) -> Result<Endpoint>,
) -> Result<Endpoint> {
    loop {
        let value = prompt_line(prompt)?;
        let value = if value.is_empty() { default } else { &value };
        match parser(value) {
            Ok(endpoint) => return Ok(endpoint),
            Err(error) => tracing::warn!(target: "towc", "invalid input: {error}"),
        }
    }
}

fn prompt_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout()
        .flush()
        .context("failed to flush input prompt")?;
    let mut value = String::new();
    let count = io::stdin()
        .read_line(&mut value)
        .context("failed to read input")?;
    if count == 0 {
        bail!("input stream closed");
    }
    Ok(value.trim().to_string())
}

fn read_defaults() -> Option<InteractiveDefaults> {
    let path = data_file(DEFAULTS_FILE)?;
    let contents = match fs::read(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
        Err(error) => {
            tracing::warn!(target: "towc", "could not read interactive defaults; continuing: {error}");
            return None;
        }
    };
    let defaults: InteractiveDefaults = match serde_json::from_slice(&contents) {
        Ok(defaults) => defaults,
        Err(error) => {
            tracing::warn!(target: "towc", "interactive defaults are corrupt; using built-in defaults: {error}");
            return None;
        }
    };
    if defaults.version != 1
        || parse_tows(&defaults.server).is_err()
        || parse_target(&defaults.target).is_err()
        || parse_listen(&defaults.listen_addr).is_err()
    {
        tracing::warn!(target: "towc", "interactive defaults have an invalid version or address; using built-in defaults");
        return None;
    }
    Some(defaults)
}

fn write_defaults(defaults: &InteractiveDefaults) {
    let Some(path) = data_file(DEFAULTS_FILE) else {
        tracing::warn!(target: "towc", "interactive defaults directory is unavailable; continuing");
        return;
    };
    if let Err(error) = write_json(&path, defaults) {
        tracing::warn!(target: "towc", "could not save interactive defaults; continuing: {error:#}");
    }
}

fn is_help(value: &str) -> bool {
    value == "--help" || value == "-h"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn flags_are_order_independent() {
        let ParsedArgs::Run(config) = parse_args(&args(&[
            "host.example",
            "--login",
            "user@example.com",
            "--listen",
            "13389",
            "--target",
            "3389",
        ]))
        .unwrap() else {
            panic!("expected run mode")
        };
        assert_eq!(config.server.to_string(), "host.example:4489");
        assert_eq!(config.target.to_string(), "127.0.0.1:3389");
        assert_eq!(config.listen.to_string(), "127.0.0.1:13389");
    }

    #[test]
    fn first_position_is_reserved_for_tows() {
        assert!(parse_args(&args(&["--target", "22"])).is_err());
    }
}
