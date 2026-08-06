use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use tokio::net::lookup_host;

pub const DEFAULT_TOWS_PORT: u16 = 4489;
pub const DEFAULT_TARGET: &str = "127.0.0.1:22";
pub const DEFAULT_LISTEN: &str = "127.0.0.1:14489";

/// 已消除端口歧义的主机与端口。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Endpoint {
    host: String,
    port: u16,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self> {
        let host = host.into();
        validate_host(&host)?;
        validate_port(port)?;
        Ok(Self { host, port })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub const fn port(&self) -> u16 {
        self.port
    }

    pub fn is_loopback(&self) -> bool {
        self.host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
            || self.host.eq_ignore_ascii_case("localhost")
    }

    /// 将监听地址解析为操作系统可绑定的地址。
    pub async fn resolve(&self) -> Result<SocketAddr> {
        let mut addresses = lookup_host((self.host.as_str(), self.port))
            .await
            .with_context(|| format!("无法解析地址 {self}"))?;
        addresses
            .next()
            .with_context(|| format!("地址 {self} 没有可用的 IP"))
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.parse::<Ipv6Addr>().is_ok() {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

/// 解析 tows 地址。允许省略端口，但不允许只写端口。
pub fn parse_tows(value: &str) -> Result<Endpoint> {
    let value = value.trim();
    if value.chars().all(|character| character.is_ascii_digit()) {
        bail!("tows 地址不能只写端口；请写 host 或 host:port");
    }
    parse_host_port(value, Some(DEFAULT_TOWS_PORT), "tows")
}

/// 解析目标地址。纯端口等价于 127.0.0.1:port。
pub fn parse_target(value: &str) -> Result<Endpoint> {
    parse_port_shorthand(value, "目标")
}

/// 解析本地监听地址。纯端口等价于 127.0.0.1:port。
pub fn parse_listen(value: &str) -> Result<Endpoint> {
    parse_port_shorthand(value, "监听")
}

fn parse_port_shorthand(value: &str, label: &str) -> Result<Endpoint> {
    let value = value.trim();
    if let Ok(port) = parse_port(value) {
        return Endpoint::new("127.0.0.1", port);
    }
    parse_host_port(value, None, label)
}

fn parse_host_port(value: &str, default_port: Option<u16>, label: &str) -> Result<Endpoint> {
    if value.is_empty() {
        bail!("{label}地址不能为空");
    }

    if let Some(after_open) = value.strip_prefix('[') {
        let close = after_open
            .find(']')
            .ok_or_else(|| anyhow!("{label} IPv6 地址缺少右方括号"))?;
        let host = &after_open[..close];
        host.parse::<Ipv6Addr>()
            .with_context(|| format!("无效的 {label} IPv6 地址: {host}"))?;
        let suffix = &after_open[close + 1..];
        let port = if suffix.is_empty() {
            default_port.context(format!("{label}地址必须包含端口"))?
        } else {
            parse_port(
                suffix
                    .strip_prefix(':')
                    .ok_or_else(|| anyhow!("{label} IPv6 地址右方括号后只能跟 :port"))?,
            )?
        };
        return Endpoint::new(host, port);
    }

    let colon_count = value.bytes().filter(|byte| *byte == b':').count();
    match colon_count {
        0 => {
            let port = default_port.context(format!("{label}地址必须包含端口"))?;
            Endpoint::new(value, port)
        }
        1 => {
            let (host, port) = value.rsplit_once(':').expect("已确认地址中恰好有一个冒号");
            if host.trim().is_empty() {
                bail!("{label}地址缺少主机名");
            }
            Endpoint::new(host.trim(), parse_port(port.trim())?)
        }
        _ => bail!("裸 IPv6 地址存在歧义，请使用 [addr] 或 [addr]:port"),
    }
}

fn parse_port(value: &str) -> Result<u16> {
    if value.is_empty() {
        bail!("端口不能为空");
    }
    let port = value
        .parse::<u16>()
        .with_context(|| format!("无效端口: {value}"))?;
    validate_port(port)?;
    Ok(port)
}

fn validate_port(port: u16) -> Result<()> {
    if port == 0 {
        bail!("端口必须在 1..=65535 范围内");
    }
    Ok(())
}

fn validate_host(host: &str) -> Result<()> {
    let host = host.trim();
    if host.is_empty() {
        bail!("主机名不能为空");
    }
    if host.contains(['/', '?', '#', '[', ']']) || host.chars().any(char::is_whitespace) {
        bail!("无效主机名: {host}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tows_supports_dns_ipv4_and_bracketed_ipv6() {
        assert_eq!(
            parse_tows("server.example").unwrap().to_string(),
            "server.example:4489"
        );
        assert_eq!(
            parse_tows("192.0.2.1:9000").unwrap().to_string(),
            "192.0.2.1:9000"
        );
        assert_eq!(
            parse_tows("[2001:db8::1]").unwrap().to_string(),
            "[2001:db8::1]:4489"
        );
    }

    #[test]
    fn shorthand_and_invalid_ports_follow_the_specification() {
        assert_eq!(parse_target("22").unwrap().to_string(), "127.0.0.1:22");
        assert_eq!(
            parse_listen("14489").unwrap().to_string(),
            "127.0.0.1:14489"
        );
        assert!(parse_tows("4489").is_err());
        assert!(parse_target("0").is_err());
        assert!(parse_target("2001:db8::1").is_err());
    }
}
