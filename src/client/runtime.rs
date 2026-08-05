use crate::{
    ConnectFailure, DEFAULT_LOCAL_LISTEN_ADDR, DEFAULT_SERVER_PORT, DEFAULT_TARGET_ADDR,
    DEFAULT_TARGET_HOST, WebVpnHeartbeatRole, build_webvpn_ws_url, connect_websocket, log_error,
    log_info, log_success, log_warn, normalize_server_addr, normalize_tcp_target_arg,
    parse_socket_addr_with_default_host, relay_stream,
};
use anyhow::{Context, Result, anyhow};
use reqwest::cookie::CookieStore;
use reqwest::header::{REFERER, USER_AGENT};
use reqwest::redirect::Policy;
use reqwest::{Client, Url};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

use super::qr;

const WEBVPN_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/login";
const WEBVPN_SESSION_PROBE_URL: &str = "https://webvpn.szut.edu.cn/";
const WEBVPN_TICKET_COOKIE_PREFIX: &str = "wengine_vpn_ticketwebvpn_szut_edu_cn=";
const WEBVPN_WECHAT_HASH: &str =
    "77726476706e69737468656265737421ffe7449269276d59660187e289446d36a8d6";
const WECHAT_APP_ID: &str = "wx16c67d169e7a9290";
const WECHAT_REDIRECT_URI: &str = "https://cas.szut.edu.cn/cas/login?service=https%3A%2F%2Fwebvpn.szut.edu.cn%2Flogin%3Fcas_login%3Dtrue&client_name=WeiXinClient";
const BROWSER_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0";
const WEBVPN_FINGERPRINT: &str = "5a0b00fe6ae8277a4bfadd4e103f6e1c";
const WECHAT_POLL_ATTEMPTS: usize = 180;
const WECHAT_POLL_TIMEOUT_SECS: u64 = 35;
const WECHAT_POLL_DELAY_MS: u64 = 1500;
const COOKIE_CACHE_FILE_NAME: &str = "webvpn.cookie";
const INTERACTIVE_DEFAULTS_CACHE_FILE_NAME: &str = "interactive.defaults";
const COOKIE_REFRESH_INTERVAL_SECS: u64 = 180;
const PORTAL_PROBE_INITIAL_INTERVAL_SECS: u64 = 60;
const PORTAL_PROBE_MAX_INTERVAL_SECS: u64 = 24 * 60 * 60;
const HTTP_REDIRECT_LIMIT: usize = 10;

struct ClientConfig {
    server: String,
    target: String,
    listen_addr: String,
}

struct InteractiveDefaults {
    server: String,
    target: String,
    listen_addr: String,
}

#[derive(Clone, Default)]
struct RedirectTracer {
    hops: Arc<Mutex<Vec<RedirectHop>>>,
}

struct RedirectHop {
    status: reqwest::StatusCode,
    source_url: String,
    destination_url: String,
}

impl RedirectTracer {
    fn start_request(&self) {
        if let Ok(mut hops) = self.hops.lock() {
            hops.clear();
        }
    }

    fn finish_request(&self) -> Vec<RedirectHop> {
        self.hops
            .lock()
            .map(|mut hops| std::mem::take(&mut *hops))
            .unwrap_or_default()
    }
}

struct CookieRefreshStatus {
    request_url: String,
    final_url: String,
    http_status: reqwest::StatusCode,
    elapsed: Duration,
    redirects: Vec<RedirectHop>,
    redirected_to_login: bool,
    ticket_cookie_present: bool,
    cookie_changed: bool,
    cookie_summary: String,
}

/// Starts the interactive local forwarding client.
pub async fn run_cli() -> Result<()> {
    install_crypto_provider();
    let config = parse_config()?;
    let cookie = Arc::new(login_or_restore().await?);
    let url = build_webvpn_ws_url(&config.server, Some(&config.target))?;
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .with_context(|| format!("failed to listen on {}", config.listen_addr))?;

    log_success(
        "client",
        format!(
            "listening: local {} -> WebVPN (authenticated) -> tows {} -> target {}; target is checked on first connection",
            listener.local_addr()?,
            config.server,
            config.target
        ),
    );

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to wait for Ctrl+C")?;
                log_info("client", "shutting down");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("failed to accept local connection")?;
                let url = url.clone();
                let cookie = Arc::clone(&cookie);
                tokio::spawn(async move {
                    if let Err(err) = forward_connection(stream, &url, cookie.as_str()).await {
                        log_error("client", format!("connection from {peer} failed: {err:#}"));
                    }
                });
            }
        }
    }
}

/// Runs an HTTP-only experiment for measuring WebVPN Cookie lifetime.
///
/// The experiment never opens a WebSocket and never contacts a tows server.
/// It refreshes the Cookie endpoint every three minutes and probes a WebVPN
/// session-protected entry at exponentially increasing intervals.
pub async fn run_cookie_keepalive_test() -> Result<()> {
    install_crypto_provider();
    let cookie = login_or_restore().await?;
    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    seed_webvpn_cookie_jar(&cookie_jar, &cookie);
    let (client, redirect_tracer) = build_cookie_test_client(Arc::clone(&cookie_jar))?;
    let started_at = Instant::now();
    let refresh_interval = Duration::from_secs(COOKIE_REFRESH_INTERVAL_SECS);
    let mut portal_probe_interval = Duration::from_secs(PORTAL_PROBE_INITIAL_INTERVAL_SECS);
    let mut refresh_count = 0_u64;
    let mut portal_probe_count = 1_u64;

    log_info(
        "cookie-test",
        "started: HTTP Cookie refresh only; no WebSocket and no tows connection",
    );
    log_info(
        "cookie-test",
        format!(
            "schedule: refresh every {}; WebVPN session probes: baseline now, then {} -> {} -> ... (cap {})",
            format_duration(refresh_interval),
            format_duration(portal_probe_interval),
            format_duration(next_portal_probe_interval(portal_probe_interval)),
            format_duration(Duration::from_secs(PORTAL_PROBE_MAX_INTERVAL_SECS)),
        ),
    );
    log_info(
        "cookie-test",
        format!("initial Cookie jar: {}", cookie_summary(Some(&cookie))),
    );

    let baseline = check_webvpn_session(&client, &cookie_jar, Some(&redirect_tracer)).await?;
    report_webvpn_session_status("WebVPN session probe #1 (baseline)", &baseline);
    if !baseline.authenticated {
        log_warn(
            "cookie-test",
            "baseline WebVPN session probe did not verify authentication; the cached login is unusable. The refresh endpoint may still update Cookie values, but that does not restore authentication, so the experiment will not continue or write those values to the cache.",
        );
        return Ok(());
    }
    save_verified_cookie_from_jar(&cookie_jar);

    let mut next_refresh_at = Instant::now() + refresh_interval;
    let mut next_portal_probe_at = Instant::now() + portal_probe_interval;
    log_info(
        "cookie-test",
        format!(
            "next refresh in {}; WebVPN session probe #2 in {}",
            format_duration(refresh_interval),
            format_duration(portal_probe_interval),
        ),
    );

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to wait for Ctrl+C")?;
                log_info("cookie-test", "stopped by user");
                return Ok(());
            }
            _ = tokio::time::sleep_until(next_refresh_at) => {
                refresh_count += 1;
                log_info(
                    "cookie-test",
                    format!("refresh #{refresh_count}: starting (elapsed {})", format_duration(started_at.elapsed())),
                );
                match refresh_cookie(&client, &cookie_jar, &redirect_tracer).await {
                    Ok(status) => {
                        report_cookie_refresh_status(refresh_count, &status);
                        next_refresh_at = Instant::now() + refresh_interval;
                        log_info(
                            "cookie-test",
                            format!("refresh #{refresh_count}: next refresh in {}", format_duration(refresh_interval)),
                        );
                    }
                    Err(err) => {
                        log_warn("cookie-test", format!("refresh #{refresh_count}: request failed: {err:#}"));
                        next_refresh_at = Instant::now() + refresh_interval;
                    }
                }
            }
            _ = tokio::time::sleep_until(next_portal_probe_at) => {
                portal_probe_count += 1;
                let completed_interval = portal_probe_interval;
                log_info(
                    "cookie-test",
                    format!(
                        "WebVPN session probe #{portal_probe_count}: starting after {} (elapsed {})",
                        format_duration(completed_interval),
                        format_duration(started_at.elapsed()),
                    ),
                );
                let status = check_webvpn_session(&client, &cookie_jar, Some(&redirect_tracer)).await?;
                report_webvpn_session_status(&format!("WebVPN session probe #{portal_probe_count}"), &status);
                if !status.authenticated {
                    log_warn("cookie-test", "WebVPN session probe could no longer verify authentication; ending the experiment");
                    return Ok(());
                }
                save_verified_cookie_from_jar(&cookie_jar);
                portal_probe_interval = next_portal_probe_interval(portal_probe_interval);
                next_portal_probe_at = Instant::now() + portal_probe_interval;
                log_info(
                    "cookie-test",
                    format!(
                        "WebVPN session probe #{portal_probe_count}: next probe in {}",
                        format_duration(portal_probe_interval),
                    ),
                );
            }
        }
    }
}

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn parse_config() -> Result<ClientConfig> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        std::process::exit(0);
    }

    let interactive = args.is_empty();
    let (server, target, listen_addr) = if interactive {
        let defaults = read_cached_interactive_defaults();
        (
            match &defaults {
                Some(defaults) => prompt_default("tows address/port", &defaults.server)?,
                None => prompt_required("tows address/port: ")?,
            },
            prompt_default(
                "target address/port",
                defaults
                    .as_ref()
                    .map(|defaults| defaults.target.as_str())
                    .unwrap_or(DEFAULT_TARGET_ADDR),
            )?,
            prompt_default(
                "local listen address/port",
                defaults
                    .as_ref()
                    .map(|defaults| defaults.listen_addr.as_str())
                    .unwrap_or(DEFAULT_LOCAL_LISTEN_ADDR),
            )?,
        )
    } else {
        let server = args[0].clone();
        let mut target = DEFAULT_TARGET_ADDR.to_string();
        let mut listen_addr = DEFAULT_LOCAL_LISTEN_ADDR.to_string();
        let mut index = 1;
        while index < args.len() {
            let value = args[index].as_str();
            index += 1;
            let next = args
                .get(index)
                .filter(|candidate| !candidate.starts_with('-'))
                .context(format!("{value} requires a value"))?
                .clone();
            index += 1;
            match value {
                "--target" => target = next,
                "--listen" => listen_addr = next,
                _ => anyhow::bail!("unsupported option: {value}"),
            }
        }
        (server, target, listen_addr)
    };

    let config = ClientConfig {
        server: normalize_server_addr(&server)?,
        target: normalize_tcp_target_arg(Some(&target))?,
        listen_addr: parse_socket_addr_with_default_host(&listen_addr, DEFAULT_TARGET_HOST)?
            .to_string(),
    };
    if interactive {
        write_cached_interactive_defaults(&config);
    }
    Ok(config)
}

fn prompt_required(label: &str) -> Result<String> {
    let value = prompt(label)?;
    if value.is_empty() {
        anyhow::bail!("{label} cannot be empty");
    }
    Ok(value)
}

fn prompt_default(label: &str, default: &str) -> Result<String> {
    let value = prompt(&format!("{label} (default: {default}): "))?;
    Ok(if value.is_empty() {
        default.to_string()
    } else {
        value
    })
}

fn prompt(label: &str) -> Result<String> {
    print!("{label}");
    io::stdout().flush().context("failed to flush prompt")?;
    let mut value = String::new();
    io::stdin()
        .read_line(&mut value)
        .context("failed to read input")?;
    Ok(value.trim().to_string())
}

async fn login_or_restore() -> Result<String> {
    if let Some(cookie) = read_cached_cookie() {
        if ticket_cookie_from_header(&cookie).is_some() {
            if let Some(cookie) = restore_cached_cookie(&cookie).await {
                log_info("client", "cached WebVPN login is valid");
                return Ok(cookie);
            }
            log_info(
                "client",
                "cached WebVPN login is unavailable; scan the QR code",
            );
        } else {
            log_warn(
                "client",
                "cached WebVPN cookie is incomplete; logging in again",
            );
        }
    }

    log_info("client", "no usable cached login; scan the WeChat QR code");
    let cookie = login_with_wechat_qr().await?;
    write_cached_cookie(&cookie);
    Ok(cookie)
}

/// Opens a WebVPN session-protected entry twice and returns the Cookie left in
/// its jar only when both requests verify authentication.
///
/// Portal redirects can rewrite an expired Cookie. The changed value is not
/// accepted or saved until a follow-up WebVPN session request verifies that it
/// is actually usable.
async fn restore_cached_cookie(cached_cookie: &str) -> Option<String> {
    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    seed_webvpn_cookie_jar(&cookie_jar, cached_cookie);
    let client = match Client::builder()
        .cookie_provider(Arc::clone(&cookie_jar))
        .user_agent(BROWSER_USER_AGENT)
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            log_warn(
                "client",
                format!("cannot create Cookie check client: {err}"),
            );
            return None;
        }
    };

    let first_status = match check_webvpn_session(&client, &cookie_jar, None).await {
        Ok(status) => status,
        Err(err) => {
            log_warn("client", format!("could not verify cached Cookie: {err:#}"));
            return None;
        }
    };
    if !first_status.authenticated {
        log_cached_cookie_rejection("cached Cookie was not accepted", &first_status);
        return None;
    }

    let verification_status = match check_webvpn_session(&client, &cookie_jar, None).await {
        Ok(status) => status,
        Err(err) => {
            log_warn(
                "client",
                format!("could not re-verify cached Cookie after WebVPN redirects: {err:#}"),
            );
            return None;
        }
    };
    if !verification_status.authenticated {
        log_cached_cookie_rejection(
            "cached Cookie changed during WebVPN redirects but did not remain usable",
            &verification_status,
        );
        return None;
    }

    let Some(cookie) = webvpn_cookie_header_from_jar(&cookie_jar) else {
        log_warn(
            "client",
            "WebVPN session probe succeeded but returned no WebVPN Cookie",
        );
        return None;
    };
    if ticket_cookie_from_header(&cookie).is_none() {
        log_warn(
            "client",
            "WebVPN session probe succeeded but returned no ticket Cookie",
        );
        return None;
    }
    if cookie != cached_cookie {
        write_cached_cookie(&cookie);
        log_info(
            "client",
            "cached WebVPN Cookie was updated by WebVPN redirects after two successful session checks",
        );
    }
    Some(cookie)
}

struct WebVpnSessionStatus {
    authenticated: bool,
    request_url: String,
    final_url: String,
    http_status: reqwest::StatusCode,
    is_login_page: bool,
    elapsed: Duration,
    body_bytes: usize,
    body: String,
    redirects: Vec<RedirectHop>,
    ticket_cookie_present: bool,
    cookie_summary: String,
}

async fn refresh_cookie(
    client: &Client,
    cookie_jar: &reqwest::cookie::Jar,
    redirect_tracer: &RedirectTracer,
) -> Result<CookieRefreshStatus> {
    let request_url = webvpn_cookie_refresh_url()?;
    let before = webvpn_cookie_header_from_jar(cookie_jar);
    redirect_tracer.start_request();
    let started_at = Instant::now();
    let response = client
        .get(&request_url)
        .header(REFERER, "https://webvpn.szut.edu.cn/")
        .send()
        .await
        .context("failed to send Cookie refresh request")?;
    let final_url = response.url().to_string();
    let http_status = response.status();
    let elapsed = started_at.elapsed();
    let redirects = redirect_tracer.finish_request();
    if !http_status.is_success() {
        anyhow::bail!(
            "Cookie refresh request failed (HTTP {}; final URL: {})",
            http_status,
            final_url
        );
    }

    let after = webvpn_cookie_header_from_jar(cookie_jar);
    let ticket_cookie_present = after
        .as_deref()
        .and_then(ticket_cookie_from_header)
        .is_some();
    let cookie_changed = before != after;
    Ok(CookieRefreshStatus {
        request_url,
        final_url: final_url.clone(),
        http_status,
        elapsed,
        redirects,
        redirected_to_login: is_login_url(&final_url),
        ticket_cookie_present,
        cookie_changed,
        cookie_summary: cookie_summary(after.as_deref()),
    })
}

async fn check_webvpn_session(
    client: &Client,
    cookie_jar: &reqwest::cookie::Jar,
    redirect_tracer: Option<&RedirectTracer>,
) -> Result<WebVpnSessionStatus> {
    if let Some(redirect_tracer) = redirect_tracer {
        redirect_tracer.start_request();
    }
    let started_at = Instant::now();
    let response = client
        .get(WEBVPN_SESSION_PROBE_URL)
        .header(REFERER, WEBVPN_LOGIN_URL)
        .send()
        .await
        .context("failed to open WebVPN session probe")?;
    let final_url = response.url().to_string();
    let http_status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read information portal response")?;
    let is_login_page = is_webvpn_login_page(&final_url, &body);
    let cookie = webvpn_cookie_header_from_jar(cookie_jar);
    let ticket_cookie_present = cookie
        .as_deref()
        .and_then(ticket_cookie_from_header)
        .is_some();
    let authenticated = http_status.is_success() && !is_login_page && ticket_cookie_present;
    Ok(WebVpnSessionStatus {
        authenticated,
        request_url: WEBVPN_SESSION_PROBE_URL.to_string(),
        final_url,
        http_status,
        is_login_page,
        elapsed: started_at.elapsed(),
        body_bytes: body.len(),
        body,
        redirects: redirect_tracer
            .map(RedirectTracer::finish_request)
            .unwrap_or_default(),
        ticket_cookie_present,
        cookie_summary: cookie_summary(cookie.as_deref()),
    })
}

fn report_cookie_refresh_status(count: u64, status: &CookieRefreshStatus) {
    let label = format!("refresh #{count}");
    log_http_redirects(&label, &status.redirects);
    log_info(
        "cookie-test",
        format!(
            "{label}: HTTP {} in {}; request URL: {}; final URL: {}; Cookie jar: {}; Cookie Jar {}",
            status.http_status,
            format_duration(status.elapsed),
            status.request_url,
            status.final_url,
            status.cookie_summary,
            if status.cookie_changed {
                "updated"
            } else {
                "unchanged"
            },
        ),
    );
    if status.redirected_to_login || !status.ticket_cookie_present {
        log_warn(
            "cookie-test",
            format!(
                "{label}: refresh endpoint ended at the login flow or without a ticket Cookie; this is not persisted and portal authentication remains the only usability check"
            ),
        );
    } else {
        log_info(
            "cookie-test",
            format!(
                "{label}: refresh endpoint completed and a ticket Cookie remains; any updated Cookie is kept only in memory until a portal probe verifies authentication"
            ),
        );
    }
}

fn report_webvpn_session_status(label: &str, status: &WebVpnSessionStatus) {
    log_http_redirects(label, &status.redirects);
    log_info(
        "cookie-test",
        format!(
            "{label}: HTTP {} in {}; request URL: {}; final URL: {}; body: {} bytes; Cookie jar: {}",
            status.http_status,
            format_duration(status.elapsed),
            status.request_url,
            status.final_url,
            status.body_bytes,
            status.cookie_summary,
        ),
    );
    if status.authenticated {
        log_success(
            "cookie-test",
            format!("{label}: WebVPN session authentication verified"),
        );
        log_info(
            "cookie-test",
            format!(
                "{label}: authenticated WebVPN response content follows:\n{}",
                terminal_safe_webpage_content(&status.body),
            ),
        );
    } else {
        log_warn(
            "cookie-test",
            format!(
                "{label}: WebVPN session authentication not verified: {}; ticket Cookie {}",
                webvpn_session_status_reason(status),
                if status.ticket_cookie_present {
                    "present"
                } else {
                    "missing"
                },
            ),
        );
    }
}

fn terminal_safe_webpage_content(body: &str) -> String {
    let mut sanitized = String::with_capacity(body.len());
    for character in body.chars() {
        match character {
            '\n' | '\r' | '\t' => sanitized.push(character),
            character if character.is_control() => {
                sanitized.push_str(&format!("\\u{{{:04x}}}", character as u32));
            }
            character => sanitized.push(character),
        }
    }
    sanitized
}

fn build_cookie_test_client(
    cookie_jar: Arc<reqwest::cookie::Jar>,
) -> Result<(Client, RedirectTracer)> {
    let redirect_tracer = RedirectTracer::default();
    let policy_tracer = redirect_tracer.clone();
    let redirect_policy = Policy::custom(move |attempt| {
        let source_url = attempt
            .previous()
            .last()
            .map(|url| redact_url_for_log(url.as_str()))
            .unwrap_or_default();
        if let Ok(mut hops) = policy_tracer.hops.lock() {
            hops.push(RedirectHop {
                status: attempt.status(),
                source_url,
                destination_url: redact_url_for_log(attempt.url().as_str()),
            });
        }
        if attempt.previous().len() > HTTP_REDIRECT_LIMIT {
            attempt.error("too many HTTP redirects")
        } else {
            attempt.follow()
        }
    });
    let client = Client::builder()
        .cookie_provider(cookie_jar)
        .user_agent(BROWSER_USER_AGENT)
        .redirect(redirect_policy)
        .build()
        .context("failed to create WebVPN HTTP client")?;
    Ok((client, redirect_tracer))
}

fn log_http_redirects(label: &str, redirects: &[RedirectHop]) {
    for (index, redirect) in redirects.iter().enumerate() {
        log_info(
            "cookie-test",
            format!(
                "{label}: redirect #{} (HTTP {}) {} -> {}",
                index + 1,
                redirect.status,
                redirect.source_url,
                redirect.destination_url,
            ),
        );
    }
}

fn webvpn_session_status_reason(status: &WebVpnSessionStatus) -> &'static str {
    if status.is_login_page {
        "request ended on the WebVPN login or fingerprint page"
    } else if !status.http_status.is_success() {
        "portal request returned a non-success HTTP status"
    } else if !status.ticket_cookie_present {
        "WebVPN ticket Cookie is missing from the Cookie jar"
    } else {
        "response did not meet the WebVPN session authentication criteria"
    }
}

fn log_cached_cookie_rejection(prefix: &str, status: &WebVpnSessionStatus) {
    log_warn(
        "client",
        format!(
            "{prefix}: {} (HTTP {}; final URL: {}; {})",
            webvpn_session_status_reason(status),
            status.http_status,
            status.final_url,
            status.cookie_summary,
        ),
    );
}

/// Saves the current Cookie only after the caller has verified portal
/// authentication for the same Cookie jar.
fn save_verified_cookie_from_jar(cookie_jar: &reqwest::cookie::Jar) {
    let Some(cookie) = webvpn_cookie_header_from_jar(cookie_jar) else {
        return;
    };
    if ticket_cookie_from_header(&cookie).is_some() {
        write_cached_cookie(&cookie);
    }
}

fn next_portal_probe_interval(interval: Duration) -> Duration {
    interval
        .checked_mul(2)
        .unwrap_or(Duration::from_secs(PORTAL_PROBE_MAX_INTERVAL_SECS))
        .min(Duration::from_secs(PORTAL_PROBE_MAX_INTERVAL_SECS))
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds > 0 && seconds % 3600 == 0 {
        format!("{}h", seconds / 3600)
    } else if seconds > 0 && seconds % 60 == 0 {
        format!("{}m", seconds / 60)
    } else if seconds > 0 {
        format!("{seconds}s")
    } else {
        format!("{}ms", duration.as_millis())
    }
}

fn webvpn_cookie_refresh_url() -> Result<String> {
    let mut url = Url::parse("https://webvpn.szut.edu.cn/wengine-vpn/cookie")
        .context("failed to create Cookie refresh URL")?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("method", "get");
        query.append_pair("host", "cas.szut.edu.cn");
        query.append_pair("scheme", "https");
        query.append_pair("path", "/personal-center");
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_millis()
            .to_string();
        query.append_pair("vpn_timestamp", &timestamp);
    }
    Ok(url.into())
}

async fn forward_connection(stream: TcpStream, url: &str, cookie: &str) -> Result<()> {
    let websocket = match connect_websocket(url, cookie).await {
        Ok(websocket) => websocket,
        Err(ConnectFailure::CookieExpired { .. }) => {
            anyhow::bail!("WebVPN login expired; restart towc and scan the QR code again")
        }
        Err(err) => return Err(anyhow!(err)),
    };
    relay_stream(websocket, stream, WebVpnHeartbeatRole::Client).await
}

async fn login_with_wechat_qr() -> Result<String> {
    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    let client = Client::builder()
        .cookie_provider(Arc::clone(&cookie_jar))
        .user_agent(BROWSER_USER_AGENT)
        .build()
        .context("failed to create WebVPN HTTP client")?;

    log_info("client", "WebVPN login: opening the login entry");
    open_webvpn_login(&client).await?;
    log_info(
        "client",
        "WebVPN login: opening the WeChat QR authorization page",
    );
    let qr_response = client
        .get(wechat_qrconnect_url()?)
        .send()
        .await
        .context("failed to open WeChat QR login page")?;
    let qr_status = qr_response.status();
    let qr_final_url = qr_response.url().to_string();
    let qr_page = qr_response
        .error_for_status()
        .with_context(|| {
            format!(
                "WeChat QR login page request failed (HTTP {qr_status}; final URL: {qr_final_url})"
            )
        })?
        .text()
        .await
        .context("failed to read WeChat QR login page")?;
    let uuid = extract_wechat_uuid(&qr_page).context("failed to find WeChat QR code")?;
    log_info(
        "client",
        format!(
            "WebVPN login: QR authorization page loaded (HTTP {qr_status}; final URL: {qr_final_url}; {} bytes)",
            qr_page.len()
        ),
    );
    let qrcode_response = client
        .get(extract_wechat_qrcode_url(&qr_page, &uuid)?)
        .send()
        .await
        .context("failed to fetch WeChat QR image")?;
    let qrcode_status = qrcode_response.status();
    let qrcode_final_url = redact_url_for_log(qrcode_response.url().as_str());
    let image = qrcode_response
        .error_for_status()
        .with_context(|| {
            format!(
                "WeChat QR image request failed (HTTP {qrcode_status}; final URL: {qrcode_final_url})"
            )
        })?
        .bytes()
        .await
        .context("failed to read WeChat QR image")?;
    qr::print(&image)?;
    log_info(
        "client",
        format!(
            "WebVPN login: QR code displayed (HTTP {qrcode_status}; {} bytes); waiting for WeChat scan",
            image.len()
        ),
    );

    let code = poll_wechat_qr_code(&client, &uuid).await?;
    log_info(
        "client",
        "WebVPN login: WeChat confirmed; completing the CAS callback",
    );
    let response = client
        .get(wechat_cas_callback_url(&code)?)
        .header(USER_AGENT, BROWSER_USER_AGENT)
        .send()
        .await
        .context("failed to complete CAS WeChat login")?;
    let callback_status = response.status();
    let callback_final_url = redact_url_for_log(response.url().as_str());
    let response = response.error_for_status().with_context(|| {
        format!("CAS WeChat login failed (HTTP {callback_status}; final URL: {callback_final_url})")
    })?;
    log_info(
        "client",
        format!(
            "WebVPN login: CAS callback completed (HTTP {callback_status}; final URL: {callback_final_url})"
        ),
    );
    activate_fingerprint_if_needed(&client, response.url().as_str()).await?;

    let cookie = webvpn_cookie_header_from_jar(&cookie_jar)
        .context("WebVPN login completed without cookies")?;
    if ticket_cookie_from_header(&cookie).is_none() {
        anyhow::bail!("WebVPN login completed without a ticket cookie");
    }
    let verification_status = check_webvpn_session(&client, &cookie_jar, None).await?;
    if !verification_status.authenticated {
        anyhow::bail!(
            "WebVPN login completed, but the WebVPN session probe did not verify authentication: {} (HTTP {}; final URL: {})",
            webvpn_session_status_reason(&verification_status),
            verification_status.http_status,
            verification_status.final_url,
        );
    }
    let cookie = webvpn_cookie_header_from_jar(&cookie_jar)
        .context("WebVPN session probe completed without cookies")?;
    if ticket_cookie_from_header(&cookie).is_none() {
        anyhow::bail!("WebVPN session probe completed without a ticket cookie");
    }
    log_success("client", "WebVPN login successful");
    Ok(cookie)
}

async fn open_webvpn_login(client: &Client) -> Result<()> {
    let response = client
        .get(WEBVPN_LOGIN_URL)
        .send()
        .await
        .context("failed to open WebVPN login")?;
    let http_status = response.status();
    let final_url = response.url().to_string();
    let response = response.error_for_status().with_context(|| {
        format!("WebVPN login request failed (HTTP {http_status}; final URL: {final_url})")
    })?;
    log_info(
        "client",
        format!("WebVPN login entry completed (HTTP {http_status}; final URL: {final_url})"),
    );
    if is_fingerprint_url(response.url().as_str()) {
        log_info("client", "WebVPN login: fingerprint activation is required");
        set_fingerprint(client, response.url().as_str()).await?;
    }
    Ok(())
}

async fn activate_fingerprint_if_needed(client: &Client, final_url: &str) -> Result<()> {
    if is_fingerprint_url(final_url) {
        set_fingerprint(client, final_url).await?;
    }
    Ok(())
}

async fn set_fingerprint(client: &Client, source_url: &str) -> Result<()> {
    let source = Url::parse(source_url).context("failed to parse WebVPN fingerprint URL")?;
    let mut url = Url::parse("https://webvpn.szut.edu.cn/set-fingerprint")
        .context("failed to create WebVPN fingerprint URL")?;
    {
        let mut query = url.query_pairs_mut();
        for (name, value) in source.query_pairs() {
            if name != "fingerprint" {
                query.append_pair(&name, &value);
            }
        }
        query.append_pair("fingerprint", WEBVPN_FINGERPRINT);
    }
    let response = client
        .get(url)
        .header(REFERER, source_url)
        .send()
        .await
        .context("failed to activate WebVPN fingerprint")?;
    let http_status = response.status();
    let final_url = response.url().to_string();
    let response = response.error_for_status().with_context(|| {
        format!("WebVPN fingerprint activation failed (HTTP {http_status}; final URL: {final_url})")
    })?;
    log_info(
        "client",
        format!(
            "WebVPN fingerprint activation completed (HTTP {http_status}; final URL: {final_url})"
        ),
    );
    if is_fingerprint_url(response.url().as_str()) {
        anyhow::bail!("WebVPN fingerprint activation did not finish");
    }
    Ok(())
}

fn wechat_qrconnect_url() -> Result<String> {
    let mut url = Url::parse(&format!(
        "https://webvpn.szut.edu.cn/https/{WEBVPN_WECHAT_HASH}/connect/qrconnect"
    ))
    .context("failed to create WeChat QR URL")?;
    url.query_pairs_mut()
        .append_pair("appid", WECHAT_APP_ID)
        .append_pair("redirect_uri", WECHAT_REDIRECT_URI)
        .append_pair("response_type", "code")
        .append_pair("self_redirect", "false")
        .append_pair("scope", "snsapi_login");
    Ok(url.into())
}

fn wechat_cas_callback_url(code: &str) -> Result<String> {
    let mut url = Url::parse(WECHAT_REDIRECT_URI).context("failed to create CAS callback URL")?;
    url.query_pairs_mut()
        .append_pair("code", code)
        .append_pair("state", "");
    Ok(url.into())
}

fn wechat_poll_url(uuid: &str, last: Option<u16>) -> Result<String> {
    let mut url = Url::parse(&format!(
        "https://webvpn.szut.edu.cn/https/{WEBVPN_WECHAT_HASH}/connect/l/qrconnect"
    ))
    .context("failed to create WeChat polling URL")?;
    url.query_pairs_mut().append_pair("uuid", uuid);
    if let Some(last) = last {
        url.query_pairs_mut().append_pair("last", &last.to_string());
    }
    Ok(url.into())
}

async fn poll_wechat_qr_code(client: &Client, uuid: &str) -> Result<String> {
    let mut last = None;
    let mut last_reported_status = None;
    for _ in 0..WECHAT_POLL_ATTEMPTS {
        let body = client
            .get(wechat_poll_url(uuid, last)?)
            .timeout(Duration::from_secs(WECHAT_POLL_TIMEOUT_SECS))
            .send()
            .await
            .context("failed to poll WeChat QR status")?
            .error_for_status()
            .context("WeChat QR polling request failed")?
            .text()
            .await
            .context("failed to read WeChat QR status")?;
        let status = parse_wechat_status(&body)
            .with_context(|| format!("failed to parse WeChat QR status: {body}"))?;
        last = Some(status.0);
        if last_reported_status != Some(status.0) {
            log_info(
                "client",
                format!(
                    "WeChat QR login: {} (status {})",
                    wechat_status_message(status.0),
                    status.0
                ),
            );
            last_reported_status = Some(status.0);
        }
        match status.0 {
            405 if !status.1.is_empty() => return Ok(status.1),
            405 => anyhow::bail!("WeChat confirmed login without a code"),
            402 => anyhow::bail!("WeChat QR code expired"),
            403 => anyhow::bail!("WeChat QR login was canceled"),
            404 => tokio::time::sleep(Duration::from_millis(300)).await,
            _ => tokio::time::sleep(Duration::from_millis(WECHAT_POLL_DELAY_MS)).await,
        }
    }
    anyhow::bail!("timed out waiting for WeChat QR login")
}

fn wechat_status_message(status: u16) -> &'static str {
    match status {
        404 => "waiting for scan",
        405 => "login confirmed",
        402 => "QR code expired",
        403 => "login canceled",
        408 => "waiting for confirmation",
        _ => "received an intermediate status",
    }
}

fn parse_wechat_status(body: &str) -> Option<(u16, String)> {
    let status = assignment_value(body, "wx_errcode")?
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    let code = assignment_value(body, "wx_code")
        .and_then(quoted_value)
        .unwrap_or_default();
    Some((status, code))
}

fn extract_wechat_uuid(html: &str) -> Option<String> {
    assignment_value(html, "G")
        .and_then(quoted_value)
        .or_else(|| extract_token_after(html, "uuid="))
}

fn extract_wechat_qrcode_url(html: &str, uuid: &str) -> Result<String> {
    if let Some(src) = html.split('<').find_map(|fragment| {
        let fragment = fragment.trim_start();
        (fragment.starts_with("img") && fragment.contains("/connect/qrcode/"))
            .then(|| attr_value(fragment, "src"))
            .flatten()
    }) {
        return absolute_webvpn_url(&src);
    }
    absolute_webvpn_url(&format!(
        "/https/{WEBVPN_WECHAT_HASH}/connect/qrcode/{uuid}?vpn-1"
    ))
}

fn absolute_webvpn_url(value: &str) -> Result<String> {
    if value.starts_with("http://") || value.starts_with("https://") {
        return Ok(value.to_string());
    }
    Url::parse("https://webvpn.szut.edu.cn/")
        .and_then(|base| base.join(value))
        .map(Into::into)
        .context("failed to create WebVPN URL")
}

fn redact_url_for_log(url: &str) -> String {
    if let Some((prefix, rest)) = url.split_once("/connect/qrcode/") {
        let suffix = rest
            .find('?')
            .map(|index| &rest[index..])
            .unwrap_or_default();
        return format!("{prefix}/connect/qrcode/<redacted>{suffix}");
    }

    let Ok(mut parsed) = Url::parse(url) else {
        return url.to_string();
    };
    let query: Vec<_> = parsed
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect();
    if query
        .iter()
        .all(|(name, _)| !matches!(name.as_str(), "code" | "uuid" | "ticket" | "token"))
    {
        return url.to_string();
    }
    parsed.set_query(None);
    {
        let mut pairs = parsed.query_pairs_mut();
        for (name, value) in query {
            pairs.append_pair(
                &name,
                if matches!(name.as_str(), "code" | "uuid" | "ticket" | "token") {
                    "<redacted>"
                } else {
                    &value
                },
            );
        }
    }
    parsed.into()
}

fn assignment_value<'a>(body: &'a str, name: &str) -> Option<&'a str> {
    let mut offset = 0;
    while let Some(relative_index) = body[offset..].find(name) {
        let index = offset + relative_index;
        let value = &body[index + name.len()..];
        if let Some(value) = value.trim_start().strip_prefix('=') {
            return Some(value.trim_start());
        }
        offset = index + name.len();
    }
    None
}

fn quoted_value(value: &str) -> Option<String> {
    let quote = value.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    value[quote.len_utf8()..]
        .find(quote)
        .map(|end| value[quote.len_utf8()..quote.len_utf8() + end].to_string())
}

fn extract_token_after(body: &str, marker: &str) -> Option<String> {
    let token: String = body
        .split_once(marker)?
        .1
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
        .collect();
    (!token.is_empty()).then_some(token)
}

fn attr_value(fragment: &str, name: &str) -> Option<String> {
    let marker = format!("{name}=");
    let value = fragment.split_once(&marker)?.1.trim_start();
    quoted_value(value)
}

fn is_fingerprint_url(url: &str) -> bool {
    url.contains("/fingerprint")
}

fn webvpn_cookie_header_from_jar(cookie_jar: &reqwest::cookie::Jar) -> Option<String> {
    let url = Url::parse("https://webvpn.szut.edu.cn/").ok()?;
    let value = cookie_jar.cookies(&url)?.to_str().ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn seed_webvpn_cookie_jar(cookie_jar: &reqwest::cookie::Jar, header: &str) {
    let url =
        Url::parse("https://webvpn.szut.edu.cn/").expect("static WebVPN cookie URL must be valid");
    for cookie in header
        .split(';')
        .map(str::trim)
        .filter(|cookie| !cookie.is_empty())
    {
        cookie_jar.add_cookie_str(cookie, &url);
    }
}

fn ticket_cookie_from_header(header: &str) -> Option<&str> {
    header.split(';').map(str::trim).find(|cookie| {
        cookie
            .strip_prefix(WEBVPN_TICKET_COOKIE_PREFIX)
            .is_some_and(|value| !value.trim().is_empty())
    })
}

fn cookie_summary(header: Option<&str>) -> String {
    let Some(header) = header else {
        return "no cookies".to_string();
    };
    let names: Vec<_> = header
        .split(';')
        .map(str::trim)
        .filter_map(|cookie| cookie.split_once('=').map(|(name, _)| name.trim()))
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return "no cookies".to_string();
    }
    format!(
        "{} cookie(s) [{}]; ticket {}",
        names.len(),
        names.join(", "),
        if ticket_cookie_from_header(header).is_some() {
            "present"
        } else {
            "missing"
        },
    )
}

fn is_login_url(url: &str) -> bool {
    url.contains("/login") || url.contains("/cas/login")
}

fn is_webvpn_login_page(url: &str, body: &str) -> bool {
    let Ok(url) = Url::parse(url) else {
        return false;
    };
    if url.domain() != Some("webvpn.szut.edu.cn") {
        return false;
    }
    matches!(url.path(), "/login" | "/fingerprint")
        || body.contains("name=\"execution\"")
        || body.contains("name='execution'")
}

fn read_cached_cookie() -> Option<String> {
    let path = cookie_cache_path()?;
    match fs::read_to_string(path) {
        Ok(cookie) => (!cookie.trim().is_empty()).then(|| cookie.trim().to_string()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => {
            log_warn("client", format!("failed to read cached cookie: {err}"));
            None
        }
    }
}

fn write_cached_cookie(cookie: &str) {
    let Some(path) = cookie_cache_path() else {
        return;
    };
    if let Some(parent) = path.parent() {
        if let Err(err) = fs::create_dir_all(parent) {
            log_warn(
                "client",
                format!("failed to create cookie cache directory: {err}"),
            );
            return;
        }
    }
    if let Err(err) = fs::write(path, format!("{cookie}\n")) {
        log_warn("client", format!("failed to save cached cookie: {err}"));
    }
}

fn read_cached_interactive_defaults() -> Option<InteractiveDefaults> {
    let path = interactive_defaults_cache_path()?;
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return None,
        Err(err) => {
            log_warn(
                "client",
                format!("failed to read interactive defaults: {err}"),
            );
            return None;
        }
    };
    let mut server = None;
    let mut target = None;
    let mut listen_addr = None;
    for line in contents.lines() {
        if let Some(value) = line.strip_prefix("server=") {
            server = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("target=") {
            target = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("listen=") {
            listen_addr = Some(value.trim().to_string());
        }
    }
    match (server, target, listen_addr) {
        (Some(server), Some(target), Some(listen_addr))
            if !server.is_empty() && !target.is_empty() && !listen_addr.is_empty() =>
        {
            Some(InteractiveDefaults {
                server,
                target,
                listen_addr,
            })
        }
        _ => {
            log_warn("client", "interactive defaults are invalid; ignoring them");
            None
        }
    }
}

fn write_cached_interactive_defaults(config: &ClientConfig) {
    let Some(path) = interactive_defaults_cache_path() else {
        return;
    };
    if let Some(parent) = path.parent() {
        if let Err(err) = fs::create_dir_all(parent) {
            log_warn(
                "client",
                format!("failed to create interactive defaults directory: {err}"),
            );
            return;
        }
    }
    let contents = format!(
        "server={}\ntarget={}\nlisten={}\n",
        config.server, config.target, config.listen_addr
    );
    if let Err(err) = fs::write(path, contents) {
        log_warn(
            "client",
            format!("failed to save interactive defaults: {err}"),
        );
    }
}

#[cfg(windows)]
fn cookie_cache_path() -> Option<PathBuf> {
    std::env::var_os("APPDATA")
        .or_else(|| std::env::var_os("LOCALAPPDATA"))
        .map(PathBuf::from)
        .map(|path| path.join("tcp_over_websocket").join(COOKIE_CACHE_FILE_NAME))
}

fn interactive_defaults_cache_path() -> Option<PathBuf> {
    cookie_cache_path().and_then(|path| {
        path.parent()
            .map(|parent| parent.join(INTERACTIVE_DEFAULTS_CACHE_FILE_NAME))
    })
}

#[cfg(not(windows))]
fn cookie_cache_path() -> Option<PathBuf> {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .map(|path| path.join("tcp_over_websocket").join(COOKIE_CACHE_FILE_NAME))
}

fn print_usage() {
    println!("Usage: towc [tows-ip[:port]] [--target host:port] [--listen host:port]");
    println!(
        "Defaults: server port {DEFAULT_SERVER_PORT}, target {DEFAULT_TARGET_ADDR}, listen {DEFAULT_LOCAL_LISTEN_ADDR}"
    );
    println!("Without arguments, towc asks for the three addresses and logs in with WeChat QR.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portal_probe_interval_doubles_until_the_daily_cap() {
        let one_minute = Duration::from_secs(PORTAL_PROBE_INITIAL_INTERVAL_SECS);
        assert_eq!(
            next_portal_probe_interval(one_minute),
            Duration::from_secs(2 * 60)
        );
        assert_eq!(
            next_portal_probe_interval(Duration::from_secs(12 * 60 * 60)),
            Duration::from_secs(PORTAL_PROBE_MAX_INTERVAL_SECS)
        );
        assert_eq!(
            next_portal_probe_interval(Duration::from_secs(PORTAL_PROBE_MAX_INTERVAL_SECS)),
            Duration::from_secs(PORTAL_PROBE_MAX_INTERVAL_SECS)
        );
    }

    #[test]
    fn cookie_summary_exposes_names_but_not_values() {
        let header = "ticket=secret-value; theme=dark";
        let summary = cookie_summary(Some(header));
        assert!(summary.contains("ticket"));
        assert!(summary.contains("theme"));
        assert!(!summary.contains("secret-value"));
        assert!(!summary.contains("dark"));
    }

    #[test]
    fn login_url_logging_redacts_one_time_identifiers() {
        let qrcode_url = "https://webvpn.szut.edu.cn/connect/qrcode/one-time-id?vpn-1";
        assert_eq!(
            redact_url_for_log(qrcode_url),
            "https://webvpn.szut.edu.cn/connect/qrcode/<redacted>?vpn-1"
        );
        let callback_url = "https://cas.szut.edu.cn/cas/login?code=one-time-code&state=keep";
        let redacted = redact_url_for_log(callback_url);
        assert!(redacted.contains("code=%3Credacted%3E"));
        assert!(redacted.contains("state=keep"));
        assert!(!redacted.contains("one-time-code"));

        let portal_url = "https://example.edu/?ticket=ST-one-time-ticket&view=home";
        let redacted = redact_url_for_log(portal_url);
        assert!(redacted.contains("ticket=%3Credacted%3E"));
        assert!(redacted.contains("view=home"));
        assert!(!redacted.contains("ST-one-time-ticket"));
    }

    #[test]
    fn webpage_content_preserves_layout_but_escapes_terminal_controls() {
        assert_eq!(
            terminal_safe_webpage_content("line 1\nline 2\u{1b}[2J"),
            "line 1\nline 2\\u{001b}[2J"
        );
    }

    #[test]
    fn webvpn_session_probe_recognizes_login_and_fingerprint_pages() {
        assert!(is_webvpn_login_page(
            "https://webvpn.szut.edu.cn/login",
            "<html></html>"
        ));
        assert!(is_webvpn_login_page(
            "https://webvpn.szut.edu.cn/fingerprint",
            "<html></html>"
        ));
        assert!(is_webvpn_login_page(
            "https://webvpn.szut.edu.cn/",
            "<input name=\"execution\">"
        ));
        assert!(!is_webvpn_login_page(
            "https://webvpn.szut.edu.cn/",
            "<html>WebVPN home</html>"
        ));
    }
}
