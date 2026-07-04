use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::cookie::CookieStore;
use reqwest::header::{ORIGIN, REFERER};
use reqwest::{Client, Url};
use serde::Deserialize;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;
use tcp_over_websocket::{
    ConnectFailure, DEFAULT_LOCAL_LISTEN_ADDR, DEFAULT_TARGET_HOST, DEFAULT_WEBVPN_WS_HOST,
    build_webvpn_ws_url, connect_websocket, log_error, log_info, log_success, log_warn,
    normalize_server_addr, normalize_tcp_target_arg, parse_socket_addr_with_default_host,
    relay_stream, rsa_encrypt,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
#[cfg(target_os = "windows")]
use winit::{
    application::ApplicationHandler,
    dpi::{LogicalPosition, LogicalSize, PhysicalSize},
    event::WindowEvent,
    event_loop::{ActiveEventLoop, EventLoop},
    window::{Window, WindowId},
};
#[cfg(target_os = "windows")]
use wry::{PageLoadEvent, Rect, WebView, WebViewBuilder};

#[cfg(target_os = "windows")]
use windows::Win32::{
    Foundation::HWND,
    UI::WindowsAndMessaging::{DestroyWindow, SW_HIDE, ShowWindow},
};
#[cfg(target_os = "windows")]
use winit::raw_window_handle::{HasWindowHandle, RawWindowHandle};

const WEBVPN_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/login";
const WEBVPN_TICKET_COOKIE_NAME: &str = "wengine_vpn_ticketwebvpn_szut_edu_cn";
const WEBVPN_CAS_HASH: &str = "77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d";
const WEBVPN_CAS_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/https/77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d/cas/login?service=https%3A%2F%2Fwebvpn.szut.edu.cn%2Flogin%3Fcas_login%3Dtrue";
const WEBVPN_FINGERPRINT: &str = "5a0b00fe6ae8277a4bfadd4e103f6e1c";
const WEBVPN_READY_ATTEMPTS: usize = 6;
const WEBVPN_READY_SETTLE_MS: u64 = 700;
const WEBVPN_READY_TIMEOUT_MS: u64 = 900;
const CAS_LOGIN_ATTEMPTS: usize = 2;
const CAS_LOGIN_RETRY_SETTLE_MS: u64 = 1500;

enum VerificationLogin {
    Sms { mobile: String },
    Email { email: String },
}

#[derive(Deserialize)]
struct PublicKeyResponse {
    modulus: String,
    exponent: String,
}

struct ResolvedCookie {
    header: String,
    should_probe: bool,
}

struct WebVpnLoginEntry {
    ticket_cookie: Option<String>,
    cas_login_url: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        log_error("client", format!("{err:#}"));
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring crypto provider");

    let mut args = std::env::args().skip(1);
    let mut listen_addr = DEFAULT_LOCAL_LISTEN_ADDR.to_string();
    let mut server = None::<String>;
    let mut target = None::<String>;
    let mut cookie = None::<String>;
    let mut login = None::<VerificationLogin>;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                listen_addr = args.next().context("missing value for --listen")?;
            }
            "--target" => {
                target = Some(args.next().context("missing value for --target")?);
            }
            "--cookie" => {
                cookie = Some(args.next().context("missing value for --cookie")?);
            }
            "--login" => {
                let value = args.next().context("missing value for --login")?;
                if login.replace(parse_login_identity(&value)?).is_some() {
                    anyhow::bail!("--login can only be specified once");
                }
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            other => {
                if other.starts_with('-') {
                    return Err(anyhow::anyhow!("unknown argument: {other}"));
                }
                if server.replace(other.to_string()).is_some() {
                    return Err(anyhow::anyhow!("unexpected extra argument: {other}"));
                }
            }
        }
    }

    let server = server.context("missing required server address, for example 192.0.2.10:4489")?;
    let url = build_webvpn_ws_url(&server, target.as_deref())?;
    let server_addr = normalize_server_addr(&server)?;
    let target_addr = normalize_tcp_target_arg(target.as_deref())?;
    let cookie = resolve_cookie(cookie, login).await?;
    let listen_addr = parse_socket_addr_with_default_host(&listen_addr, DEFAULT_TARGET_HOST)?;

    if cookie.should_probe {
        wait_for_webvpn_ready(&url, &cookie.header).await?;
    }

    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind local tcp listener on {listen_addr}"))?;
    log_success(
        "client",
        format!(
            "ready: {listen_addr} -> {DEFAULT_WEBVPN_WS_HOST} -> {server_addr} -> {target_addr}"
        ),
    );

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted.context("failed to accept local tcp connection")?;
                let url = url.clone();
                let cookie = cookie.header.clone();

                tokio::spawn(async move {
                    log_info("client", format!("tcp {peer_addr} connected"));
                    match handle_local_connection(stream, &url, &cookie).await {
                        Ok(()) => {
                            log_info("client", format!("tcp {peer_addr} closed"));
                        }
                        Err(ConnectFailure::CookieExpired { location }) => {
                            log_error(
                                "client",
                                format!(
                                    "cookie expired, please restart towc and log in again; location: {location}"
                                ),
                            );
                            std::process::exit(1);
                        }
                        Err(ConnectFailure::WebVpnFailed { location }) => {
                            log_error(
                                "client",
                                format!(
                                    "WebVPN failed, check whether tows is running and reachable; location: {location}"
                                ),
                            );
                            std::process::exit(1);
                        }
                        Err(err) => {
                            log_error("client", format!("tcp {peer_addr}: {err}"));
                        }
                    }
                });
            }
            _ = &mut shutdown => {
                log_info("client", "shutting down");
                return Ok(());
            }
        }
    }
}

async fn handle_local_connection(
    stream: TcpStream,
    url: &str,
    cookie: &str,
) -> std::result::Result<(), ConnectFailure> {
    let websocket = connect_websocket(url, cookie).await?;
    relay_stream(websocket, stream)
        .await
        .map_err(ConnectFailure::Other)
}

fn parse_login_identity(value: &str) -> Result<VerificationLogin> {
    let value = value.trim();
    if value.is_empty() {
        anyhow::bail!("--login cannot be empty");
    }

    if value.chars().all(|ch| ch.is_ascii_digit()) {
        return Ok(VerificationLogin::Sms {
            mobile: value.to_string(),
        });
    }

    if value.contains('@') {
        return Ok(VerificationLogin::Email {
            email: value.to_string(),
        });
    }

    anyhow::bail!("invalid --login value: use a numeric mobile number or an email address")
}

async fn resolve_cookie(
    cookie: Option<String>,
    verification_login: Option<VerificationLogin>,
) -> Result<ResolvedCookie> {
    if cookie.is_some() && verification_login.is_some() {
        anyhow::bail!("--cookie cannot be combined with --login");
    }

    match (cookie, verification_login) {
        (Some(cookie), None) => Ok(ResolvedCookie {
            header: cookie,
            should_probe: false,
        }),
        (None, Some(login)) => Ok(ResolvedCookie {
            header: login_with_verification_code(login).await?,
            should_probe: true,
        }),
        (None, None) => Ok(ResolvedCookie {
            header: login_and_get_cookie()?,
            should_probe: true,
        }),
        (Some(_), Some(_)) => unreachable!(),
    }
}

async fn wait_for_webvpn_ready(url: &str, cookie: &str) -> Result<()> {
    for attempt in 1..=WEBVPN_READY_ATTEMPTS {
        match probe_webvpn_ready(url, cookie).await {
            Ok(true) => return Ok(()),
            Ok(false) => {
                log_info(
                    "client",
                    format!(
                        "WebVPN tunnel closed during readiness check, retrying ({attempt}/{WEBVPN_READY_ATTEMPTS})"
                    ),
                );
            }
            Err(err) => {
                log_warn(
                    "client",
                    format!(
                        "WebVPN readiness check failed, retrying ({attempt}/{WEBVPN_READY_ATTEMPTS}): {err:#}"
                    ),
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(WEBVPN_READY_SETTLE_MS)).await;
    }

    anyhow::bail!("WebVPN tunnel did not become ready after login; please try again")
}

async fn probe_webvpn_ready(url: &str, cookie: &str) -> Result<bool> {
    let mut websocket = connect_websocket(url, cookie)
        .await
        .map_err(|err| anyhow::anyhow!(err))
        .context("failed to open readiness WebSocket")?;

    let timeout = tokio::time::sleep(Duration::from_millis(WEBVPN_READY_TIMEOUT_MS));
    tokio::pin!(timeout);

    tokio::select! {
        message = websocket.next() => {
            match message {
                Some(Ok(Message::Close(_))) | None => Ok(false),
                Some(Ok(_)) => Ok(true),
                Some(Err(err)) => Err(anyhow::anyhow!(err).context("readiness WebSocket failed")),
            }
        }
        _ = &mut timeout => Ok(true),
    }
}

async fn login_with_verification_code(login: VerificationLogin) -> Result<String> {
    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    let client = Client::builder()
        .cookie_provider(Arc::clone(&cookie_jar))
        .user_agent("towc/0.1")
        .build()
        .context("failed to build WebVPN login HTTP client")?;

    let login_entry = initialize_webvpn_ticket_cookie(&client, &cookie_jar).await?;

    let (username, send_url, label) = match login {
        VerificationLogin::Sms { mobile } => (
            mobile.clone(),
            cas_url_with_query("v2/services/sedsms", "mobile", &mobile)?,
            "SMS",
        ),
        VerificationLogin::Email { email } => (
            email.clone(),
            cas_url_with_query("v2/services/sendEmailYzm", "email", &email)?,
            "email",
        ),
    };

    log_info("client", format!("sending {label} verification code"));
    let send_body = client
        .get(send_url)
        .send()
        .await
        .context("failed to send verification code request")?
        .error_for_status()
        .context("verification code request failed")?
        .text()
        .await
        .context("failed to read verification code response")?;
    log_info(
        "client",
        format!("verification service response: {}", send_body.trim()),
    );

    let login_url = login_entry.cas_login_url;
    let login_html = client
        .get(&login_url)
        .send()
        .await
        .context("failed to fetch CAS login page")?
        .error_for_status()
        .context("CAS login page request failed")?
        .text()
        .await
        .context("failed to read CAS login page")?;
    let mut execution =
        extract_execution(&login_html).context("failed to find CAS execution token")?;

    let public_key = client
        .get(cas_url("v2/getPubKey"))
        .send()
        .await
        .context("failed to fetch CAS RSA public key")?
        .error_for_status()
        .context("CAS RSA public key request failed")?
        .json::<PublicKeyResponse>()
        .await
        .context("failed to parse CAS RSA public key response")?;

    let code = prompt_verification_code(label)?;
    let reversed_code: String = code.chars().rev().collect();
    let encrypted_code = rsa_encrypt(&reversed_code, &public_key.modulus, &public_key.exponent)?;

    let mut final_url = None::<String>;
    for attempt in 1..=CAS_LOGIN_ATTEMPTS {
        log_info("client", "submitting verification login");
        let response = client
            .post(&login_url)
            .header(ORIGIN, "https://webvpn.szut.edu.cn")
            .header(REFERER, &login_url)
            .form(&[
                ("username", username.as_str()),
                ("password", encrypted_code.as_str()),
                ("execution", execution.as_str()),
                ("_eventId", "submit"),
            ])
            .send()
            .await
            .context("failed to submit CAS login form")?
            .error_for_status()
            .context("CAS login form submission failed")?;

        let response_url = response.url().to_string();
        let body = response
            .text()
            .await
            .context("failed to read CAS login response")?;
        let next_execution = extract_execution(&body);
        if !is_cas_login_form(&response_url, next_execution.as_deref()) {
            final_url = Some(response_url);
            break;
        }

        if attempt >= CAS_LOGIN_ATTEMPTS {
            anyhow::bail!(
                "CAS login was not accepted; check whether the verification code is correct"
            );
        }

        execution = next_execution.context("CAS login retry did not include an execution token")?;
        log_warn(
            "client",
            "CAS login was not accepted yet, retrying once with the same verification code",
        );
        tokio::time::sleep(Duration::from_millis(CAS_LOGIN_RETRY_SETTLE_MS)).await;
    }
    let final_url = final_url.context("CAS login was not accepted")?;

    let activated_ticket =
        activate_webvpn_fingerprint_if_needed(&client, &cookie_jar, &final_url).await?;
    let post_login_ticket = ticket_cookie_from_jar(&cookie_jar);

    let cookie = activated_ticket
        .or(post_login_ticket)
        .or(login_entry.ticket_cookie)
        .context("login completed but WebVPN ticket cookie was not found")?;
    log_success("client", "verification login completed");
    Ok(cookie)
}

async fn initialize_webvpn_ticket_cookie(
    client: &Client,
    cookie_jar: &reqwest::cookie::Jar,
) -> Result<WebVpnLoginEntry> {
    log_info("client", "initializing WebVPN ticket cookie");
    let response = client
        .get(WEBVPN_LOGIN_URL)
        .send()
        .await
        .context("failed to open WebVPN login entry")?
        .error_for_status()
        .context("WebVPN login entry request failed")?;
    let mut final_url = response.url().to_string();

    if is_webvpn_prelogin_fingerprint_url(&final_url) {
        set_webvpn_fingerprint(client).await?;
        let response = client
            .get(WEBVPN_LOGIN_URL)
            .send()
            .await
            .context("failed to reopen WebVPN login after fingerprint")?
            .error_for_status()
            .context("WebVPN login request after fingerprint failed")?;
        final_url = response.url().to_string();
    }

    let ticket_cookie = ticket_cookie_from_jar(cookie_jar);
    if ticket_cookie.is_some() {
        log_info("client", "WebVPN ticket cookie initialized");
    } else {
        log_warn(
            "client",
            "WebVPN login entry did not set a ticket cookie; continuing with CAS login",
        );
    }

    let cas_login_url = if final_url.contains("/cas/login") {
        final_url
    } else {
        WEBVPN_CAS_LOGIN_URL.to_string()
    };

    Ok(WebVpnLoginEntry {
        ticket_cookie,
        cas_login_url,
    })
}

async fn set_webvpn_fingerprint(client: &Client) -> Result<()> {
    log_info("client", "registering WebVPN fingerprint");
    let url =
        format!("https://webvpn.szut.edu.cn/set-fingerprint?fingerprint={WEBVPN_FINGERPRINT}");
    client
        .get(url)
        .header(REFERER, "https://webvpn.szut.edu.cn/fingerprint")
        .send()
        .await
        .context("failed to register WebVPN fingerprint")?
        .error_for_status()
        .context("WebVPN fingerprint registration failed")?;

    Ok(())
}

fn cas_url(path: &str) -> String {
    format!(
        "https://webvpn.szut.edu.cn/https/{WEBVPN_CAS_HASH}/cas/{}",
        path.trim_start_matches('/')
    )
}

fn cas_url_with_query(path: &str, name: &str, value: &str) -> Result<String> {
    let mut url = Url::parse(&cas_url(path)).context("failed to build CAS request URL")?;
    url.query_pairs_mut().append_pair(name, value);
    Ok(url.into())
}

fn prompt_verification_code(label: &str) -> Result<String> {
    print!("Enter {label} verification code: ");
    io::stdout()
        .flush()
        .context("failed to flush verification code prompt")?;

    let mut code = String::new();
    io::stdin()
        .read_line(&mut code)
        .context("failed to read verification code")?;
    let code = code.trim();
    if code.is_empty() {
        anyhow::bail!("verification code cannot be empty");
    }
    Ok(code.to_string())
}

fn ticket_cookie_from_jar(cookie_jar: &reqwest::cookie::Jar) -> Option<String> {
    let url = Url::parse("https://webvpn.szut.edu.cn/").ok()?;
    let header = cookie_jar.cookies(&url)?.to_str().ok()?.to_string();
    header
        .split(';')
        .map(str::trim)
        .find(|cookie| cookie.starts_with(&format!("{WEBVPN_TICKET_COOKIE_NAME}=")))
        .map(str::to_string)
}

async fn activate_webvpn_fingerprint_if_needed(
    client: &Client,
    cookie_jar: &reqwest::cookie::Jar,
    final_url: &str,
) -> Result<Option<String>> {
    if !is_webvpn_fingerprint_url(final_url) {
        return Ok(None);
    }

    log_info("client", "activating WebVPN fingerprint");
    let response = client
        .get(final_url)
        .send()
        .await
        .context("failed to open WebVPN fingerprint activation")?
        .error_for_status()
        .context("WebVPN fingerprint activation request failed")?;

    let final_activation_url = response.url().to_string();
    if !is_webvpn_fingerprint_url(&final_activation_url) {
        return ticket_cookie_from_jar(cookie_jar)
            .context("WebVPN fingerprint activation completed without ticket cookie")
            .map(Some);
    }

    anyhow::bail!(
        "WebVPN fingerprint activation did not complete over HTTP; final URL: {final_activation_url}"
    )
}

fn is_webvpn_prelogin_fingerprint_url(url: &str) -> bool {
    url.trim_end_matches('/') == "https://webvpn.szut.edu.cn/fingerprint"
}

fn is_webvpn_fingerprint_url(url: &str) -> bool {
    url.contains("/fingerprint") && url.contains("ticket=ST-")
}

fn is_cas_login_form(final_url: &str, execution: Option<&str>) -> bool {
    final_url.contains("/cas/login") && execution.is_some()
}

fn extract_execution(html: &str) -> Option<String> {
    html.split('<').find_map(|fragment| {
        let fragment = fragment.trim_start();
        if !fragment.starts_with("input") || !has_attr_value(fragment, "name", "execution") {
            return None;
        }

        attr_value(fragment, "value")
    })
}

fn has_attr_value(fragment: &str, name: &str, expected: &str) -> bool {
    attr_value(fragment, name).is_some_and(|value| value == expected)
}

fn attr_value(fragment: &str, name: &str) -> Option<String> {
    let mut rest = fragment;
    loop {
        let index = rest.find(name)?;
        let after_name = &rest[index + name.len()..];
        let after_equals = after_name.trim_start().strip_prefix('=')?.trim_start();
        let quote = after_equals.chars().next()?;
        if quote != '"' && quote != '\'' {
            rest = &after_equals[quote.len_utf8()..];
            continue;
        }

        let value_start = quote.len_utf8();
        let value_end = after_equals[value_start..].find(quote)?;
        return Some(after_equals[value_start..value_start + value_end].to_string());
    }
}

fn print_usage() {
    eprintln!(
        "Usage: towc <server-ip[:port]> [--target <target-ip[:port]|port>] [--cookie <cookie>] [--login <mobile|email>] [--listen 127.0.0.1:9999]"
    );
    eprintln!("       server port defaults to 4489; --target defaults to 127.0.0.1:9999");
    eprintln!(
        "       --login sends a verification code by SMS for numeric values, or email when the value contains @"
    );
    #[cfg(target_os = "windows")]
    eprintln!(
        "       on Windows, towc opens {WEBVPN_LOGIN_URL} to fetch the WebVPN cookie when --cookie is omitted"
    );
    #[cfg(not(target_os = "windows"))]
    eprintln!("       on Linux, use --login for verification-code login or pass --cookie manually");
}

#[cfg(target_os = "windows")]
fn login_and_get_cookie() -> Result<String> {
    log_info(
        "client",
        format!("opening WebVPN login window: {WEBVPN_LOGIN_URL}"),
    );
    log_info(
        "client",
        "finish login in the window; it will close automatically",
    );

    let event_loop = EventLoop::<LoginSignal>::with_user_event()
        .build()
        .context("failed to create login window event loop")?;
    let proxy = event_loop.create_proxy();
    let mut app = LoginApp::new(WEBVPN_LOGIN_URL.to_string(), proxy);

    event_loop
        .run_app(&mut app)
        .context("login window event loop failed")?;

    if let Some(error) = app.error {
        anyhow::bail!(error);
    }

    app.cookie_header
        .context("login window closed before WebVPN login completed")
}

#[cfg(not(target_os = "windows"))]
fn login_and_get_cookie() -> Result<String> {
    anyhow::bail!(
        "missing --cookie or --login: automatic browser login is only available on Windows"
    )
}

#[cfg(target_os = "windows")]
enum LoginSignal {
    Navigation(String),
    PageLoaded(String),
    Tick,
}

#[cfg(target_os = "windows")]
struct LoginApp {
    login_url: String,
    proxy: winit::event_loop::EventLoopProxy<LoginSignal>,
    window: Option<Window>,
    webview: Option<WebView>,
    pending_ticket_cookie: Option<String>,
    cookie_header: Option<String>,
    saw_login_ticket_redirect: bool,
    saw_login_success_page: bool,
    warned_missing_ticket: bool,
    error: Option<String>,
}

#[cfg(target_os = "windows")]
impl LoginApp {
    fn new(login_url: String, proxy: winit::event_loop::EventLoopProxy<LoginSignal>) -> Self {
        Self {
            login_url,
            proxy,
            window: None,
            webview: None,
            pending_ticket_cookie: None,
            cookie_header: None,
            saw_login_ticket_redirect: false,
            saw_login_success_page: false,
            warned_missing_ticket: false,
            error: None,
        }
    }

    fn observe_url(&mut self, url: &str) {
        if is_login_ticket_redirect(url) && !self.saw_login_ticket_redirect {
            self.saw_login_ticket_redirect = true;
        }

        if !self.saw_login_success_page
            && (is_login_success_page(url)
                || (self.saw_login_ticket_redirect && is_post_login_url(url)))
        {
            self.saw_login_success_page = true;
        }
    }

    fn check_ticket_cookie(&mut self, event_loop: &ActiveEventLoop) {
        if self.cookie_header.is_some() {
            return;
        }

        self.capture_ticket_cookie();
        self.finish_login_if_ready(event_loop);
    }

    fn capture_ticket_cookie(&mut self) {
        if self.pending_ticket_cookie.is_some() {
            return;
        }
        let Some(webview) = self.webview.as_ref() else {
            return;
        };

        match self.find_ticket_cookie(webview) {
            Ok(Some(ticket_cookie)) => {
                self.pending_ticket_cookie = Some(ticket_cookie);
                log_info("client", "ticket cookie found; waiting for login");
            }
            Ok(None) => {
                if self.saw_login_success_page && !self.warned_missing_ticket {
                    self.warned_missing_ticket = true;
                    log_warn("client", "login complete, waiting for ticket cookie");
                }
            }
            Err(err) => {
                log_warn("client", format!("failed to inspect WebVPN cookies: {err}"));
            }
        }
    }

    fn finish_login_if_ready(&mut self, event_loop: &ActiveEventLoop) {
        if !self.saw_login_success_page {
            return;
        }

        let Some(ticket_cookie) = self.pending_ticket_cookie.take() else {
            return;
        };

        self.cookie_header = Some(ticket_cookie);
        self.drop_login_window();
        event_loop.exit();
    }

    fn find_ticket_cookie(&self, webview: &WebView) -> wry::Result<Option<String>> {
        find_webvpn_ticket_cookie(webview)
    }

    fn drop_login_window(&mut self) {
        if let Some(window) = self.window.as_ref() {
            window.set_visible(false);
        }

        self.webview.take();

        if let Some(window) = self.window.as_ref() {
            destroy_native_window(window);
        }

        self.window.take();
    }

    fn resize_webview_to_window(&self) {
        let Some(window) = self.window.as_ref() else {
            return;
        };

        self.resize_webview(window.inner_size());
    }

    fn resize_webview(&self, size: PhysicalSize<u32>) {
        let Some(window) = self.window.as_ref() else {
            return;
        };
        let Some(webview) = self.webview.as_ref() else {
            return;
        };

        let size = size.to_logical::<u32>(window.scale_factor());
        if let Err(err) = webview.set_bounds(Rect {
            position: LogicalPosition::new(0, 0).into(),
            size: LogicalSize::new(size.width, size.height).into(),
        }) {
            log_warn("client", format!("failed to resize WebView: {err}"));
        }
    }
}

#[cfg(target_os = "windows")]
impl ApplicationHandler<LoginSignal> for LoginApp {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.window.is_some() {
            return;
        }

        let mut attributes = Window::default_attributes();
        attributes.title = "towc WebVPN Login".to_string();
        attributes.inner_size = Some(LogicalSize::new(1100, 800).into());

        let window = match event_loop.create_window(attributes) {
            Ok(window) => window,
            Err(err) => {
                self.error = Some(format!("failed to create login window: {err}"));
                event_loop.exit();
                return;
            }
        };

        let proxy = self.proxy.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                if proxy.send_event(LoginSignal::Tick).is_err() {
                    break;
                }
            }
        });

        let proxy = self.proxy.clone();
        let nav_proxy = self.proxy.clone();
        let webview = match WebViewBuilder::new()
            .with_incognito(true)
            .with_url(&self.login_url)
            .with_navigation_handler(move |url| {
                let _ = nav_proxy.send_event(LoginSignal::Navigation(url));
                true
            })
            .with_on_page_load_handler(move |event, _url| {
                if matches!(event, PageLoadEvent::Finished) {
                    let _ = proxy.send_event(LoginSignal::PageLoaded(_url));
                }
            })
            .build_as_child(&window)
        {
            Ok(webview) => webview,
            Err(err) => {
                self.error = Some(format!("failed to create WebView: {err}"));
                event_loop.exit();
                return;
            }
        };

        self.window = Some(window);
        self.webview = Some(webview);
        self.resize_webview_to_window();
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, event: LoginSignal) {
        match event {
            LoginSignal::Navigation(url) | LoginSignal::PageLoaded(url) => {
                self.observe_url(&url);
                self.check_ticket_cookie(event_loop);
            }
            LoginSignal::Tick => self.check_ticket_cookie(event_loop),
        }
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        _window_id: WindowId,
        event: WindowEvent,
    ) {
        match event {
            WindowEvent::Resized(size) => {
                self.resize_webview(size);
            }
            WindowEvent::CloseRequested => {
                if self.cookie_header.is_none() && self.error.is_none() {
                    self.error =
                        Some("login canceled: window closed before login completed".into());
                }
                self.drop_login_window();
                event_loop.exit();
            }
            _ => {}
        }
    }

    fn exiting(&mut self, _event_loop: &ActiveEventLoop) {
        self.drop_login_window();
    }
}

#[cfg(target_os = "windows")]
fn find_webvpn_ticket_cookie(webview: &WebView) -> wry::Result<Option<String>> {
    let cookies = webview.cookies_for_url(WEBVPN_LOGIN_URL)?;
    Ok(cookies
        .into_iter()
        .find(|cookie| cookie.name() == WEBVPN_TICKET_COOKIE_NAME)
        .map(|cookie| format!("{}={}", cookie.name(), cookie.value())))
}

#[cfg(target_os = "windows")]
fn is_login_ticket_redirect(url: &str) -> bool {
    url.contains("ticket=ST-") || (url.contains("/cas/login") && url.contains("code="))
}

#[cfg(target_os = "windows")]
fn is_login_success_page(url: &str) -> bool {
    url.contains("/personal-center")
}

#[cfg(target_os = "windows")]
fn is_post_login_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    lower.starts_with("https://webvpn.szut.edu.cn/")
        && !lower.contains("/login")
        && !lower.contains("/cas/login")
        && !lower.contains("/connect/qrconnect")
        && !lower.contains("ticket=st-")
}

#[cfg(target_os = "windows")]
fn destroy_native_window(window: &Window) {
    let Ok(handle) = window.window_handle() else {
        return;
    };

    let RawWindowHandle::Win32(handle) = handle.as_raw() else {
        return;
    };

    let hwnd = HWND(handle.hwnd.get() as _);
    unsafe {
        let _ = ShowWindow(hwnd, SW_HIDE);
        let _ = DestroyWindow(hwnd);
    }
}
