use anyhow::{Context, Result};
use futures_util::StreamExt;
use image::GrayImage;
use reqwest::cookie::CookieStore;
use reqwest::header::{ORIGIN, REFERER, USER_AGENT};
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

const WEBVPN_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/login";
const WEBVPN_TICKET_COOKIE_NAME: &str = "wengine_vpn_ticketwebvpn_szut_edu_cn";
const WEBVPN_CAS_HASH: &str = "77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d";
const WEBVPN_CAS_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/https/77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d/cas/login?service=https%3A%2F%2Fwebvpn.szut.edu.cn%2Flogin%3Fcas_login%3Dtrue";
const WEBVPN_WECHAT_HASH: &str =
    "77726476706e69737468656265737421ffe7449269276d59660187e289446d36a8d6";
const WECHAT_APP_ID: &str = "wx16c67d169e7a9290";
const WECHAT_REDIRECT_URI: &str = "https://cas.szut.edu.cn/cas/login?service=https%3A%2F%2Fwebvpn.szut.edu.cn%2Flogin%3Fcas_login%3Dtrue&client_name=WeiXinClient";
const BROWSER_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0";
const WEBVPN_FINGERPRINT: &str = "5a0b00fe6ae8277a4bfadd4e103f6e1c";
const WEBVPN_READY_ATTEMPTS: usize = 6;
const WEBVPN_READY_SETTLE_MS: u64 = 700;
const WEBVPN_READY_TIMEOUT_MS: u64 = 900;
const CAS_LOGIN_ATTEMPTS: usize = 2;
const CAS_LOGIN_RETRY_SETTLE_MS: u64 = 1500;
const WECHAT_POLL_ATTEMPTS: usize = 180;
const WECHAT_POLL_TIMEOUT_SECS: u64 = 35;
const WECHAT_POLL_SETTLE_MS: u64 = 1800;
const WECHAT_QR_MODULES: u32 = 41;
const WECHAT_QR_BORDER_PX: u32 = 30;
const WECHAT_QR_MODULE_PX: u32 = 10;
const TERMINAL_QR_QUIET_ZONE: u32 = 4;
const QR_DARK_THRESHOLD: u8 = 160;

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

struct WechatPollStatus {
    errcode: u16,
    code: String,
}

enum WechatQrPollResult {
    Confirmed(String),
    Expired,
}

enum QrTheme {
    Aurora,
    Expired,
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
            header: login_with_wechat_qr().await?,
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

fn build_login_client(cookie_jar: Arc<reqwest::cookie::Jar>) -> Result<Client> {
    Client::builder()
        .cookie_provider(cookie_jar)
        .user_agent(BROWSER_USER_AGENT)
        .build()
        .context("failed to build WebVPN login HTTP client")
}

async fn login_with_wechat_qr() -> Result<String> {
    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    let client = build_login_client(Arc::clone(&cookie_jar))?;
    let login_entry = initialize_webvpn_ticket_cookie(&client, &cookie_jar).await?;

    log_info("client", "fetching WeChat QR login page");
    let qr_page_url = wechat_qrconnect_url()?;
    let qr_page = client
        .get(qr_page_url)
        .send()
        .await
        .context("failed to open WeChat QR login page")?
        .error_for_status()
        .context("WeChat QR login page request failed")?
        .text()
        .await
        .context("failed to read WeChat QR login page")?;

    let uuid = extract_wechat_uuid(&qr_page).context("failed to find WeChat QR uuid")?;
    let qrcode_url = extract_wechat_qrcode_url(&qr_page, &uuid)?;
    let qrcode = client
        .get(qrcode_url)
        .send()
        .await
        .context("failed to fetch WeChat QR image")?
        .error_for_status()
        .context("WeChat QR image request failed")?
        .bytes()
        .await
        .context("failed to read WeChat QR image")?;

    log_info(
        "client",
        "scan the QR code below with WeChat and confirm login",
    );

    let qr_modules = wechat_qr_modules_from_image(&qrcode)?;
    print_wechat_qr_modules(&qr_modules, QrTheme::Aurora)?;

    let code = match poll_wechat_qr_code(&client, &uuid).await? {
        WechatQrPollResult::Confirmed(code) => code,
        WechatQrPollResult::Expired => {
            log_warn(
                "client",
                "WeChat QR code expired; redrawing the expired QR code below",
            );
            print_wechat_qr_modules(&qr_modules, QrTheme::Expired)?;
            anyhow::bail!("WeChat QR code expired; please restart towc and scan again");
        }
    };
    log_info(
        "client",
        "WeChat confirmed login; completing WebVPN authentication",
    );
    let response = client
        .get(wechat_cas_callback_url(&code)?)
        .header(USER_AGENT, BROWSER_USER_AGENT)
        .send()
        .await
        .context("failed to open CAS WeChat callback")?
        .error_for_status()
        .context("CAS WeChat callback request failed")?;
    let final_url = response.url().to_string();

    let activated_ticket =
        activate_webvpn_fingerprint_if_needed(&client, &cookie_jar, &final_url).await?;
    let post_login_ticket = ticket_cookie_from_jar(&cookie_jar);
    let cookie = activated_ticket
        .or(post_login_ticket)
        .or(login_entry.ticket_cookie)
        .context("WeChat login completed but WebVPN ticket cookie was not found")?;

    log_success("client", "WeChat QR login completed");
    Ok(cookie)
}

async fn login_with_verification_code(login: VerificationLogin) -> Result<String> {
    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    let client = build_login_client(Arc::clone(&cookie_jar))?;

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

fn wechat_qrconnect_url() -> Result<String> {
    let mut url = Url::parse(&format!(
        "https://webvpn.szut.edu.cn/https/{WEBVPN_WECHAT_HASH}/connect/qrconnect"
    ))
    .context("failed to build WeChat QR login URL")?;
    url.query_pairs_mut()
        .append_pair("appid", WECHAT_APP_ID)
        .append_pair("redirect_uri", WECHAT_REDIRECT_URI)
        .append_pair("response_type", "code")
        .append_pair("self_redirect", "false")
        .append_pair("scope", "snsapi_login");
    Ok(url.into())
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

fn wechat_cas_callback_url(code: &str) -> Result<String> {
    let mut url = Url::parse(WECHAT_REDIRECT_URI).context("failed to build CAS callback URL")?;
    url.query_pairs_mut()
        .append_pair("code", code)
        .append_pair("state", "");
    Ok(url.into())
}

fn wechat_poll_url(uuid: &str, last: Option<u16>) -> Result<String> {
    let mut url = Url::parse(&format!(
        "https://webvpn.szut.edu.cn/https/{WEBVPN_WECHAT_HASH}/connect/l/qrconnect"
    ))
    .context("failed to build WeChat QR polling URL")?;
    url.query_pairs_mut().append_pair("uuid", uuid);
    if let Some(last) = last {
        url.query_pairs_mut().append_pair("last", &last.to_string());
    }
    Ok(url.into())
}

async fn poll_wechat_qr_code(client: &Client, uuid: &str) -> Result<WechatQrPollResult> {
    let mut last = None::<u16>;
    for _ in 1..=WECHAT_POLL_ATTEMPTS {
        let body = client
            .get(wechat_poll_url(uuid, last)?)
            .timeout(Duration::from_secs(WECHAT_POLL_TIMEOUT_SECS))
            .send()
            .await
            .context("failed to poll WeChat QR login status")?
            .error_for_status()
            .context("WeChat QR login status request failed")?
            .text()
            .await
            .context("failed to read WeChat QR login status")?;

        let status = parse_wechat_poll_status(&body)
            .with_context(|| format!("failed to parse WeChat QR login status: {body}"))?;
        last = Some(status.errcode);

        match status.errcode {
            405 if !status.code.is_empty() => {
                return Ok(WechatQrPollResult::Confirmed(status.code));
            }
            405 => anyhow::bail!("WeChat confirmed login but did not return a code"),
            404 => {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            408 => {
                tokio::time::sleep(Duration::from_millis(WECHAT_POLL_SETTLE_MS)).await;
            }
            403 => anyhow::bail!("WeChat QR login was canceled"),
            402 => return Ok(WechatQrPollResult::Expired),
            500 => {
                log_warn("client", "WeChat QR polling returned 500, retrying");
                tokio::time::sleep(Duration::from_millis(WECHAT_POLL_SETTLE_MS)).await;
            }
            other => {
                log_warn(
                    "client",
                    format!("unexpected WeChat QR status {other}, retrying"),
                );
                tokio::time::sleep(Duration::from_millis(WECHAT_POLL_SETTLE_MS)).await;
            }
        }
    }

    anyhow::bail!("timed out waiting for WeChat QR login")
}

fn wechat_qr_modules_from_image(bytes: &[u8]) -> Result<Vec<bool>> {
    let image = image::load_from_memory(bytes)
        .context("failed to decode WeChat QR image")?
        .to_luma8();
    sample_wechat_qr_modules(&image).context("failed to sample WeChat QR modules")
}

fn print_wechat_qr_modules(modules: &[bool], theme: QrTheme) -> Result<()> {
    let output_size = WECHAT_QR_MODULES + TERMINAL_QR_QUIET_ZONE * 2;

    println!();
    for y in (0..output_size).step_by(2) {
        for x in 0..output_size {
            let top_dark = rendered_qr_module_dark(modules, x, y, output_size);
            let bottom_dark =
                y + 1 < output_size && rendered_qr_module_dark(modules, x, y + 1, output_size);
            print_qr_half_block(top_dark, bottom_dark, x, y, &theme);
        }
        println!("\x1b[0m");
    }
    println!("\x1b[0m");
    io::stdout().flush().context("failed to flush QR code")?;
    Ok(())
}

fn sample_wechat_qr_modules(image: &GrayImage) -> Option<Vec<bool>> {
    let required_size = WECHAT_QR_BORDER_PX * 2 + WECHAT_QR_MODULES * WECHAT_QR_MODULE_PX;
    if image.width() < required_size || image.height() < required_size {
        return None;
    }

    let mut modules = Vec::with_capacity((WECHAT_QR_MODULES * WECHAT_QR_MODULES) as usize);

    for row in 0..WECHAT_QR_MODULES {
        for col in 0..WECHAT_QR_MODULES {
            let x = WECHAT_QR_BORDER_PX + col * WECHAT_QR_MODULE_PX + WECHAT_QR_MODULE_PX / 2;
            let y = WECHAT_QR_BORDER_PX + row * WECHAT_QR_MODULE_PX + WECHAT_QR_MODULE_PX / 2;
            modules.push(image.get_pixel(x, y)[0] < QR_DARK_THRESHOLD);
        }
    }

    Some(modules)
}

fn rendered_qr_module_dark(modules: &[bool], x: u32, y: u32, output_size: u32) -> bool {
    if x < TERMINAL_QR_QUIET_ZONE
        || y < TERMINAL_QR_QUIET_ZONE
        || x >= output_size - TERMINAL_QR_QUIET_ZONE
        || y >= output_size - TERMINAL_QR_QUIET_ZONE
    {
        return false;
    }

    let x = x - TERMINAL_QR_QUIET_ZONE;
    let y = y - TERMINAL_QR_QUIET_ZONE;
    let index = (y * WECHAT_QR_MODULES + x) as usize;
    modules.get(index).copied().unwrap_or(false)
}

#[derive(Clone, Copy)]
struct Rgb {
    red: u8,
    green: u8,
    blue: u8,
}

impl Rgb {
    const fn new(red: u8, green: u8, blue: u8) -> Self {
        Self { red, green, blue }
    }
}

fn print_qr_half_block(top_dark: bool, bottom_dark: bool, x: u32, y: u32, theme: &QrTheme) {
    let foreground = qr_module_color(top_dark, x, y, theme);
    let background = qr_module_color(bottom_dark, x, y + 1, theme);

    print!(
        "\x1b[38;2;{};{};{};48;2;{};{};{}m\u{2580}",
        foreground.red,
        foreground.green,
        foreground.blue,
        background.red,
        background.green,
        background.blue
    );
}

fn qr_module_color(dark: bool, x: u32, y: u32, theme: &QrTheme) -> Rgb {
    if !dark {
        return Rgb::new(250, 248, 239);
    }

    if matches!(theme, QrTheme::Expired) {
        return Rgb::new(186, 28, 28);
    }

    const AURORA: [Rgb; 7] = [
        Rgb::new(239, 90, 91),
        Rgb::new(232, 119, 49),
        Rgb::new(225, 181, 64),
        Rgb::new(72, 164, 89),
        Rgb::new(24, 164, 174),
        Rgb::new(54, 111, 199),
        Rgb::new(239, 90, 91),
    ];

    let diagonal = (x + y).saturating_sub(TERMINAL_QR_QUIET_ZONE * 2);
    let span = (WECHAT_QR_MODULES - 1) * 2;
    let scaled = diagonal * ((AURORA.len() - 1) as u32) * 256 / span.max(1);
    let index = (scaled / 256).min((AURORA.len() - 1) as u32) as usize;
    let next_index = (index + 1).min(AURORA.len() - 1);
    let amount = smoothstep_byte((scaled % 256) as u8);
    let hue_shift = ((x * 13 + y * 7 + diagonal * 5) % 17) as i16 - 8;
    let shifted_amount = offset_byte(amount, hue_shift);
    let color = blend_rgb(AURORA[index], AURORA[next_index], shifted_amount);
    let shimmer = 82 + ((x * 17 + y * 11 + diagonal * 3) % 19) as u8;

    scale_rgb(color, shimmer)
}

fn blend_rgb(start: Rgb, end: Rgb, amount: u8) -> Rgb {
    let amount = u16::from(amount);
    let inverse = 255 - amount;

    Rgb::new(
        (((u16::from(start.red) * inverse) + (u16::from(end.red) * amount)) / 255) as u8,
        (((u16::from(start.green) * inverse) + (u16::from(end.green) * amount)) / 255) as u8,
        (((u16::from(start.blue) * inverse) + (u16::from(end.blue) * amount)) / 255) as u8,
    )
}

fn smoothstep_byte(value: u8) -> u8 {
    let value = u16::from(value);
    ((value * value * (765 - 2 * value)) / (255 * 255)) as u8
}

fn offset_byte(value: u8, offset: i16) -> u8 {
    (i16::from(value) + offset).clamp(0, 255) as u8
}

fn scale_rgb(color: Rgb, percent: u8) -> Rgb {
    let percent = u16::from(percent);

    Rgb::new(
        ((u16::from(color.red) * percent) / 100) as u8,
        ((u16::from(color.green) * percent) / 100) as u8,
        ((u16::from(color.blue) * percent) / 100) as u8,
    )
}

fn extract_wechat_uuid(html: &str) -> Option<String> {
    extract_js_string_assignment(html, "G")
        .or_else(|| extract_token_after(html, "uuid="))
        .or_else(|| extract_token_after(html, "/connect/qrcode/"))
}

fn extract_wechat_qrcode_url(html: &str, uuid: &str) -> Result<String> {
    if let Some(src) = html.split('<').find_map(|fragment| {
        let fragment = fragment.trim_start();
        if !fragment.starts_with("img") || !fragment.contains("/connect/qrcode/") {
            return None;
        }
        attr_value(fragment, "src")
    }) {
        return absolute_webvpn_url(&src);
    }

    absolute_webvpn_url(&format!(
        "/https/{WEBVPN_WECHAT_HASH}/connect/qrcode/{uuid}?vpn-1"
    ))
}

fn absolute_webvpn_url(value: &str) -> Result<String> {
    if value.starts_with("https://") || value.starts_with("http://") {
        return Ok(value.to_string());
    }
    if value.starts_with("//") {
        return Ok(format!("https:{value}"));
    }
    if value.starts_with('/') {
        return Ok(format!("https://webvpn.szut.edu.cn{value}"));
    }

    Url::parse("https://webvpn.szut.edu.cn/")
        .and_then(|base| base.join(value))
        .map(|url| url.into())
        .context("failed to build absolute WebVPN URL")
}

fn parse_wechat_poll_status(body: &str) -> Option<WechatPollStatus> {
    Some(WechatPollStatus {
        errcode: extract_js_number_assignment(body, "wx_errcode")?,
        code: extract_js_string_assignment(body, "wx_code").unwrap_or_default(),
    })
}

fn extract_js_number_assignment(body: &str, name: &str) -> Option<u16> {
    let value = assignment_value(body, name)?;
    let digits: String = value
        .chars()
        .skip_while(|ch| ch.is_ascii_whitespace())
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

fn extract_js_string_assignment(body: &str, name: &str) -> Option<String> {
    let value = assignment_value(body, name)?;
    let mut chars = value.trim_start().chars();
    let quote = chars.next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }

    let start = value.find(quote)? + quote.len_utf8();
    let rest = &value[start..];
    let end = rest.find(quote)?;
    Some(rest[..end].to_string())
}

fn assignment_value<'a>(body: &'a str, name: &str) -> Option<&'a str> {
    let mut offset = 0;
    while let Some(relative_index) = body[offset..].find(name) {
        let index = offset + relative_index;
        let after_name = &body[index + name.len()..];
        if let Some(after_equals) = after_name.trim_start().strip_prefix('=') {
            return Some(after_equals.trim_start());
        }
        offset = index + name.len();
    }

    None
}

fn extract_token_after(body: &str, marker: &str) -> Option<String> {
    let rest = body.split_once(marker)?.1;
    let token: String = rest
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
        .collect();
    if token.is_empty() { None } else { Some(token) }
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
    let activation_url = webvpn_fingerprint_activation_url(final_url)?;
    let response = client
        .get(activation_url)
        .header(REFERER, final_url)
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

fn webvpn_fingerprint_activation_url(final_url: &str) -> Result<String> {
    let source = Url::parse(final_url).context("failed to parse WebVPN fingerprint URL")?;
    let mut url = Url::parse("https://webvpn.szut.edu.cn/set-fingerprint")
        .context("failed to build WebVPN fingerprint activation URL")?;
    {
        let mut query = url.query_pairs_mut();
        for (name, value) in source.query_pairs() {
            if name != "fingerprint" {
                query.append_pair(&name, &value);
            }
        }
        query.append_pair("fingerprint", WEBVPN_FINGERPRINT);
    }
    Ok(url.into())
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
    eprintln!("       when --cookie and --login are omitted, towc uses terminal WeChat QR login");
    eprintln!(
        "       --login sends a verification code by SMS for numeric values, or email when the value contains @"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_wechat_poll_status_from_vpn_eval_wrapper() {
        let body = "vpn_eval((function(){\nwindow.wx_errcode=408;window.wx_code='';\n\n}\n).toString().slice(12, -2),\"\");";
        let status = parse_wechat_poll_status(body).unwrap();

        assert_eq!(status.errcode, 408);
        assert_eq!(status.code, "");
    }

    #[test]
    fn parses_wechat_poll_status_with_code() {
        let body = "window.wx_errcode=405;window.wx_code='0813NDFa1etq0M0SGBGa1X6UNk33NDFz';";
        let status = parse_wechat_poll_status(body).unwrap();

        assert_eq!(status.errcode, 405);
        assert_eq!(status.code, "0813NDFa1etq0M0SGBGa1X6UNk33NDFz");
    }

    #[test]
    fn extracts_wechat_uuid_and_qrcode_url() {
        let html = r#"
            <img class="js_qrcode_img web_qrcode_img" src="/https/77726476706e69737468656265737421ffe7449269276d59660187e289446d36a8d6/connect/qrcode/041mYvVw0hEq100b?vpn-1"/>
            <script>var U="https://long.open.weixin.qq.com",G="041mYvVw0hEq100b";</script>
        "#;

        assert_eq!(extract_wechat_uuid(html).unwrap(), "041mYvVw0hEq100b");
        assert_eq!(
            extract_wechat_qrcode_url(html, "041mYvVw0hEq100b").unwrap(),
            "https://webvpn.szut.edu.cn/https/77726476706e69737468656265737421ffe7449269276d59660187e289446d36a8d6/connect/qrcode/041mYvVw0hEq100b?vpn-1"
        );
    }

    #[test]
    fn module_sampling_preserves_outer_black_border() {
        let module_size = WECHAT_QR_MODULE_PX;
        let border = WECHAT_QR_BORDER_PX;
        let image_size = border * 2 + WECHAT_QR_MODULES * module_size;
        let mut image = GrayImage::from_pixel(image_size, image_size, image::Luma([255]));
        for row in 0..WECHAT_QR_MODULES {
            for col in 0..WECHAT_QR_MODULES {
                if row != 0
                    && row != WECHAT_QR_MODULES - 1
                    && col != 0
                    && col != WECHAT_QR_MODULES - 1
                {
                    continue;
                }

                let start_x = border + col * module_size;
                let start_y = border + row * module_size;
                for y in start_y..start_y + module_size {
                    for x in start_x..start_x + module_size {
                        image.put_pixel(x, y, image::Luma([0]));
                    }
                }
            }
        }
        let modules = sample_wechat_qr_modules(&image).unwrap();
        let output_size = WECHAT_QR_MODULES + TERMINAL_QR_QUIET_ZONE * 2;

        assert!(!rendered_qr_module_dark(&modules, 0, 0, output_size));
        assert!(rendered_qr_module_dark(
            &modules,
            TERMINAL_QR_QUIET_ZONE,
            TERMINAL_QR_QUIET_ZONE,
            output_size
        ));
        assert!(rendered_qr_module_dark(
            &modules,
            output_size - TERMINAL_QR_QUIET_ZONE - 1,
            TERMINAL_QR_QUIET_ZONE,
            output_size
        ));
        assert!(rendered_qr_module_dark(
            &modules,
            TERMINAL_QR_QUIET_ZONE,
            output_size - TERMINAL_QR_QUIET_ZONE - 1,
            output_size
        ));
        assert!(!rendered_qr_module_dark(
            &modules,
            TERMINAL_QR_QUIET_ZONE + 1,
            TERMINAL_QR_QUIET_ZONE + 1,
            output_size
        ));
    }
}
