use anyhow::{Context, Result, bail};
use num_bigint::BigUint;
use reqwest::cookie::CookieStore;
use reqwest::header::{ORIGIN, REFERER};
use reqwest::{Client, Url};
use serde::Deserialize;
use std::fs;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::storage::{atomic_write, data_file};

const WEBVPN_ROOT: &str = "https://webvpn.szut.edu.cn/";
const TICKET_NAME: &str = "wengine_vpn_ticketwebvpn_szut_edu_cn";
const CAS_HASH: &str = "77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d";
const SERVICE: &str = "https://webvpn.szut.edu.cn/login?cas_login=true";
const CAS_LOGIN: &str = "https://webvpn.szut.edu.cn/https/77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d/cas/login?service=https%3A%2F%2Fwebvpn.szut.edu.cn%2Flogin%3Fcas_login%3Dtrue";
const FINGERPRINT: &str = "5a0b00fe6ae8277a4bfadd4e103f6e1c";
const WECHAT_APP_ID: &str = "wx16c67d169e7a9290";
const COOKIE_FILE: &str = "webvpn.cookie";
const USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36";
const RSA_CHUNK_SIZE: usize = 62;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginPreference {
    Wechat,
    Mobile(String),
    Email(String),
}

impl LoginPreference {
    pub fn from_identity(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.chars().all(|character| character.is_ascii_digit()) && !value.is_empty() {
            Ok(Self::Mobile(value.to_string()))
        } else if value.contains('@') && !value.chars().any(char::is_whitespace) {
            Ok(Self::Email(value.to_string()))
        } else {
            bail!("登录值必须是手机号或邮箱地址");
        }
    }
}

/// 认证过程与终端/GUI 之间的最小交互接口。
pub trait AuthPrompt: Send + Sync {
    fn status(&self, message: &str);
    fn show_qr(&self, jpeg_or_png: Vec<u8>) -> Result<()>;
    fn request_code(&self, label: &str) -> Result<String>;
}

#[derive(Clone)]
pub struct SessionCookie(pub(crate) Arc<Mutex<String>>);

impl SessionCookie {
    pub fn snapshot(&self) -> String {
        self.0.lock().expect("Cookie 锁中毒").clone()
    }

    pub(crate) fn replace(&self, cookie: String) {
        *self.0.lock().expect("Cookie 锁中毒") = cookie;
    }
}

#[derive(Deserialize)]
struct PublicKeyResponse {
    modulus: String,
    exponent: String,
}

pub async fn login_or_restore(
    prompt: Arc<dyn AuthPrompt>,
    preference: LoginPreference,
) -> Result<SessionCookie> {
    if let Some(cookie) = read_cached_ticket()
        && validate_cached_ticket(&cookie).await.unwrap_or(false)
    {
        prompt.status("已复用有效的 WebVPN 登录缓存");
        return Ok(SessionCookie(Arc::new(Mutex::new(cookie))));
    }

    prompt.status("需要登录 WebVPN");
    let cookie = match preference {
        LoginPreference::Wechat => login_wechat(Arc::clone(&prompt)).await?,
        LoginPreference::Mobile(mobile) => {
            login_verification(Arc::clone(&prompt), mobile, true).await?
        }
        LoginPreference::Email(email) => {
            login_verification(Arc::clone(&prompt), email, false).await?
        }
    };
    write_cached_ticket(&cookie);
    Ok(SessionCookie(Arc::new(Mutex::new(cookie))))
}

fn login_client(jar: Arc<reqwest::cookie::Jar>) -> Result<Client> {
    Client::builder()
        .cookie_provider(jar)
        .user_agent(USER_AGENT)
        .redirect(reqwest::redirect::Policy::limited(12))
        .build()
        .context("无法创建 WebVPN 登录客户端")
}

async fn fresh_login_client() -> Result<(Client, Arc<reqwest::cookie::Jar>, String)> {
    let jar = Arc::new(reqwest::cookie::Jar::default());
    let client = login_client(Arc::clone(&jar))?;
    let response = client
        .get(CAS_LOGIN)
        .send()
        .await
        .context("无法打开 CAS 登录页")?
        .error_for_status()
        .context("CAS 登录页返回错误")?;
    let _ = response.bytes().await;
    client
        .get(format!(
            "{WEBVPN_ROOT}set-fingerprint?fingerprint={FINGERPRINT}"
        ))
        .header(REFERER, format!("{WEBVPN_ROOT}fingerprint"))
        .send()
        .await
        .context("无法激活 WebVPN 指纹")?
        .error_for_status()
        .context("WebVPN 指纹激活失败")?;
    let html = client
        .get(CAS_LOGIN)
        .send()
        .await
        .context("指纹激活后无法重开 CAS 登录页")?
        .error_for_status()?
        .text()
        .await?;
    Ok((client, jar, html))
}

async fn login_wechat(prompt: Arc<dyn AuthPrompt>) -> Result<String> {
    let (client, jar, _) = fresh_login_client().await?;
    prompt.status("正在获取微信二维码");
    let state = format!("towc{}", unix_millis());
    let redirect = format!(
        "https://cas.szut.edu.cn/cas/login?service={}&client_name=WeiXinClient",
        url_encode(SERVICE)
    );
    let mut qr_page_url = Url::parse("https://open.weixin.qq.com/connect/qrconnect")?;
    qr_page_url
        .query_pairs_mut()
        .append_pair("appid", WECHAT_APP_ID)
        .append_pair("redirect_uri", &redirect)
        .append_pair("response_type", "code")
        .append_pair("scope", "snsapi_login")
        .append_pair("state", &state);
    let page = client
        .get(qr_page_url)
        .send()
        .await
        .context("无法访问微信二维码页面")?
        .error_for_status()?
        .text()
        .await?;
    let uuid = extract_wechat_uuid(&page).context("微信页面中没有二维码 UUID")?;
    let image = client
        .get(format!("https://open.weixin.qq.com/connect/qrcode/{uuid}"))
        .send()
        .await
        .context("无法下载微信二维码")?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec();
    prompt.show_qr(image)?;
    prompt.status("请用微信扫码并在手机上确认");

    let code = poll_wechat(&client, &uuid, &prompt).await?;
    prompt.status("微信已确认，正在激活 WebVPN ticket");
    let mut callback = Url::parse(&redirect)?;
    callback
        .query_pairs_mut()
        .append_pair("code", &code)
        .append_pair("state", &state);
    let response = client
        .get(callback)
        .send()
        .await
        .context("CAS 微信回调失败")?
        .error_for_status()?;
    ensure_not_login_page(response.url().as_str())?;
    ticket_from_jar(&jar).context("微信登录完成但没有获得 WebVPN ticket")
}

async fn poll_wechat(client: &Client, uuid: &str, prompt: &Arc<dyn AuthPrompt>) -> Result<String> {
    let mut last = None::<u16>;
    for _ in 0..180 {
        let mut url = Url::parse("https://lp.open.weixin.qq.com/connect/l/qrconnect")?;
        url.query_pairs_mut()
            .append_pair("uuid", uuid)
            .append_pair("_", &unix_millis().to_string());
        if let Some(last) = last {
            url.query_pairs_mut().append_pair("last", &last.to_string());
        }
        let body = client
            .get(url)
            .timeout(Duration::from_secs(35))
            .send()
            .await
            .context("轮询微信扫码状态失败")?
            .error_for_status()?
            .text()
            .await?;
        let status = extract_js_number(&body, "wx_errcode")
            .with_context(|| format!("无法解析微信扫码状态: {}", redact_code(&body)))?;
        last = Some(status);
        match status {
            405 => {
                return extract_js_string(&body, "wx_code")
                    .filter(|code| !code.is_empty())
                    .context("微信确认后没有返回回调 code");
            }
            404 => {
                prompt.status("二维码已扫描，等待手机确认");
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            408 | 500 => tokio::time::sleep(Duration::from_millis(1800)).await,
            403 => bail!("微信扫码登录已取消"),
            402 => bail!("微信二维码已过期，请重新登录"),
            _ => tokio::time::sleep(Duration::from_millis(1800)).await,
        }
    }
    bail!("等待微信扫码超时")
}

async fn login_verification(
    prompt: Arc<dyn AuthPrompt>,
    username: String,
    mobile: bool,
) -> Result<String> {
    let (client, jar, mut html) = fresh_login_client().await?;
    let (path, key, label) = if mobile {
        ("v2/services/sedsms", "mobile", "手机")
    } else {
        ("v2/services/sendEmailYzm", "email", "邮箱")
    };
    prompt.status(&format!("正在发送{label}验证码"));
    let mut send_url = Url::parse(&cas_url(path))?;
    send_url.query_pairs_mut().append_pair(key, &username);
    let response = client
        .get(send_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let result = response.trim().trim_matches('"');
    match result {
        "success" => prompt.status("验证码已发送"),
        "valid" => prompt.status("已有未过期验证码，请直接使用"),
        "unbind" => bail!("该{label}未绑定学校账号"),
        other => bail!("验证码服务返回错误: {other}"),
    }

    let key = client
        .get(cas_url("v2/getPubKey"))
        .send()
        .await?
        .error_for_status()?
        .json::<PublicKeyResponse>()
        .await
        .context("无法解析 CAS RSA 公钥")?;
    let code = tokio::task::block_in_place(|| prompt.request_code(label))?;
    if code.trim().is_empty() {
        bail!("验证码不能为空");
    }
    let reversed: String = code.trim().chars().rev().collect();
    let encrypted = rsa_encrypt(&reversed, &key.modulus, &key.exponent)?;

    for attempt in 0..2 {
        let execution =
            extract_input_value(&html, "execution").context("CAS 登录页缺少 execution token")?;
        let response = client
            .post(CAS_LOGIN)
            .header(ORIGIN, "https://webvpn.szut.edu.cn")
            .header(REFERER, CAS_LOGIN)
            .form(&[
                ("username", username.as_str()),
                ("password", encrypted.as_str()),
                ("rememberMe", "true"),
                ("execution", execution.as_str()),
                ("_eventId", "submit"),
            ])
            .send()
            .await
            .context("提交 CAS 验证码登录失败")?
            .error_for_status()?;
        let final_url = response.url().to_string();
        html = response.text().await?;
        if !final_url.contains("/cas/login") || extract_input_value(&html, "execution").is_none() {
            ensure_not_login_page(&final_url)?;
            return ticket_from_jar(&jar).context("验证码登录完成但没有获得 WebVPN ticket");
        }
        if attempt == 0 {
            prompt.status("CAS 尚未接受验证码，使用新的 execution 重试一次");
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
    }
    bail!("CAS 未接受验证码，请检查验证码是否正确")
}

pub(crate) async fn refresh_ticket(cookie: &SessionCookie) -> Result<()> {
    let jar = Arc::new(reqwest::cookie::Jar::default());
    seed_jar(&jar, &cookie.snapshot());
    let client = login_client(Arc::clone(&jar))?;
    let mut url = Url::parse(&format!("{WEBVPN_ROOT}wengine-vpn/cookie"))?;
    url.query_pairs_mut()
        .append_pair("method", "get")
        .append_pair("host", "cas.szut.edu.cn")
        .append_pair("scheme", "https")
        .append_pair("path", "/personal-center")
        .append_pair("vpn_timestamp", &unix_millis().to_string());
    let response = client
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("Cookie 保活请求失败")?;
    ensure_not_login_page(response.url().as_str())?;
    response.error_for_status().context("Cookie 保活返回错误")?;
    let refreshed = ticket_from_jar(&jar).context("Cookie 保活后 ticket 消失")?;
    if refreshed != cookie.snapshot() {
        cookie.replace(refreshed.clone());
        write_cached_ticket(&refreshed);
    }
    Ok(())
}

async fn validate_cached_ticket(cookie: &str) -> Result<bool> {
    if !valid_ticket_format(cookie) {
        return Ok(false);
    }
    let session = SessionCookie(Arc::new(Mutex::new(cookie.to_string())));
    Ok(refresh_ticket(&session).await.is_ok())
}

fn ticket_from_jar(jar: &reqwest::cookie::Jar) -> Option<String> {
    let url = Url::parse(WEBVPN_ROOT).ok()?;
    let cookies = jar.cookies(&url)?.to_str().ok()?.to_string();
    cookies
        .split(';')
        .map(str::trim)
        .find(|cookie| valid_ticket_format(cookie))
        .map(str::to_string)
}

fn seed_jar(jar: &reqwest::cookie::Jar, cookie: &str) {
    let url = Url::parse(WEBVPN_ROOT).expect("固定 WebVPN URL 合法");
    jar.add_cookie_str(cookie, &url);
}

fn valid_ticket_format(cookie: &str) -> bool {
    let Some(value) = cookie.strip_prefix(&format!("{TICKET_NAME}=")) else {
        return false;
    };
    let Some(hex) = value.strip_prefix("wrdvpn1-") else {
        return false;
    };
    hex.len() == 32 && hex.chars().all(|character| character.is_ascii_hexdigit())
}

fn read_cached_ticket() -> Option<String> {
    let path = data_file(COOKIE_FILE)?;
    match fs::read_to_string(path) {
        Ok(value) if valid_ticket_format(value.trim()) => Some(value.trim().to_string()),
        Ok(_) => None,
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            tracing::warn!("读取 WebVPN Cookie 缓存失败，不影响重新登录: {error}");
            None
        }
    }
}

fn write_cached_ticket(cookie: &str) {
    let Some(path) = data_file(COOKIE_FILE) else {
        tracing::warn!("找不到 Cookie 缓存目录");
        return;
    };
    if let Err(error) = atomic_write(&path, format!("{cookie}\n").as_bytes()) {
        tracing::warn!("写入 Cookie 缓存失败，不影响当前会话: {error:#}");
    }
}

fn cas_url(path: &str) -> String {
    format!(
        "https://webvpn.szut.edu.cn/https/{CAS_HASH}/cas/{}",
        path.trim_start_matches('/')
    )
}

fn extract_wechat_uuid(html: &str) -> Option<String> {
    extract_js_string(html, "G")
        .or_else(|| extract_token_after(html, "/connect/qrcode/"))
        .or_else(|| extract_token_after(html, "uuid="))
}

fn assignment_value<'a>(body: &'a str, name: &str) -> Option<&'a str> {
    let mut offset = 0;
    while let Some(relative) = body[offset..].find(name) {
        let index = offset + relative;
        let after = &body[index + name.len()..];
        if let Some(value) = after.trim_start().strip_prefix('=') {
            return Some(value.trim_start());
        }
        offset = index + name.len();
    }
    None
}

fn extract_js_number(body: &str, name: &str) -> Option<u16> {
    assignment_value(body, name)?
        .chars()
        .skip_while(|character| character.is_ascii_whitespace())
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

fn extract_js_string(body: &str, name: &str) -> Option<String> {
    let value = assignment_value(body, name)?.trim_start();
    let quote = value.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let rest = &value[quote.len_utf8()..];
    let end = rest.find(quote)?;
    Some(rest[..end].to_string())
}

fn extract_token_after(body: &str, marker: &str) -> Option<String> {
    let value = body.split_once(marker)?.1;
    let token: String = value
        .chars()
        .take_while(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .collect();
    (!token.is_empty()).then_some(token)
}

fn extract_input_value(html: &str, name: &str) -> Option<String> {
    html.split('<').find_map(|fragment| {
        let fragment = fragment.trim_start();
        if !fragment.starts_with("input") || attr(fragment, "name").as_deref() != Some(name) {
            return None;
        }
        attr(fragment, "value")
    })
}

fn attr(fragment: &str, name: &str) -> Option<String> {
    let mut rest = fragment;
    while let Some(index) = rest.find(name) {
        let after = &rest[index + name.len()..];
        let Some(value) = after.trim_start().strip_prefix('=') else {
            rest = after;
            continue;
        };
        let value = value.trim_start();
        let quote = value.chars().next()?;
        if quote != '\'' && quote != '"' {
            rest = &value[quote.len_utf8()..];
            continue;
        }
        let value = &value[quote.len_utf8()..];
        return Some(value[..value.find(quote)?].to_string());
    }
    None
}

fn rsa_encrypt(plain: &str, modulus_hex: &str, exponent_hex: &str) -> Result<String> {
    let modulus = BigUint::parse_bytes(modulus_hex.as_bytes(), 16).context("无效 RSA modulus")?;
    let exponent =
        BigUint::parse_bytes(exponent_hex.as_bytes(), 16).context("无效 RSA exponent")?;
    let mut codes: Vec<u16> = plain.encode_utf16().collect();
    codes.resize(codes.len().div_ceil(RSA_CHUNK_SIZE) * RSA_CHUNK_SIZE, 0);
    let mut encrypted = Vec::new();
    for chunk in codes.chunks(RSA_CHUNK_SIZE) {
        let mut bytes = Vec::with_capacity(RSA_CHUNK_SIZE * 2);
        for pair in chunk.chunks(2) {
            let high = pair.get(1).copied().unwrap_or_default();
            let digit = u32::from(pair[0]) | (u32::from(high) << 8);
            bytes.push((digit & 0xff) as u8);
            bytes.push(((digit >> 8) & 0xff) as u8);
        }
        encrypted.push(
            BigUint::from_bytes_le(&bytes)
                .modpow(&exponent, &modulus)
                .to_str_radix(16),
        );
    }
    Ok(encrypted.join(" "))
}

fn ensure_not_login_page(url: &str) -> Result<()> {
    if url.contains("webvpn.szut.edu.cn/login") || url.contains("logoutByIpChange=true") {
        bail!("WebVPN 登录未生效或来源 IP 已变化，请重新登录");
    }
    Ok(())
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn url_encode(value: &str) -> String {
    Url::parse_with_params("https://x.invalid/", [("v", value)])
        .expect("固定 URL 合法")
        .query()
        .and_then(|query| query.strip_prefix("v="))
        .unwrap_or_default()
        .to_string()
}

fn redact_code(body: &str) -> String {
    if body.len() > 120 {
        format!("{}...", &body[..120])
    } else {
        body.to_string()
    }
    .replace("wx_code", "callback")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_cache_requires_exact_expected_format() {
        let value = format!("wrdvpn1-{}", "0".repeat(32));
        assert!(valid_ticket_format(&format!("{TICKET_NAME}={value}")));
        assert!(!valid_ticket_format(&format!("other={value}")));
    }

    #[test]
    fn rsa_matches_historical_cas_javascript() {
        let encrypted = rsa_encrypt(
            "654321",
            "91c28b7f794d9aa0e73078c8f9ef68270154fbecdbc455c06afb4fe922fa433218e785e1e90402c0ab120c04296472ff310da4237339e1d15c506694add53d4b",
            "10001",
        ).unwrap();
        assert_eq!(
            encrypted,
            "1aa6cdb463265bdf0927564d3ca7160be772ebcbc71d96eb74c18bb0c2955f361c49be02c908f8387736a845214217e0a6b67c5a8b56caf2bfcec4645b49eecd"
        );
    }
}
