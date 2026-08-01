use crate::{
    ConnectFailure, DEFAULT_LOCAL_LISTEN_ADDR, DEFAULT_LOCAL_LISTEN_PORT, DEFAULT_SERVER_PORT,
    DEFAULT_TARGET_HOST, DEFAULT_TARGET_PORT, TOWS_TARGET_CONNECT_FAILURE_PREFIX,
    WebVpnHeartbeatRole, build_webvpn_ws_url, connect_websocket, log_error, log_info, log_success,
    log_warn, normalize_server_addr, normalize_tcp_target_arg, parse_socket_addr_with_default_host,
    relay_stream, rsa_encrypt,
};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::cookie::CookieStore;
use reqwest::header::{ORIGIN, REFERER, USER_AGENT};
use reqwest::{Client, Url};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

#[path = "bin/towc/qr.rs"]
mod qr;

const WEBVPN_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/login";
const WEBVPN_TICKET_COOKIE_PREFIX: &str = "wengine_vpn_ticketwebvpn_szut_edu_cn=";
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
const CACHED_LOGIN_TIMEOUT_SECS: u64 = 8;
const WEBVPN_COOKIE_REFRESH_INTERVAL_SECS: u64 = 180;
const WEBVPN_COOKIE_REFRESH_TIMEOUT_SECS: u64 = 8;
const CAS_LOGIN_ATTEMPTS: usize = 2;
const CAS_LOGIN_RETRY_SETTLE_MS: u64 = 1500;
const WECHAT_POLL_ATTEMPTS: usize = 180;
const WECHAT_POLL_TIMEOUT_SECS: u64 = 35;
const WECHAT_POLL_SETTLE_MS: u64 = 1800;
const COOKIE_CACHE_FILE_NAME: &str = "webvpn.cookie";
const INTERACTIVE_DEFAULTS_CACHE_FILE_NAME: &str = "interactive.defaults";
const INTERACTIVE_DEFAULTS_CACHE_VERSION: &str = "1";
const LOGIN_METHOD_PROMPT: &str =
    "login method (enter mobile/email, or press Enter for WeChat QR): ";
const TUNNEL_RETRY_INTERVAL_SECS: u64 = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoginMethod {
    Sms { mobile: String },
    Email { email: String },
    WechatQr,
}

impl LoginMethod {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Sms { mobile }
                if !mobile.is_empty() && mobile.chars().all(|ch| ch.is_ascii_digit()) =>
            {
                Ok(())
            }
            Self::Email { email } if email.contains('@') => Ok(()),
            Self::WechatQr => Ok(()),
            Self::Sms { .. } => anyhow::bail!("mobile number must contain only digits"),
            Self::Email { .. } => anyhow::bail!("invalid email address"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionState {
    LoggedOut,
    LoggingIn { method: LoginMethod },
    Ready,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionEvent {
    CheckingCachedCookie,
    CachedCookieUnavailable,
    LoggingIn { method: LoginMethod },
    CodeRequested { label: String },
    QrCode { jpeg: Vec<u8> },
    Ready,
    Expired,
    LoggedOut,
    Error { detail: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TunnelConfig {
    pub server: String,
    pub target: String,
    pub listen_addr: String,
}

impl TunnelConfig {
    pub fn validate(&self) -> Result<()> {
        normalize_server_addr(&self.server).context("invalid tows address")?;
        normalize_tcp_target_arg(Some(&self.target)).context("invalid target address")?;
        parse_socket_addr_with_default_host(&self.listen_addr, DEFAULT_TARGET_HOST)
            .context("invalid listen address")?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TunnelState {
    Configured,
    PendingAuth,
    Probing,
    Running,
    Retrying,
    Failed,
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TunnelEvent {
    StateChanged { tunnel_id: u64, state: TunnelState },
    LocalConnectionOpened { tunnel_id: u64, peer: String },
    LocalConnectionClosed { tunnel_id: u64, peer: String },
    Error { tunnel_id: u64, detail: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EmbeddedClientEvent {
    Session(SessionEvent),
    Tunnel(TunnelEvent),
}

pub trait EmbeddedClientUi: Send + Sync {
    fn emit(&self, event: EmbeddedClientEvent);
    fn request_verification_code(&self, label: &str) -> Result<String>;
}

#[derive(Clone)]
pub struct SessionHandle {
    cookie: Arc<Mutex<String>>,
}

impl SessionHandle {
    fn cookie(&self) -> &Arc<Mutex<String>> {
        &self.cookie
    }
}

#[derive(Clone)]
pub struct SessionManager {
    inner: Arc<SessionManagerInner>,
}

struct SessionManagerInner {
    state: Mutex<SessionState>,
    state_tx: watch::Sender<SessionState>,
    login_lock: tokio::sync::Mutex<()>,
    login_epoch: AtomicU64,
    runtime: Mutex<Option<SessionRuntime>>,
    ui: Arc<dyn EmbeddedClientUi>,
}

struct SessionRuntime {
    handle: SessionHandle,
    cancel: watch::Sender<bool>,
    refresh_task: JoinHandle<()>,
}

impl Drop for SessionRuntime {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        self.refresh_task.abort();
    }
}

impl SessionManager {
    pub fn new(ui: Arc<dyn EmbeddedClientUi>) -> Self {
        install_crypto_provider();
        let initial = SessionState::LoggedOut;
        let (state_tx, _) = watch::channel(initial.clone());
        Self {
            inner: Arc::new(SessionManagerInner {
                state: Mutex::new(initial),
                state_tx,
                login_lock: tokio::sync::Mutex::new(()),
                login_epoch: AtomicU64::new(0),
                runtime: Mutex::new(None),
                ui,
            }),
        }
    }

    pub fn state(&self) -> SessionState {
        self.inner
            .state
            .lock()
            .expect("session state poisoned")
            .clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<SessionState> {
        self.inner.state_tx.subscribe()
    }

    pub fn handle(&self) -> Option<SessionHandle> {
        if self.state() != SessionState::Ready {
            return None;
        }
        self.inner
            .runtime
            .lock()
            .expect("session runtime poisoned")
            .as_ref()
            .map(|runtime| runtime.handle.clone())
    }

    pub async fn login(&self, method: LoginMethod) -> Result<SessionHandle> {
        let _login_guard = self.inner.login_lock.lock().await;
        method.validate()?;
        if let Some(handle) = self.handle() {
            return Ok(handle);
        }
        if matches!(self.state(), SessionState::LoggingIn { .. }) {
            anyhow::bail!("WebVPN login is already in progress");
        }
        let login_epoch = self.inner.login_epoch.fetch_add(1, Ordering::AcqRel) + 1;

        self.set_state(SessionState::LoggingIn {
            method: method.clone(),
        });
        self.emit(SessionEvent::LoggingIn {
            method: method.clone(),
        });

        let result = async {
            let cookie = match method {
                LoginMethod::Sms { mobile } => {
                    login_with_verification_code(
                        VerificationLogin::Sms { mobile },
                        self.inner.ui.as_ref(),
                    )
                    .await?
                }
                LoginMethod::Email { email } => {
                    login_with_verification_code(
                        VerificationLogin::Email { email },
                        self.inner.ui.as_ref(),
                    )
                    .await?
                }
                LoginMethod::WechatQr => login_with_wechat_qr(self.inner.ui.as_ref()).await?,
            };
            if ticket_cookie_from_header(&cookie).is_none() {
                anyhow::bail!("WebVPN login completed without a ticket cookie");
            }
            write_cached_cookie(&cookie);
            Ok::<_, anyhow::Error>(cookie)
        }
        .await;

        if self.inner.login_epoch.load(Ordering::Acquire) != login_epoch {
            anyhow::bail!("WebVPN login was canceled");
        }
        let cookie = match result {
            Ok(cookie) => cookie,
            Err(err) => {
                self.set_state(SessionState::LoggedOut);
                self.emit(SessionEvent::Error {
                    detail: format!("{err:#}"),
                });
                return Err(err);
            }
        };

        let handle = SessionHandle {
            cookie: Arc::new(Mutex::new(cookie)),
        };
        let (cancel, cancel_rx) = watch::channel(false);
        let refresh_task = tokio::spawn(maintain_session_cookie_refresh(
            handle.clone(),
            cancel_rx,
            Arc::downgrade(&self.inner),
        ));
        *self.inner.runtime.lock().expect("session runtime poisoned") = Some(SessionRuntime {
            handle: handle.clone(),
            cancel,
            refresh_task,
        });
        self.set_state(SessionState::Ready);
        self.emit(SessionEvent::Ready);
        Ok(handle)
    }

    pub async fn login_with_cached_cookie(&self) -> Result<Option<SessionHandle>> {
        let _login_guard = self.inner.login_lock.lock().await;
        if let Some(handle) = self.handle() {
            return Ok(Some(handle));
        }
        let login_epoch = self.inner.login_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.emit(SessionEvent::CheckingCachedCookie);
        let cached_cookie = match try_cached_portal_login().await {
            Ok(cookie) => cookie,
            Err(err) => {
                self.emit(SessionEvent::Error {
                    detail: format!("{err:#}"),
                });
                return Err(err);
            }
        };
        let Some(cookie) = cached_cookie else {
            self.emit(SessionEvent::CachedCookieUnavailable);
            return Ok(None);
        };
        if self.inner.login_epoch.load(Ordering::Acquire) != login_epoch {
            return Ok(None);
        }
        let handle = SessionHandle {
            cookie: Arc::new(Mutex::new(cookie)),
        };
        let (cancel, cancel_rx) = watch::channel(false);
        let refresh_task = tokio::spawn(maintain_session_cookie_refresh(
            handle.clone(),
            cancel_rx,
            Arc::downgrade(&self.inner),
        ));
        *self.inner.runtime.lock().expect("session runtime poisoned") = Some(SessionRuntime {
            handle: handle.clone(),
            cancel,
            refresh_task,
        });
        self.set_state(SessionState::Ready);
        self.emit(SessionEvent::Ready);
        Ok(Some(handle))
    }

    pub fn logout(&self) {
        self.inner.login_epoch.fetch_add(1, Ordering::AcqRel);
        self.inner
            .runtime
            .lock()
            .expect("session runtime poisoned")
            .take();
        self.set_state(SessionState::LoggedOut);
        self.emit(SessionEvent::LoggedOut);
    }

    fn mark_expired(&self) {
        self.inner.login_epoch.fetch_add(1, Ordering::AcqRel);
        self.inner
            .runtime
            .lock()
            .expect("session runtime poisoned")
            .take();
        self.set_state(SessionState::LoggedOut);
        self.emit(SessionEvent::Expired);
    }

    fn mark_expired_for(&self, handle: &SessionHandle) {
        let is_current = self
            .inner
            .runtime
            .lock()
            .expect("session runtime poisoned")
            .as_ref()
            .is_some_and(|runtime| Arc::ptr_eq(&runtime.handle.cookie, &handle.cookie));
        if is_current {
            self.mark_expired();
        }
    }

    fn set_state(&self, state: SessionState) {
        *self.inner.state.lock().expect("session state poisoned") = state.clone();
        self.inner.state_tx.send_replace(state);
    }

    fn emit(&self, event: SessionEvent) {
        self.inner.ui.emit(EmbeddedClientEvent::Session(event));
    }
}

#[derive(Clone)]
pub struct TunnelHandle {
    id: u64,
    state: Arc<Mutex<TunnelState>>,
}

impl TunnelHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn state(&self) -> TunnelState {
        *self.state.lock().expect("tunnel state poisoned")
    }
}

#[derive(Clone)]
pub struct TunnelManager {
    inner: Arc<TunnelManagerInner>,
}

struct TunnelManagerInner {
    session: SessionManager,
    ui: Arc<dyn EmbeddedClientUi>,
    next_id: AtomicU64,
    tunnels: Mutex<HashMap<u64, TunnelRuntime>>,
}

struct TunnelRuntime {
    config: TunnelConfig,
    handle: TunnelHandle,
    cancel: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

impl Drop for TunnelManagerInner {
    fn drop(&mut self) {
        let Ok(mut tunnels) = self.tunnels.lock() else {
            return;
        };
        for runtime in tunnels.values_mut() {
            if let Some(cancel) = runtime.cancel.take() {
                let _ = cancel.send(true);
            }
            if let Some(task) = runtime.task.take() {
                task.abort();
            }
        }
    }
}

impl TunnelManager {
    pub fn new(session: SessionManager, ui: Arc<dyn EmbeddedClientUi>) -> Self {
        Self {
            inner: Arc::new(TunnelManagerInner {
                session,
                ui,
                next_id: AtomicU64::new(1),
                tunnels: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn add(&self, config: TunnelConfig) -> Result<u64> {
        config.validate()?;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let handle = TunnelHandle {
            id,
            state: Arc::new(Mutex::new(TunnelState::Configured)),
        };
        self.inner
            .tunnels
            .lock()
            .expect("tunnel map poisoned")
            .insert(
                id,
                TunnelRuntime {
                    config,
                    handle,
                    cancel: None,
                    task: None,
                },
            );
        Ok(id)
    }

    pub fn handle(&self, id: u64) -> Option<TunnelHandle> {
        self.inner
            .tunnels
            .lock()
            .expect("tunnel map poisoned")
            .get(&id)
            .map(|runtime| runtime.handle.clone())
    }

    pub async fn start(&self, id: u64) -> Result<()> {
        let mut tunnels = self.inner.tunnels.lock().expect("tunnel map poisoned");
        let runtime = tunnels
            .get_mut(&id)
            .with_context(|| format!("unknown tunnel id {id}"))?;
        if runtime
            .task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return Ok(());
        }

        let (cancel, cancel_rx) = watch::channel(false);
        let config = runtime.config.clone();
        let handle = runtime.handle.clone();
        let session = self.inner.session.clone();
        let ui = Arc::clone(&self.inner.ui);
        runtime.cancel = Some(cancel);
        runtime.task = Some(tokio::spawn(run_tunnel_task(
            config, handle, session, ui, cancel_rx,
        )));
        Ok(())
    }

    pub async fn stop(&self, id: u64) -> Result<()> {
        let (cancel, task, handle) = {
            let mut tunnels = self.inner.tunnels.lock().expect("tunnel map poisoned");
            let runtime = tunnels
                .get_mut(&id)
                .with_context(|| format!("unknown tunnel id {id}"))?;
            (
                runtime.cancel.take(),
                runtime.task.take(),
                runtime.handle.clone(),
            )
        };
        if let Some(cancel) = cancel {
            let _ = cancel.send(true);
        }
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        set_tunnel_state(&handle, &self.inner.ui, TunnelState::Stopped);
        Ok(())
    }

    pub async fn remove(&self, id: u64) -> Result<()> {
        self.stop(id).await?;
        self.inner
            .tunnels
            .lock()
            .expect("tunnel map poisoned")
            .remove(&id);
        Ok(())
    }
}

async fn run_tunnel_task(
    config: TunnelConfig,
    handle: TunnelHandle,
    session: SessionManager,
    ui: Arc<dyn EmbeddedClientUi>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut session_state = session.subscribe();
    loop {
        let Some(session_handle) =
            wait_for_session(&handle, &ui, &session, &mut session_state, &mut cancel).await
        else {
            break;
        };

        set_tunnel_state(&handle, &ui, TunnelState::Probing);
        let url = match build_webvpn_ws_url(&config.server, Some(&config.target)) {
            Ok(url) => url,
            Err(err) => {
                fail_tunnel(&handle, &ui, format!("{err:#}"));
                return;
            }
        };
        let server_addr = match normalize_server_addr(&config.server) {
            Ok(value) => value,
            Err(err) => {
                fail_tunnel(&handle, &ui, format!("{err:#}"));
                return;
            }
        };
        let target_addr = match normalize_tcp_target_arg(Some(&config.target)) {
            Ok(value) => value,
            Err(err) => {
                fail_tunnel(&handle, &ui, format!("{err:#}"));
                return;
            }
        };

        let probe =
            wait_for_webvpn_ready(&url, session_handle.cookie(), &server_addr, &target_addr);
        let probe_result = tokio::select! {
            result = probe => result,
            _ = cancellation_requested(&mut cancel) => break,
        };
        if let Err(err) = probe_result {
            if err
                .downcast_ref::<ReadinessFailureKind>()
                .is_some_and(|kind| *kind == ReadinessFailureKind::CookieExpired)
            {
                session.mark_expired_for(&session_handle);
                continue;
            }
            if session.state() != SessionState::Ready {
                continue;
            }
            emit_tunnel_error(&ui, handle.id, format!("{err:#}"));
            set_tunnel_state(&handle, &ui, TunnelState::Retrying);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(TUNNEL_RETRY_INTERVAL_SECS)) => {}
                _ = cancellation_requested(&mut cancel) => break,
                _ = session_state.changed() => {}
            }
            continue;
        }

        let listen_addr =
            match parse_socket_addr_with_default_host(&config.listen_addr, DEFAULT_TARGET_HOST) {
                Ok(value) => value,
                Err(err) => {
                    fail_tunnel(&handle, &ui, format!("{err:#}"));
                    return;
                }
            };
        let listener = match TcpListener::bind(listen_addr).await {
            Ok(listener) => listener,
            Err(err) => {
                fail_tunnel(
                    &handle,
                    &ui,
                    format!("failed to bind local tcp listener on {listen_addr}: {err}"),
                );
                return;
            }
        };
        set_tunnel_state(&handle, &ui, TunnelState::Running);

        let mut connections = JoinSet::new();
        let mut retry = false;
        loop {
            tokio::select! {
                _ = cancellation_requested(&mut cancel) => {
                    connections.abort_all();
                    set_tunnel_state(&handle, &ui, TunnelState::Stopped);
                    return;
                }
                changed = session_state.changed() => {
                    if changed.is_err() || *session_state.borrow() != SessionState::Ready {
                        connections.abort_all();
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, peer)) => {
                            let peer = peer.to_string();
                            ui.emit(EmbeddedClientEvent::Tunnel(
                                TunnelEvent::LocalConnectionOpened {
                                    tunnel_id: handle.id,
                                    peer: peer.clone(),
                                },
                            ));
                            let url = url.clone();
                            let cookie = Arc::clone(session_handle.cookie());
                            connections.spawn(async move {
                                let result = handle_local_connection(stream, &url, &cookie).await;
                                (peer, result)
                            });
                        }
                        Err(err) => {
                            fail_tunnel(&handle, &ui, format!("failed to accept local connection: {err}"));
                            connections.abort_all();
                            return;
                        }
                    }
                }
                result = connections.join_next(), if !connections.is_empty() => {
                    let Some(Ok((peer, result))) = result else {
                        continue;
                    };
                    ui.emit(EmbeddedClientEvent::Tunnel(
                        TunnelEvent::LocalConnectionClosed {
                            tunnel_id: handle.id,
                            peer: peer.clone(),
                        },
                    ));
                    match result {
                        Ok(()) => {}
                        Err(ConnectFailure::CookieExpired { .. }) => {
                            session.mark_expired_for(&session_handle);
                            connections.abort_all();
                            break;
                        }
                        Err(ConnectFailure::WebVpnFailed { location }) => {
                            emit_tunnel_error(
                                &ui,
                                handle.id,
                                format!("WebVPN tunnel endpoint failed: {location}"),
                            );
                            retry = true;
                            connections.abort_all();
                            break;
                        }
                        Err(err) => emit_tunnel_error(&ui, handle.id, format!("tcp {peer}: {err}")),
                    }
                }
            }
        }
        if retry {
            set_tunnel_state(&handle, &ui, TunnelState::Retrying);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(TUNNEL_RETRY_INTERVAL_SECS)) => {}
                _ = cancellation_requested(&mut cancel) => break,
                _ = session_state.changed() => {}
            }
        }
    }
    set_tunnel_state(&handle, &ui, TunnelState::Stopped);
}

async fn wait_for_session(
    handle: &TunnelHandle,
    ui: &Arc<dyn EmbeddedClientUi>,
    session: &SessionManager,
    state: &mut watch::Receiver<SessionState>,
    cancel: &mut watch::Receiver<bool>,
) -> Option<SessionHandle> {
    loop {
        if let Some(session) = session.handle() {
            return Some(session);
        }
        set_tunnel_state(handle, ui, TunnelState::PendingAuth);
        tokio::select! {
            changed = state.changed() => changed.ok()?,
            _ = cancellation_requested(cancel) => return None,
        }
    }
}

async fn cancellation_requested(cancel: &mut watch::Receiver<bool>) {
    if *cancel.borrow() {
        return;
    }
    let _ = cancel.changed().await;
}

fn set_tunnel_state(handle: &TunnelHandle, ui: &Arc<dyn EmbeddedClientUi>, state: TunnelState) {
    let mut current = handle.state.lock().expect("tunnel state poisoned");
    if *current == state {
        return;
    }
    *current = state;
    drop(current);
    ui.emit(EmbeddedClientEvent::Tunnel(TunnelEvent::StateChanged {
        tunnel_id: handle.id,
        state,
    }));
}

fn emit_tunnel_error(ui: &Arc<dyn EmbeddedClientUi>, tunnel_id: u64, detail: String) {
    ui.emit(EmbeddedClientEvent::Tunnel(TunnelEvent::Error {
        tunnel_id,
        detail,
    }));
}

fn fail_tunnel(handle: &TunnelHandle, ui: &Arc<dyn EmbeddedClientUi>, detail: String) {
    emit_tunnel_error(ui, handle.id, detail);
    set_tunnel_state(handle, ui, TunnelState::Failed);
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum VerificationLogin {
    Sms { mobile: String },
    Email { email: String },
}

#[derive(Deserialize)]
struct PublicKeyResponse {
    modulus: String,
    exponent: String,
}

#[derive(Debug, PartialEq, Eq)]
struct ClientConfig {
    server: String,
    target: Option<String>,
    listen_addr: String,
    login: Option<VerificationLogin>,
}

struct InteractiveForwardingConfig {
    target: String,
    listen_addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InteractiveDefaults {
    server: String,
    target: String,
    listen_addr: String,
}

type WebVpnClientWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, PartialEq, Eq)]
enum ParsedArgs {
    Help,
    Interactive,
    Run(ClientConfig),
}

struct WebVpnLoginEntry {
    cookie_header: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadinessFailureKind {
    CookieExpired,
    WebVpnFailed,
    TargetConnectFailed,
    ResetAfterOpen,
    ClosedAfterOpen,
    OpenFailed,
    ReadFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReadinessFailure {
    CookieExpired { location: String },
    WebVpnFailed { location: String },
    TargetConnectFailed { reason: String },
    ResetAfterOpen,
    ClosedAfterOpen { reason: Option<String> },
    OpenFailed { detail: String },
    ReadFailed { detail: String },
}

impl ReadinessFailure {
    fn kind(&self) -> ReadinessFailureKind {
        match self {
            Self::CookieExpired { .. } => ReadinessFailureKind::CookieExpired,
            Self::WebVpnFailed { .. } => ReadinessFailureKind::WebVpnFailed,
            Self::TargetConnectFailed { .. } => ReadinessFailureKind::TargetConnectFailed,
            Self::ResetAfterOpen => ReadinessFailureKind::ResetAfterOpen,
            Self::ClosedAfterOpen { .. } => ReadinessFailureKind::ClosedAfterOpen,
            Self::OpenFailed { .. } => ReadinessFailureKind::OpenFailed,
            Self::ReadFailed { .. } => ReadinessFailureKind::ReadFailed,
        }
    }

    fn observation_label(&self) -> &'static str {
        match self {
            Self::CookieExpired { .. } => "WebVPN redirected to login",
            Self::WebVpnFailed { .. } => "WebVPN returned /wengine-vpn/failed",
            Self::TargetConnectFailed { .. } => "tows reported target connect failure",
            Self::ResetAfterOpen => "WebSocket reset after opening",
            Self::ClosedAfterOpen { .. } => "WebSocket closed after opening",
            Self::OpenFailed { .. } => "WebSocket open failed",
            Self::ReadFailed { .. } => "WebSocket read failed",
        }
    }

    fn diagnostic_lines(&self, server_addr: &str, target_addr: &str) -> Vec<String> {
        match self {
            Self::CookieExpired { location } => vec![
                format!("phase: WebVPN redirected readiness check to login; location={location}"),
                "cause: WebVPN session cookie is expired or was rejected".to_string(),
                "check: restart towc and log in again".to_string(),
            ],
            Self::WebVpnFailed { location } => vec![
                format!(
                    "phase: WebVPN rejected the tunnel before tows accepted WebSocket; server={server_addr}; location={location}"
                ),
                "likely cause: tows is not running/reachable, tows address or port is wrong, firewall blocked it, or WebVPN cannot route to it".to_string(),
                "check: start tows on the target host and verify the configured server port is reachable through WebVPN".to_string(),
            ],
            Self::TargetConnectFailed { reason } => vec![
                format!(
                    "phase: tows accepted WebSocket at {server_addr}, then failed to connect target {target_addr}"
                ),
                format!("cause: target TCP connection failed; tows reported: {reason}"),
                "check: on the tows host, verify the target service is listening and --target points to the right port".to_string(),
            ],
            Self::ResetAfterOpen => vec![
                format!(
                    "phase: WebVPN reached tows at {server_addr}, then the WebSocket reset before data flowed"
                ),
                format!(
                    "likely cause: target {target_addr} is not listening/refused the connection, or tows closed after accepting the tunnel"
                ),
                "check: read the tows log; a target connect failed line confirms a target-port problem".to_string(),
            ],
            Self::ClosedAfterOpen { reason } => {
                let mut lines = vec![
                    format!(
                        "phase: WebVPN reached tows at {server_addr}, then the tunnel closed before readiness completed"
                    ),
                    format!(
                        "likely cause: target {target_addr} accepted then closed, or tows closed the tunnel early"
                    ),
                    "check: verify the target service stays open long enough for a TCP session".to_string(),
                ];
                if let Some(reason) = reason {
                    lines.insert(2, format!("detail: close reason from peer: {reason}"));
                }
                lines
            }
            Self::OpenFailed { detail } => vec![
                "phase: towc could not open the readiness WebSocket to WebVPN".to_string(),
                "likely cause: local network, DNS/TLS, proxy, or WebVPN availability issue".to_string(),
                format!("detail: {detail}"),
            ],
            Self::ReadFailed { detail } => vec![
                format!(
                    "phase: readiness WebSocket opened through WebVPN toward {server_addr}, then failed while reading"
                ),
                format!(
                    "likely cause: unstable tunnel, tows closed unexpectedly, or target {target_addr} closed/reset the connection"
                ),
                format!("detail: {detail}"),
            ],
        }
    }
}

struct TerminalUi;

impl EmbeddedClientUi for TerminalUi {
    fn emit(&self, event: EmbeddedClientEvent) {
        match event {
            EmbeddedClientEvent::Session(SessionEvent::CheckingCachedCookie) => {
                log_info("session", "trying cached WebVPN cookie");
            }
            EmbeddedClientEvent::Session(SessionEvent::CachedCookieUnavailable) => {
                log_info("session", "cached cookie unavailable; login is required");
            }
            EmbeddedClientEvent::Session(SessionEvent::QrCode { jpeg }) => {
                if let Err(err) = qr::print(&jpeg) {
                    log_error("session", format!("failed to render QR code: {err:#}"));
                }
            }
            EmbeddedClientEvent::Session(SessionEvent::Expired) => {
                log_warn("session", "WebVPN session expired");
            }
            EmbeddedClientEvent::Session(SessionEvent::Error { detail }) => {
                log_error("session", detail);
            }
            EmbeddedClientEvent::Tunnel(TunnelEvent::StateChanged { tunnel_id, state }) => {
                log_info("tunnel", format!("#{tunnel_id} state: {state:?}"));
            }
            EmbeddedClientEvent::Tunnel(TunnelEvent::LocalConnectionOpened { tunnel_id, peer }) => {
                log_success("tunnel", format!("#{tunnel_id} tcp {peer} connected"))
            }
            EmbeddedClientEvent::Tunnel(TunnelEvent::LocalConnectionClosed { tunnel_id, peer }) => {
                log_info("tunnel", format!("#{tunnel_id} tcp {peer} closed"))
            }
            EmbeddedClientEvent::Tunnel(TunnelEvent::Error { tunnel_id, detail }) => {
                log_error("tunnel", format!("#{tunnel_id}: {detail}"));
            }
            _ => {}
        }
    }

    fn request_verification_code(&self, label: &str) -> Result<String> {
        prompt_verification_code(label)
    }
}

impl std::fmt::Display for ReadinessFailureKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "WebVPN readiness failed: {self:?}")
    }
}

impl std::error::Error for ReadinessFailureKind {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedClientConfig {
    pub server: String,
    pub target: String,
    pub listen_addr: String,
    pub login: LoginMethod,
}

pub type EmbeddedLogin = LoginMethod;

pub async fn run_embedded_client(
    config: EmbeddedClientConfig,
    ui: Arc<dyn EmbeddedClientUi>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    install_crypto_provider();
    let session = SessionManager::new(Arc::clone(&ui));
    let tunnels = TunnelManager::new(session.clone(), ui);
    if session.login_with_cached_cookie().await?.is_none() {
        session.login(config.login).await?;
    }
    let id = tunnels.add(TunnelConfig {
        server: config.server,
        target: config.target,
        listen_addr: config.listen_addr,
    })?;
    tunnels.start(id).await?;
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
    tunnels.stop(id).await?;
    session.logout();
    Ok(())
}

pub fn cache_directory() -> Option<PathBuf> {
    cookie_cache_path().and_then(|path| path.parent().map(PathBuf::from))
}

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub async fn run_cli() -> Result<()> {
    log_info("client", format!("towc v{}", env!("CARGO_PKG_VERSION")));
    install_crypto_provider();

    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let parsed_args = parse_args(&raw_args)?;
    let (config, fallback_login) = match parsed_args {
        ParsedArgs::Help => {
            print_usage();
            return Ok(());
        }
        ParsedArgs::Interactive => (prepare_interactive_startup().await?, None),
        ParsedArgs::Run(config) => {
            let login = config
                .login
                .clone()
                .map(login_method_from_verification)
                .unwrap_or(LoginMethod::WechatQr);
            (config, Some(login))
        }
    };
    let ui: Arc<dyn EmbeddedClientUi> = Arc::new(TerminalUi);
    let session = SessionManager::new(Arc::clone(&ui));
    let tunnels = TunnelManager::new(session.clone(), ui);
    if session.login_with_cached_cookie().await?.is_none() {
        let login = match fallback_login {
            Some(login) => login,
            None => prompt_login_identity()?
                .map(login_method_from_verification)
                .unwrap_or(LoginMethod::WechatQr),
        };
        session.login(login).await?;
    }
    let id = tunnels.add(TunnelConfig {
        server: config.server,
        target: normalize_tcp_target_arg(config.target.as_deref())?,
        listen_addr: config.listen_addr,
    })?;
    tunnels.start(id).await?;
    tokio::signal::ctrl_c().await?;
    log_info("client", "shutting down");
    tunnels.stop(id).await?;
    session.logout();
    Ok(())
}

async fn prepare_interactive_startup() -> Result<ClientConfig> {
    let cached_defaults = read_cached_interactive_defaults();
    let server = match &cached_defaults {
        Some(defaults) => prompt_with_default(
            &format!("tows address <ip[:port]> (default: {}): ", defaults.server),
            &defaults.server,
        )?,
        None => prompt_required("tows address <ip[:port]>: ")?,
    };
    let built_in_target_default = DEFAULT_TARGET_PORT.to_string();
    let built_in_listen_default = DEFAULT_LOCAL_LISTEN_PORT.to_string();
    let target_default = cached_defaults
        .as_ref()
        .map(|defaults| defaults.target.as_str())
        .unwrap_or(&built_in_target_default);
    let listen_default = cached_defaults
        .as_ref()
        .map(|defaults| defaults.listen_addr.as_str())
        .unwrap_or(&built_in_listen_default);
    let forwarding = prompt_interactive_forwarding_config(target_default, listen_default).await?;

    let defaults = InteractiveDefaults {
        server: server.clone(),
        target: forwarding.target.clone(),
        listen_addr: forwarding.listen_addr.clone(),
    };
    validate_interactive_defaults(&defaults)?;
    write_cached_interactive_defaults(&defaults);

    Ok(ClientConfig {
        server,
        target: Some(forwarding.target),
        listen_addr: forwarding.listen_addr,
        login: None,
    })
}

fn login_method_from_verification(login: VerificationLogin) -> LoginMethod {
    match login {
        VerificationLogin::Sms { mobile } => LoginMethod::Sms { mobile },
        VerificationLogin::Email { email } => LoginMethod::Email { email },
    }
}

async fn prompt_interactive_forwarding_config(
    target_default: &str,
    listen_default: &str,
) -> Result<InteractiveForwardingConfig> {
    let target_default = target_default.to_string();
    let listen_default = listen_default.to_string();
    tokio::task::spawn_blocking(move || {
        prompt_interactive_forwarding_config_blocking(&target_default, &listen_default)
    })
    .await
    .context("interactive forwarding parameter task failed")?
}

fn prompt_interactive_forwarding_config_blocking(
    target_default: &str,
    listen_default: &str,
) -> Result<InteractiveForwardingConfig> {
    let target = prompt_with_default(
        &format!("target address/port (default: {target_default}): "),
        target_default,
    )?;
    let listen_addr = prompt_with_default(
        &format!("listen address/port (default: {listen_default}): "),
        listen_default,
    )?;

    Ok(InteractiveForwardingConfig {
        target,
        listen_addr,
    })
}

async fn handle_local_connection(
    stream: TcpStream,
    url: &str,
    cookie: &Arc<Mutex<String>>,
) -> std::result::Result<(), ConnectFailure> {
    let websocket = connect_websocket_with_current_cookie(url, cookie).await?;
    relay_stream(websocket, stream, WebVpnHeartbeatRole::Client)
        .await
        .map_err(ConnectFailure::Other)
}

async fn connect_websocket_with_current_cookie(
    url: &str,
    cookie: &Arc<Mutex<String>>,
) -> std::result::Result<WebVpnClientWebSocket, ConnectFailure> {
    loop {
        let cookie_snapshot = current_cookie(cookie);
        match connect_websocket(url, &cookie_snapshot).await {
            Err(ConnectFailure::CookieExpired { .. })
                if current_cookie(cookie) != cookie_snapshot =>
            {
                log_info(
                    "client",
                    "WebVPN cookie changed while opening a connection; retrying with the refreshed cookie",
                );
            }
            result => return result,
        }
    }
}

fn current_cookie(cookie: &Arc<Mutex<String>>) -> String {
    cookie.lock().expect("WebVPN cookie mutex poisoned").clone()
}

fn replace_current_cookie(cookie: &Arc<Mutex<String>>, value: String) {
    *cookie.lock().expect("WebVPN cookie mutex poisoned") = value;
}

enum PortalCookieRefresh {
    Refreshed(String),
    Expired,
}

async fn refresh_portal_cookie_once(
    client: &Client,
    cookie_jar: &reqwest::cookie::Jar,
) -> Result<PortalCookieRefresh> {
    let response = client
        .get(webvpn_cookie_refresh_url()?)
        .header(REFERER, "https://webvpn.szut.edu.cn/")
        .send()
        .await
        .context("failed to send WebVPN cookie refresh request")?;
    if response.url().as_str().contains("/login") || response.url().as_str().contains("/cas/login")
    {
        return Ok(PortalCookieRefresh::Expired);
    }
    response
        .error_for_status()
        .context("WebVPN cookie refresh request failed")?;

    let cookie = webvpn_cookie_header_from_jar(cookie_jar)
        .context("WebVPN cookie refresh completed without WebVPN cookies")?;
    if ticket_cookie_from_header(&cookie).is_none() {
        return Ok(PortalCookieRefresh::Expired);
    }
    Ok(PortalCookieRefresh::Refreshed(cookie))
}

async fn try_cached_portal_login() -> Result<Option<String>> {
    let Some(cookie) = read_cached_cookie() else {
        return Ok(None);
    };
    if ticket_cookie_from_header(&cookie).is_none()
        || HeaderValue::from_bytes(cookie.as_bytes()).is_err()
    {
        return Ok(None);
    }

    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    seed_webvpn_cookie_jar(&cookie_jar, &cookie);
    let client = build_login_client(Arc::clone(&cookie_jar))?;
    match tokio::time::timeout(
        Duration::from_secs(CACHED_LOGIN_TIMEOUT_SECS),
        refresh_portal_cookie_once(&client, &cookie_jar),
    )
    .await
    {
        Ok(Ok(PortalCookieRefresh::Refreshed(cookie))) => {
            write_cached_cookie(&cookie);
            Ok(Some(cookie))
        }
        Ok(Ok(PortalCookieRefresh::Expired)) => Ok(None),
        Ok(Err(err)) => Err(err).context("failed to verify cached WebVPN login"),
        Err(_) => anyhow::bail!("timed out while verifying cached WebVPN login"),
    }
}

async fn maintain_session_cookie_refresh(
    session: SessionHandle,
    mut cancel: watch::Receiver<bool>,
    manager: Weak<SessionManagerInner>,
) {
    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    seed_webvpn_cookie_jar(&cookie_jar, &current_cookie(session.cookie()));
    let Ok(client) = build_login_client(Arc::clone(&cookie_jar)) else {
        return;
    };

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(WEBVPN_COOKIE_REFRESH_INTERVAL_SECS)) => {}
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return;
                }
                continue;
            }
        }

        match tokio::time::timeout(
            Duration::from_secs(WEBVPN_COOKIE_REFRESH_TIMEOUT_SECS),
            refresh_portal_cookie_once(&client, &cookie_jar),
        )
        .await
        {
            Ok(Ok(PortalCookieRefresh::Refreshed(cookie))) => {
                replace_current_cookie(session.cookie(), cookie.clone());
                write_cached_cookie(&cookie);
            }
            Ok(Ok(PortalCookieRefresh::Expired)) => {
                if let Some(inner) = manager.upgrade() {
                    SessionManager { inner }.mark_expired();
                }
                return;
            }
            Ok(Err(err)) => log_warn("session", format!("cookie refresh failed: {err:#}")),
            Err(_) => log_warn("session", "cookie refresh timed out"),
        }
    }
}

fn webvpn_cookie_refresh_url() -> Result<String> {
    let mut url = Url::parse("https://webvpn.szut.edu.cn/wengine-vpn/cookie")
        .context("failed to build WebVPN cookie refresh URL")?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("method", "get");
        query.append_pair("host", "cas.szut.edu.cn");
        query.append_pair("scheme", "https");
        query.append_pair("path", "/personal-center");
        query.append_pair("vpn_timestamp", &unix_timestamp_millis().to_string());
    }
    Ok(url.into())
}

fn unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn parse_args(args: &[String]) -> Result<ParsedArgs> {
    if args.is_empty() {
        return Ok(ParsedArgs::Interactive);
    }

    if is_help_arg(&args[0]) {
        return Ok(ParsedArgs::Help);
    }

    let server = args[0].trim();
    if server.is_empty() || server.starts_with('-') {
        anyhow::bail!("missing required <server-ip[:port]> as the first argument");
    }

    let mut config = ClientConfig {
        server: server.to_string(),
        target: None,
        listen_addr: DEFAULT_LOCAL_LISTEN_ADDR.to_string(),
        login: None,
    };

    let mut index = 1;
    let mut listen_seen = false;
    while index < args.len() {
        match args[index].as_str() {
            "--target" => {
                if config.target.is_some() {
                    anyhow::bail!("--target can only be specified once");
                }
                config.target = Some(next_flag_value(args, &mut index, "--target")?);
            }
            "--listen" => {
                if listen_seen {
                    anyhow::bail!("--listen can only be specified once");
                }
                listen_seen = true;
                config.listen_addr = next_flag_value(args, &mut index, "--listen")?;
            }
            "--login" => {
                if config.login.is_some() {
                    anyhow::bail!("--login can only be specified once");
                }
                let value = next_flag_value(args, &mut index, "--login")?;
                config.login = Some(parse_login_identity(&value)?);
            }
            "--help" | "-h" => return Ok(ParsedArgs::Help),
            other => {
                if other.starts_with('-') {
                    anyhow::bail!("unknown argument: {other}");
                }
                anyhow::bail!("unexpected extra argument: {other}");
            }
        }
        index += 1;
    }

    Ok(ParsedArgs::Run(config))
}

fn next_flag_value(args: &[String], index: &mut usize, name: &str) -> Result<String> {
    *index += 1;
    let value = args
        .get(*index)
        .with_context(|| format!("missing value for {name}"))?;
    if value.starts_with('-') {
        anyhow::bail!("missing value for {name}");
    }

    Ok(value.to_string())
}

fn is_help_arg(value: &str) -> bool {
    value == "--help" || value == "-h"
}

fn prompt_login_identity() -> Result<Option<VerificationLogin>> {
    loop {
        let Some(value) = prompt_optional(LOGIN_METHOD_PROMPT)? else {
            return Ok(None);
        };

        match parse_login_identity(&value) {
            Ok(login) => return Ok(Some(login)),
            Err(err) => log_warn("input", err.to_string()),
        }
    }
}

fn prompt_required(prompt: &str) -> Result<String> {
    loop {
        let Some(value) = prompt_line(prompt)? else {
            anyhow::bail!("server is required");
        };
        if !value.is_empty() {
            return Ok(value);
        }
        log_warn("input", "server address is required");
    }
}

fn prompt_optional(prompt: &str) -> Result<Option<String>> {
    Ok(prompt_line(prompt)?.filter(|value| !value.is_empty()))
}

fn prompt_with_default(prompt: &str, default: &str) -> Result<String> {
    Ok(prompt_optional(prompt)?.unwrap_or_else(|| default.to_string()))
}

fn prompt_line(prompt: &str) -> Result<Option<String>> {
    print!("{prompt}");
    io::stdout().flush().context("failed to flush prompt")?;

    let mut value = String::new();
    let read_size = io::stdin()
        .read_line(&mut value)
        .context("failed to read prompt input")?;
    if read_size == 0 {
        return Ok(None);
    }

    Ok(Some(value.trim().to_string()))
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

    anyhow::bail!("invalid login value: use a numeric mobile number or an email address")
}

fn validate_interactive_defaults(defaults: &InteractiveDefaults) -> Result<()> {
    normalize_server_addr(&defaults.server).context("invalid tows address")?;
    normalize_tcp_target_arg(Some(&defaults.target)).context("invalid target address")?;
    parse_socket_addr_with_default_host(&defaults.listen_addr, DEFAULT_TARGET_HOST)
        .context("invalid listen address")?;
    Ok(())
}

fn format_interactive_defaults(defaults: &InteractiveDefaults) -> String {
    format!(
        "version={INTERACTIVE_DEFAULTS_CACHE_VERSION}\nserver={}\ntarget={}\nlisten={}\n",
        defaults.server, defaults.target, defaults.listen_addr
    )
}

fn parse_interactive_defaults(contents: &str) -> Result<InteractiveDefaults> {
    let mut version = None;
    let mut server = None;
    let mut target = None;
    let mut listen_addr = None;

    for line in contents.lines() {
        if let Some(value) = line.strip_prefix("version=") {
            version = Some(value);
        } else if let Some(value) = line.strip_prefix("server=") {
            server = Some(value);
        } else if let Some(value) = line.strip_prefix("target=") {
            target = Some(value);
        } else if let Some(value) = line.strip_prefix("listen=") {
            listen_addr = Some(value);
        }
    }

    if version != Some(INTERACTIVE_DEFAULTS_CACHE_VERSION) {
        anyhow::bail!("unsupported interactive defaults cache version");
    }

    let required_value = |value: Option<&str>, name: &str| -> Result<String> {
        value
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .with_context(|| format!("missing {name} in interactive defaults cache"))
    };
    let defaults = InteractiveDefaults {
        server: required_value(server, "server")?,
        target: required_value(target, "target")?,
        listen_addr: required_value(listen_addr, "listen")?,
    };
    validate_interactive_defaults(&defaults).context("invalid interactive defaults cache")?;
    Ok(defaults)
}

fn read_cached_interactive_defaults() -> Option<InteractiveDefaults> {
    let path = interactive_defaults_cache_path()?;
    match fs::read_to_string(&path) {
        Ok(contents) => match parse_interactive_defaults(&contents) {
            Ok(defaults) => Some(defaults),
            Err(err) => {
                log_warn(
                    "client",
                    format!(
                        "cached interactive defaults are invalid; using built-in defaults: {err:#}"
                    ),
                );
                None
            }
        },
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => {
            log_warn(
                "client",
                format!("failed to read cached interactive defaults: {err}"),
            );
            None
        }
    }
}

fn write_cached_interactive_defaults(defaults: &InteractiveDefaults) {
    let Some(path) = interactive_defaults_cache_path() else {
        log_warn(
            "client",
            "failed to locate interactive defaults cache directory",
        );
        return;
    };

    if let Some(parent) = path.parent()
        && let Err(err) = fs::create_dir_all(parent)
    {
        log_warn(
            "client",
            format!("failed to create interactive defaults cache directory: {err}"),
        );
        return;
    }

    if let Err(err) = fs::write(&path, format_interactive_defaults(defaults)) {
        log_warn(
            "client",
            format!(
                "failed to write interactive defaults cache at {}: {err}",
                path.display()
            ),
        );
    }
}

fn read_cached_cookie() -> Option<String> {
    let path = cookie_cache_path()?;
    match fs::read_to_string(&path) {
        Ok(cookie) => {
            let cookie = cookie.trim();
            if cookie.is_empty() {
                None
            } else {
                Some(cookie.to_string())
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => {
            log_warn(
                "client",
                format!("failed to read cached WebVPN cookie: {err}"),
            );
            None
        }
    }
}

fn write_cached_cookie(cookie: &str) {
    let Some(path) = cookie_cache_path() else {
        log_warn("client", "failed to locate WebVPN cookie cache directory");
        return;
    };

    if let Some(parent) = path.parent()
        && let Err(err) = fs::create_dir_all(parent)
    {
        log_warn(
            "client",
            format!("failed to create WebVPN cookie cache directory: {err}"),
        );
        return;
    }

    if let Err(err) = fs::write(&path, format!("{cookie}\n")) {
        log_warn(
            "client",
            format!(
                "failed to write WebVPN cookie cache at {}: {err}",
                path.display()
            ),
        );
    }
}

#[cfg(windows)]
fn cache_file_path(file_name: &str) -> Option<PathBuf> {
    std::env::var_os("APPDATA")
        .or_else(|| std::env::var_os("LOCALAPPDATA"))
        .map(PathBuf::from)
        .map(|path| path.join("tcp_over_websocket").join(file_name))
}

#[cfg(not(windows))]
fn cache_file_path(file_name: &str) -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))?;

    Some(base.join("tcp_over_websocket").join(file_name))
}

fn cookie_cache_path() -> Option<PathBuf> {
    cache_file_path(COOKIE_CACHE_FILE_NAME)
}

fn interactive_defaults_cache_path() -> Option<PathBuf> {
    cache_file_path(INTERACTIVE_DEFAULTS_CACHE_FILE_NAME)
}

async fn wait_for_webvpn_ready(
    url: &str,
    cookie: &Arc<Mutex<String>>,
    server_addr: &str,
    target_addr: &str,
) -> Result<()> {
    let mut failures = Vec::new();
    log_info("client", "checking WebVPN tunnel readiness");

    for attempt in 1..=WEBVPN_READY_ATTEMPTS {
        match probe_webvpn_ready(url, cookie).await {
            Ok(()) => {
                let message = if failures.is_empty() {
                    "WebVPN tunnel ready".to_string()
                } else {
                    format!("WebVPN tunnel ready after {attempt}/{WEBVPN_READY_ATTEMPTS} attempts")
                };
                log_success("client", message);
                return Ok(());
            }
            Err(failure) => {
                failures.push(failure);
                if attempt >= WEBVPN_READY_ATTEMPTS {
                    continue;
                }

                if attempt == 1 {
                    log_warn(
                        "client",
                        format!(
                            "readiness check failed; retrying ({attempt}/{WEBVPN_READY_ATTEMPTS})"
                        ),
                    );
                } else if readiness_failure_kind_changed(&failures) {
                    let label = failures
                        .last()
                        .expect("failure was just recorded")
                        .observation_label();
                    log_warn(
                        "client",
                        format!(
                            "readiness failure changed to {label}; retrying ({attempt}/{WEBVPN_READY_ATTEMPTS})"
                        ),
                    );
                }
            }
        }

        if attempt < WEBVPN_READY_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(WEBVPN_READY_SETTLE_MS)).await;
        }
    }

    for (index, line) in readiness_failure_summary_lines(&failures, server_addr, target_addr)
        .into_iter()
        .enumerate()
    {
        if index == 0 {
            log_error("client", line);
        } else {
            log_warn("client", line);
        }
    }

    Err(anyhow::Error::new(
        failures
            .last()
            .map(ReadinessFailure::kind)
            .unwrap_or(ReadinessFailureKind::OpenFailed),
    )
    .context("WebVPN tunnel did not become ready; see diagnosis above"))
}

fn readiness_failure_kind_changed(failures: &[ReadinessFailure]) -> bool {
    let [.., previous, current] = failures else {
        return false;
    };

    previous.kind() != current.kind()
}

fn readiness_failure_summary_lines(
    failures: &[ReadinessFailure],
    server_addr: &str,
    target_addr: &str,
) -> Vec<String> {
    let Some(last_failure) = failures.last() else {
        return vec!["readiness failed without a captured failure detail".to_string()];
    };

    let mut lines = vec![format!(
        "readiness failed after {} attempts: {}",
        failures.len(),
        readiness_failure_counts(failures).join(", ")
    )];

    if failures
        .iter()
        .any(|failure| failure.kind() != last_failure.kind())
    {
        lines.push(format!(
            "last readiness failure: {}",
            last_failure.observation_label()
        ));
    }

    lines.extend(last_failure.diagnostic_lines(server_addr, target_addr));
    lines
}

fn readiness_failure_counts(failures: &[ReadinessFailure]) -> Vec<String> {
    let mut counts = Vec::<(ReadinessFailureKind, &'static str, usize)>::new();

    for failure in failures {
        if let Some((_, _, count)) = counts
            .iter_mut()
            .find(|(kind, _, _)| *kind == failure.kind())
        {
            *count += 1;
            continue;
        }

        counts.push((failure.kind(), failure.observation_label(), 1));
    }

    counts
        .into_iter()
        .map(|(_, label, count)| format!("{label} x{count}"))
        .collect()
}

async fn probe_webvpn_ready(
    url: &str,
    cookie: &Arc<Mutex<String>>,
) -> std::result::Result<(), ReadinessFailure> {
    let mut websocket = connect_websocket_with_current_cookie(url, cookie)
        .await
        .map_err(readiness_failure_from_connect_failure)?;

    let timeout = tokio::time::sleep(Duration::from_millis(WEBVPN_READY_TIMEOUT_MS));
    tokio::pin!(timeout);

    let ready = tokio::select! {
        message = websocket.next() => {
            match message {
                Some(Ok(Message::Close(frame))) => {
                    Err(readiness_failure_from_close_reason(
                        frame.map(|frame| frame.reason.to_string()),
                    ))
                }
                Some(Ok(_)) => Ok(()),
                Some(Err(err)) => Err(readiness_failure_from_websocket_error(err)),
                None => Err(ReadinessFailure::ClosedAfterOpen { reason: None }),
            }
        }
        _ = &mut timeout => Ok(()),
    };

    let _ = websocket.send(Message::Close(None)).await;
    ready
}

fn readiness_failure_from_connect_failure(err: ConnectFailure) -> ReadinessFailure {
    match err {
        ConnectFailure::CookieExpired { location } => ReadinessFailure::CookieExpired { location },
        ConnectFailure::WebVpnFailed { location } => ReadinessFailure::WebVpnFailed { location },
        ConnectFailure::Other(err) => ReadinessFailure::OpenFailed {
            detail: format!("{err:#}"),
        },
    }
}

fn readiness_failure_from_websocket_error(err: WebSocketError) -> ReadinessFailure {
    match err {
        WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
            ReadinessFailure::ResetAfterOpen
        }
        err => ReadinessFailure::ReadFailed {
            detail: err.to_string(),
        },
    }
}

fn readiness_failure_from_close_reason(reason: Option<String>) -> ReadinessFailure {
    let reason = reason.filter(|reason| !reason.trim().is_empty());
    if let Some(reason) = reason {
        if reason.starts_with(TOWS_TARGET_CONNECT_FAILURE_PREFIX) {
            return ReadinessFailure::TargetConnectFailed { reason };
        }

        return ReadinessFailure::ClosedAfterOpen {
            reason: Some(reason),
        };
    }

    ReadinessFailure::ClosedAfterOpen { reason: None }
}

fn build_login_client(cookie_jar: Arc<reqwest::cookie::Jar>) -> Result<Client> {
    Client::builder()
        .cookie_provider(cookie_jar)
        .user_agent(BROWSER_USER_AGENT)
        .build()
        .context("failed to build WebVPN login HTTP client")
}

async fn login_with_wechat_qr(ui: &dyn EmbeddedClientUi) -> Result<String> {
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

    ui.emit(EmbeddedClientEvent::Session(SessionEvent::QrCode {
        jpeg: qrcode.to_vec(),
    }));

    let code = match poll_wechat_qr_code(&client, &uuid).await? {
        WechatQrPollResult::Confirmed(code) => code,
        WechatQrPollResult::Expired => {
            anyhow::bail!("WeChat QR code expired; please restart towc and scan again");
        }
    };
    log_success("client", "WeChat confirmed login");
    log_info("client", "completing WebVPN authentication");
    let response = client
        .get(wechat_cas_callback_url(&code)?)
        .header(USER_AGENT, BROWSER_USER_AGENT)
        .send()
        .await
        .context("failed to open CAS WeChat callback")?
        .error_for_status()
        .context("CAS WeChat callback request failed")?;
    let final_url = response.url().to_string();

    let activated_cookie =
        activate_webvpn_fingerprint_if_needed(&client, &cookie_jar, &final_url).await?;
    let post_login_cookie = webvpn_cookie_header_from_jar(&cookie_jar);
    let cookie = activated_cookie
        .or(post_login_cookie)
        .or(login_entry.cookie_header)
        .context("WeChat login completed but WebVPN cookie header was not found")?;

    log_success("client", "WeChat QR login completed");
    Ok(cookie)
}

async fn login_with_verification_code(
    login: VerificationLogin,
    ui: &dyn EmbeddedClientUi,
) -> Result<String> {
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

    ui.emit(EmbeddedClientEvent::Session(SessionEvent::CodeRequested {
        label: label.to_string(),
    }));
    let code = ui.request_verification_code(label)?;
    if code.trim().is_empty() {
        anyhow::bail!("verification code cannot be empty");
    }
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

    let activated_cookie =
        activate_webvpn_fingerprint_if_needed(&client, &cookie_jar, &final_url).await?;
    let post_login_cookie = webvpn_cookie_header_from_jar(&cookie_jar);

    let cookie = activated_cookie
        .or(post_login_cookie)
        .or(login_entry.cookie_header)
        .context("login completed but WebVPN cookie header was not found")?;
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

    let cookie_header = webvpn_cookie_header_from_jar(cookie_jar);
    if cookie_header
        .as_deref()
        .and_then(ticket_cookie_from_header)
        .is_some()
    {
        log_success("client", "WebVPN ticket cookie initialized");
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
        cookie_header,
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

fn webvpn_cookie_header_from_jar(cookie_jar: &reqwest::cookie::Jar) -> Option<String> {
    let url = Url::parse("https://webvpn.szut.edu.cn/").ok()?;
    let header = cookie_jar.cookies(&url)?.to_str().ok()?.trim().to_string();
    if header.is_empty() {
        None
    } else {
        Some(header)
    }
}

fn seed_webvpn_cookie_jar(cookie_jar: &reqwest::cookie::Jar, header: &str) {
    let url = Url::parse("https://webvpn.szut.edu.cn/")
        .expect("static WebVPN cookie jar URL must be valid");
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
        return webvpn_cookie_header_from_jar(cookie_jar)
            .context("WebVPN fingerprint activation completed without WebVPN cookies")
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
    println!(
        "Usage: towc\n       towc <tows-ip[:port]> [--target <host:port|port>] [--listen <host:port|port>] [--login <mobile|email>]"
    );
    println!("       server port defaults to {DEFAULT_SERVER_PORT}");
    println!(
        "       --target defaults to {DEFAULT_TARGET_PORT}; --listen defaults to {DEFAULT_LOCAL_LISTEN_PORT}"
    );
    println!(
        "       cached login is always tried first; --login is used only when the cache is missing, malformed, or expired"
    );
    println!("       without cached login or --login, towc uses terminal WeChat QR login");
    println!(
        "       --login sends a verification code by SMS for numeric values, or by email when the value contains @"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingUi {
        events: Mutex<Vec<EmbeddedClientEvent>>,
    }

    impl EmbeddedClientUi for RecordingUi {
        fn emit(&self, event: EmbeddedClientEvent) {
            self.events.lock().unwrap().push(event);
        }

        fn request_verification_code(&self, _label: &str) -> Result<String> {
            anyhow::bail!("not used by this test")
        }
    }

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn no_args_enters_interactive_mode() {
        assert_eq!(parse_args(&args(&[])).unwrap(), ParsedArgs::Interactive);
    }

    #[test]
    fn session_starts_logged_out_without_a_tunnel_dependency() {
        let ui: Arc<dyn EmbeddedClientUi> = Arc::new(RecordingUi::default());
        let session = SessionManager::new(ui);

        assert_eq!(session.state(), SessionState::LoggedOut);
        assert!(session.handle().is_none());
    }

    #[tokio::test]
    async fn multiple_tunnels_wait_for_one_shared_session_independently() {
        let ui = Arc::new(RecordingUi::default());
        let event_ui: Arc<dyn EmbeddedClientUi> = ui.clone();
        let session = SessionManager::new(Arc::clone(&event_ui));
        let tunnels = TunnelManager::new(session.clone(), event_ui);
        let config = TunnelConfig {
            server: "192.0.2.10:4489".to_string(),
            target: "22".to_string(),
            listen_addr: "127.0.0.1:14489".to_string(),
        };
        let first = tunnels.add(config.clone()).unwrap();
        let second = tunnels.add(config).unwrap();

        tunnels.start(first).await.unwrap();
        tunnels.start(second).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if tunnels.handle(first).unwrap().state() == TunnelState::PendingAuth
                    && tunnels.handle(second).unwrap().state() == TunnelState::PendingAuth
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        tunnels.stop(first).await.unwrap();
        assert_eq!(tunnels.handle(first).unwrap().state(), TunnelState::Stopped);
        assert_eq!(
            tunnels.handle(second).unwrap().state(),
            TunnelState::PendingAuth
        );
        assert_eq!(session.state(), SessionState::LoggedOut);
        tunnels.stop(second).await.unwrap();

        let events = ui.events.lock().unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            EmbeddedClientEvent::Tunnel(TunnelEvent::StateChanged {
                tunnel_id,
                state: TunnelState::PendingAuth,
            }) if *tunnel_id == first
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            EmbeddedClientEvent::Tunnel(TunnelEvent::StateChanged {
                tunnel_id,
                state: TunnelState::PendingAuth,
            }) if *tunnel_id == second
        )));
    }

    #[test]
    fn parses_server_first_and_options_in_any_order() {
        let parsed = parse_args(&args(&[
            "192.0.2.10:4489",
            "--login",
            "user@example.com",
            "--listen",
            "13389",
            "--target",
            "3389",
        ]))
        .unwrap();

        assert_eq!(
            parsed,
            ParsedArgs::Run(ClientConfig {
                server: "192.0.2.10:4489".to_string(),
                target: Some("3389".to_string()),
                listen_addr: "13389".to_string(),
                login: Some(VerificationLogin::Email {
                    email: "user@example.com".to_string(),
                }),
            })
        );
    }

    #[test]
    fn rejects_missing_first_server_argument() {
        let err = parse_args(&args(&["--target", "3389"]))
            .unwrap_err()
            .to_string();

        assert!(err.contains("first argument"));
    }

    #[test]
    fn rejects_unknown_argument() {
        let err = parse_args(&args(&["192.0.2.10", "--unknown", "value"]))
            .unwrap_err()
            .to_string();

        assert!(err.contains("unknown argument"));
    }

    #[test]
    fn readiness_summary_describes_webvpn_failed_as_tows_endpoint_issue() {
        let failures = vec![
            ReadinessFailure::WebVpnFailed {
                location: "/wengine-vpn/failed".to_string(),
            };
            6
        ];

        let lines = readiness_failure_summary_lines(&failures, "192.0.2.10:4489", "127.0.0.1:22");

        assert_eq!(
            lines[0],
            "readiness failed after 6 attempts: WebVPN returned /wengine-vpn/failed x6"
        );
        assert!(lines[1].contains("before tows accepted WebSocket"));
        assert!(lines[2].contains("likely cause: tows is not running/reachable"));
    }

    #[test]
    fn readiness_summary_describes_reset_as_probable_target_issue() {
        let failures = vec![ReadinessFailure::ResetAfterOpen; 6];

        let lines =
            readiness_failure_summary_lines(&failures, "192.0.2.10:4489", "127.0.0.1:54162");

        assert_eq!(
            lines[0],
            "readiness failed after 6 attempts: WebSocket reset after opening x6"
        );
        assert!(lines[1].contains("WebVPN reached tows"));
        assert!(lines[2].contains("likely cause: target 127.0.0.1:54162"));
    }

    #[test]
    fn readiness_close_reason_can_confirm_target_connect_failure() {
        let reason = format!(
            "{TOWS_TARGET_CONNECT_FAILURE_PREFIX}: 127.0.0.1:54162: Connection refused (os error 111)"
        );

        let failure = readiness_failure_from_close_reason(Some(reason.clone()));

        assert_eq!(
            failure,
            ReadinessFailure::TargetConnectFailed {
                reason: reason.clone()
            }
        );

        let lines =
            readiness_failure_summary_lines(&[failure], "192.0.2.10:4489", "127.0.0.1:54162");
        assert!(lines[1].contains("then failed to connect target 127.0.0.1:54162"));
        assert!(lines[2].contains("cause: target TCP connection failed"));
    }

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
    fn detects_ticket_cookie_inside_full_webvpn_cookie_header() {
        let header = "heartbeat=abc; wengine_vpn_ticketwebvpn_szut_edu_cn=ticket; refresh=xyz";

        assert_eq!(
            ticket_cookie_from_header(header),
            Some("wengine_vpn_ticketwebvpn_szut_edu_cn=ticket")
        );
    }

    #[test]
    fn rejects_empty_ticket_cookie() {
        let header = "heartbeat=abc; wengine_vpn_ticketwebvpn_szut_edu_cn=; refresh=xyz";

        assert_eq!(ticket_cookie_from_header(header), None);
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
    fn interactive_defaults_cache_round_trips_all_addresses() {
        let defaults = InteractiveDefaults {
            server: "192.0.2.10:54489".to_string(),
            target: "10.0.0.8:3389".to_string(),
            listen_addr: "127.0.0.1:13389".to_string(),
        };

        assert_eq!(
            parse_interactive_defaults(&format_interactive_defaults(&defaults)).unwrap(),
            defaults
        );
    }

    #[test]
    fn invalid_interactive_defaults_cache_is_rejected() {
        assert!(
            parse_interactive_defaults("version=2\nserver=192.0.2.10\ntarget=22\nlisten=14489\n")
                .is_err()
        );
        assert!(parse_interactive_defaults("version=1\nserver=192.0.2.10\ntarget=22\n").is_err());
        assert!(
            parse_interactive_defaults("version=1\nserver=192.0.2.10:0\ntarget=22\nlisten=14489\n")
                .is_err()
        );
    }

    #[test]
    fn interactive_defaults_cache_accepts_built_in_port_shorthand() {
        assert_eq!(
            parse_interactive_defaults("version=1\nserver=192.0.2.10\ntarget=22\nlisten=14489\n")
                .unwrap(),
            InteractiveDefaults {
                server: "192.0.2.10".to_string(),
                target: "22".to_string(),
                listen_addr: "14489".to_string(),
            }
        );
    }

    #[test]
    fn interactive_messages_use_consistent_prompt_style() {
        assert_eq!(LOGIN_METHOD_PROMPT.matches(':').count(), 1);
        assert_eq!(
            LOGIN_METHOD_PROMPT,
            "login method (enter mobile/email, or press Enter for WeChat QR): "
        );
    }
}
