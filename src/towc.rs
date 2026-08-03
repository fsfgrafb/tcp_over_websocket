use crate::{
    ConnectFailure, DEFAULT_TARGET_HOST, TOWS_READY_MESSAGE, TOWS_TARGET_CONNECT_FAILURE_PREFIX,
    WebVpnHeartbeatRole, build_webvpn_keepalive_ws_url, build_webvpn_ws_url, connect_websocket,
    log_info, log_success, log_warn, normalize_server_addr, normalize_tcp_target_arg,
    parse_socket_addr_with_default_host, relay_stream, rsa_encrypt, run_webvpn_heartbeat_websocket,
};
#[cfg(feature = "cli")]
use crate::{
    DEFAULT_LOCAL_LISTEN_ADDR, DEFAULT_LOCAL_LISTEN_PORT, DEFAULT_SERVER_PORT, DEFAULT_TARGET_PORT,
    log_error,
};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::cookie::CookieStore;
use reqwest::header::{ORIGIN, REFERER, USER_AGENT};
use reqwest::{Client, Url};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io;
#[cfg(feature = "cli")]
use std::io::Write;
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

#[cfg(feature = "cli")]
mod qr;

const WEBVPN_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/login";
const WEBVPN_TICKET_COOKIE_PREFIX: &str = "wengine_vpn_ticketwebvpn_szut_edu_cn=";
const WEBVPN_CAS_HASH: &str = "77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d";
const WEBVPN_CAS_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/https/77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d/cas/login?service=https%3A%2F%2Fwebvpn.szut.edu.cn%2Flogin%3Fcas_login%3Dtrue";
const WEBVPN_PORTAL_LOGIN_URL: &str = "https://webvpn.szut.edu.cn/https/77726476706e69737468656265737421f3f652d2342a7d44300d8db9d/cas/login?service=https%3A%2F%2Fcas.szut.edu.cn%2F";
const WEBVPN_PERSONAL_CENTER_URL: &str = "https://webvpn.szut.edu.cn/https/77726476706e69737468656265737421f3f652d2342a7d44300d8db9d/personal-center";
const WEBVPN_WECHAT_HASH: &str =
    "77726476706e69737468656265737421ffe7449269276d59660187e289446d36a8d6";
const WECHAT_APP_ID: &str = "wx16c67d169e7a9290";
const WECHAT_REDIRECT_URI: &str = "https://cas.szut.edu.cn/cas/login?service=https%3A%2F%2Fwebvpn.szut.edu.cn%2Flogin%3Fcas_login%3Dtrue&client_name=WeiXinClient";
const BROWSER_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0";
const WEBVPN_FINGERPRINT: &str = "5a0b00fe6ae8277a4bfadd4e103f6e1c";
const WEBVPN_READY_TIMEOUT_SECS: u64 = 8;
const CACHED_LOGIN_TIMEOUT_SECS: u64 = 15;
const WEBVPN_COOKIE_REFRESH_INTERVAL_SECS: u64 = 180;
const WEBVPN_COOKIE_REFRESH_TIMEOUT_SECS: u64 = 8;
const CAS_LOGIN_ATTEMPTS: usize = 2;
const CAS_LOGIN_RETRY_SETTLE_MS: u64 = 1500;
const WECHAT_POLL_ATTEMPTS: usize = 180;
const WECHAT_POLL_TIMEOUT_SECS: u64 = 35;
const WECHAT_POLL_SETTLE_MS: u64 = 1800;
const COOKIE_CACHE_FILE_NAME: &str = "webvpn.cookie";
/// Environment variable overriding the built-in Cookie/defaults cache directory.
///
/// A process manager can assign a distinct value to every `towc` child process
/// to avoid cross-process cache coordination.
pub const CACHE_DIRECTORY_ENV: &str = "TCP_OVER_WEBSOCKET_CACHE_DIR";
#[cfg(feature = "cli")]
const INTERACTIVE_DEFAULTS_CACHE_FILE_NAME: &str = "interactive.defaults";
#[cfg(feature = "cli")]
const INTERACTIVE_DEFAULTS_CACHE_VERSION: &str = "1";
#[cfg(feature = "cli")]
const LOGIN_METHOD_PROMPT: &str =
    "login method (enter mobile/email, or press Enter for WeChat QR): ";
const TUNNEL_RETRY_INTERVAL_SECS: u64 = 5;
const WEBVPN_KEEPALIVE_RECONNECT_SECS: u64 = 5;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Interactive login method used when a cached WebVPN session is unavailable.
pub enum LoginMethod {
    /// Request a verification code by SMS.
    Sms {
        /// Mobile number receiving the code.
        mobile: String,
    },
    /// Request a verification code by email.
    Email {
        /// Email address receiving the code.
        email: String,
    },
    /// Display a WeChat QR code and wait for confirmation.
    WechatQr,
}

impl LoginMethod {
    /// Validates the login identifier without performing network I/O.
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

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Observable lifecycle state of a shared WebVPN session.
pub enum SessionState {
    /// No usable login session exists.
    LoggedOut,
    /// A login flow is currently running.
    LoggingIn {
        /// Login method being used.
        method: LoginMethod,
    },
    /// The session is authenticated and can open tunnels.
    Ready,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Detailed session events delivered to an embedded UI.
pub enum SessionEvent {
    /// Cached credentials are being checked.
    CheckingCachedCookie,
    /// No valid cached credentials were available.
    CachedCookieUnavailable,
    /// An interactive login flow started.
    LoggingIn {
        /// Login method being used.
        method: LoginMethod,
    },
    /// A verification code must be supplied by the UI.
    CodeRequested {
        /// Human-readable identifier for the requested code.
        label: String,
    },
    /// A QR code is ready for display.
    QrCode {
        /// JPEG-encoded QR image.
        jpeg: Vec<u8>,
    },
    /// Authentication completed successfully.
    Ready,
    /// The gateway reported that the active session expired.
    Expired,
    /// The session was explicitly logged out.
    LoggedOut,
    /// A login or refresh operation failed.
    Error {
        /// Human-readable error chain.
        detail: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Configuration for one independently managed local forwarding tunnel.
pub struct TunnelConfig {
    /// Remote `tows` host or host-port.
    pub server: String,
    /// TCP target on the `tows` host, optionally using port-only shorthand.
    pub target: String,
    /// Local listener address, optionally using port-only shorthand.
    pub listen_addr: String,
}

impl TunnelConfig {
    /// Validates and normalizes all addresses without opening sockets.
    pub fn validate(&self) -> Result<()> {
        normalize_server_addr(&self.server).context("invalid tows address")?;
        normalize_tcp_target_arg(Some(&self.target)).context("invalid target address")?;
        parse_socket_addr_with_default_host(&self.listen_addr, DEFAULT_TARGET_HOST)
            .context("invalid listen address")?;
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Observable lifecycle state of a local tunnel.
pub enum TunnelState {
    /// Stored but not started.
    Configured,
    /// Started and waiting for an authenticated session.
    PendingAuth,
    /// Checking gateway, server, and target readiness.
    Probing,
    /// Listening locally and accepting connections.
    Running,
    /// Waiting before another readiness attempt.
    Retrying,
    /// Stopped because of a non-retryable configuration or listener error.
    Failed,
    /// Explicitly stopped or canceled.
    Stopped,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Detailed per-tunnel events delivered to an embedded UI.
pub enum TunnelEvent {
    /// The tunnel lifecycle state changed.
    StateChanged {
        /// Tunnel identifier assigned by [`TunnelManager`].
        tunnel_id: u64,
        /// New tunnel state.
        state: TunnelState,
    },
    /// A local TCP connection was accepted.
    LocalConnectionOpened {
        /// Owning tunnel identifier.
        tunnel_id: u64,
        /// Local peer address.
        peer: String,
    },
    /// A previously opened local TCP connection ended.
    LocalConnectionClosed {
        /// Owning tunnel identifier.
        tunnel_id: u64,
        /// Local peer address.
        peer: String,
    },
    /// A tunnel or one of its connections encountered an error.
    Error {
        /// Owning tunnel identifier.
        tunnel_id: u64,
        /// Human-readable error chain.
        detail: String,
    },
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Unified event stream for embedded client integrations.
pub enum EmbeddedClientEvent {
    /// Shared login-session event.
    Session(SessionEvent),
    /// Per-tunnel event.
    Tunnel(TunnelEvent),
}

/// UI boundary used by embedded applications.
///
/// Callbacks run on library tasks and should return quickly. Dispatch expensive
/// rendering or user interaction to the host application's own executor.
pub trait EmbeddedClientUi: Send + Sync {
    /// Receives an ordered session or tunnel event.
    fn emit(&self, event: EmbeddedClientEvent);
    /// Obtains a verification code after a matching `CodeRequested` event.
    fn request_verification_code(&self, label: &str) -> Result<String>;
}

#[derive(Clone)]
/// Handle to the currently authenticated session.
pub struct SessionHandle {
    cookie: Arc<Mutex<String>>,
}

impl SessionHandle {
    fn cookie(&self) -> &Arc<Mutex<String>> {
        &self.cookie
    }
}

#[derive(Clone)]
/// Owns login state and the background Cookie refresh lifecycle.
pub struct SessionManager {
    inner: Arc<SessionManagerInner>,
}

struct SessionManagerInner {
    state: Mutex<SessionState>,
    state_tx: watch::Sender<SessionState>,
    login_lock: tokio::sync::Mutex<()>,
    login_epoch: AtomicU64,
    keepalive_url: Mutex<Option<String>>,
    runtime: Mutex<Option<SessionRuntime>>,
    ui: Arc<dyn EmbeddedClientUi>,
}

struct SessionRuntime {
    handle: SessionHandle,
    cancel: watch::Sender<bool>,
    keepalive_task: Option<JoinHandle<()>>,
    refresh_task: JoinHandle<()>,
}

impl Drop for SessionRuntime {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = &self.keepalive_task {
            task.abort();
        }
        self.refresh_task.abort();
    }
}

impl SessionManager {
    /// Creates a logged-out session manager using `ui` for callbacks.
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
                keepalive_url: Mutex::new(None),
                runtime: Mutex::new(None),
                ui,
            }),
        }
    }

    /// Returns a snapshot of the current session state.
    pub fn state(&self) -> SessionState {
        self.inner
            .state
            .lock()
            .expect("session state poisoned")
            .clone()
    }

    /// Subscribes to session state changes.
    pub fn subscribe(&self) -> watch::Receiver<SessionState> {
        self.inner.state_tx.subscribe()
    }

    /// Returns the active handle when the session is ready.
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

    /// Runs an interactive login unless the manager is already ready.
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

        Ok(self.activate_session(cookie, None))
    }

    /// Validates cached credentials and starts refresh when they are usable.
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
        Ok(Some(self.activate_session(cookie, None)))
    }

    /// Validates cached credentials against the configured `tows` endpoint.
    ///
    /// Unlike portal refresh validation, this performs one direct WebSocket
    /// handshake through the same gateway route used by tunnels. A redirect to
    /// login is therefore detected before the tunnel manager is started.
    pub async fn login_with_cached_cookie_for_server(
        &self,
        server: &str,
    ) -> Result<Option<SessionHandle>> {
        self.configure_keepalive_server(server)?;
        let _login_guard = self.inner.login_lock.lock().await;
        if let Some(handle) = self.handle() {
            return Ok(Some(handle));
        }
        let login_epoch = self.inner.login_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.emit(SessionEvent::CheckingCachedCookie);
        let cached_cookie = match try_cached_tunnel_login(server).await {
            Ok(CachedCookieCheck::Ready { cookie, websocket }) => Some((cookie, Some(*websocket))),
            Ok(CachedCookieCheck::EndpointUnavailable(cookie)) => Some((cookie, None)),
            Ok(CachedCookieCheck::Unavailable) => {
                self.emit(SessionEvent::CachedCookieUnavailable);
                None
            }
            Ok(CachedCookieCheck::Expired) => {
                self.emit(SessionEvent::Expired);
                None
            }
            Err(err) => {
                self.emit(SessionEvent::Error {
                    detail: format!("{err:#}"),
                });
                return Err(err);
            }
        };
        let Some((cookie, initial_keepalive)) = cached_cookie else {
            return Ok(None);
        };
        if self.inner.login_epoch.load(Ordering::Acquire) != login_epoch {
            return Ok(None);
        }
        Ok(Some(self.activate_session(cookie, initial_keepalive)))
    }

    fn configure_keepalive_server(&self, server: &str) -> Result<()> {
        let keepalive_url = build_webvpn_keepalive_ws_url(server)?;
        *self
            .inner
            .keepalive_url
            .lock()
            .expect("session keepalive URL poisoned") = Some(keepalive_url);
        Ok(())
    }

    /// Cancels refresh and transitions the session to logged out.
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

    fn activate_session(
        &self,
        cookie: String,
        initial_keepalive: Option<WebVpnClientWebSocket>,
    ) -> SessionHandle {
        let handle = SessionHandle {
            cookie: Arc::new(Mutex::new(cookie)),
        };
        let (cancel, cancel_rx) = watch::channel(false);
        let keepalive_url = self
            .inner
            .keepalive_url
            .lock()
            .expect("session keepalive URL poisoned")
            .clone();
        let keepalive_task = keepalive_url.map(|url| {
            tokio::spawn(maintain_session_keepalive(
                url,
                handle.clone(),
                initial_keepalive,
                cancel_rx.clone(),
                Arc::downgrade(&self.inner),
            ))
        });
        let refresh_task = tokio::spawn(maintain_session_cookie_refresh(
            handle.clone(),
            cancel_rx,
            Arc::downgrade(&self.inner),
        ));
        *self.inner.runtime.lock().expect("session runtime poisoned") = Some(SessionRuntime {
            handle: handle.clone(),
            cancel,
            keepalive_task,
            refresh_task,
        });
        self.set_state(SessionState::Ready);
        self.emit(SessionEvent::Ready);
        handle
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
/// Lightweight observable handle for a configured tunnel.
pub struct TunnelHandle {
    id: u64,
    state: Arc<Mutex<TunnelState>>,
}

impl TunnelHandle {
    /// Returns the manager-assigned tunnel identifier.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns a snapshot of the current tunnel state.
    pub fn state(&self) -> TunnelState {
        *self.state.lock().expect("tunnel state poisoned")
    }
}

#[derive(Clone)]
/// Owns tunnel lifecycle state for embedded integrations.
///
/// The command-line client creates exactly one tunnel. Identifiers remain in
/// the structured API so a future host can correlate lifecycle events.
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

struct LocalConnectionEventGuard {
    ui: Arc<dyn EmbeddedClientUi>,
    tunnel_id: u64,
    peer: String,
}

impl Drop for LocalConnectionEventGuard {
    fn drop(&mut self) {
        self.ui.emit(EmbeddedClientEvent::Tunnel(
            TunnelEvent::LocalConnectionClosed {
                tunnel_id: self.tunnel_id,
                peer: self.peer.clone(),
            },
        ));
    }
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
    /// Creates a tunnel manager bound to a shared session and UI sink.
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

    /// Validates and stores a tunnel, returning its identifier.
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

    /// Returns an observable handle for a known tunnel.
    pub fn handle(&self, id: u64) -> Option<TunnelHandle> {
        self.inner
            .tunnels
            .lock()
            .expect("tunnel map poisoned")
            .get(&id)
            .map(|runtime| runtime.handle.clone())
    }

    /// Starts a tunnel; starting an already-running tunnel is a no-op.
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

    /// Stops a tunnel and all active local connections.
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

    /// Stops and removes a tunnel from the manager.
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
    let mut last_probe_error: Option<String> = None;
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
            let detail = format!("{err:#}");
            if last_probe_error.as_deref() != Some(detail.as_str()) {
                emit_tunnel_error(&ui, handle.id, detail.clone());
                last_probe_error = Some(detail);
            }
            set_tunnel_state(&handle, &ui, TunnelState::Retrying);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(TUNNEL_RETRY_INTERVAL_SECS)) => {}
                _ = cancellation_requested(&mut cancel) => break,
                _ = session_state.changed() => {}
            }
            continue;
        }
        last_probe_error = None;

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
                            let connection_event_guard = LocalConnectionEventGuard {
                                ui: Arc::clone(&ui),
                                tunnel_id: handle.id,
                                peer: peer.clone(),
                            };
                            connections.spawn(async move {
                                let _connection_event_guard = connection_event_guard;
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
#[cfg(feature = "cli")]
struct ClientConfig {
    server: String,
    target: Option<String>,
    listen_addr: String,
    login: Option<VerificationLogin>,
}

#[cfg(feature = "cli")]
struct InteractiveForwardingConfig {
    target: String,
    listen_addr: String,
}

#[cfg(feature = "cli")]
struct InteractiveStartup {
    config: ClientConfig,
    cached_cookie_preflight: JoinHandle<Result<CachedCookieCheck>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(feature = "cli")]
struct InteractiveDefaults {
    server: String,
    target: String,
    listen_addr: String,
}

type WebVpnClientWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, PartialEq, Eq)]
#[cfg(feature = "cli")]
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
    ReadyTimedOut,
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
    ReadyTimedOut,
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
            Self::ReadyTimedOut => ReadinessFailureKind::ReadyTimedOut,
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
            Self::ReadyTimedOut => "tows readiness acknowledgement timed out",
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
            Self::ReadyTimedOut => vec![
                format!(
                    "phase: WebVPN opened the route to {server_addr}, but tows did not confirm target {target_addr}"
                ),
                "likely cause: tows is outdated/stalled, or target TCP connect did not complete".to_string(),
                "check: update/restart tows and verify the target is reachable from its host".to_string(),
            ],
        }
    }
}

#[cfg(feature = "cli")]
struct TerminalUi {
    ready_message: String,
}

#[cfg(feature = "cli")]
impl TerminalUi {
    fn new(config: &TunnelConfig) -> Self {
        Self {
            ready_message: format!(
                "ready: local {} -> WebVPN -> tows {} -> target {}",
                config.listen_addr, config.server, config.target
            ),
        }
    }
}

#[cfg(feature = "cli")]
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
            EmbeddedClientEvent::Tunnel(TunnelEvent::StateChanged {
                state: TunnelState::Running,
                ..
            }) => {
                log_success("tunnel", &self.ready_message);
            }
            EmbeddedClientEvent::Tunnel(TunnelEvent::LocalConnectionOpened { peer, .. }) => {
                log_success("tunnel", format!("tcp {peer} connected"))
            }
            EmbeddedClientEvent::Tunnel(TunnelEvent::LocalConnectionClosed { peer, .. }) => {
                log_info("tunnel", format!("tcp {peer} closed"))
            }
            EmbeddedClientEvent::Tunnel(TunnelEvent::Error { detail, .. }) => {
                log_error("tunnel", detail);
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
/// Convenience configuration for the single-tunnel embedded runner.
pub struct EmbeddedClientConfig {
    /// Remote `tows` host or host-port.
    pub server: String,
    /// TCP target on the server.
    pub target: String,
    /// Local listener address.
    pub listen_addr: String,
    /// Fallback login method when cached credentials are unavailable.
    pub login: LoginMethod,
}

/// Backward-compatible name for [`LoginMethod`].
pub type EmbeddedLogin = LoginMethod;

/// Runs one authenticated tunnel until `shutdown` becomes true or closes.
pub async fn run_embedded_client(
    config: EmbeddedClientConfig,
    ui: Arc<dyn EmbeddedClientUi>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    install_crypto_provider();
    let login = config.login.clone();
    let session = SessionManager::new(Arc::clone(&ui));
    let tunnels = TunnelManager::new(session.clone(), ui);
    if session
        .login_with_cached_cookie_for_server(&config.server)
        .await?
        .is_none()
    {
        session.login(login.clone()).await?;
    }
    let id = tunnels.add(TunnelConfig {
        server: config.server,
        target: config.target,
        listen_addr: config.listen_addr,
    })?;
    tunnels.start(id).await?;
    let mut session_state = session.subscribe();
    while !*shutdown.borrow() {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            changed = session_state.changed() => {
                if changed.is_err() {
                    break;
                }
                if *session_state.borrow() == SessionState::LoggedOut {
                    tokio::select! {
                        login_result = session.login(login.clone()) => {
                            login_result.context("failed to restore expired WebVPN session")?;
                        }
                        _ = shutdown.changed() => break,
                    }
                }
            }
        }
    }
    tunnels.stop(id).await?;
    session.logout();
    Ok(())
}

/// Returns the directory used by the built-in credential and defaults cache.
pub fn cache_directory() -> Option<PathBuf> {
    cookie_cache_path().and_then(|path| path.parent().map(PathBuf::from))
}

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Runs the `towc` command-line interface.
#[cfg(feature = "cli")]
pub async fn run_cli() -> Result<()> {
    log_info("client", format!("towc v{}", env!("CARGO_PKG_VERSION")));
    install_crypto_provider();

    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let parsed_args = parse_args(&raw_args)?;
    let (config, mut login_method, cached_cookie_preflight) = match parsed_args {
        ParsedArgs::Help => {
            print_usage();
            return Ok(());
        }
        ParsedArgs::Interactive => {
            let startup = prepare_interactive_startup().await?;
            (startup.config, None, Some(startup.cached_cookie_preflight))
        }
        ParsedArgs::Run(config) => {
            let login = config
                .login
                .clone()
                .map(login_method_from_verification)
                .unwrap_or(LoginMethod::WechatQr);
            (config, Some(login), None)
        }
    };
    let tunnel_config = TunnelConfig {
        server: normalize_server_addr(&config.server)?,
        target: normalize_tcp_target_arg(config.target.as_deref())?,
        listen_addr: parse_socket_addr_with_default_host(&config.listen_addr, DEFAULT_TARGET_HOST)?
            .to_string(),
    };
    let ui: Arc<dyn EmbeddedClientUi> = Arc::new(TerminalUi::new(&tunnel_config));
    let session = SessionManager::new(Arc::clone(&ui));
    let tunnels = TunnelManager::new(session.clone(), ui);
    let cached_session = if let Some(preflight) = cached_cookie_preflight {
        session.configure_keepalive_server(&tunnel_config.server)?;
        session.emit(SessionEvent::CheckingCachedCookie);
        match preflight.await {
            Ok(Ok(CachedCookieCheck::Ready { cookie, websocket })) => {
                Some(session.activate_session(cookie, Some(*websocket)))
            }
            Ok(Ok(CachedCookieCheck::Expired)) => {
                session.emit(SessionEvent::Expired);
                None
            }
            Ok(Ok(CachedCookieCheck::Unavailable | CachedCookieCheck::EndpointUnavailable(_)))
            | Ok(Err(_))
            | Err(_) => {
                session.emit(SessionEvent::CachedCookieUnavailable);
                None
            }
        }
    } else {
        session
            .login_with_cached_cookie_for_server(&tunnel_config.server)
            .await?
    };
    if cached_session.is_none() {
        let login = match login_method.clone() {
            Some(login) => login,
            None => prompt_login_identity()?
                .map(login_method_from_verification)
                .unwrap_or(LoginMethod::WechatQr),
        };
        login_method = Some(login.clone());
        session.login(login).await?;
    }
    let id = tunnels.add(tunnel_config)?;
    tunnels.start(id).await?;
    let mut session_state = session.subscribe();
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            shutdown_result = &mut shutdown => {
                shutdown_result?;
                break;
            }
            changed = session_state.changed() => {
                if changed.is_err() {
                    break;
                }
                if *session_state.borrow() == SessionState::LoggedOut {
                    let login = match login_method.clone() {
                        Some(login) => login,
                        None => prompt_login_identity()?
                            .map(login_method_from_verification)
                            .unwrap_or(LoginMethod::WechatQr),
                    };
                    login_method = Some(login.clone());
                    tokio::select! {
                        login_result = session.login(login) => {
                            login_result.context("failed to restore expired WebVPN session")?;
                        }
                        shutdown_result = &mut shutdown => {
                            shutdown_result?;
                            break;
                        }
                    }
                }
            }
        }
    }
    log_info("client", "shutting down");
    tunnels.stop(id).await?;
    session.logout();
    Ok(())
}

#[cfg(feature = "cli")]
async fn prepare_interactive_startup() -> Result<InteractiveStartup> {
    let cached_defaults = read_cached_interactive_defaults();
    let server = match &cached_defaults {
        Some(defaults) => prompt_with_default(
            &format!("tows address/port (default: {}): ", defaults.server),
            &defaults.server,
        )?,
        None => prompt_required("tows address/port: ")?,
    };
    let preflight_server = server.clone();
    let cached_cookie_preflight =
        tokio::spawn(async move { try_cached_tunnel_login(&preflight_server).await });
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

    Ok(InteractiveStartup {
        config: ClientConfig {
            server,
            target: Some(forwarding.target),
            listen_addr: forwarding.listen_addr,
            login: None,
        },
        cached_cookie_preflight,
    })
}

#[cfg(feature = "cli")]
fn login_method_from_verification(login: VerificationLogin) -> LoginMethod {
    match login {
        VerificationLogin::Sms { mobile } => LoginMethod::Sms { mobile },
        VerificationLogin::Email { email } => LoginMethod::Email { email },
    }
}

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
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
    let verification = async {
        match refresh_portal_cookie_once(&client, &cookie_jar).await? {
            // A response from the Cookie endpoint is not sufficient evidence that
            // the ticket is usable. WebVPN still requires the CAS/portal route to
            // activate the ticket before a protected portal page can be opened.
            PortalCookieRefresh::Refreshed(_) => {
                activate_cached_portal_session(&client, &cookie_jar).await
            }
            PortalCookieRefresh::Expired => Ok(None),
        }
    };
    match tokio::time::timeout(Duration::from_secs(CACHED_LOGIN_TIMEOUT_SECS), verification).await {
        Ok(Ok(Some(cookie))) => {
            write_cached_cookie(&cookie);
            Ok(Some(cookie))
        }
        Ok(Ok(None)) => Ok(None),
        Ok(Err(err)) => Err(err).context("failed to verify cached WebVPN login"),
        Err(_) => anyhow::bail!("timed out while verifying cached WebVPN login"),
    }
}

async fn activate_cached_portal_session(
    client: &Client,
    cookie_jar: &reqwest::cookie::Jar,
) -> Result<Option<String>> {
    let login_response = client
        .get(WEBVPN_PORTAL_LOGIN_URL)
        .header(REFERER, WEBVPN_LOGIN_URL)
        .send()
        .await
        .context("failed to open the WebVPN portal login entry")?
        .error_for_status()
        .context("WebVPN portal login entry request failed")?;
    let login_final_url = login_response.url().to_string();
    let login_body = login_response
        .text()
        .await
        .context("failed to read the WebVPN portal login response")?;
    if is_cas_login_form(&login_final_url, extract_execution(&login_body).as_deref()) {
        return Ok(None);
    }
    let _ = activate_webvpn_fingerprint_if_needed(client, cookie_jar, &login_final_url).await?;

    // Re-open the protected page after the login redirect. This is the actual
    // acceptance criterion for a cached ticket; a freshly set Cookie alone is
    // deliberately not accepted.
    for _ in 0..2 {
        let response = client
            .get(WEBVPN_PERSONAL_CENTER_URL)
            .header(REFERER, WEBVPN_PORTAL_LOGIN_URL)
            .send()
            .await
            .context("failed to open the WebVPN personal center")?
            .error_for_status()
            .context("WebVPN personal center request failed")?;
        let final_url = response.url().to_string();
        if is_webvpn_fingerprint_url(&final_url) {
            activate_webvpn_fingerprint_if_needed(client, cookie_jar, &final_url).await?;
            continue;
        }
        if !is_webvpn_personal_center_url(&final_url) {
            return Ok(None);
        }

        let cookie = webvpn_cookie_header_from_jar(cookie_jar)
            .context("WebVPN portal activation completed without WebVPN cookies")?;
        if ticket_cookie_from_header(&cookie).is_none() {
            return Ok(None);
        }
        return Ok(Some(cookie));
    }

    Ok(None)
}

enum CachedCookieCheck {
    Unavailable,
    Expired,
    EndpointUnavailable(String),
    Ready {
        cookie: String,
        websocket: Box<WebVpnClientWebSocket>,
    },
}

fn cached_cookie_check_from_failure(
    cookie: String,
    failure: ConnectFailure,
) -> Result<CachedCookieCheck> {
    match failure {
        ConnectFailure::CookieExpired { .. } => Ok(CachedCookieCheck::Expired),
        // `/wengine-vpn/failed` is returned after authentication when WebVPN
        // cannot reach tows. The Cookie is valid; tunnel recovery handles the
        // endpoint outage separately.
        ConnectFailure::WebVpnFailed { .. } => Ok(CachedCookieCheck::EndpointUnavailable(cookie)),
        ConnectFailure::Other(err) => {
            Err(err).context("failed to verify cached WebVPN login through tunnel endpoint")
        }
    }
}

async fn try_cached_tunnel_login(server: &str) -> Result<CachedCookieCheck> {
    let Some(cookie) = read_cached_cookie() else {
        return Ok(CachedCookieCheck::Unavailable);
    };
    if ticket_cookie_from_header(&cookie).is_none()
        || HeaderValue::from_bytes(cookie.as_bytes()).is_err()
    {
        return Ok(CachedCookieCheck::Unavailable);
    }

    let url = build_webvpn_keepalive_ws_url(server)?;
    let result = tokio::time::timeout(
        Duration::from_secs(CACHED_LOGIN_TIMEOUT_SECS),
        connect_websocket(&url, &cookie),
    )
    .await
    .context("timed out while checking cached WebVPN cookie against tunnel endpoint")?;

    match result {
        Ok(websocket) => Ok(CachedCookieCheck::Ready {
            cookie,
            websocket: Box::new(websocket),
        }),
        Err(failure) => cached_cookie_check_from_failure(cookie, failure),
    }
}

async fn maintain_session_keepalive(
    url: String,
    session: SessionHandle,
    mut initial_websocket: Option<WebVpnClientWebSocket>,
    mut cancel: watch::Receiver<bool>,
    manager: Weak<SessionManagerInner>,
) {
    loop {
        let connection = if let Some(websocket) = initial_websocket.take() {
            Ok(websocket)
        } else {
            tokio::select! {
                result = connect_websocket_with_current_cookie(&url, session.cookie()) => result,
                _ = cancellation_requested(&mut cancel) => return,
            }
        };

        match connection {
            Ok(websocket) => {
                tokio::select! {
                    _ = run_webvpn_heartbeat_websocket(
                        websocket,
                        WebVpnHeartbeatRole::Client,
                    ) => {}
                    _ = cancellation_requested(&mut cancel) => return,
                }
            }
            Err(ConnectFailure::CookieExpired { .. }) => {
                if let Some(inner) = manager.upgrade() {
                    SessionManager { inner }.mark_expired_for(&session);
                }
                return;
            }
            Err(ConnectFailure::WebVpnFailed { .. } | ConnectFailure::Other(_)) => {}
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(WEBVPN_KEEPALIVE_RECONNECT_SECS)) => {}
            _ = cancellation_requested(&mut cancel) => return,
        }
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

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
fn is_help_arg(value: &str) -> bool {
    value == "--help" || value == "-h"
}

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
fn prompt_optional(prompt: &str) -> Result<Option<String>> {
    Ok(prompt_line(prompt)?.filter(|value| !value.is_empty()))
}

#[cfg(feature = "cli")]
fn prompt_with_default(prompt: &str, default: &str) -> Result<String> {
    Ok(prompt_optional(prompt)?.unwrap_or_else(|| default.to_string()))
}

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
fn validate_interactive_defaults(defaults: &InteractiveDefaults) -> Result<()> {
    normalize_server_addr(&defaults.server).context("invalid tows address")?;
    normalize_tcp_target_arg(Some(&defaults.target)).context("invalid target address")?;
    parse_socket_addr_with_default_host(&defaults.listen_addr, DEFAULT_TARGET_HOST)
        .context("invalid listen address")?;
    Ok(())
}

#[cfg(feature = "cli")]
fn format_interactive_defaults(defaults: &InteractiveDefaults) -> String {
    format!(
        "version={INTERACTIVE_DEFAULTS_CACHE_VERSION}\nserver={}\ntarget={}\nlisten={}\n",
        defaults.server, defaults.target, defaults.listen_addr
    )
}

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
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
    if let Some(path) = configured_cache_directory() {
        return Some(path.join(file_name));
    }
    std::env::var_os("APPDATA")
        .or_else(|| std::env::var_os("LOCALAPPDATA"))
        .map(PathBuf::from)
        .map(|path| path.join("tcp_over_websocket").join(file_name))
}

#[cfg(not(windows))]
fn cache_file_path(file_name: &str) -> Option<PathBuf> {
    if let Some(path) = configured_cache_directory() {
        return Some(path.join(file_name));
    }
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))?;

    Some(base.join("tcp_over_websocket").join(file_name))
}

fn configured_cache_directory() -> Option<PathBuf> {
    std::env::var_os(CACHE_DIRECTORY_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn cookie_cache_path() -> Option<PathBuf> {
    cache_file_path(COOKIE_CACHE_FILE_NAME)
}

#[cfg(feature = "cli")]
fn interactive_defaults_cache_path() -> Option<PathBuf> {
    cache_file_path(INTERACTIVE_DEFAULTS_CACHE_FILE_NAME)
}

async fn wait_for_webvpn_ready(
    url: &str,
    cookie: &Arc<Mutex<String>>,
    server_addr: &str,
    target_addr: &str,
) -> Result<()> {
    let failure = match probe_webvpn_ready(url, cookie).await {
        Ok(()) => return Ok(()),
        Err(failure) => failure,
    };

    let diagnosis = failure
        .diagnostic_lines(server_addr, target_addr)
        .join("; ");
    Err(anyhow::Error::new(failure.kind()).context(format!(
        "readiness failed: {}; {diagnosis}",
        failure.observation_label()
    )))
}

async fn probe_webvpn_ready(
    url: &str,
    cookie: &Arc<Mutex<String>>,
) -> std::result::Result<(), ReadinessFailure> {
    let mut websocket = connect_websocket_with_current_cookie(url, cookie)
        .await
        .map_err(readiness_failure_from_connect_failure)?;

    let ready = match tokio::time::timeout(Duration::from_secs(WEBVPN_READY_TIMEOUT_SECS), async {
        loop {
            match websocket.next().await {
                Some(Ok(Message::Text(text))) if text.as_str() == TOWS_READY_MESSAGE => {
                    return Ok(());
                }
                Some(Ok(Message::Ping(payload))) => {
                    websocket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(readiness_failure_from_websocket_error)?;
                }
                Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                Some(Ok(Message::Close(frame))) => {
                    return Err(readiness_failure_from_close_reason(
                        frame.map(|frame| frame.reason.to_string()),
                    ));
                }
                Some(Ok(_)) => {
                    return Err(ReadinessFailure::ReadFailed {
                        detail: "received data before the tows readiness acknowledgement"
                            .to_string(),
                    });
                }
                Some(Err(err)) => {
                    return Err(readiness_failure_from_websocket_error(err));
                }
                None => return Err(ReadinessFailure::ClosedAfterOpen { reason: None }),
            }
        }
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ReadinessFailure::ReadyTimedOut),
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

    log_success("client", "WebVPN login successful (WeChat)");
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
    client
        .get(send_url)
        .send()
        .await
        .context("failed to send verification code request")?
        .error_for_status()
        .context("verification code request failed")?;

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
    log_success("client", "WebVPN login successful (verification code)");
    Ok(cookie)
}

async fn initialize_webvpn_ticket_cookie(
    client: &Client,
    cookie_jar: &reqwest::cookie::Jar,
) -> Result<WebVpnLoginEntry> {
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
        .is_none()
    {
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
    let mut warned_unexpected_status = false;
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
                tokio::time::sleep(Duration::from_millis(WECHAT_POLL_SETTLE_MS)).await;
            }
            other => {
                if !warned_unexpected_status {
                    log_warn(
                        "client",
                        format!("unexpected WeChat QR status {other}; continuing to wait"),
                    );
                    warned_unexpected_status = true;
                }
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

#[cfg(feature = "cli")]
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

fn is_webvpn_personal_center_url(url: &str) -> bool {
    url.contains("webvpn.szut.edu.cn") && url.contains("/personal-center")
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

#[cfg(feature = "cli")]
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

#[cfg(all(test, feature = "cli"))]
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
    async fn tunnel_ids_remain_independent_for_embedded_event_correlation() {
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
    fn cached_cookie_login_redirect_is_classified_as_expired_immediately() {
        let result = cached_cookie_check_from_failure(
            "ticket=cached".to_string(),
            ConnectFailure::CookieExpired {
                location: "/webvpn.szut.edu.cn/login".to_string(),
            },
        )
        .unwrap();

        assert!(matches!(result, CachedCookieCheck::Expired));
    }

    #[test]
    fn cached_cookie_remains_usable_when_only_tows_is_unreachable() {
        let result = cached_cookie_check_from_failure(
            "ticket=cached".to_string(),
            ConnectFailure::WebVpnFailed {
                location: "/wengine-vpn/failed".to_string(),
            },
        )
        .unwrap();

        assert!(matches!(
            result,
            CachedCookieCheck::EndpointUnavailable(cookie) if cookie == "ticket=cached"
        ));
    }

    #[tokio::test]
    async fn aborting_a_local_connection_still_emits_its_closed_event() {
        let ui = Arc::new(RecordingUi::default());
        let event_ui: Arc<dyn EmbeddedClientUi> = ui.clone();
        let peer = "127.0.0.1:12345".to_string();
        event_ui.emit(EmbeddedClientEvent::Tunnel(
            TunnelEvent::LocalConnectionOpened {
                tunnel_id: 7,
                peer: peer.clone(),
            },
        ));
        let guard = LocalConnectionEventGuard {
            ui: event_ui,
            tunnel_id: 7,
            peer: peer.clone(),
        };
        let task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });

        task.abort();
        let _ = task.await;

        let events = ui.events.lock().unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            EmbeddedClientEvent::Tunnel(TunnelEvent::LocalConnectionClosed {
                tunnel_id: 7,
                peer: closed_peer,
            }) if closed_peer == &peer
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
    fn readiness_diagnoses_webvpn_failed_as_tows_endpoint_issue() {
        let failure = ReadinessFailure::WebVpnFailed {
            location: "/wengine-vpn/failed".to_string(),
        };

        let lines = failure.diagnostic_lines("192.0.2.10:4489", "127.0.0.1:22");

        assert!(lines[0].contains("before tows accepted WebSocket"));
        assert!(lines[1].contains("likely cause: tows is not running/reachable"));
    }

    #[test]
    fn readiness_diagnoses_reset_as_probable_target_issue() {
        let lines =
            ReadinessFailure::ResetAfterOpen.diagnostic_lines("192.0.2.10:4489", "127.0.0.1:54162");

        assert!(lines[0].contains("WebVPN reached tows"));
        assert!(lines[1].contains("likely cause: target 127.0.0.1:54162"));
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

        let lines = failure.diagnostic_lines("192.0.2.10:4489", "127.0.0.1:54162");
        assert!(lines[0].contains("then failed to connect target 127.0.0.1:54162"));
        assert!(lines[1].contains("cause: target TCP connection failed"));
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

    #[test]
    fn terminal_ready_message_contains_the_complete_tunnel_path() {
        let ui = TerminalUi::new(&TunnelConfig {
            server: "10.18.47.77:4489".to_string(),
            target: "127.0.0.1:22".to_string(),
            listen_addr: "127.0.0.1:14489".to_string(),
        });

        assert_eq!(
            ui.ready_message,
            "ready: local 127.0.0.1:14489 -> WebVPN -> tows 10.18.47.77:4489 -> target 127.0.0.1:22"
        );
    }

    #[test]
    fn cached_login_requires_the_protected_portal_destination() {
        assert!(is_webvpn_personal_center_url(WEBVPN_PERSONAL_CENTER_URL));
        assert!(!is_webvpn_personal_center_url(WEBVPN_PORTAL_LOGIN_URL));
        assert!(!is_webvpn_personal_center_url(
            "https://webvpn.szut.edu.cn/login"
        ));
    }
}
