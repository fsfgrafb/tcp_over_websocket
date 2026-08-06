use anyhow::{Context, Result, bail};
use eframe::egui;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

use crate::address::{DEFAULT_TOWS_PORT, Endpoint, parse_listen, parse_target, parse_tows};
use crate::client::{
    AuthPrompt, ClientObserver, ForwardRule, LoginPreference, ServerGroup, clear_cached_ticket,
    login_or_restore, run_dynamic_server_groups,
};
use crate::storage::BoundedLogWriter;

use super::config::{
    ConnectionConfig, DEFAULT_WS_KEEPALIVE_SECS, GuiConfig, GuiState, ImportBundle,
    MAX_COOKIE_REFRESH_SECS, MAX_WINDOW_HEIGHT, MAX_WS_KEEPALIVE_SECS, MIN_COOKIE_REFRESH_SECS,
    MIN_WINDOW_HEIGHT, MIN_WS_KEEPALIVE_SECS, MergePolicy, ThemeSetting, TunnelConfig,
    export_tunnels, import_conflicts, listen_conflicts, load_default_config, load_gui_state,
    merge_import, read_import_paths, save_default_config, save_gui_state, validate_config,
};

const INTERVAL_INPUT_WIDTH: f32 = 80.0;

pub fn run() -> Result<()> {
    let gui_state = load_gui_state();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([880.0, gui_state.window_height as f32])
            .with_min_inner_size([880.0, MIN_WINDOW_HEIGHT as f32])
            .with_max_inner_size([880.0, MAX_WINDOW_HEIGHT as f32])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        "TCP over WebSocket Client",
        options,
        Box::new(|creation| Ok(Box::new(TowcApp::new(creation)))),
    )
    .map_err(|error| anyhow::anyhow!(error.to_string()))
}

enum WorkerEvent {
    Status(String),
    Tunnel(String, String),
    Qr(Vec<u8>),
    CodeRequest {
        label: String,
        reply: std_mpsc::Sender<String>,
    },
    Log(String),
    Finished(std::result::Result<(), String>),
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum LoginKind {
    #[default]
    Wechat,
    Mobile,
    Email,
}

impl LoginKind {
    fn preference(self, identity: &str) -> Result<LoginPreference> {
        match self {
            Self::Wechat => Ok(LoginPreference::Wechat),
            Self::Mobile => match LoginPreference::from_identity(identity) {
                Ok(LoginPreference::Mobile(value)) => Ok(LoginPreference::Mobile(value)),
                _ => bail!("SMS login requires a numeric phone number"),
            },
            Self::Email => match LoginPreference::from_identity(identity) {
                Ok(LoginPreference::Email(value)) => Ok(LoginPreference::Email(value)),
                _ => bail!("email login requires a valid email address"),
            },
        }
    }
}

struct GuiBridge {
    events: std_mpsc::Sender<WorkerEvent>,
}

impl AuthPrompt for GuiBridge {
    fn status(&self, message: &str) {
        let _ = self.events.send(WorkerEvent::Status(message.to_string()));
    }

    fn show_qr(&self, image: Vec<u8>) -> Result<()> {
        self.events
            .send(WorkerEvent::Qr(image))
            .map_err(|_| anyhow::anyhow!("GUI was closed"))
    }

    fn request_code(&self, label: &str) -> Result<String> {
        let (reply, receiver) = std_mpsc::channel();
        self.events
            .send(WorkerEvent::CodeRequest {
                label: label.to_string(),
                reply,
            })
            .map_err(|_| anyhow::anyhow!("GUI was closed"))?;
        receiver
            .recv()
            .map_err(|_| anyhow::anyhow!("verification code entry was cancelled"))
    }
}

impl ClientObserver for GuiBridge {
    fn status(&self, message: &str) {
        let _ = self.events.send(WorkerEvent::Status(message.to_string()));
    }

    fn tunnel_status(&self, name: &str, message: &str) {
        let _ = self
            .events
            .send(WorkerEvent::Tunnel(name.to_string(), message.to_string()));
    }
}

#[derive(Clone)]
struct GuiLogWriter {
    events: std_mpsc::Sender<WorkerEvent>,
}

#[derive(Clone)]
struct EndpointEdit {
    host: String,
    port: String,
}

#[derive(Clone)]
struct TunnelEdit {
    target: EndpointEdit,
    listen: EndpointEdit,
}

#[derive(Clone)]
struct ConnectionEditor {
    index: Option<usize>,
    host: String,
    port: String,
    keepalive_secs: String,
    error: Option<String>,
    request_initial_focus: bool,
}

#[derive(Clone)]
struct AppSettingsEditor {
    theme: ThemeSetting,
    cookie_refresh_secs: String,
    error: Option<String>,
    request_initial_focus: bool,
}

#[derive(Clone)]
enum DeleteTarget {
    Connection(usize),
    Tunnels(Vec<usize>),
}

impl Write for GuiLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let text = String::from_utf8_lossy(bytes).trim().to_string();
        if !text.is_empty() {
            let _ = self.events.send(WorkerEvent::Log(text));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct TowcApp {
    config: GuiConfig,
    save_blocked: bool,
    warning: Option<String>,
    login_kind: LoginKind,
    mobile_identity: String,
    email_identity: String,
    running: bool,
    status: String,
    tunnel_status: HashMap<String, String>,
    logs: Vec<String>,
    file_log: Option<BoundedLogWriter>,
    events: Option<std_mpsc::Receiver<WorkerEvent>>,
    stop: Option<watch::Sender<bool>>,
    updates: Option<mpsc::UnboundedSender<Vec<ServerGroup>>>,
    cookie_interval_updates: Option<watch::Sender<Duration>>,
    pending_code: Option<(String, std_mpsc::Sender<String>)>,
    submit_code_when_requested: bool,
    code_input: String,
    qr_texture: Option<egui::TextureHandle>,
    pending_import: Option<ImportBundle>,
    pending_delete: Option<DeleteTarget>,
    export_selected: HashSet<String>,
    auto_start_pending: bool,
    login_visible: bool,
    theme: ThemeSetting,
    cookie_refresh_secs: u64,
    connected_since: Option<Instant>,
    editing_snapshot: Option<GuiConfig>,
    connected_servers: HashSet<String>,
    cookie_cycle_started: Option<Instant>,
    restart_when_stopped: bool,
    logout_when_stopped: bool,
    tunnel_edits: Vec<TunnelEdit>,
    connection_editor: Option<ConnectionEditor>,
    app_settings_editor: Option<AppSettingsEditor>,
    window_height: u32,
}

impl TowcApp {
    fn new(creation: &eframe::CreationContext<'_>) -> Self {
        install_chinese_font(&creation.egui_ctx);
        let gui_state = load_gui_state();
        creation
            .egui_ctx
            .set_theme(theme_preference(gui_state.theme));
        apply_gui_style(&creation.egui_ctx);
        let loaded = load_default_config();
        let tunnel_edits = tunnel_edits(&loaded.config);
        let auto_start_pending = !loaded.save_blocked;
        let mut app = Self {
            config: loaded.config,
            save_blocked: loaded.save_blocked,
            warning: loaded.warning,
            login_kind: LoginKind::default(),
            mobile_identity: String::new(),
            email_identity: String::new(),
            running: false,
            status: "未启动".to_string(),
            tunnel_status: HashMap::new(),
            logs: Vec::new(),
            file_log: BoundedLogWriter::for_program("towc"),
            events: None,
            stop: None,
            updates: None,
            cookie_interval_updates: None,
            pending_code: None,
            submit_code_when_requested: false,
            code_input: String::new(),
            qr_texture: None,
            pending_import: None,
            pending_delete: None,
            export_selected: HashSet::new(),
            auto_start_pending,
            login_visible: !auto_start_pending,
            theme: gui_state.theme,
            cookie_refresh_secs: gui_state.cookie_refresh_secs,
            connected_since: None,
            editing_snapshot: None,
            connected_servers: HashSet::new(),
            cookie_cycle_started: None,
            restart_when_stopped: false,
            logout_when_stopped: false,
            tunnel_edits,
            connection_editor: None,
            app_settings_editor: None,
            window_height: gui_state.window_height,
        };
        if let Some(warning) = app.warning.clone() {
            app.log(warning);
        }
        app
    }

    fn start(&mut self) {
        if self.running {
            return;
        }
        let (preference, groups) = match self.session_config() {
            Ok(session) => session,
            Err(error) => {
                self.log(format!("无法启动：{error:#}"));
                return;
            }
        };
        if let Err(error) = save_default_config(&self.config) {
            self.log(format!("无法保存配置，已取消启动：{error:#}"));
            return;
        }

        let (event_tx, event_rx) = std_mpsc::channel();
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let (updates_tx, updates_rx) = mpsc::unbounded_channel();
        let (cookie_interval_tx, cookie_interval_rx) =
            watch::channel(Duration::from_secs(self.cookie_refresh_secs));
        self.events = Some(event_rx);
        self.stop = Some(stop_tx);
        self.updates = Some(updates_tx);
        self.cookie_interval_updates = Some(cookie_interval_tx);
        self.running = true;
        self.status = "正在检查 WebVPN 登录状态".to_string();
        self.qr_texture = None;
        self.tunnel_status.clear();
        self.connected_servers.clear();
        self.cookie_cycle_started = None;

        std::thread::spawn(move || {
            let bridge = Arc::new(GuiBridge {
                events: event_tx.clone(),
            });
            let auth: Arc<dyn AuthPrompt> = bridge.clone();
            let observer: Arc<dyn ClientObserver> = bridge;
            let writer_events = event_tx.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .event_format(crate::TaggedEventFormatter {
                    default_tag: "towc",
                })
                .with_writer(move || GuiLogWriter {
                    events: writer_events.clone(),
                })
                .finish();
            let result = tracing::subscriber::with_default(subscriber, || {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .context("cannot create GUI async runtime")?
                    .block_on(async move {
                        let login = login_or_restore(auth, preference);
                        let cookie = tokio::select! {
                            result = login => result?,
                            changed = stop_rx.changed() => {
                                if changed.is_err() || *stop_rx.borrow() {
                                    return Ok(());
                                }
                                return Ok(());
                            }
                        };
                        run_dynamic_server_groups(
                            groups,
                            cookie,
                            stop_rx,
                            updates_rx,
                            observer,
                            cookie_interval_rx,
                        )
                        .await
                    })
            });
            let _ = event_tx.send(WorkerEvent::Finished(
                result.map_err(|error: anyhow::Error| format!("{error:#}")),
            ));
        });
    }

    fn session_config(&self) -> Result<(LoginPreference, Vec<ServerGroup>)> {
        if self.save_blocked {
            bail!("the configuration is protected; confirm the current GUI configuration first");
        }
        validate_config(&self.config).context("configuration validation failed")?;
        if !listen_conflicts(&self.config).is_empty() {
            bail!("enabled tunnels have conflicting listen addresses");
        }

        let identity = match self.login_kind {
            LoginKind::Wechat => "",
            LoginKind::Mobile => self.mobile_identity.trim(),
            LoginKind::Email => self.email_identity.trim(),
        };
        let preference = self.login_kind.preference(identity)?;
        if !(MIN_COOKIE_REFRESH_SECS..=MAX_COOKIE_REFRESH_SECS).contains(&self.cookie_refresh_secs)
        {
            bail!("cookie keepalive interval is outside the allowed range");
        }
        Ok((preference, self.server_groups()?))
    }

    fn server_groups(&self) -> Result<Vec<ServerGroup>> {
        let mut groups = Vec::<ServerGroup>::new();
        for connection in &self.config.connections {
            let server = parse_tows(&connection.tows)?;
            let mut rules = Vec::new();
            for tunnel in self
                .config
                .tunnels
                .iter()
                .filter(|tunnel| parse_tows(&tunnel.tows).is_ok_and(|value| value == server))
            {
                if tunnel.enabled {
                    rules.push(forward_rule(tunnel)?);
                }
            }
            groups.push(ServerGroup {
                server,
                rules,
                heartbeat_interval: Duration::from_secs(connection.ws_keepalive_secs),
            });
        }
        Ok(groups)
    }

    fn set_tunnel_enabled(&mut self, index: usize, enabled: bool) {
        let previous_config = self.config.clone();
        self.config.tunnels[index].enabled = enabled;
        let applied = self.apply_config_change(previous_config, String::new());
        if applied && enabled && !self.running {
            self.start();
        }
    }

    fn persist_gui_state(&mut self) {
        let state = GuiState {
            theme: self.theme,
            selected_tunnels: HashSet::new(),
            cookie_refresh_secs: self.cookie_refresh_secs,
            window_height: self.window_height,
        };
        if let Err(error) = save_gui_state(&state) {
            self.log(format!("无法保存界面设置：{error:#}"));
        }
    }

    fn apply_config_change(&mut self, previous: GuiConfig, success: String) -> bool {
        let validation = validate_config(&self.config).and_then(|_| {
            if listen_conflicts(&self.config).is_empty() {
                Ok(())
            } else {
                bail!("local listen address conflict")
            }
        });
        if let Err(error) = validation {
            self.config = previous;
            self.log(format!("配置修改被拒绝：{error:#}"));
            return false;
        }
        let runtime_groups = if self.running {
            match self.server_groups() {
                Ok(groups) => Some(groups),
                Err(error) => {
                    self.config = previous;
                    self.log(format!("无法应用配置修改：{error:#}"));
                    return false;
                }
            }
        } else {
            None
        };
        if !self.save_blocked
            && let Err(error) = save_default_config(&self.config)
        {
            self.config = previous;
            self.log(format!("无法保存配置修改：{error:#}"));
            return false;
        }
        if let Some(groups) = runtime_groups {
            if self
                .updates
                .as_ref()
                .is_none_or(|updates| updates.send(groups).is_err())
            {
                self.config = previous;
                if !self.save_blocked {
                    let _ = save_default_config(&self.config);
                }
                self.log("运行任务已停止，无法应用配置修改".to_string());
                return false;
            }
        }
        if !success.is_empty() {
            self.log(success);
        }
        true
    }

    fn export_selected(&mut self) {
        let tunnels = self
            .config
            .tunnels
            .iter()
            .filter(|tunnel| self.export_selected.contains(&tunnel.name))
            .cloned()
            .collect::<Vec<_>>();
        self.export_selected.clear();
        let selected_servers = tunnels
            .iter()
            .filter_map(|tunnel| parse_tows(&tunnel.tows).ok())
            .map(|server| server.to_string())
            .collect::<HashSet<_>>();
        let connections = self
            .config
            .connections
            .iter()
            .filter(|connection| {
                parse_tows(&connection.tows)
                    .is_ok_and(|server| selected_servers.contains(&server.to_string()))
            })
            .cloned()
            .collect();
        let mut dialog = rfd::FileDialog::new()
            .add_filter("JSON configuration", &["json"])
            .set_file_name("tunnels.json");
        if let Some(desktop) = desktop_dir() {
            dialog = dialog.set_directory(desktop);
        }
        let Some(path) = dialog.save_file() else {
            return;
        };
        match export_tunnels(&path, connections, tunnels) {
            Ok(()) => self.log(format!("已导出到 {}", path.display())),
            Err(error) => self.log(format!("导出失败：{error:#}")),
        }
    }

    fn stop(&mut self) {
        self.submit_code_when_requested = false;
        if let Some((_, reply)) = self.pending_code.take() {
            let _ = reply.send(String::new());
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(true);
            self.status = "正在停止".to_string();
        }
        self.updates = None;
        self.cookie_interval_updates = None;
        self.connected_since = None;
        self.cookie_cycle_started = None;
    }

    fn logout(&mut self) {
        self.restart_when_stopped = false;
        if self.running {
            self.logout_when_stopped = true;
            self.stop();
            self.status = "正在退出登录".to_string();
        } else {
            self.finish_logout();
        }
    }

    fn finish_logout(&mut self) {
        match clear_cached_ticket() {
            Ok(()) => {
                self.login_kind = LoginKind::Wechat;
                self.login_visible = true;
                self.qr_texture = None;
                self.pending_code = None;
                self.submit_code_when_requested = false;
                self.connected_since = None;
                self.connected_servers.clear();
                self.cookie_cycle_started = None;
                self.status = "已退出登录".to_string();
                self.log("已退出登录，正在重新获取微信登录二维码".to_string());
                self.auto_start_pending = true;
            }
            Err(error) => {
                self.status = "退出登录失败".to_string();
                self.log(format!("无法删除登录凭据：{error:#}"));
            }
        }
    }

    fn poll_events(&mut self, context: &egui::Context) {
        let mut events = Vec::new();
        if let Some(receiver) = &self.events {
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
        }
        for event in events {
            match event {
                WorkerEvent::Status(message) => {
                    if message == "WebVPN login required" {
                        self.login_visible = true;
                    }
                    if message.starts_with("reusing a valid")
                        || message == "WebVPN login completed"
                        || message.starts_with("connecting to tows")
                    {
                        self.login_visible = false;
                        self.qr_texture = None;
                        self.pending_code = None;
                    }
                    if (message.starts_with("reusing a valid")
                        || message == "WebVPN login completed")
                        && self.connected_since.is_none()
                    {
                        let now = Instant::now();
                        self.connected_since = Some(now);
                        self.cookie_cycle_started = Some(now);
                    }
                    if message.starts_with("connected to tows ") && self.connected_since.is_none() {
                        let now = Instant::now();
                        self.connected_since = Some(now);
                        self.cookie_cycle_started = Some(now);
                    }
                    if let Some(server) = message.strip_prefix("connected to tows ") {
                        self.connected_servers.insert(server.to_string());
                    }
                    if let Some(server) = message
                        .strip_prefix("tows ")
                        .and_then(|value| value.strip_suffix(" keepalive stopped"))
                    {
                        self.connected_servers.remove(server);
                    }
                    if let Some(server) = message
                        .strip_prefix("tows ")
                        .and_then(|value| value.split_once(" failed:").map(|(server, _)| server))
                    {
                        self.connected_servers.remove(server);
                    }
                    if message == "WebVPN cookie refreshed" {
                        let now = Instant::now();
                        self.cookie_cycle_started = Some(now);
                    }
                    self.status = message.clone();
                    self.log(message);
                }
                WorkerEvent::Tunnel(name, message) => {
                    if updates_tunnel_state(&message) {
                        self.tunnel_status.insert(name.clone(), message.clone());
                    }
                    self.log(format!("[{name}] {message}"));
                }
                WorkerEvent::Qr(bytes) => match qr_texture(context, &bytes) {
                    Ok(texture) => {
                        self.login_visible = true;
                        self.qr_texture = Some(texture);
                    }
                    Err(error) => self.log(format!("无法显示二维码：{error:#}")),
                },
                WorkerEvent::CodeRequest { label, reply } => {
                    if self.submit_code_when_requested && !self.code_input.trim().is_empty() {
                        let _ = reply.send(self.code_input.trim().to_string());
                        self.submit_code_when_requested = false;
                        self.code_input.clear();
                    } else {
                        self.pending_code = Some((label, reply));
                    }
                }
                WorkerEvent::Log(message) => self.log(message),
                WorkerEvent::Finished(result) => {
                    self.running = false;
                    self.stop = None;
                    self.updates = None;
                    self.cookie_interval_updates = None;
                    self.pending_code = None;
                    self.submit_code_when_requested = false;
                    self.qr_texture = None;
                    self.connected_since = None;
                    self.connected_servers.clear();
                    self.cookie_cycle_started = None;
                    if self.logout_when_stopped {
                        self.logout_when_stopped = false;
                        self.finish_logout();
                    } else {
                        match result {
                            Ok(()) => {
                                self.status = "已停止".to_string();
                                self.log("所有本地监听已停止".to_string());
                            }
                            Err(error) => {
                                self.status = "认证或连接已失效，请重新登录".to_string();
                                self.login_visible = true;
                                for tunnel in &self.config.tunnels {
                                    if tunnel.enabled {
                                        self.tunnel_status
                                            .insert(tunnel.name.clone(), "失败".to_string());
                                    }
                                }
                                self.log(format!("连接失败，所有监听已停止：{error}"));
                            }
                        }
                    }
                    if self.restart_when_stopped {
                        self.restart_when_stopped = false;
                        self.auto_start_pending = true;
                    }
                }
            }
        }
    }

    fn log(&mut self, message: String) {
        let message = if message.starts_with('[') && message.contains("] ") {
            message
        } else {
            format!("[towc] {message}")
        };
        let message = localize_log_line(&message);
        if let Some(file) = &mut self.file_log {
            let _ = writeln!(file, "{message}");
        }
        self.logs.push(message);
        if self.logs.len() > 500 {
            self.logs.drain(..self.logs.len() - 500);
        }
    }

    fn open_new_connection_editor(&mut self) {
        self.connection_editor = Some(ConnectionEditor {
            index: None,
            host: String::new(),
            port: String::new(),
            keepalive_secs: String::new(),
            error: None,
            request_initial_focus: true,
        });
    }

    fn open_connection_editor(&mut self, index: usize) {
        let Some(connection) = self.config.connections.get(index) else {
            return;
        };
        let Ok(endpoint) = parse_tows(&connection.tows) else {
            return;
        };
        self.connection_editor = Some(ConnectionEditor {
            index: Some(index),
            host: endpoint.host().to_string(),
            port: if endpoint.port() == DEFAULT_TOWS_PORT {
                String::new()
            } else {
                endpoint.port().to_string()
            },
            keepalive_secs: if connection.ws_keepalive_secs == DEFAULT_WS_KEEPALIVE_SECS {
                String::new()
            } else {
                connection.ws_keepalive_secs.to_string()
            },
            error: None,
            request_initial_focus: true,
        });
    }

    fn save_connection_editor(&mut self, editor: &ConnectionEditor) -> Result<()> {
        let host = editor.host.trim();
        if host.is_empty() {
            bail!("IP 或主机名不能为空");
        }
        let port = if editor.port.trim().is_empty() {
            DEFAULT_TOWS_PORT
        } else {
            editor
                .port
                .trim()
                .parse::<u16>()
                .context("端口必须是 1–65535 之间的整数")?
        };
        let server = Endpoint::new(host, port)?;
        let keepalive_secs = if editor.keepalive_secs.trim().is_empty() {
            DEFAULT_WS_KEEPALIVE_SECS
        } else {
            editor
                .keepalive_secs
                .trim()
                .parse::<u64>()
                .context("保活间隔必须是整数")?
        };
        if !(MIN_WS_KEEPALIVE_SECS..=MAX_WS_KEEPALIVE_SECS).contains(&keepalive_secs) {
            bail!("保活间隔必须在 {MIN_WS_KEEPALIVE_SECS}–{MAX_WS_KEEPALIVE_SECS} 秒之间");
        }
        if self
            .config
            .connections
            .iter()
            .enumerate()
            .any(|(index, connection)| {
                Some(index) != editor.index
                    && parse_tows(&connection.tows).is_ok_and(|existing| existing == server)
            })
        {
            bail!("该 tows 连接已存在");
        }

        let previous = self.config.clone();
        if let Some(index) = editor.index {
            let old_server = parse_tows(&self.config.connections[index].tows)?;
            self.config.connections[index] = ConnectionConfig {
                tows: server.to_string(),
                ws_keepalive_secs: keepalive_secs,
            };
            for tunnel in &mut self.config.tunnels {
                if parse_tows(&tunnel.tows).is_ok_and(|value| value == old_server) {
                    tunnel.tows = server.to_string();
                }
            }
            if !self.apply_config_change(previous, "tows 连接已更新".to_string()) {
                bail!("连接设置未能保存");
            }
        } else {
            self.config.connections.push(ConnectionConfig {
                tows: server.to_string(),
                ws_keepalive_secs: keepalive_secs,
            });
            if !self.apply_config_change(previous, "tows 连接已添加".to_string()) {
                bail!("连接设置未能保存");
            }
        }
        Ok(())
    }

    fn show_delete_confirmation(&mut self, context: &egui::Context) {
        let Some(target) = self.pending_delete.clone() else {
            return;
        };
        let description = match target {
            DeleteTarget::Connection(index) => {
                self.config.connections.get(index).map(|connection| {
                    let server = parse_tows(&connection.tows).ok();
                    let tunnel_count = self
                        .config
                        .tunnels
                        .iter()
                        .filter(|tunnel| {
                            tunnel.tows == connection.tows
                                || server.as_ref().is_some_and(|server| {
                                    parse_tows(&tunnel.tows).is_ok_and(|value| value == *server)
                                })
                        })
                        .count();
                    if tunnel_count == 0 {
                        format!("确定删除连接 {} 吗？", connection.tows)
                    } else {
                        format!(
                            "确定删除连接 {} 及其 {} 条隧道吗？",
                            connection.tows, tunnel_count
                        )
                    }
                })
            }
            DeleteTarget::Tunnels(ref indices) => (!indices.is_empty())
                .then(|| format!("确定删除选中的 {} 条隧道吗？", indices.len())),
        };
        let Some(description) = description else {
            self.pending_delete = None;
            return;
        };

        let mut decision = None;
        egui::Window::new("确认删除")
            .id(egui::Id::new("delete-confirmation-dialog-v3"))
            .collapsible(false)
            .auto_sized()
            .min_width(270.0)
            .max_width(270.0)
            .movable(true)
            .pivot(egui::Align2::CENTER_CENTER)
            .default_pos(context.screen_rect().center())
            .show(context, |ui| {
                centered_dialog_content(ui, 230.0, |ui| {
                    ui.add_sized(
                        [230.0, 0.0],
                        egui::Label::new(description)
                            .wrap()
                            .halign(egui::Align::Center),
                    );
                });
                ui.add_space(8.0);
                dialog_button_row(ui, |ui| {
                    if ui.button("取消").clicked() {
                        decision = Some(false);
                    }
                    if ui
                        .add(
                            egui::Button::new(
                                egui::RichText::new("删除").color(egui::Color32::WHITE),
                            )
                            .fill(egui::Color32::from_rgb(190, 45, 45)),
                        )
                        .clicked()
                    {
                        decision = Some(true);
                    }
                });
            });

        if let Some(confirm) = decision {
            self.pending_delete = None;
            if confirm {
                self.delete_target(target);
            }
        }
    }

    fn delete_target(&mut self, target: DeleteTarget) {
        let previous = self.config.clone();
        match target {
            DeleteTarget::Connection(index) => {
                let Some(connection) = self.config.connections.get(index) else {
                    return;
                };
                let connection_tows = connection.tows.clone();
                let parsed_connection = parse_tows(&connection_tows).ok();
                let tunnel_indices = self
                    .config
                    .tunnels
                    .iter()
                    .enumerate()
                    .filter(|(_, tunnel)| {
                        tunnel.tows == connection_tows
                            || parsed_connection.as_ref().is_some_and(|connection| {
                                parse_tows(&tunnel.tows).is_ok_and(|tows| tows == *connection)
                            })
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                let removed_names = tunnel_indices
                    .iter()
                    .map(|index| self.config.tunnels[*index].name.clone())
                    .collect::<Vec<_>>();
                for tunnel_index in tunnel_indices.iter().rev().copied() {
                    self.config.tunnels.remove(tunnel_index);
                }
                let removed = self.config.connections.remove(index);
                if self.apply_config_change(previous, format!("tows 连接 {} 已删除", removed.tows))
                {
                    for tunnel_index in tunnel_indices.iter().rev().copied() {
                        self.tunnel_edits.remove(tunnel_index);
                    }
                    for name in removed_names {
                        self.export_selected.remove(&name);
                        self.tunnel_status.remove(&name);
                    }
                }
            }
            DeleteTarget::Tunnels(mut indices) => {
                indices.sort_unstable();
                indices.dedup();
                if indices.is_empty()
                    || indices
                        .last()
                        .is_some_and(|index| *index >= self.config.tunnels.len())
                {
                    return;
                }
                let removed_names = indices
                    .iter()
                    .map(|index| self.config.tunnels[*index].name.clone())
                    .collect::<Vec<_>>();
                for index in indices.iter().rev().copied() {
                    self.config.tunnels.remove(index);
                }
                if self.apply_config_change(
                    previous,
                    format!("已删除选中的 {} 条隧道", removed_names.len()),
                ) {
                    for index in indices.iter().rev().copied() {
                        self.tunnel_edits.remove(index);
                    }
                    for name in removed_names {
                        self.export_selected.remove(&name);
                        self.tunnel_status.remove(&name);
                    }
                    self.persist_gui_state();
                }
            }
        }
    }

    fn accept_import(&mut self, policy: MergePolicy) {
        if let Some(bundle) = self.pending_import.take() {
            for message in &bundle.messages {
                self.log(message.clone());
            }
            let count = bundle.tunnels.len();
            let previous = self.config.clone();
            merge_import(&mut self.config, bundle, policy);
            if self.apply_config_change(previous, format!("已导入 {count} 条隧道，源文件未被修改"))
            {
                self.tunnel_edits = tunnel_edits(&self.config);
            }
        }
    }

    fn stage_import(&mut self, paths: &[std::path::PathBuf]) {
        let bundle = read_import_paths(paths);
        self.log(format!(
            "已读取 {} 个配置文件，共包含 {} 条隧道",
            bundle.files_read,
            bundle.tunnels.len()
        ));
        if import_conflicts(&self.config, &bundle).is_empty() {
            self.pending_import = Some(bundle);
            self.accept_import(MergePolicy::SkipExisting);
        } else {
            self.pending_import = Some(bundle);
        }
    }

    fn choose_import_files(&mut self) {
        let mut dialog = rfd::FileDialog::new().add_filter("JSON configuration", &["json"]);
        if let Some(desktop) = desktop_dir() {
            dialog = dialog.set_directory(desktop);
        }
        if let Some(paths) = dialog.pick_files() {
            self.stage_import(&paths);
        }
    }
}

impl eframe::App for TowcApp {
    fn update(&mut self, context: &egui::Context, _frame: &mut eframe::Frame) {
        if let Some(height) =
            context.input(|input| input.viewport().inner_rect.map(|rect| rect.height()))
        {
            self.window_height =
                (height.round() as u32).clamp(MIN_WINDOW_HEIGHT, MAX_WINDOW_HEIGHT);
        }
        self.poll_events(context);
        if self.auto_start_pending {
            self.auto_start_pending = false;
            self.start();
        }
        let dropped: Vec<_> = context.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .collect()
        });
        if !dropped.is_empty() {
            self.stage_import(&dropped);
        }

        self.show_delete_confirmation(context);
        let old_theme = self.theme;
        egui::CentralPanel::default().show(context, |ui| {
            egui::TopBottomPanel::bottom("fixed-bottom-actions")
                .exact_height(48.0)
                .resizable(false)
                .show_separator_line(true)
                .show_inside(ui, |ui| {
                    ui.with_layout(
                        egui::Layout::left_to_right(egui::Align::Center),
                        |ui| {
                        if ui.button("＋ 添加连接").clicked() {
                            self.open_new_connection_editor();
                        }
                        if ui
                            .add_enabled(
                                self.connected_since.is_some(),
                                egui::Button::new("退出登录"),
                            )
                            .on_hover_text("删除本机登录凭据并重新进行微信登录")
                            .clicked()
                        {
                            self.logout();
                        }
                        if ui.button("↓ 导入配置").clicked() {
                            self.choose_import_files();
                        }
                        if ui.button("↑ 导出配置").clicked() {
                            self.export_selected();
                        }
                        if settings_button(ui).on_hover_text("全局设置").clicked() {
                            self.app_settings_editor = Some(AppSettingsEditor {
                                theme: self.theme,
                                cookie_refresh_secs: if self.cookie_refresh_secs
                                    == super::config::DEFAULT_COOKIE_REFRESH_SECS
                                {
                                    String::new()
                                } else {
                                    self.cookie_refresh_secs.to_string()
                                },
                                error: None,
                                request_initial_focus: true,
                            });
                        }
                    },
                    );
                });
            let panel_width = ui.available_width();
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .scroll_bar_visibility(egui::scroll_area::ScrollBarVisibility::AlwaysHidden)
                .show(ui, |ui| {
                    let visible_width = panel_width;
                    ui.set_width(visible_width);
                    let mut auth_start_requested = false;
                    let mut login_method_clicked = false;
                    ui.horizontal_top(|ui| {
                        let auth_width = 340.0;
                        ui.vertical(|auth_column| {
                            auth_column.set_width(auth_width);
                            auth_column.heading("认证状态");
                            egui::Frame::group(auth_column.style())
                                .inner_margin(16.0)
                                .corner_radius(10.0)
                                .show(auth_column, |ui| {
                                    ui.set_min_height(188.0);
                                    ui.set_min_width(ui.available_width());
                                    if let Some(warning) = &self.warning {
                                        ui.colored_label(egui::Color32::YELLOW, warning);
                                        if self.save_blocked
                                            && primary_button(ui, "确认使用当前界面配置并允许保存")
                                                .clicked()
                                        {
                                            self.save_blocked = false;
                                            self.warning = None;
                                            self.auto_start_pending = true;
                                            self.log(
                                "配置保护已解除，源文件未被修改"
                                    .to_string(),
                            );
                                        }
                                    }
                                    if self.login_visible {
                                        let login_width = ui.available_width();
                                        ui.allocate_ui_with_layout(
                                            egui::vec2(login_width, 188.0),
                                                egui::Layout::left_to_right(egui::Align::Min),
                                                |ui| {
                                                ui.scope(|ui| {
                                                    ui.spacing_mut().button_padding.x = 2.0;
                                                    ui.vertical(|ui| {
                                                        for (kind, label) in [
                                                            (LoginKind::Wechat, "微信登录"),
                                                            (LoginKind::Mobile, "短信登录"),
                                                            (LoginKind::Email, "邮箱登录"),
                                                        ] {
                                                            if ui
                                                                .add_sized(
                                                                    [68.0, 30.0],
                                                                    egui::Button::selectable(
                                                                        self.login_kind == kind,
                                                                        label,
                                                                    )
                                                                    .wrap_mode(
                                                                        egui::TextWrapMode::Extend,
                                                                    ),
                                                                )
                                                                .clicked()
                                                            {
                                                                self.login_kind = kind;
                                                                login_method_clicked = true;
                                                            }
                                                        }
                                                    });
                                                });
                                                ui.separator();
                                                if self.login_kind == LoginKind::Wechat {
                                                    let qr_width = ui.available_width();
                                                    ui.allocate_ui_with_layout(
                                                        egui::vec2(qr_width, 188.0),
                                                        egui::Layout::centered_and_justified(
                                                            egui::Direction::LeftToRight,
                                                        ),
                                                        |ui| {
                                                            if let Some(texture) = &self.qr_texture
                                                            {
                                                                ui.image((
                                                                    texture.id(),
                                                                    egui::vec2(188.0, 188.0),
                                                                ));
                                                            } else {
                                                                ui.centered_and_justified(|ui| {
                                                                    if self.running {
                                                                        ui.spinner();
                                                                    }
                                                                });
                                                            }
                                                        },
                                                    );
                                                } else {
                                                    let identity_label =
                                                        if self.login_kind == LoginKind::Mobile {
                                                            "手机号"
                                                        } else {
                                                            "邮箱"
                                                        };
                                                    let mut identity_value =
                                                        if self.login_kind == LoginKind::Mobile {
                                                            self.mobile_identity.clone()
                                                        } else {
                                                            self.email_identity.clone()
                                                        };
                                                    let form_width =
                                                        ui.available_width().min(230.0);
                                                    let form_height = 188.0;
                                                    let form_area_width = ui.available_width();
                                                    ui.allocate_ui_with_layout(
                                                        egui::vec2(form_area_width, form_height),
                                                        egui::Layout::top_down(egui::Align::Center),
                                                        |ui| {
                                                            ui.allocate_ui_with_layout(
                                                                egui::vec2(
                                                                    form_width,
                                                                    form_height,
                                                                ),
                                                                egui::Layout::top_down(
                                                                    egui::Align::Min,
                                                                ),
                                                                |ui| {
                                                                    ui.spacing_mut().item_spacing.y =
                                                                        6.0;
                                                                    ui.label(identity_label);
                                                                    let identity_response = ui.add_sized(
                                                                        [form_width, 32.0],
                                                                        egui::TextEdit::singleline(
                                                                            &mut identity_value,
                                                                        )
                                                                        .hint_text(format!(
                                                                            "请输入{identity_label}"
                                                                        ))
                                                                        .vertical_align(
                                                                            egui::Align::Center,
                                                                        ),
                                                                    );
                                                                    if login_method_clicked {
                                                                        identity_response
                                                                            .request_focus();
                                                                    }
                                                                    ui.add_space(4.0);
                                                                    ui.label("验证码");
                                                                    let pending_verification = self
                                                                        .pending_code
                                                                        .is_some();
                                                                    let send_label = if pending_verification
                                                                        && self.status.starts_with(
                                                                            "an unexpired verification",
                                                                        )
                                                                    {
                                                                        "验证码仍有效"
                                                                    } else if pending_verification {
                                                                        "已发送"
                                                                    } else if self.running {
                                                                        "发送中…"
                                                                    } else {
                                                                        "发送验证码"
                                                                    };
                                                                    let verification_spacing = 4.0;
                                                                    let row_width = (form_width
                                                                        - verification_spacing)
                                                                        / 2.0;
                                                                    let mut submit_code = false;
                                                                    ui.horizontal(|ui| {
                                                                        ui.spacing_mut()
                                                                            .item_spacing
                                                                            .x =
                                                                            verification_spacing;
                                                                        let response = ui.add_sized(
                                                                            [row_width, 32.0],
                                                                            egui::TextEdit::singleline(
                                                                                &mut self.code_input,
                                                                            )
                                                                            .hint_text(
                                                                                "请输入验证码",
                                                                            )
                                                                            .vertical_align(
                                                                                egui::Align::Center,
                                                                            ),
                                                                        );
                                                                        submit_code |= response
                                                                            .lost_focus()
                                                                            && ui.input(|input| {
                                                                                input.key_pressed(
                                                                                    egui::Key::Enter,
                                                                                )
                                                                            });
                                                                        if ui
                                                                            .add_enabled(
                                                                                !self.running
                                                                                    && !identity_value
                                                                                        .trim()
                                                                                        .is_empty(),
                                                                                egui::Button::new(
                                                                                    send_label,
                                                                                )
                                                                                .wrap_mode(
                                                                                    egui::TextWrapMode::Extend,
                                                                                )
                                                                                .min_size(
                                                                                    egui::vec2(
                                                                                        row_width,
                                                                                        32.0,
                                                                                    ),
                                                                                ),
                                                                            )
                                                                            .clicked()
                                                                        {
                                                                            self.code_input.clear();
                                                                            auth_start_requested =
                                                                                true;
                                                                        }
                                                                    });
                                                                    let login_clicked = ui
                                                                        .with_layout(
                                                                            egui::Layout::bottom_up(
                                                                                egui::Align::Center,
                                                                            ),
                                                                            |ui| {
                                                                                primary_button_enabled(
                                                                                    ui,
                                                                                    !identity_value
                                                                                        .trim()
                                                                                        .is_empty()
                                                                                        && !self
                                                                                            .code_input
                                                                                            .trim()
                                                                                            .is_empty(),
                                                                                    "登录",
                                                                                    egui::vec2(
                                                                                        form_width,
                                                                                        32.0,
                                                                                    ),
                                                                                )
                                                                            },
                                                                        )
                                                                        .inner
                                                                        .clicked();
                                                                    submit_code |= login_clicked;
                                                                    if submit_code
                                                                        && self.pending_code.is_none()
                                                                    {
                                                                        self.submit_code_when_requested =
                                                                            true;
                                                                        if !self.running {
                                                                            auth_start_requested = true;
                                                                        }
                                                                    }
                                                                    if submit_code
                                                                        && !self
                                                                            .code_input
                                                                            .trim()
                                                                            .is_empty()
                                                                        && let Some((_, reply)) =
                                                                            self.pending_code.take()
                                                                    {
                                                                        let _ = reply.send(
                                                                            self.code_input
                                                                                .trim()
                                                                                .to_string(),
                                                                        );
                                                                        self.code_input.clear();
                                                                    }
                                                                },
                                                            );
                                                        },
                                                    );
                                                    if self.login_kind == LoginKind::Mobile {
                                                        self.mobile_identity = identity_value;
                                                    } else {
                                                        self.email_identity = identity_value;
                                                    }
                                                }
                                            },
                                        );
                                    } else {
                                        let enabled = self
                                            .config
                                            .tunnels
                                            .iter()
                                            .filter(|tunnel| tunnel.enabled)
                                            .count();
                                        let ready = self
                                            .config
                                            .tunnels
                                            .iter()
                                            .filter(|tunnel| {
                                                self.tunnel_status.get(&tunnel.name).is_some_and(
                                                    |status| status.starts_with("ready:"),
                                                )
                                            })
                                            .count();
                                        egui::Grid::new("auth-metrics")
                                            .num_columns(2)
                                            .spacing([10.0, 10.0])
                                            .show(ui, |ui| {
                                                metric_card(
                                                    ui,
                                                    "连接时长",
                                                    self.connected_since
                                                        .map(|since| {
                                                            format_elapsed(since.elapsed())
                                                        })
                                                        .unwrap_or_else(|| "--:--:--".to_string()),
                                                );
                                                metric_card(
                                                    ui,
                                                    "隧道",
                                                    format!("{ready} / {enabled}"),
                                                );
                                                ui.end_row();
                                                metric_card(
                                                    ui,
                                                    "保活连接",
                                                    self.connected_servers.len().to_string(),
                                                );
                                                metric_card(
                                                    ui,
                                                    "Cookie 刷新",
                                                    cookie_refresh_countdown(
                                                        self.cookie_cycle_started,
                                                        self.cookie_refresh_secs,
                                                    ),
                                                );
                                                ui.end_row();
                                            });
                                    }
                                });
                        });
                        let log_width = ui.available_width();
                        ui.vertical(|log_column| {
                            log_column.set_width(log_width);
                            log_column.heading("日志输出");
                            egui::Frame::group(log_column.style())
                                .inner_margin(16.0)
                                .corner_radius(10.0)
                                .show(log_column, |ui| {
                                    ui.set_min_height(188.0);
                                    ui.set_min_width(ui.available_width());
                                    egui::ScrollArea::vertical()
                                        .max_height(188.0)
                                        .auto_shrink([false, false])
                                        .stick_to_bottom(true)
                                        .show(ui, |ui| {
                                            ui.set_max_width(ui.available_width());
                                            ui.set_min_width(ui.available_width());
                                            for line in &self.logs {
                                                colored_log_line(ui, line);
                                            }
                                        });
                                });
                        });
                    });
                    if login_method_clicked && self.login_kind == LoginKind::Wechat {
                        auth_start_requested = true;
                    }
                    if login_method_clicked && self.running && self.login_visible {
                        self.restart_when_stopped =
                            auth_start_requested && self.login_kind == LoginKind::Wechat;
                        self.stop();
                        self.qr_texture = None;
                        self.status = "登录方式已切换".to_string();
                    } else if auth_start_requested && self.running && self.login_visible {
                        self.restart_when_stopped = true;
                        self.stop();
                        self.qr_texture = None;
                    } else if auth_start_requested && !self.running {
                        self.start();
                    }

                    ui.add_space(8.0);
                    ui.heading("隧道配置");
                    let frame_config = self.config.clone();
                    let conflicts = listen_conflicts(&self.config);
                    let mut name_counts = HashMap::<String, usize>::new();
                    for tunnel in &self.config.tunnels {
                        *name_counts
                            .entry(tunnel.name.trim().to_string())
                            .or_default() += 1;
                    }
                    let invalid_names = self
                        .config
                        .tunnels
                        .iter()
                        .filter(|tunnel| {
                            tunnel.name.trim().is_empty()
                                || name_counts.get(tunnel.name.trim()).copied().unwrap_or(0) > 1
                        })
                        .map(|tunnel| tunnel.name.clone())
                        .collect::<HashSet<_>>();
                    let mut toggle = None;
                    let mut add_to_server = None;
                    let mut edit_connection = None;
                    let mut edit_changed = false;
                    let mut edit_finished = false;
                    let groups = self
                        .config
                        .connections
                        .iter()
                        .enumerate()
                        .map(|(connection_index, connection)| {
                            let server = parse_tows(&connection.tows)
                                .map(|server| server.to_string())
                                .unwrap_or_else(|_| connection.tows.clone());
                            let indices = self
                                .config
                                .tunnels
                                .iter()
                                .enumerate()
                                .filter(|(_, tunnel)| {
                                    parse_tows(&tunnel.tows)
                                        .is_ok_and(|value| value.to_string() == server)
                                })
                                .map(|(index, _)| index)
                                .collect::<Vec<_>>();
                            (connection_index, server, indices)
                        })
                        .collect::<Vec<_>>();
                    for (connection_index, server, indices) in groups {
                        let connection_width = ui.available_width();
                        egui::Frame::group(ui.style())
                            .inner_margin(10.0)
                            .corner_radius(10.0)
                            .show(ui, |ui| {
                                ui.set_width((connection_width - 20.0).max(0.0));
                                ui.spacing_mut().item_spacing.y = 4.0;
                                let parsed_server = parse_tows(&server).ok();
                                let header_width = ui.available_width();
                                let (header_rect, _) = ui.allocate_exact_size(
                                    egui::vec2(header_width, 32.0),
                                    egui::Sense::hover(),
                                );
                                let mut title_ui = ui.new_child(
                                    egui::UiBuilder::new()
                                        .id_salt(("connection-title", connection_index))
                                        .max_rect(header_rect)
                                        .layout(egui::Layout::left_to_right(egui::Align::Center)),
                                );
                                title_ui.spacing_mut().item_spacing.x = 0.0;
                                if let Some(endpoint) = &parsed_server {
                                    let host = if endpoint.host().contains(':') {
                                        format!("[{}]", endpoint.host())
                                    } else {
                                        endpoint.host().to_string()
                                    };
                                    title_ui.label(
                                        egui::RichText::new(host)
                                            .monospace()
                                            .strong()
                                            .size(18.0),
                                    );
                                    title_ui.label(
                                        egui::RichText::new(format!(":{}", endpoint.port()))
                                            .monospace()
                                            .strong()
                                            .size(18.0)
                                            .color(title_ui.visuals().weak_text_color()),
                                    );
                                } else {
                                    title_ui.colored_label(
                                        egui::Color32::from_rgb(225, 72, 72),
                                        egui::RichText::new(&server)
                                            .monospace()
                                            .strong()
                                            .size(18.0),
                                    );
                                }
                                let mut actions_ui = ui.new_child(
                                    egui::UiBuilder::new()
                                        .id_salt(("connection-actions", connection_index))
                                        .max_rect(header_rect)
                                        .layout(egui::Layout::right_to_left(egui::Align::Center)),
                                );
                                if trash_button(&mut actions_ui)
                                    .on_hover_text("删除连接")
                                    .clicked()
                                {
                                    self.pending_delete =
                                        Some(DeleteTarget::Connection(connection_index));
                                }
                                if settings_button(&mut actions_ui)
                                    .on_hover_text("编辑连接")
                                    .clicked()
                                {
                                    edit_connection = Some(connection_index);
                                }
                                ui.add_space(4.0);
                                if indices.is_empty() {
                                    let empty_width = ui.available_width();
                                    ui.allocate_ui_with_layout(
                                        egui::vec2(empty_width, 104.0),
                                        egui::Layout::top_down(egui::Align::Center),
                                        |ui| {
                                            ui.add_space(8.0);
                                            ui.heading("此连接尚无隧道");
                                            ui.weak("启用隧道后将自动建立 WebSocket 保活。");
                                            ui.add_space(8.0);
                                            if ui.button("＋ 添加隧道").clicked() {
                                                add_to_server = Some(server.clone());
                                            }
                                        },
                                    );
                                } else {
                                    egui::Grid::new(format!("tunnels-{server}"))
                                    .min_row_height(26.0)
                                    .spacing([6.0, 2.0])
                                    .show(ui, |ui| {
                                        ui.allocate_ui_with_layout(
                                            egui::vec2(22.0, 18.0),
                                            egui::Layout::centered_and_justified(
                                                egui::Direction::LeftToRight,
                                            ),
                                            |_| {},
                                        );
                                        ui.allocate_ui_with_layout(
                                            egui::vec2(36.0, 18.0),
                                            egui::Layout::centered_and_justified(
                                                egui::Direction::LeftToRight,
                                            ),
                                                |ui| {
                                                    ui.label("启用");
                                                },
                                            );
                                        ui.label("名称");
                                        ui.label("目标地址");
                                        ui.label("目标端口");
                                        ui.label("监听地址");
                                        ui.label("监听端口");
                                        ui.allocate_ui_with_layout(
                                            egui::vec2(40.0, 18.0),
                                            egui::Layout::centered_and_justified(
                                                egui::Direction::LeftToRight,
                                            ),
                                            |ui| {
                                                ui.label("状态");
                                            },
                                        );
                                        ui.allocate_ui_with_layout(
                                            egui::vec2(22.0, 18.0),
                                            egui::Layout::centered_and_justified(
                                                egui::Direction::LeftToRight,
                                            ),
                                            |_| {},
                                        );
                                        ui.end_row();
                                        for index in indices.iter().copied() {
                                            let tunnel = &mut self.config.tunnels[index];
                                            let endpoint_edit = &mut self.tunnel_edits[index];
                                            tunnel.target = endpoint_edit_value(
                                                &endpoint_edit.target,
                                                "127.0.0.1",
                                                "22",
                                            );
                                            tunnel.listen = endpoint_edit_value(
                                                &endpoint_edit.listen,
                                                "127.0.0.1",
                                                "14489",
                                            );
                                            let name_valid = !invalid_names.contains(&tunnel.name);
                                            let target_valid = parse_target(&tunnel.target).is_ok();
                                            let listen_valid = parse_listen(&tunnel.listen).is_ok();
                                            let normal_text = ui.visuals().text_color();
                                            let invalid_text = egui::Color32::from_rgb(225, 72, 72);
                                            let mut selected =
                                                self.export_selected.contains(&tunnel.name);
                                            if export_checkbox(ui, &mut selected).changed() {
                                                if selected {
                                                    self.export_selected
                                                        .insert(tunnel.name.clone());
                                                } else {
                                                    self.export_selected.remove(&tunnel.name);
                                                }
                                            }
                                            let mut enabled = tunnel.enabled;
                                            ui.allocate_ui_with_layout(
                                                egui::vec2(36.0, 26.0),
                                                egui::Layout::centered_and_justified(
                                                    egui::Direction::LeftToRight,
                                                ),
                                                |ui| {
                                                    if toggle_switch(ui, &mut enabled).changed() {
                                                        toggle = Some((index, enabled));
                                                    }
                                                },
                                            );
                                            let name_response = ui.add_sized(
                                                [155.0, 22.0],
                                                egui::TextEdit::singleline(&mut tunnel.name)
                                                    .text_color(if name_valid {
                                                        normal_text
                                                    } else {
                                                        invalid_text
                                                    }),
                                            );
                                            let target_response = ui.add_sized(
                                                [145.0, 22.0],
                                                egui::TextEdit::singleline(
                                                    &mut endpoint_edit.target.host,
                                                )
                                                .hint_text("127.0.0.1")
                                                .text_color(if target_valid {
                                                    normal_text
                                                } else {
                                                    invalid_text
                                                }),
                                            );
                                            let target_port_response = ui.add_sized(
                                                [64.0, 22.0],
                                                egui::TextEdit::singleline(
                                                    &mut endpoint_edit.target.port,
                                                )
                                                .hint_text("22")
                                                .text_color(if target_valid {
                                                    normal_text
                                                } else {
                                                    invalid_text
                                                    }),
                                            );
                                            let listen_response = ui.add_sized(
                                                [145.0, 22.0],
                                                egui::TextEdit::singleline(
                                                    &mut endpoint_edit.listen.host,
                                                )
                                                .hint_text("127.0.0.1")
                                                .text_color(if listen_valid {
                                                    normal_text
                                                } else {
                                                    invalid_text
                                                }),
                                            );
                                            let listen_port_response = ui.add_sized(
                                                [70.0, 22.0],
                                                egui::TextEdit::singleline(
                                                    &mut endpoint_edit.listen.port,
                                                )
                                                .hint_text("14489")
                                                .text_color(if listen_valid {
                                                    normal_text
                                                } else {
                                                    invalid_text
                                                    }),
                                            );
                                            edit_changed |= name_response.changed()
                                                || target_response.changed()
                                                || target_port_response.changed()
                                                || listen_response.changed()
                                                || listen_port_response.changed();
                                            edit_finished |= name_response.lost_focus()
                                                || target_response.lost_focus()
                                                || target_port_response.lost_focus()
                                                || listen_response.lost_focus()
                                                || listen_port_response.lost_focus();
                                            let runtime_status = self
                                                .tunnel_status
                                                .get(&tunnel.name)
                                                .map(String::as_str)
                                                .unwrap_or(if tunnel.enabled {
                                                    "waiting"
                                                } else {
                                                    "disabled"
                                                });
                                            let color = if !name_valid {
                                                egui::Color32::from_rgb(225, 72, 72)
                                            } else if !target_valid {
                                                egui::Color32::from_rgb(225, 72, 72)
                                            } else if !listen_valid {
                                                egui::Color32::from_rgb(225, 72, 72)
                                            } else if conflicts.contains(&tunnel.name) {
                                                egui::Color32::from_rgb(225, 72, 72)
                                            } else if parse_listen(&tunnel.listen)
                                                .is_ok_and(|listen| !listen.is_loopback())
                                            {
                                                egui::Color32::from_rgb(235, 174, 52)
                                            } else if runtime_status.starts_with("ready:") {
                                                egui::Color32::from_rgb(42, 190, 116)
                                            } else if !tunnel.enabled {
                                                egui::Color32::from_gray(110)
                                            } else if runtime_status.contains("failed")
                                                || runtime_status.contains("error")
                                            {
                                                egui::Color32::from_rgb(225, 72, 72)
                                            } else {
                                                egui::Color32::from_rgb(235, 174, 52)
                                            };
                                            ui.allocate_ui_with_layout(
                                                egui::vec2(40.0, 26.0),
                                                egui::Layout::centered_and_justified(
                                                    egui::Direction::LeftToRight,
                                                ),
                                                |ui| {
                                                    status_indicator(ui, color, runtime_status);
                                                },
                                            );
                                            if compact_icon_button(ui, "× 删除隧道", true).clicked()
                                            {
                                                self.pending_delete =
                                                    Some(DeleteTarget::Tunnels(vec![index]));
                                            }
                                            ui.end_row();
                                        }
                                    });
                                    ui.allocate_ui_with_layout(
                                        egui::vec2(22.0, 22.0),
                                        egui::Layout::centered_and_justified(
                                            egui::Direction::LeftToRight,
                                        ),
                                        |ui| {
                                            if compact_icon_button(ui, "+", false)
                                                .on_hover_text("添加隧道")
                                                .clicked()
                                            {
                                                add_to_server = Some(server.clone());
                                            }
                                        },
                                    );
                                }
                            });
                        ui.add_space(6.0);
                    }
                    if self.config.connections.is_empty() {
                        egui::Frame::group(ui.style())
                            .inner_margin(24.0)
                            .corner_radius(10.0)
                            .show(ui, |ui| {
                                ui.set_min_width(ui.available_width());
                                ui.vertical_centered(|ui| {
                                    ui.heading("尚未配置连接");
                                    ui.label("导入配置，或先添加一个 tows 连接。");
                                    ui.add_space(8.0);
                                    if ui.button("＋ 添加连接").clicked() {
                                        self.open_new_connection_editor();
                                    }
                                });
                            });
                        ui.add_space(6.0);
                    }
                    if edit_changed && self.editing_snapshot.is_none() {
                        self.editing_snapshot = Some(frame_config.clone());
                    }
                    if edit_finished {
                        if let Some(previous) = self.editing_snapshot.take()
                            && previous != self.config
                        {
                            let selected_before = self.export_selected.clone();
                            if self.apply_config_change(
                                previous.clone(),
                                "隧道配置已更新".to_string(),
                            ) {
                                for (old, new) in previous.tunnels.iter().zip(&self.config.tunnels) {
                                    if old.name != new.name
                                        && selected_before.contains(&old.name)
                                    {
                                        self.export_selected.remove(&old.name);
                                        self.export_selected.insert(new.name.clone());
                                    }
                                }
                                self.persist_gui_state();
                            } else {
                                self.tunnel_edits = tunnel_edits(&self.config);
                            }
                        }
                    }
                    if let Some((index, enabled)) = toggle {
                        self.set_tunnel_enabled(index, enabled);
                    }
                    if let Some(index) = edit_connection {
                        self.open_connection_editor(index);
                    }
                    if let Some(tows) = add_to_server {
                        let previous = self.config.clone();
                        let mut number = self.config.tunnels.len() + 1;
                        while self
                            .config
                            .tunnels
                            .iter()
                            .any(|tunnel| tunnel.name == format!("隧道 {number}"))
                        {
                            number += 1;
                        }
                        self.config.tunnels.push(TunnelConfig {
                            name: format!("隧道 {number}"),
                            tows,
                            target: "127.0.0.1:22".to_string(),
                            listen: "127.0.0.1:14489".to_string(),
                            enabled: false,
                        });
                        if self.apply_config_change(previous, "隧道已添加".to_string()) {
                            self.tunnel_edits.push(TunnelEdit::empty());
                        }
                    }

                    if let Some(bundle) = &self.pending_import {
                        let files_read = bundle.files_read;
                        let tunnel_count = bundle.tunnels.len();
                        let duplicate_names = import_conflicts(&self.config, bundle);
                        let duplicate_count = duplicate_names.len();
                        let mut import_action = None;
                        egui::Window::new("发现重复隧道")
                            .id(egui::Id::new("import-conflict-dialog-v3"))
                            .collapsible(false)
                            .auto_sized()
                            .min_width(330.0)
                            .max_width(330.0)
                            .movable(true)
                            .pivot(egui::Align2::CENTER_CENTER)
                            .default_pos(context.screen_rect().center())
                            .show(context, |ui| {
                                centered_dialog_content(ui, 290.0, |ui| {
                                    ui.label(format!(
                                        "从 {} 个文件读取了 {} 条隧道，其中 {} 条与现有配置重复：",
                                        files_read, tunnel_count, duplicate_count
                                    ));
                                    ui.add(egui::Label::new(duplicate_names.join("、")).wrap());
                                });
                                dialog_button_row(ui, |ui| {
                                    if ui.button("取消").clicked() {
                                        import_action = Some(None);
                                    }
                                    if ui
                                        .add(
                                            egui::Button::new(
                                                egui::RichText::new("覆盖")
                                                    .color(egui::Color32::WHITE),
                                            )
                                            .fill(egui::Color32::from_rgb(190, 45, 45)),
                                        )
                                        .clicked()
                                    {
                                        import_action =
                                            Some(Some(MergePolicy::OverwriteExisting));
                                    }
                                    if primary_button(ui, "跳过").clicked() {
                                        import_action = Some(Some(MergePolicy::SkipExisting));
                                    }
                                });
                            });
                        if let Some(action) = import_action {
                            if let Some(policy) = action {
                                self.accept_import(policy);
                            } else {
                                self.pending_import = None;
                            }
                        }
                    }

                });
        });
        if self.connection_editor.is_some() {
            let mut save = false;
            let mut cancel = false;
            let title = if self
                .connection_editor
                .as_ref()
                .is_some_and(|editor| editor.index.is_some())
            {
                "编辑连接"
            } else {
                "添加连接"
            };
            egui::Window::new(title)
                .id(egui::Id::new("connection-editor-dialog-v3"))
                .collapsible(false)
                .auto_sized()
                .min_width(300.0)
                .max_width(300.0)
                .movable(true)
                .pivot(egui::Align2::CENTER_CENTER)
                .default_pos(context.screen_rect().center())
                .show(context, |ui| {
                    let editor = self
                        .connection_editor
                        .as_mut()
                        .expect("connection editor exists while its window is open");
                    centered_dialog_content(ui, 120.0, |ui| {
                        ui.label("tows 地址");
                        let host_response = ui.add_sized(
                            [120.0, 32.0],
                            egui::TextEdit::singleline(&mut editor.host)
                                .hint_text("IP 或主机名")
                                .vertical_align(egui::Align::Center),
                        );
                        if editor.request_initial_focus {
                            host_response.request_focus();
                            editor.request_initial_focus = false;
                        }
                        ui.add_space(8.0);
                        ui.label("端口");
                        ui.add_sized(
                            [120.0, 32.0],
                            egui::TextEdit::singleline(&mut editor.port)
                                .hint_text(DEFAULT_TOWS_PORT.to_string())
                                .vertical_align(egui::Align::Center),
                        );
                        ui.add_space(10.0);
                        ui.label("保活间隔");
                        ui.horizontal(|ui| {
                            ui.add_sized(
                                [INTERVAL_INPUT_WIDTH, 32.0],
                                egui::TextEdit::singleline(&mut editor.keepalive_secs)
                                    .hint_text(DEFAULT_WS_KEEPALIVE_SECS.to_string())
                                    .vertical_align(egui::Align::Center),
                            );
                            ui.label("秒");
                        });
                        if let Some(error) = &editor.error {
                            ui.add_space(8.0);
                            ui.add(
                                egui::Label::new(
                                    egui::RichText::new(error)
                                        .color(egui::Color32::from_rgb(225, 72, 72)),
                                )
                                .wrap(),
                            );
                        }
                    });
                    ui.add_space(14.0);
                    dialog_button_row(ui, |ui| {
                        if ui.button("取消").clicked() {
                            cancel = true;
                        }
                        if primary_button(ui, "保存").clicked() {
                            save = true;
                        }
                    });
                });
            if cancel {
                self.connection_editor = None;
            } else if save {
                let editor = self
                    .connection_editor
                    .clone()
                    .expect("connection editor exists while saving");
                match self.save_connection_editor(&editor) {
                    Ok(()) => self.connection_editor = None,
                    Err(error) => {
                        if let Some(editor) = &mut self.connection_editor {
                            editor.error = Some(format!("{error:#}"));
                        }
                    }
                }
            }
        }
        if self.app_settings_editor.is_some() {
            let mut save = false;
            let mut cancel = false;
            egui::Window::new("全局设置")
                .id(egui::Id::new("app-settings-dialog-v3"))
                .collapsible(false)
                .auto_sized()
                .min_width(300.0)
                .max_width(300.0)
                .movable(true)
                .pivot(egui::Align2::CENTER_CENTER)
                .default_pos(context.screen_rect().center())
                .show(context, |ui| {
                    let editor = self
                        .app_settings_editor
                        .as_mut()
                        .expect("settings editor exists while its window is open");
                    centered_dialog_content(ui, 160.0, |ui| {
                        ui.label("主题");
                        ui.horizontal(|ui| {
                            for (theme, label) in [
                                (ThemeSetting::System, "自动"),
                                (ThemeSetting::Light, "浅色"),
                                (ThemeSetting::Dark, "深色"),
                            ] {
                                let is_current = editor.theme == theme;
                                let response = ui.selectable_value(&mut editor.theme, theme, label);
                                if editor.request_initial_focus && is_current {
                                    response.request_focus();
                                }
                            }
                            editor.request_initial_focus = false;
                        });
                        ui.add_space(10.0);
                        ui.label("Cookie 刷新间隔");
                        ui.horizontal(|ui| {
                            ui.add_sized(
                                [INTERVAL_INPUT_WIDTH, 32.0],
                                egui::TextEdit::singleline(&mut editor.cookie_refresh_secs)
                                    .hint_text(
                                        super::config::DEFAULT_COOKIE_REFRESH_SECS.to_string(),
                                    )
                                    .vertical_align(egui::Align::Center),
                            );
                            ui.label("秒");
                        });
                        if let Some(error) = &editor.error {
                            ui.add_space(8.0);
                            ui.add(
                                egui::Label::new(
                                    egui::RichText::new(error)
                                        .color(egui::Color32::from_rgb(225, 72, 72)),
                                )
                                .wrap(),
                            );
                        }
                    });
                    ui.add_space(14.0);
                    dialog_button_row(ui, |ui| {
                        if ui.button("取消").clicked() {
                            cancel = true;
                        }
                        if primary_button(ui, "保存").clicked() {
                            save = true;
                        }
                    });
                });
            if cancel {
                self.app_settings_editor = None;
            } else if save {
                let editor = self
                    .app_settings_editor
                    .clone()
                    .expect("settings editor exists while saving");
                let interval = (|| -> Result<u64> {
                    let value = if editor.cookie_refresh_secs.trim().is_empty() {
                        super::config::DEFAULT_COOKIE_REFRESH_SECS
                    } else {
                        editor
                            .cookie_refresh_secs
                            .trim()
                            .parse::<u64>()
                            .context("Cookie 刷新间隔必须是整数")?
                    };
                    if !(MIN_COOKIE_REFRESH_SECS..=MAX_COOKIE_REFRESH_SECS).contains(&value) {
                        bail!(
                            "Cookie 刷新间隔必须在 {MIN_COOKIE_REFRESH_SECS}–{MAX_COOKIE_REFRESH_SECS} 秒之间"
                        );
                    }
                    Ok(value)
                })();
                match interval {
                    Ok(interval) => {
                        self.app_settings_editor = None;
                        let interval_changed = self.cookie_refresh_secs != interval;
                        self.theme = editor.theme;
                        self.cookie_refresh_secs = interval;
                        if interval_changed && let Some(updates) = &self.cookie_interval_updates {
                            let _ = updates.send(Duration::from_secs(self.cookie_refresh_secs));
                            self.cookie_cycle_started = Some(Instant::now());
                        }
                        self.persist_gui_state();
                    }
                    Err(error) => {
                        if let Some(editor) = &mut self.app_settings_editor {
                            editor.error = Some(format!("{error:#}"));
                        }
                    }
                }
            }
        }
        if self.theme != old_theme {
            context.set_theme(theme_preference(self.theme));
            apply_gui_style(context);
            self.persist_gui_state();
        }
        context.request_repaint_after(Duration::from_millis(100));
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.persist_gui_state();
    }
}

fn forward_rule(tunnel: &TunnelConfig) -> Result<ForwardRule> {
    Ok(ForwardRule {
        name: tunnel.name.clone(),
        target: parse_target(&tunnel.target)?,
        listen: parse_listen(&tunnel.listen)?,
    })
}

fn toggle_switch(ui: &mut egui::Ui, value: &mut bool) -> egui::Response {
    let (outer, mut response) =
        ui.allocate_exact_size(egui::vec2(32.0, 26.0), egui::Sense::click());
    if response.clicked() {
        *value = !*value;
        response.mark_changed();
    }
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::Checkbox,
            ui.is_enabled(),
            *value,
            "启用隧道",
        )
    });
    if ui.is_rect_visible(outer) {
        let rect = egui::Rect::from_center_size(outer.center(), egui::vec2(32.0, 18.0));
        let amount = ui.ctx().animate_bool_responsive(response.id, *value);
        let visuals = ui.style().interact_selectable(&response, *value);
        let track_stroke = if ui.visuals().dark_mode {
            visuals.bg_stroke
        } else if *value {
            egui::Stroke::new(1.0, egui::Color32::from_rgb(12, 103, 151))
        } else {
            egui::Stroke::new(1.0, egui::Color32::from_rgb(160, 168, 178))
        };
        let radius = rect.height() / 2.0;
        ui.painter().rect(
            rect,
            radius,
            visuals.bg_fill,
            track_stroke,
            egui::StrokeKind::Inside,
        );
        let center_x = egui::lerp((rect.left() + radius)..=(rect.right() - radius), amount);
        ui.painter().circle_filled(
            egui::pos2(center_x, rect.center().y),
            radius * 0.72,
            visuals.fg_stroke.color,
        );
    }
    response
}

fn export_checkbox(ui: &mut egui::Ui, value: &mut bool) -> egui::Response {
    let (outer, mut response) =
        ui.allocate_exact_size(egui::vec2(22.0, 26.0), egui::Sense::click());
    if response.clicked() {
        *value = !*value;
        response.mark_changed();
    }
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::Checkbox,
            ui.is_enabled(),
            *value,
            "选择用于导出",
        )
    });
    if ui.is_rect_visible(outer) {
        let visuals = ui.style().interact_selectable(&response, *value);
        let rect = egui::Rect::from_center_size(outer.center(), egui::vec2(15.0, 15.0));
        ui.painter().rect(
            rect,
            2.0,
            if *value {
                visuals.bg_fill
            } else {
                egui::Color32::TRANSPARENT
            },
            visuals.fg_stroke,
            egui::StrokeKind::Inside,
        );
        if *value {
            ui.painter().line_segment(
                [
                    egui::pos2(rect.left() + 3.5, rect.center().y),
                    egui::pos2(rect.center().x - 0.5, rect.bottom() - 4.0),
                ],
                egui::Stroke::new(1.8, visuals.fg_stroke.color),
            );
            ui.painter().line_segment(
                [
                    egui::pos2(rect.center().x - 0.5, rect.bottom() - 4.0),
                    egui::pos2(rect.right() - 3.0, rect.top() + 3.5),
                ],
                egui::Stroke::new(1.8, visuals.fg_stroke.color),
            );
        }
    }
    response
}

fn centered_dialog_content<R>(
    ui: &mut egui::Ui,
    width: f32,
    add_contents: impl FnOnce(&mut egui::Ui) -> R,
) -> R {
    let width = width.min(ui.available_width());
    let side_margin = ((ui.available_width() - width) / 2.0).max(0.0);
    ui.horizontal(|ui| {
        ui.spacing_mut().item_spacing.x = 0.0;
        ui.add_space(side_margin);
        ui.allocate_ui_with_layout(
            egui::vec2(width, 0.0),
            egui::Layout::top_down(egui::Align::Min),
            add_contents,
        )
        .inner
    })
    .inner
}

fn dialog_button_row<R>(ui: &mut egui::Ui, add_buttons: impl FnOnce(&mut egui::Ui) -> R) -> R {
    ui.allocate_ui_with_layout(
        egui::vec2(ui.available_width(), ui.spacing().interact_size.y),
        egui::Layout::right_to_left(egui::Align::Center),
        add_buttons,
    )
    .inner
}

fn compact_icon_button(ui: &mut egui::Ui, symbol: &str, destructive: bool) -> egui::Response {
    let button_size = if symbol.chars().count() > 1 {
        egui::vec2(84.0, 22.0)
    } else {
        egui::vec2(20.0, 20.0)
    };
    let (rect, response) = ui.allocate_exact_size(button_size, egui::Sense::click());
    response.widget_info(|| {
        egui::WidgetInfo::labeled(
            egui::WidgetType::Button,
            ui.is_enabled(),
            if destructive {
                "删除隧道"
            } else {
                "添加隧道"
            },
        )
    });
    if ui.is_rect_visible(rect) {
        let visuals = ui.style().interact(&response);
        let fill = if destructive {
            if response.hovered() || response.has_focus() {
                egui::Color32::from_rgb(210, 55, 55)
            } else {
                egui::Color32::from_rgb(190, 45, 45)
            }
        } else {
            visuals.weak_bg_fill
        };
        ui.painter()
            .rect(rect, 4.0, fill, visuals.bg_stroke, egui::StrokeKind::Inside);
        ui.painter().text(
            rect.center(),
            egui::Align2::CENTER_CENTER,
            symbol,
            egui::FontId::proportional(14.0),
            if destructive {
                egui::Color32::WHITE
            } else {
                visuals.fg_stroke.color
            },
        );
    }
    response
}

fn primary_button(ui: &mut egui::Ui, label: &str) -> egui::Response {
    ui.add(
        egui::Button::new(egui::RichText::new(label).color(egui::Color32::WHITE))
            .fill(egui::Color32::from_rgb(20, 125, 180)),
    )
}

fn primary_button_enabled(
    ui: &mut egui::Ui,
    enabled: bool,
    label: &str,
    size: egui::Vec2,
) -> egui::Response {
    ui.add_enabled_ui(enabled, |ui| {
        ui.add_sized(
            size,
            egui::Button::new(egui::RichText::new(label).color(egui::Color32::WHITE))
                .fill(egui::Color32::from_rgb(20, 125, 180)),
        )
    })
    .inner
}

fn settings_button(ui: &mut egui::Ui) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(32.0, 32.0), egui::Sense::click());
    response.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::Button, ui.is_enabled(), "设置")
    });
    if !ui.is_rect_visible(rect) {
        return response;
    }
    let visuals = ui.style().interact(&response);
    ui.painter().rect_filled(rect, 6.0, visuals.weak_bg_fill);
    let center = rect.center();
    let color = visuals.fg_stroke.color;
    let stroke = egui::Stroke::new(1.8, color);
    ui.painter().circle_stroke(center, 6.0, stroke);
    ui.painter().circle_stroke(center, 2.2, stroke);
    for index in 0..8 {
        let angle = index as f32 * std::f32::consts::TAU / 8.0;
        let direction = egui::vec2(angle.cos(), angle.sin());
        ui.painter().line_segment(
            [center + direction * 7.0, center + direction * 10.0],
            stroke,
        );
    }
    response
}

fn trash_button(ui: &mut egui::Ui) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(32.0, 32.0), egui::Sense::click());
    response.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::Button, ui.is_enabled(), "删除连接")
    });
    if !ui.is_rect_visible(rect) {
        return response;
    }
    let fill = if response.hovered() || response.has_focus() {
        egui::Color32::from_rgb(210, 55, 55)
    } else {
        egui::Color32::from_rgb(190, 45, 45)
    };
    ui.painter().rect_filled(rect, 6.0, fill);
    let center = rect.center();
    let stroke = egui::Stroke::new(1.7, egui::Color32::WHITE);
    let body = egui::Rect::from_min_max(
        egui::pos2(center.x - 5.5, center.y - 3.0),
        egui::pos2(center.x + 5.5, center.y + 7.0),
    );
    ui.painter()
        .rect_stroke(body, 1.0, stroke, egui::StrokeKind::Inside);
    ui.painter().line_segment(
        [
            egui::pos2(center.x - 7.0, center.y - 6.0),
            egui::pos2(center.x + 7.0, center.y - 6.0),
        ],
        stroke,
    );
    ui.painter().line_segment(
        [
            egui::pos2(center.x - 2.5, center.y - 8.0),
            egui::pos2(center.x + 2.5, center.y - 8.0),
        ],
        stroke,
    );
    response
}

fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    )
}

fn cookie_refresh_countdown(cycle_started: Option<Instant>, interval_secs: u64) -> String {
    let Some(cycle_started) = cycle_started else {
        return "--:--:--".to_string();
    };
    let remaining = interval_secs.saturating_sub(cycle_started.elapsed().as_secs());
    format!("{} 后", format_elapsed(Duration::from_secs(remaining)))
}

fn localize_log_line(line: &str) -> String {
    let (tag, body) = split_log_tag(line);
    format!("{tag} {}", localize_log_body(body))
}

fn split_log_tag(line: &str) -> (&str, &str) {
    if line.starts_with('[')
        && let Some(end) = line.find("] ")
    {
        return (&line[..=end], &line[end + 2..]);
    }
    line.split_once(' ').unwrap_or(("[towc]", line))
}

fn updates_tunnel_state(message: &str) -> bool {
    message.starts_with("ready:")
        || message == "disabled"
        || message.starts_with("enable failed:")
        || message.starts_with("listener failed:")
        || (message.starts_with("tows ") && message.contains(" failed:"))
}

fn localize_log_body(message: &str) -> String {
    let exact = match message {
        "reusing a valid WebVPN login cache" => "正在复用有效的 WebVPN 登录缓存",
        "WebVPN login required" => "需要登录 WebVPN",
        "WebVPN login completed" => "WebVPN 登录完成",
        "requesting WeChat QR code" => "正在获取微信登录二维码",
        "scan the WeChat QR code and confirm on your phone" => "请使用微信扫码并在手机上确认",
        "WeChat QR code expired; requesting a new code" => "微信二维码已过期，正在自动刷新",
        "WeChat confirmed; activating WebVPN ticket" => "微信已确认，正在激活 WebVPN 凭据",
        "QR code scanned; waiting for phone confirmation" => "二维码已扫描，正在等待手机确认",
        "verification code sent" => "验证码已发送",
        "an unexpired verification code already exists; use it directly" => {
            "已有未过期的验证码，可直接使用"
        }
        "WebVPN cookie refreshed" => "WebVPN Cookie 已刷新",
        "all local listeners stopped" => "所有本地监听已停止",
        "disabled" => "已禁用",
        "peer closed" => "对端已关闭连接",
        "OPEN timed out after 15 seconds" => "打开连接超时（15 秒）",
        "runtime stopped; cannot apply configuration change" => "运行任务已停止，无法应用配置修改",
        "tunnel update rejected: invalid tows address" => "隧道更新被拒绝：tows 地址无效",
        "tunnel update rejected: a tows connection exceeds the rule limit" => {
            "隧道更新被拒绝：单个 tows 连接超过隧道数量上限"
        }
        "connection rejected: 64 concurrent streams are already open" => {
            "连接被拒绝：当前已有 64 个并发数据流"
        }
        _ => "",
    };
    if !exact.is_empty() {
        return exact.to_string();
    }

    for (prefix, translated) in [
        ("connecting to tows ", "正在连接 tows "),
        ("connected to tows ", "已连接 tows "),
        ("tows task failed: ", "tows 任务失败："),
        ("tunnel update rejected: ", "隧道更新被拒绝："),
        ("enable failed: ", "启用失败："),
        ("listener failed: ", "监听失败："),
        ("local connection from ", "收到本地连接："),
        ("opening stream ", "正在打开数据流 "),
        ("open failed: ", "打开连接失败："),
        ("stream ", "数据流 "),
        ("sending mobile verification code", "正在发送短信验证码"),
        ("sending email verification code", "正在发送邮箱验证码"),
        ("cannot start: ", "无法启动："),
        ("cannot save GUI state: ", "无法保存界面设置："),
        ("configuration change rejected: ", "配置修改被拒绝："),
        ("cannot apply configuration change: ", "无法应用配置修改："),
        ("cannot save configuration change: ", "无法保存配置修改："),
        ("cannot display QR code: ", "无法显示二维码："),
        (
            "connection failed; all listeners stopped: ",
            "连接失败，所有监听已停止：",
        ),
        ("exported to ", "已导出到 "),
        ("export failed: ", "导出失败："),
        ("imported ", "已导入 "),
        ("read ", "已读取 "),
        ("tows connection ", "tows 连接 "),
        ("invalid input: ", "输入无效："),
        ("skipped non-JSON file ", "已跳过非 JSON 文件："),
        ("skipped ", "已跳过："),
        ("cannot read directory ", "无法读取目录："),
        ("path does not exist: ", "路径不存在："),
        (
            "could not read WebVPN cookie cache; signing in again: ",
            "无法读取 WebVPN Cookie 缓存，将重新登录：",
        ),
        (
            "cookie cache directory is unavailable",
            "Cookie 缓存目录不可用",
        ),
        (
            "could not save cookie cache; current session is unaffected: ",
            "无法保存 Cookie 缓存，当前会话不受影响：",
        ),
    ] {
        if let Some(rest) = message.strip_prefix(prefix) {
            return format!("{translated}{rest}");
        }
    }

    if let Some(rest) = message.strip_prefix("ready: ") {
        return format!("已就绪：{rest}");
    }
    if let Some(rest) = message.strip_suffix(" tows connections active") {
        return format!("{rest} 个 tows 连接处于活动状态");
    }
    if let Some((active, total)) = message
        .strip_suffix(" tows connections active")
        .and_then(|value| value.split_once('/'))
    {
        return format!("{active}/{total} 个 tows 连接处于活动状态");
    }
    if let Some(server) = message
        .strip_suffix(" keepalive stopped")
        .and_then(|value| value.strip_prefix("tows "))
    {
        return format!("tows {server} 保活已停止");
    }
    if let Some((server, error)) = message
        .strip_prefix("tows ")
        .and_then(|value| value.split_once(" failed: "))
    {
        return format!("tows {server} 连接失败：{error}");
    }
    if let Some(interval) = message
        .strip_prefix("Cookie keepalive interval updated to ")
        .and_then(|value| value.strip_suffix(" seconds"))
    {
        return format!("Cookie 刷新间隔已更新为 {interval} 秒");
    }
    message.to_string()
}

fn colored_log_line(ui: &mut egui::Ui, line: &str) {
    let (tag, body) = split_log_tag(line);
    let dark = ui.visuals().dark_mode;
    let tag_color = match tag {
        "[towc]" => {
            if dark {
                egui::Color32::from_rgb(70, 190, 225)
            } else {
                egui::Color32::from_rgb(0, 105, 155)
            }
        }
        "[tunnel]" => {
            if dark {
                egui::Color32::from_rgb(80, 210, 130)
            } else {
                egui::Color32::from_rgb(20, 125, 70)
            }
        }
        "[tows]" => {
            if dark {
                egui::Color32::from_rgb(210, 130, 225)
            } else {
                egui::Color32::from_rgb(135, 65, 155)
            }
        }
        _ => {
            if dark {
                egui::Color32::from_rgb(80, 210, 130)
            } else {
                egui::Color32::from_rgb(20, 125, 70)
            }
        }
    };
    let font_id = egui::FontId::monospace(14.0);
    let mut job = egui::text::LayoutJob::default();
    job.wrap.max_width = ui.available_width();
    job.append(
        tag,
        0.0,
        egui::TextFormat {
            font_id: font_id.clone(),
            color: tag_color,
            ..Default::default()
        },
    );
    job.append(
        &format!(" {body}"),
        0.0,
        egui::TextFormat {
            font_id,
            color: ui.visuals().text_color(),
            ..Default::default()
        },
    );
    ui.add(egui::Label::new(job).wrap());
}

#[cfg(test)]
mod log_tests {
    use super::{localize_log_line, updates_tunnel_state};

    #[test]
    fn unicode_tunnel_names_are_preserved() {
        assert_eq!(localize_log_line("[隧道 4] disabled"), "[隧道 4] 已禁用");
        assert!(!localize_log_line("[隧道 4] 已启用").contains("\\u{"));
    }

    #[test]
    fn common_runtime_messages_are_localized() {
        assert_eq!(
            localize_log_line("[towc] connected to tows 10.18.47.77:4489"),
            "[towc] 已连接 tows 10.18.47.77:4489"
        );
        assert_eq!(
            localize_log_line("[towc] WebVPN cookie refreshed"),
            "[towc] WebVPN Cookie 已刷新"
        );
    }

    #[test]
    fn stream_activity_does_not_replace_tunnel_health() {
        assert!(updates_tunnel_state(
            "ready: 127.0.0.1:14489 -> 10.18.47.77:4489 -> 127.0.0.1:80"
        ));
        assert!(updates_tunnel_state(
            "tows 10.18.47.77:4489 failed: WebSocket disconnected"
        ));
        assert!(!updates_tunnel_state("stream 1 established"));
        assert!(!updates_tunnel_state("bidirectional EOF"));
        assert!(!updates_tunnel_state(
            "open failed: target service refused the connection"
        ));
    }
}

fn apply_gui_style(context: &egui::Context) {
    context.all_styles_mut(|style| {
        style.spacing.item_spacing = egui::vec2(10.0, 8.0);
        style.spacing.button_padding = egui::vec2(12.0, 6.0);
        style.spacing.interact_size = egui::vec2(40.0, 32.0);
        style.visuals.selection.bg_fill = egui::Color32::from_rgb(20, 125, 180);
        style.visuals.selection.stroke = egui::Stroke::new(1.0, egui::Color32::WHITE);
        style.visuals.weak_text_alpha = 0.42;
        for widgets in [
            &mut style.visuals.widgets.inactive,
            &mut style.visuals.widgets.hovered,
            &mut style.visuals.widgets.active,
            &mut style.visuals.widgets.open,
        ] {
            widgets.corner_radius = egui::CornerRadius::same(6);
        }
    });
    context.style_mut_of(egui::Theme::Light, |style| {
        let visuals = &mut style.visuals;
        visuals.panel_fill = egui::Color32::from_rgb(242, 244, 247);
        visuals.window_fill = egui::Color32::from_rgb(242, 244, 247);
        visuals.faint_bg_color = egui::Color32::from_rgb(232, 235, 239);
        visuals.extreme_bg_color = egui::Color32::from_rgb(251, 252, 253);
        visuals.text_edit_bg_color = Some(egui::Color32::from_rgb(251, 252, 253));
        visuals.widgets.noninteractive.weak_bg_fill = visuals.panel_fill;
        visuals.widgets.noninteractive.bg_fill = visuals.panel_fill;
        visuals.widgets.inactive.weak_bg_fill = egui::Color32::from_rgb(212, 218, 226);
        visuals.widgets.inactive.bg_fill = egui::Color32::from_rgb(212, 218, 226);
        visuals.widgets.inactive.bg_stroke =
            egui::Stroke::new(1.0, egui::Color32::from_rgb(184, 191, 200));
        visuals.widgets.hovered.weak_bg_fill = egui::Color32::from_rgb(196, 205, 216);
        visuals.widgets.hovered.bg_fill = egui::Color32::from_rgb(196, 205, 216);
        visuals.widgets.hovered.bg_stroke =
            egui::Stroke::new(1.0, egui::Color32::from_rgb(120, 142, 162));
        visuals.widgets.active.bg_stroke =
            egui::Stroke::new(1.5, egui::Color32::from_rgb(20, 125, 180));
        visuals.widgets.open.weak_bg_fill = egui::Color32::from_rgb(204, 212, 221);
        visuals.widgets.open.bg_fill = egui::Color32::from_rgb(204, 212, 221);
    });
}

fn theme_preference(theme: ThemeSetting) -> egui::ThemePreference {
    match theme {
        ThemeSetting::System => egui::ThemePreference::System,
        ThemeSetting::Dark => egui::ThemePreference::Dark,
        ThemeSetting::Light => egui::ThemePreference::Light,
    }
}

impl TunnelEdit {
    fn empty() -> Self {
        Self {
            target: EndpointEdit {
                host: String::new(),
                port: String::new(),
            },
            listen: EndpointEdit {
                host: String::new(),
                port: String::new(),
            },
        }
    }

    fn from_tunnel(tunnel: &TunnelConfig) -> Self {
        fn edit(
            value: &str,
            parser: fn(&str) -> anyhow::Result<crate::address::Endpoint>,
        ) -> EndpointEdit {
            parser(value).map_or_else(
                |_| EndpointEdit {
                    host: String::new(),
                    port: String::new(),
                },
                |endpoint| EndpointEdit {
                    host: endpoint.host().to_string(),
                    port: endpoint.port().to_string(),
                },
            )
        }

        Self {
            target: edit(&tunnel.target, parse_target),
            listen: edit(&tunnel.listen, parse_listen),
        }
    }
}

fn tunnel_edits(config: &GuiConfig) -> Vec<TunnelEdit> {
    config.tunnels.iter().map(TunnelEdit::from_tunnel).collect()
}

fn endpoint_edit_value(edit: &EndpointEdit, default_host: &str, default_port: &str) -> String {
    let host = if edit.host.trim().is_empty() {
        default_host
    } else {
        edit.host.trim()
    };
    let port = if edit.port.trim().is_empty() {
        default_port
    } else {
        edit.port.trim()
    };

    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn desktop_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("USERPROFILE")
        .map(std::path::PathBuf::from)
        .map(|home| home.join("Desktop"))
        .filter(|path| path.is_dir())
}

fn status_indicator(ui: &mut egui::Ui, color: egui::Color32, detail: &str) {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
    ui.painter().circle_filled(rect.center(), 5.0, color);
    let mut characters = detail.chars();
    let capitalized = characters
        .next()
        .map(|first| first.to_uppercase().collect::<String>() + characters.as_str())
        .unwrap_or_default();
    response.on_hover_text(capitalized);
}

fn metric_card(ui: &mut egui::Ui, label: &str, value: String) {
    egui::Frame::new()
        .fill(ui.visuals().faint_bg_color)
        .inner_margin(12.0)
        .corner_radius(8.0)
        .show(ui, |ui| {
            ui.allocate_ui_with_layout(
                egui::vec2(150.0, 32.0),
                egui::Layout::left_to_right(egui::Align::Center),
                |ui| {
                    ui.weak(label);
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new(value).size(20.0).strong());
                },
            );
        });
}

fn qr_texture(context: &egui::Context, bytes: &[u8]) -> Result<egui::TextureHandle> {
    let image = image::load_from_memory(bytes)
        .context("cannot decode QR code")?
        .to_rgba8();
    let size = [image.width() as usize, image.height() as usize];
    let color = egui::ColorImage::from_rgba_unmultiplied(size, image.as_raw());
    Ok(context.load_texture("wechat-qr", color, egui::TextureOptions::NEAREST))
}

fn install_chinese_font(context: &egui::Context) {
    let windows = std::env::var_os("WINDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(r"C:\Windows"));
    let candidates = ["msyh.ttc", "msyhbd.ttc", "simhei.ttf"];
    for candidate in candidates {
        let path = windows.join("Fonts").join(candidate);
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        let mut fonts = egui::FontDefinitions::default();
        fonts.font_data.insert(
            "chinese".to_string(),
            egui::FontData::from_owned(bytes).into(),
        );
        for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
            fonts
                .families
                .entry(family)
                .or_default()
                .insert(0, "chinese".to_string());
        }
        context.set_fonts(fonts);
        return;
    }
}
