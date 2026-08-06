use anyhow::{Context, Result, bail};
use eframe::egui;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

use crate::address::{parse_listen, parse_target, parse_tows};
use crate::client::{
    AuthPrompt, ClientObserver, ForwardRule, LoginPreference, ServerGroup,
    login_or_restore_for_server, run_dynamic_server_groups,
};

use super::config::{
    GuiConfig, GuiState, ImportBundle, MergePolicy, ThemeSetting, TunnelConfig, export_tunnels,
    import_conflicts, listen_conflicts, load_default_config, load_gui_state, merge_import,
    read_import_paths, save_default_config, save_gui_state, validate_config,
};

pub fn run() -> Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1180.0, 900.0])
            .with_min_inner_size([1000.0, 700.0])
            .with_max_inner_size([1600.0, 900.0])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        "tcp_over_websocket",
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
                _ => bail!("mobile login requires a numeric phone number"),
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
    identity: String,
    running: bool,
    status: String,
    tunnel_status: HashMap<String, String>,
    logs: Vec<String>,
    events: Option<std_mpsc::Receiver<WorkerEvent>>,
    stop: Option<watch::Sender<bool>>,
    updates: Option<mpsc::UnboundedSender<Vec<ServerGroup>>>,
    pending_code: Option<(String, std_mpsc::Sender<String>)>,
    code_input: String,
    qr_texture: Option<egui::TextureHandle>,
    pending_import: Option<ImportBundle>,
    export_selected: HashSet<String>,
    auto_start_pending: bool,
    login_visible: bool,
    theme: ThemeSetting,
    connected_since: Option<Instant>,
    editing_snapshot: Option<GuiConfig>,
    connected_servers: HashSet<String>,
    last_cookie_refresh: Option<Instant>,
    restart_when_stopped: bool,
    tunnel_edits: Vec<TunnelEdit>,
    add_first_tunnel: bool,
    new_server_host: String,
    new_server_port: String,
}

impl TowcApp {
    fn new(creation: &eframe::CreationContext<'_>) -> Self {
        install_chinese_font(&creation.egui_ctx);
        let gui_state = load_gui_state();
        creation
            .egui_ctx
            .set_theme(theme_preference(gui_state.theme));
        creation.egui_ctx.style_mut(|style| {
            style.spacing.item_spacing = egui::vec2(10.0, 8.0);
            style.spacing.button_padding = egui::vec2(12.0, 6.0);
            style.visuals.selection.bg_fill = egui::Color32::from_rgb(20, 125, 180);
            style.visuals.widgets.inactive.corner_radius = egui::CornerRadius::same(6);
            style.visuals.widgets.hovered.corner_radius = egui::CornerRadius::same(6);
            style.visuals.widgets.active.corner_radius = egui::CornerRadius::same(6);
        });
        let loaded = load_default_config();
        let tunnel_edits = tunnel_edits(&loaded.config);
        let auto_start_pending =
            !loaded.save_blocked && loaded.config.tunnels.iter().any(|tunnel| tunnel.enabled);
        let mut app = Self {
            config: loaded.config,
            save_blocked: loaded.save_blocked,
            warning: loaded.warning,
            login_kind: LoginKind::default(),
            identity: String::new(),
            running: false,
            status: "未启动".to_string(),
            tunnel_status: HashMap::new(),
            logs: Vec::new(),
            events: None,
            stop: None,
            updates: None,
            pending_code: None,
            code_input: String::new(),
            qr_texture: None,
            pending_import: None,
            export_selected: gui_state.selected_tunnels,
            auto_start_pending,
            login_visible: !auto_start_pending,
            theme: gui_state.theme,
            connected_since: None,
            editing_snapshot: None,
            connected_servers: HashSet::new(),
            last_cookie_refresh: None,
            restart_when_stopped: false,
            tunnel_edits,
            add_first_tunnel: false,
            new_server_host: String::new(),
            new_server_port: String::new(),
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
                self.log(format!("cannot start: {error:#}"));
                return;
            }
        };
        if let Err(error) = save_default_config(&self.config) {
            self.log(format!(
                "cannot save configuration; startup cancelled: {error:#}"
            ));
            return;
        }

        let (event_tx, event_rx) = std_mpsc::channel();
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let (updates_tx, updates_rx) = mpsc::unbounded_channel();
        let probe_server = groups[0].server.clone();
        self.events = Some(event_rx);
        self.stop = Some(stop_tx);
        self.updates = Some(updates_tx);
        self.running = true;
        self.status = "正在检查 WebVPN 登录状态".to_string();
        self.qr_texture = None;
        self.tunnel_status.clear();
        self.connected_servers.clear();
        self.last_cookie_refresh = None;

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
                        let cookie = tokio::select! {
                            result = login_or_restore_for_server(auth, preference, &probe_server) => result?,
                            changed = stop_rx.changed() => {
                                if changed.is_err() || *stop_rx.borrow() {
                                    return Ok(());
                                }
                                return Ok(());
                            }
                        };
                        run_dynamic_server_groups(groups, cookie, stop_rx, updates_rx, observer)
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

        let preference = self.login_kind.preference(self.identity.trim())?;
        Ok((preference, self.server_groups()?))
    }

    fn server_groups(&self) -> Result<Vec<ServerGroup>> {
        let mut groups = Vec::<ServerGroup>::new();
        for tunnel in &self.config.tunnels {
            let server = parse_tows(&tunnel.tows)?;
            if let Some(group) = groups.iter_mut().find(|group| group.server == server) {
                if tunnel.enabled {
                    group.rules.push(forward_rule(tunnel)?);
                }
            } else {
                groups.push(ServerGroup {
                    server,
                    rules: if tunnel.enabled {
                        vec![forward_rule(tunnel)?]
                    } else {
                        Vec::new()
                    },
                });
            }
        }
        if groups.is_empty() {
            bail!("at least one tunnel must be configured");
        }
        Ok(groups)
    }

    fn set_tunnel_enabled(&mut self, index: usize, enabled: bool) {
        let previous_config = self.config.clone();
        self.config.tunnels[index].enabled = enabled;
        let name = self.config.tunnels[index].name.clone();
        let applied = self.apply_config_change(
            previous_config,
            format!(
                "tunnel {name} {}",
                if enabled { "enabled" } else { "disabled" }
            ),
        );
        if applied && enabled && !self.running {
            self.start();
        }
    }

    fn persist_gui_state(&mut self) {
        let state = GuiState {
            theme: self.theme,
            selected_tunnels: self.export_selected.clone(),
        };
        if let Err(error) = save_gui_state(&state) {
            self.log(format!("cannot save GUI state: {error:#}"));
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
            self.log(format!("configuration change rejected: {error:#}"));
            return false;
        }
        if self.running && configured_servers(&previous) != configured_servers(&self.config) {
            self.config = previous;
            self.log("tows server groups cannot change while connected".to_string());
            return false;
        }
        let runtime_groups = if self.running {
            match self.server_groups() {
                Ok(groups) => Some(groups),
                Err(error) => {
                    self.config = previous;
                    self.log(format!("cannot apply configuration change: {error:#}"));
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
            self.log(format!("cannot save configuration change: {error:#}"));
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
                self.log("runtime stopped; cannot apply configuration change".to_string());
                return false;
            }
        }
        self.log(success);
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
        let mut dialog = rfd::FileDialog::new()
            .add_filter("JSON configuration", &["json"])
            .set_file_name("tunnels.json");
        if let Some(desktop) = desktop_dir() {
            dialog = dialog.set_directory(desktop);
        }
        let Some(path) = dialog.save_file() else {
            return;
        };
        match export_tunnels(&path, tunnels) {
            Ok(()) => self.log(format!("exported to {}", path.display())),
            Err(error) => self.log(format!("export failed: {error:#}")),
        }
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(true);
            self.status = "正在停止".to_string();
        }
        self.updates = None;
        self.connected_since = None;
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
                        || message.starts_with("connecting to tows")
                    {
                        self.login_visible = false;
                        self.qr_texture = None;
                        self.pending_code = None;
                    }
                    if message.starts_with("connected to tows ") && self.connected_since.is_none() {
                        self.connected_since = Some(Instant::now());
                    }
                    if let Some(server) = message.strip_prefix("connected to tows ") {
                        self.connected_servers.insert(server.to_string());
                    }
                    if message == "WebVPN cookie refreshed" {
                        self.last_cookie_refresh = Some(Instant::now());
                    }
                    self.status = message.clone();
                    self.log(message);
                }
                WorkerEvent::Tunnel(name, message) => {
                    self.tunnel_status.insert(name.clone(), message.clone());
                    self.log(format!("[tunnel] [{name}] {message}"));
                }
                WorkerEvent::Qr(bytes) => match qr_texture(context, &bytes) {
                    Ok(texture) => {
                        self.login_visible = true;
                        self.qr_texture = Some(texture);
                    }
                    Err(error) => self.log(format!("cannot display QR code: {error:#}")),
                },
                WorkerEvent::CodeRequest { label, reply } => {
                    self.pending_code = Some((label, reply));
                    self.code_input.clear();
                }
                WorkerEvent::Log(message) => self.log(message),
                WorkerEvent::Finished(result) => {
                    self.running = false;
                    self.stop = None;
                    self.updates = None;
                    self.pending_code = None;
                    self.qr_texture = None;
                    self.connected_since = None;
                    self.connected_servers.clear();
                    match result {
                        Ok(()) => {
                            self.status = "已停止".to_string();
                            self.log("all local listeners stopped".to_string());
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
                            self.log(format!("connection failed; all listeners stopped: {error}"));
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
        let message = if message.starts_with("[towc] ")
            || message.starts_with("[tunnel] ")
            || message.starts_with("[tows] ")
        {
            message
        } else {
            format!("[towc] {message}")
        };
        let message = message.chars().flat_map(char::escape_default).collect();
        self.logs.push(message);
        if self.logs.len() > 500 {
            self.logs.drain(..self.logs.len() - 500);
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
            if self.apply_config_change(
                previous,
                format!("imported {count} tunnels; source files were not modified"),
            ) {
                self.tunnel_edits = tunnel_edits(&self.config);
            }
        }
    }

    fn stage_import(&mut self, paths: &[std::path::PathBuf]) {
        let bundle = read_import_paths(paths);
        self.log(format!(
            "read {} configuration files containing {} tunnels",
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

        egui::CentralPanel::default().show(context, |ui| {
            let panel_width = ui.available_width();
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    let visible_width = panel_width;
                    ui.set_width(visible_width);
                    let mut auth_start_requested = false;
                    let mut login_method_clicked = false;
                    let old_theme = self.theme;
                    ui.horizontal(|ui| {
                        ui.heading("认证状态");
                        ui.add_space((visible_width - 410.0).max(12.0));
                        ui.label("主题");
                        if ui
                            .selectable_label(self.theme == ThemeSetting::System, "跟随系统")
                            .clicked()
                        {
                            self.theme = ThemeSetting::System;
                        }
                        if ui
                            .selectable_label(self.theme == ThemeSetting::Dark, "深色")
                            .clicked()
                        {
                            self.theme = ThemeSetting::Dark;
                        }
                        if ui
                            .selectable_label(self.theme == ThemeSetting::Light, "浅色")
                            .clicked()
                        {
                            self.theme = ThemeSetting::Light;
                        }
                    });
                    if self.theme != old_theme {
                        context.set_theme(theme_preference(self.theme));
                        self.persist_gui_state();
                    }
                    egui::Frame::group(ui.style())
                        .inner_margin(16.0)
                        .corner_radius(10.0)
                        .show(ui, |ui| {
                            ui.set_min_height(if self.login_visible { 260.0 } else { 170.0 });
                            ui.set_min_width(ui.available_width());
                            if let Some(warning) = &self.warning {
                                ui.colored_label(egui::Color32::YELLOW, warning);
                                if self.save_blocked
                                    && ui.button("确认使用当前界面配置并允许保存").clicked()
                                {
                                    self.save_blocked = false;
                                    self.warning = None;
                                    self.auto_start_pending =
                                        self.config.tunnels.iter().any(|tunnel| tunnel.enabled);
                                    self.log(
                                "configuration protection disabled; source file was not modified"
                                    .to_string(),
                            );
                                }
                            }
                            if self.config.tunnels.is_empty() {
                                ui.vertical_centered(|ui| {
                                    ui.add_space(60.0);
                                    status_dot(ui, false);
                                    ui.heading("等待隧道配置");
                                    ui.label("请先导入配置，或在下方创建第一条隧道；启用隧道后将自动检查登录状态。");
                                });
                            } else if self.login_visible {
                                ui.horizontal(|ui| {
                                    ui.label("登录方式");
                                    login_method_clicked |= ui
                                        .selectable_value(
                                            &mut self.login_kind,
                                            LoginKind::Wechat,
                                            "微信登录",
                                        )
                                        .clicked();
                                    login_method_clicked |= ui
                                        .selectable_value(
                                            &mut self.login_kind,
                                            LoginKind::Mobile,
                                            "手机号登录",
                                        )
                                        .clicked();
                                    login_method_clicked |= ui
                                        .selectable_value(
                                            &mut self.login_kind,
                                            LoginKind::Email,
                                            "邮箱登录",
                                        )
                                        .clicked();
                                });
                                ui.add_space(8.0);
                                if self.login_kind == LoginKind::Wechat {
                                    ui.horizontal(|ui| {
                                        if let Some(texture) = &self.qr_texture {
                                            ui.image((texture.id(), egui::vec2(220.0, 220.0)));
                                        } else {
                                            ui.allocate_ui(egui::vec2(220.0, 220.0), |ui| {
                                                ui.centered_and_justified(|ui| {
                                                    ui.label("二维码已失效或尚未生成");
                                                });
                                            });
                                        }
                                        ui.vertical(|ui| {
                                            ui.heading("微信登录");
                                            ui.label("使用微信扫码并在手机上确认。");
                                            ui.label("二维码过期后可直接重新获取，无需重启程序。");
                                            ui.add_space(12.0);
                                            if ui
                                                .button(if self.qr_texture.is_some() {
                                                    "重新获取二维码"
                                                } else {
                                                    "获取二维码"
                                                })
                                                .clicked()
                                            {
                                                auth_start_requested = true;
                                            }
                                        });
                                    });
                                } else {
                                    ui.horizontal(|ui| {
                                        ui.label(if self.login_kind == LoginKind::Mobile {
                                            "手机号"
                                        } else {
                                            "邮箱"
                                        });
                                        ui.add_sized(
                                            [280.0, 30.0],
                                            egui::TextEdit::singleline(&mut self.identity)
                                                .hint_text(
                                                    if self.login_kind == LoginKind::Mobile {
                                                        "请输入手机号"
                                                    } else {
                                                        "请输入邮箱"
                                                    },
                                                ),
                                        );
                                        if ui.button("开始登录").clicked() {
                                            auth_start_requested = true;
                                        }
                                    });
                                    ui.add_space(18.0);
                                    ui.label(
                                        "验证码将在下一步发送，验证码内容不会写入日志或配置。",
                                    );
                                }
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
                                        self.tunnel_status
                                            .get(&tunnel.name)
                                            .is_some_and(|status| status.starts_with("ready:"))
                                    })
                                    .count();
                                ui.horizontal(|ui| {
                                    status_dot(ui, !self.connected_servers.is_empty());
                                    ui.heading(if self.connected_servers.is_empty() {
                                        "正在建立连接"
                                    } else {
                                        "认证有效，连接正常"
                                    });
                                });
                                ui.label(if self.connected_servers.is_empty() {
                                    self.status.as_str()
                                } else {
                                    "WebVPN 会话已建立，隧道按服务器共享保活连接。"
                                });
                                ui.add_space(12.0);
                                ui.horizontal(|ui| {
                                    metric_card(
                                        ui,
                                        "连接时长",
                                        self.connected_since
                                            .map(|since| format_elapsed(since.elapsed()))
                                            .unwrap_or_else(|| "--:--:--".to_string()),
                                    );
                                    metric_card(ui, "隧道", format!("{ready} / {enabled}"));
                                    metric_card(
                                        ui,
                                        "保活连接",
                                        self.connected_servers.len().to_string(),
                                    );
                                    metric_card(
                                        ui,
                                        "Cookie 刷新",
                                        self.last_cookie_refresh
                                            .map(|since| {
                                                format!("{} 前", format_elapsed(since.elapsed()))
                                            })
                                            .unwrap_or_else(|| "等待周期".to_string()),
                                    );
                                });
                            }
                            if let Some((label, _)) = &self.pending_code {
                                ui.separator();
                                ui.label(format!("请输入{label}验证码（不会写入日志或配置）："));
                                ui.horizontal(|ui| {
                                    let response = ui.text_edit_singleline(&mut self.code_input);
                                    let submit = ui.button("提交验证码").clicked()
                                        || (response.lost_focus()
                                            && ui.input(|input| {
                                                input.key_pressed(egui::Key::Enter)
                                            }));
                                    if submit
                                        && !self.code_input.trim().is_empty()
                                        && let Some((_, reply)) = self.pending_code.take()
                                    {
                                        let _ = reply.send(self.code_input.trim().to_string());
                                        self.code_input.clear();
                                    }
                                });
                            }
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
                    let mut remove = None;
                    let mut toggle = None;
                    let mut add_to_server = None;
                    let mut edit_started = false;
                    let mut edit_finished = false;
                    let mut selection_changed = false;
                    let mut groups = Vec::<(String, Vec<usize>)>::new();
                    for (index, tunnel) in self.config.tunnels.iter().enumerate() {
                        let server = parse_tows(&tunnel.tows)
                            .map(|server| server.to_string())
                            .unwrap_or_else(|_| tunnel.tows.clone());
                        if let Some((_, indices)) =
                            groups.iter_mut().find(|(value, _)| value == &server)
                        {
                            indices.push(index);
                        } else {
                            groups.push((server, vec![index]));
                        }
                    }
                    for (server, indices) in groups {
                        egui::Frame::group(ui.style())
                            .inner_margin(12.0)
                            .corner_radius(10.0)
                            .show(ui, |ui| {
                                ui.set_min_width(ui.available_width());
                                let parsed_server = parse_tows(&server).ok();
                                ui.horizontal(|ui| {
                                    ui.strong("服务器");
                                    if let Some(endpoint) = &parsed_server {
                                        ui.monospace(endpoint.to_string());
                                    } else {
                                        ui.colored_label(
                                            egui::Color32::from_rgb(225, 72, 72),
                                            &server,
                                        );
                                    }
                                });
                                ui.add_space(6.0);
                                egui::Grid::new(format!("tunnels-{server}"))
                                    .striped(true)
                                    .show(ui, |ui| {
                                        ui.label("");
                                        ui.label("启用");
                                        ui.label("名称");
                                        ui.label("目标地址");
                                        ui.label("端口");
                                        ui.label("监听地址");
                                        ui.label("端口");
                                        ui.label("状态");
                                        ui.end_row();
                                        for index in indices {
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
                                            if ui.checkbox(&mut selected, "").changed() {
                                                if selected {
                                                    self.export_selected
                                                        .insert(tunnel.name.clone());
                                                } else {
                                                    self.export_selected.remove(&tunnel.name);
                                                }
                                                selection_changed = true;
                                            }
                                            let mut enabled = tunnel.enabled;
                                            if toggle_switch(ui, &mut enabled).changed() {
                                                toggle = Some((index, enabled));
                                            }
                                            let name_response = ui.add_sized(
                                                [140.0, 22.0],
                                                egui::TextEdit::singleline(&mut tunnel.name)
                                                    .text_color(if name_valid {
                                                        normal_text
                                                    } else {
                                                        invalid_text
                                                    }),
                                            );
                                            let target_response = ui.add_sized(
                                                [135.0, 22.0],
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
                                                [62.0, 22.0],
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
                                                [135.0, 22.0],
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
                                                [68.0, 22.0],
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
                                            edit_started |= name_response.gained_focus()
                                                || target_response.gained_focus()
                                                || target_port_response.gained_focus()
                                                || listen_response.gained_focus()
                                                || listen_port_response.gained_focus();
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
                                            let (color, status) = if !name_valid {
                                                (
                                                    egui::Color32::from_rgb(225, 72, 72),
                                                    "名称为空或重复",
                                                )
                                            } else if !target_valid {
                                                (
                                                    egui::Color32::from_rgb(225, 72, 72),
                                                    "目标地址无效",
                                                )
                                            } else if !listen_valid {
                                                (
                                                    egui::Color32::from_rgb(225, 72, 72),
                                                    "监听地址无效",
                                                )
                                            } else if conflicts.contains(&tunnel.name) {
                                                (
                                                    egui::Color32::from_rgb(225, 72, 72),
                                                    "监听地址冲突",
                                                )
                                            } else if parse_listen(&tunnel.listen)
                                                .is_ok_and(|listen| !listen.is_loopback())
                                            {
                                                (
                                                    egui::Color32::from_rgb(235, 174, 52),
                                                    "监听已向局域网开放",
                                                )
                                            } else if runtime_status.starts_with("ready:") {
                                                (egui::Color32::from_rgb(42, 190, 116), "隧道可用")
                                            } else if !tunnel.enabled {
                                                (egui::Color32::from_gray(110), "隧道已禁用")
                                            } else if runtime_status.contains("failed")
                                                || runtime_status.contains("error")
                                            {
                                                (egui::Color32::from_rgb(225, 72, 72), "隧道异常")
                                            } else {
                                                (egui::Color32::from_rgb(235, 174, 52), "正在连接")
                                            };
                                            status_indicator(ui, color, status, runtime_status);
                                            if ui
                                                .add(
                                                    egui::Button::new(
                                                        egui::RichText::new("删除")
                                                            .color(egui::Color32::WHITE),
                                                    )
                                                    .fill(egui::Color32::from_rgb(190, 45, 45)),
                                                )
                                                .clicked()
                                            {
                                                remove = Some(index);
                                            }
                                            ui.end_row();
                                        }
                                    });
                                if ui.button("添加隧道").clicked() {
                                    add_to_server = Some(server.clone());
                                }
                            });
                        ui.add_space(6.0);
                    }
                    if self.config.tunnels.is_empty() {
                        egui::Frame::group(ui.style())
                            .inner_margin(24.0)
                            .corner_radius(10.0)
                            .show(ui, |ui| {
                                ui.set_min_width(ui.available_width());
                                ui.vertical_centered(|ui| {
                                    ui.heading("尚未配置隧道");
                                    ui.label("导入 JSON 配置，或手动创建第一条隧道。");
                                    ui.add_space(8.0);
                                    if ui.button("添加第一条隧道").clicked() {
                                        self.add_first_tunnel = true;
                                    }
                                });
                            });
                        ui.add_space(6.0);
                    }
                    if selection_changed {
                        self.persist_gui_state();
                    }
                    if edit_started && self.editing_snapshot.is_none() {
                        self.editing_snapshot = Some(frame_config.clone());
                    }
                    if edit_finished {
                        let previous = self
                            .editing_snapshot
                            .take()
                            .unwrap_or_else(|| frame_config.clone());
                        let selected_before = self.export_selected.clone();
                        if self.apply_config_change(
                            previous.clone(),
                            "tunnel configuration updated".to_string(),
                        ) {
                            for (old, new) in previous.tunnels.iter().zip(&self.config.tunnels) {
                                if old.name != new.name && selected_before.contains(&old.name) {
                                    self.export_selected.remove(&old.name);
                                    self.export_selected.insert(new.name.clone());
                                }
                            }
                            self.persist_gui_state();
                        } else {
                            self.tunnel_edits = tunnel_edits(&self.config);
                        }
                    }
                    if let Some((index, enabled)) = toggle {
                        self.set_tunnel_enabled(index, enabled);
                    }
                    if let Some(index) = remove {
                        let previous = self.config.clone();
                        let removed = self.config.tunnels.remove(index);
                        let server_still_exists = self
                            .config
                            .tunnels
                            .iter()
                            .any(|tunnel| tunnel.tows == removed.tows);
                        if self.running && !server_still_exists {
                            self.config = previous;
                            self.log(
                                "cannot delete the final tunnel of a connected tows group"
                                    .to_string(),
                            );
                        } else if self.apply_config_change(
                            previous,
                            format!("tunnel {} deleted", removed.name),
                        ) {
                            self.tunnel_edits.remove(index);
                            self.export_selected.remove(&removed.name);
                            self.tunnel_status.remove(&removed.name);
                            self.persist_gui_state();
                        }
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
                        if self.apply_config_change(previous, "tunnel added".to_string()) {
                            self.tunnel_edits.push(TunnelEdit::empty());
                        }
                    }

                    if let Some(bundle) = &self.pending_import {
                        let files_read = bundle.files_read;
                        let tunnel_count = bundle.tunnels.len();
                        let duplicate_names = import_conflicts(&self.config, bundle);
                        let mut import_action = None;
                        egui::Window::new("发现重复隧道")
                            .collapsible(false)
                            .resizable(false)
                            .show(context, |ui| {
                                ui.label(format!(
                                    "{} 个文件中的 {} 条隧道与现有配置重复：",
                                    files_read, tunnel_count
                                ));
                                ui.label(duplicate_names.join("、"));
                                ui.horizontal(|ui| {
                                    if ui.button("跳过重复项").clicked() {
                                        import_action = Some(Some(MergePolicy::SkipExisting));
                                    }
                                    if ui.button("覆盖重复项").clicked() {
                                        import_action = Some(Some(MergePolicy::OverwriteExisting));
                                    }
                                    if ui.button("整体替换").clicked() {
                                        import_action = Some(Some(MergePolicy::ReplaceAll));
                                    }
                                    if ui.button("取消").clicked() {
                                        import_action = Some(None);
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

                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button("导入隧道").clicked() {
                            self.choose_import_files();
                        }
                        if ui.button("导出隧道").clicked() {
                            self.export_selected();
                        }
                    });

                    ui.add_space(8.0);
                    ui.heading("日志输出");
                    egui::Frame::group(ui.style())
                        .inner_margin(12.0)
                        .corner_radius(10.0)
                        .show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            egui::ScrollArea::vertical()
                                .max_height(180.0)
                                .auto_shrink([false, false])
                                .stick_to_bottom(true)
                                .show(ui, |ui| {
                                    ui.set_min_width(ui.available_width());
                                    for line in &self.logs {
                                        ui.monospace(line);
                                    }
                                });
                        });
                });
        });
        if self.add_first_tunnel {
            let mut create = false;
            let mut cancel = false;
            egui::Window::new("添加第一条隧道")
                .collapsible(false)
                .resizable(false)
                .show(context, |ui| {
                    ui.label("tows 服务器");
                    ui.horizontal(|ui| {
                        ui.add_sized(
                            [220.0, 28.0],
                            egui::TextEdit::singleline(&mut self.new_server_host)
                                .hint_text("服务器 IP 或主机名"),
                        );
                        ui.label(":");
                        ui.add_sized(
                            [80.0, 28.0],
                            egui::TextEdit::singleline(&mut self.new_server_port).hint_text("4489"),
                        );
                    });
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("创建").clicked() {
                            create = true;
                        }
                        if ui.button("取消").clicked() {
                            cancel = true;
                        }
                    });
                });
            if cancel {
                self.add_first_tunnel = false;
            } else if create {
                let port = if self.new_server_port.trim().is_empty() {
                    "4489"
                } else {
                    self.new_server_port.trim()
                };
                let address = endpoint_edit_value(
                    &EndpointEdit {
                        host: self.new_server_host.clone(),
                        port: port.to_string(),
                    },
                    "",
                    "4489",
                );
                match parse_tows(&address) {
                    Ok(server) => {
                        let previous = self.config.clone();
                        self.config.tunnels.push(TunnelConfig {
                            name: "隧道 1".to_string(),
                            tows: server.to_string(),
                            target: "127.0.0.1:22".to_string(),
                            listen: "127.0.0.1:14489".to_string(),
                            enabled: false,
                        });
                        if self.apply_config_change(previous, "tunnel added".to_string()) {
                            self.tunnel_edits.push(TunnelEdit::empty());
                            self.add_first_tunnel = false;
                            self.new_server_host.clear();
                            self.new_server_port.clear();
                        }
                    }
                    Err(error) => self.log(format!("invalid tows address: {error:#}")),
                }
            }
        }
        context.request_repaint_after(Duration::from_millis(100));
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
    let height = ui.spacing().interact_size.y;
    let (rect, mut response) =
        ui.allocate_exact_size(egui::vec2(height * 1.8, height), egui::Sense::click());
    if response.clicked() {
        *value = !*value;
        response.mark_changed();
    }
    if ui.is_rect_visible(rect) {
        let amount = ui.ctx().animate_bool_responsive(response.id, *value);
        let visuals = ui.style().interact_selectable(&response, *value);
        let radius = rect.height() / 2.0;
        ui.painter().rect(
            rect,
            radius,
            visuals.bg_fill,
            visuals.bg_stroke,
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

fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    )
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

fn configured_servers(config: &GuiConfig) -> HashSet<String> {
    config
        .tunnels
        .iter()
        .filter_map(|tunnel| parse_tows(&tunnel.tows).ok())
        .map(|server| server.to_string())
        .collect()
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

fn status_dot(ui: &mut egui::Ui, healthy: bool) {
    let color = if healthy {
        egui::Color32::from_rgb(42, 190, 116)
    } else {
        egui::Color32::from_rgb(235, 174, 52)
    };
    let (rect, _) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
    ui.painter().circle_filled(rect.center(), 5.0, color);
}

fn status_indicator(ui: &mut egui::Ui, color: egui::Color32, label: &str, detail: &str) {
    ui.horizontal(|ui| {
        let (rect, _) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
        ui.painter().circle_filled(rect.center(), 5.0, color);
        ui.label(label);
    })
    .response
    .on_hover_text(detail);
}

fn metric_card(ui: &mut egui::Ui, label: &str, value: String) {
    egui::Frame::new()
        .fill(ui.visuals().faint_bg_color)
        .inner_margin(12.0)
        .corner_radius(8.0)
        .show(ui, |ui| {
            ui.set_min_width(150.0);
            ui.weak(label);
            ui.add_space(4.0);
            ui.label(egui::RichText::new(value).size(20.0).strong());
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
