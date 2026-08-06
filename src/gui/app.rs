use anyhow::{Context, Result, bail};
use eframe::egui;
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::Duration;
use tokio::sync::watch;

use crate::address::{Endpoint, parse_listen, parse_target, parse_tows};
use crate::client::{
    AuthPrompt, ClientObserver, ForwardRule, LoginPreference, login_or_restore, run_tunnels,
};

use super::config::{
    GuiConfig, ImportBundle, MergePolicy, TunnelConfig, listen_conflicts, load_default_config,
    merge_import, read_import_paths, save_default_config, validate_config,
};

pub fn run() -> Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([920.0, 720.0])
            .with_min_inner_size([720.0, 520.0])
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
                _ => bail!("手机登录需要填写纯数字手机号"),
            },
            Self::Email => match LoginPreference::from_identity(identity) {
                Ok(LoginPreference::Email(value)) => Ok(LoginPreference::Email(value)),
                _ => bail!("邮箱登录需要填写有效邮箱"),
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
            .map_err(|_| anyhow::anyhow!("GUI 已关闭"))
    }

    fn request_code(&self, label: &str) -> Result<String> {
        let (reply, receiver) = std_mpsc::channel();
        self.events
            .send(WorkerEvent::CodeRequest {
                label: label.to_string(),
                reply,
            })
            .map_err(|_| anyhow::anyhow!("GUI 已关闭"))?;
        receiver
            .recv()
            .map_err(|_| anyhow::anyhow!("验证码输入已取消"))
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
    pending_code: Option<(String, std_mpsc::Sender<String>)>,
    code_input: String,
    qr_texture: Option<egui::TextureHandle>,
    pending_import: Option<ImportBundle>,
}

impl TowcApp {
    fn new(creation: &eframe::CreationContext<'_>) -> Self {
        install_chinese_font(&creation.egui_ctx);
        let loaded = load_default_config();
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
            pending_code: None,
            code_input: String::new(),
            qr_texture: None,
            pending_import: None,
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
        let (preference, server, rules) = match self.session_config() {
            Ok(session) => session,
            Err(error) => {
                self.log(format!("无法启动: {error:#}"));
                return;
            }
        };
        if let Err(error) = save_default_config(&self.config) {
            self.log(format!("保存配置失败，未启动: {error:#}"));
            return;
        }

        let (event_tx, event_rx) = std_mpsc::channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        self.events = Some(event_rx);
        self.stop = Some(stop_tx);
        self.running = true;
        self.status = "正在登录".to_string();
        self.qr_texture = None;
        self.tunnel_status.clear();

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
                    .context("无法创建 GUI 异步运行时")?
                    .block_on(async move {
                        let cookie = login_or_restore(auth, preference).await?;
                        run_tunnels(server, rules, cookie, stop_rx, observer).await
                    })
            });
            let _ = event_tx.send(WorkerEvent::Finished(
                result.map_err(|error: anyhow::Error| format!("{error:#}")),
            ));
        });
    }

    fn session_config(&self) -> Result<(LoginPreference, Endpoint, Vec<ForwardRule>)> {
        if self.save_blocked {
            bail!("原配置处于保护状态；请先确认使用当前界面配置");
        }
        validate_config(&self.config).context("配置校验失败")?;
        if !listen_conflicts(&self.config).is_empty() {
            bail!("存在启用隧道的监听端口冲突");
        }

        let preference = self.login_kind.preference(self.identity.trim())?;
        let server = parse_tows(&self.config.tows).context("tows 地址无效")?;
        let rules = self
            .config
            .tunnels
            .iter()
            .filter(|tunnel| tunnel.enabled)
            .map(|tunnel| {
                Ok(ForwardRule {
                    name: tunnel.name.clone(),
                    target: parse_target(&tunnel.target)?,
                    listen: parse_listen(&tunnel.listen)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if rules.is_empty() {
            bail!("至少需要启用一条隧道");
        }
        Ok((preference, server, rules))
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(true);
            self.status = "正在停止".to_string();
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
                    self.status = message.clone();
                    self.log(message);
                }
                WorkerEvent::Tunnel(name, message) => {
                    self.tunnel_status.insert(name.clone(), message.clone());
                    self.log(format!("[{name}] {message}"));
                }
                WorkerEvent::Qr(bytes) => match qr_texture(context, &bytes) {
                    Ok(texture) => self.qr_texture = Some(texture),
                    Err(error) => self.log(format!("二维码显示失败: {error:#}")),
                },
                WorkerEvent::CodeRequest { label, reply } => {
                    self.pending_code = Some((label, reply));
                    self.code_input.clear();
                }
                WorkerEvent::Log(message) => self.log(message),
                WorkerEvent::Finished(result) => {
                    self.running = false;
                    self.stop = None;
                    self.pending_code = None;
                    self.qr_texture = None;
                    match result {
                        Ok(()) => {
                            self.status = "已停止".to_string();
                            self.log("全部本地监听已停止".to_string());
                        }
                        Err(error) => {
                            self.status = "失败：请手动重新登录并启动".to_string();
                            for tunnel in &self.config.tunnels {
                                if tunnel.enabled {
                                    self.tunnel_status
                                        .insert(tunnel.name.clone(), "失败".to_string());
                                }
                            }
                            self.log(format!("连接失败，全部监听已停止: {error}"));
                        }
                    }
                }
            }
        }
    }

    fn log(&mut self, message: String) {
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
            merge_import(&mut self.config, bundle, policy);
            self.log(format!("已导入 {count} 条隧道（尚未覆盖来源文件）"));
        }
    }
}

impl eframe::App for TowcApp {
    fn update(&mut self, context: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_events(context);
        let dropped: Vec<_> = context.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .collect()
        });
        if !dropped.is_empty() && !self.running {
            let bundle = read_import_paths(&dropped);
            self.log(format!(
                "读取 {} 个配置文件，共 {} 条隧道",
                bundle.files_read,
                bundle.tunnels.len()
            ));
            self.pending_import = Some(bundle);
        }

        egui::TopBottomPanel::top("top").show(context, |ui| {
            ui.horizontal(|ui| {
                ui.heading(format!("tcp_over_websocket v{}", crate::APP_VERSION));
                ui.separator();
                ui.label(&self.status);
            });
            if let Some(warning) = &self.warning {
                ui.colored_label(egui::Color32::YELLOW, warning);
                if self.save_blocked && ui.button("确认使用当前界面配置并允许保存").clicked()
                {
                    self.save_blocked = false;
                    self.warning = None;
                    self.log("已解除原配置保护；来源文件仍未被自动覆盖".to_string());
                }
            }
        });

        egui::CentralPanel::default().show(context, |ui| {
            ui.add_enabled_ui(!self.running, |ui| {
                ui.horizontal(|ui| {
                    ui.label("tows 地址");
                    ui.text_edit_singleline(&mut self.config.tows);
                });
                ui.horizontal(|ui| {
                    ui.label("登录方式");
                    ui.selectable_value(&mut self.login_kind, LoginKind::Wechat, "微信扫码");
                    ui.selectable_value(&mut self.login_kind, LoginKind::Mobile, "手机验证码");
                    ui.selectable_value(&mut self.login_kind, LoginKind::Email, "邮箱验证码");
                    if self.login_kind != LoginKind::Wechat {
                        ui.text_edit_singleline(&mut self.identity);
                    }
                });
            });

            ui.separator();
            ui.heading("隧道");
            let conflicts = listen_conflicts(&self.config);
            let mut remove = None;
            egui::Grid::new("tunnels").striped(true).show(ui, |ui| {
                ui.label("启用");
                ui.label("名称");
                ui.label("目标");
                ui.label("本地监听");
                ui.label("状态");
                ui.end_row();
                for (index, tunnel) in self.config.tunnels.iter_mut().enumerate() {
                    ui.add_enabled(
                        !self.running,
                        egui::Checkbox::without_text(&mut tunnel.enabled),
                    );
                    ui.add_enabled(
                        !self.running,
                        egui::TextEdit::singleline(&mut tunnel.name).desired_width(110.0),
                    );
                    ui.add_enabled(
                        !self.running,
                        egui::TextEdit::singleline(&mut tunnel.target).desired_width(160.0),
                    );
                    ui.add_enabled(
                        !self.running,
                        egui::TextEdit::singleline(&mut tunnel.listen).desired_width(170.0),
                    );
                    if conflicts.contains(&tunnel.name) {
                        ui.colored_label(egui::Color32::RED, "监听冲突");
                    } else if parse_listen(&tunnel.listen).is_ok_and(|listen| !listen.is_loopback())
                    {
                        ui.colored_label(egui::Color32::YELLOW, "向局域网暴露");
                    } else {
                        ui.label(
                            self.tunnel_status
                                .get(&tunnel.name)
                                .map(String::as_str)
                                .unwrap_or("—"),
                        );
                    }
                    if ui
                        .add_enabled(!self.running, egui::Button::new("删除"))
                        .clicked()
                    {
                        remove = Some(index);
                    }
                    ui.end_row();
                }
            });
            if let Some(index) = remove {
                self.config.tunnels.remove(index);
            }
            if ui
                .add_enabled(!self.running, egui::Button::new("添加隧道"))
                .clicked()
            {
                let number = self.config.tunnels.len() + 1;
                self.config.tunnels.push(TunnelConfig {
                    name: format!("隧道 {number}"),
                    target: "127.0.0.1:22".to_string(),
                    listen: format!("127.0.0.1:{}", 14489_u32 + number as u32),
                    enabled: true,
                });
            }

            if let Some(bundle) = &self.pending_import {
                let files_read = bundle.files_read;
                let tunnel_count = bundle.tunnels.len();
                let mut import_action = None;
                ui.group(|ui| {
                    ui.label(format!(
                        "待导入：{} 个文件，{} 条隧道。遇到同名项时：",
                        files_read, tunnel_count
                    ));
                    ui.horizontal(|ui| {
                        if ui.button("跳过同名").clicked() {
                            import_action = Some(Some(MergePolicy::SkipExisting));
                        }
                        if ui.button("覆盖同名").clicked() {
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
                if ui
                    .add_enabled(!self.running, egui::Button::new("保存配置"))
                    .clicked()
                {
                    if self.save_blocked {
                        self.log("配置仍处于保护状态".to_string());
                    } else {
                        match save_default_config(&self.config) {
                            Ok(()) => self.log("配置已保存".to_string()),
                            Err(error) => self.log(format!("保存失败: {error:#}")),
                        }
                    }
                }
                if ui
                    .add_enabled(!self.running, egui::Button::new("登录并启动"))
                    .clicked()
                {
                    self.start();
                }
                if ui
                    .add_enabled(self.running, egui::Button::new("停止全部"))
                    .clicked()
                {
                    self.stop();
                }
            });

            if let Some(texture) = &self.qr_texture {
                ui.separator();
                ui.label("请用微信扫码并确认：");
                ui.image((texture.id(), egui::vec2(260.0, 260.0)));
            }
            if let Some((label, _)) = &self.pending_code {
                ui.separator();
                ui.label(format!("请输入{label}验证码（不会写入日志或配置）："));
                ui.horizontal(|ui| {
                    let response = ui.text_edit_singleline(&mut self.code_input);
                    let submit = ui.button("提交验证码").clicked()
                        || (response.lost_focus()
                            && ui.input(|input| input.key_pressed(egui::Key::Enter)));
                    if submit
                        && !self.code_input.trim().is_empty()
                        && let Some((_, reply)) = self.pending_code.take()
                    {
                        let _ = reply.send(self.code_input.trim().to_string());
                        self.code_input.clear();
                    }
                });
            }

            ui.separator();
            ui.heading("日志");
            egui::ScrollArea::vertical()
                .max_height(180.0)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for line in &self.logs {
                        ui.monospace(line);
                    }
                });
        });
        context.request_repaint_after(Duration::from_millis(100));
    }
}

fn qr_texture(context: &egui::Context, bytes: &[u8]) -> Result<egui::TextureHandle> {
    let image = image::load_from_memory(bytes)
        .context("无法解码二维码")?
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
