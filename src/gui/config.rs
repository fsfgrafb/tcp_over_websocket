use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::address::{parse_listen, parse_target, parse_tows};
use crate::protocol::MAX_TUNNELS;
use crate::storage::{data_file, write_json};

const CONFIG_FILE: &str = "config.json";
const GUI_STATE_FILE: &str = "gui-state.json";
pub const DEFAULT_COOKIE_REFRESH_SECS: u64 = 600;
pub const MIN_COOKIE_REFRESH_SECS: u64 = 60;
pub const MAX_COOKIE_REFRESH_SECS: u64 = 840;
pub const DEFAULT_WS_KEEPALIVE_SECS: u64 = 60;
pub const MIN_WS_KEEPALIVE_SECS: u64 = 10;
pub const MAX_WS_KEEPALIVE_SECS: u64 = 600;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuiConfig {
    #[serde(default)]
    pub connections: Vec<ConnectionConfig>,
    #[serde(default)]
    pub tunnels: Vec<TunnelConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    pub tows: String,
    #[serde(default = "default_ws_keepalive_secs")]
    pub ws_keepalive_secs: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemeSetting {
    #[default]
    System,
    Dark,
    Light,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GuiState {
    pub theme: ThemeSetting,
    #[serde(default, skip_serializing)]
    pub selected_tunnels: HashSet<String>,
    #[serde(default = "default_cookie_refresh_secs")]
    pub cookie_refresh_secs: u64,
}

impl Default for GuiState {
    fn default() -> Self {
        Self {
            theme: ThemeSetting::System,
            selected_tunnels: HashSet::new(),
            cookie_refresh_secs: DEFAULT_COOKIE_REFRESH_SECS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TunnelConfig {
    #[serde(default)]
    pub name: String,
    pub tows: String,
    pub target: String,
    pub listen: String,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
}

#[derive(Debug)]
pub struct LoadedConfig {
    pub config: GuiConfig,
    pub save_blocked: bool,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergePolicy {
    SkipExisting,
    OverwriteExisting,
}

#[derive(Debug)]
pub struct ImportBundle {
    pub connections: Vec<ConnectionConfig>,
    pub tunnels: Vec<TunnelConfig>,
    pub messages: Vec<String>,
    pub files_read: usize,
}

pub fn config_path() -> Option<PathBuf> {
    data_file(CONFIG_FILE)
}

pub fn load_gui_state() -> GuiState {
    data_file(GUI_STATE_FILE)
        .map(|path| load_gui_state_at(&path))
        .unwrap_or_default()
}

fn load_gui_state_at(path: &Path) -> GuiState {
    let mut state: GuiState = fs::read(path)
        .ok()
        .and_then(|contents| serde_json::from_slice(&contents).ok())
        .unwrap_or_default();
    state.cookie_refresh_secs = state
        .cookie_refresh_secs
        .clamp(MIN_COOKIE_REFRESH_SECS, MAX_COOKIE_REFRESH_SECS);
    state.selected_tunnels.clear();
    state
}

pub fn save_gui_state(state: &GuiState) -> Result<()> {
    let path = data_file(GUI_STATE_FILE).context("cannot locate GUI state directory")?;
    save_gui_state_at(&path, state)
}

fn save_gui_state_at(path: &Path, state: &GuiState) -> Result<()> {
    if !(MIN_COOKIE_REFRESH_SECS..=MAX_COOKIE_REFRESH_SECS).contains(&state.cookie_refresh_secs) {
        bail!(
            "cookie refresh interval must be between {MIN_COOKIE_REFRESH_SECS} and {MAX_COOKIE_REFRESH_SECS} seconds"
        );
    }
    write_json(path, state)
}

pub fn load_default_config() -> LoadedConfig {
    let Some(path) = config_path() else {
        return LoadedConfig {
            config: GuiConfig::default(),
            save_blocked: true,
            warning: Some(
                "APPDATA/LOCALAPPDATA is unavailable; configuration cannot be saved".to_string(),
            ),
        };
    };
    load_config_at(&path)
}

fn load_config_at(path: &Path) -> LoadedConfig {
    match fs::read(path) {
        Ok(contents) => match parse_config(&contents) {
            Ok(config) => LoadedConfig {
                config,
                save_blocked: false,
                warning: None,
            },
            Err(error) => LoadedConfig {
                config: GuiConfig::default(),
                save_blocked: true,
                warning: Some(format!(
                    "cannot read configuration {}; defaults loaded without overwriting it: {error:#}",
                    path.display()
                )),
            },
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => LoadedConfig {
            config: GuiConfig::default(),
            save_blocked: false,
            warning: None,
        },
        Err(error) => LoadedConfig {
            config: GuiConfig::default(),
            save_blocked: true,
            warning: Some(format!("failed to read {}: {error}", path.display())),
        },
    }
}

pub fn save_default_config(config: &GuiConfig) -> Result<()> {
    let path = config_path().context("cannot locate configuration directory")?;
    validate_config(config)?;
    write_json(&path, config)
}

pub fn export_tunnels(
    path: &Path,
    connections: Vec<ConnectionConfig>,
    tunnels: Vec<TunnelConfig>,
) -> Result<()> {
    if tunnels.is_empty() {
        bail!("select at least one tunnel to export");
    }
    let config = GuiConfig {
        connections,
        tunnels,
    };
    validate_config(&config)?;
    write_json(path, &config)
}

pub fn parse_config(contents: &[u8]) -> Result<GuiConfig> {
    let mut config: GuiConfig = serde_json::from_slice(contents).context("invalid JSON")?;
    assign_missing_names(&mut config.tunnels);
    add_missing_connections(&mut config);
    validate_config(&config)?;
    Ok(config)
}

pub fn validate_config(config: &GuiConfig) -> Result<()> {
    let mut names = HashSet::new();
    let mut group_sizes = HashMap::<String, usize>::new();
    let mut servers = HashSet::new();
    for connection in &config.connections {
        let server =
            parse_tows(&connection.tows).context("connection has an invalid tows address")?;
        if !servers.insert(server.to_string()) {
            bail!("duplicate tows connection: {server}");
        }
        if !(MIN_WS_KEEPALIVE_SECS..=MAX_WS_KEEPALIVE_SECS).contains(&connection.ws_keepalive_secs)
        {
            bail!(
                "tows {server} keepalive interval must be between {MIN_WS_KEEPALIVE_SECS} and {MAX_WS_KEEPALIVE_SECS} seconds"
            );
        }
    }
    for (index, tunnel) in config.tunnels.iter().enumerate() {
        let name = tunnel.name.trim();
        if name.is_empty() {
            bail!("tunnel {} must have a name", index + 1);
        }
        if !names.insert(name.to_string()) {
            bail!("duplicate tunnel name: {name}");
        }
        let server = parse_tows(&tunnel.tows)
            .with_context(|| format!("tunnel {name} has an invalid tows address"))?;
        if !servers.contains(&server.to_string()) {
            bail!("tunnel {name} references an unknown tows connection: {server}");
        }
        parse_target(&tunnel.target)
            .with_context(|| format!("tunnel {name} has an invalid target"))?;
        parse_listen(&tunnel.listen)
            .with_context(|| format!("tunnel {name} has an invalid listen address"))?;
        if tunnel.enabled {
            *group_sizes.entry(server.to_string()).or_default() += 1;
        }
    }
    if let Some((server, count)) = group_sizes
        .into_iter()
        .find(|(_, count)| *count > MAX_TUNNELS)
    {
        bail!("tows {server} has {count} enabled tunnels; maximum is {MAX_TUNNELS}");
    }
    Ok(())
}

fn add_missing_connections(config: &mut GuiConfig) {
    let mut known = config
        .connections
        .iter()
        .filter_map(|connection| parse_tows(&connection.tows).ok())
        .map(|server| server.to_string())
        .collect::<HashSet<_>>();
    for tunnel in &config.tunnels {
        let Ok(server) = parse_tows(&tunnel.tows) else {
            continue;
        };
        if known.insert(server.to_string()) {
            config.connections.push(ConnectionConfig {
                tows: server.to_string(),
                ws_keepalive_secs: DEFAULT_WS_KEEPALIVE_SECS,
            });
        }
    }
}

pub fn listen_conflicts(config: &GuiConfig) -> HashSet<String> {
    let mut by_listen: HashMap<String, Vec<String>> = HashMap::new();
    for tunnel in config.tunnels.iter().filter(|tunnel| tunnel.enabled) {
        if let Ok(listen) = parse_listen(&tunnel.listen) {
            by_listen
                .entry(listen.to_string())
                .or_default()
                .push(tunnel.name.clone());
        }
    }
    by_listen
        .into_values()
        .filter(|names| names.len() > 1)
        .flatten()
        .collect()
}

pub fn read_import_paths(paths: &[PathBuf]) -> ImportBundle {
    let mut files = Vec::new();
    let mut messages = Vec::new();
    for path in paths {
        collect_json_files(path, &mut files, &mut messages);
    }
    files.sort();
    files.dedup();

    let mut connections = Vec::new();
    let mut tunnels = Vec::new();
    let mut files_read = 0;
    for path in files {
        match fs::read(&path)
            .with_context(|| format!("cannot read {}", path.display()))
            .and_then(|contents| parse_config(&contents))
        {
            Ok(config) => {
                files_read += 1;
                connections.extend(config.connections);
                tunnels.extend(config.tunnels);
            }
            Err(error) => messages.push(format!("skipped {}: {error:#}", path.display())),
        }
    }
    assign_missing_names(&mut tunnels);
    ImportBundle {
        connections,
        tunnels,
        messages,
        files_read,
    }
}

pub fn merge_import(config: &mut GuiConfig, bundle: ImportBundle, policy: MergePolicy) {
    for incoming in bundle.connections {
        let normalized = parse_tows(&incoming.tows)
            .map(|server| server.to_string())
            .unwrap_or_else(|_| incoming.tows.clone());
        if let Some(index) = config.connections.iter().position(|existing| {
            parse_tows(&existing.tows).is_ok_and(|server| server.to_string() == normalized)
        }) {
            if policy == MergePolicy::OverwriteExisting {
                config.connections[index] = incoming;
            }
        } else {
            config.connections.push(incoming);
        }
    }

    for incoming in bundle.tunnels {
        if let Some(index) = config
            .tunnels
            .iter()
            .position(|existing| existing.name.trim() == incoming.name.trim())
        {
            if policy == MergePolicy::OverwriteExisting {
                config.tunnels[index] = incoming;
            }
        } else {
            config.tunnels.push(incoming);
        }
    }
}

pub fn import_conflicts(config: &GuiConfig, bundle: &ImportBundle) -> Vec<String> {
    let existing = config
        .tunnels
        .iter()
        .map(|tunnel| tunnel.name.trim())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut conflicts = Vec::new();
    for tunnel in &bundle.tunnels {
        let name = tunnel.name.trim();
        if !seen.insert(name) || existing.contains(name) {
            conflicts.push(name.to_string());
        }
    }
    conflicts.sort();
    conflicts.dedup();
    conflicts
}

fn collect_json_files(path: &Path, files: &mut Vec<PathBuf>, messages: &mut Vec<String>) {
    if path.is_file() {
        if path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            files.push(path.to_path_buf());
        } else {
            messages.push(format!("skipped non-JSON file {}", path.display()));
        }
        return;
    }
    if path.is_dir() {
        match fs::read_dir(path) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    collect_json_files(&entry.path(), files, messages);
                }
            }
            Err(error) => {
                messages.push(format!("cannot read directory {}: {error}", path.display()))
            }
        }
    } else {
        messages.push(format!("path does not exist: {}", path.display()));
    }
}

fn assign_missing_names(tunnels: &mut [TunnelConfig]) {
    let mut used: HashSet<String> = tunnels
        .iter()
        .filter(|tunnel| !tunnel.name.trim().is_empty())
        .map(|tunnel| tunnel.name.trim().to_string())
        .collect();
    for tunnel in tunnels {
        tunnel.name = tunnel.name.trim().to_string();
        if !tunnel.name.is_empty() {
            continue;
        }
        let hash =
            fnv1a(format!("{}\0{}\0{}", tunnel.tows, tunnel.target, tunnel.listen).as_bytes());
        let base = format!("隧道-{hash:08x}");
        let mut candidate = base.clone();
        let mut suffix = 2;
        while used.contains(&candidate) {
            candidate = format!("{base}-{suffix}");
            suffix += 1;
        }
        used.insert(candidate.clone());
        tunnel.name = candidate;
    }
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

const fn enabled_by_default() -> bool {
    true
}

const fn default_cookie_refresh_secs() -> u64 {
    DEFAULT_COOKIE_REFRESH_SECS
}

const fn default_ws_keepalive_secs() -> u64 {
    DEFAULT_WS_KEEPALIVE_SECS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> GuiConfig {
        GuiConfig {
            connections: vec![ConnectionConfig {
                tows: "10.18.47.77:4489".to_string(),
                ws_keepalive_secs: DEFAULT_WS_KEEPALIVE_SECS,
            }],
            tunnels: vec![
                TunnelConfig {
                    name: "77 SSH".to_string(),
                    tows: "10.18.47.77:4489".to_string(),
                    target: "127.0.0.1:22".to_string(),
                    listen: "127.0.0.1:14489".to_string(),
                    enabled: true,
                },
                TunnelConfig {
                    name: "77 Minecraft".to_string(),
                    tows: "10.18.47.77:4489".to_string(),
                    target: "127.0.0.1:25565".to_string(),
                    listen: "127.0.0.1:25565".to_string(),
                    enabled: true,
                },
            ],
        }
    }

    #[test]
    fn defaults_do_not_create_tunnels() {
        let config = GuiConfig::default();
        assert!(config.connections.is_empty());
        assert!(config.tunnels.is_empty());
    }

    #[test]
    fn gui_state_persists_settings_but_not_tunnel_selection() {
        let state = GuiState {
            theme: ThemeSetting::Light,
            selected_tunnels: HashSet::from(["77 SSH".to_string(), "66 SSH".to_string()]),
            cookie_refresh_secs: DEFAULT_COOKIE_REFRESH_SECS,
        };
        let path = std::env::temp_dir().join(format!(
            "towc-gui-state-{}-{}.json",
            std::process::id(),
            fnv1a(b"gui-state-persistence")
        ));
        save_gui_state_at(&path, &state).unwrap();
        let stored = fs::read_to_string(&path).unwrap();
        assert!(!stored.contains("selected_tunnels"));
        let loaded = load_gui_state_at(&path);
        assert_eq!(loaded.theme, ThemeSetting::Light);
        assert_eq!(loaded.cookie_refresh_secs, DEFAULT_COOKIE_REFRESH_SECS);
        assert!(loaded.selected_tunnels.is_empty());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn missing_names_are_deterministic_and_written_on_save() {
        let source = br#"{
            "tunnels": [{"tows":"example.test","target":"22","listen":"14489"}]
        }"#;
        let first = parse_config(source).unwrap();
        let second = parse_config(source).unwrap();
        assert!(!first.tunnels[0].name.is_empty());
        assert_eq!(first.tunnels[0].name, second.tunnels[0].name);
        assert_eq!(first.connections.len(), 1);
        assert_eq!(
            first.connections[0].ws_keepalive_secs,
            DEFAULT_WS_KEEPALIVE_SECS
        );
    }

    #[test]
    fn merge_supports_skip_and_overwrite() {
        let mut config = sample_config();
        let incoming = TunnelConfig {
            name: "77 SSH".to_string(),
            tows: "10.18.47.77:4489".to_string(),
            target: "127.0.0.1:2222".to_string(),
            listen: "127.0.0.1:12222".to_string(),
            enabled: true,
        };
        merge_import(
            &mut config,
            ImportBundle {
                connections: vec![],
                tunnels: vec![incoming.clone()],
                messages: vec![],
                files_read: 1,
            },
            MergePolicy::SkipExisting,
        );
        assert_eq!(config.tunnels[0].target, "127.0.0.1:22");
        merge_import(
            &mut config,
            ImportBundle {
                connections: vec![],
                tunnels: vec![incoming],
                messages: vec![],
                files_read: 1,
            },
            MergePolicy::OverwriteExisting,
        );
        assert_eq!(config.tunnels[0].target, "127.0.0.1:2222");
    }

    #[test]
    fn import_reports_existing_and_cross_file_duplicates() {
        let config = sample_config();
        let duplicate = config.tunnels[0].clone();
        let bundle = ImportBundle {
            connections: vec![],
            tunnels: vec![duplicate.clone(), duplicate],
            messages: vec![],
            files_read: 2,
        };
        assert_eq!(import_conflicts(&config, &bundle), vec!["77 SSH"]);
    }

    #[test]
    fn import_conflict_count_is_not_the_total_tunnel_count() {
        let mut config = sample_config();
        config.tunnels[0].name = " SSH ".to_string();
        let mut ssh = config.tunnels[0].clone();
        ssh.name = "SSH".to_string();
        let mut first_new = ssh.clone();
        first_new.name = "77 SSH".to_string();
        let mut second_new = ssh.clone();
        second_new.name = "隧道 4".to_string();
        let bundle = ImportBundle {
            connections: vec![],
            tunnels: vec![first_new, ssh, second_new],
            messages: vec![],
            files_read: 1,
        };

        assert_eq!(bundle.tunnels.len(), 3);
        assert_eq!(import_conflicts(&config, &bundle), vec!["SSH"]);
    }

    #[test]
    fn export_writes_selected_tunnels_to_one_config() {
        let path = std::env::temp_dir().join(format!(
            "tow-export-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = sample_config();
        export_tunnels(&path, config.connections, config.tunnels).unwrap();
        let exported = parse_config(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(exported.tunnels.len(), 2);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(parse_config(br#"{"unexpected":true,"tunnels":[]}"#).is_err());
    }

    #[test]
    fn enabled_listen_conflicts_are_reported_by_name() {
        let mut config = sample_config();
        config.tunnels[1].listen = config.tunnels[0].listen.clone();
        let conflicts = listen_conflicts(&config);
        assert!(conflicts.contains("77 SSH"));
        assert!(conflicts.contains("77 Minecraft"));
    }

    #[test]
    fn damaged_config_is_never_overwritten_while_loading() {
        let path = std::env::temp_dir().join(format!(
            "tow-config-damaged-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let damaged = b"{ this is not json";
        fs::write(&path, damaged).unwrap();
        let loaded = load_config_at(&path);
        assert!(loaded.save_blocked);
        assert_eq!(fs::read(&path).unwrap(), damaged);
        fs::remove_file(path).unwrap();
    }
}
