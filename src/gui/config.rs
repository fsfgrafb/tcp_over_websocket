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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuiConfig {
    #[serde(default)]
    pub tunnels: Vec<TunnelConfig>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemeSetting {
    #[default]
    System,
    Dark,
    Light,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GuiState {
    pub theme: ThemeSetting,
    pub selected_tunnels: HashSet<String>,
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
    ReplaceAll,
}

#[derive(Debug)]
pub struct ImportBundle {
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
    fs::read(path)
        .ok()
        .and_then(|contents| serde_json::from_slice(&contents).ok())
        .unwrap_or_default()
}

pub fn save_gui_state(state: &GuiState) -> Result<()> {
    let path = data_file(GUI_STATE_FILE).context("cannot locate GUI state directory")?;
    save_gui_state_at(&path, state)
}

fn save_gui_state_at(path: &Path, state: &GuiState) -> Result<()> {
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

pub fn export_tunnels(path: &Path, tunnels: Vec<TunnelConfig>) -> Result<()> {
    if tunnels.is_empty() {
        bail!("select at least one tunnel to export");
    }
    let config = GuiConfig { tunnels };
    validate_config(&config)?;
    write_json(path, &config)
}

pub fn parse_config(contents: &[u8]) -> Result<GuiConfig> {
    let mut config: GuiConfig = serde_json::from_slice(contents).context("invalid JSON")?;
    assign_missing_names(&mut config.tunnels);
    validate_config(&config)?;
    Ok(config)
}

pub fn validate_config(config: &GuiConfig) -> Result<()> {
    let mut names = HashSet::new();
    let mut group_sizes = HashMap::<String, usize>::new();
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

    let mut tunnels = Vec::new();
    let mut files_read = 0;
    for path in files {
        match fs::read(&path)
            .with_context(|| format!("cannot read {}", path.display()))
            .and_then(|contents| parse_config(&contents))
        {
            Ok(config) => {
                files_read += 1;
                tunnels.extend(config.tunnels);
            }
            Err(error) => messages.push(format!("skipped {}: {error:#}", path.display())),
        }
    }
    assign_missing_names(&mut tunnels);
    ImportBundle {
        tunnels,
        messages,
        files_read,
    }
}

pub fn merge_import(config: &mut GuiConfig, bundle: ImportBundle, policy: MergePolicy) {
    let policy = if policy == MergePolicy::ReplaceAll {
        config.tunnels.clear();
        MergePolicy::OverwriteExisting
    } else {
        policy
    };

    for incoming in bundle.tunnels {
        if let Some(index) = config
            .tunnels
            .iter()
            .position(|existing| existing.name == incoming.name)
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
        .map(|tunnel| tunnel.name.as_str())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut conflicts = Vec::new();
    for tunnel in &bundle.tunnels {
        if !seen.insert(tunnel.name.as_str()) || existing.contains(tunnel.name.as_str()) {
            conflicts.push(tunnel.name.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> GuiConfig {
        GuiConfig {
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
        assert!(config.tunnels.is_empty());
    }

    #[test]
    fn gui_state_persists_theme_and_selection() {
        let state = GuiState {
            theme: ThemeSetting::Light,
            selected_tunnels: HashSet::from(["77 SSH".to_string(), "66 SSH".to_string()]),
        };
        let path = std::env::temp_dir().join(format!(
            "towc-gui-state-{}-{}.json",
            std::process::id(),
            fnv1a(b"gui-state-persistence")
        ));
        save_gui_state_at(&path, &state).unwrap();
        assert_eq!(load_gui_state_at(&path), state);
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
    }

    #[test]
    fn merge_supports_skip_overwrite_and_replace() {
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
                tunnels: vec![incoming],
                messages: vec![],
                files_read: 1,
            },
            MergePolicy::OverwriteExisting,
        );
        assert_eq!(config.tunnels[0].target, "127.0.0.1:2222");
        merge_import(
            &mut config,
            ImportBundle {
                tunnels: vec![TunnelConfig {
                    name: "only".to_string(),
                    tows: "10.18.47.66:4489".to_string(),
                    target: "127.0.0.1:22".to_string(),
                    listen: "127.0.0.1:15555".to_string(),
                    enabled: true,
                }],
                messages: vec![],
                files_read: 1,
            },
            MergePolicy::ReplaceAll,
        );
        assert_eq!(config.tunnels.len(), 1);
        assert_eq!(config.tunnels[0].name, "only");
    }

    #[test]
    fn import_reports_existing_and_cross_file_duplicates() {
        let config = sample_config();
        let duplicate = config.tunnels[0].clone();
        let bundle = ImportBundle {
            tunnels: vec![duplicate.clone(), duplicate],
            messages: vec![],
            files_read: 2,
        };
        assert_eq!(import_conflicts(&config, &bundle), vec!["77 SSH"]);
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
        let selected = sample_config().tunnels;
        export_tunnels(&path, selected).unwrap();
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
