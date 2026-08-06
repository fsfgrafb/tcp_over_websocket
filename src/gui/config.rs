use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::address::{parse_listen, parse_target, parse_tows};
use crate::storage::{data_file, write_json};

pub const CONFIG_VERSION: u32 = 1;
pub const DEFAULT_TOWS: &str = "10.18.47.77:4489";
const CONFIG_FILE: &str = "config.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuiConfig {
    pub version: u32,
    #[serde(default = "default_tows")]
    pub tows: String,
    #[serde(default)]
    pub tunnels: Vec<TunnelConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunnelConfig {
    #[serde(default)]
    pub name: String,
    pub target: String,
    pub listen: String,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
}

#[derive(Debug)]
pub struct LoadedConfig {
    pub config: GuiConfig,
    /// 配置损坏或来自更高版本时，用户确认前禁止保存。
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
    pub tows: Option<String>,
    pub tunnels: Vec<TunnelConfig>,
    pub messages: Vec<String>,
    pub files_read: usize,
}

impl Default for GuiConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            tows: default_tows(),
            tunnels: vec![
                TunnelConfig {
                    name: "SSH".to_string(),
                    target: "127.0.0.1:22".to_string(),
                    listen: "127.0.0.1:14489".to_string(),
                    enabled: true,
                },
                TunnelConfig {
                    name: "Minecraft".to_string(),
                    target: "127.0.0.1:25565".to_string(),
                    listen: "127.0.0.1:25565".to_string(),
                    enabled: true,
                },
            ],
        }
    }
}

pub fn config_path() -> Option<PathBuf> {
    data_file(CONFIG_FILE)
}

pub fn load_default_config() -> LoadedConfig {
    let Some(path) = config_path() else {
        return LoadedConfig {
            config: GuiConfig::default(),
            save_blocked: true,
            warning: Some("找不到 APPDATA/LOCALAPPDATA，无法保存配置".to_string()),
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
            Err(error) => {
                let readable_newer = serde_json::from_slice::<GuiConfig>(&contents)
                    .ok()
                    .filter(|config| config.version > CONFIG_VERSION);
                LoadedConfig {
                    config: readable_newer.unwrap_or_default(),
                    save_blocked: true,
                    warning: Some(format!(
                        "配置 {} 无法写入，已按只读方式打开且不会覆盖原文件：{error:#}",
                        path.display()
                    )),
                }
            }
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => LoadedConfig {
            config: GuiConfig::default(),
            save_blocked: false,
            warning: None,
        },
        Err(error) => LoadedConfig {
            config: GuiConfig::default(),
            save_blocked: true,
            warning: Some(format!("读取 {} 失败：{error}", path.display())),
        },
    }
}

pub fn save_default_config(config: &GuiConfig) -> Result<()> {
    let path = config_path().context("找不到配置目录")?;
    validate_config(config)?;
    write_json(&path, config)
}

pub fn parse_config(contents: &[u8]) -> Result<GuiConfig> {
    let mut config: GuiConfig = serde_json::from_slice(contents).context("JSON 格式错误")?;
    if config.version > CONFIG_VERSION {
        bail!(
            "配置版本 {} 高于本程序支持的版本 {}，只能只读查看",
            config.version,
            CONFIG_VERSION
        );
    }
    if config.version != CONFIG_VERSION {
        bail!("不支持的配置版本 {}", config.version);
    }
    assign_missing_names(&mut config.tunnels);
    validate_config(&config)?;
    Ok(config)
}

pub fn validate_config(config: &GuiConfig) -> Result<()> {
    if config.version != CONFIG_VERSION {
        bail!("配置 version 必须为 {CONFIG_VERSION}");
    }
    parse_tows(&config.tows).context("无效 tows 地址")?;
    let mut names = HashSet::new();
    for (index, tunnel) in config.tunnels.iter().enumerate() {
        let name = tunnel.name.trim();
        if name.is_empty() {
            bail!("第 {} 条隧道名称不能为空", index + 1);
        }
        if !names.insert(name.to_string()) {
            bail!("隧道名称重复: {name}");
        }
        parse_target(&tunnel.target).with_context(|| format!("隧道 {name} 的 target 无效"))?;
        parse_listen(&tunnel.listen).with_context(|| format!("隧道 {name} 的 listen 无效"))?;
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
    let mut tows = None;
    let mut files_read = 0;
    for path in files {
        match fs::read(&path)
            .with_context(|| format!("无法读取 {}", path.display()))
            .and_then(|contents| parse_config(&contents))
        {
            Ok(config) => {
                files_read += 1;
                tows = Some(config.tows);
                tunnels.extend(config.tunnels);
            }
            Err(error) => messages.push(format!("跳过 {}：{error:#}", path.display())),
        }
    }
    assign_missing_names(&mut tunnels);
    ImportBundle {
        tows,
        tunnels,
        messages,
        files_read,
    }
}

pub fn merge_import(config: &mut GuiConfig, bundle: ImportBundle, policy: MergePolicy) {
    if policy == MergePolicy::ReplaceAll {
        config.tunnels = bundle.tunnels;
        if let Some(tows) = bundle.tows {
            config.tows = tows;
        }
        return;
    }

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

fn collect_json_files(path: &Path, files: &mut Vec<PathBuf>, messages: &mut Vec<String>) {
    if path.is_file() {
        if path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            files.push(path.to_path_buf());
        } else {
            messages.push(format!("跳过非 JSON 文件 {}", path.display()));
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
            Err(error) => messages.push(format!("无法读取目录 {}：{error}", path.display())),
        }
    } else {
        messages.push(format!("路径不存在：{}", path.display()));
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
        let hash = fnv1a(format!("{}\0{}", tunnel.target, tunnel.listen).as_bytes());
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

fn default_tows() -> String {
    DEFAULT_TOWS.to_string()
}

const fn enabled_by_default() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_names_are_deterministic_and_written_on_save() {
        let source = br#"{
            "version": 1,
            "tows": "example.test",
            "tunnels": [{"target":"22","listen":"14489"}]
        }"#;
        let first = parse_config(source).unwrap();
        let second = parse_config(source).unwrap();
        assert!(!first.tunnels[0].name.is_empty());
        assert_eq!(first.tunnels[0].name, second.tunnels[0].name);
    }

    #[test]
    fn merge_supports_skip_overwrite_and_replace() {
        let mut config = GuiConfig::default();
        let incoming = TunnelConfig {
            name: "SSH".to_string(),
            target: "127.0.0.1:2222".to_string(),
            listen: "127.0.0.1:12222".to_string(),
            enabled: true,
        };
        merge_import(
            &mut config,
            ImportBundle {
                tows: None,
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
                tows: None,
                tunnels: vec![incoming],
                messages: vec![],
                files_read: 1,
            },
            MergePolicy::OverwriteExisting,
        );
        assert_eq!(config.tunnels[0].target, "127.0.0.1:2222");
    }

    #[test]
    fn higher_version_is_rejected_without_rewriting() {
        assert!(parse_config(br#"{"version":2,"tunnels":[]}"#).is_err());
    }

    #[test]
    fn enabled_listen_conflicts_are_reported_by_name() {
        let mut config = GuiConfig::default();
        config.tunnels[1].listen = config.tunnels[0].listen.clone();
        let conflicts = listen_conflicts(&config);
        assert!(conflicts.contains("SSH"));
        assert!(conflicts.contains("Minecraft"));
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
