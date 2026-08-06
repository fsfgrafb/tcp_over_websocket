#[cfg(windows)]
use anyhow::bail;
use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const APP_DIRECTORY: &str = "tcp_over_websocket";

/// 返回配置与认证缓存共用的数据目录。
pub fn data_dir() -> Option<PathBuf> {
    platform_cache_root().map(|root| root.join(APP_DIRECTORY))
}

#[cfg(windows)]
fn platform_cache_root() -> Option<PathBuf> {
    std::env::var_os("APPDATA")
        .or_else(|| std::env::var_os("LOCALAPPDATA"))
        .map(PathBuf::from)
}

#[cfg(not(windows))]
fn platform_cache_root() -> Option<PathBuf> {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
}

pub fn data_file(name: &str) -> Option<PathBuf> {
    data_dir().map(|directory| directory.join(name))
}

/// 同目录写临时文件后原子替换，避免中途退出留下半份 JSON。
pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("destination file has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create data directory {}", parent.display()))?;

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("destination file name is not valid Unicode")?;
    let temporary = parent.join(format!(".{file_name}.{nonce}.tmp"));

    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| format!("failed to create temporary file {}", temporary.display()))?;
        file.write_all(contents)
            .context("failed to write temporary file")?;
        file.sync_all().context("failed to sync temporary file")?;
        replace_file(&temporary, path)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(not(windows))]
fn replace_file(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination).with_context(|| {
        format!(
            "failed to atomically replace {} -> {}",
            source.display(),
            destination.display()
        )
    })
}

#[cfg(windows)]
fn replace_file(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let destination_display = destination.display().to_string();
    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    // SAFETY: 两个缓冲区均为以 NUL 结尾、在调用期间有效的 UTF-16 路径。
    let succeeded = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if succeeded == 0 {
        bail!(
            "failed to atomically replace {destination_display}: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut json = serde_json::to_vec_pretty(value).context("failed to serialize JSON")?;
    json.push(b'\n');
    atomic_write(path, &json)
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let contents = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&contents).with_context(|| format!("failed to parse {}", path.display()))
}
