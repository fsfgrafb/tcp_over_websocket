#[cfg(windows)]
use anyhow::bail;
use anyhow::{Context, Result};
use chrono::Local;
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub const APP_DIRECTORY: &str = "tcp_over_websocket";
pub const MAX_LOG_BYTES: u64 = 2 * 1024 * 1024;

static LOG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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

#[derive(Clone)]
pub struct BoundedLogWriter {
    path: PathBuf,
    line_start: Arc<Mutex<bool>>,
}

impl BoundedLogWriter {
    pub fn for_program(program: &str) -> Option<Self> {
        data_file(&format!("{program}.log")).map(|path| Self {
            path,
            line_start: Arc::new(Mutex::new(true)),
        })
    }

    fn append(&self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let _guard = LOG_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("log mutex poisoned");
        let mut line_start = self.line_start.lock().expect("log line mutex poisoned");
        let (bytes, next_line_start) = timestamp_lines(bytes, *line_start);
        let parent = self.path.parent().context("log path has no parent")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create data directory {}", parent.display()))?;

        let current_len = fs::metadata(&self.path).map(|meta| meta.len()).unwrap_or(0);
        if current_len.saturating_add(bytes.len() as u64) > MAX_LOG_BYTES {
            let existing = fs::read(&self.path).unwrap_or_default();
            let room = (MAX_LOG_BYTES as usize).saturating_sub(bytes.len());
            let start = existing.len().saturating_sub(room);
            let start = existing[start..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(existing.len(), |offset| start + offset + 1);
            atomic_write(&self.path, &existing[start..])?;
        }

        let bytes = if bytes.len() as u64 > MAX_LOG_BYTES {
            &bytes[bytes.len() - MAX_LOG_BYTES as usize..]
        } else {
            &bytes
        };
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("failed to open log file {}", self.path.display()))?
            .write_all(bytes)
            .context("failed to append program log")?;
        *line_start = next_line_start;
        Ok(())
    }
}

fn timestamp_lines(bytes: &[u8], mut line_start: bool) -> (Vec<u8>, bool) {
    let mut output = Vec::with_capacity(bytes.len().saturating_add(40));
    for &byte in bytes {
        if line_start {
            let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S %:z");
            output.extend_from_slice(format!("[{timestamp}] ").as_bytes());
            line_start = false;
        }
        output.push(byte);
        if byte == b'\n' {
            line_start = true;
        }
    }
    (output, line_start)
}

impl Write for BoundedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.append(bytes)
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_log_keeps_only_the_newest_bytes() {
        let path = std::env::temp_dir().join(format!(
            "tow-bounded-log-{}-{}.log",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let writer = BoundedLogWriter {
            path: path.clone(),
            line_start: Arc::new(Mutex::new(true)),
        };
        writer.append(&vec![b'a'; MAX_LOG_BYTES as usize]).unwrap();
        writer.append(b"\nnewest-line\n").unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.len() as u64 <= MAX_LOG_BYTES);
        assert!(contents.ends_with(b"newest-line\n"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn log_timestamp_is_added_once_per_line_across_fragmented_writes() {
        let (first, line_start) = timestamp_lines(b"first", true);
        let (second, line_start) = timestamp_lines(b" line\nsecond\n", line_start);
        assert!(line_start);

        let contents = String::from_utf8([first, second].concat()).unwrap();
        let lines = contents.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].ends_with(" first line"));
        assert!(lines[1].ends_with(" second"));
        assert_eq!(lines[0].matches("] ").count(), 1);
        assert_eq!(lines[1].matches("] ").count(), 1);
    }
}
