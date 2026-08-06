//! tcp_over_websocket 的共享实现。
//!
//! 项目刻意把地址、协议、网络调度、登录和界面拆开，便于学习与测试。

pub mod address;
pub mod protocol;
pub mod storage;

mod multiplex;
pub mod network;

#[cfg(feature = "client")]
pub mod client;
#[cfg(all(feature = "gui", windows))]
pub mod gui;
#[cfg(feature = "server")]
pub mod server;

pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

struct ConsoleAndLogWriter {
    console: std::io::Stdout,
    log: Option<storage::BoundedLogWriter>,
}

impl std::io::Write for ConsoleAndLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.console.write_all(bytes)?;
        if let Some(log) = &mut self.log {
            log.write_all(bytes)?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.console.flush()?;
        if let Some(log) = &mut self.log {
            log.flush()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct TaggedEventFormatter {
    default_tag: &'static str,
}

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for TaggedEventFormatter
where
    S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
    N: for<'writer> tracing_subscriber::fmt::FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        context: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let tag = match event.metadata().target() {
            "towc" => "towc",
            "tows" => "tows",
            "tunnel" => "tunnel",
            _ => self.default_tag,
        };
        if writer.has_ansi_escapes() {
            let color = match tag {
                "towc" => 36,
                "tunnel" => 32,
                "tows" => 35,
                _ => 37,
            };
            write!(writer, "\x1b[{color}m[{tag}]\x1b[0m ")?;
        } else {
            write!(writer, "[{tag}] ")?;
        }
        context
            .field_format()
            .format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// Initialize CLI logging. Calling this more than once is harmless.
pub fn init_tracing(default_tag: &'static str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let log = storage::BoundedLogWriter::for_program(default_tag);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(true)
        .event_format(TaggedEventFormatter { default_tag })
        .with_writer(move || ConsoleAndLogWriter {
            console: std::io::stdout(),
            log: log.clone(),
        })
        .try_init();
}
