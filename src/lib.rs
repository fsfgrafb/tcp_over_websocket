#![warn(missing_docs)]

//! TCP-over-WebSocket client and server components for SZUT WebVPN.
//!
//! Enable the client feature for local tunnel management or server for the
//! remote forwarding server. Both are enabled by default.

mod network;

/// Reusable local client, login, and tunnel-management APIs.
#[cfg(feature = "client")]
pub mod client;
/// Reusable remote TCP forwarding server APIs.
#[cfg(feature = "server")]
pub mod server;

pub(crate) use network::*;
