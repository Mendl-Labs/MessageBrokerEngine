//! Ultra-Logger stub for MessageBrokerEngine
//!
//! This is a lightweight stub that provides the UltraLogger API used across
//! the workspace. It routes log messages to stderr for local development and
//! testing, while the full implementation (in the LoggingEngine repository)
//! ships structured logs to Elasticsearch.

use std::collections::HashMap;

/// Configuration for the transport layer (e.g., Elasticsearch endpoint).
#[derive(Debug, Clone, Default)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub options: HashMap<String, String>,
}

/// Transport configuration.
#[derive(Debug, Clone, Default)]
pub struct TransportConfig {
    pub transport_type: String,
    pub connection: ConnectionConfig,
}

/// Top-level logger configuration.
#[derive(Debug, Clone, Default)]
pub struct LoggerConfig {
    pub level: String,
    pub transport: TransportConfig,
}

/// High-performance structured logger.
///
/// In this stub all log output goes to `eprintln!`. The production
/// implementation (in LoggingEngine) ships logs asynchronously to
/// Elasticsearch or other configured transports.
#[derive(Debug, Clone)]
pub struct UltraLogger {
    name: String,
}

impl UltraLogger {
    /// Create a logger with the given component name and default configuration.
    pub fn new(name: String) -> Self {
        Self { name }
    }

    /// Create a logger with an explicit name and transport configuration.
    pub fn with_config(name: String, _config: LoggerConfig) -> Self {
        Self { name }
    }

    // ── Async variants ──────────────────────────────────────────────────────

    pub async fn info(&self, msg: String) -> Result<(), ()> {
        eprintln!("[INFO]  [{}] {}", self.name, msg);
        Ok(())
    }

    pub async fn warn(&self, msg: String) -> Result<(), ()> {
        eprintln!("[WARN]  [{}] {}", self.name, msg);
        Ok(())
    }

    pub async fn error(&self, msg: String) -> Result<(), ()> {
        eprintln!("[ERROR] [{}] {}", self.name, msg);
        Ok(())
    }

    pub async fn debug(&self, msg: String) -> Result<(), ()> {
        eprintln!("[DEBUG] [{}] {}", self.name, msg);
        Ok(())
    }

    // ── Synchronous variants ────────────────────────────────────────────────

    pub fn info_sync(&self, msg: String) {
        eprintln!("[INFO]  [{}] {}", self.name, msg);
    }

    pub fn warn_sync(&self, msg: String) {
        eprintln!("[WARN]  [{}] {}", self.name, msg);
    }

    pub fn error_sync(&self, msg: String) {
        eprintln!("[ERROR] [{}] {}", self.name, msg);
    }

    pub fn debug_sync(&self, msg: String) {
        eprintln!("[DEBUG] [{}] {}", self.name, msg);
    }
}
