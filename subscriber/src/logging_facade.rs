//! Ultra-Logger integration for MessageBrokerEngine Subscriber
//! 
//! Provides consistent logging across the platform using ultra-logger
//! instead of tracing for unified observability.

use ultra_logger::{UltraLogger, LoggerConfig, TransportConfig, ConnectionConfig};
use std::sync::Arc;
use std::collections::HashMap;

/// Create Elasticsearch configuration for MessageBrokerEngine
fn create_elasticsearch_config(component: &str) -> LoggerConfig {
    let use_elasticsearch = std::env::var("USE_ELASTICSEARCH_LOGGING")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(true); // Default to true for production

    if use_elasticsearch {
        let endpoint = std::env::var("ELASTICSEARCH_ENDPOINT")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_ENDPOINT"))
            .unwrap_or_default();
        let username = std::env::var("ELASTICSEARCH_USERNAME")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_USERNAME"))
            .unwrap_or_else(|_| "elastic".to_string());
        let password = std::env::var("ELASTICSEARCH_PASSWORD")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_PASSWORD"))
            .unwrap_or_default();

        // Only configure elasticsearch if endpoint is set
        if endpoint.is_empty() {
            return LoggerConfig::default();
        }

        let mut options = HashMap::new();
        options.insert("index".to_string(), format!("messagebroker-{}-logs", component));

        LoggerConfig {
            level: std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            transport: TransportConfig {
                transport_type: "elasticsearch".to_string(),
                connection: ConnectionConfig {
                    host: endpoint,
                    port: 443,
                    username: Some(username),
                    password: Some(password),
                    options,
                },
            },
        }
    } else {
        LoggerConfig::default()
    }
}

/// Main subscriber logger instance
pub static SUBSCRIBER_LOGGER: once_cell::sync::Lazy<Arc<UltraLogger>> = 
    once_cell::sync::Lazy::new(|| {
        Arc::new(UltraLogger::with_config(
            "MessageBrokerEngine-subscriber".to_string(),
            create_elasticsearch_config("subscriber")
        ))
    });

/// Topic management logger
pub static TOPIC_LOGGER: once_cell::sync::Lazy<Arc<UltraLogger>> = 
    once_cell::sync::Lazy::new(|| {
        Arc::new(UltraLogger::with_config(
            "MessageBrokerEngine-topic".to_string(),
            create_elasticsearch_config("topic")
        ))
    });

/// Message processing logger
pub static MESSAGE_LOGGER: once_cell::sync::Lazy<Arc<UltraLogger>> = 
    once_cell::sync::Lazy::new(|| {
        Arc::new(UltraLogger::with_config(
            "MessageBrokerEngine-message".to_string(),
            create_elasticsearch_config("message")
        ))
    });

/// Performance-optimized logging macros for Subscriber
/// Note: These are sync-safe versions since subscriber may not always have tokio runtime

#[macro_export]
macro_rules! log_info_sync {
    ($logger:expr, $fmt:expr $(, $arg:expr)*) => {{
        let msg = format!($fmt $(, $arg)*);
        // For ultra-low latency, just print to stderr in sync context
        eprintln!("[INFO] {}", msg);
    }};
}

#[macro_export]
macro_rules! log_warn_sync {
    ($logger:expr, $fmt:expr $(, $arg:expr)*) => {{
        let msg = format!($fmt $(, $arg)*);
        eprintln!("[WARN] {}", msg);
    }};
}

#[macro_export]
macro_rules! log_error_sync {
    ($logger:expr, $fmt:expr $(, $arg:expr)*) => {{
        let msg = format!($fmt $(, $arg)*);
        eprintln!("[ERROR] {}", msg);
    }};
}

#[macro_export]
macro_rules! log_debug_sync {
    ($logger:expr, $fmt:expr $(, $arg:expr)*) => {{
        let msg = format!($fmt $(, $arg)*);
        eprintln!("[DEBUG] {}", msg);
    }};
}
