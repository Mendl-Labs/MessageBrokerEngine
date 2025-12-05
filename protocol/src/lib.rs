use std::sync::Arc;
use std::collections::HashMap;

// Ultra-Logger for high-performance logging
use ultra_logger::{UltraLogger, LoggerConfig, TransportConfig, ConnectionConfig};
use once_cell::sync::Lazy;

pub mod broker {
    pub mod messages {
        pub use crate::generated::*;
    }
}

pub mod generated;
pub mod compression;
pub use compression::{MessageCompressor, CompressionConfig, CompressionAlgorithm, AdaptiveCompressor};

// Create Elasticsearch configuration for protocol
fn create_elasticsearch_config(component: &str) -> LoggerConfig {
    let use_elasticsearch = std::env::var("USE_ELASTICSEARCH_LOGGING")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(true);

    if use_elasticsearch {
        let endpoint = std::env::var("ELASTICSEARCH_ENDPOINT")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_ENDPOINT"))
            .unwrap_or_else(|_| "https://trading-bot-observability-6b76eb.es.us-central1.gcp.cloud.es.io".to_string());
        let username = std::env::var("ELASTICSEARCH_USERNAME")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_USERNAME"))
            .unwrap_or_else(|_| "elastic".to_string());
        let password = std::env::var("ELASTICSEARCH_PASSWORD")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_PASSWORD"))
            .unwrap_or_default();

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

/// Protocol logger instance
pub static PROTOCOL_LOGGER: Lazy<Arc<UltraLogger>> = Lazy::new(|| {
    Arc::new(UltraLogger::with_config(
        "MessageBrokerEngine-protocol".to_string(),
        create_elasticsearch_config("protocol")
    ))
});