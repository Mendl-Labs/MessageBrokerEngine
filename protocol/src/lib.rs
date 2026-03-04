pub mod broker {
    pub mod messages {
        pub use crate::generated::*;
    }
}

pub mod generated;
pub mod compression;
pub mod tenant_topics;
pub use compression::{MessageCompressor, CompressionConfig, CompressionAlgorithm, AdaptiveCompressor};
pub use tenant_topics::{tenant_topic, tenant_topic_uuid, parse_tenant_topic, is_tenant_topic, is_global_topic};