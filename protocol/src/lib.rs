pub mod broker {
    pub mod messages {
        pub use crate::generated::*;
    }
}

pub mod generated;
pub mod compression;
pub use compression::{MessageCompressor, CompressionConfig, CompressionAlgorithm, AdaptiveCompressor};