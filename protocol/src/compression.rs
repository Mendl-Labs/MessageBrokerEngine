// Advanced message compression for bandwidth optimization
use std::io::{Read, Write};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use lz4::{Decoder, EncoderBuilder};
use std::time::Instant;
use std::sync::OnceLock;
use ultra_logger::UltraLogger;

// Protocol logger - initialized lazily, works without async runtime
static PROTOCOL_LOGGER: OnceLock<UltraLogger> = OnceLock::new();

fn get_logger() -> &'static UltraLogger {
    PROTOCOL_LOGGER.get_or_init(|| {
        UltraLogger::new("protocol".to_string())
    })
}

// Logging macros that use ultra-logger
macro_rules! comp_warn {
    ($fmt:expr $(, $arg:expr)*) => {
        get_logger().warn_sync(format!($fmt $(, $arg)*));
    };
}

#[allow(unused_macros)]
macro_rules! comp_debug {
    ($fmt:expr $(, $arg:expr)*) => {
        #[cfg(debug_assertions)]
        get_logger().debug_sync(format!($fmt $(, $arg)*));
    };
}

/// Compression algorithms supported by the message broker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None,
    Gzip,
    Lz4,
    Snappy,
}

/// Compression configuration with adaptive settings
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub compression_level: u32,
    pub min_message_size_for_compression: usize,
    pub adaptive_compression: bool,
    pub compression_ratio_threshold: f32, // Don't compress if ratio < threshold
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Lz4,
            compression_level: 1, // Fast compression for low latency
            min_message_size_for_compression: 1024, // Only compress messages > 1KB
            adaptive_compression: true,
            compression_ratio_threshold: 0.1, // Must achieve 10% compression
        }
    }
}

/// Message compression engine with multiple algorithms
pub struct MessageCompressor {
    config: CompressionConfig,
    compression_stats: CompressionStats,
}

/// Compression statistics for monitoring
#[derive(Debug, Default)]
pub struct CompressionStats {
    pub total_messages: u64,
    pub compressed_messages: u64,
    pub total_original_bytes: u64,
    pub total_compressed_bytes: u64,
    pub total_compression_time_ns: u64,
    pub total_decompression_time_ns: u64,
}

impl CompressionStats {
    pub fn compression_ratio(&self) -> f32 {
        if self.total_original_bytes == 0 {
            0.0
        } else {
            self.total_compressed_bytes as f32 / self.total_original_bytes as f32
        }
    }
    
    pub fn average_compression_time_ns(&self) -> u64 {
        if self.compressed_messages == 0 {
            0
        } else {
            self.total_compression_time_ns / self.compressed_messages
        }
    }
    
    pub fn average_decompression_time_ns(&self) -> u64 {
        if self.compressed_messages == 0 {
            0
        } else {
            self.total_decompression_time_ns / self.compressed_messages
        }
    }
    
    pub fn space_savings_bytes(&self) -> u64 {
        if self.total_original_bytes >= self.total_compressed_bytes {
            self.total_original_bytes - self.total_compressed_bytes
        } else {
            0
        }
    }
    
    pub fn space_savings_percentage(&self) -> f32 {
        if self.total_original_bytes == 0 {
            0.0
        } else {
            (self.space_savings_bytes() as f32 / self.total_original_bytes as f32) * 100.0
        }
    }
}

impl MessageCompressor {
    pub fn new(config: CompressionConfig) -> Self {
        Self {
            config,
            compression_stats: CompressionStats::default(),
        }
    }
    
    /// Compress a message using the configured algorithm
    pub fn compress(&mut self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let start_time = Instant::now();
        
        self.compression_stats.total_messages += 1;
        self.compression_stats.total_original_bytes += data.len() as u64;
        
        // Skip compression for small messages
        if data.len() < self.config.min_message_size_for_compression {
            comp_debug!("Skipping compression for small message ({} bytes)", data.len());
            let mut result = Vec::with_capacity(data.len() + 1);
            result.push(0u8); // 0 = uncompressed
            result.extend_from_slice(data);
            return Ok(result);
        }
        
        let compressed_result = match self.config.algorithm {
            CompressionAlgorithm::None => {
                let mut result = Vec::with_capacity(data.len() + 1);
                result.push(0u8); // 0 = uncompressed
                result.extend_from_slice(data);
                Ok(result)
            },
            CompressionAlgorithm::Gzip => self.compress_gzip(data),
            CompressionAlgorithm::Lz4 => self.compress_lz4(data),
            CompressionAlgorithm::Snappy => self.compress_snappy(data),
        };
        
        match compressed_result {
            Ok(compressed_data) => {
                let compression_time = start_time.elapsed();
                self.compression_stats.total_compression_time_ns += compression_time.as_nanos() as u64;
                
                // Check if compression is effective
                let compression_ratio = compressed_data.len() as f32 / data.len() as f32;
                
                if self.config.adaptive_compression && 
                   compression_ratio > (1.0 - self.config.compression_ratio_threshold) {
                    // Compression not effective enough, return original with header
                    comp_debug!("Compression not effective (ratio: {:.2}), using original", compression_ratio);
                    let mut result = Vec::with_capacity(data.len() + 1);
                    result.push(0u8); // 0 = uncompressed
                    result.extend_from_slice(data);
                    Ok(result)
                } else {
                    self.compression_stats.compressed_messages += 1;
                    self.compression_stats.total_compressed_bytes += compressed_data.len() as u64;
                    
                    comp_debug!("Compressed {} bytes to {} bytes (ratio: {:.2}) in {}ns", 
                          data.len(), 
                          compressed_data.len(), 
                          compression_ratio,
                          compression_time.as_nanos());
                    
                    let mut result = Vec::with_capacity(compressed_data.len() + 1);
                    result.push(1u8); // 1 = compressed
                    result.extend_from_slice(&compressed_data);
                    Ok(result)
                }
            }
            Err(e) => {
                comp_warn!("Compression failed: {}, using original data", e);
                let mut result = Vec::with_capacity(data.len() + 1);
                result.push(0u8); // 0 = uncompressed
                result.extend_from_slice(data);
                Ok(result)
            }
        }
    }
    
    /// Decompress a message using the configured algorithm
    pub fn decompress(&mut self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let start_time = Instant::now();
        
        // Check if data is empty or too small to have header
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        // Check compression header
        let is_compressed = data[0] == 1u8;
        let payload = &data[1..];
        
        let decompressed_result = if is_compressed {
            match self.config.algorithm {
                CompressionAlgorithm::None => Ok(payload.to_vec()),
                CompressionAlgorithm::Gzip => self.decompress_gzip(payload),
                CompressionAlgorithm::Lz4 => self.decompress_lz4(payload),
                CompressionAlgorithm::Snappy => self.decompress_snappy(payload),
            }
        } else {
            // Data is not compressed, return as-is
            Ok(payload.to_vec())
        };
        
        if let Ok(_) = decompressed_result {
            let decompression_time = start_time.elapsed();
            self.compression_stats.total_decompression_time_ns += decompression_time.as_nanos() as u64;
        }
        
        decompressed_result
    }
    
    /// Get compression statistics
    pub fn get_stats(&self) -> &CompressionStats {
        &self.compression_stats
    }
    
    /// Reset compression statistics
    pub fn reset_stats(&mut self) {
        self.compression_stats = CompressionStats::default();
    }
    
    fn compress_gzip(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.config.compression_level));
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }
    
    fn decompress_gzip(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }
    
    fn compress_lz4(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut encoder = EncoderBuilder::new()
            .level(self.config.compression_level)
            .build(Vec::new())?;
        encoder.write_all(data)?;
        let (compressed, result) = encoder.finish();
        result?;
        Ok(compressed)
    }
    
    fn decompress_lz4(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }
    
    fn compress_snappy(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        // Snappy compression (would require snap crate)
        // For now, fall back to LZ4
        self.compress_lz4(data)
    }
    
    fn decompress_snappy(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        // Snappy decompression (would require snap crate)
        // For now, fall back to LZ4
        self.decompress_lz4(data)
    }
}

/// Compression errors
#[derive(Debug)]
pub enum CompressionError {
    IoError(std::io::Error),
    InvalidData,
    UnsupportedAlgorithm,
}

impl From<std::io::Error> for CompressionError {
    fn from(error: std::io::Error) -> Self {
        CompressionError::IoError(error)
    }
}

impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionError::IoError(e) => write!(f, "IO error: {}", e),
            CompressionError::InvalidData => write!(f, "Invalid data for decompression"),
            CompressionError::UnsupportedAlgorithm => write!(f, "Unsupported compression algorithm"),
        }
    }
}

impl std::error::Error for CompressionError {}

/// Adaptive compression that chooses algorithm based on message characteristics
pub struct AdaptiveCompressor {
    gzip_compressor: MessageCompressor,
    lz4_compressor: MessageCompressor,
    sample_count: usize,
    decision_threshold: usize,
}

impl AdaptiveCompressor {
    pub fn new() -> Self {
        let gzip_config = CompressionConfig {
            algorithm: CompressionAlgorithm::Gzip,
            compression_level: 6, // Balanced compression
            ..Default::default()
        };
        
        let lz4_config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            compression_level: 1, // Fast compression
            ..Default::default()
        };
        
        Self {
            gzip_compressor: MessageCompressor::new(gzip_config),
            lz4_compressor: MessageCompressor::new(lz4_config),
            sample_count: 0,
            decision_threshold: 100, // Sample first 100 messages to decide
        }
    }
    
    /// Compress using adaptive algorithm selection
    pub fn compress(&mut self, data: &[u8]) -> Result<(Vec<u8>, CompressionAlgorithm), CompressionError> {
        if self.sample_count < self.decision_threshold {
            // During sampling phase, try both algorithms on every 10th message
            if self.sample_count % 10 == 0 {
                let _ = self.gzip_compressor.compress(data)?;
                let _ = self.lz4_compressor.compress(data)?;
            }
            self.sample_count += 1;
        }
        
        // Choose algorithm based on performance characteristics
        let algorithm = if self.sample_count >= self.decision_threshold {
            self.choose_optimal_algorithm()
        } else {
            CompressionAlgorithm::Lz4 // Default to fast compression
        };
        
        match algorithm {
            CompressionAlgorithm::Gzip => {
                let compressed = self.gzip_compressor.compress(data)?;
                Ok((compressed, CompressionAlgorithm::Gzip))
            }
            CompressionAlgorithm::Lz4 => {
                let compressed = self.lz4_compressor.compress(data)?;
                Ok((compressed, CompressionAlgorithm::Lz4))
            }
            _ => {
                let compressed = self.lz4_compressor.compress(data)?;
                Ok((compressed, CompressionAlgorithm::Lz4))
            }
        }
    }
    
    fn choose_optimal_algorithm(&self) -> CompressionAlgorithm {
        let gzip_stats = self.gzip_compressor.get_stats();
        let lz4_stats = self.lz4_compressor.get_stats();
        
        // If both have processed messages, compare performance
        if gzip_stats.compressed_messages > 0 && lz4_stats.compressed_messages > 0 {
            let gzip_ratio = gzip_stats.compression_ratio();
            let lz4_ratio = lz4_stats.compression_ratio();
            
            let gzip_avg_time = gzip_stats.average_compression_time_ns();
            let lz4_avg_time = lz4_stats.average_compression_time_ns();
            
            // Choose based on compression ratio vs time tradeoff
            // If gzip achieves significantly better compression (>10% better) 
            // and time difference is reasonable (<10x), prefer gzip
            if gzip_ratio < lz4_ratio * 0.9 && gzip_avg_time < lz4_avg_time * 10 {
                CompressionAlgorithm::Gzip
            } else {
                CompressionAlgorithm::Lz4
            }
        } else {
            CompressionAlgorithm::Lz4
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_compression_basic() {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            min_message_size_for_compression: 10,
            ..Default::default()
        };
        
        let mut compressor = MessageCompressor::new(config);
        let original_data = b"This is a test message that should be compressed because it's long enough and has repeating patterns. This is a test message that should be compressed because it's long enough and has repeating patterns.";
        
        let compressed = compressor.compress(original_data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        
        assert_eq!(original_data, decompressed.as_slice());
        // Compressed data should include header byte
        assert!(compressed.len() >= 1);
    }
    
    #[test]
    fn test_compression_small_message() {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            min_message_size_for_compression: 100,
            ..Default::default()
        };
        
        let mut compressor = MessageCompressor::new(config);
        let small_data = b"small";
        
        let result = compressor.compress(small_data).unwrap();
        // Small data should have header byte (0) + original data
        assert_eq!(result.len(), small_data.len() + 1);
        assert_eq!(result[0], 0u8); // Should be marked as uncompressed
        assert_eq!(&result[1..], small_data); // Rest should be original data
    }
    
    #[test]
    fn test_compression_stats() {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            min_message_size_for_compression: 10,
            ..Default::default()
        };
        
        let mut compressor = MessageCompressor::new(config);
        let data = b"This is test data for compression statistics tracking. This is test data for compression statistics tracking. This is test data for compression statistics tracking. This is test data for compression statistics tracking.";
        
        let _compressed = compressor.compress(data).unwrap();
        
        let stats = compressor.get_stats();
        assert_eq!(stats.total_messages, 1);
        // compressed_messages could be 0 or 1 depending on compression effectiveness
        assert!(stats.compressed_messages <= 1);
        assert!(stats.total_original_bytes > 0);
        assert!(stats.total_compressed_bytes > 0);
        assert!(stats.total_compression_time_ns > 0);
    }
    
    #[test]
    fn test_adaptive_compressor() {
        let mut adaptive = AdaptiveCompressor::new();
        let test_data = b"Adaptive compression test data with patterns and repetition for testing.";
        
        let (compressed, algorithm) = adaptive.compress(test_data).unwrap();
        assert!(!compressed.is_empty());
        assert!(matches!(algorithm, CompressionAlgorithm::Lz4 | CompressionAlgorithm::Gzip));
    }
    
    #[test]
    fn test_gzip_compression() {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Gzip,
            compression_level: 6,
            min_message_size_for_compression: 10,
            ..Default::default()
        };
        
        let mut compressor = MessageCompressor::new(config);
        let original_data = b"GZIP compression test with repeating data: AAAAAAAAAA BBBBBBBBBB CCCCCCCCCC AAAAAAAAAA BBBBBBBBBB CCCCCCCCCC AAAAAAAAAA BBBBBBBBBB CCCCCCCCCC";
        
        let compressed = compressor.compress(original_data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        
        assert_eq!(original_data, decompressed.as_slice());
        assert!(compressed.len() < original_data.len());
    }
}
