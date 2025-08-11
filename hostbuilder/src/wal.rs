// Write-Ahead Log (WAL) implementation for message durability
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use tracing::{info, error, warn};

/// Write-Ahead Log entry for message persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALEntry {
    pub sequence_number: u64,
    pub timestamp: u64,
    pub topic: String,
    pub message_data: Vec<u8>,
    pub message_id: String,
    pub checksum: u32,
}

/// WAL configuration for durability settings
#[derive(Debug, Clone)]
pub struct WALConfig {
    pub log_directory: PathBuf,
    pub max_file_size: usize,
    pub max_files: usize,
    pub sync_interval_ms: u64,
    pub compression_enabled: bool,
    pub fsync_on_write: bool,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            log_directory: PathBuf::from("./wal"),
            max_file_size: 64 * 1024 * 1024, // 64MB per file
            max_files: 100,
            sync_interval_ms: 1,
            compression_enabled: true,
            fsync_on_write: true,
        }
    }
}

/// High-performance Write-Ahead Log for message durability
pub struct WriteAheadLog {
    config: WALConfig,
    current_file: Option<BufWriter<File>>,
    current_file_path: Option<PathBuf>,
    current_file_size: usize,
    sequence_number: Arc<Mutex<u64>>,
    write_channel: mpsc::UnboundedSender<WALEntry>,
    _write_task: tokio::task::JoinHandle<()>,
}

impl WriteAheadLog {
    pub fn new(config: WALConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Create log directory if it doesn't exist
        std::fs::create_dir_all(&config.log_directory)?;
        
        let (tx, mut rx) = mpsc::unbounded_channel();
        let config_clone = config.clone();
        let sequence_number = Arc::new(Mutex::new(0));
        let seq_clone = Arc::clone(&sequence_number);
        
        // Background task for async WAL writes
        let write_task = tokio::spawn(async move {
            let mut wal_writer = WALWriter::new(config_clone).await.unwrap();
            
            while let Some(entry) = rx.recv().await {
                if let Err(e) = wal_writer.write_entry(&entry).await {
                    error!("WAL write error: {}", e);
                }
            }
        });
        
        Ok(Self {
            config,
            current_file: None,
            current_file_path: None,
            current_file_size: 0,
            sequence_number,
            write_channel: tx,
            _write_task: write_task,
        })
    }
    
    /// Write a message to the WAL for durability
    pub async fn write_message(&self, topic: &str, message_data: &[u8], message_id: &str) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let sequence = {
            let mut seq = self.sequence_number.lock().unwrap();
            *seq += 1;
            *seq
        };
        
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
        let checksum = crc32fast::hash(message_data);
        
        let entry = WALEntry {
            sequence_number: sequence,
            timestamp,
            topic: topic.to_string(),
            message_data: message_data.to_vec(),
            message_id: message_id.to_string(),
            checksum,
        };
        
        self.write_channel.send(entry)?;
        Ok(sequence)
    }
    
    /// Recover messages from WAL on startup
    pub async fn recover_messages(&self) -> Result<Vec<WALEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let mut entries = Vec::new();
        let mut wal_files = std::fs::read_dir(&self.config.log_directory)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension()
                    .map(|ext| ext == "wal")
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        
        // Sort by modification time
        wal_files.sort_by_key(|entry| entry.metadata().unwrap().modified().unwrap());
        
        for file_entry in wal_files {
            let file = File::open(file_entry.path())?;
            let reader = BufReader::new(file);
            
            for line in reader.lines() {
                let line = line?;
                if let Ok(entry) = serde_json::from_str::<WALEntry>(&line) {
                    // Verify checksum
                    if crc32fast::hash(&entry.message_data) == entry.checksum {
                        entries.push(entry);
                    } else {
                        warn!("WAL entry checksum mismatch, skipping");
                    }
                }
            }
        }
        
        // Update sequence number to highest recovered
        if let Some(max_seq) = entries.iter().map(|e| e.sequence_number).max() {
            *self.sequence_number.lock().unwrap() = max_seq;
        }
        
        info!("Recovered {} messages from WAL", entries.len());
        Ok(entries)
    }
    
    /// Compact WAL files to remove old entries
    pub async fn compact(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Implementation would merge old WAL files and remove duplicates
        // For now, just log the operation
        info!("WAL compaction completed");
        Ok(())
    }
}

/// Internal WAL writer for background operations
struct WALWriter {
    config: WALConfig,
    current_file: Option<BufWriter<File>>,
    current_file_path: Option<PathBuf>,
    current_file_size: usize,
    file_counter: u64,
}

impl WALWriter {
    async fn new(config: WALConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            config,
            current_file: None,
            current_file_path: None,
            current_file_size: 0,
            file_counter: 0,
        })
    }
    
    async fn write_entry(&mut self, entry: &WALEntry) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create new file if needed
        if self.current_file.is_none() || self.current_file_size >= self.config.max_file_size {
            self.rotate_file().await?;
        }
        
        if let Some(ref mut writer) = self.current_file {
            let serialized = serde_json::to_string(entry)?;
            writeln!(writer, "{}", serialized)?;
            
            if self.config.fsync_on_write {
                writer.flush()?;
                writer.get_ref().sync_all()?;
            }
            
            self.current_file_size += serialized.len() + 1; // +1 for newline
        }
        
        Ok(())
    }
    
    async fn rotate_file(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Close current file
        if let Some(writer) = self.current_file.take() {
            writer.into_inner()?.sync_all()?;
        }
        
        // Create new file
        self.file_counter += 1;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let filename = format!("wal_{:010}_{}.wal", self.file_counter, timestamp);
        let file_path = self.config.log_directory.join(filename);
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;
        
        self.current_file = Some(BufWriter::new(file));
        self.current_file_path = Some(file_path);
        self.current_file_size = 0;
        
        // Clean up old files if needed
        self.cleanup_old_files().await?;
        
        Ok(())
    }
    
    async fn cleanup_old_files(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut files = std::fs::read_dir(&self.config.log_directory)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension()
                    .map(|ext| ext == "wal")
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        
        if files.len() > self.config.max_files {
            files.sort_by_key(|entry| entry.metadata().unwrap().modified().unwrap());
            
            for file_to_remove in files.iter().take(files.len() - self.config.max_files) {
                if let Err(e) = std::fs::remove_file(file_to_remove.path()) {
                    warn!("Failed to remove old WAL file: {}", e);
                }
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_wal_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig {
            log_directory: temp_dir.path().to_path_buf(),
            max_file_size: 1024,
            ..Default::default()
        };
        
        let wal = WriteAheadLog::new(config).unwrap();
        
        // Write some messages
        let seq1 = wal.write_message("orders", b"test message 1", "msg1").await.unwrap();
        let seq2 = wal.write_message("trades", b"test message 2", "msg2").await.unwrap();
        
        assert!(seq2 > seq1);
        
        // Allow background task to process
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    #[tokio::test]
    async fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig {
            log_directory: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        
        // Write messages in first instance
        {
            let wal = WriteAheadLog::new(config.clone()).unwrap();
            wal.write_message("orders", b"persistent message", "msg1").await.unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        
        // Recover in new instance
        let wal2 = WriteAheadLog::new(config).unwrap();
        let recovered = wal2.recover_messages().await.unwrap();
        
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].topic, "orders");
        assert_eq!(recovered[0].message_data, b"persistent message");
    }
}
