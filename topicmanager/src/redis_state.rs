//! Redis State Synchronization for MessageBrokerEngine
//!
//! Persists topic registry and subscription state to Redis Cloud for:
//! - Multi-replica state sync (horizontal scaling)
//! - Crash recovery (restore subscriptions on restart)
//! - Cross-datacenter topic awareness
//!
//! ## Design
//! - **Write-behind**: State changes are synced asynchronously (fire-and-forget)
//!   to avoid adding Redis latency to the hot publish path (~176ns target)
//! - **Read-on-startup**: Full state restoration from Redis on boot
//! - **TTL-based cleanup**: Stale entries auto-expire (default: 1 hour)
//!
//! ## Redis Key Schema
//! ```text
//! mbe:topics                          → SET of topic names
//! mbe:topic:{topic_name}:subscribers  → SET of subscriber_id (u64 as string)
//! mbe:subscriber:{id}:topics          → SET of topic patterns
//! mbe:subscriber:{id}:meta            → HASH of subscription metadata
//! mbe:instance:{instance_id}:alive    → STRING "1" with TTL (heartbeat)
//! ```

#[cfg(feature = "redis-sync")]
mod inner {
    use redis::aio::ConnectionManager;
    use redis::{AsyncCommands, Client, RedisError};
    use serde::{Deserialize, Serialize};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

    /// Serializable subscription info for Redis persistence
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PersistedSubscription {
        pub subscriber_id: u64,
        pub topic_pattern: String,
        pub metadata: HashMap<String, String>,
    }

    /// Redis state sync configuration
    #[derive(Debug, Clone)]
    pub struct RedisStateConfig {
        /// Redis connection URL (e.g., `redis://user:pass@host:port`)
        pub redis_url: String,
        /// Instance ID for this broker replica (used for heartbeats)
        pub instance_id: String,
        /// TTL for subscription entries (default: 1 hour)
        pub subscription_ttl: Duration,
        /// Heartbeat interval (default: 10 seconds)
        pub heartbeat_interval: Duration,
        /// Key prefix (default: "mbe")
        pub key_prefix: String,
    }

    impl Default for RedisStateConfig {
        fn default() -> Self {
            Self {
                redis_url: "redis://127.0.0.1:6379".to_string(),
                instance_id: uuid::Uuid::new_v4().to_string(),
                subscription_ttl: Duration::from_secs(3600),
                heartbeat_interval: Duration::from_secs(10),
                key_prefix: "mbe".to_string(),
            }
        }
    }

    /// Redis-backed state synchronization for the MessageBroker.
    ///
    /// All write operations are fire-and-forget to avoid impacting
    /// the broker's ~176ns publish latency.
    pub struct RedisStateSync {
        conn: Arc<RwLock<ConnectionManager>>,
        config: RedisStateConfig,
    }

    impl RedisStateSync {
        /// Connect to Redis and create a new state sync instance.
        pub async fn new(config: RedisStateConfig) -> Result<Self, RedisError> {
            let client = Client::open(config.redis_url.as_str())?;
            let conn = ConnectionManager::new(client).await?;

            Ok(Self {
                conn: Arc::new(RwLock::new(conn)),
                config,
            })
        }

        // ── Key helpers ──────────────────────────────────────────

        fn topics_key(&self) -> String {
            format!("{}:topics", self.config.key_prefix)
        }

        fn topic_subscribers_key(&self, topic: &str) -> String {
            format!("{}:topic:{}:subscribers", self.config.key_prefix, topic)
        }

        fn subscriber_topics_key(&self, subscriber_id: u64) -> String {
            format!(
                "{}:subscriber:{}:topics",
                self.config.key_prefix, subscriber_id
            )
        }

        fn subscriber_meta_key(&self, subscriber_id: u64) -> String {
            format!(
                "{}:subscriber:{}:meta",
                self.config.key_prefix, subscriber_id
            )
        }

        fn instance_alive_key(&self) -> String {
            format!(
                "{}:instance:{}:alive",
                self.config.key_prefix, self.config.instance_id
            )
        }

        // ── Topic Operations ─────────────────────────────────────

        /// Register a topic in Redis.
        pub async fn register_topic(&self, topic_name: &str) -> Result<(), RedisError> {
            let mut conn = self.conn.write().await;
            let _: () = conn.sadd(&self.topics_key(), topic_name).await?;
            Ok(())
        }

        /// Remove a topic from Redis and clean up subscriber mappings.
        pub async fn remove_topic(&self, topic_name: &str) -> Result<(), RedisError> {
            let mut conn = self.conn.write().await;
            let key = self.topics_key();
            let sub_key = self.topic_subscribers_key(topic_name);

            // Get all subscribers for this topic before removing
            let subscriber_ids: Vec<String> = conn.smembers(&sub_key).await.unwrap_or_default();

            // Remove topic from the set
            let _: () = conn.srem(&key, topic_name).await?;

            // Remove the topic's subscriber set
            let _: () = conn.del(&sub_key).await.unwrap_or(());

            // Remove this topic from each subscriber's topic set
            for sid_str in subscriber_ids {
                if let Ok(sid) = sid_str.parse::<u64>() {
                    let st_key = self.subscriber_topics_key(sid);
                    let _: () = conn.srem(&st_key, topic_name).await.unwrap_or(());
                }
            }

            Ok(())
        }

        /// Get all registered topics.
        pub async fn get_all_topics(&self) -> Result<HashSet<String>, RedisError> {
            let mut conn = self.conn.write().await;
            let topics: HashSet<String> = conn.smembers(&self.topics_key()).await?;
            Ok(topics)
        }

        // ── Subscription Operations ──────────────────────────────

        /// Persist a subscription to Redis.
        pub async fn persist_subscription(
            &self,
            subscriber_id: u64,
            topic_pattern: &str,
            metadata: &HashMap<String, String>,
        ) -> Result<(), RedisError> {
            let mut conn = self.conn.write().await;
            let ttl_secs = self.config.subscription_ttl.as_secs() as i64;

            // Add topic to the set of topics
            let _: () = conn.sadd(&self.topics_key(), topic_pattern).await?;

            // Add subscriber to topic's subscriber set
            let ts_key = self.topic_subscribers_key(topic_pattern);
            let _: () = conn.sadd(&ts_key, subscriber_id.to_string()).await?;
            let _: () = conn.expire(&ts_key, ttl_secs).await?;

            // Add topic to subscriber's topic set
            let st_key = self.subscriber_topics_key(subscriber_id);
            let _: () = conn.sadd(&st_key, topic_pattern).await?;
            let _: () = conn.expire(&st_key, ttl_secs).await?;

            // Store metadata if non-empty
            if !metadata.is_empty() {
                let meta_key = self.subscriber_meta_key(subscriber_id);
                let serialized =
                    serde_json::to_string(metadata).unwrap_or_else(|_| "{}".to_string());
                let _: () = conn.hset(&meta_key, topic_pattern, &serialized).await?;
                let _: () = conn.expire(&meta_key, ttl_secs).await?;
            }

            Ok(())
        }

        /// Remove a subscription from Redis.
        pub async fn remove_subscription(
            &self,
            subscriber_id: u64,
            topic_pattern: &str,
        ) -> Result<(), RedisError> {
            let mut conn = self.conn.write().await;

            // Remove subscriber from topic's set
            let ts_key = self.topic_subscribers_key(topic_pattern);
            let _: () = conn.srem(&ts_key, subscriber_id.to_string()).await?;

            // Remove topic from subscriber's set
            let st_key = self.subscriber_topics_key(subscriber_id);
            let _: () = conn.srem(&st_key, topic_pattern).await?;

            // Remove metadata entry
            let meta_key = self.subscriber_meta_key(subscriber_id);
            let _: () = conn.hdel(&meta_key, topic_pattern).await.unwrap_or(());

            // Clean up empty topic entry
            let remaining: usize = conn.scard(&ts_key).await.unwrap_or(0);
            if remaining == 0 {
                let _: () = conn.srem(&self.topics_key(), topic_pattern).await?;
                let _: () = conn.del(&ts_key).await.unwrap_or(());
            }

            Ok(())
        }

        /// Remove all subscriptions for a subscriber.
        pub async fn remove_all_subscriptions(
            &self,
            subscriber_id: u64,
        ) -> Result<(), RedisError> {
            let mut conn = self.conn.write().await;
            let st_key = self.subscriber_topics_key(subscriber_id);

            // Get all topics this subscriber is on
            let topics: Vec<String> = conn.smembers(&st_key).await.unwrap_or_default();

            // Remove subscriber from each topic's set
            for topic in &topics {
                let ts_key = self.topic_subscribers_key(topic);
                let _: () = conn.srem(&ts_key, subscriber_id.to_string()).await?;

                // Clean up empty topic
                let remaining: usize = conn.scard(&ts_key).await.unwrap_or(0);
                if remaining == 0 {
                    let _: () = conn.del(&ts_key).await.unwrap_or(());
                }
            }

            // Remove subscriber's topic set and metadata
            let _: () = conn.del(&st_key).await.unwrap_or(());
            let meta_key = self.subscriber_meta_key(subscriber_id);
            let _: () = conn.del(&meta_key).await.unwrap_or(());

            Ok(())
        }

        // ── Recovery Operations ──────────────────────────────────

        /// Load all subscriptions from Redis (for crash recovery).
        ///
        /// Returns a map of `topic -> set of subscriber_ids`.
        pub async fn load_all_subscriptions(
            &self,
        ) -> Result<HashMap<String, HashSet<u64>>, RedisError> {
            let mut conn = self.conn.write().await;
            let topics: HashSet<String> = conn.smembers(&self.topics_key()).await?;
            let mut result = HashMap::new();

            for topic in topics {
                let ts_key = self.topic_subscribers_key(&topic);
                let subscriber_strs: Vec<String> = conn.smembers(&ts_key).await.unwrap_or_default();
                let subscribers: HashSet<u64> = subscriber_strs
                    .iter()
                    .filter_map(|s| s.parse::<u64>().ok())
                    .collect();

                if !subscribers.is_empty() {
                    result.insert(topic, subscribers);
                }
            }

            Ok(result)
        }

        /// Load full subscription details for recovery (including metadata).
        pub async fn load_subscription_details(
            &self,
        ) -> Result<Vec<PersistedSubscription>, RedisError> {
            let mut conn = self.conn.write().await;
            let topics: HashSet<String> = conn.smembers(&self.topics_key()).await?;
            let mut results = Vec::new();

            for topic in topics {
                let ts_key = self.topic_subscribers_key(&topic);
                let subscriber_strs: Vec<String> = conn.smembers(&ts_key).await.unwrap_or_default();

                for sid_str in subscriber_strs {
                    if let Ok(subscriber_id) = sid_str.parse::<u64>() {
                        // Try to load metadata
                        let meta_key = self.subscriber_meta_key(subscriber_id);
                        let meta_str: Option<String> =
                            conn.hget(&meta_key, &topic).await.unwrap_or(None);
                        let metadata: HashMap<String, String> = meta_str
                            .and_then(|s| serde_json::from_str(&s).ok())
                            .unwrap_or_default();

                        results.push(PersistedSubscription {
                            subscriber_id,
                            topic_pattern: topic.clone(),
                            metadata,
                        });
                    }
                }
            }

            Ok(results)
        }

        // ── Heartbeat ────────────────────────────────────────────

        /// Send a heartbeat to indicate this instance is alive.
        pub async fn heartbeat(&self) -> Result<(), RedisError> {
            let mut conn = self.conn.write().await;
            let key = self.instance_alive_key();
            let ttl = (self.config.heartbeat_interval.as_secs() * 3) as i64;
            let _: () = conn.set_ex(&key, "1", ttl as u64).await?;
            Ok(())
        }

        /// Check which broker instances are currently alive.
        pub async fn get_alive_instances(&self) -> Result<Vec<String>, RedisError> {
            let mut conn = self.conn.write().await;
            let pattern = format!("{}:instance:*:alive", self.config.key_prefix);
            let keys: Vec<String> = redis::cmd("KEYS")
                .arg(&pattern)
                .query_async(&mut *conn)
                .await?;

            // Extract instance IDs from key names
            let prefix = format!("{}:instance:", self.config.key_prefix);
            let suffix = ":alive";
            Ok(keys
                .into_iter()
                .filter_map(|k| {
                    k.strip_prefix(&prefix)
                        .and_then(|s| s.strip_suffix(suffix))
                        .map(|s| s.to_string())
                })
                .collect())
        }

        /// Start the background heartbeat loop.
        pub fn start_heartbeat(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
            let sync = Arc::clone(self);
            tokio::spawn(async move {
                let interval = sync.config.heartbeat_interval;
                loop {
                    if let Err(e) = sync.heartbeat().await {
                        eprintln!("[RedisStateSync] Heartbeat failed: {}", e);
                    }
                    tokio::time::sleep(interval).await;
                }
            })
        }

        // ── Metrics ──────────────────────────────────────────────

        /// Get the total number of persisted topics.
        pub async fn topic_count(&self) -> Result<usize, RedisError> {
            let mut conn = self.conn.write().await;
            let count: usize = conn.scard(&self.topics_key()).await?;
            Ok(count)
        }

        /// Get the total number of persisted subscriptions across all topics.
        pub async fn total_subscription_count(&self) -> Result<usize, RedisError> {
            let mut conn = self.conn.write().await;
            let topics: HashSet<String> = conn.smembers(&self.topics_key()).await?;
            let mut total = 0usize;
            for topic in topics {
                let ts_key = self.topic_subscribers_key(&topic);
                let count: usize = conn.scard(&ts_key).await.unwrap_or(0);
                total += count;
            }
            Ok(total)
        }

        /// Flush all MBE state from Redis (for testing/reset).
        pub async fn flush_all(&self) -> Result<(), RedisError> {
            let mut conn = self.conn.write().await;
            let pattern = format!("{}:*", self.config.key_prefix);
            let keys: Vec<String> = redis::cmd("KEYS")
                .arg(&pattern)
                .query_async(&mut *conn)
                .await?;

            if !keys.is_empty() {
                for key in &keys {
                    let _: () = conn.del(key).await.unwrap_or(());
                }
            }
            Ok(())
        }

        /// Get config reference
        pub fn config(&self) -> &RedisStateConfig {
            &self.config
        }
    }
}

// Re-export when feature is enabled
#[cfg(feature = "redis-sync")]
pub use inner::*;

/// Fire-and-forget async helper for non-blocking Redis sync.
///
/// Spawns a tokio task to perform the Redis operation without
/// blocking the caller. Errors are logged but not propagated.
#[cfg(feature = "redis-sync")]
#[macro_export]
macro_rules! redis_sync_fire_forget {
    ($sync:expr, $op:expr) => {
        if let Some(ref sync) = $sync {
            let sync = Arc::clone(sync);
            tokio::spawn(async move {
                if let Err(e) = $op(&sync).await {
                    eprintln!("[RedisStateSync] Background sync error: {}", e);
                }
            });
        }
    };
}

/// Stub for when Redis sync is disabled — does nothing.
#[cfg(not(feature = "redis-sync"))]
#[macro_export]
macro_rules! redis_sync_fire_forget {
    ($sync:expr, $op:expr) => {
        // No-op when redis-sync feature is disabled
        let _ = &$sync;
    };
}
