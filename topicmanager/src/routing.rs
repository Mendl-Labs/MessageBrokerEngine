// Enhanced message routing with intelligent topic management
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;
use regex::Regex;
use wildmatch::WildMatch;
use tracing::{info, debug, warn};

/// Message routing patterns for intelligent message distribution
#[derive(Debug, Clone)]
pub enum RoutingPattern {
    /// Exact topic match
    Exact(String),
    /// Wildcard pattern (e.g., "orders.*", "*.btc")
    Wildcard(String),
    /// Regular expression pattern
    Regex(String),
    /// Hash-based routing for load distribution
    HashBased { pattern: String, partition_count: usize },
    /// Geographic routing based on region
    Geographic { regions: Vec<String> },
    /// Priority-based routing
    Priority { min_priority: u8, max_priority: u8 },
}

/// Message routing rule with conditions
#[derive(Debug, Clone)]
pub struct RoutingRule {
    pub id: String,
    pub pattern: RoutingPattern,
    pub target_subscribers: HashSet<u64>,
    pub enabled: bool,
    pub metadata: HashMap<String, String>,
}

/// Advanced message router with intelligent pattern matching
pub struct IntelligentMessageRouter {
    routes: Arc<RwLock<HashMap<String, RoutingRule>>>,
    topic_cache: Arc<RwLock<HashMap<String, Vec<String>>>>, // topic -> matching rule IDs
    compiled_regexes: Arc<RwLock<HashMap<String, Regex>>>,
    routing_stats: Arc<RwLock<RoutingStats>>,
}

/// Routing statistics for monitoring
#[derive(Debug, Default, Clone)]
pub struct RoutingStats {
    pub total_messages_routed: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub routing_time_ns: u64,
    pub pattern_match_time_ns: u64,
    pub routes_per_message: Vec<usize>, // Histogram of routes per message
}

impl IntelligentMessageRouter {
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
            topic_cache: Arc::new(RwLock::new(HashMap::new())),
            compiled_regexes: Arc::new(RwLock::new(HashMap::new())),
            routing_stats: Arc::new(RwLock::new(RoutingStats::default())),
        }
    }
    
    /// Add a routing rule
    pub fn add_route(&self, rule: RoutingRule) -> Result<(), RoutingError> {
        let mut routes = self.routes.write();
        
        // Pre-compile regex patterns
        if let RoutingPattern::Regex(ref pattern) = rule.pattern {
            let regex = Regex::new(pattern)
                .map_err(|_| RoutingError::InvalidPattern(pattern.clone()))?;
            self.compiled_regexes.write().insert(rule.id.clone(), regex);
        }
        
        info!("Added routing rule: {} -> {:?}", rule.id, rule.pattern);
        routes.insert(rule.id.clone(), rule);
        
        // Clear cache since routing rules changed
        self.topic_cache.write().clear();
        
        Ok(())
    }
    
    /// Remove a routing rule
    pub fn remove_route(&self, rule_id: &str) -> bool {
        let mut routes = self.routes.write();
        let removed = routes.remove(rule_id).is_some();
        
        if removed {
            self.compiled_regexes.write().remove(rule_id);
            self.topic_cache.write().clear();
            info!("Removed routing rule: {}", rule_id);
        }
        
        removed
    }
    
    /// Route a message to appropriate subscribers
    pub fn route_message(&self, topic: &str, priority: u8, region: Option<&str>) -> Vec<u64> {
        let start_time = std::time::Instant::now();
        
        // Check cache first
        let cached_rules = {
            let cache = self.topic_cache.read();
            cache.get(topic).cloned()
        };
        
        let matching_rule_ids = if let Some(rule_ids) = cached_rules {
            self.routing_stats.write().cache_hits += 1;
            rule_ids
        } else {
            self.routing_stats.write().cache_misses += 1;
            let rule_ids = self.find_matching_rules(topic, priority, region);
            
            // Cache the result
            self.topic_cache.write().insert(topic.to_string(), rule_ids.clone());
            rule_ids
        };
        
        // Collect all target subscribers from matching rules
        let routes = self.routes.read();
        let mut target_subscribers = HashSet::new();
        
        for rule_id in &matching_rule_ids {
            if let Some(rule) = routes.get(rule_id) {
                if rule.enabled {
                    target_subscribers.extend(&rule.target_subscribers);
                }
            }
        }
        
        let routing_time = start_time.elapsed();
        
        // Update statistics
        {
            let mut stats = self.routing_stats.write();
            stats.total_messages_routed += 1;
            stats.routing_time_ns += routing_time.as_nanos() as u64;
            stats.routes_per_message.push(matching_rule_ids.len());
            
            // Keep histogram limited to last 1000 entries
            if stats.routes_per_message.len() > 1000 {
                stats.routes_per_message.remove(0);
            }
        }
        
        debug!("Routed message '{}' to {} subscribers using {} rules in {}ns", 
              topic, target_subscribers.len(), matching_rule_ids.len(), routing_time.as_nanos());
        
        target_subscribers.into_iter().collect()
    }
    
    /// Find rules that match the given topic and conditions
    fn find_matching_rules(&self, topic: &str, priority: u8, region: Option<&str>) -> Vec<String> {
        let pattern_start = std::time::Instant::now();
        let routes = self.routes.read();
        let regexes = self.compiled_regexes.read();
        let mut matching_rules = Vec::new();
        
        for (rule_id, rule) in routes.iter() {
            if !rule.enabled {
                continue;
            }
            
            let matches = match &rule.pattern {
                RoutingPattern::Exact(pattern) => topic == pattern,
                
                RoutingPattern::Wildcard(pattern) => {
                    WildMatch::new(pattern).matches(topic)
                },
                
                RoutingPattern::Regex(_) => {
                    if let Some(regex) = regexes.get(rule_id) {
                        regex.is_match(topic)
                    } else {
                        false
                    }
                },
                
                RoutingPattern::HashBased { pattern, partition_count } => {
                    if WildMatch::new(pattern).matches(topic) {
                        let hash = fxhash::hash(topic.as_bytes());
                        (hash as usize % partition_count) == 0 // Simple partitioning
                    } else {
                        false
                    }
                },
                
                RoutingPattern::Geographic { regions } => {
                    if let Some(msg_region) = region {
                        regions.contains(&msg_region.to_string())
                    } else {
                        false
                    }
                },
                
                RoutingPattern::Priority { min_priority, max_priority } => {
                    priority >= *min_priority && priority <= *max_priority
                },
            };
            
            if matches {
                matching_rules.push(rule_id.clone());
            }
        }
        
        let pattern_time = pattern_start.elapsed();
        self.routing_stats.write().pattern_match_time_ns += pattern_time.as_nanos() as u64;
        
        matching_rules
    }
    
    /// Get routing statistics
    pub fn get_stats(&self) -> RoutingStats {
        let stats = self.routing_stats.read();
        RoutingStats {
            total_messages_routed: stats.total_messages_routed,
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            routing_time_ns: stats.routing_time_ns,
            pattern_match_time_ns: stats.pattern_match_time_ns,
            routes_per_message: stats.routes_per_message.clone(),
        }
    }
    
    /// Clear routing cache
    pub fn clear_cache(&self) {
        self.topic_cache.write().clear();
        info!("Routing cache cleared");
    }
    
    /// Get all active routes
    pub fn get_routes(&self) -> Vec<RoutingRule> {
        self.routes.read().values().cloned().collect()
    }
    
    /// Update subscriber list for a route
    pub fn update_route_subscribers(&self, rule_id: &str, subscribers: HashSet<u64>) -> bool {
        let mut routes = self.routes.write();
        
        if let Some(rule) = routes.get_mut(rule_id) {
            rule.target_subscribers = subscribers;
            self.topic_cache.write().clear(); // Clear cache
            true
        } else {
            false
        }
    }
    
    /// Enable or disable a route
    pub fn set_route_enabled(&self, rule_id: &str, enabled: bool) -> bool {
        let mut routes = self.routes.write();
        
        if let Some(rule) = routes.get_mut(rule_id) {
            rule.enabled = enabled;
            self.topic_cache.write().clear(); // Clear cache
            info!("Route {} {}", rule_id, if enabled { "enabled" } else { "disabled" });
            true
        } else {
            false
        }
    }
    
    /// Bulk route update for multiple topics
    pub fn route_messages_bulk(&self, messages: &[(String, u8, Option<String>)]) -> HashMap<String, Vec<u64>> {
        let mut results = HashMap::new();
        
        for (topic, priority, region) in messages {
            let subscribers = self.route_message(topic, *priority, region.as_deref());
            results.insert(topic.clone(), subscribers);
        }
        
        results
    }
}

/// Topic subscription manager with advanced filtering
pub struct TopicSubscriptionManager {
    subscriptions: Arc<RwLock<HashMap<u64, Vec<TopicSubscription>>>>, // subscriber_id -> subscriptions
    topic_to_subscribers: Arc<RwLock<HashMap<String, HashSet<u64>>>>, // topic -> subscriber_ids
    pattern_subscriptions: Arc<RwLock<Vec<PatternSubscription>>>,
}

/// Topic subscription with filtering
#[derive(Debug, Clone)]
pub struct TopicSubscription {
    pub topic_pattern: String,
    pub filters: Vec<MessageFilter>,
    pub metadata: HashMap<String, String>,
}

/// Pattern-based subscription
#[derive(Debug, Clone)]
pub struct PatternSubscription {
    pub subscriber_id: u64,
    pub pattern: RoutingPattern,
    pub filters: Vec<MessageFilter>,
}

/// Message filters for advanced subscription management
#[derive(Debug, Clone)]
pub enum MessageFilter {
    /// Filter by message priority
    Priority { min: u8, max: u8 },
    /// Filter by message size
    SizeLimit { max_bytes: usize },
    /// Filter by geographic region
    Region(String),
    /// Filter by custom metadata
    Metadata { key: String, value: String },
    /// Rate limiting filter
    RateLimit { max_per_second: u64 },
}

impl TopicSubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            topic_to_subscribers: Arc::new(RwLock::new(HashMap::new())),
            pattern_subscriptions: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    /// Subscribe to a topic with optional filters
    pub fn subscribe(&self, subscriber_id: u64, subscription: TopicSubscription) {
        let mut subscriptions = self.subscriptions.write();
        let mut topic_map = self.topic_to_subscribers.write();
        
        // Add to subscriber's subscription list
        subscriptions.entry(subscriber_id)
            .or_insert_with(Vec::new)
            .push(subscription.clone());
        
        // Add to topic -> subscribers mapping
        topic_map.entry(subscription.topic_pattern.clone())
            .or_insert_with(HashSet::new)
            .insert(subscriber_id);
        
        info!("Subscriber {} subscribed to topic '{}'", subscriber_id, subscription.topic_pattern);
    }
    
    /// Unsubscribe from a topic
    pub fn unsubscribe(&self, subscriber_id: u64, topic_pattern: &str) -> bool {
        let mut subscriptions = self.subscriptions.write();
        let mut topic_map = self.topic_to_subscribers.write();
        
        // Remove from subscriber's subscription list
        if let Some(sub_list) = subscriptions.get_mut(&subscriber_id) {
            sub_list.retain(|sub| sub.topic_pattern != topic_pattern);
            if sub_list.is_empty() {
                subscriptions.remove(&subscriber_id);
            }
        }
        
        // Remove from topic -> subscribers mapping
        if let Some(subscribers) = topic_map.get_mut(topic_pattern) {
            subscribers.remove(&subscriber_id);
            if subscribers.is_empty() {
                topic_map.remove(topic_pattern);
            }
        }
        
        info!("Subscriber {} unsubscribed from topic '{}'", subscriber_id, topic_pattern);
        true
    }
    
    /// Get subscribers for a topic
    pub fn get_subscribers(&self, topic: &str) -> Vec<u64> {
        let topic_map = self.topic_to_subscribers.read();
        topic_map.get(topic).cloned().unwrap_or_default().into_iter().collect()
    }
    
    /// Get all subscriptions for a subscriber
    pub fn get_subscriptions(&self, subscriber_id: u64) -> Vec<TopicSubscription> {
        let subscriptions = self.subscriptions.read();
        subscriptions.get(&subscriber_id).cloned().unwrap_or_default()
    }
}

/// Routing errors
#[derive(Debug)]
pub enum RoutingError {
    InvalidPattern(String),
    RuleNotFound(String),
    CacheError,
}

impl std::fmt::Display for RoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::InvalidPattern(pattern) => write!(f, "Invalid routing pattern: {}", pattern),
            RoutingError::RuleNotFound(id) => write!(f, "Routing rule not found: {}", id),
            RoutingError::CacheError => write!(f, "Routing cache error"),
        }
    }
}

impl std::error::Error for RoutingError {}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_exact_routing() {
        let router = IntelligentMessageRouter::new();
        let mut subscribers = HashSet::new();
        subscribers.insert(123);
        
        let rule = RoutingRule {
            id: "exact_rule".to_string(),
            pattern: RoutingPattern::Exact("orders.btcusd".to_string()),
            target_subscribers: subscribers,
            enabled: true,
            metadata: HashMap::new(),
        };
        
        router.add_route(rule).unwrap();
        
        let result = router.route_message("orders.btcusd", 1, None);
        assert_eq!(result, vec![123]);
        
        let no_match = router.route_message("orders.ethusd", 1, None);
        assert!(no_match.is_empty());
    }
    
    #[test]
    fn test_wildcard_routing() {
        let router = IntelligentMessageRouter::new();
        let mut subscribers = HashSet::new();
        subscribers.insert(456);
        
        let rule = RoutingRule {
            id: "wildcard_rule".to_string(),
            pattern: RoutingPattern::Wildcard("orders.*".to_string()),
            target_subscribers: subscribers,
            enabled: true,
            metadata: HashMap::new(),
        };
        
        router.add_route(rule).unwrap();
        
        let result1 = router.route_message("orders.btcusd", 1, None);
        assert_eq!(result1, vec![456]);
        
        let result2 = router.route_message("orders.ethusd", 1, None);
        assert_eq!(result2, vec![456]);
        
        let no_match = router.route_message("trades.btcusd", 1, None);
        assert!(no_match.is_empty());
    }
    
    #[test]
    fn test_priority_routing() {
        let router = IntelligentMessageRouter::new();
        let mut subscribers = HashSet::new();
        subscribers.insert(789);
        
        let rule = RoutingRule {
            id: "priority_rule".to_string(),
            pattern: RoutingPattern::Priority { min_priority: 5, max_priority: 10 },
            target_subscribers: subscribers,
            enabled: true,
            metadata: HashMap::new(),
        };
        
        router.add_route(rule).unwrap();
        
        let high_priority = router.route_message("any.topic", 7, None);
        assert_eq!(high_priority, vec![789]);
        
        let low_priority = router.route_message("any.topic", 3, None);
        assert!(low_priority.is_empty());
    }
    
    #[test]
    fn test_subscription_manager() {
        let manager = TopicSubscriptionManager::new();
        
        let subscription = TopicSubscription {
            topic_pattern: "orders.btc".to_string(),
            filters: vec![MessageFilter::Priority { min: 1, max: 10 }],
            metadata: HashMap::new(),
        };
        
        manager.subscribe(123, subscription);
        
        let subscribers = manager.get_subscribers("orders.btc");
        assert_eq!(subscribers, vec![123]);
        
        let subscriptions = manager.get_subscriptions(123);
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].topic_pattern, "orders.btc");
        
        manager.unsubscribe(123, "orders.btc");
        let empty_subscribers = manager.get_subscribers("orders.btc");
        assert!(empty_subscribers.is_empty());
    }
    
    #[test]
    fn test_routing_cache() {
        let router = IntelligentMessageRouter::new();
        let mut subscribers = HashSet::new();
        subscribers.insert(999);
        
        let rule = RoutingRule {
            id: "cache_rule".to_string(),
            pattern: RoutingPattern::Exact("cached.topic".to_string()),
            target_subscribers: subscribers,
            enabled: true,
            metadata: HashMap::new(),
        };
        
        router.add_route(rule).unwrap();
        
        // First call should be cache miss
        let result1 = router.route_message("cached.topic", 1, None);
        assert_eq!(result1, vec![999]);
        
        // Second call should be cache hit
        let result2 = router.route_message("cached.topic", 1, None);
        assert_eq!(result2, vec![999]);
        
        let stats = router.get_stats();
        assert!(stats.cache_hits > 0);
        assert!(stats.cache_misses > 0);
    }
}
