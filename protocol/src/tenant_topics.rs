//! Tenant Topic Namespacing
//!
//! Provides topic naming conventions for multi-tenant isolation.
//! Topics follow the pattern: `tenant.{tenant_id}.{base_topic}`
//!
//! This is a zero-overhead convention (string prefix) that leverages
//! the existing flat topic system without modifying the broker core.
//!
//! ## Examples
//! ```
//! use protocol::tenant_topics;
//!
//! let topic = tenant_topics::tenant_topic("abc-123", "strategy.deployment");
//! assert_eq!(topic, "tenant.abc-123.strategy.deployment");
//!
//! let (tid, base) = tenant_topics::parse_tenant_topic(&topic).unwrap();
//! assert_eq!(tid, "abc-123");
//! assert_eq!(base, "strategy.deployment");
//! ```
//!
//! ## Standard Topic Names
//!
//! These are the conventional topic names for a strategy-runtime system built
//! on this broker: one service deploys/manages strategies (the "orchestrator"),
//! another runs them and streams market data in (the "runtime"), and a data
//! feed service supplies market data on demand.
//!
//! | Base Topic | Direction | Description |
//! |------------|-----------|-------------|
//! | `strategy.deployment` | orchestrator → runtime | New strategy deployed |
//! | `strategy.deactivation` | orchestrator → runtime | Strategy stopped/paused |
//! | `strategy.deployment.ack` | runtime → orchestrator | Ack from the strategy runtime |
//! | `strategy.status.request` | orchestrator → runtime | Status query |
//! | `strategy.status.response` | runtime → orchestrator | Status response |
//! | `market.subscription.subscribe` | runtime → data feed | Request market data |
//! | `market.subscription.unsubscribe` | runtime → data feed | Cancel market data |
//! | `market.data.{exchange}.{symbol}` | data feed → runtime | Market data stream |

use uuid::Uuid;

/// Prefix for all tenant-namespaced topics
const TENANT_PREFIX: &str = "tenant.";

/// Create a tenant-namespaced topic string.
///
/// Format: `tenant.{tenant_id}.{base_topic}`
///
/// # Arguments
/// * `tenant_id` - Tenant UUID (will be formatted without hyphens for compact topics)
/// * `base_topic` - Base topic name (e.g., "strategy.deployment")
#[inline]
pub fn tenant_topic(tenant_id: &str, base_topic: &str) -> String {
    format!("{}{}.{}", TENANT_PREFIX, tenant_id, base_topic)
}

/// Create a tenant-namespaced topic from a UUID.
#[inline]
pub fn tenant_topic_uuid(tenant_id: Uuid, base_topic: &str) -> String {
    // Use simple hex format (no hyphens) for shorter topic names
    format!("{}{}.{}", TENANT_PREFIX, tenant_id.simple(), base_topic)
}

/// Parse a tenant topic into (tenant_id, base_topic).
///
/// Returns `None` if the topic doesn't follow the `tenant.{id}.{topic}` convention.
pub fn parse_tenant_topic(topic: &str) -> Option<(&str, &str)> {
    let rest = topic.strip_prefix(TENANT_PREFIX)?;
    let dot_pos = rest.find('.')?;
    let tenant_id = &rest[..dot_pos];
    let base_topic = &rest[dot_pos + 1..];
    if tenant_id.is_empty() || base_topic.is_empty() {
        return None;
    }
    Some((tenant_id, base_topic))
}

/// Check if a topic belongs to a specific tenant.
#[inline]
pub fn is_tenant_topic(topic: &str, tenant_id: &str) -> bool {
    topic.starts_with(&format!("{}{}", TENANT_PREFIX, tenant_id))
}

/// Check if a topic is a global (non-tenant) topic.
#[inline]
pub fn is_global_topic(topic: &str) -> bool {
    !topic.starts_with(TENANT_PREFIX)
}

/// Create a wildcard subscription pattern for all topics of a tenant.
///
/// Useful for admin monitoring or tenant cleanup.
/// Format: `tenant.{tenant_id}.*`
#[inline]
pub fn tenant_wildcard(tenant_id: &str) -> String {
    format!("{}{}.*", TENANT_PREFIX, tenant_id)
}

// ============================================================================
// Standard Topic Constants
// ============================================================================

pub mod topics {
    //! Standard base topic names for a strategy-deployment/market-data system
    //! built on this broker. See the module-level doc table for the full
    //! producer/consumer direction of each topic.

    /// Strategy deployment events (orchestrator → runtime)
    pub const STRATEGY_DEPLOYMENT: &str = "strategy.deployment";

    /// Strategy deactivation events (orchestrator → runtime)
    pub const STRATEGY_DEACTIVATION: &str = "strategy.deactivation";

    /// Deployment acknowledgments (runtime → orchestrator)
    pub const DEPLOYMENT_ACK: &str = "strategy.deployment.ack";

    /// Deployment status request (orchestrator → runtime)
    pub const STATUS_REQUEST: &str = "strategy.status.request";

    /// Deployment status response (runtime → orchestrator)
    pub const STATUS_RESPONSE: &str = "strategy.status.response";

    /// Market data subscription request (runtime → data feed)
    pub const MARKET_SUBSCRIBE: &str = "market.subscription.subscribe";

    /// Market data unsubscription (runtime → data feed)
    pub const MARKET_UNSUBSCRIBE: &str = "market.subscription.unsubscribe";

    /// Market data subscription acknowledgment (data feed → runtime)
    pub const MARKET_ACK: &str = "market.subscription.ack";

    /// Format a market data stream topic for a specific exchange and symbol.
    ///
    /// Returns: `market.data.{exchange}.{symbol}`
    pub fn market_data(exchange: &str, symbol: &str) -> String {
        format!("market.data.{}.{}", exchange, symbol.replace('/', "_"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_topic() {
        let topic = tenant_topic("abc-123", "strategy.deployment");
        assert_eq!(topic, "tenant.abc-123.strategy.deployment");
    }

    #[test]
    fn test_tenant_topic_uuid() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let topic = tenant_topic_uuid(id, "strategy.deployment");
        assert_eq!(topic, "tenant.550e8400e29b41d4a716446655440000.strategy.deployment");
    }

    #[test]
    fn test_parse_tenant_topic() {
        let (tid, base) = parse_tenant_topic("tenant.abc-123.strategy.deployment").unwrap();
        assert_eq!(tid, "abc-123");
        assert_eq!(base, "strategy.deployment");
    }

    #[test]
    fn test_parse_non_tenant_topic() {
        assert!(parse_tenant_topic("global.orders").is_none());
        assert!(parse_tenant_topic("tenant.").is_none());
        assert!(parse_tenant_topic("tenant..foo").is_none());
    }

    #[test]
    fn test_is_tenant_topic() {
        assert!(is_tenant_topic("tenant.abc.strategy.deployment", "abc"));
        assert!(!is_tenant_topic("tenant.abc.strategy.deployment", "xyz"));
        assert!(!is_tenant_topic("global.orders", "abc"));
    }

    #[test]
    fn test_is_global_topic() {
        assert!(is_global_topic("orders"));
        assert!(is_global_topic("market.data.kraken.xbt_usd"));
        assert!(!is_global_topic("tenant.abc.orders"));
    }

    #[test]
    fn test_market_data_topic() {
        assert_eq!(topics::market_data("kraken", "XBT/USD"), "market.data.kraken.XBT_USD");
    }
}
