use serde::{Deserialize, Serialize};
use prost::Message;

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Order {
    #[prost(string, tag = "1")]
    pub unique_id: String,
    #[prost(string, tag = "2")]
    pub symbol: String,
    #[prost(string, tag = "3")]
    pub exchange: String,
    #[prost(float, tag = "7")]
    pub price_level: f32,
    #[prost(float, tag = "8")]
    pub quantity: f32,
    #[prost(string, tag = "9")]
    pub side: String,
    #[prost(string, tag = "10")]
    pub event: String,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Orders {
    #[prost(message, repeated, tag = "1")]
    pub orders: Vec<Order>,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Trade {
    #[prost(string, tag = "1")]
    pub symbol: String,
    #[prost(string, tag = "2")]
    pub exchange: String,
    #[prost(float, tag = "3")]
    pub price: f32,
    #[prost(float, tag = "4")]
    pub quantity: f32,
    #[prost(float, tag = "5")]
    pub qty: f32,
    #[prost(string, tag = "6")]
    pub side: String,
    #[prost(int64, tag = "7")]
    pub timestamp: i64,
    #[prost(string, tag = "8")]
    pub trade_id: String,
    #[prost(string, tag = "9")]
    pub ord_type: String,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Trades {
    #[prost(message, repeated, tag = "1")]
    pub trades: Vec<Trade>,
}

/// A completed OHLCV bar built from accumulated trade ticks.
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Bar {
    #[prost(string, tag = "1")]
    pub symbol: String,
    #[prost(string, tag = "2")]
    pub exchange: String,
    #[prost(double, tag = "3")]
    pub open: f64,
    #[prost(double, tag = "4")]
    pub high: f64,
    #[prost(double, tag = "5")]
    pub low: f64,
    #[prost(double, tag = "6")]
    pub close: f64,
    #[prost(double, tag = "7")]
    pub volume: f64,
    /// Start of the bar window (Unix ms)
    #[prost(int64, tag = "8")]
    pub bar_start_ms: i64,
    /// End of the bar window (Unix ms)
    #[prost(int64, tag = "9")]
    pub bar_end_ms: i64,
    #[prost(int32, tag = "10")]
    pub trade_count: i32,
    /// Bar resolution in seconds (e.g. 1 for 1-second bars)
    #[prost(int32, tag = "11")]
    pub interval_secs: i32,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Bars {
    #[prost(message, repeated, tag = "1")]
    pub bars: Vec<Bar>,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Quote {
    #[prost(string, tag = "1")]
    pub symbol: String,
    #[prost(string, tag = "2")]
    pub exchange: String,
    #[prost(float, tag = "3")]
    pub bid_price: f32,
    #[prost(float, tag = "4")]
    pub ask_price: f32,
    #[prost(float, tag = "5")]
    pub bid_quantity: f32,
    #[prost(float, tag = "6")]
    pub ask_quantity: f32,
    #[prost(int64, tag = "7")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct MarketData {
    #[prost(message, optional, tag = "1")]
    pub trade: Option<Trade>,
    #[prost(message, optional, tag = "2")]
    pub quote: Option<Quote>,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct ExecutionReport {
    #[prost(string, tag = "1")]
    pub order_id: String,
    #[prost(string, tag = "2")]
    pub execution_id: String,
    #[prost(string, tag = "3")]
    pub symbol: String,
    #[prost(string, tag = "4")]
    pub side: String,
    #[prost(float, tag = "5")]
    pub executed_price: f32,
    #[prost(float, tag = "6")]
    pub executed_quantity: f32,
    #[prost(float, tag = "7")]
    pub remaining_quantity: f32,
    #[prost(string, tag = "8")]
    pub status: String,
    #[prost(int64, tag = "9")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct RiskAlert {
    #[prost(string, tag = "1")]
    pub alert_id: String,
    #[prost(string, tag = "2")]
    pub alert_type: String,
    #[prost(string, tag = "3")]
    pub message: String,
    #[prost(string, tag = "4")]
    pub severity: String,
    #[prost(string, tag = "5")]
    pub affected_symbol: String,
    #[prost(int64, tag = "6")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct SystemStatus {
    #[prost(string, tag = "1")]
    pub component: String,
    #[prost(string, tag = "2")]
    pub status: String,
    #[prost(string, tag = "3")]
    pub message: String,
    #[prost(int64, tag = "4")]
    pub timestamp: i64,
    #[prost(float, tag = "5")]
    pub cpu_usage: f32,
    #[prost(float, tag = "6")]
    pub memory_usage: f32,
    #[prost(int32, tag = "7")]
    pub active_connections: i32,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct PublishRequest {
    #[prost(string, tag = "1")]
    pub topic: String,
    #[prost(oneof = "publish_request::Payload", tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 25, 26, 27, 28, 29, 30, 31, 32")]
    pub payload: Option<publish_request::Payload>,
}

pub mod publish_request {
    use super::*;

    #[derive(Clone, PartialEq, prost::Oneof, Serialize, Deserialize)]
    pub enum Payload {
        #[prost(message, tag = "2")]
        Order(Order),
        #[prost(message, tag = "3")]
        Trade(Trade),
        #[prost(message, tag = "4")]
        Quote(Quote),
        #[prost(message, tag = "5")]
        ExecutionReport(ExecutionReport),
        #[prost(message, tag = "6")]
        RiskAlert(RiskAlert),
        #[prost(message, tag = "7")]
        SystemStatus(SystemStatus),
        #[prost(bytes, tag = "8")]
        RawData(Vec<u8>),
        #[prost(message, tag = "9")]
        PortfolioPayload(PortfolioMessage),
        #[prost(message, tag = "10")]
        MarketPayload(MarketMessage),
        // Strategy deployment events
        #[prost(message, tag = "11")]
        StrategyDeployment(StrategyDeployment),
        #[prost(message, tag = "12")]
        StrategyDeactivation(StrategyDeactivation),
        #[prost(message, tag = "13")]
        StrategyDeploymentAck(StrategyDeploymentAck),
        #[prost(message, tag = "14")]
        DeploymentStatusResponse(DeploymentStatusResponse),
        #[prost(message, tag = "25")]
        DeploymentStatusRequest(DeploymentStatusRequest),
        // Market data subscription events
        #[prost(message, tag = "26")]
        MarketDataSubscribe(MarketDataSubscribe),
        #[prost(message, tag = "27")]
        MarketDataUnsubscribe(MarketDataUnsubscribe),
        #[prost(message, tag = "28")]
        MarketDataSubscriptionAck(MarketDataSubscriptionAck),
        #[prost(message, tag = "29")]
        MarketDataStatusRequest(MarketDataStatusRequest),
        #[prost(message, tag = "30")]
        MarketDataStatusResponse(MarketDataStatusResponse),
        // Live bar aggregation events
        #[prost(message, tag = "31")]
        Bar(Bar),
        #[prost(message, tag = "32")]
        Bars(Bars),
    }
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Wallet {
    #[prost(string, tag = "1")]
    pub user_id: String,
    #[prost(string, tag = "2")]
    pub symbol: String,
    #[prost(float, tag = "3")]
    pub balance: f32,
    #[prost(string, tag = "4")]
    pub currency: String,
    #[prost(int64, tag = "5")]
    pub last_updated: i64,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Wallets {
    #[prost(message, repeated, tag = "1")]
    pub wallets: Vec<Wallet>,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct PortfolioMessage {
    #[prost(string, tag = "1")]
    pub portfolio_id: String,
    #[prost(oneof = "portfolio_message::Payload", tags = "2, 3, 4, 5, 6")]
    pub payload: Option<portfolio_message::Payload>,
}

pub mod portfolio_message {
    use super::*;

    #[derive(Clone, PartialEq, prost::Oneof, Serialize, Deserialize)]
    pub enum Payload {
        #[prost(message, tag = "2")]
        Position(Wallet),
        #[prost(message, tag = "3")]
        Balance(Wallet),
        #[prost(message, tag = "4")]
        Update(ExecutionReport),
        #[prost(message, tag = "5")]
        Risk(RiskAlert),
        #[prost(message, tag = "6")]
        WalletsPayload(WalletData),
    }
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct WalletData {
    #[prost(string, tag = "1")]
    pub exchange: String,
    #[prost(message, repeated, tag = "2")]
    pub wallets: Vec<Wallet>,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct MarketMessage {
    #[prost(string, tag = "1")]
    pub market_id: String,
    #[prost(oneof = "market_message::Payload", tags = "2, 3, 4, 5, 6, 7")]
    pub payload: Option<market_message::Payload>,
}

pub mod market_message {
    use super::*;

    #[derive(Clone, PartialEq, prost::Oneof, Serialize, Deserialize)]
    pub enum Payload {
        #[prost(message, tag = "2")]
        Trade(Trade),
        #[prost(message, tag = "3")]
        Quote(Quote),
        #[prost(message, tag = "4")]
        MarketData(MarketData),
        #[prost(message, tag = "5")]
        OrdersPayload(Orders),
        #[prost(message, tag = "6")]
        TradesPayload(Trades),
        /// Completed OHLCV bar from tick accumulation
        #[prost(message, tag = "7")]
        Bar(Bar),
    }
}

// ============================================================================
// STRATEGY DEPLOYMENT MESSAGES
// ============================================================================
// These messages enable real-time deployment notifications from a strategy
// orchestrator to a strategy runtime -- the runtime subscribes to deployment
// events and hot-loads/unloads strategies without restart, instead of polling
// a database.

/// Strategy deployment event - sent when a strategy is approved and ready to deploy.
/// The strategy runtime subscribes to topic: "strategy.deployment"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct StrategyDeployment {
    /// Strategy definition ID
    #[prost(string, tag = "1")]
    pub strategy_id: String,
    /// Specific instance being deployed
    #[prost(string, tag = "2")]
    pub instance_id: String,
    /// Tenant ID for multi-tenancy isolation
    #[prost(string, tag = "3")]
    pub tenant_id: String,
    /// Strategy type (e.g., "AvellanedaStoikov", "UltraFastMomentum")
    #[prost(string, tag = "4")]
    pub strategy_type: String,
    /// Human-readable strategy name
    #[prost(string, tag = "5")]
    pub strategy_name: String,
    /// Strategy version
    #[prost(string, tag = "6")]
    pub version: String,
    /// Serialized strategy parameters (JSON)
    #[prost(bytes, tag = "7")]
    pub parameters: Vec<u8>,
    /// Initial capital allocated to this strategy
    #[prost(double, tag = "8")]
    pub initial_capital: f64,
    /// Target exchanges for execution
    #[prost(string, repeated, tag = "9")]
    pub target_exchanges: Vec<String>,
    /// Trading symbols this strategy trades
    #[prost(string, repeated, tag = "10")]
    pub symbols: Vec<String>,
    /// User who approved the deployment
    #[prost(string, tag = "11")]
    pub approved_by: String,
    /// ISO 8601 timestamp when approved
    #[prost(string, tag = "12")]
    pub approved_at: String,
    /// Performance summary from backtesting (JSON)
    #[prost(bytes, tag = "13")]
    pub performance_summary: Vec<u8>,
    /// Risk metrics from backtesting (JSON)
    #[prost(bytes, tag = "14")]
    pub risk_metrics: Vec<u8>,
    /// Whether this is a high-priority deployment (admin approved)
    #[prost(bool, tag = "15")]
    pub admin_approved: bool,
    /// Deployment timestamp
    #[prost(int64, tag = "16")]
    pub timestamp: i64,
    /// Deployment mode: "paper" or "live"
    #[prost(string, tag = "17")]
    pub mode: String,
}

/// Strategy deactivation event - sent when a strategy is deactivated.
/// The strategy runtime subscribes to topic: "strategy.deactivation"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct StrategyDeactivation {
    /// Strategy definition ID
    #[prost(string, tag = "1")]
    pub strategy_id: String,
    /// Specific instance being deactivated
    #[prost(string, tag = "2")]
    pub instance_id: String,
    /// Tenant ID for multi-tenancy isolation
    #[prost(string, tag = "3")]
    pub tenant_id: String,
    /// Reason for deactivation
    #[prost(string, tag = "4")]
    pub reason: String,
    /// User who initiated deactivation
    #[prost(string, tag = "5")]
    pub deactivated_by: String,
    /// Whether to close open positions immediately
    #[prost(bool, tag = "6")]
    pub close_positions: bool,
    /// Whether to cancel pending orders immediately
    #[prost(bool, tag = "7")]
    pub cancel_orders: bool,
    /// Deactivation timestamp
    #[prost(int64, tag = "8")]
    pub timestamp: i64,
}

/// Strategy deployment acknowledgment - sent by the strategy runtime after processing.
/// The orchestrator subscribes to topic: "strategy.deployment.ack"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct StrategyDeploymentAck {
    /// Strategy definition ID
    #[prost(string, tag = "1")]
    pub strategy_id: String,
    /// Specific instance that was deployed
    #[prost(string, tag = "2")]
    pub instance_id: String,
    /// Runtime node that deployed the strategy
    #[prost(string, tag = "3")]
    pub signal_engine_node: String,
    /// Whether deployment succeeded
    #[prost(bool, tag = "4")]
    pub success: bool,
    /// Error message if deployment failed
    #[prost(string, tag = "5")]
    pub error_message: String,
    /// Timestamp when strategy was loaded
    #[prost(int64, tag = "6")]
    pub loaded_at: i64,
    /// Exchanges where strategy is now active
    #[prost(string, repeated, tag = "7")]
    pub active_exchanges: Vec<String>,
}

/// Bulk deployment status request - get status of all deployed strategies.
/// The strategy runtime subscribes to topic: "strategy.status.request"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct DeploymentStatusRequest {
    /// Tenant ID to filter by (empty for all)
    #[prost(string, tag = "1")]
    pub tenant_id: String,
    /// Request ID for correlation
    #[prost(string, tag = "2")]
    pub request_id: String,
}

/// Deployed strategy status - response with all active strategies.
/// Published to topic: "strategy.status.response"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct DeploymentStatusResponse {
    /// Request ID for correlation
    #[prost(string, tag = "1")]
    pub request_id: String,
    /// Runtime node responding
    #[prost(string, tag = "2")]
    pub signal_engine_node: String,
    /// List of active strategies
    #[prost(message, repeated, tag = "3")]
    pub active_strategies: Vec<ActiveStrategyInfo>,
    /// Total memory used by strategies (bytes)
    #[prost(int64, tag = "4")]
    pub total_memory_bytes: i64,
    /// Total CPU utilization (0.0 - 100.0)
    #[prost(float, tag = "5")]
    pub cpu_utilization: f32,
}

/// Information about an active strategy
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct ActiveStrategyInfo {
    /// Strategy definition ID
    #[prost(string, tag = "1")]
    pub strategy_id: String,
    /// Specific instance ID
    #[prost(string, tag = "2")]
    pub instance_id: String,
    /// Tenant ID
    #[prost(string, tag = "3")]
    pub tenant_id: String,
    /// Strategy type
    #[prost(string, tag = "4")]
    pub strategy_type: String,
    /// Strategy name
    #[prost(string, tag = "5")]
    pub strategy_name: String,
    /// Exchanges where active
    #[prost(string, repeated, tag = "6")]
    pub active_exchanges: Vec<String>,
    /// Symbols being traded
    #[prost(string, repeated, tag = "7")]
    pub symbols: Vec<String>,
    /// Current unrealized P&L
    #[prost(double, tag = "8")]
    pub unrealized_pnl: f64,
    /// Total realized P&L since deployment
    #[prost(double, tag = "9")]
    pub realized_pnl: f64,
    /// Number of open positions
    #[prost(int32, tag = "10")]
    pub open_positions: i32,
    /// Number of pending orders
    #[prost(int32, tag = "11")]
    pub pending_orders: i32,
    /// Timestamp when deployed
    #[prost(int64, tag = "12")]
    pub deployed_at: i64,
    /// Number of trades since deployment
    #[prost(int64, tag = "13")]
    pub total_trades: i64,
}

// ============================================================================
// Market Data Subscription Messages
// ============================================================================
//
// These messages enable demand-driven market data streaming. A data feed
// service only connects to exchange WebSockets when the strategy runtime has
// active strategies that need specific symbols/exchanges.
//
// Flow:
// 1. Strategy deployed -> runtime publishes MarketDataSubscribe
// 2. Data feed receives request -> connects to exchange WebSocket
// 3. Data feed streams data to the broker -> runtime receives it
// 4. Strategy deactivated -> runtime publishes MarketDataUnsubscribe
// 5. Data feed disconnects (if no other subscribers for that symbol)

/// Market data subscription request - published by the strategy runtime when
/// strategies need data. The data feed service subscribes to topic:
/// "market.subscription.subscribe"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct MarketDataSubscribe {
    /// Unique subscription ID for tracking
    #[prost(string, tag = "1")]
    pub subscription_id: String,
    /// Tenant ID making the request
    #[prost(string, tag = "2")]
    pub tenant_id: String,
    /// Strategy instance requesting the data
    #[prost(string, tag = "3")]
    pub strategy_instance_id: String,
    /// Exchange to connect to (e.g., "kraken", "coinbase", "binance_us")
    #[prost(string, tag = "4")]
    pub exchange: String,
    /// Symbols to subscribe to (e.g., ["XBT/USD", "ETH/USD"])
    #[prost(string, repeated, tag = "5")]
    pub symbols: Vec<String>,
    /// Data types needed (e.g., ["trades", "orderbook", "ticker"])
    #[prost(string, repeated, tag = "6")]
    pub data_types: Vec<String>,
    /// Order book depth (if orderbook data type requested)
    #[prost(int32, tag = "7")]
    pub orderbook_depth: i32,
    /// Timestamp of request
    #[prost(int64, tag = "8")]
    pub timestamp: i64,
}

/// Market data unsubscription request - published by the strategy runtime
/// when strategies stop. The data feed service subscribes to topic:
/// "market.subscription.unsubscribe"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct MarketDataUnsubscribe {
    /// Subscription ID to cancel (from original MarketDataSubscribe)
    #[prost(string, tag = "1")]
    pub subscription_id: String,
    /// Strategy instance no longer needing data
    #[prost(string, tag = "2")]
    pub strategy_instance_id: String,
    /// Exchange to potentially disconnect from
    #[prost(string, tag = "3")]
    pub exchange: String,
    /// Symbols to unsubscribe from
    #[prost(string, repeated, tag = "4")]
    pub symbols: Vec<String>,
    /// Reason for unsubscription
    #[prost(string, tag = "5")]
    pub reason: String,
    /// Timestamp of request
    #[prost(int64, tag = "6")]
    pub timestamp: i64,
}

/// Subscription acknowledgment - sent by the data feed service after processing a subscription.
/// The strategy runtime subscribes to topic: "market.subscription.ack"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct MarketDataSubscriptionAck {
    /// Subscription ID being acknowledged
    #[prost(string, tag = "1")]
    pub subscription_id: String,
    /// Whether subscription was successful
    #[prost(bool, tag = "2")]
    pub success: bool,
    /// Error message if failed
    #[prost(string, tag = "3")]
    pub error_message: String,
    /// Data feed node handling this subscription
    #[prost(string, tag = "4")]
    pub data_engine_node: String,
    /// Topics where data will be published
    #[prost(string, repeated, tag = "5")]
    pub data_topics: Vec<String>,
    /// Timestamp of acknowledgment
    #[prost(int64, tag = "6")]
    pub timestamp: i64,
}

/// Active subscriptions status request.
/// The data feed service subscribes to topic: "market.subscription.status.request"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct MarketDataStatusRequest {
    /// Request ID for correlation
    #[prost(string, tag = "1")]
    pub request_id: String,
    /// Filter by exchange (empty for all)
    #[prost(string, tag = "2")]
    pub exchange_filter: String,
}

/// Active subscriptions status response.
/// Published to topic: "market.subscription.status.response"
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct MarketDataStatusResponse {
    /// Request ID for correlation
    #[prost(string, tag = "1")]
    pub request_id: String,
    /// Data feed node responding
    #[prost(string, tag = "2")]
    pub data_engine_node: String,
    /// Active exchange connections
    #[prost(message, repeated, tag = "3")]
    pub active_connections: Vec<ExchangeConnectionInfo>,
    /// Total active subscriptions
    #[prost(int32, tag = "4")]
    pub total_subscriptions: i32,
}

/// Information about an active exchange connection
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct ExchangeConnectionInfo {
    /// Exchange name
    #[prost(string, tag = "1")]
    pub exchange: String,
    /// Whether WebSocket is connected
    #[prost(bool, tag = "2")]
    pub connected: bool,
    /// Symbols currently subscribed
    #[prost(string, repeated, tag = "3")]
    pub symbols: Vec<String>,
    /// Data types being streamed
    #[prost(string, repeated, tag = "4")]
    pub data_types: Vec<String>,
    /// Number of strategy instances using this connection
    #[prost(int32, tag = "5")]
    pub subscriber_count: i32,
    /// Timestamp when connection was established
    #[prost(int64, tag = "6")]
    pub connected_since: i64,
    /// Messages processed since connection
    #[prost(int64, tag = "7")]
    pub messages_processed: i64,
}
