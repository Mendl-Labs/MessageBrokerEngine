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
    #[prost(oneof = "publish_request::Payload", tags = "2, 3, 4, 5, 6, 7, 8")]
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
    #[prost(oneof = "market_message::Payload", tags = "2, 3, 4, 5, 6")]
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
    }
}

// ============================================================================
// DISTRIBUTED BACKTESTING MESSAGES
// ============================================================================

/// A chunk of work for distributed backtesting
/// Workers subscribe to these and process them independently
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct BacktestChunk {
    /// Unique identifier for the parent job
    #[prost(string, tag = "1")]
    pub job_id: String,
    /// Unique identifier for this chunk
    #[prost(int32, tag = "2")]
    pub chunk_id: i32,
    /// Total number of chunks for this job
    #[prost(int32, tag = "3")]
    pub total_chunks: i32,
    /// Start timestamp (ISO8601 format)
    #[prost(string, tag = "4")]
    pub start_time: String,
    /// End timestamp (ISO8601 format)
    #[prost(string, tag = "5")]
    pub end_time: String,
    /// Trading symbol (e.g., "BTCUSDT")
    #[prost(string, tag = "6")]
    pub symbol: String,
    /// Exchange name
    #[prost(string, tag = "7")]
    pub exchange: String,
    /// Serialized strategy configuration (JSON)
    #[prost(bytes, tag = "8")]
    pub strategy_config: Vec<u8>,
    /// Initial capital for this chunk
    #[prost(double, tag = "9")]
    pub initial_capital: f64,
    /// Starting position from previous chunk (for continuity)
    #[prost(double, tag = "10")]
    pub starting_position: f64,
    /// Starting cash from previous chunk
    #[prost(double, tag = "11")]
    pub starting_cash: f64,
    /// Whether this is a Monte Carlo simulation run
    #[prost(bool, tag = "12")]
    pub is_monte_carlo: bool,
    /// Monte Carlo run index (0 for non-MC runs)
    #[prost(int32, tag = "13")]
    pub monte_carlo_run: i32,
    /// Chunk processing priority (higher = process first)
    #[prost(int32, tag = "14")]
    pub priority: i32,
}

/// Result from processing a single chunk
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct BacktestChunkResult {
    /// Parent job ID
    #[prost(string, tag = "1")]
    pub job_id: String,
    /// Chunk ID that was processed
    #[prost(int32, tag = "2")]
    pub chunk_id: i32,
    /// Worker ID that processed this chunk
    #[prost(string, tag = "3")]
    pub worker_id: String,
    /// Whether processing succeeded
    #[prost(bool, tag = "4")]
    pub success: bool,
    /// Error message if failed
    #[prost(string, tag = "5")]
    pub error_message: String,
    /// Net PnL for this chunk
    #[prost(double, tag = "6")]
    pub net_pnl: f64,
    /// Gross profit for this chunk
    #[prost(double, tag = "7")]
    pub gross_profit: f64,
    /// Gross loss for this chunk
    #[prost(double, tag = "8")]
    pub gross_loss: f64,
    /// Number of trades executed
    #[prost(int32, tag = "9")]
    pub num_trades: i32,
    /// Number of winning trades
    #[prost(int32, tag = "10")]
    pub winning_trades: i32,
    /// Maximum drawdown observed
    #[prost(double, tag = "11")]
    pub max_drawdown: f64,
    /// Total transaction costs
    #[prost(double, tag = "12")]
    pub transaction_costs: f64,
    /// Ending position (for next chunk continuity)
    #[prost(double, tag = "13")]
    pub ending_position: f64,
    /// Ending cash (for next chunk continuity)
    #[prost(double, tag = "14")]
    pub ending_cash: f64,
    /// Daily returns (serialized as JSON for Sharpe calculation)
    #[prost(bytes, tag = "15")]
    pub daily_returns: Vec<u8>,
    /// Number of events processed
    #[prost(int64, tag = "16")]
    pub events_processed: i64,
    /// Processing duration in milliseconds
    #[prost(int64, tag = "17")]
    pub processing_duration_ms: i64,
    /// Peak memory usage in bytes
    #[prost(int64, tag = "18")]
    pub peak_memory_bytes: i64,
    /// Monte Carlo run index (for MC aggregation)
    #[prost(int32, tag = "19")]
    pub monte_carlo_run: i32,
}

/// Progress update for a distributed job
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct BacktestProgress {
    /// Job ID
    #[prost(string, tag = "1")]
    pub job_id: String,
    /// Number of chunks completed
    #[prost(int32, tag = "2")]
    pub chunks_completed: i32,
    /// Total chunks in job
    #[prost(int32, tag = "3")]
    pub total_chunks: i32,
    /// Percentage complete (0.0 - 100.0)
    #[prost(float, tag = "4")]
    pub percent_complete: f32,
    /// Current phase (chunking, processing, aggregating)
    #[prost(string, tag = "5")]
    pub phase: String,
    /// Estimated time remaining in seconds
    #[prost(int64, tag = "6")]
    pub estimated_remaining_secs: i64,
    /// Running PnL aggregate (from completed chunks)
    #[prost(double, tag = "7")]
    pub running_pnl: f64,
    /// Running trade count
    #[prost(int32, tag = "8")]
    pub running_trades: i32,
}

/// Request to cancel a distributed job
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct BacktestCancelRequest {
    /// Job ID to cancel
    #[prost(string, tag = "1")]
    pub job_id: String,
    /// Reason for cancellation
    #[prost(string, tag = "2")]
    pub reason: String,
}

/// Aggregated result from all chunks
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct BacktestAggregatedResult {
    /// Job ID
    #[prost(string, tag = "1")]
    pub job_id: String,
    /// Total net PnL
    #[prost(double, tag = "2")]
    pub total_net_pnl: f64,
    /// Total gross profit
    #[prost(double, tag = "3")]
    pub total_gross_profit: f64,
    /// Total gross loss
    #[prost(double, tag = "4")]
    pub total_gross_loss: f64,
    /// Total number of trades
    #[prost(int32, tag = "5")]
    pub total_trades: i32,
    /// Total winning trades
    #[prost(int32, tag = "6")]
    pub total_winning_trades: i32,
    /// Win rate (0.0 - 1.0)
    #[prost(float, tag = "7")]
    pub win_rate: f32,
    /// Profit factor
    #[prost(float, tag = "8")]
    pub profit_factor: f32,
    /// Maximum drawdown across all chunks
    #[prost(double, tag = "9")]
    pub max_drawdown: f64,
    /// Sharpe ratio (calculated from combined daily returns)
    #[prost(float, tag = "10")]
    pub sharpe_ratio: f32,
    /// Sortino ratio
    #[prost(float, tag = "11")]
    pub sortino_ratio: f32,
    /// Calmar ratio
    #[prost(float, tag = "12")]
    pub calmar_ratio: f32,
    /// Total transaction costs
    #[prost(double, tag = "13")]
    pub total_transaction_costs: f64,
    /// Total events processed
    #[prost(int64, tag = "14")]
    pub total_events: i64,
    /// Total processing time in milliseconds
    #[prost(int64, tag = "15")]
    pub total_processing_time_ms: i64,
    /// Number of workers used
    #[prost(int32, tag = "16")]
    pub num_workers: i32,
    /// Number of chunks processed
    #[prost(int32, tag = "17")]
    pub chunks_processed: i32,
    /// Number of chunks that failed
    #[prost(int32, tag = "18")]
    pub chunks_failed: i32,
}

// ============================================================================
// DISTRIBUTED GENETIC ALGORITHM MESSAGES
// ============================================================================

/// Request to evaluate a chromosome on a remote worker
/// Workers with cached data subscribe to these and return fitness results
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct ChromosomeEvalRequest {
    /// Parent job ID
    #[prost(string, tag = "1")]
    pub job_id: String,
    /// Generation number
    #[prost(int32, tag = "2")]
    pub generation: i32,
    /// Chromosome index within generation
    #[prost(int32, tag = "3")]
    pub chromosome_id: i32,
    /// Total chromosomes in this generation
    #[prost(int32, tag = "4")]
    pub total_chromosomes: i32,
    /// Serialized chromosome parameters (JSON)
    #[prost(bytes, tag = "5")]
    pub chromosome_params: Vec<u8>,
    /// Trading symbol for data lookup
    #[prost(string, tag = "6")]
    pub symbol: String,
    /// Exchange for data lookup
    #[prost(string, tag = "7")]
    pub exchange: String,
    /// Initial capital
    #[prost(double, tag = "8")]
    pub initial_capital: f64,
    /// Data cache key (workers with this data cached process faster)
    #[prost(string, tag = "9")]
    pub data_cache_key: String,
    /// If true, this is a final evaluation - run full backtest with all analysis
    #[prost(bool, tag = "10")]
    pub is_final_evaluation: bool,
    /// Run Monte Carlo simulation (only used when is_final_evaluation=true)
    #[prost(bool, tag = "11")]
    pub run_monte_carlo: bool,
    /// Run Walk-Forward analysis (only used when is_final_evaluation=true)
    #[prost(bool, tag = "12")]
    pub run_walk_forward: bool,
    /// Train data end index (exclusive). GA fitness uses ticks [0, train_end_idx)
    #[prost(int64, tag = "13")]
    pub train_end_idx: i64,
    /// Test data end index (exclusive). Walk-forward uses ticks [train_end_idx, test_end_idx)
    #[prost(int64, tag = "14")]
    pub test_end_idx: i64,
    /// Total tick count. Final validation uses ticks [test_end_idx, total_ticks)
    #[prost(int64, tag = "15")]
    pub total_ticks: i64,
    /// Custom fitness weights (JSON serialized, optional)
    /// If empty, uses default balanced weights
    #[prost(bytes, tag = "16")]
    pub fitness_weights_json: Vec<u8>,
    /// Initial 30-day trading volume in USD (for fee tier calculations)
    /// 0.0 = use exchange default fee tier, higher values = lower fees
    #[prost(double, tag = "17")]
    pub initial_30d_volume: f64,
    /// Percentage of initial_capital to hold as base asset (e.g., 0.5 = 50% in BTC, 50% in USD)
    #[prost(double, tag = "18")]
    pub initial_inventory_pct: f64,
    /// Number of Monte Carlo simulation runs (default 1000, capped at 200 for speed)
    #[prost(uint32, tag = "19")]
    pub monte_carlo_runs: u32,
}

/// Result from chromosome evaluation
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct ChromosomeEvalResult {
    /// Parent job ID
    #[prost(string, tag = "1")]
    pub job_id: String,
    /// Generation number
    #[prost(int32, tag = "2")]
    pub generation: i32,
    /// Chromosome index
    #[prost(int32, tag = "3")]
    pub chromosome_id: i32,
    /// Worker ID that processed this
    #[prost(string, tag = "4")]
    pub worker_id: String,
    /// Whether evaluation succeeded
    #[prost(bool, tag = "5")]
    pub success: bool,
    /// Error message if failed
    #[prost(string, tag = "6")]
    pub error_message: String,
    /// Fitness score
    #[prost(double, tag = "7")]
    pub fitness: f64,
    /// Net PnL
    #[prost(double, tag = "8")]
    pub net_pnl: f64,
    /// Sharpe ratio
    #[prost(double, tag = "9")]
    pub sharpe_ratio: f64,
    /// Number of trades
    #[prost(int32, tag = "10")]
    pub num_trades: i32,
    /// Max drawdown
    #[prost(double, tag = "11")]
    pub max_drawdown: f64,
    /// Equity curve (serialized as JSON)
    #[prost(bytes, tag = "12")]
    pub equity_curve: Vec<u8>,
    /// Processing duration in milliseconds
    #[prost(int64, tag = "13")]
    pub processing_duration_ms: i64,
    /// Win rate (percentage of profitable trades)
    #[prost(double, tag = "14")]
    pub win_rate: f64,
    /// Profit factor (gross profit / gross loss)
    #[prost(double, tag = "15")]
    pub profit_factor: f64,
    /// Number of closed trades
    #[prost(int32, tag = "16")]
    pub closed_trades: i32,
    /// Total orders placed
    #[prost(int32, tag = "17")]
    pub total_orders: i32,
    /// Starting capital
    #[prost(double, tag = "18")]
    pub starting_capital: f64,
    /// Ending capital
    #[prost(double, tag = "19")]
    pub ending_capital: f64,
    /// Total return percentage
    #[prost(double, tag = "20")]
    pub total_return_pct: f64,
    /// Monte Carlo results (serialized JSON, populated when is_final_evaluation)
    #[prost(bytes, tag = "21")]
    pub monte_carlo_results: Vec<u8>,
    /// Walk-forward results (serialized JSON, populated when is_final_evaluation)
    #[prost(bytes, tag = "22")]
    pub walk_forward_results: Vec<u8>,
    /// Closed trades data (serialized JSON, populated when is_final_evaluation)
    #[prost(bytes, tag = "23")]
    pub closed_trades_data: Vec<u8>,
    /// Indicates this is a final evaluation result with full data
    #[prost(bool, tag = "24")]
    pub is_final_evaluation: bool,
    /// Statistical significance data (serialized JSON, populated when is_final_evaluation)
    #[prost(string, tag = "25")]
    pub statistical_significance: String,
    /// Best trade P&L in USD
    #[prost(double, tag = "26")]
    pub best_trade_pnl: f64,
    /// Worst trade P&L in USD
    #[prost(double, tag = "27")]
    pub worst_trade_pnl: f64,
    /// Average trade P&L in USD
    #[prost(double, tag = "28")]
    pub avg_trade_pnl: f64,
    /// Average time in trade in milliseconds
    #[prost(double, tag = "29")]
    pub avg_time_in_trade_ms: f64,
    /// Total volume traded in USD
    #[prost(double, tag = "30")]
    pub total_volume: f64,
    /// Total fees paid in USD
    #[prost(double, tag = "31")]
    pub total_fees: f64,
    /// Inventory metrics (serialized JSON)
    #[prost(bytes, tag = "32")]
    pub inventory_metrics: Vec<u8>,
    /// Execution metrics (serialized JSON)
    #[prost(bytes, tag = "33")]
    pub execution_metrics: Vec<u8>,
    /// Average inventory over the backtest period
    #[prost(double, tag = "34")]
    pub avg_inventory: f64,
    /// Max inventory held during backtest
    #[prost(double, tag = "35")]
    pub max_inventory: f64,
    /// Inventory zero crossings (sign changes)
    #[prost(int32, tag = "36")]
    pub zero_crossings: i32,
    /// Average fill rate (0.0-1.0)
    #[prost(double, tag = "37")]
    pub avg_fill_rate: f64,
    /// Average queue position
    #[prost(double, tag = "38")]
    pub avg_queue_position: f64,
    /// Partial fill percentage (0.0-100.0)
    #[prost(double, tag = "39")]
    pub partial_fill_pct: f64,
    /// Average latency in milliseconds
    #[prost(double, tag = "40")]
    pub avg_latency_ms: f64,
}

/// Request to broadcast market data to all workers for caching
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct DataBroadcastRequest {
    /// Unique cache key for this dataset
    #[prost(string, tag = "1")]
    pub cache_key: String,
    /// Trading symbol
    #[prost(string, tag = "2")]
    pub symbol: String,
    /// Exchange
    #[prost(string, tag = "3")]
    pub exchange: String,
    /// Number of data points
    #[prost(int64, tag = "4")]
    pub data_point_count: i64,
    /// Compressed market data (using LZ4)
    #[prost(bytes, tag = "5")]
    pub compressed_data: Vec<u8>,
    /// Compression algorithm used
    #[prost(string, tag = "6")]
    pub compression: String,
    /// TTL in seconds (how long workers should cache this)
    #[prost(int64, tag = "7")]
    pub ttl_seconds: i64,
}

/// Acknowledgment that worker has cached the data
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct DataCacheAck {
    /// Cache key that was stored
    #[prost(string, tag = "1")]
    pub cache_key: String,
    /// Worker ID that cached it
    #[prost(string, tag = "2")]
    pub worker_id: String,
    /// Whether caching succeeded
    #[prost(bool, tag = "3")]
    pub success: bool,
    /// Number of concurrent evaluations this worker can handle
    #[prost(int32, tag = "4")]
    pub capacity: i32,
    /// Number of ticks in the dataset (for train/test/validate splits)
    #[prost(int64, tag = "5")]
    pub tick_count: i64,
}

/// Asset information for multi-asset portfolio operations
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct AssetInfo {
    /// Trading symbol (e.g., "BTCUSD", "ETHUSD")
    #[prost(string, tag = "1")]
    pub symbol: String,
    /// Exchange name (e.g., "kraken", "binance")
    #[prost(string, tag = "2")]
    pub exchange: String,
    /// Strategy type for this asset (optional): "momentum", "mean_reversion", "market_making"
    #[prost(string, tag = "3")]
    pub strategy: String,
    /// Capital allocation weight (0.0 to 1.0)
    #[prost(double, tag = "4")]
    pub weight: f64,
}

/// Request for workers to load data from database (no data payload over broker)
/// 
/// This is the preferred approach for large datasets. Instead of broadcasting
/// the actual data over the message broker (which can be 500MB+), we send
/// query parameters and workers load data directly from the database.
/// 
/// Benefits:
/// - No serialization overhead (saves 10+ seconds)
/// - No compression overhead (saves 3+ seconds)  
/// - No 16MB message size limit issues
/// - Workers can load in parallel from database
/// - Memory efficient (no intermediate copies)
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct DataLoadRequest {
    /// Unique cache key for this dataset (job-level key)
    #[prost(string, tag = "1")]
    pub cache_key: String,
    /// Trading symbol(s) to load (legacy, use `assets` for multi-asset)
    #[prost(string, repeated, tag = "2")]
    pub symbols: Vec<String>,
    /// Exchange name (legacy, use `assets` for multi-asset)
    #[prost(string, tag = "3")]
    pub exchange: String,
    /// Start timestamp (ISO 8601 or Unix epoch seconds)
    #[prost(string, tag = "4")]
    pub start_time: String,
    /// End timestamp (ISO 8601 or Unix epoch seconds)
    #[prost(string, tag = "5")]
    pub end_time: String,
    /// TTL in seconds (how long workers should cache this)
    #[prost(int64, tag = "6")]
    pub ttl_seconds: i64,
    /// Initial capital for position sizing
    #[prost(double, tag = "7")]
    pub initial_capital: f64,
    /// Job ID for correlation
    #[prost(string, tag = "8")]
    pub job_id: String,
    /// Multi-asset portfolio: list of all assets to load
    /// If non-empty, takes precedence over symbols/exchange fields
    #[prost(message, repeated, tag = "9")]
    pub assets: Vec<AssetInfo>,
}
