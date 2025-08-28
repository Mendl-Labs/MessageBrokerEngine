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
