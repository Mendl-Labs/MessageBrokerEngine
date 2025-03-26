use async_trait::async_trait;
use mockall::automock;
use prost::bytes::BytesMut;
use protocol::broker::messages::publish_request::Payload;
use std::collections::{
    HashMap,
    VecDeque
};
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{error, info};

type TokioMutex<T> = tokio::sync::Mutex<T>;

#[automock]
#[async_trait]
pub trait TopicManagerTrait {
    async fn subscribe<'a>(
        &'a self,
        topics: Vec<&'a str>,
        stream: Arc<TokioMutex<TcpStream>>,
    ) -> Result<(), Box<dyn Error>>;
    async fn publish<'a>(&'a self, topic: Vec<&'a str>, message: Option<Payload>) -> Result<(), Box<dyn Error>>;
}

#[derive(Debug, Clone)]
pub struct TopicManager {
    topics_manager: Arc<Mutex<HashMap<String, VecDeque<Arc<Mutex<TcpStream>>>>>>,
}

impl TopicManager {
    pub fn new() -> Self {
        Self {
            topics_manager: Arc::new(Mutex::new(HashMap::new()))
        }
    }
}

#[async_trait]
impl TopicManagerTrait for TopicManager {
    async fn subscribe<'a>(
        &'a self,
        topics: Vec<&'a str>,
        stream: Arc<Mutex<TcpStream>>,
    ) -> Result<(), Box<dyn Error>> {
        if topics.is_empty() {
            error!("Topic is empty");
            return Err("Topic cannot be empty".into());
        }
        
        for topic in topics {
            info!("Subscribe to topic: {:?}", topic);
            let mut topics = self.topics_manager.lock().await;
            topics
                .entry(topic.to_string())
                .or_insert_with(VecDeque::new)
                .push_back(stream.clone());
        }
        Ok(())
    }

    async fn publish<'a>(
        &'a self,
        topics: Vec<&'a str>,
        message: Option<Payload>,
    ) -> Result<(), Box<dyn Error>> {
        let topics_manager = self.topics_manager.lock().await;
        for topic in topics {
            match topics_manager.get(topic) {
                Some(streams) => match message {
                    Some(ref payload) => {
                        for stream in streams {
                            let mut stream = stream.lock().await;
                            let buf = BytesMut::with_capacity(payload.encoded_len());
                            info!(
                                "Publish payload {:?} to topic: {:?} and stream: {:?}",
                                payload, topic, stream
                            );
                            if let Err(e) = stream.write_all(&buf).await {
                                error!("Failed to write message to stream: {:?}", e);
                                return Err(Box::new(e));
                            }
                        }

                    }
                    None => {
                        error!("Message is None");
                    }
                },
                None => {
                    error!("No subscribers for topic");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::broker::messages::{market_message::{self}, publish_request, BigDecimal, MarketMessage, Order, OrderBook, PublishRequest, Trade, Trades};
    use tokio::net::TcpListener;
    use uuid::{self, Uuid};

    #[tokio::test]
    async fn test_topic_manager_new() {
        let manager = TopicManager::new();
        assert!(manager.topics_manager.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_subscribe_valid_topic_and_stream() {
        let mut mock = MockTopicManagerTrait::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let stream = Arc::new(Mutex::new(stream));

        mock.expect_subscribe()
            .withf(|topic, _| topic == &vec!["test_topic"])
            .returning(|_, _| Ok(()));

        let result = mock.subscribe(vec!["test_topic"], stream).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_subscribe_empty_topic() {
        let mut mock = MockTopicManagerTrait::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let stream = Arc::new(Mutex::new(stream));

        mock.expect_subscribe()
            .withf(|topic, _| topic.is_empty())
            .returning(|_, _| Err("Topic cannot be empty".into()));

        let result = mock.subscribe(vec![], stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_publish_valid_topic_and_message_order() {
        let mut mock = MockTopicManagerTrait::new();

        let topics = vec!["test_topic"];

        let big_decimal_price = BigDecimal {
            digits: 24,
            scale: 4,
        };

        let bid_decimal_quantity = BigDecimal {
            digits: 10,
            scale: 0,
        };

        let order = Order {
            unique_id: Uuid::new_v4().to_string(),
            symbol: "BTC/USD".to_string(),
            exchange: "binance".to_string(),
            price_level: Some(big_decimal_price),
            quantity: Some(bid_decimal_quantity),
            side: "BUY".to_string(),
            event: "NEW".to_string(),
        };

        let orderbook = OrderBook {
            symbol: "BTC/USD".to_string(),
            exchange: "binance".to_string(),
            orders: vec![order],
        };

        let market_message = MarketMessage {
            payload: Some(market_message::Payload::OrderBookPayload(orderbook)),
        };

        let message = PublishRequest {
            topics: topics.into_iter().map(|s| s.to_string()).collect(),
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };

        // Clone the payload before passing it to mock.publish
        let payload_clone = message.payload.clone();
        let topics_clone = message.topics.clone();

        mock.expect_publish()
            .withf(move |topics, message| {
                let payload_clone = message.clone();
                *topics == topics_clone && *message == payload_clone
            })
            .returning(|_, _| Ok(()));

        let result = mock.publish(vec!["test_topic"], payload_clone).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_publish_valid_topic_and_message_trade() {
        let mut mock = MockTopicManagerTrait::new();

        let topics = vec!["test_topic"];

        let big_decimal_price = BigDecimal {
            digits: 24,
            scale: 4,
        };

        let bid_decimal_quantity = BigDecimal {
            digits: 10,
            scale: 0,
        };

        let trade = Trade {
            symbol: "BTC/USD".to_string(),
            exchange: "binance".to_string(),
            side: "BUY".to_string(),
            price: Some(big_decimal_price),
            qty: Some(bid_decimal_quantity),
            ord_type: "LIMIT".to_string(),
            trade_id: 1234567890,
            timestamp: "".to_string(),
        };

        let trades = Trades {
            trades: vec![trade],
        };

        let market_message = MarketMessage {
            payload: Some(market_message::Payload::TradesPayload(trades)),
        };

        let message = PublishRequest {
            topics: topics.into_iter().map(|s| s.to_string()).collect(),
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };

        // Clone the payload before passing it to mock.publish
        let payload_clone = message.payload.clone();
        let topics_clone = message.topics.clone();

        mock.expect_publish()
            .withf(move |topics, message| {
                let payload_clone = message.clone();
                *topics == topics_clone && *message == payload_clone
            })
            .returning(|_, _| Ok(()));

        let result = mock.publish(vec!["test_topic"], payload_clone).await;
        assert!(result.is_ok());
    }
}
