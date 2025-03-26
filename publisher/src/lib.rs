use anyhow::{Context, Result};
use async_trait::async_trait;
use mockall::automock;
use prost::bytes::BytesMut;
use prost::Message;
use protocol::broker::messages::broker_message;
use protocol::broker::messages::publish_request;
use protocol::broker::messages::BrokerMessage;
use protocol::broker::messages::PublishRequest;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{error, info};

#[automock]
#[async_trait]
pub trait PublisherTrait {
    async fn publish(&mut self, topic: &Vec<String>, payload: Option<publish_request::Payload>) -> Result<()>;
}

pub struct Publisher {
    stream: TcpStream,
}

impl Publisher {
    pub async fn new(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("Failed to connect to broker at {}", addr))?;
        Ok(Self { stream })
    }
}

#[async_trait]
impl PublisherTrait for Publisher {
    async fn publish(&mut self, topics: &Vec<String>, payload: Option<publish_request::Payload>) -> Result<()> {
        info!("Publishing message {:?}", payload);

        let publish_request = PublishRequest {
            topics: topics.to_vec(),
            payload,
        };

        let broker_message = BrokerMessage { payload: Some(broker_message::Payload::PublishRequest(publish_request)) };

        let mut buf = BytesMut::with_capacity(broker_message.encoded_len());
        if let Err(e) = broker_message.encode(&mut buf) {
            error!("Failed to encode publish request: {:?}", e);
        }

        if let Err(e) = self.stream.write_all(&buf).await {
            error!("Failed to write message to stream: {:?}", e);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::{net::TcpListener, time::timeout};

    #[tokio::test]
    async fn test_publisher_new() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = socket;
        });

        let result = timeout(Duration::from_secs(1), Publisher::new(&addr.to_string())).await;

        match result {
            Ok(publisher) => assert!(publisher.is_ok()),
            Err(_) => panic!("Connection attempt timed out"),
        }
    }
}
