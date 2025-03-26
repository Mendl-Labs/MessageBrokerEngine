use anyhow::Result;
use async_trait::async_trait;
use mockall::automock;
use prost::bytes::BytesMut;
use prost::Message;
use protocol::broker::messages::SubscribeRequest;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::info;

#[automock]
#[async_trait]
pub trait SubscriberTrait {
    async fn subscribe(&self) -> Result<Arc<tokio::sync::Mutex<TcpStream>>>;
}

#[derive(Clone)]
pub struct Subscriber {
    socket: Arc<Mutex<TcpStream>>,
    topics: Vec<String>,
}

impl Subscriber {
    pub async fn new(addr: &str, topics: &Vec<String>) -> Result<Self> {
        let socket = Arc::new(Mutex::new(TcpStream::connect(addr).await.unwrap()));
        Ok(Self {
            socket,
            topics: topics.to_vec(),
        })
    }
}

#[async_trait]
impl SubscriberTrait for Subscriber {
    async fn subscribe(&self) -> Result<Arc<Mutex<TcpStream>>> {
        info!("Subscribing to topic");
        let subscribe_request = SubscribeRequest {
            topics: self.topics.clone(),
        };
        let mut buf = BytesMut::with_capacity(subscribe_request.encoded_len());
        subscribe_request.encode(&mut buf).unwrap();
        self.socket.lock().await.write_all(&buf).await.unwrap();
        Ok(self.socket.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use anyhow::Error;
    use tokio::{io::AsyncReadExt, net::TcpListener, time::timeout};

    #[tokio::test]
    async fn test_subscriber_new() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = socket;
        });

        let result = timeout(
            Duration::from_secs(1),
            Subscriber::new(&addr.to_string(), &vec!["test_topic".to_string()]),
        )
        .await;

        match result {
            Ok(subscriber) => assert!(subscriber.is_ok()),
            Err(_) => panic!("Connection attempt timed out"),
        }
    }

    #[tokio::test]
async fn test_subscriber_subscribe_success() {
    // Create a TCP listener bound to a random port (using "127.0.0.1:0").
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    // Extract the local address of the listener.
    let addr = listener.local_addr().unwrap();

    // Spawn a background task to accept a connection.
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        
        // Read the bytes from the socket for verification if needed.
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await.unwrap();
    });

    // Create a TcpStream connected to the listener's address.
    let stream = TcpStream::connect(addr).await.unwrap();
    let socket = Arc::new(Mutex::new(stream));

    // Create the mock subscriber.
    let mut mock = MockSubscriberTrait::new();

    // Set up the expectation for the `subscribe` method.
    mock.expect_subscribe()
        .times(1)
        .returning(move || Ok(socket.clone()));

    // Call the subscribe method on the mock.
    let result = mock.subscribe().await;

    // Verify that the result is `Ok` and the TcpStream is returned.
    assert!(result.is_ok());
}


    #[tokio::test]
    async fn test_subscriber_subscribe_failure() {
        let mut mock = MockSubscriberTrait::new();
        mock.expect_subscribe()
            .times(1)
            .returning(|| Err(Error::msg("subscribe error")));

        let result = mock.subscribe().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "subscribe error");
    }
}
