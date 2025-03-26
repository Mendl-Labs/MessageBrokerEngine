use async_trait::async_trait;
use config::{Config, File, FileFormat};
use mockall::automock;
use prost::{bytes::BytesMut, Message};
use protocol::broker::messages::{broker_message, BrokerMessage};
use serde::Deserialize;
use std::{error::Error, sync::Arc};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    signal,
    sync::{Mutex, Semaphore},
};
use topicmanager::{TopicManager, TopicManagerTrait};

const MAX_PERMITS: usize = 5;

#[derive(Debug, Deserialize)]
pub struct HostConfig {
    address: String,
    port: u16,
}

impl HostConfig {
    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn port(&self) -> &u16 {
        &self.port
    }
}

impl TryFrom<Config> for HostConfig {
    type Error = config::ConfigError;

    fn try_from(config: Config) -> Result<Self, Self::Error> {
        let address = config.get::<String>(
            "MessageBrokerServerConfiguration.MessageBrokerServerSettings.Address",
        )?;
        let port = config
            .get::<u16>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.Port")?;
        Ok(HostConfig { address, port })
    }
}

#[automock]
#[async_trait]
pub trait HostedObjectTrait {
    async fn run(&self) -> Result<(), Box<dyn Error>>;
}

#[derive(Debug)]
pub struct HostedObject {
    host: HostConfig,
    topic_manager: Arc<TopicManager>,
}

impl HostedObject {
    pub fn host(&self) -> &HostConfig {
        &self.host
    }

    pub fn topic_manager(&self) -> &Arc<TopicManager> {
        &self.topic_manager
    }

    pub fn build() -> Result<Self, Box<dyn Error>> {
        let host = load_config()?.try_into()?;
        let topic_manager = Arc::new(TopicManager::new());
        Ok(Self {
            host,
            topic_manager,
        })
    }
}

#[async_trait]
impl HostedObjectTrait for HostedObject {
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        let addr = format!("{}:{}", &self.host.address, &self.host.port);
        let listener = TcpListener::bind(addr).await?;
        let semaphore = Arc::new(Semaphore::new(MAX_PERMITS));
        let topic_manager = Arc::clone(&self.topic_manager);

        let server_handle = tokio::spawn(async move {
            println!("Starting server");
            loop {
                tokio::select! {
                    Ok((socket, addr)) = listener.accept() => {
                        println!("Accepted connection from: {:?}", addr);
                        let permit = semaphore.clone().acquire_owned().await;
                        let topic_manager = Arc::clone(&topic_manager);
                        let socket = Arc::new(Mutex::new(socket));

                        if let Err(e) = permit {
                            eprintln!("Failed to acquire semaphore permit: {:?}", e);
                            continue;
                        }
                        let permit = permit.unwrap();

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(socket, topic_manager).await {
                                eprintln!("Failed to handle client: {}", e);
                            }
                            drop(permit);
                        });
                    },
                    _ = signal::ctrl_c() => {
                        println!("Stopping server");
                        break;
                    }
                }
            }
        });

        server_handle.await?;

        Ok(())
    }
}

async fn handle_connection(
    socket: Arc<Mutex<TcpStream>>,
    topic_manager: Arc<TopicManager>,
) -> Result<(), Box<dyn Error>> {
    let mut buf = BytesMut::with_capacity(8192);

    loop {
        buf.clear();
        let mut socket_lock = socket.lock().await; // Lock the socket for reading
        match socket_lock.read_buf(&mut buf).await {
            Ok(0) => break, // Connection closed
            Ok(_) => {
                // Deserialize the buffer into a BrokerMessage
                match BrokerMessage::decode(&buf[..]) {
                    Ok(message) => {
                        // Handle the message
                        println!("decoded message: {:?}", message);
                        let _ = handle_message(message, &topic_manager, Arc::clone(&socket)).await;
                    }
                    Err(e) => {
                        eprintln!("Failed to decode Protobuf message: {:?}", e);
                        return Err(Box::new(e));
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to read from socket; err = {:?}", e);
                return Err(Box::new(e));
            }
        }
    }
    Ok(())
}

async fn handle_message<T: TopicManagerTrait>(
    message: BrokerMessage,
    topic_manager: &Arc<T>,
    socket: Arc<Mutex<TcpStream>>,
) -> Result<(), Box<dyn Error>> {
    match message.payload {
        Some(broker_message::Payload::SubscribeRequest(req)) => {
            println!("Subscribing to topic: {:?}", req.topics);
            topic_manager.subscribe(req.topics.iter().map(|s| s.as_str()).collect(), socket).await?;
        }
        Some(broker_message::Payload::PublishRequest(req)) => {
            println!("Publishing to topic: {:?}", req.topics);
            topic_manager.publish(req.topics.iter().map(|s| s.as_str()).collect(), req.payload).await?;
        }
        _ => {
            println!("Unknown message type");
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unknown message type",
            )));
        }
    }

    Ok(())
}

fn load_config() -> Result<Config, config::ConfigError> {
    let builder = Config::builder()
        .set_default("default", "1")?
        .add_source(File::new("appsettings", FileFormat::Json))
        .set_override("override", "1")?;

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::Config;
    use protocol::broker::messages::{publish_request, PublishRequest, SubscribeRequest};
    use tokio::io::AsyncWriteExt;
    use topicmanager::MockTopicManagerTrait;

    #[test]
    fn test_host_config_address() {
        let config = HostConfig {
            address: "127.0.0.1".to_string(),
            port: 8080,
        };
        assert_eq!(config.address(), "127.0.0.1");
    }

    #[test]
    fn test_host_config_port() {
        let config = HostConfig {
            address: "127.0.0.1".to_string(),
            port: 8080,
        };
        assert_eq!(config.port(), &8080);
    }

    #[test]
    fn test_host_config_try_from_success() {
        let config_data = r#"
        {
            "MessageBrokerServerConfiguration": {
                "MessageBrokerServerSettings": {
                    "Address": "127.0.0.1",
                    "Port": 8080
                }
            }
        }"#;
        let config = Config::builder()
            .add_source(File::from_str(config_data, FileFormat::Json))
            .build()
            .unwrap();
        let host_config: HostConfig = config.try_into().unwrap();
        assert_eq!(host_config.address(), "127.0.0.1");
        assert_eq!(host_config.port(), &8080);
    }

    #[test]
    fn test_host_config_try_from_failure() {
        let config_data = r#"
        {
            "MessageBrokerServerConfiguration": {
                "MessageBrokerServerSettings": {
                    "Address": "127.0.0.1"
                }
            }
        }"#;
        let config = Config::builder()
            .add_source(File::from_str(config_data, FileFormat::Json))
            .build();
        let result: Result<HostConfig, config::ConfigError> = config.and_then(|cfg| cfg.try_into());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_run_success() {
        let mut mock = MockHostedObjectTrait::new();
        mock.expect_run().returning(|| Ok(()));
        let result = mock.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_failure() {
        let mut mock = MockHostedObjectTrait::new();
        mock.expect_run().returning(|| {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "some error",
            )))
        });
        let result = mock.run().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_connection_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let message = BrokerMessage {
                payload: Some(broker_message::Payload::SubscribeRequest(Default::default())),
            };
            let mut buf = BytesMut::with_capacity(8192);
            message.encode(&mut buf).unwrap();
            socket.write_all(&buf).await.unwrap();
        });
        let mock_stream = TcpStream::connect(addr).await.unwrap();
        let mock_stream = Arc::new(Mutex::new(mock_stream));
        let topic_manager = Arc::new(TopicManager::new());
        let result = handle_connection(mock_stream, topic_manager).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_connection_failure() {
        use tokio::io::AsyncWriteExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let malformed_message = vec![0u8; 10];
            socket.write_all(&malformed_message).await.unwrap();
        });
        let mock_stream = TcpStream::connect(addr).await.unwrap();
        let mock_stream = Arc::new(Mutex::new(mock_stream));
        let topic_manager = Arc::new(TopicManager::new());
        let result = handle_connection(mock_stream, topic_manager).await;
        assert!(
            result.is_err(),
            "Expected handle_connection to fail with malformed message"
        );
    }

    #[tokio::test]
    async fn test_handle_message_subscribe_success() {
        let mut mock_topic_manager = MockTopicManagerTrait::new();
        mock_topic_manager
            .expect_subscribe()
            .withf(|topics, _| topics == &vec!["test_topic"])
            .times(1)
            .returning(|_, _| Ok(()));

        let message = BrokerMessage {
            payload: Some(broker_message::Payload::SubscribeRequest(
                SubscribeRequest {
                    topics: vec!["test_topic".to_string()],
                },
            )),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mock_stream = TcpStream::connect(addr).await.unwrap();
        let mock_stream = Arc::new(Mutex::new(mock_stream));
        let result = handle_message(message, &Arc::new(mock_topic_manager), mock_stream).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_message_subscribe_failure() {
        // Mock the TopicManager
        let mut mock_topic_manager = MockTopicManagerTrait::new();

        // Set up the expectation for the subscribe method to return an error
        mock_topic_manager
            .expect_subscribe()
            .withf(|topics, _| topics == &vec!["test_topic"])
            .times(1)
            .returning(|_, _| Err("Failed to subscribe".into()));

        // Create a subscribe request message
        let message = BrokerMessage {
            payload: Some(broker_message::Payload::SubscribeRequest(
                SubscribeRequest {
                    topics: vec!["test_topic".to_string()],
                },
            )),
        };

        // Mock the TcpStream
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mock_stream = TcpStream::connect(addr).await.unwrap();
        let mock_stream = Arc::new(Mutex::new(mock_stream));

        // Call handle_message and check that it returns an error
        let result = handle_message(message, &Arc::new(mock_topic_manager), mock_stream).await;

        assert!(
            result.is_err(),
            "Expected handle_message to fail due to subscribe error"
        );
    }

    #[tokio::test]
    async fn test_handle_message_publish_success() {
        // Mock the TopicManager
        let mut mock_topic_manager = MockTopicManagerTrait::new();

        // Set up the expectation for the publish method to succeed
        mock_topic_manager
            .expect_publish()
            .withf(|topics, _| topics == &vec!["test_topic"])
            .times(1)
            .returning(|_, _| Ok(()));


        // Create a publish request message
        let message = BrokerMessage {
            payload:  Some(broker_message::Payload::PublishRequest(PublishRequest {
                topics: vec!["test_topic".to_string()],
                payload: Some(publish_request::Payload::MarketPayload(Default::default())),
            })),
        };

        // Mock the TcpStream
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mock_stream = TcpStream::connect(addr).await.unwrap();
        let mock_stream = Arc::new(Mutex::new(mock_stream));

        // Call handle_message and check that it returns Ok
        let result = handle_message(message, &Arc::new(mock_topic_manager), mock_stream).await;

        assert!(
            result.is_ok(),
            "Expected handle_message to succeed with valid publish request"
        );
    }

    #[tokio::test]
    async fn test_handle_message_publish_failure() {
        // Mock the TopicManager
        let mut mock_topic_manager = MockTopicManagerTrait::new();

        // Set up the expectation for the publish method to fail
        mock_topic_manager
            .expect_publish()
            .withf(|topics, _| topics == &vec!["test_topic"])
            .times(1)
            .returning(|_, _| Err("Failed to publish".into()));

        // Create a publish request message
        let message = BrokerMessage {
            payload:  Some(broker_message::Payload::PublishRequest(PublishRequest {
                topics: vec!["test_topic".to_string()],
                payload: Some(publish_request::Payload::MarketPayload(Default::default())),
            })),
        };

        // Mock the TcpStream
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mock_stream = TcpStream::connect(addr).await.unwrap();
        let mock_stream = Arc::new(Mutex::new(mock_stream));

        // Call handle_message and check that it returns an error
        let result = handle_message(message, &Arc::new(mock_topic_manager), mock_stream).await;

        assert!(
            result.is_err(),
            "Expected handle_message to fail due to publish error"
        );
    }
}
