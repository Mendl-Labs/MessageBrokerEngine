use protocol::{
    MessageCompressor, CompressionConfig, CompressionAlgorithm, AdaptiveCompressor,
    tenant_topic, tenant_topic_uuid, parse_tenant_topic, is_tenant_topic, is_global_topic,
};
use uuid::Uuid;
use prost::Message;
use protocol::generated::{
    StrategyDeployment, StrategyDeactivation,
    StrategyDeploymentAck, DeploymentStatusResponse, ActiveStrategyInfo,
};

// ── Compression tests ─────────────────────────────────────────────────────────

#[test]
fn test_compression_lz4_round_trip() {
    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        min_message_size_for_compression: 10,
        ..Default::default()
    };
    let mut compressor = MessageCompressor::new(config);
    let original = b"Round-trip LZ4: AAAAAA BBBBBB CCCCCC AAAAAA BBBBBB CCCCCC AAAAAA BBBBBB";
    let compressed = compressor.compress(original).unwrap();
    let decompressed = compressor.decompress(&compressed).unwrap();
    assert_eq!(original.as_slice(), decompressed.as_slice());
}

// ---------------------------------------------------------------------------
// Strategy deployment protocol tests
// ---------------------------------------------------------------------------

#[test]
fn test_strategy_deployment_encode_decode() {
    let deployment = StrategyDeployment {
        strategy_id: "strat-001".to_string(),
        instance_id: "inst-abc".to_string(),
        tenant_id: "tenant-x".to_string(),
        strategy_type: "AvellanedaStoikov".to_string(),
        strategy_name: "AS Market Maker".to_string(),
        version: "1.0.0".to_string(),
        parameters: br#"{"gamma":0.1}"#.to_vec(),
        initial_capital: 10_000.0,
        target_exchanges: vec!["kraken".to_string(), "binance".to_string()],
        symbols: vec!["XBTUSD".to_string()],
        approved_by: "admin".to_string(),
        approved_at: "2026-01-01T00:00:00Z".to_string(),
        performance_summary: vec![],
        risk_metrics: vec![],
        admin_approved: true,
        timestamp: 1_700_000_000,
        mode: "paper".to_string(),
    };

    let mut buf = Vec::new();
    deployment.encode(&mut buf).expect("encode failed");
    assert!(!buf.is_empty());

    let decoded = StrategyDeployment::decode(buf.as_slice()).expect("decode failed");
    assert_eq!(decoded.strategy_id, "strat-001");
    assert_eq!(decoded.instance_id, "inst-abc");
    assert_eq!(decoded.strategy_type, "AvellanedaStoikov");
    assert_eq!(decoded.initial_capital, 10_000.0);
    assert!(decoded.admin_approved);
    assert_eq!(decoded.target_exchanges, vec!["kraken", "binance"]);
    assert_eq!(decoded.symbols, vec!["XBTUSD"]);
}

#[test]
fn test_strategy_deactivation_encode_decode() {
    let deactivation = StrategyDeactivation {
        strategy_id: "strat-001".to_string(),
        instance_id: "inst-abc".to_string(),
        tenant_id: "tenant-x".to_string(),
        reason: "Manual stop".to_string(),
        deactivated_by: "operator".to_string(),
        close_positions: true,
        cancel_orders: true,
        timestamp: 1_700_001_000,
    };

    let mut buf = Vec::new();
    deactivation.encode(&mut buf).expect("encode failed");

    let decoded = StrategyDeactivation::decode(buf.as_slice()).expect("decode failed");
    assert_eq!(decoded.strategy_id, "strat-001");
    assert_eq!(decoded.reason, "Manual stop");
    assert!(decoded.close_positions);
    assert!(decoded.cancel_orders);
}

#[test]
fn test_strategy_deployment_ack_success() {
    let ack = StrategyDeploymentAck {
        strategy_id: "strat-001".to_string(),
        instance_id: "inst-abc".to_string(),
        signal_engine_node: "node-1".to_string(),
        success: true,
        error_message: String::new(),
        loaded_at: 1_700_000_050,
        active_exchanges: vec!["kraken".to_string()],
    };

    let mut buf = Vec::new();
    ack.encode(&mut buf).expect("encode failed");

    let decoded = StrategyDeploymentAck::decode(buf.as_slice()).expect("decode failed");
    assert!(decoded.success);
    assert!(decoded.error_message.is_empty());
    assert_eq!(decoded.active_exchanges, vec!["kraken"]);
}

#[test]
fn test_strategy_deployment_ack_failure() {
    let ack = StrategyDeploymentAck {
        strategy_id: "strat-002".to_string(),
        instance_id: "inst-xyz".to_string(),
        signal_engine_node: "node-2".to_string(),
        success: false,
        error_message: "Out of memory".to_string(),
        loaded_at: 0,
        active_exchanges: vec![],
    };

    let mut buf = Vec::new();
    ack.encode(&mut buf).expect("encode failed");

    let decoded = StrategyDeploymentAck::decode(buf.as_slice()).expect("decode failed");
    assert!(!decoded.success);
    assert_eq!(decoded.error_message, "Out of memory");
    assert!(decoded.active_exchanges.is_empty());
}

#[test]
fn test_deployment_status_response_encode_decode() {
    let strategy_info = ActiveStrategyInfo {
        strategy_id: "strat-001".to_string(),
        instance_id: "inst-abc".to_string(),
        tenant_id: "tenant-x".to_string(),
        strategy_type: "Momentum".to_string(),
        strategy_name: "Fast Momentum".to_string(),
        active_exchanges: vec!["binance".to_string()],
        symbols: vec!["BTCUSDT".to_string()],
        unrealized_pnl: 250.5,
        realized_pnl: 1000.0,
        open_positions: 2,
        pending_orders: 5,
        deployed_at: 1_699_990_000,
        total_trades: 42,
    };

    let response = DeploymentStatusResponse {
        request_id: "req-123".to_string(),
        signal_engine_node: "node-1".to_string(),
        active_strategies: vec![strategy_info],
        total_memory_bytes: 52_428_800,
        cpu_utilization: 15.7,
    };

    let mut buf = Vec::new();
    response.encode(&mut buf).expect("encode failed");

    let decoded = DeploymentStatusResponse::decode(buf.as_slice()).expect("decode failed");
    assert_eq!(decoded.request_id, "req-123");
    assert_eq!(decoded.active_strategies.len(), 1);
    let info = &decoded.active_strategies[0];
    assert_eq!(info.strategy_type, "Momentum");
    assert_eq!(info.total_trades, 42);
    assert!((info.unrealized_pnl - 250.5).abs() < 1e-6);
}

#[test]
fn test_publish_request_with_strategy_deployment_payload() {
    let deployment = StrategyDeployment {
        strategy_id: "strat-003".to_string(),
        instance_id: "inst-def".to_string(),
        tenant_id: "tenant-y".to_string(),
        strategy_type: "MeanReversion".to_string(),
        strategy_name: "ETH Mean Rev".to_string(),
        version: "2.0.0".to_string(),
        parameters: br#"{"window":20}"#.to_vec(),
        initial_capital: 5_000.0,
        target_exchanges: vec!["coinbase".to_string()],
        symbols: vec!["ETHUSD".to_string()],
        approved_by: "user".to_string(),
        approved_at: "2026-02-01T00:00:00Z".to_string(),
        performance_summary: vec![],
        risk_metrics: vec![],
        admin_approved: false,
        timestamp: 1_700_100_000,
        mode: "paper".to_string(),
    };

    let request = PublishRequest {
        topic: "strategy.deployment".to_string(),
        payload: Some(Payload::StrategyDeployment(deployment)),
    };

    let mut buf = Vec::new();
    request.encode(&mut buf).expect("encode failed");

    let decoded = PublishRequest::decode(buf.as_slice()).expect("decode failed");
    assert_eq!(decoded.topic, "strategy.deployment");
    match decoded.payload {
        Some(Payload::StrategyDeployment(d)) => {
            assert_eq!(d.strategy_id, "strat-003");
            assert_eq!(d.strategy_type, "MeanReversion");
        }
        other => panic!("unexpected payload variant: {:?}", other),
    }
}

#[test]
fn test_publish_request_with_strategy_deactivation_payload() {
    let deactivation = StrategyDeactivation {
        strategy_id: "strat-003".to_string(),
        instance_id: "inst-def".to_string(),
        tenant_id: "tenant-y".to_string(),
        reason: "User requested".to_string(),
        deactivated_by: "user".to_string(),
        close_positions: false,
        cancel_orders: true,
        timestamp: 1_700_200_000,
    };

    let request = PublishRequest {
        topic: "strategy.deactivation".to_string(),
        payload: Some(Payload::StrategyDeactivation(deactivation)),
    };

    let mut buf = Vec::new();
    request.encode(&mut buf).expect("encode failed");

    let decoded = PublishRequest::decode(buf.as_slice()).expect("decode failed");
    match decoded.payload {
        Some(Payload::StrategyDeactivation(d)) => {
            assert_eq!(d.reason, "User requested");
            assert!(d.cancel_orders);
            assert!(!d.close_positions);
        }
        other => panic!("unexpected payload variant: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Market data subscription protocol tests
// ---------------------------------------------------------------------------

#[test]
fn test_market_data_subscribe_encode_decode() {
    let subscribe = MarketDataSubscribe {
        subscription_id: "sub-001".to_string(),
        tenant_id: "tenant-x".to_string(),
        strategy_instance_id: "inst-abc".to_string(),
        exchange: "kraken".to_string(),
        symbols: vec!["XBTUSD".to_string(), "ETHUSD".to_string()],
        data_types: vec!["trade".to_string(), "orderbook".to_string()],
        orderbook_depth: 10,
        timestamp: 1_700_000_000,
    };

    let mut buf = Vec::new();
    subscribe.encode(&mut buf).expect("encode failed");

    let decoded = MarketDataSubscribe::decode(buf.as_slice()).expect("decode failed");
    assert_eq!(decoded.subscription_id, "sub-001");
    assert_eq!(decoded.tenant_id, "tenant-x");
    assert_eq!(decoded.exchange, "kraken");
    assert_eq!(decoded.symbols, vec!["XBTUSD", "ETHUSD"]);
    assert_eq!(decoded.data_types, vec!["trade", "orderbook"]);
    assert_eq!(decoded.orderbook_depth, 10);
}

#[test]
fn test_market_data_unsubscribe_encode_decode() {
    let unsubscribe = MarketDataUnsubscribe {
        subscription_id: "sub-001".to_string(),
        strategy_instance_id: "inst-abc".to_string(),
        exchange: "kraken".to_string(),
        symbols: vec!["XBTUSD".to_string()],
        reason: "Strategy deactivated".to_string(),
        timestamp: 1_700_001_000,
    };

    let mut buf = Vec::new();
    unsubscribe.encode(&mut buf).expect("encode failed");

    let decoded = MarketDataUnsubscribe::decode(buf.as_slice()).expect("decode failed");
    assert_eq!(decoded.subscription_id, "sub-001");
    assert_eq!(decoded.reason, "Strategy deactivated");
}

#[test]
fn test_market_data_subscription_ack_success() {
    let ack = MarketDataSubscriptionAck {
        subscription_id: "sub-001".to_string(),
        success: true,
        error_message: String::new(),
        data_engine_node: "dataengine-1".to_string(),
        data_topics: vec!["market_data.kraken.XBTUSD".to_string()],
        timestamp: 1_700_000_010,
    };

    let mut buf = Vec::new();
    ack.encode(&mut buf).expect("encode failed");

    let decoded = MarketDataSubscriptionAck::decode(buf.as_slice()).expect("decode failed");
    assert!(decoded.success);
    assert_eq!(decoded.data_topics, vec!["market_data.kraken.XBTUSD"]);
}

// ---------------------------------------------------------------------------
// Original stub tests (kept for backward compatibility)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_protocol_module_exists() {
    // Basic test to ensure the protocol module compiles and is accessible
    // The actual message structures are generated from protobuf files
    
    // This test mainly validates that:
    // 1. The protocol module can be imported
    // 2. The build process works correctly
    // 3. Generated code compiles without errors
    //
    // Reaching this point without panicking is itself the assertion.
}

#[test]
fn test_compression_gzip_round_trip() {
    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Gzip,
        compression_level: 6,
        min_message_size_for_compression: 10,
        ..Default::default()
    };
    let mut compressor = MessageCompressor::new(config);
    let original = b"Round-trip Gzip: AAAAAA BBBBBB CCCCCC AAAAAA BBBBBB CCCCCC AAAAAA BBBBBB";
    let compressed = compressor.compress(original).unwrap();
    let decompressed = compressor.decompress(&compressed).unwrap();
    assert_eq!(original.as_slice(), decompressed.as_slice());
}

#[test]
fn test_compression_none_algorithm() {
    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::None,
        min_message_size_for_compression: 0,
        ..Default::default()
    };
    let mut compressor = MessageCompressor::new(config);
    let original = b"uncompressed payload";
    let result = compressor.compress(original).unwrap();
    // With None algorithm the header byte is 0 (uncompressed)
    assert_eq!(result[0], 0u8);
    let decompressed = compressor.decompress(&result).unwrap();
    assert_eq!(original.as_slice(), decompressed.as_slice());
}

#[test]
fn test_compression_small_message_skipped() {
    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        min_message_size_for_compression: 1000,
        ..Default::default()
    };
    let mut compressor = MessageCompressor::new(config);
    let small = b"tiny";
    let result = compressor.compress(small).unwrap();
    // Small messages bypass compression: header byte 0 + original data
    assert_eq!(result[0], 0u8);
    assert_eq!(&result[1..], small);
    let decompressed = compressor.decompress(&result).unwrap();
    assert_eq!(small.as_slice(), decompressed.as_slice());
}

#[test]
fn test_compression_empty_decompress() {
    let config = CompressionConfig::default();
    let mut compressor = MessageCompressor::new(config);
    let result = compressor.decompress(&[]).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_compression_stats_tracking() {
    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        min_message_size_for_compression: 10,
        ..Default::default()
    };
    let mut compressor = MessageCompressor::new(config);
    let data = b"Statistics tracking: AAAAAAA BBBBBBB CCCCCCC AAAAAAA BBBBBBB CCCCCCC AAAAAAA";
    compressor.compress(data).unwrap();
    let stats = compressor.get_stats();
    assert_eq!(stats.total_messages, 1);
    assert_eq!(stats.total_original_bytes, data.len() as u64);
}

#[test]
fn test_compression_stats_reset() {
    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        min_message_size_for_compression: 10,
        ..Default::default()
    };
    let mut compressor = MessageCompressor::new(config);
    let data = b"Some data for compression stats reset test data data data data";
    compressor.compress(data).unwrap();
    compressor.reset_stats();
    let stats = compressor.get_stats();
    assert_eq!(stats.total_messages, 0);
    assert_eq!(stats.total_original_bytes, 0);
}

#[test]
fn test_compression_stats_calculations() {
    use protocol::compression::CompressionStats;
    let mut stats = CompressionStats::default();
    // Zero-state: no division by zero
    assert_eq!(stats.compression_ratio(), 0.0);
    assert_eq!(stats.average_compression_time_ns(), 0);
    assert_eq!(stats.average_decompression_time_ns(), 0);
    assert_eq!(stats.space_savings_bytes(), 0);
    assert_eq!(stats.space_savings_percentage(), 0.0);

    stats.total_original_bytes = 1000;
    stats.total_compressed_bytes = 600;
    stats.compressed_messages = 1;
    stats.total_compression_time_ns = 5000;
    stats.total_decompression_time_ns = 3000;

    assert!((stats.compression_ratio() - 0.6).abs() < 1e-6);
    assert_eq!(stats.average_compression_time_ns(), 5000);
    assert_eq!(stats.average_decompression_time_ns(), 3000);
    assert_eq!(stats.space_savings_bytes(), 400);
    assert!((stats.space_savings_percentage() - 40.0).abs() < 1e-4);
}

#[test]
fn test_adaptive_compressor_returns_data() {
    let mut adaptive = AdaptiveCompressor::new();
    let data = b"Adaptive compressor test: repeated pattern repeated pattern repeated pattern";
    let (compressed, algorithm) = adaptive.compress(data).unwrap();
    assert!(!compressed.is_empty());
    assert!(matches!(algorithm, CompressionAlgorithm::Lz4 | CompressionAlgorithm::Gzip));
}

#[tokio::test]
async fn test_compression_concurrent_access() {
    use std::sync::{Arc, Mutex};
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];
    for i in 0u8..4 {
        let results_clone = results.clone();
        let handle = tokio::spawn(async move {
            let config = CompressionConfig {
                algorithm: CompressionAlgorithm::Lz4,
                min_message_size_for_compression: 10,
                ..Default::default()
            };
            let mut compressor = MessageCompressor::new(config);
            let data = vec![i; 80];
            let compressed = compressor.compress(&data).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();
            results_clone.lock().unwrap().push(decompressed == data);
        });
        handles.push(handle);
    }
    for h in handles { h.await.unwrap(); }
    let r = results.lock().unwrap();
    assert_eq!(r.len(), 4);
    assert!(r.iter().all(|&ok| ok));
}

// ── Tenant-topic tests ────────────────────────────────────────────────────────

#[test]
fn test_tenant_topic_format() {
    let topic = tenant_topic("abc-123", "strategy.deployment");
    assert_eq!(topic, "tenant.abc-123.strategy.deployment");
}

#[test]
fn test_tenant_topic_uuid_format() {
    let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
        .expect("hard-coded UUID string must be valid");
    let topic = tenant_topic_uuid(id, "strategy.deployment");
    assert_eq!(topic, "tenant.550e8400e29b41d4a716446655440000.strategy.deployment");
}

#[test]
fn test_parse_tenant_topic_valid() {
    let (tid, base) = parse_tenant_topic("tenant.abc-123.strategy.deployment").unwrap();
    assert_eq!(tid, "abc-123");
    assert_eq!(base, "strategy.deployment");
}

#[test]
fn test_parse_tenant_topic_invalid() {
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

// ── Tests for extended PublishRequest payload variants ───────────────────────

use protocol::broker::messages::{
    publish_request::Payload, DeploymentStatusRequest,
    ExchangeConnectionInfo, MarketDataStatusRequest, MarketDataStatusResponse,
    MarketDataSubscribe, MarketDataSubscriptionAck, MarketDataUnsubscribe, PublishRequest,
};

#[test]
fn test_publish_request_deployment_status_request_payload() {
    let status_req = DeploymentStatusRequest {
        tenant_id: "tenant-abc".to_string(),
        request_id: "req-001".to_string(),
    };
    let req = PublishRequest {
        topic: "strategy.status.request".to_string(),
        payload: Some(Payload::DeploymentStatusRequest(status_req)),
    };
    if let Some(Payload::DeploymentStatusRequest(s)) = req.payload {
        assert_eq!(s.tenant_id, "tenant-abc");
    } else {
        panic!("expected DeploymentStatusRequest payload");
    }
}

#[test]
fn test_publish_request_market_data_subscribe_payload() {
    let sub = MarketDataSubscribe {
        subscription_id: "sub-1".to_string(),
        tenant_id: "tenant-abc".to_string(),
        strategy_instance_id: "inst-1".to_string(),
        exchange: "kraken".to_string(),
        symbols: vec!["XBT/USD".to_string(), "ETH/USD".to_string()],
        data_types: vec!["trades".to_string(), "orderbook".to_string()],
        orderbook_depth: 10,
        timestamp: 1_700_000_000,
    };
    let req = PublishRequest {
        topic: "market.subscription.subscribe".to_string(),
        payload: Some(Payload::MarketDataSubscribe(sub)),
    };
    if let Some(Payload::MarketDataSubscribe(s)) = req.payload {
        assert_eq!(s.exchange, "kraken");
        assert_eq!(s.symbols.len(), 2);
        assert_eq!(s.orderbook_depth, 10);
    } else {
        panic!("expected MarketDataSubscribe payload");
    }
}

#[test]
fn test_publish_request_market_data_unsubscribe_payload() {
    let unsub = MarketDataUnsubscribe {
        subscription_id: "sub-1".to_string(),
        strategy_instance_id: "inst-1".to_string(),
        exchange: "kraken".to_string(),
        symbols: vec!["XBT/USD".to_string()],
        reason: "strategy deactivated".to_string(),
        timestamp: 1_700_001_000,
    };
    let req = PublishRequest {
        topic: "market.subscription.unsubscribe".to_string(),
        payload: Some(Payload::MarketDataUnsubscribe(unsub)),
    };
    if let Some(Payload::MarketDataUnsubscribe(u)) = req.payload {
        assert_eq!(u.reason, "strategy deactivated");
    } else {
        panic!("expected MarketDataUnsubscribe payload");
    }
}

#[test]
fn test_publish_request_market_data_subscription_ack_payload() {
    let ack = MarketDataSubscriptionAck {
        subscription_id: "sub-1".to_string(),
        success: true,
        error_message: String::new(),
        data_engine_node: "data-engine-1".to_string(),
        data_topics: vec!["market.kraken.XBT/USD".to_string()],
        timestamp: 1_700_000_100,
    };
    let req = PublishRequest {
        topic: "market.subscription.ack".to_string(),
        payload: Some(Payload::MarketDataSubscriptionAck(ack)),
    };
    if let Some(Payload::MarketDataSubscriptionAck(a)) = req.payload {
        assert!(a.success);
        assert_eq!(a.data_topics.len(), 1);
    } else {
        panic!("expected MarketDataSubscriptionAck payload");
    }
}

#[test]
fn test_publish_request_market_data_status_request_payload() {
    let status_req = MarketDataStatusRequest {
        request_id: "req-42".to_string(),
        exchange_filter: "kraken".to_string(),
    };
    let req = PublishRequest {
        topic: "market.subscription.status.request".to_string(),
        payload: Some(Payload::MarketDataStatusRequest(status_req)),
    };
    if let Some(Payload::MarketDataStatusRequest(s)) = req.payload {
        assert_eq!(s.exchange_filter, "kraken");
    } else {
        panic!("expected MarketDataStatusRequest payload");
    }
}

#[test]
fn test_publish_request_market_data_status_response_payload() {
    let conn = ExchangeConnectionInfo {
        exchange: "kraken".to_string(),
        connected: true,
        symbols: vec!["XBT/USD".to_string()],
        data_types: vec!["trades".to_string()],
        subscriber_count: 3,
        connected_since: 1_699_000_000,
        messages_processed: 50_000,
    };
    let status_resp = MarketDataStatusResponse {
        request_id: "req-42".to_string(),
        data_engine_node: "data-engine-1".to_string(),
        active_connections: vec![conn],
        total_subscriptions: 1,
    };
    let req = PublishRequest {
        topic: "market.subscription.status.response".to_string(),
        payload: Some(Payload::MarketDataStatusResponse(status_resp)),
    };
    if let Some(Payload::MarketDataStatusResponse(r)) = req.payload {
        assert_eq!(r.total_subscriptions, 1);
        assert_eq!(r.active_connections[0].exchange, "kraken");
    } else {
        panic!("expected MarketDataStatusResponse payload");
    }
}

