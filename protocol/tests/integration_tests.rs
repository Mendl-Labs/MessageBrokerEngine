use protocol::{
    MessageCompressor, CompressionConfig, CompressionAlgorithm, AdaptiveCompressor,
    tenant_topic, tenant_topic_uuid, parse_tenant_topic, is_tenant_topic, is_global_topic,
};
use uuid::Uuid;

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
    publish_request::Payload, BacktestAggregatedResult, BacktestCancelRequest, BacktestChunk,
    BacktestChunkResult, BacktestProgress, ChromosomeEvalRequest, ChromosomeEvalResult,
    DataBroadcastRequest, DataCacheAck, DataLoadRequest, DeploymentStatusRequest,
    ExchangeConnectionInfo, MarketDataStatusRequest, MarketDataStatusResponse,
    MarketDataSubscribe, MarketDataSubscriptionAck, MarketDataUnsubscribe, PublishRequest,
};

#[test]
fn test_publish_request_backtest_chunk_payload() {
    let chunk = BacktestChunk {
        job_id: "job-1".to_string(),
        chunk_id: 0,
        total_chunks: 4,
        start_time: "2024-01-01T00:00:00Z".to_string(),
        end_time: "2024-04-01T00:00:00Z".to_string(),
        symbol: "BTCUSDT".to_string(),
        exchange: "binance".to_string(),
        initial_capital: 10_000.0,
        ..Default::default()
    };
    let req = PublishRequest {
        topic: "backtest.chunk".to_string(),
        payload: Some(Payload::BacktestChunk(chunk.clone())),
    };
    if let Some(Payload::BacktestChunk(c)) = req.payload {
        assert_eq!(c.job_id, "job-1");
        assert_eq!(c.total_chunks, 4);
    } else {
        panic!("expected BacktestChunk payload");
    }
}

#[test]
fn test_publish_request_backtest_chunk_result_payload() {
    let result = BacktestChunkResult {
        job_id: "job-1".to_string(),
        chunk_id: 0,
        worker_id: "worker-a".to_string(),
        success: true,
        net_pnl: 500.0,
        num_trades: 42,
        ..Default::default()
    };
    let req = PublishRequest {
        topic: "backtest.chunk.result".to_string(),
        payload: Some(Payload::BacktestChunkResult(result)),
    };
    if let Some(Payload::BacktestChunkResult(r)) = req.payload {
        assert!(r.success);
        assert_eq!(r.num_trades, 42);
    } else {
        panic!("expected BacktestChunkResult payload");
    }
}

#[test]
fn test_publish_request_backtest_progress_payload() {
    let progress = BacktestProgress {
        job_id: "job-1".to_string(),
        chunks_completed: 2,
        total_chunks: 4,
        percent_complete: 50.0,
        phase: "processing".to_string(),
        ..Default::default()
    };
    let req = PublishRequest {
        topic: "backtest.progress".to_string(),
        payload: Some(Payload::BacktestProgress(progress)),
    };
    if let Some(Payload::BacktestProgress(p)) = req.payload {
        assert_eq!(p.chunks_completed, 2);
        assert_eq!(p.phase, "processing");
    } else {
        panic!("expected BacktestProgress payload");
    }
}

#[test]
fn test_publish_request_backtest_cancel_request_payload() {
    let cancel = BacktestCancelRequest {
        job_id: "job-1".to_string(),
        reason: "user cancelled".to_string(),
    };
    let req = PublishRequest {
        topic: "backtest.cancel".to_string(),
        payload: Some(Payload::BacktestCancelRequest(cancel)),
    };
    if let Some(Payload::BacktestCancelRequest(c)) = req.payload {
        assert_eq!(c.reason, "user cancelled");
    } else {
        panic!("expected BacktestCancelRequest payload");
    }
}

#[test]
fn test_publish_request_backtest_aggregated_result_payload() {
    let agg = BacktestAggregatedResult {
        job_id: "job-1".to_string(),
        total_net_pnl: 1_234.56,
        total_trades: 100,
        win_rate: 0.60,
        sharpe_ratio: 1.8,
        num_workers: 4,
        ..Default::default()
    };
    let req = PublishRequest {
        topic: "backtest.result".to_string(),
        payload: Some(Payload::BacktestAggregatedResult(agg)),
    };
    if let Some(Payload::BacktestAggregatedResult(a)) = req.payload {
        assert_eq!(a.total_trades, 100);
        assert!((a.win_rate - 0.60).abs() < 1e-6);
    } else {
        panic!("expected BacktestAggregatedResult payload");
    }
}

#[test]
fn test_publish_request_chromosome_eval_request_payload() {
    let eval_req = ChromosomeEvalRequest {
        job_id: "ga-job-1".to_string(),
        generation: 5,
        chromosome_id: 12,
        strategy_type: "market_making".to_string(),
        initial_capital: 50_000.0,
        ..Default::default()
    };
    let req = PublishRequest {
        topic: "ga.eval.request".to_string(),
        payload: Some(Payload::ChromosomeEvalRequest(eval_req)),
    };
    if let Some(Payload::ChromosomeEvalRequest(e)) = req.payload {
        assert_eq!(e.generation, 5);
        assert_eq!(e.strategy_type, "market_making");
    } else {
        panic!("expected ChromosomeEvalRequest payload");
    }
}

#[test]
fn test_publish_request_chromosome_eval_result_payload() {
    let eval_res = ChromosomeEvalResult {
        job_id: "ga-job-1".to_string(),
        generation: 5,
        chromosome_id: 12,
        success: true,
        fitness: 0.87,
        sharpe_ratio: 2.1,
        ..Default::default()
    };
    let req = PublishRequest {
        topic: "ga.eval.result".to_string(),
        payload: Some(Payload::ChromosomeEvalResult(eval_res)),
    };
    if let Some(Payload::ChromosomeEvalResult(r)) = req.payload {
        assert!(r.success);
        assert!((r.fitness - 0.87).abs() < 1e-9);
    } else {
        panic!("expected ChromosomeEvalResult payload");
    }
}

#[test]
fn test_publish_request_data_broadcast_request_payload() {
    let data_req = DataBroadcastRequest {
        cache_key: "btcusdt-2024".to_string(),
        symbol: "BTCUSDT".to_string(),
        exchange: "binance".to_string(),
        data_point_count: 1_000_000,
        compression: "lz4".to_string(),
        ttl_seconds: 3600,
        ..Default::default()
    };
    let req = PublishRequest {
        topic: "ga.data.broadcast".to_string(),
        payload: Some(Payload::DataBroadcastRequest(data_req)),
    };
    if let Some(Payload::DataBroadcastRequest(d)) = req.payload {
        assert_eq!(d.cache_key, "btcusdt-2024");
        assert_eq!(d.compression, "lz4");
    } else {
        panic!("expected DataBroadcastRequest payload");
    }
}

#[test]
fn test_publish_request_data_cache_ack_payload() {
    let ack = DataCacheAck {
        cache_key: "btcusdt-2024".to_string(),
        worker_id: "worker-1".to_string(),
        success: true,
        capacity: 8,
        tick_count: 1_000_000,
    };
    let req = PublishRequest {
        topic: "ga.data.cache.ack".to_string(),
        payload: Some(Payload::DataCacheAck(ack)),
    };
    if let Some(Payload::DataCacheAck(a)) = req.payload {
        assert!(a.success);
        assert_eq!(a.tick_count, 1_000_000);
    } else {
        panic!("expected DataCacheAck payload");
    }
}

#[test]
fn test_publish_request_data_load_request_payload() {
    let load_req = DataLoadRequest {
        cache_key: "btcusdt-2024".to_string(),
        exchange: "binance".to_string(),
        start_time: "2024-01-01".to_string(),
        end_time: "2024-12-31".to_string(),
        job_id: "ga-job-1".to_string(),
        ttl_seconds: 7200,
        initial_capital: 10_000.0,
        ..Default::default()
    };
    let req = PublishRequest {
        topic: "ga.data.load".to_string(),
        payload: Some(Payload::DataLoadRequest(load_req)),
    };
    if let Some(Payload::DataLoadRequest(l)) = req.payload {
        assert_eq!(l.exchange, "binance");
        assert_eq!(l.ttl_seconds, 7200);
    } else {
        panic!("expected DataLoadRequest payload");
    }
}

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

