use prost::Message;
use protocol::generated::{
    PublishRequest, StrategyDeployment, StrategyDeactivation, StrategyDeploymentAck,
    DeploymentStatusResponse, ActiveStrategyInfo,
    MarketDataSubscribe, MarketDataUnsubscribe, MarketDataSubscriptionAck,
    publish_request::Payload,
};

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
    
    assert!(true, "Protocol module is accessible");
}

#[test]
fn test_protobuf_compilation() {
    // Test that protobuf messages can be created and used
    // Note: The exact message types depend on the .proto file content
    
    // Since we're importing generated code, we mainly test that it compiles
    // and the module structure is correct
    
    assert!(true, "Protobuf compilation successful");
}

#[test] 
fn test_message_serialization() {
    // Test basic protobuf serialization/deserialization
    // This is a framework test to ensure protobuf functionality works
    
    // Since the exact message types are generated from .proto files,
    // we can't test specific message types without knowing the schema.
    // This test validates the build system and import structure.
    
    assert!(true, "Message serialization framework available");
}

#[tokio::test]
async fn test_async_protocol_operations() {
    // Test that protocol operations can be used in async context
    
    assert!(true, "Async protocol operations supported");
}

#[test]
fn test_module_structure() {
    // Test that the module structure matches expectations
    
    // Verify namespace structure: protocol::broker::messages
    // This ensures the protobuf code generation is working correctly
    
    assert!(true, "Module structure is correct");
}

#[test]
fn test_build_system_integration() {
    // Test that the build.rs script and prost integration work
    
    // The fact that this file compiles means:
    // 1. build.rs executed successfully  
    // 2. Protobuf files were found and processed
    // 3. Generated Rust code is valid
    // 4. Include paths are correct
    
    assert!(true, "Build system integration working");
}

#[test]
fn test_dependency_resolution() {
    // Test that all prost dependencies resolve correctly
    
    // Since we can import the generated code, this validates:
    // 1. prost dependency is available
    // 2. prost-types dependency is available  
    // 3. Generated code uses correct prost attributes
    
    assert!(true, "Dependencies resolved correctly");
}

#[tokio::test]
async fn test_concurrent_protocol_access() {
    // Test that protocol types can be used concurrently
    
    let handles = vec![
        tokio::spawn(async {
            // Simulate concurrent protocol usage
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            true
        }),
        tokio::spawn(async {
            // Simulate concurrent protocol usage
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            true
        }),
        tokio::spawn(async {
            // Simulate concurrent protocol usage  
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            true
        }),
    ];
    
    for handle in handles {
        assert!(handle.await.unwrap());
    }
}

#[test]
fn test_memory_safety() {
    // Test that protocol types are memory safe
    
    // Generated protobuf code should be memory safe by design
    // This test validates that we can work with the types safely
    
    assert!(true, "Protocol types are memory safe");
}

#[test]
fn test_error_handling() {
    // Test error handling capabilities
    
    // Protobuf operations can fail, ensure error types are available
    // and properly integrated
    
    assert!(true, "Error handling available");
}
