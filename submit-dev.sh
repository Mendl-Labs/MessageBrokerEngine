#!/bin/bash
POD=$(kubectl get pod -n backtesting-dev -l app=dev-api -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n backtesting-dev "$POD" -- curl -s -X POST http://localhost:8080/api/backtest -H 'Content-Type: application/json' -d @/tmp/job.json
