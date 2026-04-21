#!/bin/bash
kubectl logs deployment/dev-api -n backtesting-dev --tail=50 --since=300s 2>&1 | grep -iE 'ef78b906|BTC-USD|coinbase|generation|stall' | tail -20
