#!/bin/bash
# Create symlinks on the HOST filesystem so the data files match the normalized symbol format
HOST_DATA=/var/lib/rancher/k3s/storage/pvc-e4a3e012-e8cd-4388-b9c7-62710601a9ef_backtestingengine_backtesting-engine-market-data
cd "$HOST_DATA"
# BTC/USDT -> BTCUSDT files already exist, just create symlinks with the slash replaced
# The worker searches for exact: binance_BTC/USDT_trades.csv.gz (won't work on filesystem)
# Fuzzy: filename must contain "BTC/USDT" (won't match BTCUSDT)
# Actually we need to fix the symbol. Let's just symlink without slash
# The real fix: strip / from symbol before matching. For now, create files with dash
for f in *_trades.csv.gz; do
    echo "File: $f"
done
