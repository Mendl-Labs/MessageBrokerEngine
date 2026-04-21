#!/bin/bash
HOST_DATA="/var/lib/rancher/k3s/storage/pvc-e4a3e012-e8cd-4388-b9c7-62710601a9ef_backtestingengine_backtesting-engine-market-data"

# The GA worker tries: {exchange}_{symbol}_trades.csv.gz with symbol as-is from DB (BTC/USDT)
# But filesystem can't have / in filename. The fuzzy match does name.contains("BTC/USDT") which won't match BTCUSDT.
# 
# Solution: Create copies/symlinks with dash instead of slash, and hope fuzzy match catches it.
# Actually no - fuzzy match looks for the exact sym_upper string in the filename.
# We need the symbol WITHOUT the slash to appear in filenames.
#
# The only way is to remount as read-write and create symlinks using a name that the exact match will find.
# But exact match uses the symbol verbatim which has / - can't create that filename.
#
# REAL solution: We need to strip / from sym_upper in the fuzzy match. Since we can't change code,
# let's check if there's an env var or config that affects symbol normalization.
#
# Alternative: check if worker code also has a symbol stripping step we're missing.
echo "Checking if there are any other symbol-related env vars or configs..."
kubectl exec -n backtesting-dev deployment/dev-worker -- env | grep -iE 'symbol|normalize'
kubectl exec -n backtesting-dev deployment/dev-worker -- cat /app/config.toml 2>/dev/null || echo "No config.toml"
