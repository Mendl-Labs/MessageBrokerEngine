#!/bin/bash
HOST_DATA="/var/lib/rancher/k3s/storage/pvc-e4a3e012-e8cd-4388-b9c7-62710601a9ef_backtestingengine_backtesting-engine-market-data"

# The worker's fuzzy match does: name.to_uppercase().contains(&sym_upper) && name.to_uppercase().contains(&exchange_upper)
# sym_upper = "BTC/USDT" (has slash)
# Files have BTCUSDT (no slash)
# "BINANCE_BTCUSDT_TRADES.CSV.GZ".contains("BTC/USDT") = false
#
# We can't create files with / in the name. But we CAN override the symbol at the API level.
# 
# Actually, the simplest approach: just build a patched image with the fix and use that.
# We already have the v2 commit. Let's just also fix the symbol matching while we're at it.
echo "done"
