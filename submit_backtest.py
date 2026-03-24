import json
import urllib.request

# Read the strategy file
with open(r"c:\Users\ikenn\Projects\TradingPlatform\test_strategy.py", "r") as f:
    strategy_code = f.read()

payload = {
    "python_source_code": strategy_code,
    "strategy_type": "custom",
    "exchange": "kraken",
    "symbol": "BTC-USD",
    "initial_capital": 100000,
    "population_size": 10,
    "generations": 5,
}

data = json.dumps(payload).encode("utf-8")
req = urllib.request.Request(
    "http://localhost:3000/api/backtest",
    data=data,
    headers={"Content-Type": "application/json"},
    method="POST"
)

try:
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        print(f"Job ID: {body.get('job_id')}")
        print(f"Status: {body.get('status')}")
        print(f"Message: {body.get('message')}")
except urllib.error.HTTPError as e:
    print(f"HTTP Error {e.code}: {e.read().decode('utf-8')}")
