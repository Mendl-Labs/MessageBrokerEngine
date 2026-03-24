"""Submit a test backtest to the local backtesting engine."""
import json
import urllib.request

STRATEGY = r'''"""
Bollinger Band + RSI + Volume Confirmation Strategy
"""

from trading_platform import BaseStrategy, Signal
import numpy as np


class Strategy(BaseStrategy):
    def __init__(self):
        self.params = {
            "bb_period": 20,
            "bb_std": 2.0,
            "rsi_period": 14,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
            "volume_spike_mult": 1.5,
            "atr_period": 14,
            "risk_per_trade": 0.02,
            "trailing_stop_atr_mult": 2.0,
            "max_hold_ticks": 5000,
        }

    def name(self):
        return "BollingerRSIMomentum"

    def state_schema(self):
        return {
            "prices":           {"type": "window[float]", "capacity": 200},
            "volumes":          {"type": "window[float]", "capacity": 200},
            "in_position":      {"type": "bool",  "default": False},
            "position_side":    {"type": "enum",  "values": ["flat", "long", "short"], "default": "flat"},
            "entry_price":      {"type": "float", "default": 0.0},
            "entry_tick":       {"type": "int",   "default": 0},
            "trailing_stop":    {"type": "float", "default": 0.0},
            "peak_since_entry": {"type": "float", "default": 0.0},
        }

    def parameter_space(self):
        return {
            "bb_period":             {"type": "int",   "min": 10,   "max": 50,   "default": 20},
            "bb_std":                {"type": "float", "min": 1.0,  "max": 3.5,  "default": 2.0},
            "rsi_period":            {"type": "int",   "min": 7,    "max": 28,   "default": 14},
            "rsi_oversold":          {"type": "int",   "min": 15,   "max": 40,   "default": 30},
            "rsi_overbought":        {"type": "int",   "min": 60,   "max": 85,   "default": 70},
            "volume_spike_mult":     {"type": "float", "min": 1.0,  "max": 3.0,  "default": 1.5},
            "atr_period":            {"type": "int",   "min": 7,    "max": 28,   "default": 14},
            "risk_per_trade":        {"type": "float", "min": 0.005,"max": 0.05, "default": 0.02},
            "trailing_stop_atr_mult":{"type": "float", "min": 1.0,  "max": 4.0,  "default": 2.0},
            "max_hold_ticks":        {"type": "int",   "min": 1000, "max": 20000,"default": 5000},
        }

    def update_parameters(self, params):
        self.params.update(params)

    def generate_signals(self, data, context):
        prices = context["prices"]
        volumes = context["volumes"]
        tick = context["tick_number"]
        portfolio = context["portfolio"]
        price = data["price"]
        volume = data["volume"]

        warmup = max(self.params["bb_period"], self.params["rsi_period"], self.params["atr_period"])
        if len(prices) < warmup + 1:
            return Signal.hold()

        p = np.array(prices[-self.params["bb_period"]:])
        sma = np.mean(p)
        std = np.std(p, ddof=1) if len(p) > 1 else 0.0001
        upper_band = sma + self.params["bb_std"] * std
        lower_band = sma - self.params["bb_std"] * std

        rsi = self._compute_rsi(prices, self.params["rsi_period"])

        vol_arr = np.array(volumes[-self.params["atr_period"]:])
        avg_volume = np.mean(vol_arr) if len(vol_arr) > 0 else 1.0
        volume_spike = volume > avg_volume * self.params["volume_spike_mult"]

        atr = self._compute_atr(prices, self.params["atr_period"])

        if self.in_position:
            hold_duration = tick - self.entry_tick

            if self.position_side == "long":
                self.peak_since_entry = max(self.peak_since_entry, price)
                self.trailing_stop = self.peak_since_entry - self.params["trailing_stop_atr_mult"] * atr

                if price <= self.trailing_stop:
                    self._reset_position()
                    return Signal.close(reason="trailing stop (long)")
                if price >= sma and hold_duration > 50:
                    self._reset_position()
                    return Signal.close(reason="mean reversion target")
                if hold_duration > self.params["max_hold_ticks"]:
                    self._reset_position()
                    return Signal.close(reason="max hold time")

            elif self.position_side == "short":
                self.peak_since_entry = min(self.peak_since_entry, price)
                self.trailing_stop = self.peak_since_entry + self.params["trailing_stop_atr_mult"] * atr

                if price >= self.trailing_stop:
                    self._reset_position()
                    return Signal.close(reason="trailing stop (short)")
                if price <= sma and hold_duration > 50:
                    self._reset_position()
                    return Signal.close(reason="mean reversion target")
                if hold_duration > self.params["max_hold_ticks"]:
                    self._reset_position()
                    return Signal.close(reason="max hold time")

            return Signal.hold()

        position_size = self._size_by_volatility(portfolio["equity"], atr, price)

        bb_long = price <= lower_band
        rsi_long = rsi < self.params["rsi_oversold"]
        long_signals = int(bb_long) + int(rsi_long) + int(volume_spike)
        if long_signals >= 2:
            self.in_position = True
            self.position_side = "long"
            self.entry_price = price
            self.entry_tick = tick
            self.peak_since_entry = price
            self.trailing_stop = price - self.params["trailing_stop_atr_mult"] * atr
            reasons = [s for s, v in [("BB lower", bb_long), ("RSI oversold", rsi_long), ("vol spike", volume_spike)] if v]
            return Signal.buy(quantity=position_size, reason=" + ".join(reasons))

        bb_short = price >= upper_band
        rsi_short = rsi > self.params["rsi_overbought"]
        short_signals = int(bb_short) + int(rsi_short) + int(volume_spike)
        if short_signals >= 2:
            self.in_position = True
            self.position_side = "short"
            self.entry_price = price
            self.entry_tick = tick
            self.peak_since_entry = price
            self.trailing_stop = price + self.params["trailing_stop_atr_mult"] * atr
            reasons = [s for s, v in [("BB upper", bb_short), ("RSI overbought", rsi_short), ("vol spike", volume_spike)] if v]
            return Signal.sell(quantity=position_size, reason=" + ".join(reasons))

        return Signal.hold()

    def _compute_rsi(self, prices, period):
        if len(prices) < period + 1:
            return 50.0
        deltas = [prices[-i] - prices[-i - 1] for i in range(1, period + 1)]
        gains = [d for d in deltas if d > 0]
        losses = [-d for d in deltas if d < 0]
        avg_gain = sum(gains) / period if gains else 0.0001
        avg_loss = sum(losses) / period if losses else 0.0001
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _compute_atr(self, prices, period):
        if len(prices) < period + 1:
            return abs(prices[-1]) * 0.01
        ranges = [abs(prices[-i] - prices[-i - 1]) for i in range(1, period + 1)]
        return sum(ranges) / len(ranges)

    def _size_by_volatility(self, equity, atr, price):
        if atr <= 0 or price <= 0:
            return 0.01
        risk_amount = equity * self.params["risk_per_trade"]
        stop_distance = self.params["trailing_stop_atr_mult"] * atr
        raw_size = risk_amount / stop_distance
        max_size = equity * 0.1 / price
        return round(min(raw_size, max_size), 6)

    def _reset_position(self):
        self.in_position = False
        self.position_side = "flat"
        self.entry_price = 0.0
        self.trailing_stop = 0.0
        self.peak_since_entry = 0.0
'''

body = {
    "strategy_type": "custom",
    "optimization_method": "genetic",
    "initial_capital": 10000.0,
    "exchange": "kraken",
    "symbol": "BTC-USD",
    "python_source_code": STRATEGY,
    "data_config": {
        "exchange": "kraken",
        "symbol": "BTC-USD",
        "date_range": {
            "start": "2024-01-01",
            "end": "2024-03-31"
        }
    }
}

data = json.dumps(body).encode("utf-8")
req = urllib.request.Request(
    "http://localhost:3000/api/backtest",
    data=data,
    headers={"Content-Type": "application/json"},
    method="POST"
)

try:
    with urllib.request.urlopen(req) as resp:
        print(f"Status: {resp.status}")
        print(resp.read().decode())
except urllib.error.HTTPError as e:
    print(f"HTTP Error: {e.code}")
    print(e.read().decode())
except Exception as e:
    print(f"Error: {e}")
