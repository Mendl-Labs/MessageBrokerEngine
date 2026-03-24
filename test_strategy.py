from trading_platform import BaseStrategy, Signal
import numpy as np

class Strategy(BaseStrategy):
    def name(self):
        return "BollingerRSIMomentum"

    def state_schema(self):
        return {
            "prices": {"type": "window[200]", "default": []},
            "volumes": {"type": "window[200]", "default": []},
            "in_position": {"type": "bool", "default": False},
            "position_side": {"type": "str", "default": "none"},
            "entry_price": {"type": "float", "default": 0.0},
            "entry_tick": {"type": "int", "default": 0},
            "trailing_stop": {"type": "float", "default": 0.0},
            "peak_since_entry": {"type": "float", "default": 0.0},
        }

    def parameter_space(self):
        return {
            "bb_period": {"type": "int", "min": 10, "max": 50, "default": 20},
            "bb_std": {"type": "float", "min": 1.0, "max": 3.0, "default": 2.0},
            "rsi_period": {"type": "int", "min": 5, "max": 30, "default": 14},
            "rsi_oversold": {"type": "float", "min": 15.0, "max": 40.0, "default": 30.0},
            "rsi_overbought": {"type": "float", "min": 60.0, "max": 85.0, "default": 70.0},
            "volume_threshold": {"type": "float", "min": 0.5, "max": 2.0, "default": 1.2},
            "confirmation_required": {"type": "int", "min": 1, "max": 3, "default": 2},
            "trailing_stop_pct": {"type": "float", "min": 0.005, "max": 0.05, "default": 0.02},
            "profit_target_pct": {"type": "float", "min": 0.01, "max": 0.10, "default": 0.03},
            "max_hold_ticks": {"type": "int", "min": 50, "max": 500, "default": 200},
        }

    def generate_signals(self, data, context):
        prices = context["prices"]
        volumes = context["volumes"]
        tick = context["tick_number"]
        price = data["price"]

        if len(prices) < self.params.get("bb_period", 20) + 5:
            return []

        bb_period = self.params.get("bb_period", 20)
        bb_std_mult = self.params.get("bb_std", 2.0)
        rsi_period = self.params.get("rsi_period", 14)
        rsi_oversold = self.params.get("rsi_oversold", 30.0)
        rsi_overbought = self.params.get("rsi_overbought", 70.0)
        volume_threshold = self.params.get("volume_threshold", 1.2)
        confirmation_required = int(self.params.get("confirmation_required", 2))
        trailing_stop_pct = self.params.get("trailing_stop_pct", 0.02)
        profit_target_pct = self.params.get("profit_target_pct", 0.03)
        max_hold_ticks = int(self.params.get("max_hold_ticks", 200))

        prices_arr = np.array(prices[-bb_period:], dtype=np.float64)
        bb_mean = np.mean(prices_arr)
        bb_std = np.std(prices_arr)
        upper_band = bb_mean + bb_std_mult * bb_std
        lower_band = bb_mean - bb_std_mult * bb_std

        deltas = np.diff(np.array(prices[-(rsi_period+1):], dtype=np.float64))
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        avg_gain = np.mean(gains) if len(gains) > 0 else 0.0
        avg_loss = np.mean(losses) if len(losses) > 0 else 0.0
        rsi = 100.0 - (100.0 / (1.0 + avg_gain / max(avg_loss, 1e-10)))

        vol_arr = np.array(volumes[-bb_period:], dtype=np.float64)
        avg_volume = np.mean(vol_arr)
        current_volume = volumes[-1] if len(volumes) > 0 else 0.0
        high_volume = current_volume > avg_volume * volume_threshold

        if not self.in_position:
            buy_signals = 0
            sell_signals = 0
            if price < lower_band:
                buy_signals += 1
            if rsi < rsi_oversold:
                buy_signals += 1
            if high_volume:
                buy_signals += 1
                sell_signals += 1
            if price > upper_band:
                sell_signals += 1
            if rsi > rsi_overbought:
                sell_signals += 1
            if buy_signals >= confirmation_required:
                self.in_position = True
                self.position_side = "long"
                self.entry_price = price
                self.entry_tick = tick
                self.trailing_stop = price * (1.0 - trailing_stop_pct)
                self.peak_since_entry = price
                return [Signal.buy(0.1)]
            elif sell_signals >= confirmation_required:
                self.in_position = True
                self.position_side = "short"
                self.entry_price = price
                self.entry_tick = tick
                self.trailing_stop = price * (1.0 + trailing_stop_pct)
                self.peak_since_entry = price
                return [Signal.sell(0.1)]
        else:
            ticks_held = tick - self.entry_tick
            if self.position_side == "long":
                self.peak_since_entry = max(self.peak_since_entry, price)
                self.trailing_stop = max(self.trailing_stop, self.peak_since_entry * (1.0 - trailing_stop_pct))
                pnl_pct = (price - self.entry_price) / max(self.entry_price, 1e-10)
                if price <= self.trailing_stop or pnl_pct >= profit_target_pct or ticks_held >= max_hold_ticks:
                    self.in_position = False
                    self.position_side = "none"
                    return [Signal.close()]
            elif self.position_side == "short":
                self.peak_since_entry = min(self.peak_since_entry, price)
                self.trailing_stop = min(self.trailing_stop, self.peak_since_entry * (1.0 + trailing_stop_pct))
                pnl_pct = (self.entry_price - price) / max(self.entry_price, 1e-10)
                if price >= self.trailing_stop or pnl_pct >= profit_target_pct or ticks_held >= max_hold_ticks:
                    self.in_position = False
                    self.position_side = "none"
                    return [Signal.close()]
        return []
