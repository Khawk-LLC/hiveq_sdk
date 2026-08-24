"""Ten-year futures daily-bar validation using 18:00-17:00 ET sessions."""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import math
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import open_positions as open_position_rows, completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


SYMBOL = "ES.v.0"


class SdkT55FuturesSessionDailyBars:
    def __init__(self):
        self.current_day = None
        self.daily = None
        self.minute_keys = set()
        self.daily_keys = set()
        self.previous_ts = None
        self.state = {
            "minute_bars": 0, "daily_bars": 0, "duplicate_minutes": 0,
            "duplicate_days": 0, "non_monotonic": 0, "invalid_ohlc": 0,
            "outside_session": 0, "filled_orders": 0, "rejected_orders": 0,
            "first_day": None, "last_day": None, "samples": [],
        }
        self.cycle_open = False

    def on_start(self, ctx, event):
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.FUTURES, interval="1m")

    @staticmethod
    def _session_day(value):
        # Futures trading day: prior calendar day at 18:00 ET through the
        # named trading day at 17:00 ET.
        return value.date() + timedelta(days=1) if value.hour >= 18 else value.date()

    def on_bar(self, ctx, event):
        bar = event.data()
        day = self._session_day(bar.time)
        if self.current_day != day:
            self._flush(ctx)
            self.current_day = day
            self.minute_keys = set()
            self.daily = {
                "day": str(day), "open": float(bar.open), "high": float(bar.high),
                "low": float(bar.low), "close": float(bar.close),
                "volume": float(bar.volume), "count": 0,
                "first_time": bar.time.isoformat(), "last_time": bar.time.isoformat(),
                "contracts": set(),
            }

        key = (str(bar.symbol), int(bar.ts_event))
        if key in self.minute_keys:
            self.state["duplicate_minutes"] += 1
            return
        self.minute_keys.add(key)
        self.state["minute_bars"] += 1
        if self.previous_ts is not None and int(bar.ts_event) <= self.previous_ts:
            self.state["non_monotonic"] += 1
        self.previous_ts = int(bar.ts_event)

        start_date = day - timedelta(days=1)
        if bar.time.date() not in {start_date, day}:
            self.state["outside_session"] += 1
        elif bar.time.date() == start_date and bar.time.hour < 18:
            self.state["outside_session"] += 1
        elif (bar.time.date() == day and
              (bar.time.hour > 17 or (bar.time.hour == 17 and bar.time.minute > 0))):
            self.state["outside_session"] += 1

        row = self.daily
        row["high"] = max(row["high"], float(bar.high))
        row["low"] = min(row["low"], float(bar.low))
        row["close"] = float(bar.close)
        row["volume"] += float(bar.volume) if row["count"] else 0.0
        row["count"] += 1
        row["last_time"] = bar.time.isoformat()
        row["contracts"].add(str(bar.symbol))

    def _flush(self, ctx):
        if not self.daily:
            return
        row = self.daily
        key = row["day"]
        if key in self.daily_keys:
            self.state["duplicate_days"] += 1
        self.daily_keys.add(key)
        values = [row[x] for x in ("open", "high", "low", "close", "volume")]
        valid = (all(math.isfinite(x) for x in values) and row["volume"] >= 0
                 and row["high"] >= max(row["open"], row["close"], row["low"])
                 and row["low"] <= min(row["open"], row["close"], row["high"]))
        self.state["invalid_ohlc"] += int(not valid)
        self.state["daily_bars"] += 1
        self.state["first_day"] = self.state["first_day"] or key
        self.state["last_day"] = key
        serial = {**row, "contracts": sorted(row["contracts"])}
        if len(self.state["samples"]) < 5:
            self.state["samples"].append(serial)
        ctx.add_event_log(
            f"ES.v.0 session daily bar {key}", sub_event_type="FUTURES_DAILY_BAR",
            symbol=SYMBOL, state_variable=serial,
        )
        if not self.cycle_open:
            self.cycle_open = ctx.buy_order(SYMBOL, 1.0) is not None
        else:
            self.cycle_open = not (ctx.close_position(SYMBOL) is not None)
        self.daily = None

    def on_order(self, ctx, event):
        order = event.data()
        if order.is_filled:
            self.state["filled_orders"] += 1
        elif "REJECT" in str(order.status).upper():
            self.state["rejected_orders"] += 1

    def on_stop(self, ctx, event):
        self._flush(ctx)
        emit_checkpoint(ctx, "t55_futures_session_daily_bars", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT55FuturesSessionDailyBars", type="SdkT55FuturesSessionDailyBars",
            symbols=[SYMBOL],
        )],
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(
            symbols=[SYMBOL], start_date="2016-01-01", end_date="2025-12-31",
            initial_capital=1_000_000.0, session_start="18:00", session_end="17:00",
            enable_auto_rollover=False, auto_flatten_at_close=False,
        ),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}", flush=True)
    state = completed_checkpoint(run, "t55_futures_session_daily_bars")
    positions = run.positions()
    open_positions = open_position_rows(positions)
    finish("t55_futures_session_daily_bars", {
        "ten_year_daily_data_present": state["daily_bars"] >= 2400,
        "minute_source_data_present": state["minute_bars"] > 1_000_000,
        "no_duplicate_source_bars": state["duplicate_minutes"] == 0,
        "no_duplicate_session_days": state["duplicate_days"] == 0,
        "timestamps_strictly_increase": state["non_monotonic"] == 0,
        "all_bars_inside_18_to_17_session": state["outside_session"] == 0,
        "daily_ohlcv_valid": state["invalid_ohlc"] == 0,
        "strategy_traded": state["filled_orders"] == (state["daily_bars"] // 2) * 2,
        "no_order_rejections": state["rejected_orders"] == 0,
        "final_position_flat": open_positions.empty,
    }, extra=str(state))
