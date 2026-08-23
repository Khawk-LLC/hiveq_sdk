"""Ten-year native equity daily bars: one valid bar per ET calendar date."""
from __future__ import annotations

from pathlib import Path
import math
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


SYMBOL = "AAPL"


class SdkT56EquityCalendarDailyBars:
    def __init__(self):
        self.keys = set()
        self.previous_ts = None
        self.unique_bars = 0
        self.cycle_open = False
        self.state = {
            "bars": 0, "unique_days": 0, "duplicate_days": 0,
            "duplicate_timestamps": 0, "non_monotonic": 0,
            "invalid_interval": 0, "invalid_ohlc": 0,
            "filled_orders": 0, "rejected_orders": 0,
            "first_day": None, "last_day": None, "samples": [],
        }

    def on_start(self, ctx, event):
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1d")

    def on_bar(self, ctx, event):
        bar = event.data()
        self.state["bars"] += 1
        day = str(bar.time.date())
        ts = int(bar.ts_event)
        if day in self.keys:
            self.state["duplicate_days"] += 1
        if self.previous_ts is not None and ts == self.previous_ts:
            self.state["duplicate_timestamps"] += 1
        elif self.previous_ts is not None and ts < self.previous_ts:
            self.state["non_monotonic"] += 1
        self.previous_ts = ts
        if day in self.keys:
            return
        self.keys.add(day)
        self.unique_bars += 1
        self.state["unique_days"] = self.unique_bars
        self.state["first_day"] = self.state["first_day"] or day
        self.state["last_day"] = day

        self.state["invalid_interval"] += int(
            str(bar.interval) != "1d" or int(bar.interval_millis) != 86_400_000
        )
        values = [float(getattr(bar, x)) for x in ("open", "high", "low", "close", "volume")]
        valid = (all(math.isfinite(x) for x in values) and values[4] >= 0
                 and values[1] >= max(values[0], values[2], values[3])
                 and values[2] <= min(values[0], values[1], values[3]))
        self.state["invalid_ohlc"] += int(not valid)
        row = {
            "calendar_day": day, "symbol": str(bar.symbol), "ts_event": ts,
            "time": bar.time.isoformat(), "open": values[0], "high": values[1],
            "low": values[2], "close": values[3], "volume": values[4],
            "interval": str(bar.interval), "interval_millis": int(bar.interval_millis),
        }
        if len(self.state["samples"]) < 5:
            self.state["samples"].append(row)
        ctx.add_event_log(
            f"AAPL calendar daily bar {day}", sub_event_type="EQUITY_DAILY_BAR",
            symbol=SYMBOL, state_variable=row,
        )

        if not self.cycle_open:
            self.cycle_open = ctx.buy_order(SYMBOL, 1.0) is not None
        else:
            self.cycle_open = not (ctx.sell_order(SYMBOL, 1.0) is not None)

    def on_order(self, ctx, event):
        order = event.data()
        if order.is_filled:
            self.state["filled_orders"] += 1
        elif "REJECT" in str(order.status).upper():
            self.state["rejected_orders"] += 1

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t56_equity_calendar_daily_bars", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT56EquityCalendarDailyBars", type="SdkT56EquityCalendarDailyBars",
            symbols=[SYMBOL],
        )],
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ",
                       "schema": ["bars_1d"]}],
        backtest_config=BacktestConfig(
            symbols=[SYMBOL], start_date="2016-01-01", end_date="2025-12-31",
            initial_capital=1_000_000.0, session_start="04:00", session_end="18:30",
            auto_flatten_at_close=False, export_orders_csv=True,
        ),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}", flush=True)
    state = completed_checkpoint(run, "t56_equity_calendar_daily_bars")
    positions = run.positions()
    open_positions = positions[positions["quantity"] != 0]
    finish("t56_equity_calendar_daily_bars", {
        "ten_year_daily_data_present": state["unique_days"] >= 2400,
        "no_duplicate_calendar_days": state["duplicate_days"] == 0,
        "no_duplicate_timestamps": state["duplicate_timestamps"] == 0,
        "timestamps_strictly_increase": state["non_monotonic"] == 0,
        "daily_interval_metadata_valid": state["invalid_interval"] == 0,
        "daily_ohlcv_valid": state["invalid_ohlc"] == 0,
        "strategy_traded": state["filled_orders"] == (state["unique_days"] // 2) * 2,
        "no_order_rejections": state["rejected_orders"] == 0,
        "final_position_flat": open_positions.empty,
    }, extra=str(state))
