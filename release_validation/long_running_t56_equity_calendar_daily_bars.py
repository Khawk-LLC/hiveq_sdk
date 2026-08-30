"""Ten-year native equity daily bars: one valid bar per ET calendar date."""
from __future__ import annotations

from pathlib import Path
import math
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import open_positions as open_position_rows, completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


SYMBOL = "AAPL"


class SdkT56EquityCalendarDailyBars:
    def __init__(self):
        self.keys = set()
        self.previous_ts = None
        self.unique_bars = 0
        self.state = {
            "bars": 0, "unique_days": 0, "duplicate_days": 0,
            "duplicate_timestamps": 0, "non_monotonic": 0,
            "invalid_interval": 0, "invalid_ohlc": 0,
            "orders_placed": 0, "filled_orders": 0, "rejected_orders": 0,
            "reject_reasons": {}, "rejected_days": [],
            "final_position": 0.0,
            "first_day": None, "last_day": None, "samples": [],
        }
        self.current_day = None

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

        # The next leg is chosen from the actual position, not from whether the
        # previous order object came back: a rejected order used to flip the
        # flag anyway, so one reject desynchronized the buy/sell cycle for the
        # rest of the run and every later assertion inherited that.
        self.current_day = day
        order = (ctx.sell_order(SYMBOL, 1.0) if ctx.net_position(SYMBOL) > 0
                 else ctx.buy_order(SYMBOL, 1.0))
        if order is not None:
            self.state["orders_placed"] += 1

    def on_order(self, ctx, event):
        order = event.data()
        if order.is_filled:
            self.state["filled_orders"] += 1
        elif "REJECT" in str(order.status).upper():
            self.state["rejected_orders"] += 1
            reason = str(order.reject_reason or "unstated")
            self.state["reject_reasons"][reason] = (
                self.state["reject_reasons"].get(reason, 0) + 1
            )
            # Which sessions the engine refused, so the reviewer sees the
            # pattern (early closes) rather than only a count.
            if len(self.state["rejected_days"]) < 40:
                # The order's own timestamp, not the bar being processed: a
                # reject can arrive after the next session's bar has already
                # advanced the strategy's day.
                stamp = order.time.isoformat() if order.time else str(self.current_day)
                self.state["rejected_days"].append(
                    [stamp, str(order.side).upper(), reason]
                )

    def on_stop(self, ctx, event):
        self.state["final_position"] = float(ctx.net_position(SYMBOL))
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
            auto_flatten_at_close=False,
        ),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}", flush=True)
    state = completed_checkpoint(run, "t56_equity_calendar_daily_bars")
    positions = run.positions()
    placed = state["orders_placed"]
    finish("t56_equity_calendar_daily_bars", {
        "ten_year_daily_data_present": state["unique_days"] >= 2400,
        "no_duplicate_calendar_days": state["duplicate_days"] == 0,
        "no_duplicate_timestamps": state["duplicate_timestamps"] == 0,
        "timestamps_strictly_increase": state["non_monotonic"] == 0,
        "daily_interval_metadata_valid": state["invalid_interval"] == 0,
        "daily_ohlcv_valid": state["invalid_ohlc"] == 0,
        # One order per session, and every one of them resolved: the accounting
        # has to close before the fill rate means anything.
        "one_order_per_session": placed == state["unique_days"],
        "every_order_resolved": (
            state["filled_orders"] + state["rejected_orders"] == placed
        ),
        "strategy_traded": placed > 0 and state["filled_orders"] >= placed * 0.95,
        # A daily bar's market order is filled at the session's regular close,
        # so on an early-close session (Jul 3, the day after Thanksgiving,
        # Dec 24) that fill never happens and the order is rejected "Market
        # center not open" at session end. That is the venue being shut, not a
        # defect, so the contract is that every rejection says so and that they
        # stay as rare as the early-close calendar -- a rejection with any other
        # reason, or a rate above 2% of sessions, is a regression.
        "rejections_only_when_market_closed": (
            set(state["reject_reasons"]) <= {"Market center not open"}
        ),
        "rejections_no_more_common_than_early_closes": (
            state["rejected_orders"] <= max(1, int(placed * 0.02))
        ),
        # The alternating cycle may legitimately end holding the one share it
        # opened on the last session; anything beyond that is drift.
        "position_never_drifted": abs(state["final_position"]) <= 1.0,
        "positions_reconcilable": len(open_position_rows(positions)) <= 1,
    }, extra=str(state))
