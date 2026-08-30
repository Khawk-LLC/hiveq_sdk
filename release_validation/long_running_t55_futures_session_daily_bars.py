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
            "outside_session": 0, "orders_placed": 0, "filled_orders": 0,
            "rejected_orders": 0, "reject_reasons": {}, "rejected_days": [],
            "final_position": 0.0, "contracts_traded": [],
            "sessions_traded": 0,
            "first_day": None, "last_day": None, "samples": [],
        }

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
            self._flush(ctx, str(bar.symbol))
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

    def _flush(self, ctx, next_contract=None, trade=True):
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
        # Both legs in the same session, on the contract the callbacks are
        # actually delivering.
        #
        # Nothing may be carried overnight here. With
        # enable_auto_rollover=False a held position does not migrate, and
        # closing through the continuous alias is then refused -- "active
        # contract is ESM0, but non-active physical position(s) remain open
        # (ESH6=1)". Closing the *named* contract instead only moved the
        # problem: one failed close left the cycle waiting on a contract that
        # had expired, so a run placed 122 orders across 2566 sessions and
        # stopped trading in June 2016. A flat round trip per session cannot
        # deadlock and leaves rollover behaviour to the cases that test it
        # (t08/t38/t49-t52).
        contracts = sorted(row["contracts"])
        contract = next_contract or (contracts[-1] if contracts else SYMBOL)
        if trade:
            for leg in (ctx.buy_order(contract, 1.0), ctx.sell_order(contract, 1.0)):
                if leg is not None:
                    self.state["orders_placed"] += 1
            self.state["sessions_traded"] += 1
            self.state["contracts_traded"] = sorted(
                set(self.state["contracts_traded"]) | {contract}
            )[-6:]
        self.daily = None

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
            if len(self.state["rejected_days"]) < 40:
                stamp = order.time.isoformat() if order.time else ""
                self.state["rejected_days"].append(
                    [stamp, str(order.side).upper(), reason]
                )

    def on_stop(self, ctx, event):
        # The last flush happens at on_stop, after the final session has ended,
        # so its round trip cannot be placed -- the previous run came out two
        # orders short of "two per session" for exactly that reason. Record the
        # session, skip the trade.
        self._flush(ctx, trade=False)
        self.state["final_position"] = float(ctx.net_position(SYMBOL))
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
    placed = state["orders_placed"]
    finish("t55_futures_session_daily_bars", {
        "ten_year_daily_data_present": state["daily_bars"] >= 2400,
        "minute_source_data_present": state["minute_bars"] > 1_000_000,
        "no_duplicate_source_bars": state["duplicate_minutes"] == 0,
        "no_duplicate_session_days": state["duplicate_days"] == 0,
        "timestamps_strictly_increase": state["non_monotonic"] == 0,
        "all_bars_inside_18_to_17_session": state["outside_session"] == 0,
        "daily_ohlcv_valid": state["invalid_ohlc"] == 0,
        "every_session_traded": (
            state["sessions_traded"] == state["daily_bars"] - 1
        ),
        "two_orders_per_traded_session": placed == state["sessions_traded"] * 2,
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
        # Both legs are placed in the same session, so nothing may be held.
        "flat_after_every_session": state["final_position"] == 0.0,
        "positions_reconcilable": open_position_rows(positions).empty,
    }, extra=str(state))
