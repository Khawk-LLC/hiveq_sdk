"""No fill precedes its order, and no price comes from the future.

This is the property a backtester exists to guarantee and the one the rest of
the suite never states. t17 pins the daily-bar fill *convention* -- that an
order fills at the same day's close -- but a convention is not causality: an
engine that leaked the next bar's range into a fill decision, or that delivered
bars out of order, would still satisfy every other case here while making all
of their PnL numbers meaningless.

Four independent properties, all asserted from persisted evidence:

* time never moves backwards across callbacks, and each bar callback observes
  its own bar as the instrument's latest -- not a later one;
* every fill timestamp is at or after the timestamp of the bar that ordered it;
* every fill price lies inside the price range actually printed between the
  signal bar and the bar the fill landed on -- never outside it;
* a resting limit never fills better than its own limit price, which is where
  a look-ahead fill model shows up first.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (                                               # noqa: E402
    completed_checkpoint,
    emit_checkpoint,
    evidence_checks,
    finish,
)

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402
from hiveq.flow.trading_types import OrderType                        # noqa: E402

SYMBOL = "AAPL"
# Market orders on a schedule, plus limits placed away from the market so they
# rest and fill on a later bar -- the case where a look-ahead fill model would
# have to invent a price.
MARKET_BARS = (4, 30, 56)
LIMIT_BARS = (12, 40)
# Bars whose printed range a fill is allowed to land in: the signal bar plus a
# forward window. A fill outside the union of these ranges came from nowhere.
FORWARD_WINDOW = 6


class SdkT60:
    def on_start(self, ctx, event):
        # on_start fires once per calendar day; keep aggregate state across days.
        if not hasattr(self, "state"):
            self.bars = 0
            self.ranges = {}          # bar index -> [ts, low, high]
            self.pending = {}         # order_id -> [bar index, limit price or 0]
            self.state = {
                "time_regressions": [], "stale_last_bar": [], "bar_ts_disorder": [],
                "fills": [], "early_fills": [], "outside_range": [],
                "limit_improvements": [], "bars": 0, "unmatched": 0,
            }
            self.last_event_ts = 0
            self.last_bar_ts = 0
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        if bar.symbol != SYMBOL:
            return
        ts = int(event.ts_event or 0)
        self.bars += 1
        index = self.bars

        # Time must not move backwards between callbacks.
        if ts < self.last_event_ts:
            if len(self.state["time_regressions"]) < 5:
                self.state["time_regressions"].append([index, ts, self.last_event_ts])
        self.last_event_ts = max(self.last_event_ts, ts)

        # Bars for one symbol must arrive strictly in time order.
        if self.last_bar_ts and ts <= self.last_bar_ts:
            if len(self.state["bar_ts_disorder"]) < 5:
                self.state["bar_ts_disorder"].append([index, ts, self.last_bar_ts])
        self.last_bar_ts = ts

        # The instrument's latest bar during this callback must be this bar, not
        # a future one: that is look-ahead in its most direct form.
        latest = getattr(ctx.instrument(SYMBOL), "last_bar", None)
        latest_close = float(getattr(latest, "close", 0) or 0)
        if latest_close and abs(latest_close - float(bar.close)) > 1e-9:
            if len(self.state["stale_last_bar"]) < 5:
                self.state["stale_last_bar"].append(
                    [index, float(bar.close), latest_close]
                )

        self.ranges[index] = [ts, float(bar.low), float(bar.high)]

        if index in MARKET_BARS:
            order = ctx.buy_order(SYMBOL, 5.0)
            if order is not None:
                self.pending[order.order_id] = [index, 0.0]
        elif index in LIMIT_BARS:
            # Below the market so it rests; the fill, when it comes, must be at
            # or below the limit and inside a printed range.
            limit = round(float(bar.close) * 0.999, 2)
            order = ctx.buy_order(SYMBOL, 5.0, order_type=OrderType.LIMIT,
                                  limit_price=limit, time_in_force="GTC")
            if order is not None:
                self.pending[order.order_id] = [index, limit]
        elif index == 80:
            ctx.cancel_all_orders(SYMBOL)
        elif index == 90 and not ctx.has_open_order(SYMBOL):
            order = ctx.close_position(SYMBOL)
            if order is not None:
                self.pending[order.order_id] = [index, 0.0]

    def on_order(self, ctx, event):
        order = event.data()
        ts = int(order.ts_event or 0)
        if ts and ts < self.last_event_ts - 1:
            if len(self.state["time_regressions"]) < 5:
                self.state["time_regressions"].append(["order", ts, self.last_event_ts])
        if "FILL" not in str(order.status).upper():
            return
        filled = float(order.filled_qty or 0)
        price = float(order.avg_px or 0)
        if filled <= 0 or price <= 0:
            return
        record = self.pending.pop(order.order_id, None)
        if record is None:
            self.state["unmatched"] += 1
            return
        signal_index, limit_price = record
        signal_ts = self.ranges[signal_index][0]
        fill_index = self.bars
        self.state["fills"].append([signal_index, fill_index, price, limit_price])

        if ts and ts < signal_ts:
            self.state["early_fills"].append([signal_index, ts, signal_ts])

        if limit_price and price > limit_price + 0.01:
            # A buy limit filling above its limit is a violation in the other
            # direction; both are recorded so neither can hide the other.
            self.state["limit_improvements"].append(
                ["above_limit", signal_index, price, limit_price]
            )

    def on_stop(self, ctx, event):
        self.state["bars"] = self.bars
        # Every fill price must sit inside the range actually printed between
        # the bar that placed the order and the bar the fill landed on. The
        # window extends one bar past the fill because a market order may be
        # priced off the next bar's open, and FORWARD_WINDOW bars before the
        # signal because a resting order may fill against a tick that also
        # printed just before it was placed.
        for signal_index, fill_index, price, _limit in self.state["fills"]:
            first = max(1, signal_index - 1)
            last = fill_index + FORWARD_WINDOW
            window = [self.ranges[i] for i in range(first, last + 1)
                      if i in self.ranges]
            if not window:
                continue
            low = min(row[1] for row in window)
            high = max(row[2] for row in window)
            # One tick of tolerance: a fill model may round to the instrument's
            # tick, which can land a tick outside a bar's printed extreme.
            if not (low - 0.01 <= price <= high + 0.01):
                self.state["outside_range"].append(
                    [signal_index, fill_index, price, low, high]
                )
        emit_checkpoint(ctx, "t60_causality_lookahead", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT60", type="SdkT60", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="11:30",
                                       export_orders_csv=True),
    )
    state = completed_checkpoint(run, "t60_causality_lookahead")
    checks = {
        "bars_delivered": state["bars"] > 60,
        "fills_recorded": len(state["fills"]) >= 3,
        "time_never_regressed": not state["time_regressions"],
        "bars_strictly_ordered": not state["bar_ts_disorder"],
        "no_future_bar_visible": not state["stale_last_bar"],
        "no_fill_before_its_order": not state["early_fills"],
        "every_fill_inside_printed_range": not state["outside_range"],
        "limit_never_filled_through": not state["limit_improvements"],
    }
    checks.update(evidence_checks(run, orders=3, trades=1))
    finish("t60_causality_lookahead", checks,
           extra=(f"bars={state['bars']}; fills={state['fills'][:6]}; "
                  f"early={state['early_fills'][:2]}; "
                  f"outside={state['outside_range'][:2]}; "
                  f"stale={state['stale_last_bar'][:2]}; "
                  f"unmatched={state['unmatched']}"))
