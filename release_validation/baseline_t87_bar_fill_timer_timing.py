"""Bar-fill mode: an order sent from on_timer never fills before it was sent.

With only bars subscribed the paper broker fills against bars (bar-fill mode).
Timers used to fire *before* the broker consumed the bar being delivered, so an
order sent from on_timer at 14:40:00 filled against the 14:39-14:40 bar: fill
stamped 14:39, priced off a bar that closed before the order existed (sigma
08bc5f2b). t60 never caught it: it sends from on_bar, which always ran after
the broker.

AAPL 1m bars only, orders sent exclusively from on_timer:

  14:40:00  MARKET to +1   timer on a minute boundary (the bar-end case)
  14:50:00  MARKET to  0
  14:55:30  MARKET to +1   timer mid-bar
  15:05:30  MARKET to  0
  15:10:00  LIMIT buy 1, marketable (last close + 0.50)
  15:20:00  MARKET to  0

Checks, from what the strategy saw and the platform persisted:
  * every fill is stamped at or after the clock its order was sent at;
  * every MARKET fill is priced inside the range of the first bar that ends
    after the send -- never off a bar that closed before the order existed;
  * the clock never runs backwards across on_bar / on_timer / on_order;
  * every order filled and the strategy ends flat.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import datetime as dt

import pandas as pd

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.trading_types import OrderType

NAME = "t87_bar_fill_timer_timing"
SYMBOL = "AAPL"
DAY = "2025-01-08"                     # EST: 09:30 ET = 14:30 UTC
PLAN = [                               # (UTC time, action, target / side)
    ("14:40:00", "market", 1),
    ("14:50:00", "market", 0),
    ("14:55:30", "market", 1),
    ("15:05:30", "market", 0),
    ("15:10:00", "limit", 1),
    ("15:20:00", "market", 0),
]


def _ns(value):
    return pd.Timestamp(value).value if value is not None else None


class SdkT87:
    def on_start(self, ctx, event):
        self.s = {"sends": [], "fills": [], "bars": [], "backwards": [], "errors": []}
        self.done = set()
        self.last_clock = None
        self.last_close = None
        self.filled = {}
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")
        ctx.set_timer("t87", dt.timedelta(seconds=30))

    def _clock(self, ctx, where):
        now = _ns(ctx.now_utc())
        if now is not None:
            if self.last_clock is not None and now < self.last_clock:
                self.s["backwards"].append([where, now, self.last_clock])
            self.last_clock = now
        return now

    def on_bar(self, ctx, event):
        self._clock(ctx, "on_bar")
        bar = event.data()
        self.last_close = float(bar.close)
        self.s["bars"].append([int(bar.ts_event), float(bar.open), float(bar.high),
                               float(bar.low), float(bar.close)])

    def on_timer(self, ctx, event):
        now = self._clock(ctx, "on_timer")
        if now is None:
            return
        for hhmmss, action, arg in PLAN:
            key = (hhmmss, action)
            if key in self.done or now < _ns(f"{DAY} {hhmmss}+00:00"):
                continue
            self.done.add(key)
            try:
                if action == "market":
                    order = ctx.order_to_target(SYMBOL, target_quantity=arg)
                else:
                    limit = round(self.last_close + 0.50, 2)
                    order = ctx.buy_order(SYMBOL, arg, order_type=OrderType.LIMIT, limit_price=limit)
                oid = str(getattr(order, "order_id", "")) if order is not None else ""
                self.s["sends"].append({"at": hhmmss, "kind": action, "order_id": oid, "send_clock": now})
            except Exception as exc:  # noqa: BLE001
                self.s["errors"].append(f"{hhmmss}: {exc!r}")

    def on_order(self, ctx, event):
        self._clock(ctx, "on_order")
        order = event.data()
        oid = str(order.order_id)
        filled = float(order.filled_qty or 0)
        if filled > self.filled.get(oid, 0.0):
            self.filled[oid] = filled
            self.s["fills"].append({"order_id": oid, "fill_ts": int(order.ts_event or 0),
                                    "price": float(order.avg_px or 0)})

    def on_stop(self, ctx, event):
        self.s["final_position"] = float(ctx.net_position(SYMBOL))
        self.s["open_order"] = bool(ctx.has_open_order(SYMBOL))
        emit_checkpoint(ctx, NAME, self.s)


def _evaluate(s):
    sends = {x["order_id"]: x for x in s["sends"] if x["order_id"]}
    bars = sorted(s["bars"])                       # [end_ns, open, high, low, close]
    early, off_bar = [], []
    for f in s["fills"]:
        send = sends.get(f["order_id"])
        if send is None:
            continue
        if f["fill_ts"] < send["send_clock"]:
            early.append([send["at"], f["fill_ts"], send["send_clock"]])
        if send["kind"] == "market" and f["price"] > 0:
            nxt = next((b for b in bars if b[0] > send["send_clock"]), None)
            if nxt is None or not (nxt[3] - 1e-9 <= f["price"] <= nxt[2] + 1e-9):
                off_bar.append([send["at"], f["price"], nxt])
    filled_ids = {f["order_id"] for f in s["fills"]}
    unfilled = [x["at"] for x in s["sends"] if x["order_id"] not in filled_ids]
    return early, off_bar, unfilled


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT87", type="SdkT87", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date=DAY, end_date=DAY,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:30"))
    s = completed_checkpoint(run, NAME)
    early, off_bar, unfilled = _evaluate(s)
    finish(NAME, {
        "all_six_orders_sent": len(s["sends"]) == len(PLAN) and not s["errors"],
        "no_fill_before_its_send": not early,
        "market_fills_priced_on_the_next_bar": not off_bar,
        "clock_never_backwards": not s["backwards"],
        "every_order_filled": not unfilled,
        "ends_flat": s["final_position"] == 0 and not s["open_order"],
    }, extra=f"sends={[(x['at'], x['send_clock']) for x in s['sends']]}, early={early}, "
             f"off_bar={off_bar}, unfilled={unfilled}, backwards={s['backwards'][:3]}, errors={s['errors']}")
