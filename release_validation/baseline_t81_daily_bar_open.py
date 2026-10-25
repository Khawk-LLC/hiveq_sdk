"""t81: daily bars delivered at the session open (remote translation of Flow QA t14).

t13 covers the default (a daily bar delivered at its session close). With the
'open' opt-in the same equity daily bar is delivered at its session open --
deliberately exposing that session's OHLCV before it trades:

* start_time == session open (09:30 New York), end_time == session close
  (16:00 New York);
* ts_event == bar.time == ctx.now() == start_time;
* delivered before every regular-session trade of that day;
* an order placed in that callback is sent at the delivery time (the paper
  broker's order clock agrees with ctx.now()) and fills that same day, never
  before it was sent (with trades streaming it fills against them);
* one bar per trading day, on the same day.
"""

from pathlib import Path
import sys

# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from collections import defaultdict                                   # noqa: E402

from qa_common import completed_checkpoint, emit_checkpoint, finish   # noqa: E402

import pandas as pd                                                   # noqa: E402

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import StrategyConfig                                 # noqa: E402
from hiveq.flow.config import AssetType, EventType                               # noqa: E402

CHECKPOINT = "t81_daily_bar_open"
SYMBOL = "AAPL"
START, END = "2025-06-02", "2025-06-03"
NY = "America/New_York"
SAMPLES = 5
STATE = {"bars": [], "violations": defaultdict(list), "last_rth_trade": {}, "delivered_days": set(),
         "orders": {}, "filled": []}


def nanos(value):
    return int(pd.Timestamp(value).value)


def session(day):
    return (nanos(pd.Timestamp(f"{day} 09:30", tz=NY)), nanos(pd.Timestamp(f"{day} 16:00", tz=NY)))


def violation(key, sample):
    if len(STATE["violations"][key]) < SAMPLES:
        STATE["violations"][key].append(sample)
    STATE["violations"][key + "_n"].append(1)


class SdkT81:
    def on_start(self, ctx, event):
        ctx.subscribe_trades([SYMBOL], asset_type=AssetType.EQUITY)
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1d")

    def on_order(self, ctx, event):
        order = event.data()
        expected = STATE["orders"].get(order.order_id)
        if expected is None:
            return
        delivered, day, close = expected
        if event.type == EventType.ORDER_SUBMITTED and int(order.ts_init or 0) != delivered:
            violation("order_not_sent_at_delivery", [day, int(order.ts_init or 0), delivered])
        if event.type == EventType.ORDER_FILLED:
            STATE["filled"].append(day)
            filled_at = nanos(order.time) if order.time else 0
            filled_day = pd.Timestamp(filled_at, tz="UTC").tz_convert(NY).strftime("%Y-%m-%d")
            if filled_day != day or filled_at < delivered:
                violation("order_not_filled_same_day", [day, str(order.time), order.avg_px])

    def on_trade(self, ctx, event):
        ts = int(event.ts_event)
        day = pd.Timestamp(ts, tz="UTC").tz_convert(NY).strftime("%Y-%m-%d")
        open_ns, close_ns = session(day)
        if open_ns <= ts < close_ns:
            if day not in STATE["delivered_days"]:
                violation("session_trade_before_daily_bar", [day, ts])
            STATE["last_rth_trade"][day] = ts

    def on_bar(self, ctx, event):
        bar = event.data()
        if bar.interval != "1d":
            violation("unexpected_bar", [bar.symbol, bar.interval])
            return
        start, end = nanos(bar.start_time), nanos(bar.end_time)
        now = nanos(ctx.now())
        day = pd.Timestamp(start, tz="UTC").tz_convert(NY).strftime("%Y-%m-%d")
        open_ns, close_ns = session(day)
        if start != open_ns or end != close_ns:
            violation("daily_not_on_session", [day, str(bar.start_time), str(bar.end_time)])
        if int(event.ts_event) != start or nanos(bar.time) != start:
            violation("ts_event_not_open", [day, int(event.ts_event), start])
        if now != start:
            violation("now_not_open", [day, now, start])
        STATE["delivered_days"].add(day)
        STATE["bars"].append(day)
        order = ctx.buy_order(SYMBOL, 1)
        if order is not None and order.order_id:
            STATE["orders"][order.order_id] = (start, day, float(bar.close))

    def on_stop(self, ctx, event):
        # on_stop fires once per calendar day; the last checkpoint covers the run.
        emit_checkpoint(ctx, CHECKPOINT, {
            "bars": list(STATE["bars"]),
            "filled": list(STATE["filled"]),
            "violations": {k: (len(x) if k.endswith("_n") else x) for k, x in STATE["violations"].items()},
        })


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT81", type="SdkT81", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date=START, end_date=END,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": [s]}
                      for s in ("bars_1d", "eq_trades")],
        config={"daily_bar_delivery": "open"})
    state = completed_checkpoint(run, CHECKPOINT)
    v = state["violations"]
    checks = {
        "one_daily_bar_per_day": state["bars"] == sorted(set(state["bars"])) and len(state["bars"]) == 2,
        "daily_on_session": not v.get("daily_not_on_session_n"),
        "ts_event_is_open": not v.get("ts_event_not_open_n"),
        "now_is_open": not v.get("now_not_open_n"),
        "before_session_trades": not v.get("session_trade_before_daily_bar_n"),
        "no_unexpected_bars": not v.get("unexpected_bar_n"),
        "orders_filled": sorted(state["filled"]) == sorted(state["bars"]),
        "order_sent_at_delivery": not v.get("order_not_sent_at_delivery_n"),
        "order_filled_same_day": not v.get("order_not_filled_same_day_n"),
    }
    finish(CHECKPOINT, checks,
           extra=f"bars={STATE['bars']} violations={ {k: x for k, x in v.items() if not k.endswith('_n')} }")
