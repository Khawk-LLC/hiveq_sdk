#!/usr/bin/env python3
"""Wall-clock scheduling with timers — ctx.set_timer / on_timer + ctx.now() (§16.5).

Demonstrates (every ctx call verified against the SDK type surface):
  - ``ctx.set_timer(timer_id, timer_interval)`` with a ``datetime.timedelta``
    relative interval; the engine re-fires ``on_timer`` each interval (§5.8).
  - ``on_timer`` receives a ``TimerEventData`` (``event.data().timer_id``, §7.8).
  - wall-clock scheduling: gate actions on ``ctx.now()`` — already ET, no UTC
    math (R5/R6). Here we flatten near the 15:55 ET close like a MOC guard.
  - ``ctx.cancel_timer(timer_id)`` once the day's job is done.

Shape: long-only SMA-free demo — buy a starter position on the first bar, then
let a 1-minute timer (not the bar stream) drive the end-of-day flatten.

Run:  python timers_scheduling.py
"""
from datetime import timedelta

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

POLL = timedelta(minutes=1)      # timer cadence (§5.8 — timedelta interval)
FLAT_BY = (15, 55)               # ET: flatten before the 16:00 close


class TimerScheduling:
    def __init__(self):
        self.entered = set()                                 # symbols we've opened
        self.done = False                                    # day's flatten fired

    def on_start(self, ctx, event):                          # subscribe + arm timer in START (R3)
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                            asset_type=AssetType.EQUITY, interval="1m")
        ctx.set_timer("eod_flat", POLL)                      # relative timedelta (§5.8)
        ctx.add_event_log("armed eod_flat timer", sub_event_type="INIT")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        if bar.symbol not in self.entered and ctx.is_flat(bar.symbol):
            ctx.buy_order(bar.symbol, quantity=100)
            self.entered.add(bar.symbol)
            ctx.add_event_log(f"starter long {bar.symbol} @ {bar.close:.2f}",
                              symbol=bar.symbol)

    def on_timer(self, ctx, event):
        timer = event.data()                                 # -> TimerEventData (§7.8)
        now = ctx.now()                                      # ET datetime (R5/R6) — no tz math
        if self.done or (now.hour, now.minute) < FLAT_BY:
            return
        # wall-clock cutoff reached: flatten everything and stop the timer
        ctx.add_event_log(f"timer {timer.timer_id} -> EOD flatten at {now:%H:%M} ET",
                          sub_event_type="TIMER")
        ctx.flatten_all()
        ctx.cancel_timer("eod_flat")
        self.done = True


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="TimerScheduling", type="TimerScheduling")],
        symbols=["AAPL", "MSFT"],
        start_date="2025-08-01",
        end_date="2025-08-02",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    print(run.logs())
    print(run.event_logs())
