#!/usr/bin/env python3
"""Futures session window — CME Globex RTH/overnight, R6 18:00->17:00 ET.

Demonstrates (every ctx call verified against the SDK type surface):
  - the futures session window: CME Globex opens 18:00 ET (T-1) and closes
    17:00 ET (T). With ``dataset='HIVEQ_US_FUT'`` those session defaults are
    applied automatically; you can also pin them with ``BacktestConfig
    session_start='18:00' / session_end='17:00'`` (§9.3, doc §16.5/R6).
  - ``ctx.subscribe_futures_bars(symbols=['ES.v.0'], interval=...)`` — subscribe by
    the futures SYMBOL STRING. ``ES.v.0`` = volume-roll front continuous; ``ES.c.0``
    = calendar/front; ``ES.H25`` = a dated contract (§5.1).
  - TIME IS EST/EDT: ``ctx.now()`` is already ET; never convert UTC (R5/R6).

Shape: a single momentum-vs-open lean on ES, flat by the 16:30 ET halt.

Run:  python futures_session.py   (needs HIVEQ_API_KEY)
"""
import hiveq.flow as hf
from hiveq.flow import StrategyConfig, BacktestConfig

SYMBOL = "ES.v.0"            # ROOT.roll.rank: ES, volume-roll (v), front (0). 'ES.c.0' = calendar/front. (§5.1)
HALT_BY = (16, 15)           # ET: flatten before the 16:15-16:30 trading halt


class FuturesSession:
    def __init__(self):
        self.session_open = {}                               # symbol -> first close of session

    def on_start(self, ctx, event):                          # subscribe in START (R3)
        # Subscribe by the futures symbol string.
        ctx.subscribe_futures_bars(symbols=[SYMBOL], interval="1m")
        ctx.add_event_log(f"futures session start {SYMBOL}", sub_event_type="INIT")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        now = ctx.now()                                      # ET datetime (R5/R6) — no tz math

        # new session opens at 18:00 ET — reset the reference price
        if now.hour == 18 and now.minute == 0:
            self.session_open[bar.symbol] = bar.close

        if (now.hour, now.minute) >= HALT_BY:                # flat before the halt
            if not ctx.is_flat(bar.symbol):
                ctx.close_position(bar.symbol)
            return

        ref = self.session_open.get(bar.symbol)
        if ref is None:
            self.session_open[bar.symbol] = bar.close        # first bar seen this session
            return

        # lean long when price holds above the session open; exit on the reverse
        if bar.close > ref and ctx.is_flat(bar.symbol):
            ctx.buy_order(bar.symbol, quantity=1)
            ctx.add_event_log(f"long {bar.symbol} close={bar.close:.2f} open={ref:.2f}",
                              symbol=bar.symbol)
        elif bar.close < ref and ctx.is_net_long(bar.symbol):
            ctx.close_position(bar.symbol)
            ctx.add_event_log(f"exit {bar.symbol}", symbol=bar.symbol)

    def on_order(self, ctx, event):
        o = event.data()                                     # -> SigmaOrder (§7.3)
        if o.is_filled:
            ctx.add_event_log(f"fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}",
                              symbol=o.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="FuturesSession", type="FuturesSession")],
        start_date="2025-11-03",
        end_date="2025-11-05",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="18:00", session_end="17:00"),
    )
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
