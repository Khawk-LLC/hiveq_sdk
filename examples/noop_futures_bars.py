#!/usr/bin/env python3
"""No-op futures strategy — subscribe to 1-minute ES bars and print each one.

Places no orders. It exists to prove the plumbing end to end: the subscription
is accepted, bars arrive on the expected cadence, and the timestamps look right.
Use it as the skeleton for a real strategy, or as a smoke test for a new symbol
or date window.

Notes on the details that actually matter here:
  - futures bars come from ``subscribe_bars`` with the full continuous symbol
    string ``ROOT.roll.rank`` — ``ES.v.0`` is volume roll, front contract
    (§5.1). Subscribe in ``on_start``; identical repeat calls are deduped (R3).
  - NO ``bar.symbol`` filter. On a continuous subscription ``bar.symbol`` is the
    RESOLVED outright (``ESU6``), never ``ES.v.0`` — comparing against the
    continuous string drops every bar (§15). One instrument is subscribed, so
    every bar delivered is ours.
  - ``ctx.now()`` / ``bar.time`` are already ET wall clock — no tz math (R5).
  - one 1m bar == one minute of market time, so logging per bar IS the
    "every minute" print. ``logger.info`` is silent at the default WARNING
    level (§3.1), so the run also writes an ``add_event_log`` row per bar —
    those come back from ``run.event_logs()`` at any log level (§10.2). The
    run below passes ``hiveq_log_level='INFO'`` so the log lines show up too.
  - R12 ("0 trades == failed iteration") does not apply: this strategy is
    deliberately flat. The canary here is the bar COUNT, printed at the end.

Run:  python examples/noop_futures_bars.py [START END]
"""
import hiveq.flow as hf
from hiveq.flow import StrategyConfig, BacktestConfig
from hiveq.flow.config import AssetType   # not re-exported at hiveq.flow top level
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()               # module level — REQUIRED in every strategy (R10)

SYMBOL = "ES.v.0"                    # continuous front contract, volume roll
INTERVAL = "1m"                      # one bar per minute


class NoopFuturesBars:
    def __init__(self):
        # __init__ fires exactly ONCE per run; this instance gets every callback (§4).
        self.bars = 0                # bounded state only (R13)

    def on_start(self, ctx, event):
        # on_start fires once per CALENDAR day — subscribe here (R3).
        ctx.subscribe_bars(symbols=[SYMBOL], asset_type=AssetType.FUTURES, interval=INTERVAL)
        logger.info(f"[START] {ctx.trading_day} subscribed {INTERVAL} bars {SYMBOL}")
        ctx.add_event_log(f"subscribed {INTERVAL} {SYMBOL}", sub_event_type="INIT")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        self.bars += 1

        # bar.time is the ET close of the bar; ctx.now() is the engine's ET clock.
        logger.info(f"[BAR {self.bars}] {bar.time} {bar.symbol} "
                    f"o={bar.open:.2f} h={bar.high:.2f} l={bar.low:.2f} "
                    f"c={bar.close:.2f} v={bar.volume:.0f} ({bar.interval})")
        ctx.add_event_log(f"bar {bar.symbol} c={bar.close:.2f} v={bar.volume:.0f}",
                          symbol=bar.symbol,
                          state_variable={"open": bar.open, "high": bar.high,
                                          "low": bar.low, "close": bar.close,
                                          "volume": bar.volume, "n": self.bars})
        # no orders — this is a no-op strategy by design.


if __name__ == "__main__":           # required: unguarded run_backtest = NameError (R14)
    import sys

    start, end = (sys.argv[1], sys.argv[2]) if len(sys.argv) > 2 else ("2026-08-27", "2026-08-28")

    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="NoopFuturesBars", type="NoopFuturesBars")],
        symbols=[SYMBOL],
        start_date=start,
        end_date=end,
        data_configs=[{"type": "hiveq_historical",
                       "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        # Futures session defaults to 18:00 -> 17:00 ET next day (R6).
        backtest_config=BacktestConfig(initial_capital=100_000.0),
        config={"hiveq_log_level": "INFO"},   # surface the per-bar logger.info lines
    )
    print(f"run {run.run_id} task {run.task_id}  window {start} -> {end}")
    run.wait(progress=False)                                 # deploy is non-blocking (R11)

    # The per-bar rows, straight from the event-log table (§10.2). Columns as
    # actually returned: ts_event (UTC, even though in-strategy time is ET),
    # symbol, event_log_type, sub_event_type, message, state_variables.
    df = run.event_logs()
    bars = df[df["event_log_type"] == "USER_LOG"] if len(df) else df
    print(f"\nbars printed: {len(bars)}   (event-log rows: {len(df)})")
    if len(bars):
        bars = bars.sort_values("ts_event")
        print(bars[["ts_event", "symbol", "message"]].to_string(max_rows=20))
