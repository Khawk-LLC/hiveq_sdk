#!/usr/bin/env python3
"""ES momentum (futures) — fast/slow SMA crossover on 1-minute bars, long and short.

A deliberately simple momentum strategy on the S&P 500 e-mini continuous front
contract. Written against the installed SDK spec (``hiveq docs`` -> llms.txt):

  - futures bars come from the unambiguous ``subscribe_bars`` API with the full
    continuous symbol string ``ROOT.roll.rank`` (§5.1). ``ES.v.0`` = volume roll,
    front contract, so it tracks liquidity.
  - NO ``bar.symbol`` filtering. On a continuous subscription ``bar.symbol`` is
    the RESOLVED outright (``ESU6``), never ``ES.v.0`` — comparing against the
    continuous string drops every bar and yields 0 trades (§15). Only one
    instrument is subscribed here, so every bar we get is ours.
  - no engine-side history buffer: keep your own rolling ``deque`` (§16.2) and
    compute the indicator by hand with ``numpy`` (no TA library — §16.3).
  - ``ctx.now()`` is already ET wall clock — compare directly, never do tz math
    (R5/R6).
  - ``order_to_target`` trades the signed delta and is idempotent (it skips while
    an order is working), so calling it every bar converges one fill at a time
    and reverses long -> short in a single order (§5.2).
  - HiveQ logger at module level, DEBUG per bar / INFO at decisions (R10). The
    engine default level is WARNING, so a healthy run stays quiet; re-run with
    ``config={'hiveq_log_level': 'DEBUG'}`` to surface all of it (§2.1, §11.5).

Shape: trade momentum in the RTH window only, flat outside it. Long 1 contract
while fast SMA > slow SMA, short 1 contract while fast < slow, flat overnight —
so the position never rides a contract roll.

Run:  python examples/es_momentum.py
"""
from collections import deque

import numpy as np

import hiveq.flow as hf
from hiveq.flow import StrategyConfig, BacktestConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()               # module level — REQUIRED in every strategy (R10)

SYMBOL = "ES.v.0"                    # continuous front contract, volume roll
FAST, SLOW = 10, 30                  # SMA lengths, in 1-minute bars
QTY = 1                              # contracts per side (ES multiplier is 50)
ENTRY_OPEN = (9, 35)                 # ET — skip the first few minutes after the open
FLAT_BY = (15, 55)                   # ET — flat before the 16:00 cash close


class ESMomentum:
    def __init__(self):
        # __init__ fires exactly ONCE per run; this instance gets every callback (§4).
        self.closes = deque(maxlen=SLOW)     # bounded state only — R13 (2 GB cap)
        self.target = 0                      # signed contracts we want to hold

    def on_start(self, ctx, event):
        # on_start fires once per CALENDAR day; subscribe here (R3). Repeating an
        # identical request is safe — the engine dedupes it.
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.FUTURES, interval="1m")
        logger.info(f"[START] {ctx.trading_day} subscribed 1m bars {SYMBOL} "
                    f"fast={FAST} slow={SLOW} qty={QTY}")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        self.closes.append(bar.close)                        # no symbol filter — see docstring

        now = ctx.now()                                      # ET datetime — no tz math (R5)
        hm = (now.hour, now.minute)
        pos = ctx.net_position(bar.symbol)
        logger.debug(f"[BAR] {bar.time} {bar.symbol} close={bar.close:.2f} "
                     f"win={len(self.closes)}/{SLOW} pos={pos} target={self.target}")

        # Outside the entry window: be flat. close_position is a no-op when flat,
        # so this is safe to call on every off-hours bar.
        if not (ENTRY_OPEN <= hm < FLAT_BY):
            if pos != 0:
                logger.info(f"[FLAT] {bar.time} outside entry window — closing {pos} "
                            f"{bar.symbol} at {bar.close:.2f}")
                ctx.close_position(bar.symbol)
                ctx.add_event_log(f"session flat, closed {pos:+.0f}",
                                  symbol=bar.symbol,
                                  state_variable={"close": bar.close})
            self.target = 0
            return

        if len(self.closes) < SLOW:
            logger.debug(f"[WARMUP] {bar.time} {len(self.closes)}/{SLOW} bars")
            return

        closes = np.asarray(self.closes)
        fast = closes[-FAST:].mean()
        slow = closes.mean()
        want = QTY if fast > slow else -QTY                  # momentum, both directions
        logger.debug(f"[SIGNAL] {bar.time} fast={fast:.2f} slow={slow:.2f} "
                     f"spread={fast - slow:+.2f} want={want:+d} pos={pos}")

        if want == pos:
            return                                           # already positioned

        # order_to_target trades the signed delta (reversal = one order) and skips
        # while an order is working, so this converges instead of double-trading.
        logger.info(f"[ORDER] {bar.time} target {want:+d} from {pos:+.0f} on "
                    f"{bar.symbol} at {bar.close:.2f} "
                    f"(fast={fast:.2f} {'>' if fast > slow else '<'} slow={slow:.2f})")
        ctx.order_to_target(bar.symbol, target_quantity=want)
        ctx.add_event_log(f"target {want:+d} (fast={fast:.2f} slow={slow:.2f})",
                          symbol=bar.symbol,
                          state_variable={"fast": float(fast), "slow": float(slow),
                                          "close": bar.close, "position": float(pos)})
        self.target = want

    def on_order(self, ctx, event):
        order = event.data()                                 # -> SigmaOrder (§7.3)
        # Fills arrive HERE — there is no on_order_filled (§4).
        if order.is_filled:
            fill = order.last_fill                           # -> SigmaFill (§7.4)
            logger.info(f"[FILL] {order.symbol} {order.side} {order.filled_qty} "
                        f"@ {getattr(fill, 'price', order.avg_px)} status={order.status}")
        else:
            logger.info(f"[ORDER] {order.symbol} status={order.status} "
                        f"leaves={order.leaves_qty} reject={order.reject_reason}")


if __name__ == "__main__":
    import sys

    # Optional window override:  python examples/es_momentum.py START END
    start, end = (sys.argv[1], sys.argv[2]) if len(sys.argv) > 2 else ("2026-08-03", "2026-08-28")

    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="ESMomentum", type="ESMomentum")],
        symbols=[SYMBOL],
        start_date=start,
        end_date=end,
        data_configs=[{"type": "hiveq_historical",
                       "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        # Futures session defaults to 18:00-17:00 ET; the strategy trades RTH only.
        backtest_config=BacktestConfig(initial_capital=1_000_000.0),
    )
    print(f"run {run.run_id} task {run.task_id}  window {start} -> {end}")
    run.wait(progress=False)                                 # block quietly (R11)

    report = run.report()                                    # -> PerformanceReport (§10.1)
    print(report.return_stats.to_string())

    # R12 canary: a run that completes with 0 trades is a failed iteration.
    print(f"\nTotal Trades: {report.stats.get('Total Trades')}   "
          f"Net PnL: {report.stats.get('Net PnL')}   Sharpe: {report.stats.get('Sharpe')}")
