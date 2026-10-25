#!/usr/bin/env python3
"""NoOpMixedStrategy — one strategy, an equity AND a future, on a MIXED container.

Deployed STRAIGHT to LiveSim (no backtest). It subscribes to ``AAPL`` and the
continuous front ES contract ``ES.c.0`` — trades, quotes and 1-minute bars for
both — counts what arrives per symbol and stream, and once a minute writes one
stats line per symbol. It places no orders. It exists to prove that a MIXED
container delivers market data for both asset classes into a single strategy.

Details that actually matter here:
  - **MIXED container.** ``livesim-mixed-1`` hosts futures, equity and options
    strategies in one engine, fed by the shared market-data server
    (``SigmaMarketData``) — the only feed that serves every asset class.
  - **Tag one member asset.** A MIXED container accepts a strategy tagged
    FUTURES, EQUITY or OPTIONS — never MIXED itself. The tag only drives
    placement (and signal topics); subscriptions below set the asset per symbol.
  - **Both tick schemas.** ``eq_trades`` for AAPL and ``fut_trades`` for ES go
    in the one ``live`` data config.
  - **Continuous symbol.** Live futures feeds publish under ``ES.c.0``; ticks
    and bars arrive under the RESOLVED contract (``ESZ6``), never ``ES.c.0``
    (§15), so stats are keyed by the symbol each payload carries.
  - **Timers are data-driven in LiveSim.** The 1-minute stats timer fires only
    while data flows — ES trades around the clock, so it keeps firing; a
    missing stats line means no data, which is exactly what we want to see.
  - **Clock.** Payload ``.time`` is logged untouched — no timezone math (R5).
  - **Bounded state only.** Counters and the last value per symbol (R13).
  - **R12 does not apply.** Deliberately flat; the canary is the per-minute
    counts in ``deployment.events()``, not trades.

Run:
    python examples/deploy_noop_mixed.py                 # deploy and report
    python examples/deploy_noop_mixed.py --observe 300   # watch it longer
    python examples/deploy_noop_mixed.py --cleanup       # tear it down after
"""

import argparse
import time
from datetime import timedelta

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType      # not re-exported at hiveq.flow top level
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()          # module level — REQUIRED in every strategy (R10)

EQUITY_SYMBOL = "AAPL"
FUTURES_SYMBOL = "ES.c.0"       # continuous front contract — what live feeds publish
CONTAINER_ID = "livesim-mixed-1"
INTERVAL = "1m"
STATS_TIMER = "noop-mixed-stats"
STREAMS = ("trades", "quotes", "bars")


class NoOpMixedStrategy:
    """Subscribe to AAPL and ES, report per-minute data stats, trade nothing."""

    def __init__(self):
        # __init__ fires exactly ONCE; this instance receives every callback (§4).
        # symbol -> {stream: count}. Keyed by the symbol each payload carries,
        # so ES lands under its resolved contract. A handful of keys (R13).
        self.minute = {}            # counts since the last stats line
        self.total = {}             # counts since start
        self.last = {}              # symbol -> last trade/quote/bar values
        self.reports = 0

    def _count(self, symbol, stream):
        for counts in (self.minute, self.total):
            per = counts.setdefault(symbol, dict.fromkeys(STREAMS, 0))
            per[stream] += 1

    def on_start(self, ctx, event):
        # Subscriptions belong in on_start (R3). on_start can fire more than
        # once; identical subscribe_* calls are deduplicated by the engine.
        ctx.subscribe_trades([EQUITY_SYMBOL], AssetType.EQUITY)
        ctx.subscribe_quotes([EQUITY_SYMBOL], AssetType.EQUITY)
        ctx.subscribe_bars([EQUITY_SYMBOL], AssetType.EQUITY, INTERVAL)
        ctx.subscribe_trades([FUTURES_SYMBOL], AssetType.FUTURES)
        ctx.subscribe_quotes([FUTURES_SYMBOL], AssetType.FUTURES)
        ctx.subscribe_bars([FUTURES_SYMBOL], AssetType.FUTURES, INTERVAL)
        ctx.set_timer(STATS_TIMER, timedelta(minutes=1))
        logger.info(
            f"[START] subscribed trades + quotes + {INTERVAL} bars "
            f"{EQUITY_SYMBOL} (EQUITY), {FUTURES_SYMBOL} (FUTURES); "
            f"stats every 1m"
        )
        ctx.add_event_log(
            f"NoOpMixedStrategy started: {EQUITY_SYMBOL} + {FUTURES_SYMBOL}",
            sub_event_type="INIT",
            state_variable={"equity": EQUITY_SYMBOL, "futures": FUTURES_SYMBOL,
                            "streams": list(STREAMS), "interval": INTERVAL},
        )

    def on_trade(self, ctx, event):
        tick = event.data()                              # -> SigmaTradeTick (§7.5)
        self._count(tick.symbol, "trades")
        last = self.last.setdefault(tick.symbol, {})
        last["trade_px"] = tick.price
        last["trade_time"] = str(tick.time)
        logger.debug(f"[TRADE] {tick.time} {tick.symbol} px={tick.price} sz={tick.size}")

    def on_quote(self, ctx, event):
        quote = event.data()                             # -> SigmaQuoteTick (§7.6)
        self._count(quote.symbol, "quotes")
        last = self.last.setdefault(quote.symbol, {})
        last["bid"] = quote.bid_price
        last["ask"] = quote.ask_price
        last["quote_time"] = str(quote.time)
        logger.debug(
            f"[QUOTE] {quote.time} {quote.symbol} "
            f"{quote.bid_price}x{quote.ask_price}"
        )

    def on_bar(self, ctx, event):
        bar = event.data()                               # -> SigmaBar (§7.1)
        self._count(bar.symbol, "bars")
        last = self.last.setdefault(bar.symbol, {})
        last["bar_close"] = bar.close
        last["bar_volume"] = bar.volume
        last["bar_time"] = str(bar.time)
        logger.debug(
            f"[BAR] {bar.time} {bar.symbol} c={bar.close:.2f} v={bar.volume:.0f}"
        )

    def on_timer(self, ctx, event):
        if event.data().timer_id != STATS_TIMER:
            return
        self.reports += 1
        now = ctx.now()
        if not self.total:
            logger.info(f"[STATS {self.reports}] {now} no market data received yet")
            ctx.add_event_log(f"stats #{self.reports}: no market data yet",
                              sub_event_type="STATS")
        for symbol in sorted(self.total):
            minute = self.minute.get(symbol, dict.fromkeys(STREAMS, 0))
            total = self.total[symbol]
            last = self.last.get(symbol, {})
            line = (
                f"[STATS {self.reports}] {now} {symbol} "
                f"last 1m: trades={minute['trades']} quotes={minute['quotes']} "
                f"bars={minute['bars']} | total: trades={total['trades']} "
                f"quotes={total['quotes']} bars={total['bars']} | "
                f"px={last.get('trade_px')} bid={last.get('bid')} "
                f"ask={last.get('ask')} last_trade={last.get('trade_time')}"
            )
            logger.info(line)
            ctx.add_event_log(
                line,
                sub_event_type="STATS",
                symbol=symbol,
                state_variable={"minute": minute, "total": total, "last": last},
            )
        self.minute = {}
        # The engine re-fires the timer every interval — no re-arm needed.
        # No orders. Ever. That is the whole point of this strategy.


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instance-name",
        # Unique per run: an instance name is unique within its container, so a
        # fixed one collides with whatever the previous run left behind
        # (LIVESIM_INSTANCE_NAME_IN_USE).
        default=f"NoOpMixed{int(time.time()) % 100000}",
    )
    parser.add_argument(
        "--observe", type=int, default=180,
        help="Seconds to watch for stats lines after it settles (default 180).",
    )
    parser.add_argument(
        "--cleanup", action="store_true",
        help="Terminate the deployment at the end instead of leaving it up.",
    )
    args = parser.parse_args()

    from hiveq.flow.livesim import _base_url

    # Print it every run: a deploy quietly going to the wrong platform is the
    # easiest mistake to make here.
    print(f"platform      : {_base_url()}")

    strategy = StrategyConfig(
        name="NoOpMixedStrategy",
        type="NoOpMixedStrategy",           # the CLASS NAME, exactly (R2)
        symbols=[EQUITY_SYMBOL, FUTURES_SYMBOL],
        # One member asset of the MIXED container (MIXED itself is not a
        # strategy asset). Subscriptions set the asset per symbol.
        params={"assetType": "FUTURES"},
    )

    data_configs = [
        {
            "type": "live",
            "market_data_source": "SigmaMarketData",
            "schema": ["eq_trades", "fut_trades"],
        }
    ]

    deployment = hf.deploy_livesim(
        [strategy],
        data_configs=data_configs,
        instance_name=args.instance_name,
        # Explicit target (needs livesim:select_target): auto-placement prefers
        # a dedicated FUTURES container over the MIXED one.
        container_id=CONTAINER_ID,
    )
    print(f"\ndeployment    : {deployment.deployment_id}")
    print(f"  artifact    : {deployment.artifact_id}")

    state = deployment.wait(timeout=120)
    instance = (state.get("instance_names") or [args.instance_name])[0]
    print(f"  status      : {state['status']}")
    print(f"  container   : {state.get('container_id')}")
    print(f"  strategy    : {instance}")
    if state.get("failure_reason"):
        print(f"  FAILED      : {state['failure_reason']}")
        return 1

    # Every read is a pull, not a stream: each call is a complete snapshot.
    print(f"\nwatching {args.observe}s for STATS lines:")
    deadline = time.time() + args.observe
    seen = 0
    while time.time() < deadline:
        time.sleep(20)
        rows = [r for r in deployment.events(limit=1000)
                if r.get("sub_event_type") == "STATS"]
        for row in rows[seen:]:
            print(f"  {row.get('message')}")
        seen = len(rows)
    print(f"  stats rows  : {seen}")
    print(f"  orders      : {len(deployment.orders(limit=1000))} (expected 0 — no-op)")

    if args.cleanup:
        # Per-instance, not bare: a bare terminate() removes the bookkeeping
        # but never tells the engine, so the instance stays registered.
        print("\nterminate:")
        deployment.terminate(strategy=instance)
        print(f"  terminated {instance}")
    else:
        print("\nstill deployed. tear it down with:")
        print(f"  hf.get_deployment('{deployment.deployment_id}')"
              f".terminate(strategy='{instance}')")
    return 0


if __name__ == "__main__":       # REQUIRED: the module is re-imported to rebuild
    raise SystemExit(main())     # the strategy, so a deploy at import re-runs itself
