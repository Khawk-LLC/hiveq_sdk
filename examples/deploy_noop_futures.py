#!/usr/bin/env python3
"""NoOpFutures — deployed STRAIGHT to LiveSim. No backtest anywhere in the path.

The live twin of ``noop_futures_bars.py``: subscribe to the continuous front ES
contract, log every trade tick and every 1-minute bar, place no orders. It
exists to prove the live plumbing end to end — the artifact is accepted, the
strategy comes up on a container, the subscription registers, and data arrives
on the expected cadence.

``hf.deploy_livesim(...)`` captures this module as source, uploads it as an
immutable artifact and deploys it; the handle is ``deployment_id`` (§3.3).
There is no ``run_id`` in the LiveSim API — a LiveSim run id names a container
engine lifetime, not your deployment.

Details that actually matter here:
  - **Continuous symbol.** Live futures feeds publish under ``ES.c.0``; a dated
    contract like ``ESU6`` subscribes to nothing.
  - **Tag the asset.** With no backtest to infer from, a strategy that does not
    set ``assetType`` in its params falls back to the FUTURES default and can
    land on the wrong container. We want FUTURES, but say so explicitly.
  - **Clock.** In LiveSim the engine clock and market ``ts_event`` share one
    base; log payload timestamps UNTOUCHED and never convert them (R5).
  - **Bounded state only.** Counters, not lists of ticks (R13).
  - **R12 does not apply.** This strategy is deliberately flat; the canary is
    the tick/bar COUNT in ``deployment.events()``, not trades.
  - **Placement ignores the schedule.** A container outside its window accepts
    the deployment and it sits idle until the window opens — check
    ``hf.containers(asset="FUTURES")`` before concluding anything is wrong.

Run:
    python examples/deploy_noop_futures.py                 # deploy and report
    python examples/deploy_noop_futures.py --observe 300   # watch it longer
    python examples/deploy_noop_futures.py --cleanup       # tear it down after
"""

import argparse
import os
import time

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType      # not re-exported at hiveq.flow top level
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()          # module level — REQUIRED in every strategy (R10)

SYMBOL = "ES.c.0"               # continuous front contract — what live feeds publish
INTERVAL = "1m"                 # one bar per minute


class NoOpFutures:
    """Subscribe to live ES, count what arrives, trade nothing."""

    # Log a tick line only every Nth trade: ES prints thousands a minute and the
    # log is a snapshot pull, not a stream.
    TICK_LOG_EVERY = 500

    def __init__(self):
        # __init__ fires exactly ONCE; this instance receives every callback (§4).
        self.trades = 0         # bounded state only (R13)
        self.bars = 0

    def on_start(self, ctx, event):
        # Subscriptions belong in on_start (R3). Trades are the container's
        # native futures feed; bars are aggregated from them by the engine.
        ctx.subscribe_trades([SYMBOL], AssetType.FUTURES)
        ctx.subscribe_bars([SYMBOL], AssetType.FUTURES, INTERVAL)
        logger.info(f"[START] subscribed trades + {INTERVAL} bars {SYMBOL}")
        # The engine logs the start and the subscription regardless of data
        # flow — which is the liveness check on a closed market, since a timer
        # is data-driven in LiveSim and would never fire.
        ctx.add_event_log(
            f"NoOpFutures started: trades + {INTERVAL} bars {SYMBOL}",
            sub_event_type="INIT",
        )

    def on_trade(self, ctx, event):
        tick = event.data()                              # -> SigmaTradeTick (§7.5)
        self.trades += 1
        if self.trades % self.TICK_LOG_EVERY:
            return
        # tick.time as the engine hands it over — no timezone math (R5).
        logger.info(
            f"[TRADE {self.trades}] {tick.time} {tick.symbol} "
            f"px={tick.price} sz={tick.size}"
        )
        ctx.add_event_log(
            f"trade #{self.trades} {tick.symbol} px={tick.price}",
            symbol=tick.symbol,
            state_variable={"price": tick.price, "size": tick.size,
                            "trades": self.trades},
        )

    def on_bar(self, ctx, event):
        bar = event.data()                               # -> SigmaBar (§7.1)
        self.bars += 1
        # NO bar.symbol filter: on a continuous subscription bar.symbol is the
        # RESOLVED outright (ESZ6), never ES.c.0 (§15). One instrument is
        # subscribed, so every bar delivered is ours.
        logger.info(
            f"[BAR {self.bars}] {bar.time} {bar.symbol} o={bar.open:.2f} "
            f"h={bar.high:.2f} l={bar.low:.2f} c={bar.close:.2f} "
            f"v={bar.volume:.0f} ({bar.interval})"
        )
        ctx.add_event_log(
            f"bar {bar.symbol} c={bar.close:.2f} v={bar.volume:.0f}",
            symbol=bar.symbol,
            state_variable={"open": bar.open, "high": bar.high, "low": bar.low,
                            "close": bar.close, "volume": bar.volume,
                            "bars": self.bars, "trades": self.trades},
        )
        # No orders. Ever. That is the whole point of this strategy.


def _fleet_note() -> str:
    """One line on whether a FUTURES container is actually up right now."""
    try:
        rows = hf.containers(asset="FUTURES", runtime="hiveq_flow")
    except Exception as exc:                             # never block the deploy
        return f"  (could not read the fleet: {exc})"
    if not rows:
        return "  (no FUTURES hiveq_flow container in the fleet)"
    return "\n".join(
        f"  {c['id']:22} {c['state']['status']:9} {c['state'].get('reason') or ''} "
        f"[{c['available_capacity']}/{c['capacity']} free, "
        f"{c['schedule'].get('start_time')}-{c['schedule'].get('end_time')} "
        f"{c['schedule'].get('frequency') or c['schedule'].get('days_of_week')}]"
        for c in rows
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instance-name",
        # Unique per run: an instance name is unique within its container, so a
        # fixed one collides with whatever the previous run left behind
        # (LIVESIM_INSTANCE_NAME_IN_USE).
        default=f"NoOpFutures{int(time.time()) % 100000}",
    )
    parser.add_argument(
        "--observe", type=int, default=90,
        help="Seconds to watch for events after it settles (default 90).",
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
    print("fleet (FUTURES):")
    print(_fleet_note())

    strategy = StrategyConfig(
        name="NoOpFutures",
        type="NoOpFutures",                 # the CLASS NAME, exactly (R2)
        symbols=[SYMBOL],
        params={"assetType": "FUTURES"},    # tag the asset — placement depends on it
    )

    data_configs = [
        {
            "type": "live",
            "market_data_source": "Activ",
            "schema": ["fut_trades"],
        }
    ]

    deployment = hf.deploy_livesim(
        [strategy],
        data_configs=data_configs,
        instance_name=args.instance_name,
    )
    print(f"\ndeployment    : {deployment.deployment_id}")
    print(f"  artifact    : {deployment.artifact_id}")
    print(f"  operation   : {deployment.operation_id}")

    state = deployment.wait(timeout=120)
    instance = (state.get("instance_names") or [args.instance_name])[0]
    print(f"  status      : {state['status']}")
    print(f"  container   : {state.get('container_id')}")
    print(f"  strategy    : {instance}")
    if state.get("failure_reason"):
        print(f"  FAILED      : {state['failure_reason']}")
        return 1

    # --- what it has seen ---------------------------------------------------
    # Every read is a pull, not a stream: each call is a complete snapshot.
    # Zero rows outside the container's window is normal, not a failure.
    print(f"\nwatching {args.observe}s for events:")
    deadline = time.time() + args.observe
    events = deployment.events(limit=1000)
    while time.time() < deadline and len(events) < 3:
        time.sleep(10)
        events = deployment.events(limit=1000)
        print(f"  ... {len(events)} event rows, {int(deadline - time.time())}s left")
    print(f"  event rows  : {len(events)}")
    for row in events[-5:]:
        print(f"    {row.get('ts_event')} {row.get('message')}")
    print(f"  orders      : {len(deployment.orders(limit=1000))} (expected 0 — no-op)")

    logs = deployment.logs(tail=40)
    print(f"\nlogs ({len(logs.splitlines())} lines), last 10:")
    for line in logs.splitlines()[-10:]:
        print(f"  {line}")

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
