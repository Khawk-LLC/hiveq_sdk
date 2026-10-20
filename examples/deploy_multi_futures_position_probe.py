#!/usr/bin/env python3
"""LiveSim probe for independent positions across ten continuous futures.

Each continuous instrument has its own deterministic pseudo-random schedule and
position-size limit.  At every scheduled action it chooses a new signed target
(long, flat, or short), then uses ``order_to_target`` so retries cannot stack
duplicate orders.  Structured position snapshots compare the requested target
with both the continuous-series position and its currently resolved contract.

Running this file deploys directly to ``livesim-futures-synthetic``.  It does
not submit a backtest.
"""

from __future__ import annotations

from datetime import timedelta
import random
import re
import time

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()

SYMBOLS = [
    "ES.c.0",
    "NQ.c.0",
    "YM.c.0",
    "RTY.c.0",
    "CL.c.0",
    "GC.c.0",
    "SI.c.0",
    "HG.c.0",
    "ZB.c.0",
    "6E.c.0",
]
CONTAINER_ID = "livesim-futures-synthetic"
INTERVAL = "1m"
AUDIT_TIMER = "multi-futures-position-audit"
BASE_SEED = 20261018
EPSILON = 1e-9

# Outright contract examples: ESZ6, CLF27, 6EZ6.  The non-greedy root capture
# keeps the final futures month code and year digits out of the root.
OUTRIGHT = re.compile(r"^([0-9A-Z]+?)[FGHJKMNQUVXZ]\d{1,2}$")


def _root(symbol):
    match = OUTRIGHT.match(str(symbol))
    return match.group(1) if match else str(symbol).split(".", 1)[0]


class MultiFuturesPositionProbe:
    """Trade ten independent random targets and continuously audit positions."""

    def __init__(self):
        self.by_root = {symbol.split(".", 1)[0]: symbol for symbol in SYMBOLS}
        self.rng = {
            symbol: random.Random(BASE_SEED + index * 1009)
            for index, symbol in enumerate(SYMBOLS)
        }

        # A shuffled 1..10 assignment guarantees distinct sizing limits while
        # keeping which instrument gets which size reproducibly random.
        sizes = list(range(1, len(SYMBOLS) + 1))
        random.Random(BASE_SEED).shuffle(sizes)
        self.size = dict(zip(SYMBOLS, sizes))

        self.next_action = {symbol: None for symbol in SYMBOLS}
        self.pending_target = {symbol: None for symbol in SYMBOLS}
        self.expected_target = {symbol: 0.0 for symbol in SYMBOLS}
        self.bars_seen = {symbol: 0 for symbol in SYMBOLS}
        self.actions = {symbol: 0 for symbol in SYMBOLS}

    def on_start(self, ctx, event):
        ctx.subscribe_bars(SYMBOLS, AssetType.FUTURES, INTERVAL)
        ctx.set_timer(AUDIT_TIMER, timedelta(minutes=15))
        ctx.add_event_log(
            "registered ten continuous futures",
            sub_event_type="INIT",
            state_variable={
                "symbols": SYMBOLS,
                "maximum_position_sizes": self.size,
                "seed": BASE_SEED,
            },
        )
        logger.debug("subscribed to %s", SYMBOLS)

    def _continuous_for_contract(self, contract):
        return self.by_root.get(_root(contract))

    def _choose_new_target(self, ctx, continuous):
        size = float(self.size[continuous])
        current = float(ctx.portfolio().net_position(continuous))
        choices = [-size, 0.0, size]
        choices = [target for target in choices if abs(target - current) > EPSILON]
        return self.rng[continuous].choice(choices)

    def _schedule_next(self, continuous, now):
        # A fresh random delay after every action makes each instrument follow
        # a different, irregular cadence.  With 1m bars, execution occurs on
        # the first bar at or after this timestamp.
        delay = self.rng[continuous].randint(3, 30)
        self.next_action[continuous] = now + timedelta(minutes=delay)
        return delay

    def _snapshot(self, ctx):
        portfolio = ctx.portfolio()
        actual = {}
        resolved_contract = {}
        resolved_actual = {}
        working = {}
        mismatches = {}

        for continuous in SYMBOLS:
            actual[continuous] = float(portfolio.net_position(continuous))
            instrument = ctx.instrument(continuous)
            contract = str(getattr(instrument, "current_contract", "") or "")
            resolved_contract[continuous] = contract
            resolved_actual[continuous] = (
                float(portfolio.net_position(contract)) if contract else 0.0
            )
            working[continuous] = bool(ctx.has_open_order(continuous))
            if (
                not working[continuous]
                and abs(actual[continuous] - self.expected_target[continuous]) > EPSILON
            ):
                mismatches[continuous] = {
                    "expected": self.expected_target[continuous],
                    "actual": actual[continuous],
                }

        return {
            "expected": dict(self.expected_target),
            "actual": actual,
            "resolved_contract": resolved_contract,
            "resolved_actual": resolved_actual,
            "working_order": working,
            "mismatches": mismatches,
            "bars_seen": dict(self.bars_seen),
            "actions": dict(self.actions),
        }

    def on_bar(self, ctx, event):
        bar = event.data()
        continuous = self._continuous_for_contract(bar.symbol)
        if continuous is None:
            logger.debug("ignored unmatched futures bar %s", bar.symbol)
            return

        self.bars_seen[continuous] += 1
        now = ctx.now()  # Already America/New_York; do not convert it.
        due = self.next_action[continuous]
        if due is None:
            delay = self._schedule_next(continuous, now)
            logger.debug("%s first action in %sm", continuous, delay)
            return
        if now < due:
            return

        target = self.pending_target[continuous]
        if target is None:
            target = self._choose_new_target(ctx, continuous)
            self.pending_target[continuous] = target

        # Address the continuous series.  The engine resolves it to the current
        # outright, while portfolio.net_position(continuous) remains the
        # aggregate series view that this probe is validating.
        order = ctx.order_to_target(continuous, target_quantity=target)
        if order is None:
            logger.debug(
                "%s target=%s deferred; working=%s current=%s",
                continuous,
                target,
                ctx.has_open_order(continuous),
                ctx.portfolio().net_position(continuous),
            )
            return

        self.pending_target[continuous] = None
        self.expected_target[continuous] = float(target)
        self.actions[continuous] += 1
        delay = self._schedule_next(continuous, now)
        ctx.add_event_log(
            f"{continuous} target {target:+g}; next random action in {delay}m",
            symbol=continuous,
            sub_event_type="TARGET_ORDER",
            state_variable={
                "continuous_symbol": continuous,
                "delivered_contract": str(bar.symbol),
                "target": float(target),
                "previous_position": float(ctx.portfolio().net_position(continuous)),
                "next_delay_minutes": delay,
            },
        )

    def on_order(self, ctx, event):
        order = event.data()
        continuous = self._continuous_for_contract(order.symbol)
        if continuous is None:
            return

        status = str(order.status).upper()
        if "REJECT" in status:
            self.expected_target[continuous] = float(
                ctx.portfolio().net_position(continuous)
            )
            self.pending_target[continuous] = None
            ctx.add_event_log(
                f"rejected {continuous}: {order.reject_reason}",
                symbol=continuous,
                sub_event_type="ORDER_REJECTED",
                state_variable=self._snapshot(ctx),
            )
            return

        if order.is_filled:
            snapshot = self._snapshot(ctx)
            ctx.add_event_log(
                f"filled {continuous} {order.side} {order.filled_qty} @ {order.avg_px}",
                symbol=continuous,
                sub_event_type=(
                    "POSITION_MISMATCH" if snapshot["mismatches"] else "POSITION_OK"
                ),
                state_variable=snapshot,
            )

    def on_timer(self, ctx, event):
        timer = event.data()
        if timer.timer_id != AUDIT_TIMER:
            return
        snapshot = self._snapshot(ctx)
        ctx.add_event_log(
            f"ten-futures position audit at {ctx.now():%Y-%m-%d %H:%M} ET",
            sub_event_type=(
                "POSITION_MISMATCH" if snapshot["mismatches"] else "POSITION_AUDIT"
            ),
            state_variable=snapshot,
        )

    def on_rollover(self, ctx, event):
        roll = event.data()
        ctx.add_event_log(
            f"rollover {roll.continuous_symbol}: "
            f"{roll.prev_contract} -> {roll.current_contract}",
            symbol=roll.continuous_symbol,
            sub_event_type="ROLLOVER",
            state_variable=self._snapshot(ctx),
        )


def main():
    from hiveq.flow.livesim import _base_url

    instance_name = f"MultiFuturesPositionProbe{int(time.time()) % 100000}"
    print(f"platform      : {_base_url()}")
    print(f"instance      : {instance_name}")

    deployment = hf.deploy_livesim(
        strategy_configs=[
            StrategyConfig(
                name="MultiFuturesPositionProbe",
                type="MultiFuturesPositionProbe",
                symbols=SYMBOLS,
                params={"assetType": "FUTURES"},
            )
        ],
        data_configs=[
            {
                "type": "live",
                "market_data_source": "SyntheticMarketData",
                "schema": ["fut_trades"],
            }
        ],
        instance_name=instance_name,
        container_id=CONTAINER_ID,
    )

    print(f"deployment    : {deployment.deployment_id}")
    print(f"artifact      : {deployment.artifact_id}")
    print(f"operation     : {deployment.operation_id}")
    state = deployment.wait(timeout=120)
    print(f"status        : {state.get('status')}")
    print(f"container     : {state.get('container_id')}")
    print(f"strategies    : {state.get('instance_names')}")
    if state.get("failure_reason"):
        print(f"failure       : {state['failure_reason']}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
