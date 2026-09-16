#!/usr/bin/env python3
"""End-to-end example: author a strategy, deploy it straight to LiveSim, watch it.

Flow: this script captures your strategy and deploys it to LiveSim with no
backtest anywhere in the path. You get a ``Deployment`` handle back, addressed
by ``deployment_id`` -- read state with ``status()``, logs with ``logs()``,
records with ``orders()`` / ``trades()`` / ``positions()`` / ``metrics()`` /
``events()``, and drive it with ``start()`` / ``stop()`` / ``pause()`` /
``terminate()``.

Every read is a pull, not a stream: each call returns a complete snapshot.

Prerequisites
-------------
1. Install the SDK:   pip install hiveq-sdk
2. Have a HiveQ API key available -- the only credential needed.

Run:
    python deploy_livesim.py               # deploy and inspect
    python deploy_livesim.py --dry-run     # validate and plan, deploy nothing
    python deploy_livesim.py --cleanup     # terminate it again at the end
"""

import argparse
import time

import hiveq.flow as hf
from hiveq.flow import StrategyConfig


# --- strategy ---------------------------------------------------------------
class LivesimPing:
    """Place one small limit order per interval, so there is something to read."""

    def __init__(self):
        self.last_ns = 0

    def on_start(self, ctx, event):
        ctx.add_event_log(message="LivesimPing started", symbol=ctx.symbols[0])

    def on_bar(self, ctx, event):
        bar = event.data()
        interval_ns = 60 * 1_000_000_000
        if bar.timestamp - self.last_ns < interval_ns:
            return
        self.last_ns = bar.timestamp
        ctx.place_order(
            symbol=bar.symbol,
            quantity=1,
            limit_price=round(bar.close * 0.99, 2),
        )


def _await_status(deployment, expected: str, timeout: float = 30.0) -> str:
    """Poll until the deployment reports ``expected``, or the timeout expires."""
    deadline = time.time() + timeout
    status = deployment.status()["status"]
    while status != expected and time.time() < deadline:
        time.sleep(1)
        status = deployment.status()["status"]
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--instance-name", default="LivesimPingExample")
    args = parser.parse_args()

    strategy = StrategyConfig(
        name="LivesimPing",
        type="LivesimPing",
        symbols=["AAPL"],
        # Tag the asset so the platform places you on an equities container.
        # With no backtest to infer from, a strategy that does not tag itself
        # falls back to the futures default and lands on the wrong box.
        params={"assetType": "EQUITY"},
    )

    # The live data this strategy wants.
    data_configs = [
        {
            "type": "live",
            "market_data_source": "Activ",
            "schema": ["eq_trades"],
        }
    ]

    # --- deploy -------------------------------------------------------------
    deployment = hf.deploy_livesim(
        [strategy],
        data_configs=data_configs,
        instance_name=args.instance_name,
        dry_run=args.dry_run,
    )
    print(f"deployment    : {deployment}")
    print(f"  operation_id: {deployment.operation_id}")
    print(f"  artifact_id : {deployment.artifact_id}")

    if args.dry_run:
        # A preview materializes nothing, so it has no id to track.
        print("\ndry run: validated and planned, nothing deployed")
        return 0

    # --- wait for it to settle ---------------------------------------------
    state = deployment.wait(timeout=120)
    print(f"\nstatus        : {state['status']}")
    print(f"  container   : {state['container_id']}")
    print(f"  strategies  : {', '.join(state['instance_names'])}")
    if state.get("failure_reason"):
        # Set when the engine could not bring the strategy up.
        print(f"  failed      : {state['failure_reason']}")
        return 1

    # --- logs ---------------------------------------------------------------
    logs = deployment.logs(tail=20)
    print(f"\nlogs ({len(logs.splitlines())} lines):")
    for line in logs.splitlines()[-5:]:
        print(f"  {line}")

    # --- records ------------------------------------------------------------
    # Empty until the strategy trades -- outside market hours that is expected,
    # not a failure.
    print("\nrecords:")
    for name in ("orders", "trades", "positions", "metrics", "events"):
        rows = getattr(deployment, name)(limit=100)
        print(f"  {name:10}: {len(rows)} rows")

    # CSV instead of rows, for anything you want to keep.
    csv = deployment.orders(limit=100, format="csv")
    print(f"  orders.csv: {len(csv.splitlines())} lines")

    # --- lifecycle ----------------------------------------------------------
    # Lifecycle commands travel to the engine and are applied asynchronously,
    # so poll for the state rather than sleeping a fixed amount.
    print("\nlifecycle:")
    deployment.pause()
    print(f"  paused    -> {_await_status(deployment, 'paused')}")
    deployment.start()
    print(f"  started   -> {_await_status(deployment, 'running')}")

    if args.cleanup:
        # Removes the deployment entirely. Not reversible.
        deployment.terminate()
        print("  terminated")
    else:
        print(f"\nstill running. terminate with:")
        print(
            f"  hf.get_deployment('{deployment.deployment_id}').terminate()"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
