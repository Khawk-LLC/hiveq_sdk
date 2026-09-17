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
    python deploy_livesim.py --local                      # against a local stack
    python deploy_livesim.py --local --observe 300        # wait longer for orders
    python deploy_livesim.py --local --dry-run  # validate and plan, deploy nothing
    python deploy_livesim.py --local --cleanup  # terminate it again at the end

--local loads ~/.hiveq/profiles/local.env. Without it, the run targets whatever
~/.hiveq/.env points at. Either way it prints the platform it chose, because a
deploy going somewhere unintended is the easiest mistake to make here.
"""

import argparse
import os
import time


# --- strategy ---------------------------------------------------------------
# Module level, not a CLI arg: the strategy is shipped as source and rebuilt on
# the platform, so anything it references has to be in that source.
# Continuous front-month, which is what the platform's futures feeds
# publish under. A dated contract like "ESU6" subscribes to nothing.
SYMBOL = "ES.c.0"


class LivesimPing:
    """Rest one passive limit order per interval, so there is order flow to see.

    Driven by a **timer**, not by trades. A trade-driven probe only fires when
    ticks arrive, so it goes silent in a quiet overnight session and there is
    nothing to check a pause against. A timer fires regardless.

    The limit sits well below the market, so it produces orders to read without
    filling or taking a position.
    """

    TIMER_ID = "ping"
    AWAY_FRACTION = 0.90  # far enough below the market that it will not trade

    def __init__(self):
        self.last_price = 0.0
        self.sent = 0

    def on_start(self, ctx, event):
        from datetime import timedelta

        from hiveq.flow.config import AssetType

        # Subscriptions belong in on_start (R3). Listing symbols on the
        # StrategyConfig alone does not subscribe you.
        ctx.subscribe_trades([SYMBOL], AssetType.FUTURES)
        ctx.set_timer(self.TIMER_ID, timedelta(seconds=20))
        ctx.add_event_log(message="LivesimPing started", symbol=SYMBOL)

    def on_trade(self, ctx, event):
        # Only track the last price here; the order goes out on the timer.
        price = float(getattr(event.data(), "price", 0) or 0)
        if price > 0:
            self.last_price = price

    def on_timer(self, ctx, event):
        if self.last_price <= 0:
            return

        from hiveq.flow.trading import price_utils
        from hiveq.flow.trading_types import OrderSide, OrderType

        # Round to the instrument's tick. ES ticks at 0.25, so a plain
        # round(price, 2) is rejected OFF_TICK and never reaches the book.
        limit = price_utils.adjust_tick_size(
            SYMBOL, self.last_price * self.AWAY_FRACTION
        )
        ctx.place_order(
            SYMBOL, OrderSide.BUY, 1, OrderType.LIMIT, limit_price=limit
        )
        self.sent += 1
        ctx.add_event_log(
            message=f"ping #{self.sent}: resting buy at {limit}",
            symbol=SYMBOL,
        )


def _load_profile(name: str) -> None:
    """Load ~/.hiveq/profiles/<name>.env into the environment, overriding."""
    path = os.path.join(
        os.path.expanduser("~"), ".hiveq", "profiles", f"{name}.env"
    )
    if not os.path.isfile(path):
        raise SystemExit(f"no such profile: {path}")
    with open(path) as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


def _order_count(deployment) -> int:
    """How many orders this deployment has placed so far."""
    return len(deployment.orders(limit=10_000))


def _await_orders(deployment, timeout: float, poll: float = 10.0) -> int:
    """Poll until the strategy places its first orders, or the window expires.

    Returns the count seen. Zero is a legitimate outcome -- outside market
    hours nothing will trade, and that is not a failure of the deployment.
    """
    deadline = time.time() + timeout
    count = _order_count(deployment)
    while count == 0 and time.time() < deadline:
        remaining = int(deadline - time.time())
        print(f"  ... none yet, {remaining}s left")
        time.sleep(poll)
        count = _order_count(deployment)
    return count


def _await_gone(deployment, timeout: float = 30.0) -> bool:
    """True once the deployment stops resolving, which is what terminate means.

    Checking the handle rather than trusting the call is the only way to tell a
    termination that took from one that was merely accepted.
    """
    from hiveq.flow.livesim import LivesimError

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            deployment.status()
        except LivesimError as exc:
            if exc.status == 404:
                return True
            raise
        time.sleep(2)
    return False


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
    parser.add_argument(
        "--instance-name",
        # Unique per run: an instance name is unique within its
        # container, so a fixed one makes the second run collide with
        # whatever the first left behind.
        default=f"LivesimPing{int(time.time()) % 100000}",
    )
    parser.add_argument(
        "--observe",
        type=int,
        default=180,
        help="Seconds to wait for the strategy to place its first orders "
        "(default 180). Without orders a pause cannot be verified.",
    )
    parser.add_argument(
        "--settle",
        type=int,
        default=60,
        help="Seconds to hold after pausing and after resuming, to see "
        "whether order flow actually stopped and restarted (default 60).",
    )
    parser.add_argument(
        "--profile",
        help="Load ~/.hiveq/profiles/<name>.env for this run, overriding the "
        "active ~/.hiveq/.env. Use this to target a platform other than the "
        "one your profile currently points at.",
    )
    parser.add_argument(
        "--local",
        action="store_const",
        const="local",
        dest="profile",
        help="Shorthand for --profile local",
    )
    args = parser.parse_args()

    # Applied before the SDK is imported: it reads ~/.hiveq/.env at import time
    # and only fills in what is unset, so overriding afterwards is too late.
    # The whole profile is loaded, not just the URL -- each platform has its own
    # API key, and pointing the URL at one platform while still holding
    # another's key just fails authentication.
    if args.profile:
        _load_profile(args.profile)

    import hiveq.flow as hf
    from hiveq.flow import StrategyConfig
    from hiveq.flow.livesim import _base_url

    # Print it every run. A deploy silently going to the wrong platform is the
    # single easiest mistake to make here.
    print(f"platform      : {_base_url()}")

    # Futures by default: they trade nearly around the clock, so the pause and
    # resume checks below have order flow to measure. An equities symbol only
    # trades during US market hours, and outside them there is nothing to
    # prove a pause against.
    strategy = StrategyConfig(
        name="LivesimPing",
        type="LivesimPing",
        symbols=[SYMBOL],
        # Tag the asset so the platform places you on the right container.
        # With no backtest to infer from, an untagged strategy falls back to
        # the futures default.
        params={"assetType": "FUTURES"},
    )

    data_configs = [
        {
            "type": "live",
            "market_data_source": "Activ",
            "schema": ["fut_trades"],
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

    # --- watch it trade -----------------------------------------------------
    # Wait for the strategy to actually place orders. Without this there is
    # nothing to check a pause against: a deployment that has never traded
    # looks identical whether it is running or paused.
    print(f"\nwatching for orders (up to {args.observe}s):")
    traded = _await_orders(deployment, args.observe)
    if traded == 0:
        print("  no orders yet -- the market is likely closed for this symbol.")
        print("  lifecycle is still exercised below, but without order-flow")
        print("  evidence the pause can only be checked by its reported state.")
    else:
        print(f"  {traded} orders placed")

    print("\nrecords:")
    for name in ("orders", "trades", "positions", "metrics", "events"):
        rows = getattr(deployment, name)(limit=1000)
        print(f"  {name:10}: {len(rows)} rows")
    csv = deployment.orders(limit=1000, format="csv")
    print(f"  orders.csv: {len(csv.splitlines())} lines")

    # --- pause, and prove it took ------------------------------------------
    print("\npause:")
    before = _order_count(deployment)
    deployment.pause()
    status = _await_status(deployment, "paused")
    print(f"  status      : {status}")
    print(f"  orders at pause: {before}")

    print(f"  holding {args.settle}s to see whether anything still trades...")
    time.sleep(args.settle)
    during = _order_count(deployment)
    print(f"  orders after   : {during}")
    if traded == 0:
        print("  inconclusive: it was not trading before the pause either.")
    elif during == before:
        print("  PAUSED: no new orders while paused.")
    else:
        print(f"  NOT PAUSED: {during - before} new orders arrived while paused.")

    # --- resume, and prove that took too ------------------------------------
    print("\nresume:")
    deployment.start()
    print(f"  status      : {_await_status(deployment, 'running')}")
    if traded:
        print(f"  holding {args.settle}s to see trading resume...")
        time.sleep(args.settle)
        after = _order_count(deployment)
        print(f"  orders after   : {after}")
        if after > during:
            print(f"  RESUMED: {after - during} new orders since resuming.")
        else:
            print("  no new orders yet -- may simply be between intervals.")

    # --- terminate, and confirm it is gone ----------------------------------
    if args.cleanup:
        print("\nterminate:")
        deployment.terminate()
        gone = _await_gone(deployment)
        if gone:
            print("  TERMINATED: the handle no longer resolves.")
        else:
            print("  still resolving -- termination did not take.")
            return 1
    else:
        print("\nstill running. terminate with:")
        print(f"  hf.get_deployment('{deployment.deployment_id}').terminate()")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
