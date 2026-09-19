#!/usr/bin/env python3
"""Change a parameter on a running LiveSim deployment, restart its container,
and prove the second start comes up on the changed value -- not the default.

Why this example exists
-----------------------
The engine is the source of truth for parameter values. It keeps its own param
store, writes it on every change, and reloads it at startup -- so a value that
reached the engine is the value the container comes back up on, ahead of
whatever the deployment was configured with.

That is why ``set_params()`` pushes into the running engine rather than only
recording the change: a change that never reached the engine is not in force,
however faithfully it was stored. It writes the deployment's stored
configuration too, which seeds a strategy on its first run and is the record
you read back with ``params()``.

The corollary is that this needs the engine running. On a stopped deployment
the stored configuration is updated but the engine never hears, and the next
start comes up on what the engine last had.

What it proves, in order
------------------------
  params()          the deployed default
  set_params()      changed + old_values, and the engine reports it live
  restart           the container goes down and comes back
  params()          the changed value is still there
  event logs        the strategy's own startup line echoes the value it
                    loaded -- the proof it booted on it, not merely that the
                    file survived

That last step is the one that matters. Re-reading params() after a restart
only shows the file kept the value; the strategy's startup log shows the
strategy actually used it.

Prerequisites
-------------
1. pip install hiveq-sdk
2. A HiveQ API key.
3. **Restarting a container is a system-admin action.** It is the one step
   here with no SDK method, because it is not a per-deployment operation: one
   container hosts many people's strategies, and restarting it restarts all of
   them. Without the admin role, run with --no-restart and bounce the
   container yourself, or see --manual-restart below.

Run:
    python livesim_param_restart.py --local
    python livesim_param_restart.py --local --no-restart   # stop before the restart
    python livesim_param_restart.py --local --cleanup      # terminate at the end
"""

import argparse
import os
import time

# --- strategy ---------------------------------------------------------------
# Module level: the strategy ships as source and is rebuilt on the platform, so
# anything it references has to live in this module.
SYMBOL = "ES.c.0"

# The parameter this example moves. Declared on the StrategyConfig below,
# because only parameters a strategy already declares can be set -- an unknown
# name is rejected rather than quietly added, so a typo cannot look like it
# worked.
PARAM = "ping_seconds"
DEFAULT_PING_SECONDS = 20
CHANGED_PING_SECONDS = 45


class ParamRestartPing:
    """Rests a passive limit order every ``ping_seconds``.

    Reads its parameter from ``ctx.strategy_config`` in ``on_start`` and logs
    what it read. That log line is what the restart check below looks for.

    **This reading is not optional.** Nothing seeds a deployed parameter onto
    a strategy's own attributes at startup -- the framework only assigns
    ``self.<name>`` when a parameter *changes* at runtime. A strategy that sets
    a value in ``__init__`` and never re-reads the config will pick up a live
    change (because that path assigns the attribute) and then come back on the
    hardcoded default after a restart. That is the same silent revert this
    example exists to rule out, one layer up, and only the strategy's own code
    can prevent it.
    """

    TIMER_ID = "ping"
    AWAY_FRACTION = 0.90  # far below the market, so it rests without filling

    def __init__(self):
        self.last_price = 0.0
        self.sent = 0
        # Held so on_param_change can re-arm the timer. It is handed one in
        # on_start; None until then, because a parameter can in principle be
        # changed before the strategy has started.
        self.ctx = None
        # A starting point only. on_start overwrites it from the config; see
        # the class docstring for why relying on this value would be a bug.
        self.ping_seconds = DEFAULT_PING_SECONDS

    def on_start(self, ctx, event):
        from datetime import timedelta

        from hiveq.flow.config import AssetType

        # The deployed value, which after a restart is whatever was last
        # persisted -- not what this class was written with.
        self.ctx = ctx
        params = getattr(ctx.strategy_config, "params", None) or {}
        self.ping_seconds = int(params.get("ping_seconds", DEFAULT_PING_SECONDS))

        ctx.subscribe_trades([SYMBOL], AssetType.FUTURES)
        ctx.set_timer(self.TIMER_ID, timedelta(seconds=self.ping_seconds))

        # The line the restart check greps for. An event log rather than a
        # logger call: event logs are readable through the API at any log
        # level, and the executor's default level is WARNING.
        ctx.add_event_log(
            message=f"started with ping_seconds={self.ping_seconds}",
            symbol=SYMBOL,
        )

    def on_param_change(self, param_name, old_value, new_value):
        """Fires when the value changes while the strategy is running."""
        if param_name != "ping_seconds":
            return
        from datetime import timedelta

        self.ping_seconds = int(new_value)
        # Re-arm: the timer was created with the old interval, so without this
        # the new value would be recorded and never acted on. No ctx yet means
        # the strategy has not started, and on_start will arm it correctly.
        if self.ctx is not None:
            self.ctx.set_timer(
                self.TIMER_ID, timedelta(seconds=self.ping_seconds)
            )

    def on_trade(self, ctx, event):
        price = float(getattr(event.data(), "price", 0) or 0)
        if price > 0:
            self.last_price = price

    def on_timer(self, ctx, event):
        if self.last_price <= 0:
            return

        from hiveq.flow.trading import price_utils
        from hiveq.flow.trading_types import OrderSide, OrderType

        # ES ticks at 0.25, so round to the instrument's tick or the order is
        # rejected OFF_TICK and never reaches the book.
        limit = price_utils.adjust_tick_size(
            SYMBOL, self.last_price * self.AWAY_FRACTION
        )
        ctx.place_order(
            SYMBOL, OrderSide.BUY, 1, OrderType.LIMIT, limit_price=limit
        )
        self.sent += 1
        ctx.add_event_log(
            message=f"ping #{self.sent} at {limit} "
            f"(every {self.ping_seconds}s)",
            symbol=SYMBOL,
        )


# --- helpers ----------------------------------------------------------------


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


def _restart_container(container_id: str) -> None:
    """Restart a LiveSim container through the admin REST surface.

    Deliberately not an SDK method: a container is shared infrastructure, not
    part of one deployment, so restarting it restarts every strategy on it.
    It lives on the v0 admin surface and needs the system-admin role.
    """
    import requests

    from hiveq.flow.config import platform_origin

    origin = (platform_origin() or "http://localhost").rstrip("/")
    url = (
        f"{origin}/api/strategy/v0/run/livesim/containers/"
        f"{container_id}/restart"
    )
    response = requests.post(
        url, headers={"X-API-Key": os.environ["HIVEQ_API_KEY"]}, timeout=120
    )
    if response.status_code == 403:
        raise SystemExit(
            "restarting a container requires the system-admin role.\n"
            "Re-run with --no-restart and bounce it yourself:\n"
            f"  docker restart {container_id}\n"
            "then re-run with --deployment-id to check the value that came back."
        )
    response.raise_for_status()


def _persisted(deployment, strategy: str):
    """The stored value of PARAM -- what the next container start will load."""
    # live=False so this still answers while the container is down, which is
    # exactly when it is most worth asking.
    entry = deployment.params(strategy=strategy, live=False)[strategy]
    return entry["params"].get(PARAM)


def _await_engine(deployment, strategy: str, timeout: float = 240.0):
    """Wait for the engine to answer again after a restart.

    Returns its live value for PARAM, or None if it never came back. The
    engine is unreachable for as long as the container is down, which is what
    live_available reports.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        entry = deployment.params(strategy=strategy)[strategy]
        if entry.get("live_available"):
            return entry.get("live", {}).get(PARAM)
        remaining = int(deadline - time.time())
        print(f"  ... engine still down, {remaining}s left")
        time.sleep(10)
    return None


def _startup_lines(deployment, strategy: str) -> list:
    """Every 'started with ping_seconds=N' the strategy has logged.

    One per engine lifetime: the strategy logs it in on_start, and a container
    restart starts it again. The last one is therefore the value the current
    start came up on.
    """
    rows = deployment.events(strategy=strategy, limit=10_000)
    return [
        str(row.get("message") or "")
        for row in rows
        if "started with ping_seconds=" in str(row.get("message") or "")
    ]


def _await_startup_line(deployment, strategy: str, seen: int, timeout: float = 240.0):
    """Wait for a startup line beyond the ``seen`` already recorded.

    Polled rather than read once: event logs reach the read path through
    Kafka, so a line the strategy has already written is not queryable the
    instant the engine is back.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        lines = _startup_lines(deployment, strategy)
        if len(lines) > seen:
            return lines[-1]
        remaining = int(deadline - time.time())
        print(f"  ... no new startup log yet, {remaining}s left")
        time.sleep(10)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="Change the parameter and stop before the restart. Use when you "
        "lack the admin role and want to bounce the container yourself.",
    )
    parser.add_argument(
        "--deployment-id",
        help="Skip deploying and check an existing deployment instead. Pair "
        "with a manual restart: run once with --no-restart, restart the "
        "container by hand, then re-run with this.",
    )
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument(
        "--instance-name",
        # Unique per run: instance names are unique within a container, so a
        # fixed one collides with whatever the last run left behind.
        default=f"ParamRestart{int(time.time()) % 100000}",
    )
    parser.add_argument(
        "--settle",
        type=int,
        default=45,
        help="Seconds to let the strategy run before changing the parameter, "
        "so there is a before-and-after to compare (default 45).",
    )
    parser.add_argument("--profile")
    parser.add_argument(
        "--local", action="store_const", const="local", dest="profile"
    )
    args = parser.parse_args()

    # Before the SDK is imported: it reads ~/.hiveq/.env at import time and
    # only fills in what is unset, so overriding afterwards is too late.
    if args.profile:
        _load_profile(args.profile)

    import hiveq.flow as hf
    from hiveq.flow import StrategyConfig
    from hiveq.flow.livesim import _base_url

    print(f"platform      : {_base_url()}")

    # --- deploy, or re-attach ----------------------------------------------
    if args.deployment_id:
        deployment = hf.get_deployment(args.deployment_id)
        state = deployment.status()
        strategy = state["instance_names"][0]
        print(f"deployment    : {args.deployment_id} (existing)")
    else:
        deployment = hf.deploy_livesim(
            [
                StrategyConfig(
                    name="ParamRestartPing",
                    type="ParamRestartPing",
                    symbols=[SYMBOL],
                    params={
                        # Tag the asset or an untagged strategy falls back to
                        # the futures default and may land elsewhere.
                        "assetType": "FUTURES",
                        # Declared here so it can be changed later.
                        PARAM: DEFAULT_PING_SECONDS,
                    },
                )
            ],
            data_configs=[
                {
                    "type": "live",
                    "market_data_source": "Activ",
                    "schema": ["fut_trades"],
                }
            ],
            instance_name=args.instance_name,
        )
        state = deployment.wait(timeout=120)
        strategy = state["instance_names"][0]
        print(f"deployment    : {deployment.deployment_id}")
        print(f"  status      : {state['status']}")
        if state.get("failure_reason"):
            print(f"  failed      : {state['failure_reason']}")
            return 1

    container = deployment.status()["container_id"]
    print(f"  container   : {container}")
    print(f"  strategy    : {strategy}")

    # --- 1. the deployed default -------------------------------------------
    entry = deployment.params(strategy=strategy)[strategy]
    print("\n1. deployed value")
    print(f"   persisted  : {PARAM}={entry['params'].get(PARAM)}")
    if entry.get("live_available"):
        print(f"   live       : {PARAM}={entry.get('live', {}).get(PARAM)}")
        print(f"   drift      : {entry.get('drift')}")

    if not args.deployment_id:
        print(f"\n   letting it run {args.settle}s before changing anything...")
        time.sleep(args.settle)

    startup_count = len(_startup_lines(deployment, strategy))

    # --- 2. change it -------------------------------------------------------
    print(f"\n2. set_params({{{PARAM}: {CHANGED_PING_SECONDS}}})")
    result = deployment.set_params(
        {PARAM: CHANGED_PING_SECONDS}, strategy=strategy
    )[strategy]
    print(f"   changed    : {result['changed']}")
    print(f"   old_values : {result.get('old_values')}")
    print(f"   engine     : notified={result.get('engine_notified')}")
    if result.get("engine_note"):
        # Persisted but not live: the value is safe on disk and will apply at
        # the next start. Not a failure, and worth telling apart from one.
        print(f"   engine note: {result['engine_note']}")

    entry = deployment.params(strategy=strategy)[strategy]
    print(f"   persisted  : {PARAM}={entry['params'].get(PARAM)}")
    if entry.get("live_available"):
        print(f"   live       : {PARAM}={entry.get('live', {}).get(PARAM)}")

    if args.no_restart:
        print("\nstopping before the restart, as asked. To finish the check:")
        print(f"  docker restart {container}")
        print(
            f"  python {os.path.basename(__file__)} "
            f"--deployment-id {deployment.deployment_id}"
        )
        return 0

    # --- 3. restart the container ------------------------------------------
    print(f"\n3. restarting container {container}")
    print("   (shared infrastructure: this restarts every strategy on it)")
    _restart_container(container)

    live_after = _await_engine(deployment, strategy)
    if live_after is None:
        print("   engine did not come back in time")
        return 1
    print("   engine back up")

    # --- 4. what came back --------------------------------------------------
    print("\n4. after the restart")
    print(f"   persisted  : {PARAM}={_persisted(deployment, strategy)}")
    print(f"   live       : {PARAM}={live_after}")

    # --- 5. the proof -------------------------------------------------------
    # params() alone only shows the file kept the value. The strategy's own
    # startup line shows it came up on it.
    print("\n5. the strategy's startup log from this second start")
    line = _await_startup_line(deployment, strategy, startup_count)
    if line is None:
        print("   no new startup line -- cannot confirm what it booted on")
        return 1
    print(f"   {line!r}")

    expected = f"started with ping_seconds={CHANGED_PING_SECONDS}"
    if line.strip().endswith(expected):
        print(
            f"\n   PASS: the second start came up on {CHANGED_PING_SECONDS}, "
            f"the changed value."
        )
        ok = True
    else:
        print(
            f"\n   FAIL: expected {expected!r}. The change did not survive the "
            f"restart -- it reached the engine but not the stored config."
        )
        ok = False

    if args.cleanup:
        print("\nterminating")
        deployment.terminate()
    else:
        print("\nstill running. terminate with:")
        print(f"  hf.get_deployment('{deployment.deployment_id}').terminate()")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
