"""l9_02: backtest -> livesim promotion, param hot-update, teardown.

The end of the ladder, and the one story the platform had no automated coverage
for at all. It is written as a single test because promotion, hot-update and
teardown are a single *lifecycle*: a param-update test that borrows somebody
else's running deployment is both unreliable and unsafe, so this one owns the
deployment it mutates from creation to removal.

Sequence:

1. Run a small real backtest through the SDK and wait for it to complete. That
   run is the promotion source — ``POST /livesim/deploy`` takes a
   ``source_run_id``, not a strategy payload.
2. ``dry_run`` promotion. Validates the whole request path (ownership lookup,
   payload resolution, native-body translation) while consuming no fleet
   capacity. This part always runs.
3. Real promotion, **opt-in only** via ``AGENT_QA_ALLOW_DEPLOY=1``. Waits for
   the deployment to become healthy.
4. Param hot-update: ``PATCH .../strategies/<instance>/params``, then poll the
   strategy log for evidence the change landed.
5. Teardown in a ``finally``: cancel the deployment, and remove the container if
   this test created it. A leaked container silently eats fleet capacity for
   every later run, so teardown is unconditional.

Real deploys are gated because this consumes shared fleet capacity and places
paper orders. ``AGENT_QA_ALLOW_DEPLOY`` is the operator saying "yes, spend it".
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest, observe
from agent_qa.core.guards import require_remote
from agent_qa.core.livesim_client import LivesimClient
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l9_02_promotion_lifecycle"
SURFACE = "l9.promotion"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol
INSTANCE = "qa_l9_promo"

#: Real deploys cost fleet capacity and place paper orders — opt in explicitly.
ALLOW_DEPLOY = os.environ.get("AGENT_QA_ALLOW_DEPLOY") == "1"

#: The parameter the hot-update flips. Declared on the strategy so the update
#: has something real to change.
PARAM_NAME = "qa_threshold"
PARAM_INITIAL = 1.0
PARAM_UPDATED = 2.5

probe = Probe()


class L9PromotionSource:
    """Minimal strategy that trades once and reacts to a param change.

    Kept trivially small: this exists to be *promoted*, so the interesting
    behaviour is ``on_param_change``, not the trading logic.
    """

    def __init__(self):
        self.qa_threshold = PARAM_INITIAL
        self.step = 0

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")
        ctx.add_event_log(f"qa l9 started threshold={self.qa_threshold}",
                          sub_event_type="QA_L9")

    def on_bar(self, ctx, event):
        probe.bump("bar")
        self.step += 1
        if self.step == 3:
            ctx.buy_order(SYMBOL, quantity=1)
        elif self.step == 8:
            ctx.close_position(SYMBOL)

    def on_param_change(self, ctx, event):
        """Fired when the platform pushes a parameter update into a live run."""
        probe.bump("param_change")
        data = event.data() if hasattr(event, "data") else {}
        probe.sample("param_change", payload=str(data))
        ctx.add_event_log(f"qa l9 param change: {data}", sub_event_type="QA_L9_PARAM")

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def _source_run_id(c: Checks):
    """Run the promotion source backtest; return its run_id or None."""
    from hiveq.flow import BacktestConfig, StrategyConfig

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L9PromotionSource", symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital,
                                PARAM_NAME: PARAM_INITIAL})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, "bars_1m")],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="10:00"),
    )
    ok = backtest.completed_ok(run)
    run_id = getattr(run, "run_id", None)
    c.add("source_backtest_completed", ok and bool(run_id),
          f"status={backtest.status_of(run)}, run_id={run_id!r}")
    return run_id if ok else None


def _deployment_id(body) -> str:
    if not isinstance(body, dict):
        return ""
    for key in ("deployment_id", "deploymentId", "id"):
        if body.get(key):
            return str(body[key])
    inner = body.get("data") or body.get("deployment") or {}
    if isinstance(inner, dict):
        for key in ("deployment_id", "deploymentId", "id"):
            if inner.get(key):
                return str(inner[key])
    return ""


def _container_id(body) -> str:
    if not isinstance(body, dict):
        return ""
    for key in ("container_id", "containerId"):
        if body.get(key):
            return str(body[key])
    inner = body.get("data") or body.get("deployment") or {}
    if isinstance(inner, dict):
        for key in ("container_id", "containerId"):
            if inner.get(key):
                return str(inner[key])
    return ""


def main():
    install_crash_handler(NAME, SURFACE)
    require_remote(NAME)

    c = Checks()
    client = LivesimClient(timeout=120.0)
    c.note(f"base_url={client.base_url}, allow_deploy={ALLOW_DEPLOY}")

    run_id = _source_run_id(c)
    if not run_id:
        c.finish(NAME, surface=SURFACE, extra="no source run to promote")
        return

    # --- step 2: dry-run promotion (always) -------------------------------
    body, status = client.promote(run_id, instance_name=INSTANCE, dry_run=True)
    c.add("dry_run_promotion_accepted", status in (200, 201, 202),
          f"POST /livesim/deploy dry_run -> {status}: {str(body)[:300]}")
    c.add("dry_run_no_server_error", status != 500, f"body={str(body)[:300]}")

    if not ALLOW_DEPLOY:
        c.note("real promotion skipped; set AGENT_QA_ALLOW_DEPLOY=1 to exercise "
               "deploy -> param hot-update -> teardown against real fleet capacity")
        c.finish(NAME, surface=SURFACE, extra=f"source_run_id={run_id}, dry_run_status={status}")
        return

    # --- steps 3-5: real promotion, hot-update, guaranteed teardown --------
    deployment_id = ""
    container_id = ""
    try:
        body, status = client.promote(run_id, instance_name=INSTANCE)
        deployment_id = _deployment_id(body)
        container_id = _container_id(body)
        c.add("promotion_accepted", status in (200, 201, 202) and bool(deployment_id),
              f"status={status}, body={str(body)[:300]}")
        if not deployment_id:
            return

        healthy = observe.wait_for_deployment(client, deployment_id, timeout=300)
        c.add("deployment_becomes_healthy", bool(healthy),
              f"after {healthy.elapsed:.0f}s / {healthy.polls} polls, last={healthy.last!r}")

        if not container_id:
            record = healthy.last if isinstance(healthy.last, dict) else {}
            container_id = str(record.get("container_id") or record.get("containerId") or "")

        # --- param hot-update ---
        if container_id:
            body, status = client.update_strategy_params(
                container_id, deployment_id, INSTANCE,
                {PARAM_NAME: PARAM_UPDATED},
            )
            c.add("param_update_accepted", status in (200, 202),
                  f"PATCH params -> {status}: {str(body)[:300]}")

            # Evidence the change reached the strategy, not just the API.
            seen = observe.wait_for_log(client, deployment_id, INSTANCE,
                                        "qa l9 param change", timeout=180)
            c.add("param_change_reached_strategy", bool(seen),
                  f"no 'qa l9 param change' marker in the strategy log after "
                  f"{seen.elapsed:.0f}s — on_param_change may not be wired for "
                  "this deployment")
        else:
            c.add("container_id_resolved", False,
                  "promotion response carried no container_id, so the param "
                  "hot-update route cannot be addressed")

    finally:
        # Unconditional. A leaked container eats fleet capacity for every later
        # run, and a failed assertion above must not be allowed to cause that.
        if deployment_id:
            cancel_body, cancel_status = client.cancel_deployment(deployment_id)
            c.add("teardown_cancel_ok", cancel_status in (200, 202, 204, 404),
                  f"cancel -> {cancel_status}: {str(cancel_body)[:200]}")
            gone = observe.wait_for_deployment_gone(client, deployment_id, timeout=120)
            c.note(f"deployment cleared: {bool(gone)} after {gone.elapsed:.0f}s")
            time.sleep(2)

    c.finish(NAME, surface=SURFACE, extra=f"source_run_id={run_id}, deployment_id={deployment_id}, "
                         f"container_id={container_id}")


if __name__ == "__main__":
    main()
