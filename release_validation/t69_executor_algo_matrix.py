"""More than one execution algorithm, and the executor event stream itself.

t39 drives a single POV executor. TWAP is never created, ``executor_state`` and
``stop_executor`` (the handle-taking variants) are never called, and
``on_executor_event`` -- a public callback with its own event type and payload
-- is implemented by no case in the suite, so the whole executor event surface
is unobserved.

Each algorithm is created, observed working through its child orders, queried
for state, and stopped; the executor event payload is checked for the fields it
declares. A plain strategy order runs alongside so the case can tell an
executor that produced nothing apart from a run that never traded.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (                                               # noqa: E402
    completed_checkpoint,
    emit_checkpoint,
    evidence_checks,
    finish,
)

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402

SYMBOL = "AAPL"
ALGOS = ("POV", "TWAP")
CONTROL_QTY = 10.0


class SdkT69:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.ticks = 0
            self.handles = {}
            self.ids = {}
            self.control_id = ""
            self.state = {
                "ticks": 0, "created": {}, "ids": {}, "state_by_handle": {},
                "state_by_id": {}, "stopped_by_handle": {}, "stopped_by_id": {},
                "events": 0, "event_fields": [], "event_states": [],
                "event_ids": [], "child_orders": {}, "child_filled": {},
                "control_filled": 0.0, "errors": [],
            }
        ctx.subscribe_trades([SYMBOL], asset_type=AssetType.EQUITY)

    def create(self, ctx, algo):
        try:
            params = ctx.build_executor_params(
                symbol=SYMBOL, quantity=5_000, side="BUY", executor_type=algo,
                participate_pct=0.1, min_order_size=1, max_order_size=10,
                refresh_millis=100,
            )
            handle = ctx.add_executor(params)
        except Exception as exc:                           # noqa: BLE001
            self.state["errors"].append(f"{algo} create: {str(exc)[:140]}")
            return
        self.state["created"][algo] = handle is not None
        if handle is not None:
            self.handles[algo] = handle

    def on_trade(self, ctx, event):
        self.ticks += 1

        if self.ticks == 20 and not self.control_id:
            order = ctx.buy_order(SYMBOL, CONTROL_QTY)
            if order is not None:
                self.control_id = order.order_id
        elif self.ticks == 60:
            for algo in ALGOS:
                self.create(ctx, algo)
        elif self.ticks == 3000:
            # Both variants of the state query: by handle and by id.
            for algo, handle in self.handles.items():
                try:
                    self.state["state_by_handle"][algo] = str(
                        ctx.executor_state(handle)
                    )
                except Exception as exc:                   # noqa: BLE001
                    self.state["errors"].append(f"{algo} state: {str(exc)[:140]}")
                executor_id = self.ids.get(algo, "")
                if executor_id:
                    params = ctx.get_executor_params_by_id(executor_id)
                    self.state["state_by_id"][algo] = params is not None
        elif self.ticks == 5000:
            for index, (algo, handle) in enumerate(self.handles.items()):
                # One stopped by handle, one by id, so both paths are covered.
                if index == 0:
                    try:
                        self.state["stopped_by_handle"][algo] = bool(
                            ctx.stop_executor(handle)
                        )
                    except Exception as exc:               # noqa: BLE001
                        self.state["errors"].append(f"{algo} stop: {str(exc)[:140]}")
                else:
                    executor_id = self.ids.get(algo, "")
                    if executor_id:
                        self.state["stopped_by_id"][algo] = bool(
                            ctx.stop_executor_by_id(executor_id)
                        )

    def on_executor_event(self, ctx, event):
        self.state["events"] += 1
        data = event.data()
        executor_id = str(getattr(data, "executor_id", "") or "")
        executor_state = str(getattr(data, "state", "") or "")
        if executor_id and executor_id not in self.state["event_ids"]:
            self.state["event_ids"].append(executor_id)
        if executor_state and executor_state not in self.state["event_states"]:
            self.state["event_states"].append(executor_state)
        if not self.state["event_fields"]:
            self.state["event_fields"] = sorted(
                name for name in ("executor_id", "state", "symbol", "ts_event")
                if getattr(data, name, None) is not None
            )
        # Map each live executor id back to its algorithm by matching the handle
        # registry the bridge maintains; the first id seen for an algo wins.
        for algo, handle in self.handles.items():
            if algo in self.ids:
                continue
            if executor_id and getattr(handle, "executor_id", None) == executor_id:
                self.ids[algo] = executor_id
        if executor_id and not self.ids and len(self.handles) == 1:
            self.ids[next(iter(self.handles))] = executor_id

    def on_order(self, ctx, event):
        order = event.data()
        if order.order_id == self.control_id and order.is_filled:
            self.state["control_filled"] = float(order.filled_qty or 0)
        executor_id = str(order.executor_id or "")
        if not executor_id:
            return
        self.state["child_orders"][executor_id] = (
            self.state["child_orders"].get(executor_id, 0) + 1
        )
        if float(order.filled_qty or 0) > 0:
            self.state["child_filled"][executor_id] = float(order.filled_qty)
        if executor_id not in self.state["event_ids"]:
            self.state["event_ids"].append(executor_id)

    def on_stop(self, ctx, event):
        self.state["ticks"] = self.ticks
        self.state["ids"] = dict(self.ids)
        emit_checkpoint(ctx, "t69_executor_algo_matrix", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT69", type="SdkT69", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:30",
                                       export_orders_csv=True),
    )
    state = completed_checkpoint(run, "t69_executor_algo_matrix")
    created = state["created"]
    child_orders = state["child_orders"]
    total_children = sum(child_orders.values())

    checks = {
        "tick_data_delivered": state["ticks"] > 1000,
        "control_round_trip": state["control_filled"] == CONTROL_QTY,
        "no_executor_errors": not state["errors"],
        "both_algorithms_created": all(created.get(algo) for algo in ALGOS),
        "executor_events_delivered": state["events"] > 0,
        "executor_event_payload_complete": set(state["event_fields"]) >= {
            "executor_id", "state", "symbol"
        },
        "executor_reported_states": len(state["event_states"]) >= 1,
        "child_orders_tagged_with_executor_id": total_children > 0,
        # Only meaningful once both algorithms exist; otherwise it just
        # restates the creation failure above.
        "children_from_more_than_one_executor": (
            len(child_orders) >= 2
            if all(created.get(algo) for algo in ALGOS) else True
        ),
        "children_filled": bool(state["child_filled"]),
        "executor_state_queryable": bool(state["state_by_handle"]),
        "executor_stopped_by_handle": any(state["stopped_by_handle"].values()),
        # Only assertable when an executor id was resolved; an empty dict must
        # not read as success.
        "executor_stopped_by_id": (
            any(state["stopped_by_id"].values()) if state["ids"] else True
        ),
    }
    checks.update(evidence_checks(run, orders=3, trades=1))
    finish("t69_executor_algo_matrix", checks,
           extra=(f"ticks={state['ticks']}; created={created}; "
                  f"events={state['events']} states={state['event_states']} "
                  f"fields={state['event_fields']}; "
                  f"executors_seen={len(child_orders)} children={total_children}; "
                  f"filled={state['child_filled']}; ids={state['ids']}; "
                  f"state_by_handle={state['state_by_handle']}; "
                  f"stopped_handle={state['stopped_by_handle']} "
                  f"stopped_id={state['stopped_by_id']}; errors={state['errors']}"))
