"""More than one execution algorithm, and the executor event stream itself.

t39 drives a single POV executor. TWAP is never created, ``executor_state`` and
``stop_executor`` (the handle-taking variants) are never called, and
``on_executor`` -- the callback carrying EXECUTOR_EVENT -- is implemented by no
case in the suite, so the whole executor event surface is unobserved.

Each algorithm is created, observed working through its child orders, queried
for state, and stopped; the executor event payload is checked for the fields it
declares. A plain strategy order runs alongside so the case can tell an
executor that produced nothing apart from a run that never traded.
"""
from datetime import timedelta
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

# Futures, not equities: POV and TWAP need a tick stream, and `fut_trades` is
# the schema that delivers one.
SYMBOL = "ES.v.0"
# One executor per symbol: the executor contract is "never stack a second
# executor on the same target", so POV and TWAP get a symbol each instead of
# both working ES.
TWAP_SYMBOL = "NQ.v.0"
SYMBOLS = [SYMBOL, TWAP_SYMBOL]
ALGOS = ("POV", "TWAP")
# Documented executor_state values that mean the executor is no longer working.
TERMINAL_STATES = {"FILLED", "STOPPED", "STOPPING", "UNDEFINED", "INVALID"}
CONTROL_QTY = 2.0
TWAP_MINUTES = 20
# Contracts, not shares: an equity-sized quantity is not a fillable futures order.
EXECUTOR_QTY = 20


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
                "twap_attempts": {}, "twap_fallback": False,
                "stop_attempted": {"handle": False, "id": False},
                "state_at_stop": {}, "state_timeline": {},
            }
        ctx.subscribe_trades(SYMBOLS, asset_type=AssetType.FUTURES)

    def _pov_params(self, ctx, symbol):
        return ctx.build_executor_params(
            symbol=symbol, quantity=EXECUTOR_QTY, side="BUY", executor_type="POV",
            participate_pct=0.1, min_order_size=1, max_order_size=10,
            refresh_millis=100,
        )

    def _twap_param_forms(self, ctx, symbol):
        """Every documented way to hand TWAP its `[start_time, end_time]`.

        §5.10 lists `start_time`/`end_time` as TWAP's key params but never says
        what type they are, so each plausible form is tried and the engine's
        answer recorded.  Without this the case only reported "TWAP was not
        created" and could not say whether the algorithm or the parameter
        contract was at fault.
        """
        now = ctx.now()
        forms = []
        if now is not None:
            end = now + timedelta(minutes=TWAP_MINUTES)
            forms.append(("datetime", {"start_time": now, "end_time": end}))
            forms.append(("string", {
                "start_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
            }))
            try:
                import PySigma

                forms.append(("pysigma_datetime", {
                    "start_time": PySigma.Chrono.dateTimeFromString(
                        now.strftime("%Y-%m-%d %H:%M:%S")),
                    "end_time": PySigma.Chrono.dateTimeFromString(
                        end.strftime("%Y-%m-%d %H:%M:%S")),
                }))
            except Exception as exc:                       # noqa: BLE001
                self.state["twap_attempts"]["pysigma_import"] = str(exc)[:100]
        forms.append(("no_window", {}))
        return forms

    def create(self, ctx, algo):
        if algo == "POV":
            try:
                handle = ctx.add_executor(self._pov_params(ctx, SYMBOL))
            except Exception as exc:                       # noqa: BLE001
                self.state["errors"].append(f"POV create: {str(exc)[:140]}")
                return
            self.state["created"]["POV"] = handle is not None
            if handle is not None:
                self.handles["POV"] = handle
            return

        for label, window in self._twap_param_forms(ctx, TWAP_SYMBOL):
            try:
                params = ctx.build_executor_params(
                    symbol=TWAP_SYMBOL, quantity=EXECUTOR_QTY, side="BUY",
                    executor_type="TWAP", min_order_size=1, max_order_size=10,
                    refresh_millis=100, **window,
                )
                handle = ctx.add_executor(params)
            except Exception as exc:                       # noqa: BLE001
                self.state["twap_attempts"][label] = f"raised: {str(exc)[:120]}"
                continue
            self.state["twap_attempts"][label] = (
                "created" if handle is not None else "add_executor returned None"
            )
            if handle is not None:
                self.state["created"]["TWAP"] = True
                self.handles["TWAP"] = handle
                return
        self.state["created"]["TWAP"] = False
        # A second executor regardless, so the two-executor lifecycle -- child
        # attribution, stop by handle and stop by id -- is still exercised and
        # the TWAP result stays a single, isolated finding.
        try:
            handle = ctx.add_executor(self._pov_params(ctx, TWAP_SYMBOL))
        except Exception as exc:                           # noqa: BLE001
            self.state["errors"].append(f"POV fallback create: {str(exc)[:140]}")
            return
        if handle is not None:
            self.state["twap_fallback"] = True
            self.handles["POV_FALLBACK"] = handle

    def on_trade(self, ctx, event):
        self.ticks += 1

        if self.ticks == 20 and not self.control_id:
            order = ctx.buy_order(SYMBOL, CONTROL_QTY)
            if order is not None:
                self.control_id = order.order_id
        elif self.ticks == 60:
            for algo in ALGOS:
                self.create(ctx, algo)
        elif self.ticks in (500, 1500, 4500):
            # A state sample per executor over its working life: a lifecycle
            # that ends early is then visible in the result instead of being
            # inferred from a single reading.
            for algo, handle in self.handles.items():
                try:
                    reading = str(ctx.executor_state(handle))
                except Exception as exc:                   # noqa: BLE001
                    reading = f"raised: {str(exc)[:60]}"
                self.state["state_timeline"].setdefault(algo, []).append(
                    [self.ticks, reading]
                )
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
            # One stopped by handle, one by id.  The id path is attempted on
            # every executor whose id was resolved except the one already
            # stopped by handle, and whether it was attempted at all is
            # recorded -- an empty result used to read as success.
            for index, (algo, handle) in enumerate(self.handles.items()):
                if index == 0:
                    self.state["stop_attempted"]["handle"] = True
                    try:
                        self.state["stopped_by_handle"][algo] = bool(
                            ctx.stop_executor(handle)
                        )
                    except Exception as exc:               # noqa: BLE001
                        self.state["errors"].append(f"{algo} stop: {str(exc)[:140]}")
                    continue
                executor_id = self.ids.get(algo, "") or str(
                    getattr(handle, "executor_id", "")
                    or getattr(handle, "executorID", "") or ""
                )
                if executor_id:
                    self.state["stop_attempted"]["id"] = True
                    # The state at the moment of the stop: an executor that has
                    # already reached a terminal state cannot be stopped, so
                    # False there is correct and must not read as a defect.
                    try:
                        self.state["state_at_stop"][algo] = str(
                            ctx.executor_state(handle)
                        )
                    except Exception as exc:               # noqa: BLE001
                        self.state["state_at_stop"][algo] = f"raised: {str(exc)[:60]}"
                    self.state["stopped_by_id"][algo] = bool(
                        ctx.stop_executor_by_id(executor_id)
                    )

    def on_executor(self, ctx, event):
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
            handle_id = str(getattr(handle, "executor_id", "")
                            or getattr(handle, "executorID", "") or "")
            if executor_id and handle_id == executor_id:
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
        strategy_configs=[StrategyConfig(name="SdkT69", type="SdkT69", symbols=SYMBOLS)],
        symbols=SYMBOLS, start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
            "schema": ["fut_trades"],
        }],
        backtest_config=BacktestConfig(
            initial_capital=100_000_000.0,
            session_start="09:30", session_end="10:30",
        ),
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
        "children_from_more_than_one_executor": len(child_orders) >= 2,
        "children_filled": bool(state["child_filled"]),
        "executor_state_queryable": bool(state["state_by_handle"]),
        "executor_stopped_by_handle": (
            state["stop_attempted"]["handle"]
            and all(state["stopped_by_handle"].values())
            and bool(state["stopped_by_handle"])
        ),
        # Attempted on the second executor; the flag is what makes the empty
        # case a failure to exercise the path rather than a silent pass. An
        # executor already in a terminal state is not stoppable, so False is
        # the correct answer there -- the API contract is that a live executor
        # stops and a dead one reports that it did not.
        "executor_stopped_by_id": (
            state["stop_attempted"]["id"]
            and bool(state["stopped_by_id"])
            and all(
                stopped or state["state_at_stop"].get(algo, "") in TERMINAL_STATES
                for algo, stopped in state["stopped_by_id"].items()
            )
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
                  f"stopped_id={state['stopped_by_id']} "
                  f"stop_attempted={state['stop_attempted']} "
                  f"state_at_stop={state['state_at_stop']}; "
                  f"state_timeline={state['state_timeline']}; "
                  f"twap_attempts={state['twap_attempts']} "
                  f"twap_fallback={state['twap_fallback']}; "
                  f"errors={state['errors']}"))
