"""A tick-driven POV executor can be created, queried, retargeted, stopped, and observed."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


class SdkT39:
    def on_start(self, ctx, event):
        self.ticks = 0
        self.executor = None
        self.executor_id = ""
        self.state = {
            "trade_ticks": 0,
            "created": False,
            "initial_state": "",
            "params_read": False,
            "replace_result": None,
            "stop_result": None,
            "executor_events": 0,
            "child_order_events": 0,
            "child_filled_qty": 0.0,
            "states": [],
        }
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)

    def on_trade(self, ctx, event):
        self.ticks += 1
        self.state["trade_ticks"] = self.ticks
        if self.executor is None:
            params = ctx.build_executor_params(
                symbol="AAPL",
                quantity=10_000,
                side="BUY",
                executor_type="POV",
                participate_pct=0.1,
                min_order_size=1,
                max_order_size=10,
                refresh_millis=100,
            )
            self.executor = ctx.add_executor(params)
            self.state["created"] = self.executor is not None
            if self.executor is not None:
                self.executor_id = str(self.executor.executorID)
                self.state["initial_state"] = ctx.executor_state(self.executor)
            return

        if self.ticks == 2:
            current = ctx.get_executor_params_by_id(self.executor_id)
            self.state["params_read"] = current is not None
            replacement = ctx.build_executor_params(
                symbol="AAPL",
                quantity=10_000,
                side="BUY",
                executor_type="POV",
                participate_pct=1.0,
                min_order_size=1,
                max_order_size=25,
                refresh_millis=100,
            )
            self.state["replace_result"] = bool(
                ctx.replace_executor_params_by_id(self.executor_id, replacement)
            )
        elif self.ticks == 200 and self.state["stop_result"] is None:
            self.state["stop_result"] = bool(ctx.stop_executor_by_id(self.executor_id))

    def on_executor(self, ctx, event):
        self.state["executor_events"] += 1
        # The callback payload is authoritative. Do not dereference the raw
        # executor after STOPPED: the registry removes it before dispatch.
        data = event.data()
        state = str(getattr(data, "state", ""))
        if state and state not in self.state["states"]:
            self.state["states"].append(state)

    def on_order(self, ctx, event):
        order = event.data()
        if self.executor_id and str(order.executor_id) == self.executor_id:
            self.state["child_order_events"] += 1
            self.state["child_filled_qty"] = max(
                self.state["child_filled_qty"], float(order.filled_qty)
            )

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t39_executor_lifecycle", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT39", type="SdkT39", symbols=["AAPL"])],
        symbols=["AAPL"],
        start_date="2025-12-02",
        end_date="2025-12-02",
        data_configs=[{
            "type": "hiveq_historical",
            "dataset": "HIVEQ_US_EQ",
            "schema": ["eq_trades"],
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="16:00"),
    )
    state = completed_checkpoint(run, "t39_executor_lifecycle")
    states = set(state["states"])
    finish("t39_executor_lifecycle", {
        "tick_stream_present": state["trade_ticks"] >= 200,
        "executor_created": state["created"] and bool(state["initial_state"]),
        "params_query_worked": state["params_read"],
        "retarget_worked": state["replace_result"] is True,
        "stop_worked": state["stop_result"] is True,
        "lifecycle_callback_seen": state["executor_events"] > 0,
        "child_orders_linked": state["child_order_events"] > 0,
        "stopped_callback_seen": "STOPPED" in states,
        "no_invalid_lifecycle_state": "INVALID" not in states,
        "lifecycle_state_observed": bool(states & {
            "STARTED", "NEW", "PARTIALLY_FILLED", "FILLED", "STOPPING", "STOPPED"
        }),
    }, extra=str(state))
