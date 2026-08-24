"""Hold five ES.v.0 contracts across a CME overnight session boundary."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import finish_validation, open_positions as open_position_rows, export_run_artifacts, wait_for_final

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

SYMBOL = "ES.v.0"
QUANTITY = 5.0


class SdkT53OvernightFuturesStrategy:
    def __init__(self):
        self.entry_sent = False
        self.exit_sent = False
        self.boundary_logged = False
        self.midnight_logged = False

    def on_start(self, ctx, event):
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.FUTURES, interval="1m")

    def _snapshot(self, ctx, bar, phase):
        quantity = float(ctx.quantity(bar.symbol))
        ctx.add_event_log(
            f"{phase} {bar.symbol} quantity={quantity}", sub_event_type=phase,
            state_variable={"continuous_symbol": SYMBOL, "contract": bar.symbol,
                            "quantity": quantity, "time": bar.time.isoformat()},
        )

    def on_bar(self, ctx, event):
        bar = event.data()
        now = ctx.now()
        if (now.date().isoformat() == "2025-09-15"
                and (16, 0) <= (now.hour, now.minute) < (17, 0)):
            if not self.entry_sent:
                ctx.buy_order(bar.symbol, quantity=QUANTITY)
                self.entry_sent = True
            return
        if (self.entry_sent and not self.boundary_logged
                and now.date().isoformat() == "2025-09-15" and now.hour >= 18):
            self._snapshot(ctx, bar, "OVERNIGHT_BOUNDARY_POSITION")
            self.boundary_logged = True
        if (self.entry_sent and not self.midnight_logged
                and now.date().isoformat() == "2025-09-16" and now.hour < 1):
            self._snapshot(ctx, bar, "AFTER_MIDNIGHT_POSITION")
            self.midnight_logged = True
        if (now.date().isoformat() == "2025-09-16" and (now.hour, now.minute) >= (10, 0)
                and not self.exit_sent):
            self._snapshot(ctx, bar, "PRE_EXIT_POSITION")
            ctx.close_position(bar.symbol)
            self.exit_sent = True

    def on_order(self, ctx, event):
        order = event.data()
        if order.is_filled:
            ctx.add_event_log(
                f"FILLED {order.symbol} quantity={order.filled_qty}",
                sub_event_type="OVERNIGHT_FILL", symbol=order.symbol,
                state_variable={"contract": order.symbol,
                                "filled_qty": float(order.filled_qty)},
            )


def _state(value):
    # In-process runs serialize state_variables with orjson, so this column
    # holds bytes; without decoding, every payload silently reads as {}.
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value or "{}")
    return value if isinstance(value, dict) else {}


def analyze(run) -> dict:
    orders = run.orders()
    positions = run.positions()
    events = run.event_logs().sort_values("ts_event")
    snapshots = {}
    for phase in ("OVERNIGHT_BOUNDARY_POSITION", "AFTER_MIDNIGHT_POSITION",
                  "PRE_EXIT_POSITION"):
        rows = events[events["sub_event_type"] == phase]
        snapshots[phase] = None if rows.empty else _state(rows.iloc[-1]["state_variables"])
    held_at_every_checkpoint = all(
        state is not None and float(state.get("quantity", 0)) == QUANTITY
        for state in snapshots.values()
    )
    open_positions = open_position_rows(positions)
    all_filled = len(orders) == 2 and bool((orders["status"] == "FILLED").all())
    result = {
        "symbol": SYMBOL, "expected_quantity": QUANTITY, "orders": len(orders),
        "all_orders_filled": all_filled, "position_snapshots": snapshots,
        "held_at_boundary_midnight_and_pre_exit": held_at_every_checkpoint,
        "open_positions_after_exit": len(open_positions),
    }
    result["passed"] = bool(all_filled and held_at_every_checkpoint
                            and len(open_positions) == 0)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2025-09-15")
    parser.add_argument("--end", default="2025-09-16")
    parser.add_argument("--timeout", type=float, default=7200.0)
    args = parser.parse_args()
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT53OvernightFuturesStrategy",
            type="SdkT53OvernightFuturesStrategy", symbols=[SYMBOL],
        )],
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(
            symbols=[SYMBOL], start_date=args.start, end_date=args.end,
            initial_capital=22_500_000.0, session_start="18:00", session_end="17:00",
            enable_auto_rollover=True, auto_flatten_at_close=False,
        ),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}", flush=True)
    wait_for_final(run, timeout=args.timeout)
    validation = analyze(run)
    artifacts = export_run_artifacts(run, validation=validation)
    print(json.dumps(validation, indent=2), flush=True)
    print(f"run_artifacts={artifacts}", flush=True)
    finish_validation("t53_overnight_futures_strategy", validation)


if __name__ == "__main__":
    main()
