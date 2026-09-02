"""Long-run ES continuous-futures rollover reconciliation.

Runs a one-contract buy-and-hold and exports orders, trades, positions,
event_logs, and a machine-readable validation summary under run_artifacts/RUN_ID.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import event_logs_until, finish_validation, open_positions as open_positions_rows, export_run_artifacts, wait_for_final

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()

EXPECTED_ROLLOVERS = 40
EXPECTED_FINAL_CONTRACT = "ESH6"
MIN_ROLLOVER_SPACING = pd.Timedelta(days=20)


class SdkT49LongRolloverBuyHold:
    def __init__(self):
        self.bought = False
        self.rollovers = 0

    def on_start(self, ctx, event):
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                           asset_type=AssetType.FUTURES, interval="1m")
        logger.info(f"[START] symbols={ctx.strategy_config.symbols} bought={self.bought}")

    def on_bar(self, ctx, event):
        bar = event.data()
        logger.debug(f"[BAR] {bar.time} {bar.symbol} close={bar.close}")
        if not self.bought:
            ctx.buy_order(bar.symbol, quantity=1.0)
            self.bought = True
            logger.info(f"[ENTRY] buy 1 {bar.symbol}")

    def on_rollover(self, ctx, event):
        data = event.data()
        self.rollovers += 1
        ctx.add_event_log(
            f"ROLLOVER_DONE {data.prev_contract}->{data.current_contract}",
            sub_event_type="ROLLOVER_DONE",
            state_variable={
                "sequence": self.rollovers,
                "continuous_symbol": data.continuous_symbol,
                "prev_contract": data.prev_contract,
                "current_contract": data.current_contract,
                "payload_ts_event": int(data.ts_event),
                "event_ts_event": int(event.ts_event),
            },
        )

    def on_order(self, ctx, event):
        order = event.data()
        logger.info(f"[ORDER] {order.symbol} {order.status} filled={order.filled_qty}")


def analyze(run, continuous_symbol: str) -> dict:
    orders = run.orders()
    positions = run.positions()
    events = event_logs_until(run, "ROLLOVER_DONE")
    rolls = events[events["sub_event_type"] == "ROLLOVER_DONE"].copy()
    rolls["ts_event"] = pd.to_datetime(rolls["ts_event"])
    rolls = rolls.sort_values("ts_event")
    def decode_state(value):
        if value is None:
            return {}
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        if isinstance(value, str):
            return json.loads(value or "{}")
        return value

    roll_states = [decode_state(value) for value in rolls["state_variables"]]
    roll_times = list(rolls["ts_event"])
    spacings = [
        roll_times[index] - roll_times[index - 1]
        for index in range(1, len(roll_times))
    ]
    chain_continuous = all(
        roll_states[index - 1].get("current_contract")
        == roll_states[index].get("prev_contract")
        for index in range(1, len(roll_states))
    )
    final_roll_contract = (
        roll_states[-1].get("current_contract") if roll_states else None
    )
    rollover_count_exact = len(rolls) == EXPECTED_ROLLOVERS
    rollover_spacing_valid = all(
        spacing >= MIN_ROLLOVER_SPACING for spacing in spacings
    )
    order_columns = {str(column).lower(): column for column in orders.columns}
    status_column = order_columns.get("status")
    nonfilled = (
        orders[~orders[status_column].astype(str).str.upper().str.endswith("FILLED")]
        if status_column is not None else orders
    )
    # positions() is a historical snapshot table (one row per contract), not
    # a current portfolio view. Derive the final live position from the filled
    # order ledger instead, which gives one authoritative net quantity per
    # contract after all rollover sells/buys.
    symbol_column = order_columns.get("symbol")
    side_column = order_columns.get("side")
    qty_column = order_columns.get("fillqty") or order_columns.get("filled_qty")
    if status_column is not None and symbol_column is not None:
        filled = orders[orders[status_column].astype(str).str.upper().str.endswith("FILLED")].copy()
        if qty_column is not None and side_column is not None:
            signed_qty = filled[qty_column].astype(float) * filled[side_column].astype(str).str.upper().map({"BUY": 1.0, "SELL": -1.0}).fillna(0.0)
            net = signed_qty.groupby(filled[symbol_column]).sum()
            open_positions = pd.DataFrame({"symbol": net.index, "quantity": net.values})
            open_positions = open_positions_rows(open_positions)
        else:
            open_positions = positions.iloc[0:0]
    else:
        open_positions = positions.iloc[0:0]
    stale_positions = open_positions.iloc[:-1] if len(open_positions) > 1 else open_positions.iloc[0:0]
    return {
        "continuous_symbol": continuous_symbol,
        "orders": len(orders),
        "all_orders_filled": nonfilled.empty,
        "rollover_done_events": len(rolls),
        "expected_rollover_done_events": EXPECTED_ROLLOVERS,
        "rollover_count_exact": rollover_count_exact,
        "rollover_timestamps_strictly_increase": all(
            spacing > pd.Timedelta(0) for spacing in spacings
        ),
        "rollover_spacing_at_least_20_days": rollover_spacing_valid,
        "rollover_chain_continuous": chain_continuous,
        "expected_final_contract": EXPECTED_FINAL_CONTRACT,
        "final_roll_contract": final_roll_contract,
        "final_contract_correct": final_roll_contract == EXPECTED_FINAL_CONTRACT,
        "open_positions": len(open_positions),
        "stale_open_positions": len(stale_positions),
        "final_open_symbol": None if open_positions.empty else open_positions.iloc[-1]["symbol"],
        "passed": bool(
            nonfilled.empty
            and len(open_positions) == 1
            and stale_positions.empty
            and rollover_count_exact
            and rollover_spacing_valid
            and chain_continuous
            and final_roll_contract == EXPECTED_FINAL_CONTRACT
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="ES.v.0")
    parser.add_argument("--start", default="2016-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--timeout", type=float, default=14400.0)
    args = parser.parse_args()

    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT49LongRolloverBuyHold", type="SdkT49LongRolloverBuyHold",
            symbols=[args.symbol],
        )],
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
            "schema": ["bars_1m"],
        }],
        backtest_config=BacktestConfig(
            symbols=[args.symbol], start_date=args.start, end_date=args.end,
            initial_capital=500_000.0, session_start="18:00", session_end="17:00",
            enable_auto_rollover=True, auto_flatten_at_close=False
        ),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}", flush=True)
    wait_for_final(run, timeout=args.timeout)
    validation = analyze(run, args.symbol)
    artifacts = export_run_artifacts(run, validation=validation)
    print(json.dumps(validation, indent=2), flush=True)
    print(f"run_artifacts={artifacts}", flush=True)
    finish_validation("t49_long_rollover_buy_hold", validation)


if __name__ == "__main__":
    main()
