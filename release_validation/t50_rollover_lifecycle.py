"""Validate ES.v.0 rollover lifecycle payloads over several years."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import finish_validation, open_positions as open_position_rows, export_run_artifacts, wait_for_final

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()
SYMBOL = "ES.v.0"
EXPECTED_ROLLOVERS = 12
EXPECTED_FINAL_CONTRACT = "ESH6"
MIN_ROLLOVER_SPACING = pd.Timedelta(days=20)


class SdkT50RolloverLifecycleProbe:
    def __init__(self):
        self.bought = False
        self.sequence = 0

    def on_start(self, ctx, event):
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.FUTURES, interval="1m")
        logger.info(f"[START] subscribed {SYMBOL} bought={self.bought}")

    def on_bar(self, ctx, event):
        bar = event.data()
        logger.debug(f"[BAR] {bar.time} {bar.symbol} close={bar.close}")
        if not self.bought:
            ctx.buy_order(bar.symbol, quantity=1.0)
            self.bought = True
            logger.info(f"[ENTRY] buy 1 {bar.symbol}")

    def on_security_event(self, ctx, event):
        data = event.data()
        phase = str(data.event_type)
        if phase.startswith("ROLLOVER_"):
            ctx.add_event_log(
                f"{phase} symbol={data.symbol} rollover_symbol={data.rollover_symbol}",
                sub_event_type=phase,
                state_variable={
                    "phase": phase, "symbol": data.symbol,
                    "rollover_symbol": data.rollover_symbol,
                    "payload_ts_event": int(data.ts_event),
                    "event_ts_event": int(event.ts_event),
                },
            )

    def on_rollover(self, ctx, event):
        data = event.data()
        self.sequence += 1
        ctx.add_event_log(
            f"ROLLOVER_DONE {data.prev_contract}->{data.current_contract}",
            sub_event_type="ROLLOVER_DONE",
            state_variable={
                "sequence": self.sequence,
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


def _state(value):
    # An in-process run serializes state_variables with orjson, so the column
    # holds bytes rather than str. Falling through to {} here made every
    # rollover payload look empty: final_roll_contract came back None and
    # done_payload_pairs_complete False, while the payloads were in fact
    # complete. qa_common.checkpoint() decodes the same way.
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value or "{}")
    return value if isinstance(value, dict) else {}


def analyze(run) -> dict:
    events = run.event_logs().sort_values("ts_event")
    lifecycle = events[events["sub_event_type"].astype(str).str.startswith("ROLLOVER_")]
    done = lifecycle[lifecycle["sub_event_type"] == "ROLLOVER_DONE"]
    due = lifecycle[lifecycle["sub_event_type"] == "ROLLOVER_DUE"]
    complete = lifecycle[lifecycle["sub_event_type"] == "ROLLOVER_COMPLETE"]
    done_states = [_state(value) for value in done["state_variables"]]
    done_times = list(pd.to_datetime(done["ts_event"]))
    spacings = [
        done_times[index] - done_times[index - 1]
        for index in range(1, len(done_times))
    ]
    complete_pairs = all(
        state.get("continuous_symbol") == SYMBOL
        and state.get("prev_contract")
        and state.get("current_contract")
        and state.get("prev_contract") != state.get("current_contract")
        for state in done_states
    )
    continuity = all(
        done_states[index - 1].get("current_contract") == state.get("prev_contract")
        for index, state in enumerate(done_states[1:], start=1)
    )
    positions = run.positions()
    open_positions = open_position_rows(positions)
    result = {
        "symbol": SYMBOL,
        # Counted for visibility only. Rollovers are delivered by on_rollover;
        # on_security_event carries no rollover phases, so these stay at zero by
        # design and must not gate the result -- a direct probe saw 0 security
        # events across 13,786 bars containing a real roll while on_rollover
        # fired correctly.
        "rollover_due_events": len(due),
        "rollover_complete_events": len(complete),
        "rollover_done_events": len(done),
        "expected_rollover_done_events": EXPECTED_ROLLOVERS,
        "rollover_count_exact": len(done) == EXPECTED_ROLLOVERS,
        "rollover_timestamps_strictly_increase": all(
            spacing > pd.Timedelta(0) for spacing in spacings
        ),
        "rollover_spacing_at_least_20_days": all(
            spacing >= MIN_ROLLOVER_SPACING for spacing in spacings
        ),
        "expected_final_contract": EXPECTED_FINAL_CONTRACT,
        "final_roll_contract": (
            done_states[-1].get("current_contract") if done_states else None
        ),
        "done_payload_pairs_complete": complete_pairs,
        "done_contract_chain_continuous": continuity,
        "open_positions": len(open_positions),
        "stale_open_positions": max(0, len(open_positions) - 1),
    }
    result["passed"] = bool(
        len(done) == EXPECTED_ROLLOVERS and complete_pairs and continuity
        and all(spacing >= MIN_ROLLOVER_SPACING for spacing in spacings)
        and bool(done_states)
        and done_states[-1].get("current_contract") == EXPECTED_FINAL_CONTRACT
        and len(open_positions) == 1
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--timeout", type=float, default=7200.0)
    args = parser.parse_args()
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT50RolloverLifecycleProbe", type="SdkT50RolloverLifecycleProbe",
            symbols=[SYMBOL],
        )],
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
            "schema": ["bars_1m"],
        }],
        backtest_config=BacktestConfig(
            symbols=[SYMBOL], start_date=args.start, end_date=args.end,
            initial_capital=500_000.0, session_start="18:00", session_end="17:00",
            enable_auto_rollover=True, auto_flatten_at_close=False,
        ),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}", flush=True)
    wait_for_final(run, timeout=args.timeout)
    validation = analyze(run)
    artifacts = export_run_artifacts(run, validation=validation)
    print(json.dumps(validation, indent=2), flush=True)
    print(f"run_artifacts={artifacts}", flush=True)
    finish_validation("t50_rollover_lifecycle", validation)


if __name__ == "__main__":
    main()
