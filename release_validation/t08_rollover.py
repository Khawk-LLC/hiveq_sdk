"""The ES.c.0 June 2026 quarterly rollover emits the public on_rollover callback."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig


class SdkT08:
    def __init__(self):
        self.state = {"bars": 0, "contracts": [], "rollovers": []}

    def on_start(self, ctx, event):
        # The data config loads bars_1m, so the subscription has to ask for
        # bars -- the engine does not aggregate fut_trades ticks into bars.
        # Repeated daily subscriptions are deduplicated by the engine.
        ctx.subscribe_futures_bars(symbols=["ES.c.0"], interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        self.state["bars"] += 1
        if bar.symbol not in self.state["contracts"]:
            self.state["contracts"].append(bar.symbol)

    def on_rollover(self, ctx, event):
        rollover = event.data()
        row = {
            "continuous_symbol": rollover.continuous_symbol,
            "prev_contract": rollover.prev_contract,
            "current_contract": rollover.current_contract,
            "payload_ts_event": int(rollover.ts_event),
            "event_ts_event": int(event.ts_event),
        }
        self.state["rollovers"].append(row)
        ctx.add_event_log(
            f"ROLLOVER {row['continuous_symbol']} {row['prev_contract']}->{row['current_contract']}",
            sub_event_type="ROLLOVER",
            state_variable=row,
        )

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t08_rollover", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT08", type="SdkT08", symbols=["ES.c.0"])],
        symbols=["ES.c.0"],
        # ESM6 expires Fri 2026-06-19; the window spans both an early
        # volume-style roll and a calendar roll at expiry.
        start_date="2026-06-08",
        end_date="2026-06-19",
        data_configs=[{
            "type": "hiveq_historical",
            "dataset": "HIVEQ_US_FUT",
            "schema": ["bars_1m"],
            "filter_mode": "continuous",
        }],
        backtest_config=BacktestConfig(
            enable_auto_rollover=True,
            session_start="18:00",
            session_end="17:00",
        ),
    )
    state = completed_checkpoint(run, "t08_rollover")
    rows = state["rollovers"]
    finish("t08_rollover", {
        "on_rollover_fired": len(rows) >= 1,
        "public_payload_complete": bool(rows) and all(
            row["continuous_symbol"] == "ES.c.0"
            and row["prev_contract"]
            and row["current_contract"]
            and row["prev_contract"] != row["current_contract"]
            for row in rows
        ),
        "timestamp_fields_present": bool(rows) and all(
            isinstance(row["event_ts_event"], int)
            and isinstance(row["payload_ts_event"], int)
            for row in rows
        ),
        "expected_quarterly_roll": any(
            row["prev_contract"] == "ESM6" and row["current_contract"] == "ESU6"
            for row in rows
        ),
    }, extra=str(state))
