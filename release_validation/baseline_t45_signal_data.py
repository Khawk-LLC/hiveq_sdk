"""HIVEQ_QUANT_SIGNALS deliver signal_json rows to on_custom_data via the simple data_config shape."""
import json
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

SIGNAL_ID = "SignalTest"
SIGNAL_SYMBOL = "Spy-ClusterDecay-5m-v2"


class SdkT45:
    def on_start(self, ctx, event):
        self.state = {"bars": 0, "signals": 0, "parse_errors": 0, "with_json_fields": 0,
                      "nonempty_sym": 0, "samples": []}
        ctx.subscribe_bars(ctx.strategy_config.symbols, asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_data(SIGNAL_ID)

    def on_bar(self, ctx, event):
        self.state["bars"] += 1

    def on_custom_data(self, ctx, event):
        data = event.data()
        self.state["signals"] += 1
        symbol = data.column_data("sym", default="")
        status = data.column_data("signal_status", default="")
        raw = data.column_data("signal_json", default=None)
        parsed = None
        if raw:
            try:
                # C++ adapter pipe-encodes commas and C-escapes quotes.
                decoded = raw.replace("|", ",").replace('\\"', '"').replace("'", '"')
                parsed = json.loads(decoded)
            except (json.JSONDecodeError, ValueError):
                self.state["parse_errors"] += 1
        if symbol:
            self.state["nonempty_sym"] += 1
        if isinstance(parsed, dict) and parsed:
            self.state["with_json_fields"] += 1
        if len(self.state["samples"]) < 5:
            self.state["samples"].append({
                "sym": str(symbol),
                "status": str(status),
                "json_keys": sorted(parsed.keys()) if isinstance(parsed, dict) else None,
            })

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t45_signal_data_example", self.state)


if __name__ == "__main__":
    # HIVEQ_QUANT_SIGNALS uses the simple shape (schema+symbols): its wire filter
    # column is `symbol`, which matches the default `symbolFilterKey` — no
    # explicit filters block needed. Signal payload fields (`ticker`, `flag`,
    # `state`, …) vary by source, so this case only asserts the transport surface
    # — signals delivered, `sym` populated, `signal_json` parses to a dict — and
    # leaves source-specific field extraction to strategy code.
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT45", type="SdkT45", symbols=["AAPL"])],
        symbols=["AAPL"],
        start_date="2024-08-27",
        end_date="2024-08-27",
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]},
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_QUANT_SIGNALS",
                "schema": ["signals"],
                "id": SIGNAL_ID,
                "symbols": [SIGNAL_SYMBOL],
            },
        ],
    )
    state = completed_checkpoint(run, "t45_signal_data_example")
    # Every derivative check is guarded on signals>0 so an empty run fails
    # every gate instead of vacuously passing the equality/absence traps
    # (parse_errors==0 with nothing to parse, nonempty_sym==signals at 0==0).
    finish("t45_signal_data_example", {
        "timeline_bars_present": state["bars"] > 0,
        "signal_callbacks_present": state["signals"] > 0,
        "signal_json_parses": state["signals"] > 0 and state["parse_errors"] == 0,
        "signal_json_has_fields": state["with_json_fields"] > 0,
        "symbols_present": state["signals"] > 0 and state["nonempty_sym"] == state["signals"],
        "payload_samples_persisted": bool(state["samples"]),
    }, extra=str(state))
