"""HIVEQ_QUANT_FEATURES_V3 delivers typed feature columns to on_custom_data; non-default fields are requested via `filters.fields`."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

FEATURES_ID = "FeaturesTest"
FEATURES_SYMBOL = "0930_NBV_Mom_Long_ES_V0"

SIGNAL_COLUMNS = ("signal1", "signal2", "signal3")
WEIGHT_COLUMNS = ("weight1", "weight2", "weight3")


def _is_valid(value) -> bool:
    """Column values arrive as strings; treat missing/empty/NaN sentinels as invalid."""
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    return text.lower() not in {"nan", "none", "null"}


class SdkT48:
    def on_start(self, ctx, event):
        self.state = {"bars": 0, "rows": 0, "nonempty_symbol": 0, "nonempty_time": 0,
                      "with_any_signal": 0, "with_any_weight": 0,
                      "with_all_signals": 0, "with_all_weights": 0,
                      "projection_missing_weights": 0, "samples": []}
        ctx.subscribe_bars(ctx.strategy_config.symbols, asset_type=AssetType.FUTURES, interval="1m")
        ctx.subscribe_data(FEATURES_ID)

    def on_bar(self, ctx, event):
        self.state["bars"] += 1

    def on_custom_data(self, ctx, event):
        row = event.data()
        self.state["rows"] += 1
        # The delivered row exposes the identifier column as `sym`.
        symbol = row.column_data("sym", default="")
        timestamp = row.column_data("time", default="")
        if symbol:
            self.state["nonempty_symbol"] += 1
        if timestamp:
            self.state["nonempty_time"] += 1

        try:
            header = str(getattr(row, "header", "") or "")
        except Exception:
            header = ""
        header_columns = {name.strip() for name in header.split(",") if name.strip()}
        if not header_columns.intersection(WEIGHT_COLUMNS):
            self.state["projection_missing_weights"] += 1

        signals = {name: row.column_data(name, default="") for name in SIGNAL_COLUMNS}
        weights = {name: row.column_data(name, default="") for name in WEIGHT_COLUMNS}
        signal_valid = [_is_valid(value) for value in signals.values()]
        weight_valid = [_is_valid(value) for value in weights.values()]
        if any(signal_valid):
            self.state["with_any_signal"] += 1
        if all(signal_valid):
            self.state["with_all_signals"] += 1
        if any(weight_valid):
            self.state["with_any_weight"] += 1
        if all(weight_valid):
            self.state["with_all_weights"] += 1

        if any(signal_valid):
            payload = {"symbol": str(symbol), "time": str(timestamp)}
            payload.update({name: str(value) for name, value in signals.items()})
            payload.update({name: str(value) for name, value in weights.items()})
            ctx.add_event_log(
                f"FEATURES_V3_SIGNAL {payload['symbol']} {payload['time']}",
                sub_event_type="FEATURES_V3_SIGNAL",
                state_variable=payload,
            )

        if len(self.state["samples"]) < 5:
            try:
                full_data = dict(row.data) if row.data else {}
            except Exception as exc:
                full_data = {"__error__": f"{type(exc).__name__}: {exc}"}
            self.state["samples"].append({
                "row_symbol": str(getattr(row, "symbol", "")),
                "column_sym": str(symbol),
                "time": str(timestamp),
                "header": header,
                "raw_row": str(getattr(row, "row", "")),
                "data_keys": sorted(full_data.keys()),
                "data": full_data,
            })

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t48_features_v3_data_example", self.state)


if __name__ == "__main__":
    # HIVEQ_QUANT_FEATURES_V3 returns a default set of fields per row
    # (`date,time,symbol,ticker,root,score,price,volume,freq,signal1`).
    # Additional columns like `signal2/3`, `weight1..3`, `stop_px`,
    # `target_px` are available but must be requested explicitly via
    # `filters.fields` on the data_config.
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT48", type="SdkT48", symbols=["ES.c.0"])],
        symbols=["ES.c.0"],
        start_date="2026-06-30",
        end_date="2026-06-30",
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT", "schema": ["bars_1m"]},
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_QUANT_FEATURES_V3",
                "schema": ["quant_features_v3"],
                "id": FEATURES_ID,
                "symbols": [FEATURES_SYMBOL],
                # Request non-default columns (weights, extra signals, stop/target).
                "filters": {
                    "fields": "date,time,symbol,signal1,signal2,signal3,weight1,weight2,weight3,stop_px,target_px",
                },
            },
        ],
    )
    state = completed_checkpoint(run, "t48_features_v3_data_example")
    # Every derivative check is guarded on rows>0 so an empty run fails every
    # gate instead of vacuously passing the equality traps (nonempty==rows
    # and projection_missing_weights==0 both hold at 0==0).
    finish("t48_features_v3_data_example", {
        "timeline_bars_present": state["bars"] > 0,
        "feature_callbacks_present": state["rows"] > 0,
        "symbols_present": state["rows"] > 0 and state["nonempty_symbol"] == state["rows"],
        "timestamps_present": state["rows"] > 0 and state["nonempty_time"] == state["rows"],
        "signals_present": state["with_any_signal"] > 0,
        "weights_present": state["with_any_weight"] > 0,
        "weight_columns_projected": state["rows"] > 0 and state["projection_missing_weights"] == 0,
        "payload_samples_persisted": bool(state["samples"]),
    }, extra=str(state))
