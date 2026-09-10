#!/usr/bin/env python3
"""Quant features (v3) — subscribe to a HIVEQ_QUANT_FEATURES_V3 feed and read its columns.

Standalone example (no test-harness dependencies). Demonstrates:
  - a HIVEQ_QUANT_FEATURES_V3 data source declared in ``data_configs`` with an
    ``id``, wired to the strategy via ``ctx.subscribe_data(data_id=...)``.
  - HIVEQ_QUANT_FEATURES_V3 returns a default set of fields per row
    (``date,time,symbol,ticker,root,score,price,volume,freq,signal1``).
    Additional columns like ``signal2/3``, ``weight1..3``, ``stop_px``,
    ``target_px`` must be requested explicitly via ``filters.fields``.
  - the delivered row exposes the identifier column as ``sym``; read it
    with ``data.column_data("sym")`` (or ``data.symbol``).
  - every valid signal (any of ``signal1..3`` populated) is written to the
    run's event log as ``sub_event_type='FEATURES_V3_SIGNAL'`` so a reviewer
    can confirm data delivery via ``run.event_logs()``.

Run:
    python quant_features_v3.py

Exits 0 on a clean run (data delivered on all gates), 1 otherwise.
"""
from __future__ import annotations

import sys

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

FEATURES_ID = "FeaturesTest"
FEATURES_SYMBOL = "0930_NBV_Mom_Long_ES_V0"
BAR_SYMBOL = "ES.c.0"
TRADE_DATE = "2026-06-30"

SIGNAL_COLUMNS = ("signal1", "signal2", "signal3")
WEIGHT_COLUMNS = ("weight1", "weight2", "weight3")
EXTRA_FIELDS = "date,time,symbol,signal1,signal2,signal3,weight1,weight2,weight3,stop_px,target_px"


def _is_valid(value) -> bool:
    """Column values arrive as strings; treat empty/NaN sentinels as invalid."""
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    return text.lower() not in {"nan", "none", "null"}


class QuantFeaturesV3:
    def on_start(self, ctx, event):
        self.state = {
            "bars": 0, "rows": 0,
            "nonempty_symbol": 0, "nonempty_time": 0,
            "with_any_signal": 0, "with_any_weight": 0,
            "samples": [],
        }
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                           asset_type=AssetType.FUTURES, interval="1m")
        ctx.subscribe_data(data_id=FEATURES_ID)
        ctx.add_event_log(f"subscribed to {FEATURES_ID}", sub_event_type="INIT")

    def on_bar(self, ctx, event):
        self.state["bars"] += 1

    def on_custom_data(self, ctx, event):
        row = event.data()
        self.state["rows"] += 1

        symbol = row.column_data("sym", default="")
        timestamp = row.column_data("time", default="")
        if symbol:
            self.state["nonempty_symbol"] += 1
        if timestamp:
            self.state["nonempty_time"] += 1

        signals = {name: row.column_data(name, default="") for name in SIGNAL_COLUMNS}
        weights = {name: row.column_data(name, default="") for name in WEIGHT_COLUMNS}
        if any(_is_valid(v) for v in signals.values()):
            self.state["with_any_signal"] += 1
        if any(_is_valid(v) for v in weights.values()):
            self.state["with_any_weight"] += 1

        if any(_is_valid(v) for v in signals.values()):
            payload = {"symbol": str(symbol), "time": str(timestamp)}
            payload.update({name: str(value) for name, value in signals.items()})
            payload.update({name: str(value) for name, value in weights.items()})
            ctx.add_event_log(
                f"FEATURES_V3_SIGNAL {symbol} {timestamp}",
                sub_event_type="FEATURES_V3_SIGNAL",
                state_variable=payload,
            )

        if len(self.state["samples"]) < 5:
            self.state["samples"].append({
                "sym": str(symbol),
                "time": str(timestamp),
                "header": str(getattr(row, "header", "") or ""),
                "raw_row": str(getattr(row, "row", "") or ""),
                "signals": {k: str(v) for k, v in signals.items()},
                "weights": {k: str(v) for k, v in weights.items()},
            })

    def on_stop(self, ctx, event):
        ctx.add_event_log(
            f"QuantFeaturesV3 summary: {self.state}",
            sub_event_type="SUMMARY",
            state_variable=self.state,
        )


def _print_summary(state: dict) -> bool:
    checks = {
        "timeline_bars_present": state["bars"] > 0,
        "feature_callbacks_present": state["rows"] > 0,
        "symbols_present": state["rows"] > 0
                            and state["nonempty_symbol"] == state["rows"],
        "timestamps_present": state["rows"] > 0
                              and state["nonempty_time"] == state["rows"],
        "signals_present": state["with_any_signal"] > 0,
        "weights_present": state["with_any_weight"] > 0,
        "samples_captured": bool(state["samples"]),
    }
    failed = [name for name, ok in checks.items() if not ok]
    status = "PASS" if not failed else "FAIL"

    print(f"\n=== quant_features_v3 result: {status} ===")
    for name, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'}  {name}")
    print(f"  bars={state['bars']}  rows={state['rows']}  "
          f"signals={state['with_any_signal']}  weights={state['with_any_weight']}")
    if state["samples"]:
        print("  sample_header:", state["samples"][0]["header"])
        for sample in state["samples"]:
            print(f"    {sample['time']}  sym={sample['sym']}  "
                  f"signal1={sample['signals']['signal1']}  "
                  f"weight1={sample['weights']['weight1']}")
    return not failed


def _extract_state(run) -> dict:
    """Read the SUMMARY event that on_stop wrote for this run."""
    logs = run.event_logs()
    if logs is None or getattr(logs, "empty", True):
        return {}
    rows = logs[logs["sub_event_type"] == "SUMMARY"]
    if rows.empty:
        return {}
    value = rows.iloc[-1]["state_variables"]
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        import json
        value = json.loads(value or "{}")
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="QuantFeaturesV3", type="QuantFeaturesV3",
                                         symbols=[BAR_SYMBOL])],
        symbols=[BAR_SYMBOL],
        start_date=TRADE_DATE,
        end_date=TRADE_DATE,
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT", "schema": ["bars_1m"]},
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_QUANT_FEATURES_V3",
                "schema": ["quant_features_v3"],
                "id": FEATURES_ID,
                "symbols": [FEATURES_SYMBOL],
                # Promote non-default columns (weights, extra signals, stop/target).
                "filters": {"fields": EXTRA_FIELDS},
            },
        ],
    )
    run.wait()
    print("status:", run.status())

    state = _extract_state(run)
    if not state:
        print("FAIL: no SUMMARY event captured from strategy (run may have errored).")
        sys.exit(1)

    sys.exit(0 if _print_summary(state) else 1)
