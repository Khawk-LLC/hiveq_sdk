"""t44 triage: does rows=0 come from the config wiring or from absent vm data?

Variant A reproduces t44's short-form config verbatim.
Variant B uses the documented full `filters` wiring (symbolFilterKey='sym',
continuous-contract resolution, timestampColumn='time'), no tag filter.
Variant C is B with a bare root symbol instead of the stored '/ES 26M'.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE)]
from qa_common import completed_checkpoint, emit_checkpoint  # noqa: E402

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

VARIANT = sys.argv[1] if len(sys.argv) > 1 else "B"
DATE = sys.argv[2] if len(sys.argv) > 2 else "2026-05-07"


class SdkProbe44:
    def on_start(self, ctx, event):
        self.state = {"bars": 0, "rows": 0, "samples": []}
        ctx.subscribe_bars(["ES.c.0"], asset_type=AssetType.FUTURES, interval="1m")
        ctx.subscribe_data("clusters")

    def on_bar(self, ctx, event):
        self.state["bars"] += 1

    def on_custom_data(self, ctx, event):
        row = event.data()
        self.state["rows"] += 1
        if len(self.state["samples"]) < 5:
            self.state["samples"].append({
                "sym": str(row.column_data("sym", "")),
                "time": str(row.column_data("time", "")),
                "tag": str(row.column_data("tag", "")),
            })

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "probe44", self.state)


CONFIGS = {
    # t44 as written: short form, stored analytics notation, no symbolFilterKey.
    "A": {"type": "hiveq_historical", "dataset": "HIVEQ_QUANT_CLUSTERS",
          "id": "clusters", "schema": ["clusters"], "symbols": ["/ES 26M"]},
    # Documented wiring, continuous-contract resolution, no tag filter.
    "B": {"type": "hiveq_historical", "dataset": "HIVEQ_QUANT_CLUSTERS",
          "data_type": "custom", "id": "clusters", "schema": ["clusters"],
          "filters": {"adapter": "HiveQUserDataAdapter",
                      "dataset": "HIVEQ_QUANT_CLUSTERS", "schema": "clusters",
                      "symbols": "ES.v.0", "symbolFilterKey": "sym",
                      "symbolResolutionMode": "continuous_contract",
                      "timestampColumn": "time"}},
    # Proposed t44 fix: exact stored `sym` value, filtered under the documented
    # key, no continuous-contract resolution (analytics store `/ES 26M`).
    "D": {"type": "hiveq_historical", "dataset": "HIVEQ_QUANT_CLUSTERS",
          "data_type": "custom", "id": "clusters", "schema": ["clusters"],
          "filters": {"adapter": "HiveQUserDataAdapter",
                      "dataset": "HIVEQ_QUANT_CLUSTERS", "schema": "clusters",
                      "symbols": "/ES 26M", "symbolFilterKey": "sym",
                      "timestampColumn": "time"}},
    # No symbol filter at all: proves whether the table has ANY row that day.
    "C": {"type": "hiveq_historical", "dataset": "HIVEQ_QUANT_CLUSTERS",
          "data_type": "custom", "id": "clusters", "schema": ["clusters"],
          "filters": {"adapter": "HiveQUserDataAdapter",
                      "dataset": "HIVEQ_QUANT_CLUSTERS", "schema": "clusters",
                      "timestampColumn": "time"}},
}

if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkProbe44", type="SdkProbe44",
                                         symbols=["ES.c.0"])],
        symbols=["ES.c.0"],
        start_date=DATE,
        end_date=DATE,
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT", "schema": ["bars_1m"]},
            CONFIGS[VARIANT],
        ],
        backtest_config=BacktestConfig(session_start="09:30", session_end="16:00"),
    )
    state = completed_checkpoint(run, "probe44")
    print(f"PROBE variant={VARIANT} date={DATE} -> {state}")
