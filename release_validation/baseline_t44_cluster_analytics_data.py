"""ClickHouse-backed cluster analytics route through on_custom_data with payload evidence."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

CLUSTER_SYMBOL = "/ES 26M"


class SdkT44:
    def on_start(self, ctx, event):
        self.state = {"bars": 0, "rows": 0, "samples": [], "nonempty_sym": 0, "nonempty_time": 0}
        ctx.subscribe_bars(["ES.c.0"], asset_type=AssetType.FUTURES, interval="1m")
        ctx.subscribe_data("clusters")

    def on_bar(self, ctx, event):
        self.state["bars"] += 1

    def on_custom_data(self, ctx, event):
        row = event.data()
        self.state["rows"] += 1
        symbol = row.column_data("sym", "")
        timestamp = row.column_data("time", "")
        if symbol:
            self.state["nonempty_sym"] += 1
        if timestamp:
            self.state["nonempty_time"] += 1
        if len(self.state["samples"]) < 5:
            self.state["samples"].append({
                "sym": str(symbol),
                "time": str(timestamp),
                "tag": str(row.column_data("tag", "")),
                "mode": str(row.column_data("mode", "")),
            })

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t44_cluster_analytics_data", self.state)


if __name__ == "__main__":
    # staging_quant_001.clusters_v1 carries 5,261 `tag=ON` rows for the active
    # ES contract on 2026-05-07. Analytics symbols use the source notation
    # `/ES 26M`, not the engine/Data API notation `ESM6`, so filter by the exact
    # stored value rather than applying continuous-contract resolution.
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT44", type="SdkT44", symbols=["ES.c.0"])],
        symbols=["ES.c.0"],
        start_date="2026-05-07",
        end_date="2026-05-07",
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT", "schema": ["bars_1m"]},
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_QUANT_CLUSTERS",
                "id": "clusters",
                "schema": ["clusters"],
                "symbols": [CLUSTER_SYMBOL],
            },
        ],
        backtest_config=BacktestConfig(session_start="09:30", session_end="16:00"),
    )
    state = completed_checkpoint(run, "t44_cluster_analytics_data")
    finish("t44_cluster_analytics_data", {
        "timeline_data_present": state["bars"] > 0,
        "cluster_callbacks_present": state["rows"] > 0,
        "symbols_present": state["nonempty_sym"] == state["rows"],
        "timestamps_present": state["nonempty_time"] == state["rows"],
        "payload_samples_persisted": bool(state["samples"]),
    }, extra=str(state))
