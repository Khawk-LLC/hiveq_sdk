"""Diagnostic: is t03's eq1m=0 a bars_1m coverage gap, or the 09:30-10:00 session?

Two runs over t03's own window; only the session config differs.
"""
import sys
from pathlib import Path
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


class SdkProbeEq1m:
    def on_start(self, ctx, event):
        if not hasattr(self, "c"):
            self.c = {"eq1m": 0, "eq1d": 0, "fut1m": 0}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1d")
        ctx.subscribe_bars(["ES.c.0"], asset_type=AssetType.FUTURES, interval="1m")

    def on_bar(self, ctx, event):
        b = event.data()
        if b.symbol == "AAPL" and b.interval == "1m":
            self.c["eq1m"] += 1
        elif b.symbol == "AAPL" and b.interval == "1d":
            self.c["eq1d"] += 1
        elif b.interval == "1m":
            self.c["fut1m"] += 1

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "probe_eq1m", self.c)


def probe(label, start, end, bcfg):
    schemas = [("HIVEQ_US_EQ", "bars_1m"), ("HIVEQ_US_EQ", "bars_1d"),
               ("HIVEQ_US_FUT", "bars_1m")]
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkProbeEq1m", type="SdkProbeEq1m",
                                         symbols=["AAPL"])],
        symbols=["AAPL"], start_date=start, end_date=end,
        data_configs=[{"type": "hiveq_historical", "dataset": d, "schema": [s]}
                      for d, s in schemas],
        backtest_config=bcfg)
    print(f"PROBE {label}: {completed_checkpoint(run, 'probe_eq1m')}", flush=True)


if __name__ == "__main__":
    probe("july2026_session_0930_1000", "2026-07-01", "2026-07-31",
          BacktestConfig(session_start="09:30", session_end="10:00"))
    probe("july2026_default_session", "2026-07-01", "2026-07-03", BacktestConfig())
    probe("june2025_default_session", "2025-06-02", "2025-06-03", BacktestConfig())
