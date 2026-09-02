"""Diagnostic: is there any window where equity bars_1m AND eq_trades both exist?

t03 subscribes to both in one run. bars_1m is empty over July 2026 and populated
over June 2025; eq_trades is the opposite. If no window carries both, t03 cannot
be fixed by re-windowing alone.
"""
import sys
from pathlib import Path
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

WINDOWS = [
    ("2022-11-15", "2022-11-16"),
    ("2026-06-18", "2026-06-19"),
    ("2026-08-20", "2026-08-21"),
    ("2025-06-02", "2025-06-03"),
]


class SdkProbeEqOverlap:
    def on_start(self, ctx, event):
        if not hasattr(self, "c"):
            self.c = {"eq1m": 0, "trades": 0}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)

    def on_bar(self, ctx, event):
        if event.data().interval == "1m":
            self.c["eq1m"] += 1

    def on_trade(self, ctx, event):
        self.c["trades"] += 1

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "probe_eq_overlap", self.c)


if __name__ == "__main__":
    schemas = [("HIVEQ_US_EQ", "bars_1m"), ("HIVEQ_US_EQ", "eq_trades")]
    for start, end in WINDOWS:
        try:
            run = hf.run_backtest(
                strategy_configs=[StrategyConfig(name="SdkProbeEqOverlap",
                                                 type="SdkProbeEqOverlap",
                                                 symbols=["AAPL"])],
                symbols=["AAPL"], start_date=start, end_date=end,
                data_configs=[{"type": "hiveq_historical", "dataset": d, "schema": [s]}
                              for d, s in schemas],
                backtest_config=BacktestConfig(session_start="09:30",
                                               session_end="10:00"))
            print(f"PROBE {start}..{end}: "
                  f"{completed_checkpoint(run, 'probe_eq_overlap')}", flush=True)
        except Exception as exc:
            print(f"PROBE {start}..{end}: FAILED {str(exc)[:160]}", flush=True)
