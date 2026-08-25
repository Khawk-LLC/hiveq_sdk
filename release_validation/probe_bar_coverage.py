"""Diagnostic: which continuous futures aliases actually deliver 1m bars.

Not a validation -- it asserts nothing and is not collected by ``run_all.py``
(the runner only picks up ``tNN_*.py``). It exists because
``t52_multi_symbol_long_rollover`` reports most of its thirty-seven symbols as
never entered, and the only way to tell "the case never traded them" from "the
engine never delivered a bar for them" is to subscribe to all of them over a
short window and count what arrives, per delivered contract, next to what
``instrument(alias).current_contract`` claims.

Run it directly: ``python release_validation/probe_bar_coverage.py``.
"""
import sys
from pathlib import Path
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

SYMBOLS = ["6A.v.0","6B.v.0","6E.v.0","6J.v.0","6S.v.0","BTC.v.0","BZ.v.0","CL.v.0",
           "ES.v.0","ETH.v.0","GC.v.0","HG.v.0","HH.v.0","KE.v.0","MBT.v.0","MCL.v.0",
           "MES.v.0","MET.v.0","MNQ.v.0","MYM.v.0","NG.v.0","NIY.v.0","NQ.v.0","PL.v.0",
           "RTY.v.0","SI.v.0","VX.v.0","YM.v.0","ZB.v.0","ZC.v.0","ZF.v.0","ZL.v.0",
           "ZM.v.0","ZN.v.0","ZS.v.0","ZT.v.0","ZW.v.0"]


class SdkProbeBarCoverage:
    def on_start(self, ctx, event):
        if not hasattr(self, "counts"):
            self.counts = {}
            self.current = {}
        ctx.subscribe_bars(SYMBOLS, asset_type=AssetType.FUTURES, interval="1m")
        for symbol in SYMBOLS:
            try:
                self.current[symbol] = str(
                    getattr(ctx.instrument(symbol), "current_contract", "") or "")
            except Exception as exc:
                self.current[symbol] = f"raised: {str(exc)[:40]}"

    def on_bar(self, ctx, event):
        bar = event.data()
        self.counts[str(bar.symbol)] = self.counts.get(str(bar.symbol), 0) + 1

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "probe_bar_coverage",
                        {"delivered": self.counts, "current_contract": self.current})


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkProbeBarCoverage",
                                         type="SdkProbeBarCoverage", symbols=SYMBOLS)],
        symbols=SYMBOLS, start_date="2025-09-15", end_date="2025-09-17",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="18:00", session_end="17:00",
                                       enable_auto_rollover=True),
    )
    print(f"run_id={run.run_id}", flush=True)
    state = completed_checkpoint(run, "probe_bar_coverage")
    delivered = state["delivered"]
    print("DELIVERED:", sorted(delivered.items()))
    print("CURRENT:", state["current_contract"])
