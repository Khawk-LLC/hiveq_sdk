"""Isolate t02: do eq_trades dispatch under a narrow session window?

Same symbols/dates as baseline_t02; only session_start/session_end vary.
Usage: python release_validation/probe_t02_eq_window.py [start end]
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, BacktestConfig


class ProbeEqWindow:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.state = {"MSFT": 0, "AAPL": 0, "VIX": 0, "SPX": 0,
                          "first": {}, "last": {}}
        ctx.subscribe_trades(["MSFT"], asset_type=AssetType.EQUITY)
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)
        ctx.subscribe_index(["VIX"])
        ctx.subscribe_index(["SPX"])

    def _record(self, symbol, when):
        if symbol not in self.state:
            return
        self.state[symbol] += 1
        self.state["first"].setdefault(symbol, when)
        self.state["last"][symbol] = when

    def on_trade(self, ctx, event):
        data = event.data()
        self._record(data.symbol, data.time.isoformat())

    def on_index_price(self, ctx, event):
        data = event.data()
        self._record(data.symbol, data.time.isoformat())

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "probe_t02_eq_window", self.state)


if __name__ == "__main__":
    window = [a for a in sys.argv[1:3] if ":" in a]
    dates = [a for a in sys.argv[1:5] if ":" not in a] or ["2025-06-02", "2025-06-04"]
    kwargs = {}
    if window:
        kwargs["backtest_config"] = BacktestConfig(
            session_start=window[0], session_end=window[1]
        )
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="ProbeEqWindow", type="ProbeEqWindow",
                                         symbols=["MSFT", "AAPL"])],
        symbols=["MSFT", "AAPL"],
        start_date=dates[0], end_date=dates[-1],
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]},
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_IND", "schema": ["indices_values"]},
        ],
        **kwargs,
    )
    state = completed_checkpoint(run, "probe_t02_eq_window")
    print(f"PROBE_DATES={dates} PROBE_WINDOW={window or 'default'} "
          f"MSFT={state['MSFT']} AAPL={state['AAPL']} "
          f"VIX={state['VIX']} SPX={state['SPX']}")
    print(f"PROBE_FIRST={state['first']}")
    print(f"PROBE_LAST={state['last']}")
