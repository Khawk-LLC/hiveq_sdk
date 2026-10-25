"""Minimal subscribe-only probe: subscribe to 29 .v.0 futures and do nothing.

No orders, no state, no on_rollover handling, no event logs.  The only Python
work per bar is a single ``pass``.  If this still OOMs the executor container,
the memory cost is on the engine / data layer, not on the strategy; a fuller
test with orders would then be certain to OOM too.

Not a release-validation case (no RESULT line, no scoring); it's a diagnostic
on top of t79's crash.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import wait_for_final

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, EngineConfig, StrategyConfig
from hiveq.flow.config import AssetType


SERIES = [f"{root}.v.0" for root in [
    "6A", "6B", "6E", "6J", "6S",
    "BZ", "CL", "ES", "GC", "HG",
    "HH", "KE", "MGC", "NG", "NIY",
    "NQ", "PL", "SI", "UB", "YM",
    "ZB", "ZC", "ZF", "ZL", "ZM",
    "ZN", "ZS", "ZT", "ZW",
]]


class SdkFuturesSubscribeOnlyProbe:
    def on_start(self, ctx, event):
        ctx.subscribe_bars(
            ctx.strategy_config.symbols,
            asset_type=AssetType.FUTURES,
            interval="1m",
        )

    def on_bar(self, ctx, event):
        pass


def main() -> int:
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkFuturesSubscribeOnlyProbe",
            type="SdkFuturesSubscribeOnlyProbe",
            symbols=SERIES,
        )],
        symbols=SERIES,
        start_date="2013-01-02",
        end_date="2026-10-19",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(
            initial_capital=25_000_000.0,
            auto_flatten_at_close=False,
            session_start="18:00", session_end="17:00",
        ),
        engine_config=EngineConfig(timezone="America/New_York"),
    )
    print(f"[probe] run_id={run.run_id} task_id={run.task_id}", flush=True)
    status = wait_for_final(run, timeout=21600.0)
    print(f"[probe] final status: {status}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
