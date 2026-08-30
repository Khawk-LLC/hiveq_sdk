"""Which futures roots actually deliver bars, and under which roll rule?

t52 receives bars for only 7 of its 37 roots although ClickHouse holds bars and
continuous definitions for all of them. Callbacks are stamped with the delivered
*outright* (ESU4), not the continuous alias, so counting has to map the outright
back to its root -- comparing against "ES.v.0" matches nothing and reports a
false zero for every symbol.

Set PROBE_ROLL_RULE to 'v' or 'c' to measure one rule per run; both resolve to
outrights, so a single run cannot attribute a bar to one rule or the other.
"""
import os
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint            # noqa: E402

import hiveq.flow as hf                                                # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                  # noqa: E402
from hiveq.flow.config import AssetType                                # noqa: E402

ROOTS = [
    "6A", "6B", "6E", "6J", "6S", "BTC", "BZ", "CL", "ES", "ETH", "GC", "HG",
    "HH", "KE", "MBT", "MCL", "MES", "MET", "MNQ", "MYM", "NG", "NIY", "NQ",
    "PL", "RTY", "SI", "VX", "YM", "ZB", "ZC", "ZF", "ZL", "ZM", "ZN", "ZS",
    "ZT", "ZW",
]
RULE = os.environ.get("PROBE_ROLL_RULE", "c")
SYMBOLS = [f"{root}.{RULE}.0" for root in ROOTS]


def root_for(symbol: str) -> str:
    """Longest root that prefixes the delivered outright (MES before ME)."""
    best = ""
    for root in ROOTS:
        if symbol.startswith(root) and len(root) > len(best):
            best = root
    return best


class SdkProbeAlias:
    def on_start(self, ctx, event):
        if not hasattr(self, "counts"):
            self.counts = {root: 0 for root in ROOTS}
            self.delivered = {}
        # Subscribe what the run was submitted with, not the module-level list:
        # PROBE_ROLL_RULE is set on the submitting shell and never reaches the
        # container, so a module-level SYMBOLS silently measures the default
        # rule on every run regardless of what was requested.
        self.symbols = list(ctx.strategy_config.symbols)
        ctx.subscribe_bars(self.symbols, asset_type=AssetType.FUTURES, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        symbol = str(getattr(bar, "symbol", "") or "")
        root = root_for(symbol)
        if root:
            self.counts[root] += 1
            self.delivered.setdefault(root, symbol)

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "probe_alias",
                        {"counts": self.counts, "delivered": self.delivered,
                         "subscribed": self.symbols[:3]})


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkProbeAlias", type="SdkProbeAlias",
                                         symbols=SYMBOLS)],
        symbols=SYMBOLS, start_date="2024-06-10", end_date="2024-06-12",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(initial_capital=50_000_000.0),
    )
    state = completed_checkpoint(run, "probe_alias")
    counts, delivered = state["counts"], state.get("delivered", {})
    live = sorted(r for r, n in counts.items() if n > 0)
    dead = sorted(r for r, n in counts.items() if n == 0)
    print(f"RESULT: PROBE submitted={state.get('subscribed')} "
          f"delivering={len(live)}/{len(ROOTS)}")
    for root in ROOTS:
        print(f"  {root:5s} bars={counts[root]:8d}  outright={delivered.get(root, '-')}")
    print(f"LIVE: {live}")
    print(f"DEAD: {dead}")
