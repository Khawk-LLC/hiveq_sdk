"""t84: ``ctx.instrument(symbol, asset_type=None)`` registers with the right asset type.

``ctx.instrument`` used to register any symbol it had not seen as FUTURES, so
``ctx.instrument("NVDA")`` produced a FUTURES NVDA for every strategy in the
container. It now takes an optional ``asset_type``:

* with ``asset_type`` the symbol registers as exactly that type
  (MSFT EQUITY, ESM5 FUTURES);
* without it (deprecated) the symbol is resolved instead of guessed: an equity
  in the reference data registers as EQUITY (AMD), a futures contract as
  FUTURES (NQM5), and the answer is cached (a second call agrees);
* a symbol that resolves to nothing returns None rather than a FUTURES stub;
* FUTURES is refused for a symbol that is not a futures contract (TSLA has no
  month code or year) and the refusal registers nothing, so TSLA still
  registers as EQUITY afterwards;
* a symbol registered this way trades: a one-share MSFT round trip fills.
"""

from pathlib import Path
import sys

# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, evidence_checks, finish  # noqa: E402

import hiveq.flow as hf                                    # noqa: E402
from hiveq.flow import StrategyConfig                      # noqa: E402
from hiveq.flow.config import AssetType, BacktestConfig    # noqa: E402

CHECKPOINT = "t84_instrument_asset_type"
DAY = "2025-06-02"          # ESM5 / NQM5 are the front contracts
TRADED = "MSFT"


def asset_type_name(instrument):
    if instrument is None:
        return None
    value = getattr(instrument, "asset_type", None)
    return str(getattr(value, "name", value)).upper() if value is not None else None


class SdkT84:
    def on_start(self, ctx, event):
        self.s = {"types": {}, "errors": {}, "bars": 0,
                  "buy_filled": False, "sell_filled": False, "flat_at_end": None}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

        # (label, symbol, asset_type) in call order; TSLA FUTURES must come before TSLA EQUITY.
        calls = [
            ("explicit_equity", "MSFT", AssetType.EQUITY),
            ("explicit_futures", "ESM5", AssetType.FUTURES),
            ("resolved_equity", "AMD", None),
            ("resolved_equity_again", "AMD", None),
            ("resolved_futures", "NQM5", None),
            ("unresolvable", "ZZQXJ", None),
            ("futures_on_equity_symbol", "TSLA", AssetType.FUTURES),
            ("equity_after_refused_futures", "TSLA", AssetType.EQUITY),
        ]
        for label, symbol, asset_type in calls:
            try:
                inst = ctx.instrument(symbol) if asset_type is None else ctx.instrument(symbol, asset_type)
                self.s["types"][label] = asset_type_name(inst)
            except Exception as exc:  # noqa: BLE001 -- the result is the finding
                self.s["errors"][label] = repr(exc)

        ctx.subscribe_bars([TRADED], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        if event.data().symbol != TRADED:
            return
        self.s["bars"] += 1
        if self.s["bars"] == 2:
            ctx.buy_order(TRADED, quantity=1)
        elif self.s["bars"] == 10 and not ctx.has_open_order(TRADED):
            ctx.close_position(TRADED)
        elif self.s["bars"] == 20:
            self.s["flat_at_end"] = ctx.is_flat(TRADED)

    def on_order(self, ctx, event):
        order = event.data()
        if "FILL" in str(getattr(order, "status", "")).upper() and float(order.filled_qty or 0) > 0:
            side = str(order.side).upper()
            if side.endswith("BUY"):
                self.s["buy_filled"] = True
            elif side.endswith("SELL"):
                self.s["sell_filled"] = True

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, CHECKPOINT, self.s)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT84", type="SdkT84", symbols=["AAPL"])],
        symbols=["AAPL"], start_date=DAY, end_date=DAY,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:30"))
    s = completed_checkpoint(run, CHECKPOINT)
    types, errors = s["types"], s["errors"]
    expected = {
        "explicit_equity": "EQUITY",
        "explicit_futures": "FUTURES",
        "resolved_equity": "EQUITY",
        "resolved_equity_again": "EQUITY",
        "resolved_futures": "FUTURES",
        "unresolvable": None,
        "futures_on_equity_symbol": None,
        "equity_after_refused_futures": "EQUITY",
    }
    checks = {f"{label}_{want or 'none'}".lower(): label not in errors and types.get(label) == want
              for label, want in expected.items()}
    checks.update({
        "registered_symbol_bars_received": s["bars"] >= 10,
        "buy_filled": s["buy_filled"],
        "sell_filled": s["sell_filled"],
        "flat_after_round_trip": s["flat_at_end"] is True,
        **evidence_checks(run, orders=2, trades=1),
    })
    finish(CHECKPOINT, checks, extra=str(s))
