"""t83: read-only accessors on a symbol the strategy has not registered.

On 2026-11-11 a livesim strategy called ``ctx.has_open_order("NVDA")`` in
``on_start`` before subscribing NVDA as EQUITY. The read registered NVDA with
the old FUTURES default, the engine's security cache is shared by every
strategy in the container, so the later EQUITY subscribe (and another
strategy's EQUITY registration) got the FUTURES instrument back. That
strategy's MOC exit was rejected ("Auction orders are supported only for
Equities") and its position file tagged NVDA as FUTURES.

Here every symbol-taking read accessor runs on NVDA before NVDA is registered:

* each returns its neutral answer (no orders, flat, zero PnL, no position)
  and none raises;
* none of them registers the symbol: subscribing NVDA as EQUITY afterwards
  gives an EQUITY instrument;
* NVDA then trades as an equity (a one-share round trip fills).
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

CHECKPOINT = "t83_accessors_before_registration"
SYMBOL = "NVDA"

# accessor name -> expected neutral answer for a symbol with no orders or position
# (None: any non-raising answer is accepted, the call itself is what is tested;
# position reads are scored by quantity, see FLAT_READS).
EXPECTED = {
    "has_open_order": False,
    "open_order_qty": 0.0,
    "net_position": 0.0,
    "quantity": 0.0,
    "is_flat": True,
    "is_net_long": False,
    "is_net_short": False,
    "cancel_all_orders": None,
    "clear_pending_order": None,
    "portfolio.position": None,
    "portfolio.net_position": 0.0,
    "portfolio.positions_for_symbol": None,
    "portfolio.is_flat": True,
    "portfolio.is_net_long": False,
    "portfolio.is_net_short": False,
    "portfolio.realized_pnl": 0.0,
    "portfolio.unrealized_pnl": 0.0,
    "portfolio.total_pnl": 0.0,
    "portfolio.day_pnl": 0.0,
}


# Position reads return no position or a flat one (quantity 0), never an open one.
FLAT_READS = ("portfolio.position", "portfolio.positions_for_symbol")


def flat(value):
    values = value if isinstance(value, list) else [value]
    return all(v is None or float(getattr(v, "quantity", 0) or 0) == 0 for v in values)


def asset_type_name(instrument):
    if instrument is None:
        return None
    value = getattr(instrument, "asset_type", None)
    return str(getattr(value, "name", value)).upper() if value is not None else None


class SdkT83:
    def on_start(self, ctx, event):
        self.s = {"reads": {}, "errors": {}, "asset_type_after_subscribe": None,
                  "bars": 0, "buy_filled": False, "sell_filled": False, "flat_at_end": None}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

        portfolio = ctx.portfolio()
        for name in EXPECTED:
            owner, attr = (portfolio, name.split(".", 1)[1]) if name.startswith("portfolio.") else (ctx, name)
            try:
                value = getattr(owner, attr)(SYMBOL)
                if name in FLAT_READS:
                    self.s["reads"][name + ".flat"] = flat(value)
                if isinstance(value, (bool, int, float)) or value is None:
                    self.s["reads"][name] = value
                elif isinstance(value, list):
                    self.s["reads"][name] = [repr(v) for v in value]
                else:
                    self.s["reads"][name] = repr(value)
            except Exception as exc:  # noqa: BLE001 -- the result is the finding
                self.s["errors"][name] = repr(exc)

        # Only now register NVDA, as EQUITY.
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")
        # Already registered by the subscribe: a cache hit, the same call on every SDK version.
        self.s["asset_type_after_subscribe"] = asset_type_name(ctx.instrument(SYMBOL))

    def on_bar(self, ctx, event):
        if event.data().symbol != SYMBOL:
            return
        self.s["bars"] += 1
        if self.s["bars"] == 2:
            ctx.buy_order(SYMBOL, quantity=1)
        elif self.s["bars"] == 10 and not ctx.has_open_order(SYMBOL):
            ctx.close_position(SYMBOL)
        elif self.s["bars"] == 20:
            self.s["flat_at_end"] = ctx.is_flat(SYMBOL)

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
        strategy_configs=[StrategyConfig(name="SdkT83", type="SdkT83", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:30"))
    s = completed_checkpoint(run, CHECKPOINT)
    reads, errors = s["reads"], s["errors"]
    checks = {f"no_raise_{name}": name not in errors for name in EXPECTED}
    checks.update({f"neutral_{name}": reads.get(name) == want
                   for name, want in EXPECTED.items() if want is not None and name not in errors})
    checks.update({f"flat_{name}": reads.get(name + ".flat") is True for name in FLAT_READS})
    checks.update({
        "reads_did_not_register_as_futures": s["asset_type_after_subscribe"] == "EQUITY",
        "nvda_bars_received": s["bars"] >= 10,
        "buy_filled": s["buy_filled"],
        "sell_filled": s["sell_filled"],
        "flat_after_round_trip": s["flat_at_end"] is True,
        **evidence_checks(run, orders=2, trades=1),
    })
    finish(CHECKPOINT, checks, extra=str(s))
