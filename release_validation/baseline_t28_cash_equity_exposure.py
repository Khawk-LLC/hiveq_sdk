"""Account cash/equity identities and manual two-symbol exposure arithmetic."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger(); INITIAL = 100_000.0; QTY = {"AAPL": 100, "MSFT": 50}


class SdkT28:
    def on_start(self, ctx, event):
        self.counts = {s: 0 for s in QTY}; self.sent = set(); self.last = {}
        self.s = {"flat": None, "held": None, "fills": 0}
        ctx.subscribe_bars(list(QTY), asset_type=AssetType.EQUITY, interval="1m")
    def snap(self, ctx):
        p = ctx.portfolio()
        return {"capital": p.initial_capital, "cash": p.cash, "equity": p.equity,
            "realized": p.realized_pnl(), "unrealized": p.unrealized_pnl(), "fees": p.fees,
            "gross": p.gross_exposure(), "net": p.net_exposure(),
            "positions": {s: p.net_position(s) for s in QTY}}
    def on_bar(self, ctx, event):
        bar = event.data(); symbol = bar.symbol; self.counts[symbol] += 1; self.last[symbol] = bar.close
        if self.s["flat"] is None and all(v >= 1 for v in self.counts.values()): self.s["flat"] = self.snap(ctx)
        if self.counts[symbol] == 5 and symbol not in self.sent:
            self.sent.add(symbol); ctx.buy_order(symbol, QTY[symbol])
        if self.s["fills"] >= 2 and all(v >= 15 for v in self.counts.values()):
            self.s["held"] = self.snap(ctx) | {"last": dict(self.last)}
    def on_order(self, ctx, event):
        if event.data().is_filled: self.s["fills"] += 1
    def on_stop(self, ctx, event): emit_checkpoint(ctx, "t28_cash_equity_exposure", self.s)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT28", type="SdkT28", symbols=list(QTY))],
        symbols=list(QTY), start_date="2025-12-02", end_date="2025-12-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(initial_capital=INITIAL, session_start="09:30", session_end="11:00"))
    state = completed_checkpoint(run, "t28_cash_equity_exposure"); flat=state["flat"]; held=state["held"]
    expected = sum(QTY[s] * held["last"][s] for s in QTY) if held else 0
    finish("t28_cash_equity_exposure", {
        "two_fills": state["fills"] >= 2,
        "capital_fixed": flat is not None and held is not None and flat["capital"] == held["capital"] == INITIAL,
        "flat_cash_equity_capital": abs(flat["cash"]-INITIAL)<.01 and abs(flat["equity"]-INITIAL)<.01,
        "cash_spent_after_buys": held["cash"] < INITIAL,
        "cash_equals_equity_minus_holdings": abs(held["cash"]-(held["equity"]-held["net"])) < 2,
        "equity_identity": abs(held["equity"]-(INITIAL+held["realized"]+held["unrealized"])) <= held["fees"]+.1,
        "positions_exact": held["positions"] == {"AAPL":100.0,"MSFT":50.0},
        "long_only_gross_equals_net": abs(held["gross"]-held["net"]) < 1e-6,
        "exposure_matches_prices": abs(held["gross"]-expected)/expected < .01,
    }, extra=f"flat={flat}, held={held}, expected={expected}")
