"""Remote metric arithmetic on an option round trip, reconciled from fills.

Standard equity options are 100-share contracts, so ``net_pnl`` on a 1-lot
round trip must be ``(exit_prem - entry_prem) * 100 - fees``. An equity-only
metrics test uses multiplier=1 and can't catch a build where the option
multiplier is dropped. This case buys 1 SPXW 0DTE call at the ask, sells at
the bid, and reconciles ``report.net_pnl`` against the independently computed
notional using the multiplier the engine itself reports for the contract.
"""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.trading.price_utils import adjust_tick_size
from hiveq.flow.trading_types import OrderType

INITIAL = 1_000_000.0

class SdkT10c:
    def on_start(self, ctx, event):
        self.contract = ""
        self.entry_id = ""
        self.exit_id = ""
        self.fills = []            # [side, qty, avg_px] per fill
        self.state = {"entry_filled": False, "exit_filled": False,
                      "multiplier": None, "contract": ""}
        ctx.subscribe_option_snaps("SPXW", option_type="C",
                                   expiration_type="0dte", interval="1s")
    def on_snap(self, ctx, event):
        snap = event.data()
        if snap.bid_px is None or snap.ask_px is None or snap.ask_px <= 0:
            return
        if not self.entry_id:
            self.contract = snap.chain
            limit = adjust_tick_size(self.contract, float(snap.ask_px))
            order = ctx.buy_order(self.contract, 1.0,
                                  order_type=OrderType.LIMIT, limit_price=limit)
            if order is not None:
                self.entry_id = order.order_id
        elif self.state["entry_filled"] and not self.exit_id and snap.chain == self.contract:
            limit = adjust_tick_size(self.contract, float(snap.bid_px))
            order = ctx.sell_order(self.contract, 1.0,
                                   order_type=OrderType.LIMIT, limit_price=limit)
            if order is not None:
                self.exit_id = order.order_id
    def on_order(self, ctx, event):
        order = event.data()
        if not order.is_filled:
            return
        px = float(order.avg_px or order.last_px or 0.0)
        qty = float(order.filled_qty or 0.0)
        self.fills.append([str(order.side).upper(), qty, px])
        if order.order_id == self.entry_id:
            self.state["entry_filled"] = True
        elif order.order_id == self.exit_id:
            self.state["exit_filled"] = True
    def on_stop(self, ctx, event):
        # Multiplier comes from the resolved OCC contract, not the SPXW root,
        # because option roots do not carry a multiplier of their own.
        try:
            self.state["multiplier"] = float(ctx.instrument(self.contract).multiplier)
        except Exception:
            self.state["multiplier"] = None
        self.state["contract"] = self.contract
        self.state["fills"] = self.fills
        emit_checkpoint(ctx, "t10c_metrics_validation_options", self.state)

if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT10c", type="SdkT10c", symbols=["SPXW"])],
        symbols=["SPXW"], start_date="2026-04-24", end_date="2026-04-24",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_OPT",
                       "schema": ["snaps_1s"]}],
        backtest_config=BacktestConfig(initial_capital=INITIAL,
                                       session_start="15:30", session_end="16:00"))
    state = completed_checkpoint(run, "t10c_metrics_validation_options")
    report = run.report()
    fills = state["fills"]
    buys = [x for x in fills if x[0].endswith("BUY")]
    sells = [x for x in fills if x[0].endswith("SELL")]
    def weighted_average(rows):
        return sum(q * px for _, q, px in rows) / max(1e-9, sum(q for _, q, _ in rows))
    mult = state["multiplier"]
    entry = weighted_average(buys) if buys else 0.0
    exit = weighted_average(sells) if sells else 0.0
    qty = sum(q for _, q, _ in buys) if buys else 0.0
    fees = float(report.total_fees or 0)
    # Standard equity option is 100 shares/contract. A multiplier of 1 here
    # is the bug we're catching -- the engine failed to attach the option
    # contract's security metadata.
    multiplier_present = isinstance(mult, float) and mult >= 100.0
    gross = (exit - entry) * qty * (mult or 1.0)
    expected = gross - fees
    trades = run.trades()
    pnl_columns = [c for c in trades.columns if str(c).lower() == "pnl"]
    trades_ok = not pnl_columns or abs(float(trades[pnl_columns[0]].sum()) - gross) < max(2, abs(gross) * .1)
    finish("t10c_metrics_validation_options", {
        "occ_contract_resolved": bool(state["contract"]),
        "round_trip_recorded": bool(buys) and bool(sells),
        "option_multiplier_ge_100": multiplier_present,
        "net_pnl_matches_fills_with_multiplier":
            abs(report.net_pnl - expected) < max(2, abs(expected) * .05),
        "trades_pnl_consistent": trades_ok,
        "fees_non_negative": fees >= 0,
    }, extra=(f"contract={state['contract']}, entry={entry}, exit={exit}, "
             f"qty={qty}, multiplier={mult}, gross={gross}, "
             f"expected={expected}, report={report.net_pnl}"))
