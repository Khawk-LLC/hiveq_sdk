"""An order far beyond the account's capital: what happens, and does the book still add up.

The suite has no pre-trade risk coverage at all. t19 rejects malformed orders on
the Python side, which is argument validation, not risk -- nothing asks what the
engine does with a well-formed order the account cannot possibly afford.

Backtest risk is off unless the caller opts in (SigmaConfigBuilder only builds
RiskSettings when ``risk.enable``/``risk.globalEnable`` is passed through
EngineConfig.params), so the default posture is expected to be "no pre-trade
capital gate". That is a legitimate design choice and this case does not fail
it. What must hold either way is the accounting: whichever way the order goes,
cash, equity, position and fees have to reconcile exactly, and the recorded
posture has to be unambiguous rather than silently drifting between releases.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (                                               # noqa: E402
    completed_checkpoint,
    emit_checkpoint,
    evidence_checks,
    finish,
)

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402

SYMBOL = "AAPL"
CAPITAL = 25_000.0
CONTROL_QTY = 5.0
# ~200 USD a share against 25k of capital: roughly 40x the account.
OVERSIZED_QTY = 5_000.0


class SdkT62:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = 0
            self.control_id = ""
            self.oversized_id = ""
            self.state = {
                "bars": 0, "control_filled": 0.0, "oversized_submitted": None,
                "oversized_status": [], "oversized_filled": 0.0,
                "oversized_reject_reason": "", "oversized_avg_px": 0.0,
                "before": {}, "after": {},
            }
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def snapshot(self, ctx):
        portfolio = ctx.portfolio()
        return {
            "capital": portfolio.initial_capital, "cash": round(portfolio.cash, 6),
            "equity": round(portfolio.equity, 6), "fees": round(portfolio.fees, 6),
            "realized": round(portfolio.realized_pnl(), 6),
            "unrealized": round(portfolio.unrealized_pnl(), 6),
            "position": float(ctx.net_position(SYMBOL)),
        }

    def on_bar(self, ctx, event):
        self.bars += 1
        if self.bars == 3:
            order = ctx.buy_order(SYMBOL, CONTROL_QTY)
            if order is not None:
                self.control_id = order.order_id
        elif self.bars == 20:
            self.state["before"] = self.snapshot(ctx)
            try:
                order = ctx.buy_order(SYMBOL, OVERSIZED_QTY)
            except Exception as exc:                       # noqa: BLE001
                # A local guard refusing the order is itself a valid posture.
                self.state["oversized_submitted"] = False
                self.state["oversized_reject_reason"] = f"local: {exc}"
                order = None
            else:
                self.state["oversized_submitted"] = order is not None
            if order is not None:
                self.oversized_id = order.order_id
        elif self.bars == 40:
            self.state["after"] = self.snapshot(ctx)

    def on_order(self, ctx, event):
        order = event.data()
        if order.order_id == self.control_id and order.is_filled:
            self.state["control_filled"] = float(order.filled_qty or 0)
        if not self.oversized_id or order.order_id != self.oversized_id:
            return
        status = str(order.status).upper()
        if status not in self.state["oversized_status"]:
            self.state["oversized_status"].append(status)
        if float(order.filled_qty or 0) > 0:
            self.state["oversized_filled"] = float(order.filled_qty)
            self.state["oversized_avg_px"] = round(float(order.avg_px or 0), 6)
        if order.reject_reason:
            self.state["oversized_reject_reason"] = str(order.reject_reason)

    def on_stop(self, ctx, event):
        self.state["bars"] = self.bars
        if not self.state["after"]:
            self.state["after"] = self.snapshot(ctx)
        emit_checkpoint(ctx, "t62_capital_and_risk_posture", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT62", type="SdkT62", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(initial_capital=CAPITAL, session_start="09:30",
                                       session_end="11:00", export_orders_csv=True),
    )
    state = completed_checkpoint(run, "t62_capital_and_risk_posture")
    before = state["before"]
    after = state["after"]
    filled = state["oversized_filled"]
    rejected = bool(
        state["oversized_submitted"] is False
        or "REJECT" in " ".join(state["oversized_status"])
        or state["oversized_reject_reason"]
    )
    gated = rejected and filled == 0.0
    admitted = (not rejected) and filled > 0.0

    notional = filled * state["oversized_avg_px"]
    fee_delta = after["fees"] - before["fees"]
    cash_expected = before["cash"] - notional - fee_delta
    position_expected = before["position"] + filled

    checks = {
        "bars_delivered": state["bars"] > 30,
        "control_round_trip": state["control_filled"] == CONTROL_QTY,
        # Exactly one posture, never an ambiguous half-state where an order is
        # reported rejected and still moves the book.
        "posture_unambiguous": gated != admitted,
        "position_matches_outcome": abs(after["position"] - position_expected) < 1e-6,
        "cash_reflects_executed_notional": abs(after["cash"] - cash_expected) < 1.0,
        "equity_identity_holds": abs(
            after["equity"] - (CAPITAL + after["realized"] + after["unrealized"])
        ) <= after["fees"] + 0.5,
        "capital_unchanged": before["capital"] == after["capital"] == CAPITAL,
        # An unaffordable buy that is admitted must drive cash negative; if it
        # does not, cash is not tracking executions at all.
        "unaffordable_fill_drives_cash_negative": (not admitted) or after["cash"] < 0,
    }
    checks.update(evidence_checks(run, orders=2, trades=0))
    posture = "pre_trade_gate" if gated else ("no_pre_trade_gate" if admitted else "ambiguous")
    finish("t62_capital_and_risk_posture", checks,
           extra=(f"posture={posture}; statuses={state['oversized_status']}; "
                  f"reject_reason={state['oversized_reject_reason'][:120]!r}; "
                  f"filled={filled} of {OVERSIZED_QTY} @ {state['oversized_avg_px']}; "
                  f"before={before}; after={after}"))
