"""Trading costs actually respond to the knobs that claim to control them.

``BacktestConfig`` exposes commission, slippage and per-asset fee rates, and
the suite sets almost none of them -- so a release could change a fee default,
or stop wiring a rate through to the OMS entirely, and every other case would
still pass while every PnL number silently moved.

The same fixed strategy is replayed at three ``equity_fee`` rates and the costs
have to move with them: total fees strictly increasing, net PnL strictly
decreasing by the fee delta, the reported fee reconciling against per-order
commission, and the per-share rate matching what was configured. A fourth
replay changes only ``slippage`` and checks that it reaches the fill model.
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
INITIAL = 500_000.0
QTY = 400.0
# Round trips on a fixed schedule: the traded share count is identical in every
# replay, so any cost difference is the configuration and nothing else.
ENTRIES = (4, 24, 44)
EXITS = (14, 34, 54)
FEE_RATES = (0.0, 0.0011, 0.05)


class SdkT64:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = 0
            self.state = {"bars": 0, "fills": [], "shares": 0.0,
                          "commission_total": 0.0, "portfolio": {}}
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        self.bars += 1
        if self.bars in ENTRIES:
            ctx.buy_order(SYMBOL, QTY)
        elif self.bars in EXITS and not ctx.has_open_order(SYMBOL):
            ctx.close_position(SYMBOL)

    def on_order(self, ctx, event):
        order = event.data()
        if "FILL" in str(order.status).upper() and float(order.filled_qty or 0) > 0:
            self.state["fills"].append([
                str(order.side).upper(), float(order.filled_qty),
                round(float(order.avg_px or 0), 6),
                round(float(order.commission or 0), 8),
            ])
            self.state["shares"] += float(order.filled_qty)
            self.state["commission_total"] += float(order.commission or 0)

    def on_stop(self, ctx, event):
        portfolio = ctx.portfolio()
        self.state["bars"] = self.bars
        self.state["commission_total"] = round(self.state["commission_total"], 8)
        self.state["portfolio"] = {
            "fees": round(portfolio.fees, 8),
            "realized": round(portfolio.realized_pnl(), 6),
            "equity": round(portfolio.equity, 6),
        }
        emit_checkpoint(ctx, "t64_cost_model_sensitivity", self.state)


def replay(equity_fee: float, slippage: float = 0.0):
    backtest_config = BacktestConfig(
        initial_capital=INITIAL, session_start="09:30", session_end="11:00",
        # No export_orders_csv: the thin SDK's BacktestConfig has no such field,
        # and passing it raises TypeError before the run is even submitted.
        equity_fee=equity_fee, slippage=slippage,
    )
    # What the client actually submits, read from the same serialization the
    # deploy uses. Without this a cost that never moves is ambiguous between
    # "the SDK dropped the knob" and "the engine ignored it".
    submitted = backtest_config.to_dict()
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT64", type="SdkT64", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=backtest_config,
    )
    state = completed_checkpoint(run, "t64_cost_model_sensitivity")
    return run, state, submitted


if __name__ == "__main__":
    results = []
    for rate in FEE_RATES:
        run, state, submitted = replay(rate)
        report = run.report()
        results.append({
            "rate": rate, "run": run, "state": state, "submitted": submitted,
            "report_fees": round(float(report.total_fees or 0), 8),
            "net_pnl": round(float(report.net_pnl or 0), 6),
        })

    # Only slippage differs from the middle configuration.
    slipped_run, slipped_state, slipped_submitted = replay(FEE_RATES[1], slippage=0.01)

    baseline = results[1]
    shares = [item["state"]["shares"] for item in results]
    fees = [item["state"]["portfolio"]["fees"] for item in results]
    pnl = [item["net_pnl"] for item in results]

    def prices(state):
        return [row[2] for row in state["fills"]]

    expected_mid = FEE_RATES[1] * shares[1]
    checks = {
        "all_replays_traded": all(len(item["state"]["fills"]) >= 4 for item in results),
        # Client-side: the configured rates reach the submitted payload.
        "rates_present_in_submitted_config": all(
            float(item["submitted"].get("equity_fee", -1)) == item["rate"]
            for item in results
        ),
        "slippage_present_in_submitted_config": (
            float(slipped_submitted.get("slippage", -1)) == 0.01
        ),
        "identical_share_count": len(set(shares)) == 1 and shares[0] == QTY * 6,
        "zero_rate_costs_nothing": abs(fees[0]) < 1e-6,
        "fees_increase_with_rate": fees[0] < fees[1] < fees[2],
        "net_pnl_falls_as_fees_rise": pnl[0] > pnl[1] > pnl[2],
        "fee_delta_equals_pnl_delta": (
            abs((pnl[0] - pnl[2]) - (fees[2] - fees[0])) < max(1.0, fees[2] * 0.02)
        ),
        "portfolio_fees_match_report": all(
            abs(item["state"]["portfolio"]["fees"] - item["report_fees"]) < 0.01
            for item in results
        ),
        "fees_match_sum_of_order_commission": all(
            abs(item["state"]["portfolio"]["fees"]
                - item["state"]["commission_total"]) < 0.01
            for item in results
        ),
        # Checked at every rate, not only the middle one: the middle rate is
        # also the default, so a fee model that ignored configuration entirely
        # would still satisfy a single-rate assertion.
        "per_share_rate_as_configured_at_every_rate": all(
            abs(fee - rate * count) < max(0.01, rate * count * 0.02)
            for rate, fee, count in zip(FEE_RATES, fees, shares)
        ),
        # Same strategy, same bars, only slippage changed: if the parameter is
        # wired to the fill model at all, the executed prices must move.
        "slippage_parameter_reaches_fill_model": prices(slipped_state) != prices(baseline["state"]),
    }
    checks.update(evidence_checks(baseline["run"], orders=6, trades=3))
    finish("t64_cost_model_sensitivity", checks,
           extra=(f"rates={FEE_RATES}; shares={shares}; fees={fees}; net_pnl={pnl}; "
                  f"commission_totals={[i['state']['commission_total'] for i in results]}; "
                  f"expected_mid_fee={round(expected_mid, 6)}; "
                  f"slipped_fees={slipped_state['portfolio']['fees']}; "
                  f"submitted_fees={[i['submitted'].get('equity_fee') for i in results]}; "
                  f"submitted_slippage={slipped_submitted.get('slippage')}; "
                  f"baseline_px={prices(baseline['state'])[:3]}; "
                  f"slipped_px={prices(slipped_state)[:3]}"))
