"""A 0DTE option snapshot can drive a tick-valid limit entry and exit with report evidence."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.trading.price_utils import adjust_tick_size
from hiveq.flow.trading_types import OrderType


class SdkT40:
    def on_start(self, ctx, event):
        self.contract = ""
        self.entry_id = ""
        self.exit_id = ""
        self.latest = {}
        self.state = {
            "snaps": 0,
            "quoted_snaps": 0,
            "entry_submitted": False,
            "entry_filled": False,
            "exit_submitted": False,
            "exit_filled": False,
            "fill_prices": [],
            "final_position": None,
        }
        ctx.subscribe_option_snaps(
            "SPXW", option_type="C", expiration_type="0dte", interval="1s"
        )

    def on_snap(self, ctx, event):
        snap = event.data()
        self.state["snaps"] += 1
        if snap.bid_px is None or snap.ask_px is None or snap.ask_px <= 0:
            return
        self.state["quoted_snaps"] += 1
        self.latest[snap.chain] = (float(snap.bid_px), float(snap.ask_px))
        if not self.entry_id:
            self.contract = snap.chain
            limit = adjust_tick_size(self.contract, float(snap.ask_px))
            order = ctx.buy_order(
                self.contract, 1.0,
                order_type=OrderType.LIMIT, limit_price=limit
            )
            if order is not None:
                self.entry_id = order.order_id
                self.state["entry_submitted"] = True
        elif self.state["entry_filled"] and not self.exit_id and snap.chain == self.contract:
            limit = adjust_tick_size(self.contract, float(snap.bid_px))
            order = ctx.sell_order(
                self.contract, 1.0, order_type=OrderType.LIMIT, limit_price=limit
            )
            if order is not None:
                self.exit_id = order.order_id
                self.state["exit_submitted"] = True

    def on_order(self, ctx, event):
        order = event.data()
        if not order.is_filled:
            return
        if order.order_id == self.entry_id:
            self.state["entry_filled"] = True
        elif order.order_id == self.exit_id:
            self.state["exit_filled"] = True
        else:
            return
        self.state["fill_prices"].append(float(order.avg_px or order.last_px or 0.0))

    def on_stop(self, ctx, event):
        self.state["contract"] = self.contract
        self.state["final_position"] = (
            float(ctx.net_position(self.contract)) if self.contract else None
        )
        emit_checkpoint(ctx, "t40_option_order_fill", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT40", type="SdkT40", symbols=["SPXW"])],
        symbols=["SPXW"],
        start_date="2026-04-24",
        end_date="2026-04-24",
        data_configs=[{
            "type": "hiveq_historical",
            "dataset": "HIVEQ_US_OPT",
            "schema": ["snaps_1s"],
        }],
        backtest_config=BacktestConfig(session_start="15:30", session_end="16:00"),
    )
    state = completed_checkpoint(run, "t40_option_order_fill")
    orders = run.orders()
    trades = run.trades()
    finish("t40_option_order_fill", {
        "snap_data_present": state["snaps"] > 0 and state["quoted_snaps"] > 0,
        "occ_contract_identified": bool(state["contract"]),
        "option_limit_entry_filled": state["entry_submitted"] and state["entry_filled"],
        "exit_limit_filled": state["exit_submitted"] and state["exit_filled"],
        "position_closed": state["final_position"] == 0.0,
        "order_results_present": orders is not None and len(orders) >= 2,
        "round_trip_trade_present": trades is not None and len(trades) >= 1,
    }, extra=str(state))
