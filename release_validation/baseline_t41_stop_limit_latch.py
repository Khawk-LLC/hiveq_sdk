"""A triggered STOP_LIMIT remains active as a limit order after price recrosses its stop."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading_types import OrderType

logger = _get_logger()


class SdkT41:
    def on_start(self, ctx, event):
        self.entry_id = ""
        self.stop_limit_id = ""
        self.entry_filled = False
        self.state = {
            "submitted": False,
            "filled": False,
            "fill_price": 0.0,
            "gap_bar_seen": False,
            "final_position": 0.0,
            "rejects": [],
        }
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info("[T41] subscribed to AAPL 1m bars")

    def on_bar(self, ctx, event):
        bar = event.data()
        minute = bar.time.strftime("%H:%M")
        logger.debug(
            f"[T41] bar={minute} open={bar.open} high={bar.high} "
            f"low={bar.low} close={bar.close} entry_filled={self.entry_filled}"
        )

        if minute == "09:30" and not self.entry_id:
            order = ctx.buy_order("AAPL", 1.0)
            if order is not None:
                self.entry_id = order.order_id
                logger.info("[T41] submitted entry BUY AAPL qty=1")

        # The following 09:40 bar trades down to 197.04, through the 197.10
        # stop and below the 197.05 sell limit, then returns to 197.15. The
        # order may fill on that return only if its trigger remains latched.
        if minute == "09:39" and self.entry_filled and not self.stop_limit_id:
            order = ctx.sell_order(
                "AAPL",
                1.0,
                order_type=OrderType.STOP_LIMIT,
                stop_price=197.10,
                limit_price=197.05,
                time_in_force="GTC",
            )
            if order is not None:
                self.stop_limit_id = order.order_id
                self.state["submitted"] = True
                logger.info(
                    "[T41] submitted sell STOP_LIMIT AAPL qty=1 stop=197.10 limit=197.05"
                )

        if minute == "09:40":
            self.state["gap_bar_seen"] = (
                float(bar.low) < 197.05 and float(bar.close) > 197.10
            )
            logger.debug(f"[T41] gap-and-return condition={self.state['gap_bar_seen']}")

    def on_order(self, ctx, event):
        order = event.data()
        fill_price = float(order.last_fill.last_px) if order.last_fill is not None else 0.0
        logger.info(
            f"[T41] order={order.order_id} status={order.status} "
            f"filled={order.is_filled} fill_price={fill_price}"
        )
        if order.order_id == self.entry_id and order.is_filled:
            self.entry_filled = True
        if order.order_id == self.stop_limit_id:
            if "REJECT" in str(order.status).upper():
                self.state["rejects"].append(str(order.reject_reason or ""))
            if order.is_filled:
                self.state["filled"] = True
                self.state["fill_price"] = fill_price

    def on_stop(self, ctx, event):
        self.state["final_position"] = float(ctx.net_position("AAPL"))
        emit_checkpoint(ctx, "t41_stop_limit_latch", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT41", type="SdkT41", symbols=["AAPL"])],
        symbols=["AAPL"],
        start_date="2025-04-04",
        end_date="2025-04-04",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="09:41"),
    )
    state = completed_checkpoint(run, "t41_stop_limit_latch")
    finish("t41_stop_limit_latch", {
        "stop_limit_submitted": state["submitted"],
        "gap_beyond_limit_then_recross_stop": state["gap_bar_seen"],
        "trigger_remained_latched": state["filled"],
        "filled_at_valid_limit_price": state["fill_price"] >= 197.05,
        "no_rejection": not state["rejects"],
        "position_flat": state["final_position"] == 0.0,
    }, extra=str(state))
