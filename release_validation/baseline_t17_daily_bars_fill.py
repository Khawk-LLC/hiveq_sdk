"""Daily-bar market orders fill at that day's close, not on the next day."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT17:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.state = {"bars": 0, "issued": 0, "filled": 0, "rejected": 0,
                          "canceled": 0, "price_errors": [], "time_errors": []}
            self.expected = {}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1d")
        logger.info("[START] subscribed to AAPL daily bars")

    def on_bar(self, ctx, event):
        bar = event.data()
        self.state["bars"] += 1
        logger.debug(f"[BAR] time={bar.time} close={bar.close}")
        for order in (ctx.buy_order("AAPL", 1), ctx.sell_order("AAPL", 1)):
            if order and order.order_id:
                self.expected[order.order_id] = {"close": float(bar.close), "date": str(bar.time.date())}
                self.state["issued"] += 1

    def on_order(self, ctx, event):
        order = event.data()
        if order.order_id not in self.expected:
            return
        if event.type == EventType.ORDER_FILLED:
            self.state["filled"] += 1
            expected = self.expected[order.order_id]
            if order.avg_px is None or abs(float(order.avg_px) - expected["close"]) > 1e-6:
                self.state["price_errors"].append([order.order_id, expected["close"], order.avg_px])
            # With the SDK's default equity session, the synthetic daily-bar
            # close event is stamped at session_end=18:30 ET. The invariant
            # under test is same-day execution at bar.close, never T+1.
            if (order.time is None or str(order.time.date()) != expected["date"]
                    or (order.time.hour, order.time.minute) != (18, 30)):
                self.state["time_errors"].append([order.order_id, expected["date"], str(order.time)])
        elif event.type == EventType.ORDER_REJECTED:
            self.state["rejected"] += 1
        elif event.type == EventType.ORDER_CANCELED:
            self.state["canceled"] += 1

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t17_daily_bars_fill", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT17", type="SdkT17", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2026-04-01", end_date="2026-04-07",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1d"]}])
    state = completed_checkpoint(run, "t17_daily_bars_fill")
    finish("t17_daily_bars_fill", {
        "daily_bars_delivered": state["bars"] > 0,
        "orders_issued": state["issued"] > 0,
        "every_order_filled": state["filled"] == state["issued"],
        "no_rejections": state["rejected"] == 0,
        "no_cancellations": state["canceled"] == 0,
        "filled_at_bar_close": not state["price_errors"],
        "filled_same_day_at_default_session_end": not state["time_errors"],
    }, extra=str(state))
