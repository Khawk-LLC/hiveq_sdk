"""Python-side order validation rejects malformed orders before broker dispatch."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading.price_utils import adjust_tick_size
from hiveq.flow.trading_types import OrderType

logger = _get_logger()


class SdkT19:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.done = False
            self.state = {"bars": 0, "errors": {"off_tick": [], "invalid_qty": [],
                "missing_limit": [], "missing_stop": []}, "raw": 100.123,
                "rounded": None, "rounded_accepted": False, "broker_rejections": []}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info("[START] subscribed for order-gate validation")

    def capture(self, key, callback):
        try:
            callback()
        except ValueError as exc:
            self.state["errors"][key].append(str(exc))
            logger.info(f"[EXPECTED/{key}] {exc}")

    def on_bar(self, ctx, event):
        self.state["bars"] += 1
        if self.done:
            return
        self.done = True
        self.capture("off_tick", lambda: ctx.buy_order("AAPL", 1,
            order_type=OrderType.LIMIT, limit_price=self.state["raw"]))
        rounded = adjust_tick_size("AAPL", self.state["raw"])
        self.state["rounded"] = rounded
        try:
            self.state["rounded_accepted"] = ctx.buy_order("AAPL", 1,
                order_type=OrderType.LIMIT, limit_price=rounded) is not None
        except Exception as exc:
            self.state["errors"]["off_tick"].append(f"rounded order failed: {exc}")
        self.capture("invalid_qty", lambda: ctx.buy_order("AAPL", 0))
        self.capture("missing_limit", lambda: ctx.buy_order("AAPL", 1, order_type=OrderType.LIMIT))
        self.capture("missing_stop", lambda: ctx.buy_order("AAPL", 1, order_type=OrderType.STOP))

    def on_order(self, ctx, event):
        if event.type == EventType.ORDER_REJECTED:
            order = event.data()
            self.state["broker_rejections"].append(order.reject_reason or "")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t19_order_gate", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT19", type="SdkT19", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2026-04-01", end_date="2026-04-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}])
    state = completed_checkpoint(run, "t19_order_gate")
    errors = state["errors"]
    finish("t19_order_gate", {
        "control_bars_delivered": state["bars"] > 0,
        "off_tick_rejected_locally": any("REJECT/OFF_TICK" in e for e in errors["off_tick"]),
        "invalid_qty_rejected_locally": any("REJECT/INVALID_QTY" in e for e in errors["invalid_qty"]),
        "missing_limit_rejected_locally": any("REJECT/MISSING_LIMIT_PRICE" in e for e in errors["missing_limit"]),
        "missing_stop_rejected_locally": any("REJECT/MISSING_STOP_PRICE" in e for e in errors["missing_stop"]),
        "tick_helper_rounded": abs(state["rounded"] - 100.12) < 1e-9,
        "rounded_order_accepted": state["rounded_accepted"],
        "malformed_orders_never_reached_broker": not state["broker_rejections"],
    }, extra=str(state))
