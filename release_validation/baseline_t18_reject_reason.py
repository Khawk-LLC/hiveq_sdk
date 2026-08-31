"""A C++ order rejection reaches the SDK callback with non-empty reason text."""
from pathlib import Path
import sys
from datetime import time
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading_types import OrderType

logger = _get_logger()


class SdkT18:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.sent = False
            self.state = {"bars": 0, "issued": 0, "rejects": []}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info("[START] will issue a deliberately late MOO order")

    def on_bar(self, ctx, event):
        self.state["bars"] += 1
        # Equity replay starts in premarket. Wait until the opening-auction
        # entry cutoff (09:28 ET) is definitely past before submitting MOO.
        if self.sent or event.data().time.time() < time(9, 30):
            return
        order = ctx.buy_order("AAPL", 1, order_type=OrderType.MOO)
        self.state["issued"] += int(order is not None)
        self.sent = True
        logger.info(f"[ORDER] late MOO issued={order is not None}")

    def on_order(self, ctx, event):
        if event.type != EventType.ORDER_REJECTED:
            return
        order = event.data()
        self.state["rejects"].append({"order_id": order.order_id,
            "symbol": order.symbol, "reason": order.reject_reason or ""})
        logger.info(f"[REJECT] id={order.order_id} reason={order.reject_reason}")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t18_reject_reason", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT18", type="SdkT18", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2026-04-01", end_date="2026-04-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}])
    state = completed_checkpoint(run, "t18_reject_reason")
    reasons = [row["reason"] for row in state["rejects"]]
    finish("t18_reject_reason", {
        "control_bars_delivered": state["bars"] > 0,
        "order_issued": state["issued"] == 1,
        "rejection_callback_fired": bool(state["rejects"]),
        "reject_reason_nonempty": bool(reasons) and all(reasons),
        "auction_cutoff_explained": any(
            "auction orders sent after cutoff time" in reason.lower()
            for reason in reasons
        ),
    }, extra=f"rejects={state['rejects']}")
