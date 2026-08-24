"""Ten-symbol MOO/MOC round trips fill on official auction prints."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import open_positions, completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading_types import OrderType

logger = _get_logger()
SYMBOLS = ["AAPL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "GOOGL", "JPM", "IBM", "BAC"]


class SdkT24:
    def on_start(self, ctx, event):
        self.state = {"trades": 0, "moo_placed": {}, "moc_placed": {},
            "moo_fills": {}, "moc_fills": {}, "rejects": []}
        self.moo_ids = {}; self.moc_ids = {}
        ctx.subscribe_trades(SYMBOLS, asset_type=AssetType.EQUITY)
        logger.info(f"[START] subscribed equity trades for {SYMBOLS}")
        # MOO orders must be resting before the opening-auction cutoff.  The
        # session starts at 04:00 ET, so submit them here rather than waiting
        # for each symbol's first trade (which may not arrive until 09:30 or
        # later and would make the order ineligible for the opening auction).
        for symbol in SYMBOLS:
            order = ctx.buy_order(symbol, 1.0, order_type=OrderType.MOO)
            if order:
                self.moo_ids[symbol] = order.order_id
                self.state["moo_placed"][symbol] = True
                logger.info(f"[MOO] submitted BUY 1 {symbol}")

    def on_trade(self, ctx, event):
        trade = event.data(); self.state["trades"] += 1
        symbol = str(trade.symbol)
        logger.debug(f"[TRADE] {symbol} time={trade.time} price={trade.price}")

    def on_order(self, ctx, event):
        order = event.data()
        if event.type == EventType.ORDER_FILLED:
            value = {"time": order.time.strftime("%H:%M") if order.time else None,
                     "price": order.avg_px, "qty": order.filled_qty}
            symbol = str(order.symbol)
            if self.moo_ids.get(symbol) == order.order_id:
                self.state["moo_fills"][symbol] = value
                logger.info(f"[MOO FILL] {symbol} {value}")
                close = ctx.sell_order(symbol, 1.0, order_type=OrderType.MOC)
                if close:
                    self.moc_ids[symbol] = close.order_id
                    self.state["moc_placed"][symbol] = True
                    logger.info(f"[MOC] submitted SELL 1 {symbol}")
            elif self.moc_ids.get(symbol) == order.order_id:
                self.state["moc_fills"][symbol] = value
                logger.info(f"[MOC FILL] {symbol} {value}")
        elif event.type == EventType.ORDER_REJECTED:
            self.state["rejects"].append({
                "symbol": str(order.symbol), "reason": order.reject_reason or ""
            })

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t24_auction_orders", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT24", type="SdkT24", symbols=SYMBOLS)],
        symbols=SYMBOLS, start_date="2025-09-19", end_date="2025-09-19",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["eq_trades"]}],
        backtest_config=BacktestConfig(session_start="04:00", session_end="18:30"))
    state = completed_checkpoint(run, "t24_auction_orders")
    positions = run.positions()
    finish("t24_auction_orders", {
        "trade_ticks_delivered": state["trades"] > 0,
        "ten_moo_orders_placed": set(state["moo_placed"]) == set(SYMBOLS),
        "ten_moo_filled_at_0930_ET": set(state["moo_fills"]) == set(SYMBOLS) and
            all(x["time"] == "09:30" for x in state["moo_fills"].values()),
        "ten_moc_orders_placed": set(state["moc_placed"]) == set(SYMBOLS),
        "ten_moc_filled_at_1600_ET": set(state["moc_fills"]) == set(SYMBOLS) and
            all(x["time"] == "16:00" for x in state["moc_fills"].values()),
        "auction_orders_not_rejected": not state["rejects"],
        "twenty_public_orders": len(run.orders()) >= 20,
        "twenty_public_fills": len(run.fills()) >= 20,
        "ten_round_trip_trades": len(run.trades()) >= 10,
        "all_symbols_flat": open_positions(positions).empty,
    }, extra=str(state))
