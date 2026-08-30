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
SYMBOLS = ["AAPL"]
PRIMARY_MARKET = {
    "AAPL": "NASDAQ", "MSFT": "NASDAQ", "AMZN": "NASDAQ",
    "META": "NASDAQ", "NVDA": "NASDAQ", "TSLA": "NASDAQ",
    "GOOGL": "NASDAQ", "JPM": "NYSE", "IBM": "NYSE", "BAC": "NYSE",
}


class SdkT24Single:
    def on_start(self, ctx, event):
        self.state = {"trades": 0, "moo_placed": {}, "moc_placed": {},
            "moo_fills": {}, "moc_fills": {}, "rejects": [],
            "moo_submitted_at": {}, "moc_submitted_at": {},
            # Terminal state per auction order: an auction order that neither
            # fills nor is rejected is canceled, and when that happened is the
            # evidence that separates "no auction print for this symbol" from
            # "the order was never eligible".
            "moo_terminal": {}, "moc_terminal": {}}
        self.moo_ids = {}; self.moc_ids = {}
        self.moo_sent = False
        self.moc_sent = False
        ctx.subscribe_trades(SYMBOLS, asset_type=AssetType.EQUITY)
        logger.info(f"[START] subscribed equity trades for {SYMBOLS}")

    def _submit_moo(self, ctx, now):
        for symbol in SYMBOLS:
            order = ctx.buy_order(
                symbol, 1.0, order_type=OrderType.MOO,
                market_center=PRIMARY_MARKET[symbol],
            )
            if order:
                self.moo_ids[symbol] = order.order_id
                self.state["moo_placed"][symbol] = True
                self.state["moo_submitted_at"][symbol] = now.isoformat()
                logger.info(f"[MOO] submitted BUY 1 {symbol}")

    def _submit_moc(self, ctx, now):
        for symbol in self.state["moo_fills"]:
            if symbol in self.state["moc_placed"]:
                continue
            close = ctx.sell_order(
                symbol, 1.0, order_type=OrderType.MOC,
                market_center=PRIMARY_MARKET[symbol],
            )
            if close:
                self.moc_ids[symbol] = close.order_id
                self.state["moc_placed"][symbol] = True
                self.state["moc_submitted_at"][symbol] = now.isoformat()
                logger.info(f"[MOC] submitted SELL 1 {symbol}")

    def on_trade(self, ctx, event):
        trade = event.data(); self.state["trades"] += 1
        symbol = str(trade.symbol)
        # Auctions are scheduled off the tick clock, not a timer. The engine
        # stamps order entry with the time market data has reached, so a timer
        # firing on the strategy clock can enter an order the broker then stamps
        # much later -- past the open-auction cutoff, or in a multi-session run
        # after the previous session's close, with no tick to anchor it. Reading
        # the schedule from the trades themselves keeps both clocks the same one.
        moment = trade.time
        if moment is not None:
            minutes = moment.hour * 60 + moment.minute
            if not self.moo_sent and minutes >= 7 * 60 + 50:
                self.moo_sent = True
                self._submit_moo(ctx, moment)
            # Noon is before the requested 15:00 cutoff and also precedes the
            # 13:00 close of a shortened session.
            if not self.moc_sent and minutes >= 12 * 60:
                self.moc_sent = True
                self._submit_moc(ctx, moment)
        logger.debug(f"[TRADE] {symbol} time={trade.time} price={trade.price}")

    def on_order(self, ctx, event):
        order = event.data()
        status = str(order.status).upper().rsplit(".", 1)[-1]
        if status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
            symbol = str(order.symbol)
            stamp = order.time.strftime("%H:%M:%S") if order.time else None
            if self.moo_ids.get(symbol) == order.order_id:
                self.state["moo_terminal"][symbol] = [status, stamp]
            elif self.moc_ids.get(symbol) == order.order_id:
                self.state["moc_terminal"][symbol] = [status, stamp]
        if event.type == EventType.ORDER_FILLED:
            value = {"time": order.time.strftime("%H:%M") if order.time else None,
                     "price": order.avg_px, "qty": order.filled_qty}
            symbol = str(order.symbol)
            if self.moo_ids.get(symbol) == order.order_id:
                self.state["moo_fills"][symbol] = value
                logger.info(f"[MOO FILL] {symbol} {value}")
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
    # Isolation probe: identical to baseline_t24 except that it runs one symbol.
    # Default log levels (engine WARNING, OMS error) -- debug logging filled the
    # container's capped log tmpfs and killed the run with an uncaught
    # Poco::WriteFileException, reported as exit 139.
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT24Single", type="SdkT24Single", symbols=SYMBOLS)],
        symbols=SYMBOLS, start_date="2026-08-12", end_date="2026-08-12",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["eq_trades"]}],
        backtest_config=BacktestConfig(session_start="04:00", session_end="16:30"))
    state = completed_checkpoint(run, "t24_auction_orders")
    positions = run.positions()
    # A symbol that does not fill is a failure, not an exemption: MOO/MOC round
    # trips are the contract under test. Sessions whose tick data is missing are
    # excluded by choosing the window (see the module docstring), not by
    # softening the assertions.
    moo_fills, moc_fills = state["moo_fills"], state["moc_fills"]
    terminal = state["moo_terminal"]
    finish("t24_auction_orders", {
        "trade_ticks_delivered": state["trades"] > 0,
        "ten_moo_orders_placed": set(state["moo_placed"]) == set(SYMBOLS),
        "ten_moo_filled_at_0930_ET": set(moo_fills) == set(SYMBOLS) and
            all(x["time"] == "09:30" for x in moo_fills.values()),
        "ten_moc_orders_placed": set(state["moc_placed"]) == set(SYMBOLS),
        "ten_moc_filled_at_1600_ET": set(moc_fills) == set(SYMBOLS) and
            all(x["time"] == "16:00" for x in moc_fills.values()),
        "every_moo_resolved": set(terminal) == set(SYMBOLS) and all(
            status in {"FILLED", "CANCELED"} for status, _ in terminal.values()),
        "moo_submitted_before_0800_ET": set(state["moo_submitted_at"]) == set(SYMBOLS) and
            all(value[11:16] < "08:00" for value in state["moo_submitted_at"].values()),
        "moc_submitted_before_1500_ET": set(state["moc_submitted_at"]) == set(SYMBOLS) and
            all(value[11:16] < "15:00" for value in state["moc_submitted_at"].values()),
        "auction_orders_not_rejected": not state["rejects"],
        "twenty_public_orders": len(run.orders()) >= 20,
        "twenty_public_fills": len(run.fills()) >= 20,
        "ten_round_trip_trades": len(run.trades()) >= 10,
        "all_symbols_flat": open_positions(positions).empty,
    }, extra=str(state))
