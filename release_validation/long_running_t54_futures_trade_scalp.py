"""Converted .164 ES scalp: futures quotes, brackets, sessions, and rollover.

The window spans the September ES roll, because the validation requires a
rollover to have happened -- a stretch of ordinary sessions would cost far
more `fut_trades` volume and still prove less.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import finish_validation, open_positions as open_position_rows, export_run_artifacts, wait_for_final

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.trading_types import OrderType

SYMBOL = "ES.v.0"


class SdkT54FuturesTradeScalp:
    def __init__(self):
        self.state = "IDLE"
        self.entry_id = None
        self.take_profit_id = None
        self.stop_id = None
        self.entry_price = None
        self.current_session = None
        self.day_ended = False
        self.quotes = 0
        self.in_window = 0
        self.entries = 0
        self.exits = 0
        self.cancels = 0
        self.rollovers = []
        self.last_quote_second = None
        self.cleanup_symbol = None
        self.flatten_order_id = None

    def on_start(self, ctx, event):
        ctx.subscribe_futures_trades(continuous=SYMBOL)
        ctx.add_event_log("ES.v.0 futures scalp started", sub_event_type="SCALP_START")

    def on_quote(self, ctx, event):
        quote = event.data()
        self.quotes += 1
        if not quote.bid_price or not quote.ask_price or quote.time is None:
            return
        now = quote.time
        quote_second = now.replace(microsecond=0)
        if quote_second == self.last_quote_second:
            return
        self.last_quote_second = quote_second
        session = now.date() if now.hour >= 18 else (now - __import__("datetime").timedelta(days=1)).date()
        if session != self.current_session:
            self.current_session = session
            self.day_ended = False
            ctx.add_event_log(str(session), sub_event_type="SCALP_SESSION")
        minute = now.hour * 60 + now.minute
        if 16 * 60 <= minute < 18 * 60:
            if not self.day_ended:
                self._end_day(ctx, quote.symbol)
            self._drive_cleanup(ctx, quote.symbol)
            return
        if not 12 * 60 <= minute < 16 * 60 or self.day_ended:
            return
        self.in_window += 1
        if self.state == "IDLE" and self.entry_id is None and ctx.is_flat(quote.symbol):
            tick = float(ctx.instrument(SYMBOL).min_tick or 0.25)
            mid = (float(quote.bid_price) + float(quote.ask_price)) / 2.0
            price = round((mid - 1.0) / tick) * tick
            order = ctx.buy_order(
                quote.symbol, 1.0, order_type=OrderType.LIMIT,
                limit_price=price, time_in_force="GTC",
            )
            if order is not None:
                self.entry_id = order.order_id
                self.state = "BUY_PENDING"
                ctx.add_event_log(
                    f"BUY_LIMIT {quote.symbol} @{price}", sub_event_type="SCALP_ENTRY_ORDER",
                    symbol=quote.symbol, state_variable={"contract": quote.symbol,
                                                        "limit_price": price, "mid": mid},
                )

    def on_order(self, ctx, event):
        order = event.data()
        status = str(order.status).upper()
        if order.order_id == self.entry_id and order.is_filled:
            self.entry_price = float(order.avg_px)
            self.entry_id = None
            self.entries += 1
            tick = float(ctx.instrument(SYMBOL).min_tick or 0.25)
            tp = ctx.sell_order(
                order.symbol, 1.0, order_type=OrderType.LIMIT,
                limit_price=round((self.entry_price + 1.0) / tick) * tick,
                time_in_force="GTC",
            )
            stop = ctx.sell_order(
                order.symbol, 1.0, order_type=OrderType.STOP,
                stop_price=round((self.entry_price - 5.0) / tick) * tick,
                time_in_force="GTC",
            )
            self.take_profit_id = None if tp is None else tp.order_id
            self.stop_id = None if stop is None else stop.order_id
            self.state = "IN_POSITION"
            ctx.add_event_log(
                f"ENTRY_FILL {order.symbol} @{self.entry_price}",
                sub_event_type="SCALP_ENTRY_FILL", symbol=order.symbol,
                state_variable={"contract": order.symbol, "quantity": 1.0,
                                "entry_price": self.entry_price,
                                "take_profit_id": self.take_profit_id,
                                "stop_id": self.stop_id},
            )
        elif self.state == "IN_POSITION" and order.is_filled and order.order_id in {
                self.take_profit_id, self.stop_id}:
            sibling = self.stop_id if order.order_id == self.take_profit_id else self.take_profit_id
            if sibling:
                self.cancels += int(bool(ctx.cancel_order(sibling)))
            self.exits += 1
            ctx.add_event_log(
                f"EXIT_FILL {order.symbol} @{order.avg_px}", sub_event_type="SCALP_EXIT_FILL",
                symbol=order.symbol,
                state_variable={"contract": order.symbol, "filled_order_id": order.order_id,
                                "canceled_sibling_id": sibling},
            )
            self.state = "IDLE"
            self.entry_price = None
            self.take_profit_id = None
            self.stop_id = None
        elif order.order_id == self.entry_id and ("CANCEL" in status or "REJECT" in status):
            self.entry_id = None
            self.state = "IDLE"
        if order.order_id == self.flatten_order_id and order.is_filled:
            self.flatten_order_id = None
            self.cleanup_symbol = None
            self.state = "IDLE"

    def _end_day(self, ctx, symbol):
        self.day_ended = True
        self.cleanup_symbol = symbol
        self.state = "CLEANUP"
        ctx.cancel_all_orders(symbol)
        self.entry_id = self.take_profit_id = self.stop_id = None
        self.entry_price = None
        ctx.add_event_log("16:00 ET cleanup", sub_event_type="SCALP_EOD", symbol=symbol)

    def _drive_cleanup(self, ctx, symbol):
        if self.cleanup_symbol != symbol or self.flatten_order_id is not None:
            return
        if ctx.has_open_order(symbol):
            return
        if ctx.is_flat(symbol):
            self.cleanup_symbol = None
            self.state = "IDLE"
            return
        order = ctx.close_position(symbol)
        if order is not None:
            self.flatten_order_id = order.order_id
            ctx.add_event_log(
                f"EOD_FLATTEN {symbol} quantity={ctx.quantity(symbol)}",
                sub_event_type="SCALP_EOD_FLATTEN", symbol=symbol,
            )

    def on_rollover(self, ctx, event):
        data = event.data()
        row = {"continuous_symbol": data.continuous_symbol,
               "prev_contract": data.prev_contract,
               "current_contract": data.current_contract,
               "event_ts_event": int(event.ts_event)}
        self.rollovers.append(row)
        ctx.add_event_log(
            f"ROLLOVER_DONE {data.prev_contract}->{data.current_contract}",
            sub_event_type="ROLLOVER_DONE", state_variable=row,
        )

    def on_stop(self, ctx, event):
        ctx.add_event_log(
            "futures scalp summary", sub_event_type="SCALP_SUMMARY",
            state_variable={"quotes": self.quotes, "in_window": self.in_window,
                            "entries": self.entries, "exits": self.exits,
                            "sibling_cancel_requests": self.cancels,
                            "rollovers": self.rollovers},
        )


def _state(value):
    # In-process runs serialize state_variables with orjson, so this column
    # holds bytes; returning it undecoded made the scalp summary unreadable.
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    return json.loads(value or "{}") if isinstance(value, str) else value


def analyze(run):
    orders = run.orders()
    positions = run.positions()
    events = run.event_logs()
    summaries = events[events["sub_event_type"] == "SCALP_SUMMARY"]
    summary = {} if summaries.empty else _state(summaries.iloc[-1]["state_variables"])
    open_positions = open_position_rows(positions)
    result = {
        "symbol": SYMBOL, "orders": len(orders), "trades": len(run.trades()),
        "all_orders_terminal": bool(len(orders)) and bool(
            orders["status"].astype(str).isin(["FILLED", "CANCELED", "CANCELLED"]).all()
        ),
        "open_positions": len(open_positions), "summary": summary,
    }
    result["passed"] = bool(
        summary.get("quotes", 0) > 0 and summary.get("in_window", 0) > 0
        and summary.get("entries", 0) > 0 and summary.get("exits", 0) > 0
        and len(summary.get("rollovers", [])) > 0 and len(open_positions) == 0
    )
    return result


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT54FuturesTradeScalp", type="SdkT54FuturesTradeScalp",
            symbols=[SYMBOL],
        )],
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["fut_trades"]}],
        backtest_config=BacktestConfig(
            symbols=[SYMBOL], start_date="2025-09-08", end_date="2025-09-19",
            initial_capital=1_000_000.0, session_start="18:00", session_end="17:00",
            enable_auto_rollover=True, auto_flatten_at_close=False,
        ),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}", flush=True)
    wait_for_final(run, timeout=7200.0)
    validation = analyze(run)
    artifacts = export_run_artifacts(run, validation=validation)
    print(json.dumps(validation, indent=2), flush=True)
    print(f"run_artifacts={artifacts}", flush=True)
    finish_validation("t54_futures_trade_scalp", validation)
