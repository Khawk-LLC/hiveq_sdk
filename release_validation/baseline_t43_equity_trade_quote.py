"""One equity eq_trades source drives both trade and quote callbacks with valid payloads."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


class SdkT43:
    def on_start(self, ctx, event):
        self.state = {
            "trades": 0, "quotes": 0, "volume": 0.0,
            "bad_trade_payloads": 0, "bad_quote_payloads": 0,
            "locked_quotes": 0, "crossed_quotes": 0, "crossed_samples": [],
            "quote_samples": [], "aggressors": [], "symbols": [],
        }
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)
        ctx.subscribe_quotes(["AAPL"], asset_type=AssetType.EQUITY)

    def on_trade(self, ctx, event):
        trade = event.data()
        self.state["trades"] += 1
        self.state["volume"] += float(trade.size)
        if trade.symbol != "AAPL" or trade.price <= 0 or trade.size <= 0 or trade.ts_event <= 0:
            self.state["bad_trade_payloads"] += 1
        side = str(trade.aggressor_side)
        if side not in self.state["aggressors"]:
            self.state["aggressors"].append(side)
        if trade.symbol not in self.state["symbols"]:
            self.state["symbols"].append(trade.symbol)

    def on_quote(self, ctx, event):
        quote = event.data()
        self.state["quotes"] += 1
        if (
            quote.symbol != "AAPL" or quote.bid_price <= 0 or quote.ask_price <= 0
            or quote.bid_size < 0 or quote.ask_size < 0 or quote.ts_event <= 0
        ):
            self.state["bad_quote_payloads"] += 1
        sample = {
            "row": self.state["quotes"], "symbol": quote.symbol,
            "bid": float(quote.bid_price), "ask": float(quote.ask_price),
            "bid_size": float(quote.bid_size), "ask_size": float(quote.ask_size),
            "spread": float(quote.spread), "ts_event": int(quote.ts_event),
        }
        if quote.bid_price == quote.ask_price:
            self.state["locked_quotes"] += 1
        elif quote.bid_price > quote.ask_price:
            self.state["crossed_quotes"] += 1
            if len(self.state["crossed_samples"]) < 10:
                self.state["crossed_samples"].append(sample)
        if self.state["quotes"] == 1 or self.state["quotes"] % 500 == 0:
            self.state["quote_samples"].append(sample)

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t43_equity_trade_quote", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT43", type="SdkT43", symbols=["AAPL"])],
        symbols=["AAPL"],
        start_date="2025-09-23",
        end_date="2025-09-23",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:00"),
    )
    state = completed_checkpoint(run, "t43_equity_trade_quote")
    finish("t43_equity_trade_quote", {
        "trade_callbacks_present": state["trades"] > 0,
        "quote_callbacks_present": state["quotes"] > 0,
        "positive_aggregate_volume": state["volume"] > 0,
        "trade_payloads_valid": state["bad_trade_payloads"] == 0,
        "quote_payloads_valid": state["bad_quote_payloads"] == 0,
        "quote_samples_recorded": bool(state["quote_samples"]),
        "requested_symbol_only": state["symbols"] == ["AAPL"],
        "aggressor_field_present": bool(state["aggressors"]),
    }, extra=str(state))
