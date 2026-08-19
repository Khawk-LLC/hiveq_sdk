"""Measure AAPL eq_trades dispatch over the default equity session for two days."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()
EXPECTED_DATES = ["2025-09-22", "2025-09-23"]


class AaplEqTradesFullSession:
    def __init__(self):
        self.state = {
            "trade_callbacks": 0,
            "dates": {},
            "first_trade": None,
            "last_trade": None,
            "wrong_symbol": 0,
        }

    def on_start(self, ctx, event):
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)
        logger.info(
            f"[START] subscribed AAPL eq_trades using the default equity session; "
            f"trading_day={ctx.trading_day}"
        )

    def on_trade(self, ctx, event):
        trade = event.data()
        timestamp_text = trade.time.isoformat()
        date_text = trade.time.strftime("%Y-%m-%d")
        logger.debug(f"[TRADE] symbol={trade.symbol} time={timestamp_text}")

        self.state["trade_callbacks"] += 1
        self.state["dates"][date_text] = self.state["dates"].get(date_text, 0) + 1
        if self.state["first_trade"] is None:
            self.state["first_trade"] = timestamp_text
        self.state["last_trade"] = timestamp_text
        if trade.symbol != "AAPL":
            self.state["wrong_symbol"] += 1

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "aapl_eq_trades_full_session", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="AaplEqTradesFullSession",
            type="AaplEqTradesFullSession",
            symbols=["AAPL"],
        )],
        symbols=["AAPL"],
        start_date=EXPECTED_DATES[0],
        end_date=EXPECTED_DATES[-1],
        data_configs=[{
            "type": "hiveq_historical",
            "dataset": "HIVEQ_US_EQ",
            "schema": ["eq_trades"],
        }],
    )
    state = completed_checkpoint(run, "aapl_eq_trades_full_session")
    finish("aapl_eq_trades_full_session", {
        "trade_callbacks_present": state["trade_callbacks"] > 0,
        "both_days_dispatched": sorted(state["dates"]) == EXPECTED_DATES,
        "every_day_has_trades": all(state["dates"].get(day, 0) > 0 for day in EXPECTED_DATES),
        "only_aapl_dispatched": state["wrong_symbol"] == 0,
    }, extra=str(state))
