"""Validate AAPL eq_trades dispatch only from 14:00 through 16:30 ET for two days."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()


SESSION_START_MINUTE = 14 * 60
SESSION_END_MINUTE = 16 * 60 + 30
EXPECTED_DATES = ["2025-09-22", "2025-09-23"]


class AaplEqTradesTimeWindow:
    def __init__(self):
        self.state = {
            "trade_callbacks": 0,
            "dates": {},
            "first_trade": None,
            "last_trade": None,
            "outside_window": [],
            "wrong_symbol": 0,
        }
        self.current_day = None
        self.daily = None
        self.emitted_days = set()

    def emit_daily_summary(self, ctx):
        if not self.daily or self.daily["date"] in self.emitted_days:
            return
        ctx.add_event_log(
            f"AAPL eq_trades daily ticks {self.daily['date']}",
            sub_event_type="EQ_TRADES_DAILY_TICKS",
            symbol="AAPL",
            state_variable=dict(self.daily),
        )
        self.emitted_days.add(self.daily["date"])

    def on_start(self, ctx, event):
        self.emit_daily_summary(ctx)
        self.current_day = str(ctx.trading_day)
        self.daily = {
            "date": self.current_day,
            "tick_count": 0,
            "first_tick": None,
            "last_tick": None,
            "outside_window_count": 0,
        }
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)
        logger.info(
            f"[START] subscribed AAPL eq_trades for 14:00-16:30 ET; "
            f"trading_day={ctx.trading_day}"
        )

    def on_trade(self, ctx, event):
        trade = event.data()
        timestamp = trade.time
        timestamp_text = timestamp.isoformat()
        date_text = timestamp.strftime("%Y-%m-%d")
        minute = timestamp.hour * 60 + timestamp.minute
        logger.debug(f"[TRADE] symbol={trade.symbol} time={timestamp_text}")

        self.state["trade_callbacks"] += 1
        self.state["dates"][date_text] = self.state["dates"].get(date_text, 0) + 1
        if self.state["first_trade"] is None:
            self.state["first_trade"] = timestamp_text
        self.state["last_trade"] = timestamp_text
        self.daily["tick_count"] += 1
        if self.daily["first_tick"] is None:
            self.daily["first_tick"] = timestamp_text
        self.daily["last_tick"] = timestamp_text

        if trade.symbol != "AAPL":
            self.state["wrong_symbol"] += 1
        if not SESSION_START_MINUTE <= minute <= SESSION_END_MINUTE:
            self.daily["outside_window_count"] += 1
            logger.warning(f"[OUTSIDE_WINDOW] symbol={trade.symbol} time={timestamp_text}")
            if len(self.state["outside_window"]) < 20:
                self.state["outside_window"].append(timestamp_text)

    def on_stop(self, ctx, event):
        self.emit_daily_summary(ctx)
        emit_checkpoint(ctx, "aapl_eq_trades_time_window", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="AaplEqTradesTimeWindow",
            type="AaplEqTradesTimeWindow",
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
        backtest_config=BacktestConfig(
            session_start="14:00",
            session_end="16:30",
        ),
    )
    state = completed_checkpoint(run, "aapl_eq_trades_time_window")
    observed_dates = sorted(state["dates"])
    finish("aapl_eq_trades_time_window", {
        "trade_callbacks_present": state["trade_callbacks"] > 0,
        "both_days_dispatched": observed_dates == EXPECTED_DATES,
        "every_day_has_trades": all(state["dates"].get(day, 0) > 0 for day in EXPECTED_DATES),
        "only_aapl_dispatched": state["wrong_symbol"] == 0,
        "all_trades_within_1400_1630_ET": not state["outside_window"],
    }, extra=str(state))
