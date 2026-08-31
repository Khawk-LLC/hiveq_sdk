"""Successive subscription calls accumulate across symbols and asset types."""
from pathlib import Path
import sys
# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, evidence_checks, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, BacktestConfig

CONTROL = "MSFT"
CONTROL_QTY = 10.0


class SdkT02:
    def on_start(self, ctx, event):
        if not hasattr(self, "counts"):
            self.counts = {"MSFT": 0, "AAPL": 0, "VIX": 0, "SPX": 0}
            self.days = {key: set() for key in self.counts}
            self.logged_days = set()
            self.control = {"entry": 0.0, "exit": 0.0, "opened": False, "closed": False}
        self.day_counts = {key: 0 for key in self.counts}
        ctx.subscribe_trades(["MSFT"], asset_type=AssetType.EQUITY)
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)
        ctx.subscribe_index(["VIX"]); ctx.subscribe_index(["SPX"])

    def record(self, ctx, symbol, when):
        self.counts[symbol] += 1
        self.days[symbol].add(when)
        self.day_counts[symbol] = self.day_counts.get(symbol, 0) + 1
        # One persisted observation per stream per session: accumulation across
        # successive subscribe calls becomes readable from run.event_logs()
        # rather than only from the final in-strategy tally.
        key = (symbol, when)
        if key not in self.logged_days:
            self.logged_days.add(key)
            ctx.add_event_log(
                f"stream_first_tick {symbol} {when}",
                sub_event_type="STREAM_DAY_OPEN", symbol=symbol,
                state_variable={"symbol": symbol, "session": when,
                                "running_total": self.counts[symbol]},
            )

    def maybe_trade(self, ctx):
        # Control round trip so the case leaves real order/trade/position
        # evidence, per the suite's PASS contract, without disturbing the
        # accumulation counts under test.
        if not self.control["opened"]:
            if ctx.buy_order(CONTROL, CONTROL_QTY) is not None:
                self.control["opened"] = True
        elif not self.control["closed"] and self.counts[CONTROL] > 5000:
            if not ctx.has_open_order(CONTROL) and not ctx.is_flat(CONTROL):
                if ctx.close_position(CONTROL) is not None:
                    self.control["closed"] = True
    def on_trade(self, ctx, event):
        data = event.data()
        if data.symbol in self.counts:
            self.record(ctx, data.symbol, str(data.time.date()))
            if data.symbol == CONTROL:
                self.maybe_trade(ctx)

    def on_index_price(self, ctx, event):
        data = event.data()
        if data.symbol in self.counts:
            self.record(ctx, data.symbol, str(data.time.date()))

    def on_order(self, ctx, event):
        order = event.data()
        if order.symbol != CONTROL or "FILL" not in str(order.status).upper():
            return
        if float(order.filled_qty or 0) <= 0:
            return
        price = round(float(order.avg_px or 0), 6)
        side = "entry" if str(order.side).upper().endswith("BUY") else "exit"
        if not self.control[side]:
            self.control[side] = price
            ctx.add_event_log(
                f"control_{side} {CONTROL} @ {price}",
                sub_event_type="CONTROL_FILL", symbol=CONTROL,
                state_variable={"side": side, "price": price,
                                "quantity": float(order.filled_qty)},
            )
    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t02_successive_accumulate", {
            **self.counts, "days": {key: sorted(value) for key, value in self.days.items()},
            "control": dict(self.control),
        })

if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT02", type="SdkT02", symbols=["MSFT", "AAPL"])],
        symbols=["MSFT", "AAPL"], start_date="2026-07-01", end_date="2026-07-31",
        data_configs=[
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["eq_trades"]},
            {"type":"hiveq_historical","dataset":"HIVEQ_US_IND","schema":["indices_values"]},
        ], backtest_config=BacktestConfig(session_start="09:30", session_end="10:00"))
    s = completed_checkpoint(run, "t02_successive_accumulate")
    logs = run.event_logs()
    sub_types = set(logs["sub_event_type"].astype(str)) if len(logs) else set()
    day_open_rows = int((logs["sub_event_type"].astype(str) == "STREAM_DAY_OPEN").sum()) if len(logs) else 0
    control = s.get("control") or {}
    checks_extra = {
        "per_stream_day_logs_persisted": day_open_rows >= 4 * 20,
        "control_fill_logs_persisted": "CONTROL_FILL" in sub_types,
        "control_round_trip_filled": bool(control.get("entry")) and bool(control.get("exit")),
    }
    finish("t02_successive_accumulate", {
        "trades_first_call_MSFT":s["MSFT"]>0,
        "trades_second_call_AAPL":s["AAPL"]>0,
        "index_first_call_VIX":s["VIX"]>0,
        "index_second_call_SPX":s["SPX"]>0,
        "MSFT_daily_records":len(s["days"]["MSFT"])>=20,
        "AAPL_daily_records":len(s["days"]["AAPL"])>=20,
        "VIX_daily_records":len(s["days"]["VIX"])>=20,
        "SPX_daily_records":len(s["days"]["SPX"])>=20,
        **checks_extra,
        **evidence_checks(run, orders=2, trades=1),
    }, extra=f"day_open_rows={day_open_rows}, sub_types={sorted(sub_types)}, {s}")
