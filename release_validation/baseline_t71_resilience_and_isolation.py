"""A bad symbol and a throwing callback must not take the run down with them.

t32 proves a callback exception stays visible in the logs. It does not prove the
run survives one: whether the remaining callbacks still fire, whether the other
symbols keep receiving data, and whether the run still reaches a terminal state
with readable results. Those are the properties that decide whether one bad
strategy line costs a user their whole backtest.

So one subscription names a symbol that does not exist, one callback raises on a
fixed schedule, and orders are aimed at a symbol that was never subscribed --
while a control symbol trades normally throughout and has to come out intact.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (                                               # noqa: E402
    completed_checkpoint,
    emit_checkpoint,
    evidence_checks,
    finish,
)

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402

CONTROL = "AAPL"
NEIGHBOUR = "MSFT"
MISSING = "ZZZZNOTREAL"
UNSUBSCRIBED = "TSLA"
# Bars on which on_bar deliberately raises.
THROW_BARS = (7, 19, 33)


class SdkT71:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = {}
            self.state = {
                "bars": {}, "throws_attempted": 0, "throws_survived": 0,
                "after_last_throw": 0, "missing_symbol_bars": 0,
                "unsubscribed_order": "", "fills": 0, "final": {},
                "subscribe_error": "",
            }
        # A symbol that does not exist, subscribed alongside two that do: the
        # real ones must keep flowing regardless of what the bad one does.
        try:
            ctx.subscribe_bars([CONTROL, NEIGHBOUR, MISSING],
                               asset_type=AssetType.EQUITY, interval="1m")
        except Exception as exc:                           # noqa: BLE001
            self.state["subscribe_error"] = str(exc)[:200]
            ctx.subscribe_bars([CONTROL, NEIGHBOUR],
                               asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        symbol = bar.symbol
        self.bars[symbol] = self.bars.get(symbol, 0) + 1
        count = self.bars[symbol]
        if symbol == MISSING:
            self.state["missing_symbol_bars"] += 1
            return
        if symbol != CONTROL:
            return

        # Count bars seen after the last scheduled throw: if the engine stopped
        # dispatching to this strategy, this stays at zero.
        if count > max(THROW_BARS):
            self.state["after_last_throw"] += 1

        if count == 3:
            ctx.buy_order(CONTROL, 10.0)
        elif count == 5 and not self.state["unsubscribed_order"]:
            # An order on a symbol this strategy never subscribed to.
            try:
                order = ctx.buy_order(UNSUBSCRIBED, 1.0)
            except Exception as exc:                       # noqa: BLE001
                self.state["unsubscribed_order"] = f"raised: {str(exc)[:120]}"
            else:
                self.state["unsubscribed_order"] = (
                    f"accepted: {order is not None}"
                )
        elif count == 50 and not ctx.has_open_order(CONTROL):
            ctx.close_position(CONTROL)

        if count in THROW_BARS:
            self.state["throws_attempted"] += 1
            raise RuntimeError(f"t71 deliberate failure on {symbol} bar {count}")

    def on_order(self, ctx, event):
        order = event.data()
        if "FILL" in str(order.status).upper() and float(order.filled_qty or 0) > 0:
            self.state["fills"] += 1

    def on_stop(self, ctx, event):
        self.state["bars"] = dict(self.bars)
        self.state["throws_survived"] = self.state["throws_attempted"]
        self.state["final"] = {
            "control_position": float(ctx.net_position(CONTROL)),
            "equity": round(ctx.portfolio().equity, 4),
            "realized": round(ctx.portfolio().realized_pnl(), 4),
        }
        emit_checkpoint(ctx, "t71_resilience_and_isolation", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT71", type="SdkT71",
                                         symbols=[CONTROL, NEIGHBOUR, MISSING])],
        symbols=[CONTROL, NEIGHBOUR, MISSING],
        start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="11:30"),
    )
    state = completed_checkpoint(run, "t71_resilience_and_isolation")
    bars = state["bars"]
    status = run.status()
    logs = run.event_logs()

    checks = {
        "run_reached_terminal_state": bool(status) and (
            status.get("is_final") or str(status.get("status", "")).lower()
            in {"completed", "done", "stopped"}
        ),
        "control_symbol_received_data": bars.get(CONTROL, 0) > 100,
        "neighbour_symbol_received_data": bars.get(NEIGHBOUR, 0) > 100,
        "missing_symbol_delivered_nothing": bars.get(MISSING, 0) == 0,
        "missing_symbol_did_not_block_others": (
            bars.get(CONTROL, 0) > 0 and bars.get(NEIGHBOUR, 0) > 0
        ),
        "callback_raised_as_scheduled": state["throws_attempted"] == len(THROW_BARS),
        # The core resilience property: dispatch continued after every throw.
        "dispatch_survived_every_throw": state["after_last_throw"] > 20,
        "unsubscribed_order_handled_cleanly": bool(state["unsubscribed_order"]),
        "control_round_trip_completed": state["fills"] >= 2,
        "control_flat_at_end": state["final"]["control_position"] == 0.0,
        "results_readable_after_failures": len(logs) >= 1,
    }
    checks.update(evidence_checks(run, orders=2, trades=1))
    finish("t71_resilience_and_isolation", checks,
           extra=(f"bars={bars}; throws={state['throws_attempted']}; "
                  f"bars_after_last_throw={state['after_last_throw']}; "
                  f"missing_bars={state['missing_symbol_bars']}; "
                  f"unsubscribed_order={state['unsubscribed_order']!r}; "
                  f"subscribe_error={state['subscribe_error']!r}; "
                  f"fills={state['fills']}; final={state['final']}; "
                  f"status={status.get('status')}"))
