"""on_param_change fires once per real change, in both documented shapes.

Two strategies in one run, each changing its own params through
ctx.strategy_config.params (the same bridge path a set_params push lands on):

  SdkT86Event  declares on_param_change(self, ctx, event)  -- like every callback
  SdkT86Args   declares on_param_change(self, name, old, new)

Each makes two real changes (size 100 -> 125, threshold 0.5 -> 0.75) and one
write of a value already held (size = 125), which must not fire.

Regressions this catches:
  - the (ctx, event) hook never running (it was called as (name, old, new),
    raised TypeError, and the bridge swallowed it while the value still applied);
  - the echo: a self-change reported twice, first as a no-op (v -> v);
  - the hook running before the new value is readable from strategy_config.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

EXPECTED = [("size", 100, 125), ("threshold", 0.5, 0.75)]


class SdkT86Base:
    CHECKPOINT = ""

    def on_start(self, ctx, event):
        self.n = 0
        self.ctx = ctx
        self.s = {"calls": [], "value_visible_in_hook": True, "errors": []}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

    def _record(self, name, old, new):
        self.s["calls"].append([name, old, new])
        held = (self.ctx.strategy_config.params or {}).get(name)
        if str(held) != str(new):
            self.s["value_visible_in_hook"] = False

    def on_bar(self, ctx, event):
        self.n += 1
        params = ctx.strategy_config.params
        try:
            if self.n == 3:
                params["size"] = 125
            elif self.n == 6:
                params["threshold"] = 0.75
            elif self.n == 9:
                params["size"] = 125          # unchanged: must not fire
        except Exception as exc:  # noqa: BLE001
            self.s["errors"].append(repr(exc))

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, self.CHECKPOINT, self.s)


class SdkT86Event(SdkT86Base):
    CHECKPOINT = "t86_on_param_change_ctx_event"

    def on_param_change(self, ctx, event):
        change = event.data()
        self._record(change.name, change.old_value, change.new_value)


class SdkT86Args(SdkT86Base):
    CHECKPOINT = "t86_on_param_change_name_old_new"

    def on_param_change(self, param_name, old_value, new_value):
        self._record(param_name, old_value, new_value)


def _calls_ok(state):
    got = [(n, o, v) for n, o, v in state["calls"]]
    return [(n, str(o), str(v)) for n, o, v in got] == [(n, str(o), str(v)) for n, o, v in EXPECTED]


if __name__ == "__main__":
    params = {"size": 100, "threshold": 0.5}
    run = hf.run_backtest(
        strategy_configs=[
            StrategyConfig(name="SdkT86Event", type="SdkT86Event", symbols=["AAPL"], params=dict(params)),
            StrategyConfig(name="SdkT86Args", type="SdkT86Args", symbols=["AAPL"], params=dict(params)),
        ],
        symbols=["AAPL"], start_date="2025-09-02", end_date="2025-09-02",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30", session_end="09:50"))
    ev = completed_checkpoint(run, SdkT86Event.CHECKPOINT)
    args = completed_checkpoint(run, SdkT86Args.CHECKPOINT)
    finish("t86_on_param_change_callback", {
        "ctx_event_hook_fires_once_per_change": _calls_ok(ev),
        "name_old_new_hook_fires_once_per_change": _calls_ok(args),
        "new_value_visible_inside_hook": ev["value_visible_in_hook"] and args["value_visible_in_hook"],
        "no_errors_changing_params": not ev["errors"] and not args["errors"],
    }, extra=f"ctx_event={ev['calls']}, name_old_new={args['calls']}")
