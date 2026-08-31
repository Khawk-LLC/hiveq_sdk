"""Canonical option filters: call/put accumulation, 0DTE, strike, and expiration."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import checkpoint, emit_checkpoint, finish, wait_for_final
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT26A:
    def on_start(self, ctx, event):
        self.s = {"C": 0, "P": 0, "bad_0dte": 0, "samples": []}
        ctx.subscribe_option_snaps("SPXW", option_type="C", expiration_type="0dte", interval="1s")
        ctx.subscribe_option_snaps("SPXW", option_type="P", expiration_type="0dte", interval="1s")
    def on_snap(self, ctx, event):
        d = event.data(); kind = str(d.option_type).upper()
        if kind in self.s: self.s[kind] += 1
        if str(d.date).replace("-", "") != str(d.expiration_date).replace("-", ""):
            self.s["bad_0dte"] += 1
        if len(self.s["samples"]) < 4:
            self.s["samples"].append([kind, d.strike, d.date, d.expiration_date])
    def on_stop(self, ctx, event): emit_checkpoint(ctx, "t26_calls_puts_0dte", self.s)


class SdkT26B:
    def on_start(self, ctx, event):
        self.s = {"count": 0, "bad_strikes": []}
        ctx.subscribe_option_snaps("SPXW", option_type="C", strike=5950.0,
                                    expiration_type="0dte", interval="1s")
    def on_snap(self, ctx, event):
        strike = float(event.data().strike); self.s["count"] += 1
        if strike != 5950.0 and strike not in self.s["bad_strikes"]: self.s["bad_strikes"].append(strike)
    def on_stop(self, ctx, event): emit_checkpoint(ctx, "t26_strike", self.s)


class SdkT26C:
    def on_start(self, ctx, event):
        self.s = {"count": 0, "expirations": []}
        ctx.subscribe_option_snaps("SPXW", option_type="C",
                                    expiration_type="2022-01-03", interval="1s")
    def on_snap(self, ctx, event):
        value = str(event.data().expiration_date); self.s["count"] += 1
        if value not in self.s["expirations"]: self.s["expirations"].append(value)
    def on_stop(self, ctx, event): emit_checkpoint(ctx, "t26_expiration", self.s)


def run_case(cls, name, start, checkpoint_name):
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name=name, type=cls.__name__, symbols=["SPXW"])],
        symbols=["SPXW"], start_date=start, end_date=start,
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_OPT","schema":["snaps_1s"]}])
    wait_for_final(run)
    return checkpoint(run, checkpoint_name)


if __name__ == "__main__":
    cp = run_case(SdkT26A, "SdkT26A", "2022-01-03", "t26_calls_puts_0dte")
    strike = run_case(SdkT26B, "SdkT26B", "2025-01-02", "t26_strike")
    expiration = run_case(SdkT26C, "SdkT26C", "2022-01-03", "t26_expiration")
    finish("t26_option_filters", {
        "calls_delivered": cp["C"] > 0,
        "puts_delivered": cp["P"] > 0,
        "every_row_0dte": cp["bad_0dte"] == 0,
        "0dte_samples_recorded": bool(cp["samples"]),
        "strike_rows_delivered": strike["count"] > 0,
        "only_requested_strike": not strike["bad_strikes"],
        "expiration_rows_delivered": expiration["count"] > 0,
        "only_requested_expiration": len(expiration["expirations"]) == 1
            and expiration["expirations"][0].replace("-", "") == "20220103",
    }, extra=f"calls_puts={cp}, strike={strike}, expiration={expiration}")
