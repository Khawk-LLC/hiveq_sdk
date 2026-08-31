"""Nasdaq imbalance delivery and venue-specific payload validation."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT14:
    def on_start(self, ctx, event):
        self.state = {"imbalances": 0, "nonzero": 0, "ref_price_populated": 0,
                      "near_price_populated": 0, "far_price_populated": 0,
                      "payload_ok": True, "observed_sides": [],
                      "observed_cross_types": [], "samples": []}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info("[START] waiting for AAPL nasd_imbalance rows")

    def on_imbalance(self, ctx, event):
        data = event.data()
        self.state["imbalances"] += 1
        if data.imbalance != 0:
            self.state["nonzero"] += 1
        if data.ref_price is not None:
            self.state["ref_price_populated"] += 1
        if data.near_price is not None:
            self.state["near_price_populated"] += 1
        if data.far_price is not None:
            self.state["far_price_populated"] += 1
        if data.side not in self.state["observed_sides"]:
            self.state["observed_sides"].append(data.side)
        if data.cross_type not in self.state["observed_cross_types"]:
            self.state["observed_cross_types"].append(data.cross_type)
        valid = (
            event.type == EventType.IMBALANCE
            and data.symbol == "AAPL"
            and isinstance(data.side, str) and bool(data.side)
            and isinstance(data.imbalance, (int, float))
            and isinstance(data.paired_shares, (int, float)) and data.paired_shares >= 0
            # Nasdaq also emits A for its no-imbalance/after-cross state.
            and (data.cross_type is None or data.cross_type in {"O", "C", "A"})
            and (data.ref_price is None or isinstance(data.ref_price, (int, float)))
            and (data.near_price is None or isinstance(data.near_price, (int, float)))
            and (data.far_price is None or isinstance(data.far_price, (int, float)))
            and isinstance(event.ts_event, int)
        )
        self.state["payload_ok"] = self.state["payload_ok"] and valid
        if self.state["imbalances"] == 1 or self.state["imbalances"] % 100 == 0:
            self.state["samples"].append({"row": self.state["imbalances"],
                "symbol": data.symbol, "side": data.side,
                "imbalance": data.imbalance, "paired_shares": data.paired_shares,
                "ref_price": data.ref_price,
                "near_price": data.near_price, "far_price": data.far_price,
                "cross_type": data.cross_type, "event_ts_event": event.ts_event})
        logger.debug(f"[IMBALANCE] count={self.state['imbalances']} valid={valid}")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t14_nasdaq_imbalance", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT14", type="SdkT14", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2026-07-01", end_date="2026-07-01",
        data_configs=[
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]},
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["nasd_imbalance"]},
        ])
    state = completed_checkpoint(run, "t14_nasdaq_imbalance")
    has_rows = state["imbalances"] > 0
    checks = {"nasdaq_rows_delivered": has_rows,
              "nasdaq_payload_contract": has_rows and state["payload_ok"],
              "nonzero_imbalances_delivered": state["nonzero"] > 0,
              "ref_price_values_delivered": state["ref_price_populated"] > 0,
              "periodic_samples_recorded": len(state["samples"]) > 1}
    finish("t14_nasdaq_imbalance", checks,
           extra=(f"count={state['imbalances']}, nonzero={state['nonzero']}, "
                  f"ref_price_populated={state['ref_price_populated']}, "
                  f"near_price_populated={state['near_price_populated']}, "
                  f"far_price_populated={state['far_price_populated']}, "
                  f"sides={state['observed_sides']}, "
                  f"cross_types={state['observed_cross_types']}, "
                  f"samples={state['samples']}"))
