"""NYSE imbalance delivery and venue-specific payload validation."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT15:
    def on_start(self, ctx, event):
        self.state = {"imbalances": 0, "nonzero": 0, "signed_side_ok": True,
                      "clearing_price_populated": 0, "payload_ok": True,
                      "samples": []}
        ctx.subscribe_bars(["IBM"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info("[START] waiting for IBM nyse_imbalance rows")

    def on_imbalance(self, ctx, event):
        data = event.data()
        self.state["imbalances"] += 1
        if data.imbalance != 0:
            self.state["nonzero"] += 1
        if data.clearing_price is not None:
            self.state["clearing_price_populated"] += 1
        if data.side == "SELL_IMBALANCE" and data.imbalance > 0:
            self.state["signed_side_ok"] = False
        if data.side == "BUY_IMBALANCE" and data.imbalance < 0:
            self.state["signed_side_ok"] = False
        clearing = (data.clearing_price, data.cont_book_clearing_price, data.closing_only_clearing_price)
        valid = (
            event.type == EventType.IMBALANCE
            and data.symbol == "IBM"
            and isinstance(data.side, str) and bool(data.side)
            # NYSE represents sell imbalance quantities as negative values.
            and isinstance(data.imbalance, (int, float))
            and isinstance(data.paired_shares, (int, float)) and data.paired_shares >= 0
            and all(value is None or isinstance(value, (int, float)) for value in clearing)
            and isinstance(event.ts_event, int)
        )
        self.state["payload_ok"] = self.state["payload_ok"] and valid
        if self.state["imbalances"] == 1 or self.state["imbalances"] % 200 == 0:
            self.state["samples"].append({"row": self.state["imbalances"],
                "symbol": data.symbol, "side": data.side,
                "imbalance": data.imbalance, "paired_shares": data.paired_shares,
                "clearing_price": data.clearing_price,
                "cont_book_clearing_price": data.cont_book_clearing_price,
                "closing_only_clearing_price": data.closing_only_clearing_price,
                "event_ts_event": event.ts_event})
        logger.debug(f"[IMBALANCE] count={self.state['imbalances']} valid={valid}")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t15_nyse_imbalance", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT15", type="SdkT15", symbols=["IBM"])],
        symbols=["IBM"], start_date="2026-07-01", end_date="2026-07-01",
        data_configs=[
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]},
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["nyse_imbalance"]},
        ])
    state = completed_checkpoint(run, "t15_nyse_imbalance")
    has_rows = state["imbalances"] > 0
    checks = {"nyse_rows_delivered": has_rows,
              "nyse_payload_contract": has_rows and state["payload_ok"],
              "nonzero_imbalances_delivered": state["nonzero"] > 0,
              "signed_side_consistent": has_rows and state["signed_side_ok"],
              "clearing_price_values_delivered": state["clearing_price_populated"] > 0,
              "periodic_samples_recorded": len(state["samples"]) > 1}
    finish("t15_nyse_imbalance", checks,
           extra=(f"count={state['imbalances']}, nonzero={state['nonzero']}, "
                  f"clearing_price_populated={state['clearing_price_populated']}, "
                  f"samples={state['samples']}"))
