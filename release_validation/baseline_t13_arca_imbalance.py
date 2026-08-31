"""NYSE Arca imbalance delivery and venue-specific payload validation."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT13:
    def on_start(self, ctx, event):
        self.state = {"imbalances": 0, "actionable": 0, "no_imbalance": 0,
                      "ref_price_populated": 0, "market_imbalance_populated": 0,
                      "payload_ok": True, "semantic_ok": True, "samples": []}
        # A subscribed clock stream drives the replay timeline on which the
        # independently configured imbalance records are dispatched.
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info("[START] waiting for AAPL arca_imbalance rows")

    def on_imbalance(self, ctx, event):
        data = event.data()
        self.state["imbalances"] += 1
        no_imbalance = data.side == "NO_IMBALANCE_ON_CLOSE"
        if no_imbalance:
            self.state["no_imbalance"] += 1
        else:
            self.state["actionable"] += 1
        if data.ref_price is not None:
            self.state["ref_price_populated"] += 1
        if data.market_imbalance is not None:
            self.state["market_imbalance_populated"] += 1
        valid = (
            event.type == EventType.IMBALANCE
            and data.symbol == "AAPL"
            # The feed can explicitly report that no closing imbalance exists.
            and isinstance(data.side, str) and bool(data.side)
            and isinstance(data.imbalance, (int, float)) and data.imbalance >= 0
            and isinstance(data.paired_shares, (int, float)) and data.paired_shares >= 0
            and (data.ref_price is None or isinstance(data.ref_price, (int, float)))
            and (data.market_imbalance is None or isinstance(data.market_imbalance, (int, float)))
            and isinstance(event.ts_event, int)
        )
        self.state["payload_ok"] = self.state["payload_ok"] and valid
        semantic = (
            (not no_imbalance)
            or (data.imbalance == 0 and data.paired_shares == 0)
        )
        self.state["semantic_ok"] = self.state["semantic_ok"] and semantic

        # Capture the first row and then every 250th row across the session.
        # This is compact enough for a checkpoint while showing field changes.
        if self.state["imbalances"] == 1 or self.state["imbalances"] % 250 == 0:
            self.state["samples"].append({"row": self.state["imbalances"],
                "symbol": data.symbol, "side": data.side,
                "imbalance": data.imbalance, "paired_shares": data.paired_shares,
                "ref_price": data.ref_price, "market_imbalance": data.market_imbalance,
                "event_ts_event": event.ts_event})
        logger.debug(f"[IMBALANCE] count={self.state['imbalances']} valid={valid}")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t13_arca_imbalance", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT13", type="SdkT13", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2026-07-01", end_date="2026-07-01",
        data_configs=[
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]},
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["arca_imbalance"]},
        ])
    state = completed_checkpoint(run, "t13_arca_imbalance")
    has_rows = state["imbalances"] > 0
    checks = {"arca_rows_delivered": has_rows,
              "arca_payload_contract": has_rows and state["payload_ok"],
              "no_imbalance_semantics": has_rows and state["semantic_ok"],
              "actionable_rows_delivered": state["actionable"] > 0,
              "ref_price_values_delivered": state["ref_price_populated"] > 0,
              "periodic_samples_recorded": len(state["samples"]) > 1}
    finish("t13_arca_imbalance", checks,
           extra=(f"count={state['imbalances']}, actionable={state['actionable']}, "
                  f"no_imbalance={state['no_imbalance']}, "
                  f"ref_price_populated={state['ref_price_populated']}, "
                  f"market_imbalance_populated={state['market_imbalance_populated']}, "
                  f"samples={state['samples']}"))
