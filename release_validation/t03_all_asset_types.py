"""One remote strategy receives every supported market-data asset stream."""
from pathlib import Path
import sys
# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

class SdkT03:
    def on_start(self, ctx, event):
        if not hasattr(self, "c"):
            self.c = {k:0 for k in ("eq1m","eq1d","trades","futures","index","snaps")}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1d")
        ctx.subscribe_trades(["MSFT"], asset_type=AssetType.EQUITY)
        ctx.subscribe_bars(["ES.c.0"], asset_type=AssetType.FUTURES, interval="1m")
        ctx.subscribe_index_bars(["VIX"], interval="1d")
        ctx.subscribe_option_snaps("SPXW", option_type="C", expiration_type="0dte", interval="1s")
    def on_bar(self, ctx, event):
        b=event.data()
        if b.symbol=="AAPL" and b.interval=="1m": self.c["eq1m"]+=1
        elif b.symbol=="AAPL" and b.interval=="1d": self.c["eq1d"]+=1
        elif b.symbol.startswith("ES"): self.c["futures"]+=1
    def on_trade(self, ctx, event): self.c["trades"]+=1
    def on_index_price(self, ctx, event): self.c["index"]+=1
    def on_snap(self, ctx, event): self.c["snaps"]+=1
    def on_stop(self, ctx, event): emit_checkpoint(ctx,"t03_all_asset_types",self.c)

if __name__ == "__main__":
    schemas=[("HIVEQ_US_EQ","bars_1m"),("HIVEQ_US_EQ","bars_1d"),("HIVEQ_US_EQ","eq_trades"),
             ("HIVEQ_US_FUT","bars_1m"),("HIVEQ_US_IND","indices_values"),("HIVEQ_US_OPT","snaps_1s")]
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT03",type="SdkT03",symbols=["AAPL","MSFT"])],
        symbols=["AAPL","MSFT"],start_date="2025-06-02",end_date="2025-07-01",
        data_configs=[{"type":"hiveq_historical","dataset":d,"schema":[s]} for d,s in schemas],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:00"))
    s=completed_checkpoint(run,"t03_all_asset_types")
    finish("t03_all_asset_types",{k:s[k]>0 for k in s},extra=str(s))
