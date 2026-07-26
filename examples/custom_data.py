#!/usr/bin/env python3
"""Custom data feed — bring your own CSV signal file into a strategy.

Demonstrates (every ctx call verified against the SDK type surface):
  - a CSV custom data source in ``data_configs``: ``{'type':'csv',
    'data_type':'custom','id':...,'path':...}`` (§9.2), wired to the strategy
    via ``ctx.subscribe_data(data_id=...)`` (§5.1). ``id`` must match the arg.
  - rows arrive in ``on_custom_data`` as ``SigmaCustomData`` (§7.9): read your
    own columns with ``data.column_data(name, default=...)`` and trade on them.
  - subscribe to bars too so the venue is initialized before placing orders.

The feed (examples/userdata/user_signals.csv) has columns
``date,time,sym,signal,weight,action``; we act on the ``action`` column.

Run:  python custom_data.py
"""
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

DATA_ID = "UserData"             # must match data_configs 'id' + subscribe_data(data_id=...)
# Platform object-storage path. Upload from the examples directory with:
#   cd userdata && hiveq-data -u user_signals.csv
# Absolute local paths do not exist inside the remote executor.
CSV_PATH = "user_signals.csv"


class CustomData:
    def on_start(self, ctx, event):                          # subscribe in START (R3)
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                            asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_data(data_id=DATA_ID)                  # our CSV custom feed (§5.1)
        ctx.add_event_log("subscribed to custom CSV feed", sub_event_type="INIT")

    def on_custom_data(self, ctx, event):
        data = event.data()                                  # -> SigmaCustomData (§7.9)
        sym = data.column_data("sym", default=None)
        action = data.column_data("action", default="")
        weight = float(data.column_data("weight", default=0.0) or 0.0)
        if sym is None:
            return
        ctx.add_event_log(f"custom row {sym} action={action} weight={weight:.2f}",
                          sub_event_type="CUSTOM_DATA", symbol=sym)

        # trade off your own column semantics
        if action in ("BUY_SIGNAL", "STRONG_BUY") and ctx.is_flat(sym):
            ctx.buy_order(sym, quantity=int(100 * max(weight, 1.0)))
            ctx.add_event_log(f"enter long {sym} on {action}", symbol=sym)
        elif action in ("SELL_SIGNAL", "WEAK_SELL") and ctx.is_net_long(sym):
            ctx.close_position(sym)
            ctx.add_event_log(f"exit {sym} on {action}", symbol=sym)

    def on_order(self, ctx, event):
        o = event.data()                                     # -> SigmaOrder (§7.3)
        if o.is_filled:
            ctx.add_event_log(f"fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}",
                              symbol=o.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="CustomData", type="CustomData")],
        symbols=["AAPL"],
        start_date="2025-08-01",
        end_date="2025-08-02",
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]},
            {"type": "csv", "data_type": "custom", "id": DATA_ID, "path": CSV_PATH},
        ],
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
