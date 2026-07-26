#!/usr/bin/env python3
"""Quant signals — subscribe to a HIVEQ_QUANT_SIGNALS feed and trade on it.

Demonstrates (every ctx call verified against the SDK type surface):
  - a ``HIVEQ_QUANT_SIGNALS`` data source declared in ``data_configs`` with an
    ``'id'`` (§9.1), wired to the strategy via ``ctx.subscribe_data(data_id=...)``
    (§5.1). The ``id`` and the ``subscribe_data`` arg must match.
  - signal rows arrive as custom data in ``on_custom_data`` — the payload is a
    ``SigmaCustomData`` (§7.9); read columns with ``data.column_data(name)`` and
    parse the JSON ``signal_json`` column for the actual signal fields.
  - trading off the parsed signal: go long on a strong long flag, flat otherwise.
  - subscribe to bars too so the venue is initialized before we trade.

Shape: act on the signal feed; long when flag=='long' & weight high, else flat.

Run:  python quant_signals.py
"""
import json

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

SIGNAL_ID = "SignalTest"         # stable source ID; must match data_configs 'id'
SIGNAL_KEY = "Prillach_MC_ES"    # signal key stored in HIVEQ_QUANT_SIGNALS
MIN_WEIGHT = 0.7                 # only act on high-conviction signals


def decode_signal_json(raw):
    """Decode either plain JSON or the current Sigma CSV-escaped form."""
    normalized = raw.replace(r'\"', '"').replace("|", ",")
    return json.loads(normalized)


class QuantSignals:
    def on_start(self, ctx, event):                          # subscribe in START (R3)
        # bars init the venue so orders are routable
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                            asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_data(data_id=SIGNAL_ID)
        ctx.add_event_log("subscribed to quant signals", sub_event_type="INIT")

    def on_custom_data(self, ctx, event):
        data = event.data()                                  # -> SigmaCustomData (§7.9)
        symbol = data.column_data("sym", default="UNKNOWN")

        signal_json = data.column_data("signal_json", default=None)
        ctx.add_event_log(
            f"custom data received symbol={symbol} signal_json={signal_json!r}",
            sub_event_type="CUSTOM_DATA_RECEIVED",
            symbol=str(symbol),
        )
        if not signal_json:
            return
        try:
            sig = decode_signal_json(signal_json)
        except (json.JSONDecodeError, TypeError):
            ctx.add_event_log("bad signal_json", sub_event_type="ERROR", symbol=symbol)
            return

        ticker = sig.get("ticker", symbol)
        flag = sig.get("flag", "flat")
        weight = float(sig.get("weight1", 0.0) or 0.0)
        ctx.add_event_log(f"signal {ticker} flag={flag} weight={weight:.2f}",
                          sub_event_type="SIGNAL", symbol=ticker)

        if flag == "long" and weight >= MIN_WEIGHT and ctx.is_flat(ticker):
            ctx.buy_order(ticker, quantity=100)
            ctx.add_event_log(f"enter long {ticker} on signal", symbol=ticker)
        elif flag != "long" and ctx.is_net_long(ticker):
            ctx.close_position(ticker)
            ctx.add_event_log(f"exit {ticker} on signal", symbol=ticker)

    def on_order(self, ctx, event):
        o = event.data()                                     # -> SigmaOrder (§7.3)
        if o.is_filled:
            ctx.add_event_log(f"fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}",
                              symbol=o.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="QuantSignals", type="QuantSignals")],
        symbols=["AAPL"],
        start_date="2024-08-27",
        end_date="2025-12-17",
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]},
            {"type": "hiveq_historical", "dataset": "HIVEQ_QUANT_SIGNALS",
             "schema": ["signals"], "id": SIGNAL_ID, "symbols": [SIGNAL_KEY]},
        ],
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
