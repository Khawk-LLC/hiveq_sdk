"""Every TradeTick field is readable, correctly typed, and carries data.

Field-level coverage for the trade DTO. Each field is classified:

* REQUIRED    - must be present and non-default on every tick
* CATEGORICAL - must be one of a known set; the observed distribution is reported
* TYPED       - must come back as a specific enum type, not a bare int/str
* KNOWN-EMPTY - legitimately always empty; asserted empty *with a reason* so the
                field stays covered instead of silently dropping out of the suite

The KNOWN-EMPTY class matters: ``trade_id`` has no engine field behind it, and
``raw_trade_condition`` is only populated by the HiveQ adapter. Asserting them
empty (rather than skipping) means this test fails loudly if that ever changes.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

SYMBOL = "AAPL"
AGGRESSOR_VALUES = {"BUY", "SELL", "NO_AGGRESSOR", "UNDEFINED"}


class SdkT47:
    def on_start(self, ctx, event):
        self.state = {
            "trades": 0,
            # per-field population counters
            "populated": {},
            "bad": {},
            # observed values / types
            "condition_values": {},
            "condition_types": {},
            "exchange_values": {},
            "exchange_types": {},
            "aggressor_values": {},
            "raw_condition_values": {},
            "exchange_raw_values": {},
            "condition_raw_values": {},
            "undefined_venue_ticks": 0,
            "undefined_with_raw": 0,
            "trade_id_nonempty": 0,
            "last_price_zero": 0,
            "last_price_eq_price": 0,
            "last_price_other": 0,
            "market_center_matches_exchange": 0,
            "samples": [],
            "errors": [],
        }
        ctx.subscribe_trades([SYMBOL], asset_type=AssetType.EQUITY)

    def _bump(self, bucket, key):
        d = self.state[bucket]
        d[key] = d.get(key, 0) + 1

    def on_trade(self, ctx, event):
        t = event.data()
        s = self.state
        s["trades"] += 1
        try:
            # --- REQUIRED scalars -------------------------------------------
            for name, value, ok in (
                ("symbol", t.symbol, isinstance(t.symbol, str) and t.symbol == SYMBOL),
                ("price", t.price, isinstance(t.price, float) and t.price > 0),
                ("size", t.size, isinstance(t.size, float) and t.size > 0),
                ("ts_event", t.ts_event, isinstance(t.ts_event, int) and t.ts_event > 0),
                ("time", t.time, t.time is not None and t.time.tzinfo is not None),
                ("time_utc", t.time_utc, t.time_utc is not None and t.time_utc.tzinfo is not None),
            ):
                if ok:
                    self._bump("populated", name)
                else:
                    self._bump("bad", name)

            # --- TYPED enums ------------------------------------------------
            cond = t.condition
            self._bump("condition_types", type(cond).__name__)
            self._bump("condition_values", str(getattr(cond, "name", cond)))
            if type(cond).__name__ == "TradeCondition":
                self._bump("populated", "condition")
            else:
                self._bump("bad", "condition")

            exch = t.exchange
            self._bump("exchange_types", type(exch).__name__)
            self._bump("exchange_values", str(getattr(exch, "name", exch)))
            if type(exch).__name__ == "MarketCenter":
                self._bump("populated", "exchange")
            else:
                self._bump("bad", "exchange")

            if t.market_center == exch:
                s["market_center_matches_exchange"] += 1
                self._bump("populated", "market_center")
            else:
                self._bump("bad", "market_center")

            # --- CATEGORICAL ------------------------------------------------
            side = str(t.aggressor_side)
            self._bump("aggressor_values", side)
            if side in AGGRESSOR_VALUES:
                self._bump("populated", "aggressor_side")
            else:
                self._bump("bad", "aggressor_side")

            # --- last_price: gated by condition, so 0.0 is meaningful -------
            lp = t.last_price
            if not isinstance(lp, float) or lp < 0:
                self._bump("bad", "last_price")
            else:
                self._bump("populated", "last_price")
                if lp == 0.0:
                    s["last_price_zero"] += 1
                elif lp == t.price:
                    s["last_price_eq_price"] += 1
                else:
                    s["last_price_other"] += 1

            # --- KNOWN-EMPTY -------------------------------------------------
            rtc = t.raw_trade_condition
            if isinstance(rtc, int):
                self._bump("populated", "raw_trade_condition")
                self._bump("raw_condition_values", str(rtc))
            else:
                self._bump("bad", "raw_trade_condition")

            if t.trade_id != "":
                s["trade_id_nonempty"] += 1
            self._bump("populated", "trade_id")   # readable; emptiness asserted below

            # exchange_raw: the feed's own venue string. An unmapped venue must
            # still be identifiable here, otherwise it is silently lost.
            raw_ex = t.exchange_raw
            if isinstance(raw_ex, str):
                self._bump("populated", "exchange_raw")
                self._bump("exchange_raw_values", raw_ex or "<empty>")
            else:
                self._bump("bad", "exchange_raw")
            # condition_raw: the feed's textual condition, pre-normalization.
            # condition (the enum) is lossy -- several feed codes collapse onto
            # one member -- so this must carry the original.
            cond_raw = t.condition_raw
            if isinstance(cond_raw, str):
                self._bump("populated", "condition_raw")
                self._bump("condition_raw_values", cond_raw or "<empty>")
            else:
                self._bump("bad", "condition_raw")

            if str(getattr(exch, "name", exch)) == "UNDEFINED_MARKET_CENTER":
                s["undefined_venue_ticks"] += 1
                if raw_ex:
                    s["undefined_with_raw"] += 1

            if len(s["samples"]) < 8:
                s["samples"].append({
                    "n": s["trades"],
                    "symbol": t.symbol,
                    "price": float(t.price),
                    "size": float(t.size),
                    "last_price": float(lp),
                    "condition": str(getattr(cond, "name", cond)),
                    "raw_trade_condition": rtc,
                    "exchange": str(getattr(exch, "name", exch)),
                    "market_center": str(getattr(t.market_center, "name", t.market_center)),
                    "aggressor_side": side,
                    "trade_id": t.trade_id,
                    "ts_event": int(t.ts_event),
                    "time": str(t.time),
                })
        except Exception as exc:  # noqa: BLE001 - recorded, asserted on below
            if len(s["errors"]) < 10:
                s["errors"].append(f"{type(exc).__name__}: {exc}")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t47_trade_tick_fields", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT47", type="SdkT47", symbols=[SYMBOL])],
        symbols=[SYMBOL],
        start_date="2025-09-23",
        end_date="2025-09-23",
        # eq_trades, not tbbo: the engine's TBBO parser hardcodes
        # TradeCondition::Regular and never calls setExchange/setRawTradeCondition,
        # so a tbbo-backed run cannot exercise condition/exchange/raw at all.
        # The eq_trades path runs resolveTradeCondition() and parseExchange().
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"],
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:00"),
    )
    state = completed_checkpoint(run, "t47_trade_tick_fields")

    n = state["trades"]
    pop = state["populated"]
    bad = state["bad"]
    fields = [
        "symbol", "price", "size", "ts_event", "time", "time_utc",
        "condition", "exchange", "market_center", "aggressor_side",
        "last_price", "raw_trade_condition", "trade_id", "exchange_raw", "condition_raw",
    ]

    print(f"\ntrades observed: {n:,}")
    print(f"{'field':<22} {'populated':>10} {'bad':>6}")
    print("-" * 42)
    for f in fields:
        print(f"{f:<22} {pop.get(f, 0):>10,} {bad.get(f, 0):>6,}")
    print(f"\ncondition types      : {state['condition_types']}")
    print(f"condition values     : {state['condition_values']}")
    print(f"exchange types       : {state['exchange_types']}")
    print(f"exchange values      : {state['exchange_values']}")
    print(f"aggressor values     : {state['aggressor_values']}")
    print(f"raw_trade_condition  : {state['raw_condition_values']}")
    print(f"exchange_raw values  : {state['exchange_raw_values']}")
    print(f"condition_raw values : {state['condition_raw_values']}")
    print(f"undefined venues     : {state['undefined_venue_ticks']:,} ticks, "
          f"{state['undefined_with_raw']:,} of them carry exchange_raw")
    print(f"last_price zero={state['last_price_zero']:,} "
          f"==price={state['last_price_eq_price']:,} other={state['last_price_other']:,}")
    print("\nsample ticks:")
    for s in state["samples"]:
        print(f"  {s}")

    checks = {"trades_present": n > 0, "no_read_errors": not state["errors"]}
    # every field must be readable on every tick, with zero bad reads
    for f in fields:
        checks[f"{f}_all_ticks"] = n > 0 and pop.get(f, 0) == n and bad.get(f, 0) == 0
    checks["market_center_eq_exchange"] = state["market_center_matches_exchange"] == n
    checks["condition_is_enum"] = set(state["condition_types"]) == {"TradeCondition"}
    checks["exchange_is_enum"] = set(state["exchange_types"]) == {"MarketCenter"}
    checks["aggressor_in_known_set"] = set(state["aggressor_values"]) <= AGGRESSOR_VALUES
    # An unmapped venue is allowed to be UNDEFINED -- but it must always be
    # identifiable via exchange_raw, which is the whole point of that field.
    checks["undefined_venues_carry_raw"] = (
        state["undefined_with_raw"] == state["undefined_venue_ticks"]
    )

    # --- data-bearing checks: type-correct is not enough ------------------
    # An equity session prints many conditions (odd lots, FormT, ISO, avg-price).
    # A single distinct value means the adapter is hardcoding it -- which is
    # exactly what the tbbo path does -- so demand real variety here.
    conds = state["condition_values"]
    checks["condition_has_variety"] = len(conds) > 1
    checks["condition_not_all_regular"] = set(conds) != {"Regular"}
    # Real venues, not just the SOR default that TradeData initializes to.
    venues = set(state["exchange_values"])
    checks["exchange_has_real_venues"] = len(venues - {"SOR"}) >= 2

    # last_price is gated by condition: a print either updates last price
    # (last_price == price) or does not (0.0). Every tick must fall in one of
    # those two buckets -- never a third value -- and both buckets must be
    # non-empty, which proves the gating is actually being exercised.
    zero, eq, other = (state["last_price_zero"], state["last_price_eq_price"],
                       state["last_price_other"])
    checks["last_price_partitions_cleanly"] = other == 0 and (zero + eq) == n
    checks["last_price_gating_exercised"] = zero > 0 and eq > 0

    # --- KNOWN-EMPTY: assert empty *with a reason* so a change fails loudly
    # engine TradeData has no per-print trade id field
    checks["trade_id_known_empty"] = state["trade_id_nonempty"] == 0
    # the API returns the condition as a string (resolved into `condition`);
    # the numeric raw code is not carried, so this stays 0 on this feed
    checks["raw_condition_known_zero"] = set(state["raw_condition_values"]) == {"0"}
    # condition_raw must carry real text -- it is the only record of what the
    # feed actually sent, since the enum mapping is lossy.
    craw = state["condition_raw_values"]
    checks["condition_raw_populated"] = bool(craw) and "<empty>" not in craw
    checks["condition_raw_has_variety"] = len(craw) > 1

    finish("t47_trade_tick_fields", checks)
