"""t82: depth-enabled (L3/MBO) backtest -- the book a strategy sees and the queue it waits in.

`engine_config.params.enable_mbo=True` replays the contract's DataBento MBO day
next to the HiveQ trades and fills on DepthPaperBroker. The same strategy runs
twice on MES.c.0 (MESZ6), 2026-10-20 10:00-10:40 New York:

depth run (enable_mbo=True, DepthPaperBroker):
* ``ctx.instrument(sym).orderBookDepth`` is valid on (nearly) every trade, ten
  levels a side, never crossed, bids strictly descending / asks strictly
  ascending, every level sized;
* the tape trades inside that book (within a tick of the touch);
* a 1-lot buy limit joined at the best bid fills only at its limit, only after
  a print at or through it, and at least once *waits past* the first print at
  its price -- the queue ahead of it is real (PaperBrokerV2 fills the first
  print it sees);
* the market exit fills at the touch it was sent against (within a tick);

control run (default, PaperBrokerV2): the same strategy sees no depth at all,
so the depth above comes from the flag and nowhere else.
"""

from pathlib import Path
import sys

# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish   # noqa: E402

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, EngineConfig, StrategyConfig   # noqa: E402
from hiveq.flow.config import AssetType, EventType                    # noqa: E402
from hiveq.flow.trading_types import OrderType                        # noqa: E402

CHECKPOINT = "t82_depth_mbo_backtest"
SYMBOL = "MES.c.0"
DAY = "2026-10-20"
TICK = 0.25
LEVELS = 10
CYCLES = 12                       # passive entries per run
PASSIVE_TIMEOUT_NS = 90 * 10**9   # cancel an unfilled join after 90 s of event time
COOLDOWN_NS = 5 * 10**9           # flat time between cycles
SAMPLES = 5


class SdkT82:
    def __init__(self):
        self.s = {"trades": 0, "depth_valid": 0, "ten_levels": 0, "crossed": 0, "bad_ladder": 0,
                  "inside": 0, "depth_symbol": "", "passive": [], "exits": [], "rejects": 0,
                  "samples": {"crossed": [], "bad_ladder": [], "outside": []}}
        self.last_key = None
        self.symbol = None          # physical contract the trades arrive on
        self.phase = "IDLE"
        self.order = None           # the working passive join
        self.exit_id = None
        self.next_at = 0
        self.prints = []            # (ts, price, size) while a join is working
        # A fill and the print that caused it share a timestamp, and the engine
        # delivers ORDER_FILLED before on_trade for that print (2026-11-01, MESZ6
        # 7750.75 join: the 1-lot print at the fill's own ts arrived after the
        # fill). A fill with no prior print stays open here until event time
        # passes its ts, so that same-ts print can still be credited.
        self.awaiting_print = None

    def sample(self, key, value):
        if len(self.s["samples"][key]) < SAMPLES:
            self.s["samples"][key].append(value)

    def on_start(self, ctx, event):
        ctx.subscribe_trades([SYMBOL], asset_type=AssetType.FUTURES)

    def depth(self, ctx):
        for sym in (self.symbol, SYMBOL):
            inst = ctx.instrument(sym) if sym else None
            book = getattr(inst, "orderBookDepth", None) if inst else None
            if book is not None and book.isValid:
                self.s["depth_symbol"] = sym
                return book
        return None

    def check_book(self, book, price, ts):
        bids = [(lvl.price, lvl.size) for lvl in book.bids]
        asks = [(lvl.price, lvl.size) for lvl in book.asks]
        self.s["depth_valid"] += 1
        if len(bids) >= LEVELS and len(asks) >= LEVELS:
            self.s["ten_levels"] += 1
        if not bids or not asks or book.bestBid >= book.bestAsk:
            self.s["crossed"] += 1
            self.sample("crossed", [ts, book.bestBid, book.bestAsk])
            return
        ordered = (all(a[0] > b[0] for a, b in zip(bids, bids[1:]))
                   and all(a[0] < b[0] for a, b in zip(asks, asks[1:]))
                   and all(size > 0 for _, size in bids + asks))
        if not ordered:
            self.s["bad_ladder"] += 1
            self.sample("bad_ladder", [ts, bids[:3], asks[:3]])
        if book.bestBid - TICK <= price <= book.bestAsk + TICK:
            self.s["inside"] += 1
        else:
            self.sample("outside", [ts, price, book.bestBid, book.bestAsk])

    def on_trade(self, ctx, event):
        t = event.data()
        ts, price, size = int(event.ts_event), float(t.price), float(t.size or 0)
        # A continuous subscription also delivers each print on the physical
        # contract; count and act on one copy.
        if ".c." not in t.symbol:
            self.symbol = t.symbol
        key = (ts, price, size)
        if key == self.last_key:
            return
        self.last_key = key
        self.s["trades"] += 1
        pending = self.awaiting_print
        if pending is not None:
            if ts > pending["fill_ts"]:
                self.awaiting_print = None
            elif price <= pending["limit"]:
                pending["print_before_fill"] = True
                self.awaiting_print = None
        if self.phase == "WORKING":
            self.prints.append((ts, price, size))

        book = self.depth(ctx)
        if book is not None:
            self.check_book(book, price, ts)

        if self.phase == "WORKING" and ts - self.order["placed_ts"] > PASSIVE_TIMEOUT_NS:
            self.phase = "CANCELING"
            ctx.cancel_order(self.order["id"])
        elif (self.phase == "IDLE" and self.symbol and ts >= self.next_at
              and len(self.s["passive"]) < CYCLES):
            self.join(ctx, book, ts)

    def join(self, ctx, book, ts):
        if book is not None:
            limit, queue = float(book.bestBid), float(book.bestBidSize)
        else:  # control run: no book, join at the last print
            limit, queue = self.last_key[1], 0.0
        order = ctx.buy_order(self.symbol, 1, order_type=OrderType.LIMIT,
                              limit_price=limit, time_in_force="GTC")
        if order is None or not order.order_id:
            self.s["rejects"] += 1
            self.next_at = ts + COOLDOWN_NS
            return
        self.order = {"id": order.order_id, "limit": limit, "queue_ahead": queue,
                      "placed_ts": ts, "fill_ts": None, "fill_px": None,
                      "first_print_ts": None, "print_before_fill": None}
        self.prints = []
        self.phase = "WORKING"

    def on_order(self, ctx, event):
        order = event.data()
        oid = order.order_id
        if self.order is not None and oid == self.order["id"]:
            if event.type == EventType.ORDER_FILLED:
                fill_ts = int(event.ts_event)
                limit = self.order["limit"]
                at_or_through = [p for p in self.prints if p[0] <= fill_ts and p[1] <= limit]
                at_price = [p for p in self.prints if p[1] == limit]
                self.order.update(fill_ts=fill_ts, fill_px=float(order.avg_px),
                                  print_before_fill=bool(at_or_through),
                                  first_print_ts=at_price[0][0] if at_price else None)
                if not at_or_through:
                    self.awaiting_print = self.order
                self.close(ctx, fill_ts)
            elif event.type in (EventType.ORDER_CANCELED, EventType.ORDER_REJECTED):
                self.s["rejects"] += int(event.type == EventType.ORDER_REJECTED)
                self.order["first_print_ts"] = next(
                    (p[0] for p in self.prints if p[1] == self.order["limit"]), None)
                self.s["passive"].append(self.order)
                self.order = None
                self.phase = "IDLE"
                self.next_at = int(event.ts_event) + COOLDOWN_NS
        elif oid == self.exit_id and event.type == EventType.ORDER_FILLED:
            self.s["exits"][-1]["fill_px"] = float(order.avg_px)
            self.exit_id = None
            self.phase = "IDLE"
            self.next_at = int(event.ts_event) + COOLDOWN_NS

    def close(self, ctx, ts):
        self.s["passive"].append(self.order)
        self.order = None
        book = self.depth(ctx)
        exit_order = ctx.sell_order(self.symbol, 1)
        self.s["exits"].append({"bid_at_send": float(book.bestBid) if book is not None else None,
                                "fill_px": None})
        self.exit_id = exit_order.order_id if exit_order is not None else None
        self.phase = "EXITING" if self.exit_id else "IDLE"
        self.next_at = ts + COOLDOWN_NS

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, CHECKPOINT, self.s)


def run_once(enable_mbo: bool):
    params = {"enable_mbo": True, "exchange_sim_adapter": "DepthPaperBroker",
              "databento.dataset": "GLBX.MDP3"} if enable_mbo else {}
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT82", type="SdkT82", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date=DAY, end_date=DAY,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["fut_trades"]}],
        backtest_config=BacktestConfig(session_start="10:00", session_end="10:40"),
        engine_config=EngineConfig(timezone="America/New_York", params=params),
        task_name=f"t82 depth={'on' if enable_mbo else 'off'} {SYMBOL} {DAY}")
    print(f"enable_mbo={enable_mbo} run_id={run.run_id}", flush=True)
    return completed_checkpoint(run, CHECKPOINT)


def ratio(num, den):
    return num / den if den else 0.0


if __name__ == "__main__":
    depth = run_once(True)
    control = run_once(False)

    filled = [p for p in depth["passive"] if p["fill_ts"] is not None]
    waited = [p for p in filled
              if p["queue_ahead"] > 0 and p["first_print_ts"] is not None
              and p["fill_ts"] > p["first_print_ts"]]
    exits = [e for e in depth["exits"] if e["fill_px"] is not None and e["bid_at_send"] is not None]
    checks = {
        "depth_on_trades": ratio(depth["depth_valid"], depth["trades"]) >= 0.95,
        "ten_levels_a_side": ratio(depth["ten_levels"], depth["depth_valid"]) >= 0.90,
        "book_never_crossed": depth["crossed"] == 0,
        "ladder_ordered_and_sized": depth["bad_ladder"] == 0,
        "tape_inside_book": ratio(depth["inside"], depth["depth_valid"]) >= 0.95,
        "passive_orders_placed": len(depth["passive"]) >= CYCLES // 2 and depth["rejects"] == 0,
        "passive_fills": len(filled) >= 1,
        "passive_fill_at_limit": all(p["fill_px"] == p["limit"] for p in filled),
        "passive_fill_needs_print": all(p["print_before_fill"] for p in filled),
        "queue_ahead_honored": len(waited) >= 1,
        "exit_at_touch": bool(exits) and all(e["fill_px"] >= e["bid_at_send"] - TICK for e in exits),
        "control_has_no_depth": control["trades"] > 0 and control["depth_valid"] == 0,
    }
    finish(CHECKPOINT, checks, extra=(
        f"depth: trades={depth['trades']} valid={depth['depth_valid']} ten={depth['ten_levels']} "
        f"inside={depth['inside']} crossed={depth['crossed']} bad_ladder={depth['bad_ladder']} "
        f"symbol={depth['depth_symbol']} passive={len(depth['passive'])} filled={len(filled)} "
        f"waited={len(waited)} exits={len(exits)} samples={depth['samples']} | "
        f"control: trades={control['trades']} valid={control['depth_valid']} "
        f"passive={len(control['passive'])}"))
