"""Multileg (spread) packages: place / fill / modify / cancel / close on SPXW 0DTE snapshots.

One short window on 2026-04-24 (10:00-10:10 ET). Once the chain is quoted:

  A  4-leg iron condor (short puts/calls ATM -/+ 25, wings -/+ 50), 1 package,
     limit at the natural -> marketable, fills inside place_multileg_order
     (events arrive before the call returns; the id comes from NEW_REQUEST).
  B  3-leg 1:2:1 call butterfly (ATM -10 / ATM / ATM +10) at 0.05 debit, far
     below the market -> rests; modified to 0.10 (REPLACED), then cancelled.
  A' the reverse of A ('C' legs) at the natural -> fills; every leg ends flat.

Plus: a 1-leg package and a fractional quantity are refused (ValueError);
cancelling the filled package returns False; each leg fill raises on_position;
option fees are 0.65 per leg contract (llms.txt §5.3.1, §7.15, §13).
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish, orders_frame

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.trading_types import OrderSide

NAME = "t88_multileg_package"
OPTION_FEE = 0.65
BUY, SELL = OrderSide.BUY, OrderSide.SELL


class SdkT88:
    def on_start(self, ctx, event):
        ctx.subscribe_option_snaps("SPXW", expiration_type="0dte", interval="1s")
        self.quotes = {}        # chain -> (bid, ask)
        self.chain_of = {}      # ('C'|'P', strike) -> chain
        self.step = "WAIT"      # WAIT -> A_SENT -> B_MODIFY -> B_CANCEL -> CLOSE -> DONE
        self.sending = None     # which package a NEW_REQUEST belongs to
        self.ids = {}           # 'A' | 'B' | 'A_CLOSE' -> package id
        self.last = {}          # key -> latest SigmaMultilegOrder
        self.legs = {}          # key -> [(chain, side, ratio)]
        self.state = {
            "snaps": 0, "events": {"A": [], "B": [], "A_CLOSE": []},
            "leg_fills": {"A": [], "A_CLOSE": [], "B": []},
            "returned_id_matches_event_id": {}, "limits": {},
            "refused_one_leg": False, "refused_fractional": False,
            "modify_sent": False, "cancel_sent": False, "cancel_filled_returned": None,
            "position_events": 0, "final_positions": {}, "fees": None, "leg_contracts": 0,
            "achieved_net": {}, "errors": [],
        }

    # ---- helpers -----------------------------------------------------------
    def q(self, chain):
        bid, ask = self.quotes.get(chain, (0.0, 0.0))
        return (bid, ask) if bid > 0 and ask >= bid else None

    def natural(self, legs):
        total = 0.0
        for chain, side, ratio in legs:
            bid, ask = self.q(chain)
            total += ratio * (ask if side is BUY else -bid)
        return round(total, 2)

    def atm(self):
        best = None
        for (typ, k), call in self.chain_of.items():
            put = self.chain_of.get(("P", k))
            if typ != "C" or not put or not self.q(call) or not self.q(put):
                continue
            (cb, ca), (pb, pa) = self.q(call), self.q(put)
            diff = abs((cb + ca) - (pb + pa))
            if best is None or diff < best[0]:
                best = (diff, k)
        return best[1] if best else None

    def place(self, ctx, key, legs, net, qty=1, open_close="O"):
        self.legs[key] = legs
        self.state["limits"][key] = net
        self.sending = key
        pkg = ctx.place_multileg_order(
            [hf.MultilegLeg(c, s, r, open_close) for c, s, r in legs], net_price=net, quantity=qty)
        self.sending = None
        self.state["returned_id_matches_event_id"][key] = bool(pkg) and pkg == self.ids.get(key)
        if pkg:
            self.ids[key] = pkg
        return pkg

    def working(self, key):
        o = self.last.get(key)
        return o is not None and o.is_open and not o.pending_replace

    # ---- data --------------------------------------------------------------
    def on_snap(self, ctx, event):
        s = event.data()
        self.state["snaps"] += 1
        if not s.chain or s.bid_px is None or s.ask_px is None:
            return
        self.quotes[s.chain] = (float(s.bid_px), float(s.ask_px))
        self.chain_of[(str(s.option_type)[:1].upper(), float(s.strike))] = s.chain
        # Let the whole chain fill in (30s of snapshots) before choosing ATM.
        self.first_ts = getattr(self, "first_ts", None) or s.ts_event
        if s.ts_event - self.first_ts < 30_000_000_000:
            return
        try:
            self.drive(ctx)
        except Exception as exc:  # recorded; the checks fail on it
            self.state["errors"].append(f"{type(exc).__name__}: {exc}")
            self.step = "DONE"

    def drive(self, ctx):
        if self.step == "WAIT":
            k = self.atm()
            if k is None:
                return
            want = {("P", k - 25): SELL, ("P", k - 50): BUY, ("C", k + 25): SELL, ("C", k + 50): BUY}
            fly = [(("C", k - 10), BUY, 1), (("C", k), SELL, 2), (("C", k + 10), BUY, 1)]
            chains = {key: self.chain_of.get(key) for key in list(want) + [f[0] for f in fly]}
            if not all(c and self.q(c) for c in chains.values()):
                return
            condor = [(chains[key], side, 1) for key, side in want.items()]
            butterfly = [(chains[key], side, ratio) for key, side, ratio in fly]
            self.state["atm"] = k

            # Refusals: raised before anything reaches the engine.
            try:
                ctx.place_multileg_order([hf.MultilegLeg(condor[0][0], SELL)], net_price=-1.0, quantity=1)
            except ValueError:
                self.state["refused_one_leg"] = True
            try:
                ctx.place_multileg_order([hf.MultilegLeg(c, s) for c, s, _ in condor[:2]],
                                         net_price=-1.0, quantity=1.5)
            except ValueError:
                self.state["refused_fractional"] = True

            self.step = "A_SENT"
            self.place(ctx, "A", condor, self.natural(condor))          # marketable credit
            self.place(ctx, "B", butterfly, 0.05)                         # far below: rests
        elif self.step == "A_SENT" and self.working("B") and self.last["B"].status == "NEW":
            self.step = "B_MODIFY"      # before the call: REPLACED can arrive inside it
            self.state["modify_sent"] = ctx.modify_multileg_order(self.ids["B"], net_price=0.10)
        elif self.step == "B_CANCEL" and self.working("B"):
            self.step = "CLOSE"         # before the call: CANCELLED can arrive inside it
            self.state["cancel_sent"] = ctx.cancel_multileg_order(self.ids["B"])
        elif self.step == "CLOSE" and self.last.get("A") and self.last["A"].is_filled:
            self.state["cancel_filled_returned"] = ctx.cancel_multileg_order(self.ids["A"])
            close = [(c, SELL if s is BUY else BUY, r) for c, s, r in self.legs["A"]]
            if all(self.q(c) for c, _, _ in close):
                self.step = "DONE"
                self.place(ctx, "A_CLOSE", close, max(self.natural(close), 0.05), open_close="C")

    # ---- package events ----------------------------------------------------
    def on_multileg(self, ctx, event):
        ev = event.data()
        o = ev.order
        if ev.event_type == "NEW_REQUEST" and self.sending:
            self.ids[self.sending] = o.package_id
        key = next((k for k, v in self.ids.items() if v == o.package_id), None)
        if key is None:
            return
        self.last[key] = o
        self.state["events"][key].append(ev.event_type)
        if ev.is_leg_fill:
            leg = next(l for l in o.legs if l.leg_ref_id == ev.leg_ref_id)
            self.state["leg_fills"][key].append(
                [ev.leg_ref_id, ev.symbol, ev.side.value, leg.ratio, ev.fill_qty, ev.fill_price])
        if ev.event_type == "FILLED":
            self.state["achieved_net"][key] = round(sum(
                (l.avg_price if l.side is BUY else -l.avg_price) * l.ratio for l in o.legs), 4)
        if key == "B" and ev.event_type == "REPLACED" and self.step == "B_MODIFY":
            self.state["b_net_after_replace"] = o.net_price
            self.step = "B_CANCEL"

    def on_position(self, ctx, event):
        if any(event.data().symbol == c for legs in self.legs.values() for c, _, _ in legs):
            self.state["position_events"] += 1

    def on_stop(self, ctx, event):
        symbols = {c for legs in self.legs.values() for c, _, _ in legs}
        self.state["final_positions"] = {c: float(ctx.net_position(c)) for c in symbols}
        self.state["fees"] = float(ctx.portfolio().fees)
        self.state["leg_contracts"] = sum(f[4] for k in ("A", "A_CLOSE", "B") for f in self.state["leg_fills"][k])
        self.state["ids"] = self.ids
        emit_checkpoint(ctx, NAME, self.state)


def lifecycle(events):
    return [e for e in events if e not in ("LEG_FILL", "PARTIALLY_FILLED")]


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT88", type="SdkT88", symbols=["SPXW"])],
        symbols=["SPXW"],
        start_date="2026-04-24",
        end_date="2026-04-24",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_OPT", "schema": ["snaps_1s"]}],
        backtest_config=BacktestConfig(session_start="10:00", session_end="10:10",
                                       option_fee=OPTION_FEE, export_orders_csv=True),
    )
    s = completed_checkpoint(run, NAME)
    ev, fills, limits = s["events"], s["leg_fills"], s["limits"]

    def legs_whole(key, qty=1):
        return bool(fills[key]) and all(abs(f[4] - qty * f[3]) < 1e-9 for f in fills[key])

    orders = orders_frame(run)
    leg_rows = []
    if orders is not None and len(orders):
        ids = {str(v) for v in s.get("ids", {}).values()}
        col = "OrderID" if "OrderID" in orders.columns else "order_id"
        leg_rows = [str(i) for i in orders[col] if ":" in str(i) and str(i).split(":")[0] in ids]

    finish(NAME, {
        "snap_data_present": s["snaps"] > 0,
        "no_strategy_errors": not s["errors"],
        "one_leg_package_refused": s["refused_one_leg"],
        "fractional_quantity_refused": s["refused_fractional"],
        # A: 4 legs, fills within the place call, one LEG_FILL per leg, limit honoured
        "condor_lifecycle": lifecycle(ev["A"]) == ["NEW_REQUEST", "ACCEPTED", "FILLED"],
        "condor_four_leg_fills_whole": len({f[0] for f in fills["A"]}) == 4 and legs_whole("A"),
        "condor_credit_at_least_limit": "A" in s["achieved_net"] and s["achieved_net"]["A"] <= limits["A"] + 1e-9,
        "place_returns_package_id": all(s["returned_id_matches_event_id"].get(k) for k in ("A", "B", "A_CLOSE")),
        "cancel_of_filled_package_false": s["cancel_filled_returned"] is False,
        # B: 1:2:1 rests, REPLACED to 0.10, CANCELLED, never filled
        "butterfly_lifecycle": lifecycle(ev["B"]) == ["NEW_REQUEST", "ACCEPTED", "REPLACE_REQUEST",
                                                     "REPLACED", "CANCEL_REQUEST", "CANCELLED"],
        "butterfly_modify_and_cancel_sent": s["modify_sent"] and s["cancel_sent"],
        "butterfly_replaced_net": abs(s.get("b_net_after_replace", 0) - 0.10) < 1e-9,
        "butterfly_no_fills": not fills["B"],
        # A': reverse package closes every leg
        "close_lifecycle": lifecycle(ev["A_CLOSE"]) == ["NEW_REQUEST", "ACCEPTED", "FILLED"],
        "close_debit_at_most_limit": "A_CLOSE" in s["achieved_net"]
                                     and s["achieved_net"]["A_CLOSE"] <= limits["A_CLOSE"] + 1e-9,
        "legs_flat_at_stop": bool(s["final_positions"]) and all(abs(v) < 1e-9 for v in s["final_positions"].values()),
        "leg_position_events": s["position_events"] >= 8,
        "option_fee_per_leg_contract": s["fees"] is not None and s["leg_contracts"] > 0
                                       and abs(s["fees"] - OPTION_FEE * s["leg_contracts"]) < 1e-6,
        "report_has_leg_orders": len(set(leg_rows)) >= 8,
    }, extra=str({k: s[k] for k in ("atm", "limits", "achieved_net", "fees", "leg_contracts", "events")
                  if k in s}))
