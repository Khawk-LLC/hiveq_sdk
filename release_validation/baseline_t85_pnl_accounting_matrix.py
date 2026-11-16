"""Realized/unrealized PnL and equity through every position transition, per fill.

A fill can only open, add, reduce, close, or reverse a position, long or short.
The paths below cover every one of those transitions plus the boundary cases --
a 1-lot (1-share, 1-contract) reversal, an add after a partial exit, and a
multi-symbol portfolio whose net quantity is zero -- for futures, equities and
options. Every step is a single ``ctx.order_to_target``, so a step that changes
sign is a direct reversal (one order crossing zero).

Each scenario keeps its own average-cost ledger built only from the fills the
engine delivered (``on_order`` filled-quantity and average-price deltas). After
every fill and on every bar/snap, ``ctx.portfolio()`` position, realized PnL,
unrealized PnL (at the engine's own mark), fees and equity must equal it.

Regressions this guards (fixed in sigma 0.3.24):
* a direct reversal re-counted the closed leg in realized PnL until flat;
* a 1-lot side of a reversal was not seen as a cross-over, so the new leg was
  marked at the old entry price;
* portfolio unrealized PnL was zeroed whenever quantities netted to zero
  across symbols (long 3 ES / short 3 NQ);
* adding to a position after a partial exit re-averaged the cost of units
  already sold, moving realized PnL without any exit.

Window: 2026-06-01 is the date where ES/NQ 1m bars, AAPL 1m bars and SPXW 0DTE
option snaps all deliver (see t03).
"""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import checkpoint, completed_checkpoint, emit_checkpoint, evidence_checks, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, BacktestConfig

NAME = "t85_pnl_accounting_matrix"
DATE = "2026-06-01"
ES, NQ, AAPL, SPXW = "ES.c.0", "NQ.c.0", "AAPL", "SPXW"
INITIAL = 1_000_000.0
FEES = {"FUTURES": 1.25, "EQUITY": 0.005, "OPTIONS": 0.0}   # per contract / share
TOL = 0.01                                                   # dollars
BAR_STEP, SNAP_STEP = 6, 60      # ticks between steps: 6 x 1m bars, 60 x 1s snaps

PATH = [3, 5, 3, 4, -2, -3, -1, 0]   # open, add, reduce, add after reduce, reverse,
                                     # add short, reduce short, close
ONE_LOT = [1, -1, 1, 0]              # 1-lot reversals both ways

# scenario -> (asset, {symbol: [target, ...]}); targets are applied one per step.
SCENARIOS = {
    "fut_long_path":        ("FUTURES", {ES: PATH}),
    "fut_short_path":       ("FUTURES", {ES: [-t for t in PATH]}),
    "fut_one_lot_reverse":  ("FUTURES", {ES: ONE_LOT}),
    "fut_pairs_es_nq":      ("FUTURES", {ES: [3, -2, 0], NQ: [-3, 2, 0]}),
    "eq_long_path":         ("EQUITY",  {AAPL: [100 * t for t in PATH]}),
    "eq_one_share_reverse": ("EQUITY",  {AAPL: ONE_LOT}),
    "opt_long_path":        ("OPTIONS", {SPXW: PATH}),
    "opt_one_lot_reverse":  ("OPTIONS", {SPXW: ONE_LOT}),
}


class SdkT85Ledger:
    """Average-cost book for one symbol, fed only by delivered fills."""

    def __init__(self, fee):
        self.fee = fee
        self.pos = self.avg_cost = self.realized = self.fees = 0.0

    def fill(self, buy: bool, qty: float, px: float, mult: float) -> None:
        self.fees += self.fee * qty
        if self.pos != 0 and (self.pos > 0) != buy:            # reduce / close / reverse
            closed = min(abs(self.pos), qty)
            side = 1.0 if self.pos > 0 else -1.0
            self.realized += (px - self.avg_cost) * closed * mult * side
            self.pos -= side * closed
            opened = qty - closed
            if abs(self.pos) < 1e-9:
                self.pos = self.avg_cost = 0.0
            if opened > 1e-9:                                   # crossed zero: new leg at px
                self.pos, self.avg_cost = (opened if buy else -opened), px
        else:                                                   # open / add
            new_pos = self.pos + (qty if buy else -qty)
            self.avg_cost = (self.avg_cost * abs(self.pos) + px * qty) / abs(new_pos)
            self.pos = new_pos

    def unrealized(self, mark: float, mult: float) -> float:
        return 0.0 if self.pos == 0 else (mark - self.avg_cost) * self.pos * mult


class SdkT85:
    def on_start(self, ctx, event):
        self.scenario = ctx.strategy_config.params["scenario"]
        asset, self.plan = SCENARIOS[self.scenario]
        self.asset = getattr(AssetType, asset)
        self.legs = list(self.plan)                  # plan keys: ES.c.0, AAPL, SPXW ...
        self.contract = {}                           # leg -> traded symbol (option contract)
        self.ticks = 0
        self.step = {leg: 0 for leg in self.legs}
        self.ledgers = {leg: SdkT85Ledger(FEES[asset]) for leg in self.legs}
        self.order_seen = {}                         # order_id -> (filled_qty, avg_px)
        self.positions_seen = {leg: [] for leg in self.legs}
        self.mults = {}
        self.mismatches = []
        self.checks_run = self.fills = 0
        if self.asset == AssetType.OPTIONS:
            ctx.subscribe_option_snaps(SPXW, option_type="C", expiration_type="0dte", interval="1s")
        else:
            ctx.subscribe_bars(self.legs, asset_type=self.asset, interval="1m")

    # -- symbols -----------------------------------------------------------------
    def _traded(self, leg):
        return self.contract.get(leg, leg)

    def _leg_of(self, symbol):
        """Map a delivered symbol (ESM6, an OCC contract, AAPL) to its scenario leg."""
        symbol = str(symbol)
        for leg in self.legs:
            traded = self._traded(leg)
            if symbol == traded or (self.asset == AssetType.FUTURES and symbol[:2] == leg[:2]):
                return leg
        return None

    def _mult(self, ctx, leg):
        if leg not in self.mults:
            self.mults[leg] = float(ctx.instrument(self._traded(leg), self.asset).multiplier or 1.0)
        return self.mults[leg]

    # -- reconciliation ----------------------------------------------------------
    def _reconcile(self, ctx, where):
        pf = ctx.portfolio()
        self.checks_run += 1
        # Read the engine values first: unrealized_pnl() is what re-marks positions,
        # so position.last_price is only current after it.
        got = {"realized": float(pf.realized_pnl()), "unrealized": float(pf.unrealized_pnl()),
               "fees": float(pf.fees), "equity": float(pf.equity)}
        want_realized = want_unrealized = want_fees = 0.0
        for leg, led in self.ledgers.items():
            traded = self._traded(leg)
            got_pos = float(pf.net_position(traded)) if leg in self.contract or self.asset != AssetType.OPTIONS else 0.0
            if abs(got_pos - led.pos) > 1e-9:
                self._miss(ctx, where, f"{traded} position engine={got_pos} ledger={led.pos}")
            want_realized += led.realized
            want_fees += led.fees
            if led.pos:
                position = pf.position(traded)
                mark = float(position.last_price) if position is not None else 0.0
                if not mark:
                    self._miss(ctx, where, f"{traded} has no engine mark with position {led.pos}")
                    return
                want_unrealized += led.unrealized(mark, self._mult(ctx, leg))
        want = {"realized": want_realized, "unrealized": want_unrealized, "fees": want_fees,
                "equity": INITIAL + want_realized + want_unrealized - want_fees}
        for label in ("realized", "unrealized", "fees", "equity"):
            if abs(got[label] - want[label]) > TOL:
                self._miss(ctx, where, f"{label} engine={got[label]:.2f} ledger={want[label]:.2f}")

    def _miss(self, ctx, where, text):
        if len(self.mismatches) < 25:
            self.mismatches.append(f"tick{self.ticks} {where}: {text}")
            ctx.add_event_log(f"{self.scenario} mismatch", sub_event_type="T85_MISMATCH",
                              state_variable={"tick": self.ticks, "where": where, "detail": text})

    # -- driving -----------------------------------------------------------------
    def _tick(self, ctx, step_every):
        self.ticks += 1
        self._reconcile(ctx, "tick")
        for leg in self.legs:
            targets, i = self.plan[leg], self.step[leg]
            traded = self._traded(leg)
            if i < len(targets) and self.ticks >= 1 + i * step_every and not ctx.has_open_order(traded):
                ctx.order_to_target(traded, target_quantity=targets[i])
                self.step[leg] = i + 1

    # Every strategy in the run receives every stream, so each scenario steps only on
    # its own asset's data.
    def on_bar(self, ctx, event):
        if self.asset == AssetType.OPTIONS:
            return
        if self._leg_of(event.data().symbol) == self.legs[0]:     # one tick per minute
            self._tick(ctx, BAR_STEP)

    def on_snap(self, ctx, event):
        if self.asset != AssetType.OPTIONS:
            return
        snap = event.data()
        if SPXW not in self.contract:
            bid, ask = snap.bid_px, snap.ask_px
            if not bid or not ask or bid <= 0 or ask < bid or ask < 1.0:
                return                                   # wait for a liquid, two-sided contract
            self.contract[SPXW] = str(snap.chain)
        if str(snap.chain) == self.contract[SPXW]:
            self._tick(ctx, SNAP_STEP)

    def on_order(self, ctx, event):
        order = event.data()
        filled = float(order.filled_qty or 0)
        oid = str(order.order_id)
        prev_filled, prev_avg = self.order_seen.get(oid, (0.0, 0.0))
        if filled <= prev_filled + 1e-12:
            return
        avg = float(order.avg_px or 0)
        qty = filled - prev_filled
        px = (filled * avg - prev_filled * prev_avg) / qty      # exact for any fill split
        self.order_seen[oid] = (filled, avg)
        leg = self._leg_of(order.symbol)
        if leg is None:
            self._miss(ctx, "fill", f"fill for unknown symbol {order.symbol}")
            return
        side = str(order.side).upper()
        buy = "BUY" in side and "SELL" not in side
        led = self.ledgers[leg]
        led.fill(buy, qty, px, self._mult(ctx, leg))
        self.fills += 1
        if not self.positions_seen[leg] or self.positions_seen[leg][-1] != led.pos:
            self.positions_seen[leg].append(led.pos)
        ctx.add_event_log(f"{self.scenario} fill", sub_event_type="T85_FILL",
                          state_variable={"symbol": self._traded(leg), "buy": buy, "qty": qty,
                                          "px": px, "ledger_pos": led.pos,
                                          "ledger_realized": led.realized,
                                          "engine_realized": float(ctx.portfolio().realized_pnl()),
                                          "engine_equity": float(ctx.portfolio().equity)})
        self._reconcile(ctx, "fill")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, f"{NAME}:{self.scenario}", {
            "contract": self.contract,
            "positions_seen": self.positions_seen,
            "fills": self.fills,
            "checks_run": self.checks_run,
            "mismatches": self.mismatches,
        })


if __name__ == "__main__":
    configs = [StrategyConfig(name=f"SdkT85_{s}", type="SdkT85", symbols=list(plan),
                              params={"scenario": s}) for s, (_, plan) in SCENARIOS.items()]
    schemas = [("HIVEQ_US_FUT", "bars_1m"), ("HIVEQ_US_EQ", "bars_1m"), ("HIVEQ_US_OPT", "snaps_1s")]
    run = hf.run_backtest(
        strategy_configs=configs,
        symbols=[ES, NQ, AAPL, SPXW], start_date=DATE, end_date=DATE,
        data_configs=[{"type": "hiveq_historical", "dataset": d, "schema": [s]} for d, s in schemas],
        backtest_config=BacktestConfig(initial_capital=INITIAL, commission=0.0, slippage=0.0,
                                       futures_fee=FEES["FUTURES"], equity_fee=FEES["EQUITY"],
                                       session_start="09:30", session_end="11:00",
                                       auto_flatten_at_close=False, enable_auto_rollover=False,
                                       export_orders_csv=True))
    checks, detail = {}, {}
    for n, (scenario, (_, plan)) in enumerate(SCENARIOS.items()):
        name = f"{NAME}:{scenario}"
        state = completed_checkpoint(run, name) if n == 0 else checkpoint(run, name)
        expected = {leg: [float(t) for t in targets] for leg, targets in plan.items()}
        checks[f"{scenario}_path"] = state["positions_seen"] == expected
        checks[f"{scenario}_reconciled"] = state["checks_run"] > 0 and not state["mismatches"]
        detail[scenario] = {"fills": state["fills"], "checks": state["checks_run"],
                            "contract": state["contract"], "path": state["positions_seen"],
                            "mismatches": state["mismatches"][:3]}
    orders = sum(len(targets) for _, plan in SCENARIOS.values() for targets in plan.values())
    checks.update(evidence_checks(run, orders=orders, trades=len(SCENARIOS)))
    finish(NAME, checks, extra=str(detail))
