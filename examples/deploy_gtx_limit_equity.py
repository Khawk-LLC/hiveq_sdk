#!/usr/bin/env python3
"""GTX limit order on Equity LiveSim — the order shape that works in every session.

Deploys straight to LiveSim (no backtest). On the first valid quote it sends ONE
LIMIT buy at the ask with ``time_in_force="GTX"``, logs the order's progress, and
then HOLDS the position. Nothing sells it unless you ask for a round trip.

Why LIMIT + GTX (§5.2):
  - **No MARKET orders in pre-market or after-hours** — they are rejected.
    A LIMIT with an explicit ``limit_price`` is the only order that works across
    pre-market, the regular session and after-hours.
  - **Time in force is separate from order type.** Left to default, a LIMIT gets
    ``GTC``; GTX must be asked for. A GTX order is not cancelled at the open or
    the close; it stays working until the container shuts down.
  - **Price off the live quote, not a bar.** A marketable buy sits at the ask, a
    sell at the bid. Quotes arrive on ``on_quote`` from the trade subscription
    (``eq_trades`` rows carry bid/ask). The quote is already on the tick grid,
    sub-penny prices for sub-$1 stocks included, so it goes out as is.
  - **The sizing helpers can't set GTX** (``order_to_target`` / ``close_position``
    / ``flatten_all``), so this calls ``buy_order`` / ``sell_order`` directly.
  - **One order at a time.** ``self.pending`` blocks a second send while one is
    working; it clears only on a terminal status in ``on_order``.

Params (``StrategyConfig.params``):
  qty        shares to buy (default 1)
  roundtrip  "true" -> after the buy fills, sell the same quantity back with a
             LIMIT/GTX at the bid, ending flat (default "false": hold)

A held position survives a container restart (§3.3); stopping or terminating the
strategy does not flatten it.

Run:
    python examples/deploy_gtx_limit_equity.py                     # buy 1 AAPL, hold
    python examples/deploy_gtx_limit_equity.py --roundtrip --cleanup
    python examples/deploy_gtx_limit_equity.py --symbol NVDA --qty 5
"""

import argparse
import time

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType              # not re-exported at hiveq.flow top level
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading_types import OrderStatus, OrderType

logger = _get_logger()          # module level — REQUIRED in every strategy (R10)

TERMINAL = {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}


class GtxLimitEquity:
    """Buy once at the ask with LIMIT/GTX; hold, or sell back at the bid."""

    def __init__(self):
        self.symbol = None
        self.qty = 1.0
        self.roundtrip = False
        self.bid = self.ask = None
        self.stage = "WAIT_QUOTE"   # WAIT_QUOTE -> BUYING -> HOLD | SELLING -> DONE
        self.pending = False        # an order is working: never send another

    def on_start(self, ctx, event):
        cfg = ctx.strategy_config
        self.symbol = cfg.symbols[0]
        # LiveSim may hand params back as strings — cast them.
        self.qty = float(cfg.params.get("qty", 1))
        self.roundtrip = str(cfg.params.get("roundtrip", "false")).lower() == "true"
        ctx.subscribe_trades([self.symbol], asset_type=AssetType.EQUITY)   # R3

        held = ctx.net_position(self.symbol)    # restored after a restart (§3.3)
        if held:
            self.stage = "HOLD"
            logger.warning(f"[START] already holding {held} {self.symbol}; no new buy")
        else:
            logger.warning(f"[START] {self.symbol} qty={self.qty} roundtrip={self.roundtrip}")

    def on_quote(self, ctx, event):
        q = event.data()                         # -> SigmaQuoteTick (§7.6)
        bid, ask = q.bid_price, q.ask_price
        if not (bid and ask and 0 < bid <= ask):
            return                               # one-sided or crossed: wait for a clean one
        self.bid, self.ask = bid, ask
        if self.pending:
            return
        if self.stage == "WAIT_QUOTE":
            self._send(ctx, buy=True, price=ask)
            self.stage = "BUYING"

    def on_order(self, ctx, event):
        order = event.data()                     # -> SigmaOrder (§7.3)
        if order.symbol != self.symbol:
            return
        logger.warning(
            f"[ORDER] {order.order_id} {order.side} {order.order_type} {order.time_in_force} "
            f"limit={order.limit_price} status={order.status} filled={order.filled_qty} "
            f"avg_px={order.avg_px} reject={order.reject_reason}"
        )
        if order.status not in TERMINAL:
            return                               # accepted / partial: keep waiting
        self.pending = False
        if order.status != OrderStatus.FILLED:
            logger.warning(f"[STOP] order ended {order.status}; not retrying, no MARKET fallback")
            self.stage = "DONE"
            return
        if self.stage == "BUYING":
            self.stage = "HOLD"
            logger.warning(f"[HOLD] long {ctx.net_position(self.symbol)} {self.symbol}")
            if self.roundtrip and self.bid:
                self._send(ctx, buy=False, price=self.bid)
                self.stage = "SELLING"
        elif self.stage == "SELLING":
            self.stage = "DONE"
            logger.warning(f"[DONE] flat: position={ctx.net_position(self.symbol)}")

    def _send(self, ctx, buy, price):
        place = ctx.buy_order if buy else ctx.sell_order
        order = place(self.symbol, self.qty, order_type=OrderType.LIMIT,
                      limit_price=price, time_in_force="GTX")
        if order is None:
            logger.warning(f"[SEND] {'buy' if buy else 'sell'} not placed")
            self.stage = "DONE"
            return
        self.pending = True
        logger.warning(f"[SEND] {'BUY' if buy else 'SELL'} {self.qty} {self.symbol} "
                       f"LIMIT {price} GTX (bid={self.bid} ask={self.ask})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--qty", type=float, default=1)
    parser.add_argument("--roundtrip", action="store_true",
                        help="Sell back at the bid after the buy fills (ends flat).")
    parser.add_argument("--observe", type=int, default=120,
                        help="Seconds to watch the orders (default 120).")
    parser.add_argument("--cleanup", action="store_true",
                        help="Terminate the strategy at the end. Does NOT flatten a held position.")
    parser.add_argument("--instance-name", default=f"GtxLimitEquity{int(time.time()) % 100000}")
    args = parser.parse_args()

    strategy = StrategyConfig(
        name="GtxLimitEquity",
        type="GtxLimitEquity",                       # the CLASS NAME, exactly (R2)
        symbols=[args.symbol],
        params={"assetType": "EQUITY",               # tag the asset — placement depends on it
                "qty": args.qty,
                "roundtrip": "true" if args.roundtrip else "false"},
    )
    deployment = hf.deploy_livesim(
        [strategy],
        data_configs=[{"type": "live", "market_data_source": "Activ", "schema": ["eq_trades"]}],
        instance_name=args.instance_name,
    )
    print(f"deployment_id={deployment.deployment_id}")

    state = deployment.wait(timeout=180)
    instance = (state.get("instance_names") or [args.instance_name])[0]
    print(f"status={state['status']} container={state.get('container_id')} instance={instance}")
    if state.get("failure_reason"):
        print(f"failure_reason={state['failure_reason']}")
        return 1

    # Reads are snapshots, not streams: poll until the expected orders are terminal.
    want = 2 if args.roundtrip else 1
    deadline = time.time() + args.observe
    while time.time() < deadline:
        time.sleep(10)
        orders = deployment.orders(limit=50)
        done = [o for o in orders if str(o.get("status", "")).upper() in
                ("FILLED", "CANCELED", "CANCELLED", "REJECTED")]
        if len(done) >= want:
            break
    for o in deployment.orders(limit=50):
        print(f"order: {o.get('side')} {o.get('quantity')} {o.get('symbol')} "
              f"{o.get('order_type')} {o.get('time_in_force')} limit={o.get('price')} "
              f"status={o.get('status')} avg_px={o.get('avg_px')}")
    print(f"positions: {deployment.positions()}")
    for line in deployment.logs(tail=200).splitlines():
        if any(tag in line for tag in ("[START]", "[SEND]", "[ORDER]", "[HOLD]", "[DONE]", "[STOP]")):
            print(line)

    if args.cleanup:
        deployment.terminate(strategy=instance)       # per-instance; does not flatten
        print(f"terminated={instance}")
    else:
        print(f"left running: hf.get_deployment('{deployment.deployment_id}')"
              f".terminate(strategy='{instance}')")
    return 0


if __name__ == "__main__":       # REQUIRED: the module is re-imported to rebuild the strategy
    raise SystemExit(main())
