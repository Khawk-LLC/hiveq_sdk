"""Stateless price math helpers tied to the active strategy bridge.

The framework registers the currently-dispatching bridge here so utilities like
``adjust_tick_size(symbol, price)`` can look up the instrument's tick without
forcing the caller to thread ``ctx`` through every call.

Usage from a strategy callback::

    from hiveq.flow.trading.price_utils import adjust_tick_size

    def on_trade(self, ctx, event):
        raw = event.data().price - 1.0
        limit = adjust_tick_size('ES.v.0', raw)
        ctx.buy_order('ES.v.0', 1, order_type=OrderType.LIMIT, limit_price=limit)

Internally this looks the symbol up against the active strategy bridge and
reads ``instrument.security.minTick``. If no tick is available (no bridge
registered, unknown symbol, missing reference data) the input is returned
unchanged so callers don't have to defensively wrap every call.

NOTE (thin client): on the client there is no active bridge, so these return
the input unchanged. The real tick lookup happens on the platform executor,
where the full engine registers the bridge during dispatch.
"""
from typing import Optional


# Module-level handle to the strategy bridge currently dispatching a callback.
# Single-threaded backtest assumption: each strategy registers its bridge when
# its SigmaContext is constructed; last-write-wins. Multi-strategy parallel
# dispatch (if/when added) would want a contextvars.ContextVar instead.
_active_bridge = None


def _set_active_bridge(bridge) -> None:
    """Framework hook: register the strategy bridge currently in scope."""
    global _active_bridge
    _active_bridge = bridge


def get_min_tick(symbol: str) -> Optional[float]:
    """Return the minTick for ``symbol`` from the active bridge, or None.

    Returns None (not a default) when tick info isn't available, so callers
    can branch on "tick known vs unknown" rather than silently treating it
    as zero.
    """
    if _active_bridge is None:
        return None
    try:
        inst = _active_bridge.get_instrument(symbol)
    except Exception:
        return None
    if inst is None:
        return None
    try:
        sec = getattr(inst, 'security', None)
        sec = sec() if callable(sec) else sec
        if sec is None:
            return None
        mt = getattr(sec, 'minTick', None)
        if mt is None:
            return None
        tick = float(mt() if callable(mt) else mt)
        return tick if tick > 0 else None
    except Exception:
        return None


def adjust_tick_size(symbol: str, price: float) -> float:
    """Round ``price`` to the nearest tick increment for ``symbol``.

    Looks up the instrument's tick size via the active strategy bridge. If the
    tick size cannot be determined (no bridge registered, unknown symbol, or
    reference data missing), the input price is returned unchanged.

    Parameters
    ----------
    symbol : str
        Trading symbol whose tick grid to round to.
    price : float
        Input price.

    Returns
    -------
    float
        Tick-aligned price, or the input price unchanged when tick is unknown.
    """
    if price is None or price <= 0:
        return price
    tick = get_min_tick(symbol)
    if tick is None or tick <= 0:
        return price
    return round(price / tick) * tick
