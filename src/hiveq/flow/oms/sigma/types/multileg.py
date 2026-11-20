"""Multileg (spread) order types.

A multileg order is one *package*: 2..12 legs that fill together at one net
price. The strategy places it with ``ctx.place_multileg_order`` and follows it
through ``on_multileg`` (payload: :class:`SigmaMultilegEvent`).

Plain Python snapshots: values are copied out of the engine when the event is
built, so a strategy may keep them after the callback returns.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from hiveq.flow.trading_types import OrderSide


@dataclass(frozen=True)
class MultilegLeg:
    """One leg of a package, as the strategy specifies it.

    Parameters
    ----------
    symbol : str
        Leg instrument, e.g. an OCC option ticker ``"SPXW  230616C04400000"``.
    side : OrderSide
        ``OrderSide.BUY`` or ``OrderSide.SELL`` for one package.
    ratio : int
        Contracts of this leg per package (>= 1). Ratios are reduced across the
        legs before sending (2:2 x 1 is sent as 1:1 x 2).
    open_close : str
        ``'O'`` opening or ``'C'`` closing. Required by option venues; the
        backtest simulator ignores it.
    """
    symbol: str
    side: OrderSide
    ratio: int = 1
    open_close: str = 'O'


@dataclass(frozen=True)
class SigmaMultilegLegState:
    """Fill progress of one leg of a package."""
    leg_ref_id: str
    symbol: str
    side: OrderSide
    ratio: int
    open_close: str
    filled_qty: float
    avg_price: float


@dataclass(frozen=True)
class SigmaMultilegOrder:
    """A package: identity, terms and fill progress.

    ``quantity`` / ``filled_qty`` / ``leaves_qty`` count whole packages; a leg's
    contracts are ``quantity * leg.ratio``. ``net_price`` is per package
    (positive = debit paid, negative = credit received).
    """
    package_id: str
    clordid: str
    account: str
    net_price: float
    quantity: float
    filled_qty: float
    leaves_qty: float
    status: str               # PENDING_NEW, NEW, PARTIALLY_FILLED, FILLED, CANCELLED, REJECTED
    is_open: bool
    pending_replace: bool
    pending_cancel: bool
    text: str
    legs: tuple = field(default_factory=tuple)   # tuple[SigmaMultilegLegState, ...]

    @property
    def is_filled(self) -> bool:
        return self.status == 'FILLED'


@dataclass(frozen=True)
class SigmaMultilegEvent:
    """Payload of ``on_multileg``: one package transition.

    ``event_type`` is one of NEW_REQUEST, ACCEPTED, LEG_FILL, PARTIALLY_FILLED,
    FILLED, REPLACE_REQUEST, REPLACED, REPLACE_REJECTED, CANCEL_REQUEST,
    CANCELLED, CANCEL_REJECTED, REJECTED. The leg-fill fields are set only for
    LEG_FILL (``None`` otherwise); the fill is already in the leg's position.
    """
    event_type: str
    order: SigmaMultilegOrder
    text: str
    ts_event: int
    leg_ref_id: Optional[str] = None
    symbol: Optional[str] = None
    side: Optional[OrderSide] = None
    fill_qty: Optional[float] = None
    fill_price: Optional[float] = None
    exec_id: Optional[str] = None

    @property
    def package_id(self) -> str:
        return self.order.package_id

    @property
    def is_leg_fill(self) -> bool:
        return self.event_type == 'LEG_FILL'

    @property
    def is_terminal(self) -> bool:
        return self.event_type in ('FILLED', 'CANCELLED', 'REJECTED')


def _side(sigma_side) -> OrderSide:
    name = getattr(sigma_side, 'name', str(sigma_side))
    return OrderSide.BUY if name == 'BUY' else OrderSide.SELL


def leg_state_from_sigma(leg) -> SigmaMultilegLegState:
    return SigmaMultilegLegState(
        leg_ref_id=leg.leg_ref_id, symbol=leg.symbol, side=_side(leg.side), ratio=int(leg.ratio),
        open_close=leg.open_close, filled_qty=float(leg.filled_qty), avg_price=float(leg.avg_price))


def order_from_sigma(pkg) -> SigmaMultilegOrder:
    """Build from a PySigma ``MultilegPackageInfo`` snapshot."""
    return SigmaMultilegOrder(
        package_id=pkg.package_id, clordid=pkg.clordid, account=pkg.account,
        net_price=float(pkg.net_price), quantity=float(pkg.order_qty),
        filled_qty=float(pkg.filled_qty), leaves_qty=float(pkg.leaves_qty),
        status=getattr(pkg.state, 'name', str(pkg.state)), is_open=bool(pkg.is_open),
        pending_replace=bool(pkg.pending_replace), pending_cancel=bool(pkg.pending_cancel),
        text=pkg.text, legs=tuple(leg_state_from_sigma(l) for l in pkg.legs))


def event_from_sigma(ev) -> SigmaMultilegEvent:
    """Build from a PySigma ``MultilegEvent`` snapshot."""
    fill = ev.leg is not None
    return SigmaMultilegEvent(
        event_type=ev.type.name, order=order_from_sigma(ev.package), text=ev.text,
        ts_event=int(ev.ts_event_ns or 0),
        leg_ref_id=ev.leg.leg_ref_id if fill else None,
        symbol=ev.symbol if fill else None,
        side=_side(ev.side) if fill else None,
        fill_qty=float(ev.fill_qty) if fill else None,
        fill_price=float(ev.fill_price) if fill else None,
        exec_id=ev.exec_id if fill else None)
