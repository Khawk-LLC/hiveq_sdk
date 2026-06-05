"""Trading types for order management and portfolio analysis.

This module defines the public enums used across HiveQ Flow strategies
(``OrderSide``, ``OrderType``, ``OrderStatus``, ``MarketCenter``).

The runtime objects (Order, Fill, Position, Portfolio) returned from the
context are provided by the active OMS adapter — see
``hiveq.flow.oms.sigma.types`` for the Sigma implementations. The
public names ``Order``, ``Fill``, ``Position``, and ``Portfolio`` are
re-exported from ``hiveq.flow`` as aliases of those classes.

See Also
--------
Context : Strategy context with order placement methods
"""
from enum import Enum


class _ValueStrMixin:
    """Mixin: ``str(member)`` returns the member's value, not ``ClassName.MEMBER``.

    Python 3.11 changed the default ``Enum.__str__`` to emit ``ClassName.NAME``
    where earlier versions returned ``ClassName.NAME`` for plain ``Enum`` but
    *did* return the value for ``(str, Enum)`` mixins — which is what older
    HiveQ-Flow callers got used to. Strategies that relied on
    ``str(OrderSide.BUY) == "BUY"`` (or used it in dispatch tables) broke
    silently when upgrading Python interpreters even when this SDK hadn't
    changed.

    Mixing this in restores the value-based ``str()`` shape across all
    Python versions — fully independent of upstream Python changes. Use
    ``.value`` (canonical) if you want the underlying string explicitly;
    ``.name`` for the enum identifier (which happens to be identical here
    because every member's name matches its value).
    """

    def __str__(self) -> str:  # type: ignore[override]
        return self.value


class OrderSide(_ValueStrMixin, Enum):
    """Order side enumeration.

    Specifies whether an order is buying or selling.

    Attributes
    ----------
    BUY : str
        Buy order (long entry or short cover)
    SELL : str
        Sell order (long exit or short entry)

    Examples
    --------
    >>> from hiveq.flow.trading_types import OrderSide
    >>> order = ctx.buy_order("AAPL", quantity=100)
    >>> if order.side == OrderSide.BUY:
    ...     print("Buying order")
    """
    BUY = "BUY"
    SELL = "SELL"


class OrderType(_ValueStrMixin, Enum):
    """Order type enumeration.

    Specifies the order execution type and pricing behavior.

    Attributes
    ----------
    MARKET : str
        Market order - executes immediately at best available price
    LIMIT : str
        Limit order - executes only at specified price or better
    STOP : str
        Stop market order - becomes market order when stop price reached
    STOP_LIMIT : str
        Stop limit order - becomes limit order when stop price reached
    MOO : str
        Market-On-Open order - executes at market open price
    MOC : str
        Market-On-Close order - executes at market close price
    LOO : str
        Limit-On-Open order - limit order that executes at market open
    LOC : str
        Limit-On-Close order - limit order that executes at market close
    """
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
    MOO = "MOO"  # Market-On-Open
    MOC = "MOC"  # Market-On-Close
    LOO = "LOO"  # Limit-On-Open
    LOC = "LOC"  # Limit-On-Close


class MarketCenter(_ValueStrMixin, Enum):
    """Market center / exchange enumeration for order routing.

    Specifies the exchange destination for order routing, particularly
    important for auction orders (MOO, MOC, LOO, LOC).
    """
    NYSE = "NYSE"
    NASDAQ = "NASDAQ"
    ARCA = "ARCA"
    BATS = "BATS"
    AMEX = "AMEX"
    CME = "CME"
    CBOE = "CBOE"
    NYMEX = "NYMEX"
    CBOT = "CBOT"


class OrderStatus(_ValueStrMixin, Enum):
    """Order status enumeration.

    Tracks order state throughout its lifecycle from submission to completion.

    Attributes
    ----------
    PENDING : str
        Order created but not yet submitted
    SUBMITTED : str
        Order submitted to exchange/broker
    ACCEPTED : str
        Order accepted by exchange/broker
    REJECTED : str
        Order rejected by exchange/broker
    CANCELED : str
        Order canceled before completion
    FILLED : str
        Order completely filled
    PARTIALLY_FILLED : str
        Order partially filled with remaining quantity pending
    """
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    CANCELED = "CANCELED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
