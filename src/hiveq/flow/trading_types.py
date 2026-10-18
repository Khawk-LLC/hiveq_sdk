"""Trading types for order management and portfolio analysis.

This module defines the public enums used across HiveQ Flow strategies
(``OrderSide``, ``OrderType``, ``OrderStatus``, ``MarketCenter``,
``PegType``, ``FixTag``, ``ExecInst``).

The runtime objects (Order, Fill, Position, Portfolio) returned from the
context are provided by the active OMS adapter — see
``hiveq.flow.oms.sigma.types`` for the Sigma implementations. The
public names ``Order``, ``Fill``, ``Position``, and ``Portfolio`` are
re-exported from ``hiveq.flow`` as aliases of those classes.

See Also
--------
Context : Strategy context with order placement methods
"""
from enum import Enum, IntEnum


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
    PEGGED : str
        Pegged order (FIX 40=P); the peg flavor is the order's ``peg_type``
        (``PegType``), e.g. ``PegType.MID_PRICE_PEG`` for mid-peg.

    The remaining members complete the FIX 4.2 OrdType(40) set.
    """
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
    MOO = "MOO"  # Market-On-Open
    MOC = "MOC"  # Market-On-Close
    LOO = "LOO"  # Limit-On-Open
    LOC = "LOC"  # Limit-On-Close
    # Rest of the FIX 4.2 OrdType(40) set. The peg flavor of PEGGED is peg_type
    # (PegType); venue-specific attributes go in custom_fix_params. An OrdType not
    # listed here can still be sent by overriding tag 40 through custom_fix_params.
    WITH_OR_WITHOUT = "WITH_OR_WITHOUT"
    LIMIT_OR_BETTER = "LIMIT_OR_BETTER"
    LIMIT_WITH_OR_WITHOUT = "LIMIT_WITH_OR_WITHOUT"
    ON_BASIS = "ON_BASIS"
    ON_CLOSE = "ON_CLOSE"
    FOREX_MARKET = "FOREX_MARKET"
    PREVIOUSLY_QUOTED = "PREVIOUSLY_QUOTED"
    PREVIOUSLY_INDICATED = "PREVIOUSLY_INDICATED"
    FOREX_LIMIT = "FOREX_LIMIT"
    FOREX_SWAP = "FOREX_SWAP"
    FOREX_PREVIOUSLY_QUOTED = "FOREX_PREVIOUSLY_QUOTED"
    FUNARI = "FUNARI"
    PEGGED = "PEGGED"  # 40=P; needs peg_type, e.g. PegType.MID_PRICE_PEG


class PegType(_ValueStrMixin, Enum):
    """Peg flavor of an ``OrderType.PEGGED`` order, sent as FIX ExecInst(18).

    Example (mid-peg)::

        ctx.buy_order("AAPL", 100, order_type=OrderType.PEGGED,
                      peg_type=PegType.MID_PRICE_PEG, limit_price=187.50)

    The peg type is kept on ``modify_order``; a replace changes qty and price only.
    """
    MID_PRICE_PEG = "MID_PRICE_PEG"  # 18=M: midpoint of bid and offer
    MARKET_PEG = "MARKET_PEG"        # 18=P: offer for buys, bid for sells
    PRIMARY_PEG = "PRIMARY_PEG"      # 18=R: bid for buys, offer for sells


class FixTag(IntEnum):
    """Named FIX tag numbers for ``custom_fix_params`` keys.

    Example (mid-peg routed to SGMA)::

        ctx.buy_order("AAPL", 100, order_type=OrderType.PEGGED,
                      peg_type=PegType.MID_PRICE_PEG, limit_price=187.50,
                      custom_fix_params={
                          FixTag.MAX_FLOOR: 0,
                          FixTag.MIN_QTY: 0,
                          FixTag.EX_DESTINATION: "SGMA",
                      })

    Any tag not listed here can still be passed as a plain int. Values given in
    ``custom_fix_params`` take priority over what the venue adapter derives. The Instinet
    adapter uses only its route tags (57, 1100) and sends no other custom tag.
    """
    ACCOUNT = 1
    EXEC_INST = 18
    HANDL_INST = 21
    ORDER_QTY = 38
    ORD_TYPE = 40
    PRICE = 44
    SIDE = 54
    TARGET_SUB_ID = 57
    TIME_IN_FORCE = 59
    STOP_PX = 99
    EX_DESTINATION = 100
    MIN_QTY = 110
    MAX_FLOOR = 111
    LOCATE_REQD = 114
    ON_BEHALF_OF_SUB_ID = 116
    EXPIRE_TIME = 126
    DELIVER_TO_COMP_ID = 128
    SECURITY_EXCHANGE = 207
    MAX_SHOW = 210
    PEG_DIFFERENCE = 211
    DISCRETION_INST = 388
    DISCRETION_OFFSET = 389
    EXPIRE_DATE = 432
    LOCATE_BROKER = 5700
    DQUOTE_ROUTE = 1100  # Sigma shorthand: picks a configured D-Quote route; not sent on the wire


class ExecInst(_ValueStrMixin, Enum):
    """FIX ExecInst(18) values, for ``custom_fix_params[FixTag.EXEC_INST]``.

    Several instructions can be combined space-separated, e.g.
    ``f"{ExecInst.NOT_HELD} {ExecInst.ALL_OR_NONE}"``. The peg flavor of a PEGGED
    order is not set here but through ``peg_type`` (PegType).
    """
    NOT_HELD = "1"
    WORK = "2"
    GO_ALONG = "3"
    OVER_THE_DAY = "4"
    HELD = "5"
    PARTICIPATE_DONT_INITIATE = "6"  # post-only / add-liquidity-only
    STRICT_SCALE = "7"
    TRY_TO_SCALE = "8"
    STAY_ON_BID_SIDE = "9"
    STAY_ON_OFFER_SIDE = "0"
    NO_CROSS = "A"
    OK_TO_CROSS = "B"
    CALL_FIRST = "C"
    PERCENT_OF_VOLUME = "D"
    DO_NOT_INCREASE = "E"
    DO_NOT_REDUCE = "F"
    ALL_OR_NONE = "G"
    INSTITUTIONS_ONLY = "I"
    LAST_PEG = "L"
    NON_NEGOTIABLE = "N"
    OPENING_PEG = "O"
    SUSPEND = "S"
    TRAILING_STOP_PEG = "a"



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
