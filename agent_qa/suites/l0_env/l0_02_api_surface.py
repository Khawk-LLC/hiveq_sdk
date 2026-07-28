"""l0_02: the installed package still exposes the API the suite asserts against.

Every other test in this suite is written against ``hiveq-sdk/docs/llms.txt``,
the canonical source-verified spec. This test closes the loop in the other
direction: it checks that the enum members, config classes and context methods
the spec documents are actually present in whichever ``hiveq.flow`` is
installed.

That matters for two reasons:

* It catches a renamed/removed symbol as one clear preflight failure instead of
  fifty ``AttributeError`` ERRORs across the corpus.
* It is the first thing the QA agent should look at after a commit touching
  ``config.py``, ``context.py`` or ``trading_types.py`` — a missing member here
  means the spec and the code have diverged, which is itself a finding.

Runs under both engines, because the two packages are supposed to present the
same authoring surface; a member present in one and missing in the other is a
genuine defect.
"""

import inspect
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core.profiles import detect_engine
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l0_02_api_surface"
SURFACE = "l0.api_surface"

# --- §12: exact enum members. Not a sample — the whole documented set.
EVENT_TYPES = """
START STOP BAR BAR_1_MIN BAR_5_MIN BAR_15_MIN BAR_30_MIN BAR_1_HOUR BAR_1_DAY
TICK TRADE QUOTE SNAP ORDER ORDER_SUBMITTED ORDER_ACCEPTED ORDER_REJECTED
ORDER_FILLED ORDER_CANCELED ORDER_DENIED ORDER_EMULATED ORDER_EXPIRED
ORDER_INITIALIZED ORDER_PENDING_CANCEL ORDER_PENDING_UPDATE ORDER_UPDATED
ORDER_TRIGGERED ORDER_RELEASED ORDER_CANCEL_REJECTED ORDER_MODIFY_REJECTED
POSITION POSITION_OPENED POSITION_CHANGED POSITION_CLOSED CUSTOM_DATA TIMER
INDEX_PRICE ROLLOVER EXECUTOR_EVENT SECURITY_EVENT
""".split()

ASSET_TYPES = ["EQUITY", "OPTIONS", "FUTURES", "CRYPTO", "INDEX"]
ORDER_TYPES = ["MARKET", "LIMIT", "STOP", "STOP_LIMIT", "MOO", "MOC", "LOO", "LOC"]
ORDER_SIDES = ["BUY", "SELL"]
ORDER_STATUSES = ["PENDING", "SUBMITTED", "ACCEPTED", "REJECTED", "CANCELED",
                  "FILLED", "PARTIALLY_FILLED"]
EVENT_LOG_TYPES = ["POSITION", "ORDER", "FILL", "CUSTOM_DATA", "USER_LOG",
                   "ENTRY_TRADE", "EXIT_TRADE", "PARAM_CHANGE"]
MARKET_CENTERS = ["NYSE", "NASDAQ", "ARCA", "BATS", "AMEX", "CME", "CBOE",
                  "NYMEX", "CBOT"]

# --- §5: the context methods tests call. Checked on the class, not an instance,
# because building a live context requires a running engine.
CONTEXT_METHODS = [
    # subscriptions (§5.1)
    "subscribe_bars", "subscribe_futures_bars", "subscribe_trades",
    "subscribe_quotes", "subscribe_data",
    # orders (§5.2)
    "buy_order", "sell_order", "short_order", "cancel_all_orders", "close_position",
    # queries (§5.3-5.5)
    "portfolio", "global_portfolio", "net_position", "is_flat", "get_order_state",
    # time + timers (§5.7-5.8)
    "now", "now_utc", "trading_day", "set_timer", "cancel_timer",
    # event logging (§5.9)
    "add_event_log", "log_parameter_change",
]


def members(enum_cls):
    return {m.name for m in enum_cls}


def main():
    install_crash_handler(NAME, SURFACE)
    c = Checks()
    c.note(f"engine={detect_engine()}")

    import hiveq.flow as hf
    from hiveq.flow import BacktestConfig, StrategyConfig
    from hiveq.flow.config import AssetType, EventLogType, EventType
    from hiveq.flow.trading_types import MarketCenter, OrderSide, OrderStatus, OrderType

    for label, cls, expected in (
        ("EventType", EventType, EVENT_TYPES),
        ("AssetType", AssetType, ASSET_TYPES),
        ("OrderType", OrderType, ORDER_TYPES),
        ("OrderSide", OrderSide, ORDER_SIDES),
        ("OrderStatus", OrderStatus, ORDER_STATUSES),
        ("EventLogType", EventLogType, EVENT_LOG_TYPES),
        ("MarketCenter", MarketCenter, MARKET_CENTERS),
    ):
        missing = sorted(set(expected) - members(cls))
        c.add(f"{label}_members", not missing, f"missing={missing}")

    # §12 also pins `.value == name` for EventType/AssetType; code that switches
    # on the string form breaks silently if that ever stops holding.
    bad_values = [m.name for m in EventType if m.value != m.name]
    c.add("EventType_value_equals_name", not bad_values, f"mismatched={bad_values[:5]}")

    # §5 — the context surface.
    #
    # `hiveq.flow.context.Context` is only a public *type-hint alias*: its
    # __init__ raises, and it carries none of the methods. The runtime object is
    # `hiveq.flow.oms.sigma.sigma_context.SigmaContext`, which exists in the fat
    # engine package and not in the thin client — remotely the context lives in
    # the executor, so there is nothing local to introspect. Check the alias in
    # both engines and the real surface only where it can exist.
    try:
        from hiveq.flow.context import Context  # noqa: F401
        c.add("context_alias_importable", True)
    except ImportError as exc:
        c.add("context_alias_importable", False, str(exc))

    try:
        from hiveq.flow.oms.sigma.sigma_context import SigmaContext
        c.add("runtime_context_importable", True)
    except ImportError as exc:
        SigmaContext = None
        c.add("runtime_context_importable", False, str(exc))

    # The thin client ships SigmaContext as an empty stub for type hints, so the
    # import succeeding proves nothing there — only the fat engine package holds
    # the real implementation. Assert the method surface where it can exist.
    if SigmaContext is not None and detect_engine() == "inproc":
        missing_ctx = [m for m in CONTEXT_METHODS if not hasattr(SigmaContext, m)]
        c.add("context_methods_present", not missing_ctx, f"missing={missing_ctx}")
    else:
        c.note("context method surface not asserted under --engine=remote: the "
               "SDK's SigmaContext is a type stub and the real context lives in "
               "the executor")

    # §2 — run_backtest keeps the six positional parameters both packages share.
    sig = inspect.signature(hf.run_backtest)
    required = ["strategy_configs", "symbols", "start_date", "end_date",
                "data_configs", "backtest_config"]
    missing_params = [p for p in required if p not in sig.parameters]
    c.add("run_backtest_signature", not missing_params, f"missing={missing_params}")

    # §13 — config dataclasses used throughout the corpus.
    c.add("StrategyConfig_fields",
          all(hasattr(StrategyConfig, "__dataclass_fields__") and f in StrategyConfig.__dataclass_fields__
              for f in ("name", "type")),
          "StrategyConfig must expose name/type (type is the class name string, R2)")
    c.add("BacktestConfig_fields",
          all(f in getattr(BacktestConfig, "__dataclass_fields__", {})
              for f in ("start_date", "end_date", "session_start", "session_end")),
          f"got={sorted(getattr(BacktestConfig, '__dataclass_fields__', {}))[:12]}")

    c.finish(NAME, surface=SURFACE)


if __name__ == "__main__":
    main()
