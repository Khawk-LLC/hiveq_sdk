"""Reproducibly map every examples/bt source file to release-validation evidence."""
from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
INVENTORY = HERE / "examples_bt_inventory.json"

HELPERS = {
    "daily_time_filter_common.py",
    "data_fetch_validation_strategy.py",
    "test_utils.py",
    "validation_utils.py",
}

DIAGNOSTICS = {
    "benchmark_bars_market_data.py": "non-deterministic throughput benchmark, retained outside release gates",
    "benchmark_bars_profiled.py": "profiling wrapper, not a distinct functional contract",
    "noop_perf_baseline.py": "performance baseline, not a functional pass/fail contract",
    "performance_test_intraday.py": "wall-clock performance harness, environment-dependent",
    "performance_test_overnight.py": "wall-clock performance harness, environment-dependent",
    "sigma_timing_profile.py": "profiling harness, environment-dependent",
    "oom.py": "intentional memory-stress reproducer, unsafe as a release gate",
    "probe_market_center_latency.py": "uses undocumented latency_millis engine diagnostics",
    "validate_vs_backtrader.py": "third-party Backtrader comparison, not an SDK contract",
    "notebook_simulation_class.py": "interactive notebook demonstration, not a validation oracle",
    "probe_userdata_time_window.py": "uses undocumented data_configs start_time/end_time fields",
    "test_deploy_publish.py": "C++ publisher logging diagnostic without a publication-result oracle",
    "pre_built_compare_daily.py": "references the platform-internal UserSignalStrategy, outside the public authoring API",
    "pre_built_compare_prefetch.py": "references the platform-internal UserSignalStrategy, outside the public authoring API",
    "pre_built_strategy.py": "references the platform-internal UserSignalStrategy and obsolete UTC parameter guidance",
    "user_signal_strategy_examples.py": "configuration-only reference for a platform-internal strategy, with obsolete UTC guidance",
    "22_user_signal_asset_types.py": "configuration-only reference for a platform-internal strategy, including unsupported crypto data",
    "tca_example.py": "TCA report presentation example, not a standalone execution release gate",
}

# Ordered: specific behaviors must win before broad words such as bars/order/futures.
RULES = [
    (("inverse_head_shoulders", "es_scalp"),
     "t41_stop_limit_latch.py,t46_stop_bracket_orders.py,t05_order_lifecycle.py",
     "latched STOP_LIMIT behavior, protective exits, and manual sibling cancellation"),
    (("strat8_deploy",), "t44_cluster_analytics_data.py,t07_timers.py,t05_order_lifecycle.py",
     "cluster analytics evidence plus timer and direct-order lifecycle"),
    (("options_signals_strategy", "zerodte_custom_data", "zerodte_signal_strategy", "signal_dynamic_data"),
     "t26_option_filters.py,t40_option_order_fill.py,t45_multileg_options.py,t36_custom_csv_routing.py,t37_hosted_signals.py",
     "combined option and custom/hosted-signal routing"),
    (("option", "0dte", "snap", "strike", "expiration", "iron_condor", "zerodte", "all_filters"),
     "t26_option_filters.py,t40_option_order_fill.py,t45_multileg_options.py",
     "option snapshots, filters, single-contract fills, and multi-leg baskets"),
    (("executor", "watermark", "peg_direct"), "t39_executor_lifecycle.py", "managed executor lifecycle"),
    (("rollover",), "t08_rollover.py,t38_multi_rollover_squareoff.py", "roll callback, carry, and repeated square-off"),
    (("session", "time_filter", "timezone", "calendar_day"),
     "t21_session_start_boundaries.py,t27_equity_futures_sessions.py,t35_calendar_day_start.py",
     "ET sessions, DST/weekends, and calendar-day callbacks"),
    (("holiday",), "t22_equity_holidays.py", "holiday suppression"),
    (("custom_data", "signal_data", "signals_", "userdata"),
     "t36_custom_csv_routing.py,t37_hosted_signals.py", "custom CSV and hosted signal routing"),
    (("global_function",), "t23_global_dispatch.py", "module-level dispatch"),
    (("global_portfolio", "multi_strategy_portfolio"), "t25_global_portfolio.py", "global aggregation"),
    (("moo_moc", "auction"), "t24_auction_orders.py", "auction order fills"),
    (("reject", "offtick", "order_gate"), "t18_reject_reason.py,t19_order_gate.py", "local and engine rejection paths"),
    (("cancel_order", "modify_order", "order_management"), "t20_cancel_modify_order.py", "order query/modify/cancel lifecycle"),
    (("limit_ioc", "unfilled_limit", "limit_simple"), "t30_ioc_gtc_limits.py", "limit persistence and TIF"),
    (("daily_bars_fill", "bar_fill_validation"), "t17_daily_bars_fill.py", "daily-bar close fills"),
    (("enum_str",), "t16_enum_str_stability.py", "enum wire values"),
    (("error_visibility", "crash", "trace_crash"), "t32_callback_error_visibility.py", "callback failure visibility"),
    (("bars_vs_clickhouse",), "t42_stream_data_api_parity.py", "stream/Data API count parity"),
    (("all_asset", "fills_all_assets"),
     "t03_all_asset_types.py,t28_cash_equity_exposure.py,t38_multi_rollover_squareoff.py,t40_option_order_fill.py",
     "multi-asset delivery plus equity, futures, and option fills"),
    (("pnl", "cash_equity", "exposure", "sharpe"),
     "t06_position_portfolio.py,t10_metrics_validation.py,t28_cash_equity_exposure.py", "PnL, returns, cash, and exposure arithmetic"),
    (("timer", "adaptive_parameters"), "t07_timers.py,t33_parameter_changes.py", "timers and adaptive parameter evidence"),
    (("dynamic_subscription",), "t02_successive_accumulate.py", "successive subscription accumulation"),
    (("tbbo", "trades_quotes", "equity_trades", "order_flow"),
     "t12_quotes_tbbo.py,t43_equity_tbbo_trade_quote.py", "trade/quote delivery and payloads"),
    (("on_position_not_firing",), "t06_position_portfolio.py,t29_long_short_transitions.py", "position callbacks and transitions"),
    (("index",), "t02_successive_accumulate.py,t03_all_asset_types.py", "index values and bars"),
    (("futures", "subscribe_futures", "data_flow"),
     "t03_all_asset_types.py,t27_equity_futures_sessions.py", "futures data and sessions"),
    (("bars", "bar_data", "equity_data", "data_availability", "multi_day", "prefetch", "daily"),
     "t01_bars_multi_interval.py,t42_stream_data_api_parity.py", "bar delivery, intervals, and counts"),
    (("order", "market", "fill", "hold_and_close", "buy_and_hold", "sma", "stat_arb", "scalp",
      "head_shoulders", "example.py", "validate.py", "ai_test", "rerun", "strat8", "pre_built",
      "user_signal", "sigma_backtest", "deploy_publish"),
     "t05_order_lifecycle.py,t06_position_portfolio.py,t29_long_short_transitions.py",
     "direct-order lifecycle and position state; application signal math adds no SDK surface"),
    (("subscribe", "data_exploration", "no_filtering"), "t03_all_asset_types.py", "market-data subscription delivery"),
]


def mapping(file_name: str) -> tuple[str, str, str | None]:
    base = file_name.rsplit("/", 1)[-1]
    if base in HELPERS:
        return "helper", "shared source helper/configuration; no standalone behavior", None
    if base in DIAGNOSTICS:
        return "diagnostic_only", DIAGNOSTICS[base], None
    lower = file_name.lower()
    for needles, target, reason in RULES:
        if any(needle in lower for needle in needles):
            # Probe/validate/test files are the source behavior translated into the
            # canonical gate. Other examples are semantic duplicates of that gate.
            stem = base.lower()
            status = "ported" if stem.startswith(("probe_", "validate", "test_")) else "duplicate"
            return status, reason, target
    raise RuntimeError(f"unclassified source file: {file_name}")


def main() -> None:
    doc = json.loads(INVENTORY.read_text())
    for row in doc["files"]:
        status, reason, target = mapping(row["file"])
        row.update(status=status, mapped_to=target, reason=reason)
    counts = Counter(row["status"] for row in doc["files"])
    if counts["pending"]:
        raise RuntimeError(f"pending inventory entries: {counts['pending']}")
    doc["status_counts"] = dict(sorted(counts.items()))
    INVENTORY.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"accounted={len(doc['files'])} status_counts={dict(counts)}")


if __name__ == "__main__":
    main()
