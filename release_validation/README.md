# HiveQ SDK installed-wheel release validation

Current conversion/accounting and execution results: [REPORT.md](REPORT.md).

This suite deduplicates and translates the validation behaviors from the 165
Python files under `hiveq-flow/examples/bt`
to the thin SDK's real execution model. Strategies run remotely, persist their
callback state through structured event-log checkpoints, and are asserted only
through public `Run` APIs.

The runner invokes each file from the project root, matching ordinary
`python release_validation/tNN_....py` usage and its normal `.env` resolution.
The convention/import audits verify the SDK strategy and package wiring.

Run each test individually after installing the candidate wheel and signing in:

```bash
python release_validation/t01_bars_multi_interval.py
```

Run `python release_validation/run_all.py` for the complete installed-wheel
scorecard. Each process is isolated because a remote OMS run is process-scoped.
The runner first executes `audit_sdk_convention.py`, which enforces `SdkTxx`
strategy naming, matching `StrategyConfig.type`, platform submission, and a
public result/log evidence reader.
It runs exactly one validation process at a time, waits for its platform result,
adds a short inter-test cooldown, and retries the same test on a max-concurrent
response. A timeout, infrastructure error, or unknown active-run state stops the
runner before another strategy is submitted.

Tests requiring datasets absent from QA report `GAP`; missing optional data is
not treated as an implementation regression. Core datasets remain hard gates.

## Review order and coverage

| SDK test | Contract |
|---|---|
| `t01_bars_multi_interval.py` | successive 1m/1d subscriptions and bar payloads |
| `t02_successive_accumulate.py` | successive trade/index subscriptions accumulate |
| `t03_all_asset_types.py` | equity, futures, index, and option streams coexist |
| `t04_callbacks_contract.py` | callback order, event types, timestamps, and context |
| `t05_order_lifecycle.py` | market fill, resting limit, cancel, and flatten |
| `t06_position_portfolio.py` | position callbacks, exposures, equity, and realized PnL |
| `t07_timers.py` | timer cadence, identifiers, and cancellation |
| `t08_rollover.py` | futures contract rollover and position carry |
| `t09_event_logs_reports.py` | structured event logs and every public Run result surface |
| `t10_metrics_validation.py` | independent PnL, fee, return, and trade arithmetic |
| `t11_imbalance_flow.py` | public imbalance callback contract; no QA rows is `GAP` |
| `t12_quotes_tbbo.py` | futures TBBO delivery and non-crossed quotes |
| `t13_arca_imbalance.py` | NYSE Arca imbalance delivery and venue-specific fields |
| `t14_nasdaq_imbalance.py` | Nasdaq imbalance delivery, indicative prices, and cross type |
| `t15_nyse_imbalance.py` | NYSE imbalance delivery and clearing-price fields |
| `t16_enum_str_stability.py` | platform strategy confirms public trading enums stringify to stable wire values |
| `t17_daily_bars_fill.py` | daily-bar orders fill at the same day's 16:00 ET close |
| `t18_reject_reason.py` | engine rejection text reaches the public order payload |
| `t19_order_gate.py` | malformed orders fail locally with stable rejection codes |
| `t20_cancel_modify_order.py` | targeted cancel/modify, open quantity, and invalid-ID safety |
| `t21_session_start_boundaries.py` | configured ET session start controls start/data timing |
| `t22_equity_holidays.py` | market holidays deliver neither bars nor strategy orders |
| `t23_global_dispatch.py` | conventional SDK strategy capture and callback dispatch |
| `t24_auction_orders.py` | MOO/MOC orders fill on official auction prints |
| `t25_global_portfolio.py` | per-strategy positions and account-wide aggregation |
| `t26_option_filters.py` | call/put accumulation, 0DTE, strike, and expiration filters |
| `t27_equity_futures_sessions.py` | same-day/overnight sessions, weekends, and DST |
| `t28_cash_equity_exposure.py` | cash/equity identities and two-symbol exposure arithmetic |
| `t29_long_short_transitions.py` | long, flat, and short position/helper transitions |
| `t30_ioc_gtc_limits.py` | IOC versus resting GTC limit-order behavior |
| `t31_instrument_metadata.py` | instrument multiplier/tick metadata and price helpers |
| `t32_callback_error_visibility.py` | callback exceptions remain tagged and traceable in logs |
| `t33_parameter_changes.py` | adaptive values persist as PARAM_CHANGE event logs |
| `t34_run_isolation.py` | sequential runs keep state and event logs isolated |
| `t35_calendar_day_start.py` | one start callback per calendar day, including closures |
| `t36_custom_csv_routing.py` | CSV custom-data payload and subscriber isolation |
| `t37_hosted_signals.py` | static and dynamic hosted-signal selectors and JSON payloads |
| `t38_multi_rollover_squareoff.py` | repeated rolls square old contracts without fractional fills |
| `t39_executor_lifecycle.py` | POV creation, query, in-place retarget, linked child orders, and stop |
| `t40_option_order_fill.py` | tick-valid 0DTE option limit entry/exit and completed trade evidence |
| `t41_tca_report.py` | automatic TCA summary plus per-fill and per-trade analysis |
| `t42_stream_data_api_parity.py` | exact equity/futures callback counts versus the Data API |
| `t43_equity_tbbo_trade_quote.py` | one TBBO source drives valid trade and quote payloads |
| `t44_cluster_analytics_data.py` | cluster-analytics custom rows or an explicit data-availability gap |
| `t45_multileg_options.py` | four-leg 0DTE long/short basket entry, exit, and flatness |
| `t46_stop_bracket_orders.py` | STOP/STOP_LIMIT fills and manual bracket sibling cancellation |
