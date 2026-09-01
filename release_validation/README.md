# HiveQ SDK installed-wheel release validation

## Selecting a test environment

Release validators remain environment-agnostic. Use the repository-only
launcher to select `local`, `vm`, or `staging` without editing a validation
or sharing credentials between platforms:

```bash
python release_validation/hiveq_env.py check vm
python release_validation/hiveq_env.py run vm release_validation/long_running_t49_long_rollover_buy_hold.py
python release_validation/hiveq_env.py run local release_validation/run_all.py --suite baseline
```

The first use of a profile opens that platform's browser sign-in. Credentials
are stored separately in `~/.hiveq/profiles/<profile>.env` with mode `0600`.
The launcher clears inherited `HIVEQ_*` values before starting the validator,
performs health and authentication checks, and passes configuration only via
the child process environment. `login <profile>` forces credential renewal.

This launcher is QA infrastructure, not public SDK functionality. It is outside
`src/`, is not installed by setuptools, and must not be imported by examples,
validation strategies, or public package code.

### Running one validation from PyCharm

The repository includes three shared run configurations under `.run/`:

* `HiveQ Validation - Local`
* `HiveQ Validation - VM`
* `HiveQ Validation - Staging`

Open the validation `.py` file you want to execute so it is the active editor
file, select the desired configuration in PyCharm's run-configuration dropdown,
and choose **Run** or **Debug**. The configuration passes PyCharm's `$FilePath$`
macro to the same profile launcher used on the command line. It uses the
project’s configured Python interpreter and never adds environment logic to the
selected validation file.

When the selected file is `run_all.py`, the launcher automatically enables
remote-run semantics and disables engine-only order CSV capture. An explicit
`--local-runs` or `--remote-runs` argument overrides that default.

This suite deduplicates and translates the validation behaviors from the 165
Python files under `hiveq-flow/examples/bt`
to the thin SDK's real execution model. Strategies run remotely, persist their
callback state through structured event-log checkpoints, and are asserted only
through public `Run` APIs.

The runner invokes each file from the project root, matching ordinary
`python release_validation/baseline_tNN_....py` usage and its normal `.env`
resolution. Validation filenames are the suite manifest: use `baseline_` for
quick platform-health checks and `long_running_` for endurance cases. New
strategies need no runner edit when they follow one of those prefixes.
The convention/import audits verify the SDK strategy and package wiring.
Every run completed through the shared checkpoint helper automatically exports
its sanitized `orders.csv`, `trades.csv`, `positions.csv`, `event_logs.csv`, and
`validation.json`; long-run tests use the same exporter directly.

Run each test individually after installing the candidate wheel and signing in:

```bash
python release_validation/baseline_t01_bars_multi_interval.py
```

Run the quick platform-health gate on its own with:

```bash
python release_validation/run_all.py --suite baseline
```

Run `python release_validation/run_all.py --suite all` for final release
validation. It runs the baseline phase first and starts the long-running phase
only when the baseline has no `FAIL`, `ERROR`, or `BLOCKED` result (`GAP` remains
non-failing for optional data). Each process is isolated because a remote OMS
run is process-scoped.

Run the long-running phase directly, without collecting or gating on baseline
tests, with `--suite long-running`. `RELEASE_VALIDATION_START` still selects an
inclusive numeric resume point. For example, resume staging at t50 with:

```bash
RELEASE_VALIDATION_START=50 python release_validation/hiveq_env.py run staging release_validation/run_all.py --suite long-running
```
`baseline_t00_sdk_convention.py` is the first validation collected: a static audit (no
platform run) that enforces `SdkTxx` strategy naming, matching
`StrategyConfig.type`, platform submission via `run_backtest`, a public
result/log evidence reader, and one validation per number.
It runs exactly one validation process at a time, waits for its result, adds a
short inter-test cooldown, and retries the same test on a max-concurrent
response. Remote suites keep retrying until a platform slot becomes available;
local/in-process suites retain a bounded retry count. Exiting 0 without printing
a `RESULT:` line is recorded as `ERROR`,
not `PASS`: no RESULT line means `finish()` never ran and nothing was asserted.
With `LOCAL_RUNS = True` (in-process runs, which die with their process) a
timeout skips that validation and the suite continues, so the scorecard covers
every test rather than a prefix; set it to `False` when driving remote platform
submissions, where an abandoned run can still be active after the client gives
up.

Local order capture is a runner argument, not a hard-coded mode. `--export-orders`
(the default while `LOCAL_RUNS = True`) sets
`RELEASE_VALIDATION_EXPORT_ORDERS_CSV=1` for every validation process, so
`qa_common` defaults each `BacktestConfig` to `export_orders_csv=True` and the
in-memory result readers can read `run.orders()`/`run.fills()` back from the
streamed CSV. Pass `--no-export-orders` for remote platform submissions, which
publish orders from C++ and never write that file:

```bash
python release_validation/run_all.py --suite baseline --export-orders
```

Tests requiring datasets absent from QA report `GAP`; missing optional data is
not treated as an implementation regression. Core datasets remain hard gates.

## Reading a FAIL: defect in the validation, or defect in the product

A red row is only useful if it names the right defect, so every check has to
fail for one reason and say which surface produced it. The suite has repeatedly
scored its own mistakes as product regressions, and those are the ones to fix
first:

* **A configuration value recorded as a check.** `finish_validation` promotes
  every boolean in a validation mapping to a named check, so
  `"enable_auto_rollover": False` -- a record of what the run disabled -- became
  a permanently failing check (t48). Keep configuration in the mapping as a
  string.
* **An assertion on evidence a platform run cannot produce.**
  `qa_common.order_events` reads the streamed capture file, which only an
  in-process run writes. Assert on the checkpoint the strategy emitted and use
  the capture file as a second opinion (t61).
* **Comparing two surfaces on different terms.** The Data API resolves no
  continuous alias, delivers a whole window rather than a spliced front
  contract, treats its `end` as exclusive where the engine delivers a bar
  stamped at `session_end`, and still holds duplicate rows for some sessions.
  Parity has to be per `(symbol, session day)` on distinct timestamps (t42).
* **A cycle driven by whether an order object came back.** A rejected order
  still returns an order, so a flag-driven buy/sell alternation desynchronizes
  from the real position on the first reject and every later assertion inherits
  it. Drive the next leg from the position (t55, t56).
* **A floor no strategy in the case could reach.** `on_start` runs once per
  session, so a per-session tally must be snapshotted per session and the
  order/fill floors derived from the sessions actually run (t57).
* **A gate that hides the property under test.** Entering only when
  `instrument(continuous).current_contract` equals the delivered contract left
  thirty of thirty-seven symbols untraded and the rollover reconciliation
  running on seven (t52); the documented TWAP key params were omitted, so
  "TWAP was not created" said nothing about TWAP (t69).
* **One finding spread across several checks.** Split by surface instead: the
  analytics drawdown, the strategy's own recomputation and
  `portfolio.max_drawdown` are three claims, and only the third is wrong
  (t65). Where the client could be at fault, assert the submitted payload too
  (t64) so a red row is unambiguously server-side.

## Mandatory PASS evidence

Every release-validation case must exercise the complete public SDK path, not
only prove that a callback fired. A case may report `PASS` only when all of the
following are true:

1. **The strategy generates completed trades.** It must place orders that fill
   and produce persisted trade rows during the validation window. A run with no
   completed trade evidence is not a passing release validation, even when its
   callback/data assertions succeed. Choose a deterministic, liquid control
   instrument and entry/exit sequence that does not interfere with the contract
   under test.
2. **Orders are read through the public result API.** The test must call
   `run.orders()` after the run is final and assert the expected submitted,
   filled, canceled, or rejected lifecycle evidence relevant to the case. Use
   `qa_common.evidence_checks(run, orders=N, trades=M)` to fold that requirement
   into the case's own `checks` dict, so the RESULT line states it rather than a
   helper asserting it invisibly. An in-process (`is_local`) run only populates
   `run.orders()` when the backtest sets `BacktestConfig.export_orders_csv=True`;
   `qa_common.orders_frame(run)` falls back to the streamed capture file so an
   empty table cannot be mistaken for a run that did not trade, and
   `qa_common.order_events(run)` exposes the intermediate states -- partial
   fills, cancel-rejects -- that `run.orders()` collapses away.
3. **Event logs are read through the public result API.** Persist strategy
   observations with `ctx.add_event_log(...)`, retrieve them through
   `run.event_logs()`, and use those persisted rows as assertion evidence. Do
   not pass a test solely from process-local or in-memory strategy state.
4. **The review artifacts are exported.** The run's sanitized `orders.csv`,
   `trades.csv`, `positions.csv`, `event_logs.csv`, and `validation.json` must
   exist under `release_validation/run_artifacts/<run_id>/`. Empty orders,
   trades, or event logs fail the evidence requirement rather than counting as
   a successful export.

Checkpoint state is a compact index into the persisted evidence; it is not a
substitute for orders, trades, or event logs. Tests whose primary contract is a
rejection, cancellation, holiday, error, or data-gap behavior must still add an
independent control round trip on available data so the SDK execution and
result surfaces are validated end to end.

## Review order and coverage

| SDK test | Contract |
|---|---|
| `baseline_t01_bars_multi_interval.py` | successive 1m/1d subscriptions and bar payloads |
| `baseline_t02_successive_accumulate.py` | successive trade/index subscriptions accumulate |
| `baseline_t03_all_asset_types.py` | equity, futures, index, and option streams coexist |
| `baseline_t04_callbacks_contract.py` | callback order, event types, timestamps, and context |
| `baseline_t05_order_lifecycle.py` | market fill, resting limit, cancel, and flatten |
| `baseline_t06_position_portfolio.py` | position callbacks, exposures, equity, and realized PnL |
| `baseline_t07_timers.py` | timer cadence, identifiers, and cancellation |
| `baseline_t08_rollover.py` | futures contract rollover and position carry |
| `baseline_t09_event_logs_reports.py` | structured event logs and every public Run result surface |
| `baseline_t10_metrics_validation.py` | independent PnL, fee, return, and trade arithmetic |
| `baseline_t11_imbalance_flow.py` | public imbalance callback contract; no QA rows is `GAP` |
| `baseline_t12_quotes.py` | futures tick delivery (`fut_trades`) and non-crossed quotes |
| `baseline_t13_arca_imbalance.py` | NYSE Arca imbalance delivery and venue-specific fields |
| `baseline_t14_nasdaq_imbalance.py` | Nasdaq imbalance delivery, indicative prices, and cross type |
| `baseline_t15_nyse_imbalance.py` | NYSE imbalance delivery and clearing-price fields |
| `baseline_t16_enum_str_stability.py` | platform strategy confirms public trading enums stringify to stable wire values |
| `baseline_t17_daily_bars_fill.py` | daily-bar orders fill at the same day's 16:00 ET close |
| `baseline_t18_reject_reason.py` | engine rejection text reaches the public order payload |
| `baseline_t19_order_gate.py` | malformed orders fail locally with stable rejection codes |
| `baseline_t20_cancel_modify_order.py` | targeted cancel/modify, open quantity, and invalid-ID safety |
| `baseline_t21_session_start_boundaries.py` | configured ET session start controls start/data timing |
| `baseline_t22_equity_holidays.py` | market holidays deliver neither bars nor strategy orders |
| `baseline_t23_global_dispatch.py` | conventional SDK strategy capture and callback dispatch |
| `baseline_t24_auction_orders.py` | ten-symbol one-day MOO/MOC round trips fill on official auction prints |
| `baseline_t25_global_portfolio.py` | per-strategy positions and account-wide aggregation |
| `baseline_t26_option_filters.py` | call/put accumulation, 0DTE, strike, and expiration filters |
| `baseline_t27_equity_futures_sessions.py` | same-day/overnight sessions, weekends, and DST |
| `baseline_t28_cash_equity_exposure.py` | cash/equity identities and two-symbol exposure arithmetic |
| `baseline_t29_long_short_transitions.py` | long, flat, and short position/helper transitions |
| `baseline_t30_ioc_gtc_limits.py` | IOC versus resting GTC limit-order behavior |
| `baseline_t31_instrument_metadata.py` | instrument multiplier/tick metadata and price helpers |
| `baseline_t32_callback_error_visibility.py` | callback exceptions remain tagged and traceable in logs |
| `baseline_t33_parameter_changes.py` | adaptive values persist as PARAM_CHANGE event logs |
| `baseline_t34_run_isolation.py` | sequential runs keep state and event logs isolated |
| `baseline_t35_calendar_day_start.py` | one start callback per calendar day, including closures |
| `baseline_t36_custom_csv_routing.py` | CSV custom-data payload and subscriber isolation |
| `baseline_t37_hosted_signals.py` | static and dynamic hosted-signal selectors and JSON payloads |
| `baseline_t38_multi_rollover_squareoff.py` | repeated rolls square old contracts without fractional fills |
| `baseline_t39_executor_lifecycle.py` | POV creation, query, in-place retarget, linked child orders, and stop |
| `baseline_t40_option_order_fill.py` | tick-valid 0DTE option limit entry/exit and completed trade evidence |
| `baseline_t41_stop_limit_latch.py` | STOP_LIMIT remains triggered after a gap beyond its limit and fills on the return |
| `baseline_t42_stream_data_api_parity.py` | exact equity/futures callback counts versus the Data API |
| `baseline_t43_equity_trade_quote.py` | one `eq_trades` source drives valid trade and quote payloads |
| `baseline_t44_cluster_analytics_data.py` | staging cluster-analytics rows delivered through the custom-data callback |
| `long_running_t45_multileg_options.py` | one week of daily four-leg 0DTE long/short basket entries, exits, flatness, and persisted trade evidence |
| `baseline_t46_stop_bracket_orders.py` | STOP/STOP_LIMIT fills and manual bracket sibling cancellation |
| `baseline_t47_trade_tick_fields.py` | public futures/equity trade-tick payload fields |
| `long_running_t48_replay_set_a_cap_slice_v3.py` | seven-year CSV-driven ES.v.0 replay with complete run-artifact export |
| `long_running_t49_long_rollover_buy_hold.py` | ten-year ES.v.0 buy-and-hold rollover and stale-position reconciliation |
| `long_running_t50_rollover_lifecycle.py` | multi-year `on_rollover` payload, roll count/spacing, and contract-chain continuity (`on_security_event` carries no rollover phases) |
| `long_running_t51_memory_session_comparison.py` | sequential 100-symbol full-session versus 14:00–16:30 memory probes |
| `long_running_t52_multi_symbol_long_rollover.py` | ten-year buy-and-hold rollover reconciliation across 37 designated `.v.0` futures |
| `long_running_t53_overnight_futures_strategy.py` | five-contract ES.v.0 position survives boundary/midnight, then exits next morning |
| `long_running_t54_futures_trade_scalp.py` | converted .164 ES.v.0 trade/quote scalp with brackets, cleanup, and rollover evidence, over a window spanning the September roll |
| `long_running_t55_futures_session_daily_bars.py` | ten-year ES.v.0 daily OHLCV aggregated by the 18:00-17:00 ET futures session, with duplicate checks |
| `long_running_t56_equity_calendar_daily_bars.py` | ten-year native AAPL daily OHLCV keyed by ET calendar/trading date, with duplicate checks |
| `long_running_t57_auction_orders_endurance.py` | ten-symbol MOO entries and MOC exits across a holiday-shortened week, covering a full closure and a 13:00 early close |
| `long_running_t58_100_symbol_monthly_short_session.py` | 100-symbol month-long memory and delivery endurance in a shortened ET session |
| `baseline_t59_determinism_replay.py` | one fixed configuration replayed twice agrees on every order, fill, trade and closing portfolio |
| `baseline_t60_causality_lookahead.py` | time never regresses, no future bar is visible, no fill precedes its order or leaves the printed range |
| `baseline_t61_partial_fills.py` | an oversized limit fills in pieces with monotonic `leaves_qty`, weighted `avg_px`, and a cancellable remainder |
| `baseline_t62_capital_and_risk_posture.py` | an unaffordable order's posture is unambiguous and cash, equity and position still reconcile |
| `baseline_t63_cancel_modify_rejects.py` | cancel/modify against filled, unknown, and already-cancelled orders refuse without mutating state |
| `baseline_t64_cost_model_sensitivity.py` | fee and slippage configuration reaches the fill and fee model and moves net PnL |
| `baseline_t65_analytics_reconciliation.py` | Sharpe, drawdown and trade PnL recomputed from the persisted equity curve; `overview`/`tearsheet` render |
| `baseline_t66_order_type_tif_matrix.py` | every public order type and time-in-force submitted; nothing accepted is silently dropped |
| `baseline_t67_portfolio_position_surface.py` | every public position and portfolio accessor reconciled against fills, long and short |
| `baseline_t68_order_helper_surface.py` | `place_order`, `order_to_target`, `flatten_all`, `get_order_state`, `clear_pending_order` and the order field surface |
| `baseline_t69_executor_algo_matrix.py` | POV and TWAP creation, `executor_state`, both stop paths, and the `on_executor` event stream |
| `baseline_t70_option_expiry_lifecycle.py` | long and short 0DTE options carried into expiry resolve and settle into cash |
| `baseline_t71_resilience_and_isolation.py` | a missing symbol and a throwing callback do not stop dispatch, trading, or readable results |

Files prefixed `long_running_` receive a four-hour per-test timeout; files
prefixed `baseline_` use the normal timeout. Every run writes its review files beneath
`release_validation/run_artifacts/<run_id>/`.

`run_all.py` also writes an incremental HTML scorecard beneath
`release_validation/reports/`; `reports/latest.html` always contains the newest
suite. The report groups rows by phase and shows whether the baseline gate is
open or closed. Each test row shows status and duration, captured runner output, and
direct links to every associated run's `validation.json`, `orders.csv`,
`trades.csv`, `positions.csv`, and `event_logs.csv`. Missing exports are shown
explicitly, and the report is updated after every test so a partial suite is
still independently reviewable.
