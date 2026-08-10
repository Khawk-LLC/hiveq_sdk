# In-memory release validation results — 2026-08-08

Run: `release_validation/t*.py`, each in its own process, sequentially, no timeout.
Engine: installed `hiveq` package is the **full hiveq-flow v0.3.10 engine**, so
`run_backtest` executes the engine **in-process** and never deploys. `Run.is_local`
is true for every run below.

`t41_tca_report.py` is deleted in the working tree, so 45 of 46 tests ran.

**SUMMARY: 29 PASS · 11 FAIL · 4 ERROR · 1 GAP**

## FAIL (11)

| Test | Failed checks | Evidence |
|---|---|---|
| `t02_successive_accumulate` | all 4 | `{'MSFT': 0, 'AAPL': 0, 'VIX': 0, 'SPX': 0}` — no trade or index callbacks at all under `session_start=09:30, session_end=10:30` |
| `t03_all_asset_types` | `futures` | eq1m=1034, eq1d=2, trades=838559, index=3502, snaps=100107, **futures=0** |
| `t08_rollover` | `expected_december_roll` | callback fired, but `prev_contract='ES.v.0'` (the continuous alias) instead of `ESZ5`; current=`ESH6`; bars=0, contracts=[] |
| `t09_event_logs_reports` | `structured_event_logs` | every report surface ok (orders=2, trades=1) but `run.event_logs()` returned **0 rows** |
| `t12_quotes_tbbo` | `quotes_delivered` | `{'quotes': 0, 'crossed': 0, 'symbols': []}` — no futures TBBO |
| `t27_equity_futures_sessions` | `futures_ticks_delivered`, `futures_sessions_start_1800_ET`, `futures_spans_EST_and_EDT` | equity side fully ok (1170 bars, 09:30 ET starts, no weekend bars); futures ticks=0 across all 7 sessions (2026-03-04 … 03-10) |
| `t31_instrument_metadata` | `public_tick_helper_futures` | `ES.c.0` reported `min_tick=0.01` / `multiplier=1.0` (expected 0.25 / 50); `current_contract='ES.c.0'` unresolved; no ES trades delivered (`streams={'AAPL': 1}`) |
| `t33_parameter_changes` | `param_change_rows_persisted`, `parameter_name_visible` | 3 changes emitted and final value updated in-strategy, but **0 PARAM_CHANGE rows** readable |
| `t34_run_isolation` | `distinct_run_ids`, `first_log_in_first_run`, `second_log_in_second_run` | both runs report `run_id='local'`; both received data (61 bars each); no cross-leakage |
| `t38_multi_rollover_squareoff` | `old_contracts_squared`, `final_contracts_long_one` | 6 rollovers, 4 contracts/root as expected, but **ESH5 and NQH5 left long 1**; only the first carry filled |
| `t44_cluster_analytics_data` | `timeline_data_present` | bars=0, rows=0 |

## ERROR (4) — crashed before producing a RESULT

| Test | Cause |
|---|---|
| `t24_auction_orders` | `AttributeError: 'Run' object has no attribute 'fills'` — the strategy itself succeeded (2 fills, 1 trade, net PnL 427.78); the assertion line crashed |
| `t32_callback_error_visibility` | `AttributeError: 'Run' object has no attribute 'logs'` — the intentional error was raised and logged as designed |
| `t36_custom_csv_routing` | fixture upload shells out to the `hiveq` CLI, which offers only `login`/`docs`/`datasets` — no upload subcommand. Failed at 0s, before any run |
| `t42_stream_data_api_parity` | `404` on `POST http://localhost:5010/api/read/v0/data` — the platform Data API is not running locally |

## GAP (1)

`t37_hosted_signals` — control bars delivered (433 static / 433 dynamic), 0 signals, 0 parse errors. Reported as GAP by design.

## Root-cause grouping

1. **SDK/engine `Run` API parity** — `t24`, `t32`.
   `Run.fills()` (`src/hiveq/flow/runs.py:460`) and `Run.logs()` exist in the SDK but
   not on the engine's `Run`. The engine `Run` exposes only: `status`, `overview`,
   `summary`, `report`, `metrics`, `daily_returns`, `equity_curve`, `positions`,
   `orders`, `trades`, `event_logs`, `wait`, `check_credentials`.

2. **Event logs are unreadable via `Run` for in-process runs** — `t09`, `t33`, `t34`.
   `run.event_logs()` returns an empty frame when `is_local`; the logs live in the app
   singleton and are only reachable via `hf.event_logs()`. `qa_common._event_logs`
   already has this fallback (which is why every checkpoint resolved), but these three
   tests call `run.event_logs()` directly. `t34` additionally needs distinct run IDs,
   and every in-process run is assigned the literal id `'local'`.

3. **Futures streams deliver nothing in the requested windows** — `t03`, `t08`, `t12`,
   `t27`, `t31`, and likely `t44`. Not a blanket futures outage: `t39` pulled 371,472
   futures trade ticks and `t40`/`t45` pulled ~278k–380k option snapshots successfully.
   So this is window/dataset specific, not "no futures data".

4. **Continuous-contract alias not resolved** — `t08`, `t31`, and the first roll in
   `t38`. `ES.v.0` / `ES.c.0` is returned where a concrete contract (`ESZ5`, `ESH5`) is
   expected, and `ES.c.0` carries default metadata (mult 1.0, tick 0.01) rather than
   real ES metadata. In `t38` the *later* rolls do report concrete contracts
   (`ESH5→ESM5`, `ESM5→ESU5`) — only the first roll shows the alias.

5. **Local infrastructure absent** — `t36` (no CLI upload path), `t42` (no Data API on
   localhost:5010). Both are environment gaps for in-memory runs, not SDK defects.

6. **Genuine behaviour defects, environment-independent** — `t38` rollover square-off
   (old legs stranded long 1 after the first carry; matches the known
   `rollover-squareoff-position-lookup` defect) and `t02` (zero callbacks under a
   narrow session window while the same subscriptions deliver in `t03`).

Full per-test stdout/stderr: `/tmp/claude-1001/-home-rmadanagopal-PycharmProjects-hiveq-sdk/4a70a48f-0018-4658-953d-3120af197e55/scratchpad/logs/`
