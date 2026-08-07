# HiveQ Flow SDK — examples

Complete, runnable strategies that deploy to the HiveQ platform. Each file is
self-contained: a strategy class authored against the SDK type surface plus a
`run_backtest(...)` deploy block. Every `ctx` call is verified against the SDK
stubs; the canonical reference is the single-file spec at
[`docs/llms.txt`](../docs/llms.txt) (a copy ships in the wheel;
`hiveq docs` prints the installed path).

## Authoring basics

| File | Shape | Teaches |
|---|---|---|
| `deploy_buy_and_hold.py` | buy once, hold | the minimal deploy + results round-trip |
| `intraday_momentum_equity.py` | long-only SMA crossover | per-symbol state (`deque`), numpy indicator, **EST time window + EOD flat** (R5/R6), `close_position` |
| `global_dispatch.py` | buy-and-hold, single dispatch | the opt-in `on_hiveq_event(ctx, event)` contract (branch on `event.type`) vs per-event callbacks (§4) |
| `global_portfolio.py` | multi-strategy | `ctx.portfolio()` (strategy-scoped) vs `ctx.global_portfolio()` (account-wide) accessors (§8) |

## Orders & execution

| File | Shape | Teaches |
|---|---|---|
| `order_lifecycle.py` | working limit orders | market/limit orders, `modify_order`/`cancel_order`/`cancel_all_orders`, fills in `on_order` |
| `bracket_stop_take_profit.py` | bracketed entry | stop+target children on fill, **OCO emulation**, trailing stop via `modify_order` (no native bracket — §16.4) |
| `auction_moo_moc.py` | open/close auctions | MOO/MOC orders + venue cutoff timing (§5.2.1) |
| `executor_pov_sliced.py` | executor-driven entry | POV executor, one handle per target, `executor_state` checks, re-target in place (§5.10/§16.6) |

## Asset classes & data

| File | Shape | Teaches |
|---|---|---|
| `futures_continuous_rollover.py` | futures breakout | continuous-contract subscription, `enable_auto_rollover`, `on_rollover` (§7.12) |
| `futures_session.py` | futures session window | CME Globex session 18:00→17:00 ET via `BacktestConfig(session_start/session_end)` (R6) |
| `options_0dte_iron_condor.py` | 0DTE options | `subscribe_option_snaps`, `SigmaSnapData`, multi-leg all-or-nothing structure |
| `quant_signals.py` | signal-driven | `HIVEQ_QUANT_SIGNALS` source + `ctx.subscribe_data(data_id=...)` → `on_custom_data` |
| `custom_data.py` | bring-your-own CSV | a `type='csv'` custom feed (§9.2) → `on_custom_data` / `SigmaCustomData` |
| `imbalance_data.py` | early auction imbalance feed | `early_imbalance` → `on_imbalance` / `ImbalanceData` (§7.14) |
| `imbalance_arca.py` | NYSE Arca auction imbalance | metadata-routed `arca_imbalance` → `on_imbalance` |
| `imbalance_nasdaq.py` | Nasdaq auction imbalance | metadata-routed `nasd_imbalance` → `on_imbalance` |
| `imbalance_nyse.py` | NYSE auction imbalance | metadata-routed `nyse_imbalance` → `on_imbalance` |
| `pairs_stat_arb.py` | 2-symbol pairs | rolling z-score spread with per-symbol state, `short_order` |
| `timers_scheduling.py` | wall-clock scheduling | `ctx.set_timer`/`on_timer` + `ctx.now()` ET checks (§16.5) |

## Function registry & remote functions

| File | Shape | Teaches |
|---|---|---|
| `functions_push.py` | register a function | `hf.push_function(...)` → store a reusable function in your namespace (§2.2) |
| `functions_run.py` | use it on the platform | `hf.run_function(job)` runs a QUANT_SCRIPTS task that loads the registered function and applies it (§2.2). Run after `functions_push.py`. |

## Conventions these examples model (the easy-to-get-wrong parts)

- **Time is EST/EDT.** `ctx.now()` is already the configured-tz (ET) datetime, and
  all delivered timestamps are ET. Compare wall-clock directly — **never** convert
  to/from UTC (R5/R6). Use `.time_utc` / `ctx.now_utc()` only if you explicitly need UTC.
- **Fills arrive in `on_order`** (check `order.is_filled`), not a separate callback.
- **No engine history buffer, no TA library, no native brackets** — keep your own
  rolling window (`deque`), compute indicators with numpy/pandas, and build
  stop/target/OCO/trailing logic from explicit child orders (§16).
- **This is a thin client.** It captures and deploys your strategy; the platform
  runs the engine and fetches data. You can't pull data locally.

## Running

```bash
pip install hiveq-sdk
python examples/intraday_momentum_equity.py
```
