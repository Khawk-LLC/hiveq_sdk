<!--
CANONICAL MACHINE-READABLE API SPEC FOR HIVEQ FLOW.
Audience: HiveQ SDK users.
Every signature, type, enum value, and dict key in this tree is verified against source.
If you are unsure of a value, use the documented dataset/schema codes in §9 / data-reference.md rather than guessing — the platform fetches the data at run time (this thin client has no local data access).
-->

# HiveQ Flow — Canonical API Specification (index)

- **package**: `hiveq-flow` · **import root**: `hiveq.flow` · **version**: `0.3.6`
- **python**: `>=3.11`
- **scope of this doc**: backtest authoring · reading results · remote deploy + observability. (Live trading is out of scope here.)
- **how to read this**: this is a reference spec, not a tutorial, split into one file per section below so you only load what the current question needs. Signatures are exact. Defaults are exact. Enum `.value` strings are exact. Use the field tables in §7 to know what `event.data()` returns — it is otherwise untyped. Read §0 first, always — it's short and every other section assumes it.

Prose elsewhere in this doc set (and in code comments) refers to sections as `§N` — use the number column below to resolve those to a file.

| § | File | What's in it |
|---|---|---|
| 0 | [00-hard-rules.md](00-hard-rules.md) | Hard rules / invariants — read first, always. The non-negotiables (strategy shape, `event.data()` typing, subscription timing, timezone, logging level, run-output discipline, must-produce-trades). |
| 1 | [01-minimal-example.md](01-minimal-example.md) | Minimal working example (canonical) — a complete runnable strategy + `run_backtest` call. |
| 2 | [02-entry-points.md](02-entry-points.md) | Entry points (module `hiveq.flow`) — `run_backtest`, engine config via `**kwargs`, the function registry / remote functions. |
| 3 | [03-credentials.md](03-credentials.md) | Credentials — first-run sign-in flow, what NOT to do when generating deliverable code. |
| 4 | [04-strategy-contract.md](04-strategy-contract.md) | Strategy contract — the class shape (`__init__`/`on_start`/`on_bar`/`on_order`/...), lifecycle facts (init-once, on_start-per-calendar-day). |
| 5 | [05-context-api.md](05-context-api.md) | Context API — `ctx` (`SigmaContext`): subscriptions, order placement + sizing helpers, order management, position/order queries, portfolio accessors, instrument, time, timers, event logging, executors (POV/TWAP/VWAP/AUCTION). Largest section — by far the most common single lookup. |
| 6 | [06-event-object.md](06-event-object.md) | The `Event` object — the small wrapper every callback receives. |
| 7 | [07-event-payloads.md](07-event-payloads.md) | Event payloads — what `event.data()` returns, per `event.type`. The field tables (SigmaBar, SigmaPosition, SigmaOrder, SigmaFill, SigmaTradeTick, SigmaQuoteTick, SigmaSnapData, TimerEventData, SigmaCustomData, SigmaInstrument, IndexPrice, Rollover, EXECUTOR_EVENT/SECURITY_EVENT). |
| 8 | [08-portfolio-api.md](08-portfolio-api.md) | Portfolio API — `SigmaPortfolio` / `SigmaGlobalPortfolio` shared surface. |
| 9 | [09-data-configs.md](09-data-configs.md) | `data_configs` schema — `type='hiveq_historical'` and `type='csv'`, dataset/schema catalog pointers. |
| 10 | [10-results.md](10-results.md) | Results — `Run` handle, `report()`, `positions()`/`trades()`/`tearsheet()`/`event_logs()`/`logs()`, quantstats, phantom-PnL canary checks. |
| 11 | [11-remote-deploy.md](11-remote-deploy.md) | Remote deploy + observability (`hiveq.flow.jobs`) — generic `submit`/observe, plus `deploy_job` (§11.6) for deploying an arbitrary fetch/compute/publish script with `requirements` and a recurring `Schedule`. |
| 12 | [12-enums.md](12-enums.md) | Enums — exact members and `.value` strings. |
| 13 | [13-config-dataclasses.md](13-config-dataclasses.md) | Config dataclasses (`BacktestConfig`, `StrategyConfig`, `EngineConfig`, ...). |
| 14 | [14-imports-cheatsheet.md](14-imports-cheatsheet.md) | Imports cheat-sheet, incl. §14.1 ancillary data packages shipped as stubs with the SDK. |
| 15 | [15-common-pitfalls.md](15-common-pitfalls.md) | Common pitfalls (accurate) — short, worth reading alongside §0. |
| 16 | [16-authoring-patterns.md](16-authoring-patterns.md) | Authoring patterns — idioms for capabilities without a built-in helper (e.g. multi-bar executor state machines). |

Related, outside this tree:
- [`../data-reference.md`](../data-reference.md) — the dataset/schema catalog referenced by §9.
- [`../../data_driver/latest/api_reference/index.md`](../../data_driver/latest/api_reference/index.md) — the data driver config DSL (separate tool, not part of this spec).
- [`../../data_driver/latest/hiveq_data_api_reference.md`](../../data_driver/latest/hiveq_data_api_reference.md) — the lower-level `hiveq_data` SDK client the driver's transport calls internally.
