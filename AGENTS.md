# Agent guide — HiveQ Flow SDK

This repo is the **thin-client SDK** for authoring HiveQ Flow trading strategies and
deploying backtests to the HiveQ platform. If you are an AI / code-gen agent asked to
write a strategy or use this SDK, read these two, **in order**:

1. **[`docs/HIVEQ_FLOW_API.md`](docs/HIVEQ_FLOW_API.md)** — the canonical, versioned API
   spec (every signature verified against source). **Single source of truth.** Start with
   its §0 "Hard rules / invariants".
2. **[`examples/`](examples/)** — complete, runnable strategies (see
   [`examples/README.md`](examples/README.md)). Mirror their structure.

## Hard rules (summary — full text in docs/HIVEQ_FLOW_API.md §0)

- **R1** — a strategy is a Python class with per-event callbacks (`on_start`/`on_bar`/`on_order`/`on_trade`/…).
- **R2** — `StrategyConfig.type` is the strategy **class name as a string**; must match exactly.
- **R3** — call `ctx.subscribe_*()` inside `on_start`.
- **R4** — `event.data()` returns a **different payload type per `event.type`** (map in §7.0).
- **R5** — **time is EST/EDT.** Payload `.time` and `ctx.now()` are already the configured tz (ET). **Never convert to/from UTC** — `.time_utc`/`ctx.now_utc()` exist if you truly need UTC.
- **R6** — `session_start`/`session_end` are ET (`America/New_York`) `"HH:MM"` strings, always.
- **R7** — quantities are floats; `buy_order` / `sell_order` (exit long) / `short_order`.
- **R8** — credentials come from env; **`HIVEQ_API_KEY` is the only required one** (user/org auto-resolve). Never hard-code.
- **R9** — prefer `ctx.portfolio()` (strategy-scoped) for P&L/position queries; `ctx.global_portfolio()` aggregates account-wide.

## Key facts

- **Thin client.** It captures + deploys your strategy; the **platform** runs the engine and
  fetches data. You cannot pull market data locally.
- **The authoring surface is the `.pyi` stubs** (e.g. `src/hiveq/flow/oms/sigma/sigma_context.pyi`,
  `oms/sigma/types/*.pyi`) — that is the authoritative `ctx`/payload API.
- `run_backtest(...)` returns a **`Run`** handle; read results via
  `run.report()` / `run.orders()` / `run.trades()` / `run.event_logs()` / `run.logs()`.
- **Do not invent API.** If a method/field/enum isn't in the spec or the `.pyi` stubs, it
  does not exist — don't guess.
