# Agent guide — HiveQ Flow SDK

This repo is the **thin-client SDK** for authoring HiveQ Flow trading strategies and
deploying backtests to the HiveQ platform. If you are an AI / code-gen agent asked to
write a strategy or use this SDK, read these two, **in order**:

1. **The version-matched API spec** — `docs/<version>/api-reference.md`. The canonical
   API spec (every signature verified against source) is **versioned per SDK release**:
   each release has its own folder under `docs/` named for its version, e.g.
   [`docs/0.3.4/api-reference.md`](docs/0.3.4/api-reference.md). **Always read the folder
   matching the installed SDK version** — find it with
   `python -c "import hiveq.flow as hf; print(hf.__version__)"`, then open
   `docs/<that-version>/api-reference.md`. It is the **single source of truth**; start
   with its §0 "Hard rules / invariants".
2. **[`examples/`](examples/)** — complete, runnable strategies (see
   [`examples/README.md`](examples/README.md)). Mirror their structure.

## Hard rules (summary — full text in `docs/<version>/api-reference.md` §0)

- **R1** — a strategy is a Python class with per-event callbacks (`on_start`/`on_bar`/`on_order`/`on_trade`/…).
- **R2** — `StrategyConfig.type` is the strategy **class name as a string**; must match exactly.
- **R3** — call `ctx.subscribe_*()` inside `on_start`.
- **R4** — `event.data()` returns a **different payload type per `event.type`** (map in §7.0).
- **R5** — **time is EST/EDT.** Payload `.time` and `ctx.now()` are already the configured tz (ET). **Never convert to/from UTC** — `.time_utc`/`ctx.now_utc()` exist if you truly need UTC.
- **R6** — `session_start`/`session_end` are ET (`America/New_York`) `"HH:MM"` strings, always.
- **R7** — quantities are floats; `buy_order` / `sell_order` (exit long) / `short_order`.
- **R8** — a **HiveQ API key** is the only credential required; the first call with no key opens a browser sign-in (loopback redirect) that saves the key to `~/.hiveq/.env` automatically — no copy/paste. The sign-in host comes from `HIVEQ_AUTH_URL` (env / `~/.hiveq/.env`). Never hard-code it.
- **R9** — prefer `ctx.portfolio()` (strategy-scoped) for P&L/position queries; `ctx.global_portfolio()` aggregates account-wide.

## Key facts

- **Thin client.** It captures + deploys your strategy; the **platform** runs the engine and
  fetches data. You cannot pull market data locally.
- **The authoring surface is the `.pyi` stubs** (e.g. `src/hiveq/flow/oms/sigma/sigma_context.pyi`,
  `oms/sigma/types/*.pyi`) — that is the authoritative `ctx`/payload API.
- `run_backtest(...)` returns a **`Run`** handle; read results via
  `run.report()` / `run.orders()` / `run.trades()` / `run.tearsheet()` (PDF) / `run.event_logs()` / `run.logs()`.
- **Do not invent API.** If a method/field/enum isn't in the spec or the `.pyi` stubs, it
  does not exist — don't guess.
