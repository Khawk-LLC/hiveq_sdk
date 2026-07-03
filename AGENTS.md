# Agent guide — HiveQ Flow SDK

This repo is the **thin-client SDK** for authoring HiveQ Flow trading strategies and
deploying backtests to the HiveQ platform. If you are an AI / code-gen agent asked to
write a strategy or use this SDK, read these two, **in order**:

1. **The version-matched API spec** — `docs/<version>/api-reference/`. The canonical
   API spec (every signature verified against source) is **versioned per SDK release**:
   each release has its own folder under `docs/` named for its version, e.g.
   [`docs/latest/api-reference/`](docs/latest/api-reference/). **Always read the `latest` folder**
   (symlink to current release), or a specific version folder if needed. It is split
   one file per topic — open `docs/<that-version>/api-reference/index.md` first, it
   lists every section with a one-line description and a link; read only the
   section(s) relevant to your task, not the whole tree. It is the **single source
   of truth**; read `00-hard-rules.md` (§0) at least once per session — every other
   section assumes it. Superseded versions live under `docs/_archive/` — don't read
   those unless you specifically need the diff against an old release.
2. **`docs/<version>/data-reference.md`** (same folder as the API spec) — the
   catalog of every `dataset`/`schema` code available for `data_configs`
   (§9 of the API spec links here). This is where the platform/data team's
   updates to available data land — always defer to it over your own
   assumptions about what data exists.
3. **[`examples/`](examples/)** — complete, runnable strategies (see
   [`examples/README.md`](examples/README.md)). Mirror their structure.
4. **The data driver reference** — if the task involves `hiveq.driver`/`dd.load`/`dd.save`
   (a separate tool from the API spec above), read
   [`docs/data_driver/latest/api_reference/index.md`](docs/data_driver/latest/api_reference/index.md)
   the same way: index first, then only the relevant section(s).

## Hard rules (summary — full text in `docs/<version>/api-reference/00-hard-rules.md`, §0)

- **R1** — a strategy is a Python class with per-event callbacks (`on_start`/`on_bar`/`on_order`/`on_trade`/…).
- **R2** — `StrategyConfig.type` is the strategy **class name as a string**; must match exactly.
- **R3** — call `ctx.subscribe_*()` inside `on_start`.
- **R4** — `event.data()` returns a **different payload type per `event.type`** (map in §7.0).
- **R5** — **time is EST/EDT.** Payload `.time` and `ctx.now()` are already the configured tz (ET). **Never convert to/from UTC** — `.time_utc`/`ctx.now_utc()` exist if you truly need UTC.
- **R6** — `session_start`/`session_end` are ET (`America/New_York`) `"HH:MM"` strings, always.
- **R7** — quantities are floats; `buy_order` / `sell_order` (exit long) / `short_order`.
- **R8** — a **HiveQ API key** is the only credential required; the first call with no key opens a browser sign-in (loopback redirect) that saves the key to `~/.hiveq/.env` automatically — no copy/paste. The sign-in host defaults to `https://staging.hiveq.ai`; override with `HIVEQ_AUTH_URL` in env / `~/.hiveq/.env` to point at a different platform.
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
