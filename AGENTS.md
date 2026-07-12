# Agent guide — HiveQ Flow SDK

This repo is the **thin-client SDK** for authoring HiveQ Flow trading strategies and
deploying backtests to the HiveQ platform. If you are an AI / code-gen agent asked to
write a strategy or use this SDK, read these two, **in order**:

1. **The API spec** — [`docs/llms.txt`](docs/llms.txt). The canonical
   API spec (every signature verified against source), one plain-markdown file designed
   to be loaded in a **single read** (~31k tokens) — do not read it piecemeal across many
   small reads. It always matches the SDK release (the version is stated in its header), and a
   copy ships inside the wheel — `hiveq docs` prints the installed path. For a targeted question,
   jump to a section by searching for a line starting `## N.` — prose refers to sections
   as `§N`. It is the **single source of truth**; read §0 (hard rules) at least once per
   session — every other section assumes it.
2. **The Data Reference appendix** — at the end of the same file (§A.1–§A.4): the
   catalog of every `dataset`/`schema` code available for `data_configs` (§9 links
   there). This is where the platform/data team's updates to available data land —
   always defer to it over your own assumptions about what data exists.
3. **[`examples/`](examples/)** — complete, runnable strategies (see
   [`examples/README.md`](examples/README.md)). Mirror their structure.
4. **The data driver reference** — if the task involves `hiveq.driver`/`dd.load`/`dd.save`
   (a separate tool from the API spec above), read
   [`docs/data_driver/llms.txt`](docs/data_driver/llms.txt) the same
   way: one file, one read; Part II of it covers the underlying `hiveq_data` SDK client.

## Hard rules (summary — full text in §0 of [`docs/llms.txt`](docs/llms.txt))

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
