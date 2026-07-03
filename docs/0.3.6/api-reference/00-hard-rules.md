## 0. Hard rules / invariants (read first)

```
R1  A strategy is a Python class with PER-EVENT CALLBACK methods — on_start, on_bar, on_order,
    on_position, on_timer, ... (full list §4). This is the canonical/DEFAULT contract; prefer it.
    There is NO on_order_filled callback — fills arrive in on_order (see §4/§7.0).
    (A single global on_hiveq_event(self, ctx, event) dispatch is also supported but is NOT the
    default — use it only when you specifically want one method that branches on event.type. §4.)
R2  StrategyConfig.type is the STRATEGY CLASS NAME AS A STRING. It must exactly match the class name.
R3  ctx.subscribe_*() only RECORDS a subscription request; the engine applies it. Put subscription
    calls in the START handler so they are registered before data flows (and so day-by-day
    execution can discover your symbol universe). They are not bound to START by the
    engine, but START is the correct, supported place.  [Older docs over-stated this as "or no data" — that is wrong.]
R4  event.data() returns a DIFFERENT type per event.type. See the EventType→payload map in §7.0.
R5  Timestamps on payloads: ts_event / ts_init are int NANOSECONDS. Use .time (configured tz) or
    .time_utc (UTC) for datetime. ctx.now() is configured-tz datetime; ctx.now_utc() is UTC.
R6  session_start / session_end are ET (America/New_York) wall-clock "HH:MM" strings, always.
R7  Quantities are floats. Buy with buy_order, sell/exit-long with sell_order, open short with short_order.
R8  A HiveQ API key is the only credential required (§3); auth is fully automatic via browser sign-in. On the FIRST run with no key, sign-in opens a browser and BLOCKS ~5 min waiting for the user — this is expected, NOT a hang. The user's whole experience is "a browser opens, I sign in, done"; tell them only that, and (if the browser didn't open) the bare link. NEVER show the user internal commands (`hf.login()`, `hiveq-login`), env vars, or file paths; NEVER bisect, kill the process, or fall back to a manual/`export HIVEQ_API_KEY` key (§3.1). Trigger sign-in invisibly on the user's behalf; never put it in deliverable code.
R9  Prefer ctx.portfolio() (strategy-scoped) for P&L/position queries; ctx.global_portfolio() aggregates
    across all strategies. ctx also exposes shortcut aliases (ctx.net_position, ctx.is_flat, ...) — same data.
R10 Every strategy MUST include logging via the HiveQ logger throughout all callbacks and decision
    branches. This is MANDATORY — not optional. Use logger.debug(...) for per-bar state, condition
    checks, and intermediate values; logger.info(...) for milestone events (signal triggered, order
    placed, pattern detected). Because the default level is INFO, debug lines are silent in normal
    runs and add zero noise. When the strategy misbehaves, re-run with hiveq_log_level='DEBUG'
    (§2.1) and all context surfaces immediately — no code changes needed.

    Import and instantiate at module level (NOT inside the class):
        from hiveq.flow.logger import logger as _get_logger
        logger = _get_logger()

    DO NOT use logging.getLogger(__name__) or logging.basicConfig — those are silenced by the
    executor and do not respond to hiveq_log_level (§5.9.1).

    Minimum logging required in every strategy:
    - on_start:  log what was subscribed and any initial config values
    - on_bar:    log bar time, close, and key state variables at DEBUG level every bar
    - every signal/condition check: log the values being compared and the outcome at DEBUG
    - every order placement: log symbol, side, quantity, and the reason at INFO
    - on_order:  log the order status and fill price at INFO
```

---

