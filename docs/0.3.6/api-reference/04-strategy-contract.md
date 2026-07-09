## 4. Strategy contract

**Canonical / default: per-event callback methods.** Define only the handlers you need; each has the signature `(self, ctx, event)`. Branch-free, one focused method per event type.

```python
class MyStrategy:
    def __init__(self): ...                       # per-strategy state lives here
    def on_start(self, ctx, event): ...           # subscribe here (R3)
    def on_bar(self, ctx, event): ...             # event.data() -> SigmaBar (§7.1)
    def on_order(self, ctx, event): ...           # fills/rejects/cancels -> SigmaOrder (§7.3)
    def on_position(self, ctx, event): ...        # event.data() -> SigmaPosition (§7.2)
    def on_stop(self, ctx, event): ...            # see note — NO orders here
```

- Register with `StrategyConfig(name='X', type='MyStrategy')` (R2).
- **Full set of recognized callbacks** (define any subset; unknown names are ignored):

| callback | fires on EventType(s) | event.data() |
|---|---|---|
| `on_start` | `START` | — |
| `on_stop` | `STOP` | — |
| `on_bar` | `BAR` | `SigmaBar` (§7.1) |
| `on_trade` | `TRADE` | `SigmaTradeTick` (§7.5) |
| `on_quote` | `QUOTE` | `SigmaQuoteTick` (§7.6) |
| `on_snap` | `SNAP` | `SigmaSnapData` (§7.7) |
| `on_order` | `ORDER`, `ORDER_SUBMITTED/ACCEPTED/REJECTED/FILLED/CANCELED/CANCEL_REJECTED/MODIFY_REJECTED` | `SigmaOrder` (§7.3) |
| `on_position` | `POSITION`, `POSITION_OPENED/CHANGED/CLOSED` | `SigmaPosition` (§7.2) |
| `on_timer` | `TIMER` | `TimerEventData` (§7.8) |
| `on_custom_data` | `CUSTOM_DATA` | `SigmaCustomData` (§7.9) |
| `on_index_price` (alias `on_index`) | `INDEX_PRICE` | `IndexPrice` (§7.11) |
| `on_rollover` | `ROLLOVER` | `Rollover` (§7.12) |
| `on_executor` | `EXECUTOR_EVENT` | executor payload (opaque; §7.13) |
| `on_security_event` | `SECURITY_EVENT` | security payload (opaque; §7.13) |

- **There is NO `on_order_filled`.** Fills are delivered to **`on_order`**; check `order.is_filled` / `order.status` / `order.last_fill`.
- **Order lifecycle contract (FIX-style: status ≠ events).** `order.status` is the order's state; events are the history. Terminal statuses (`FILLED`/`CANCELED`/`REJECTED`) are **sticky** — later request-level events never change them. The canonical race: you cancel a resting order, but a fill lands while the cancel is in flight. You then receive **`ORDER_FILLED` first (with the fill — never lost), followed by `ORDER_CANCEL_REJECTED`** ("too late to cancel"). The order's `status` reads `FILLED` on both events. Handle it as:
  - Act on fills from the `ORDER_FILLED` event, using the cumulative `filled_qty`/`leaves_qty` (idempotent).
  - Treat `ORDER_CANCEL_REJECTED` / `ORDER_MODIFY_REJECTED` as informational no-ops when `order.is_filled` — the position was already handled by the fill; do **not** count them toward reject/error limits.
  - `ORDER_REJECTED` is reserved for the order itself being rejected (entry rejects); it never fires for cancel/replace request rejections.
  - An order canceled after a **partial** fill ends `CANCELED` with `filled_qty > 0` — account for the partial from its fill events.
  - Never infer an order's disposition from its *last event*; use `order.status` / `filled_qty`.
- **`on_stop` / `EventType.STOP`**: fires after the engine has STOPPED. Do **not** place orders in STOP — they are rejected.
- **`__init__` fires exactly ONCE per backtest run**, and the same instance receives every subsequent callback. There is no per-session re-instantiation for the strategies covered here. Use `self.*` state normally in `__init__`; no module-level containers are required.
- **`on_start` fires ONCE per CALENDAR day**, on the same instance, **including Saturdays, Sundays, and market holidays.** Empirically verified over a 90-calendar-day daily-bar backtest: `on_start` was called exactly 90 times. Do not put one-time-only setup in `on_start` unguarded — it re-runs every calendar day. Guard with a `self._started` boolean if you need "only the very first call." Repeated identical `ctx.subscribe_bars(...)` calls are safe — the engine dedupes them internally.
- **Global single-dispatch (opt-in, NOT default):** if you specifically want one method, branch on `event.type` (EventType → payload map in §7.0) in a single `on_hiveq_event`. Two equivalent forms: (a) a **class** with `on_hiveq_event(self, ctx, event)` deployed via `StrategyConfig(name=..., type='YourClass')`; or (b) a **module-level** `def on_hiveq_event(ctx, event):` deployed with `run_backtest(strategy_configs=[], ...)` — the engine auto-discovers the captured function. (Empty `strategy_configs` is accepted *only* for this global form; otherwise it errors.) Use this only on explicit request — per-event callbacks are the default.

---

