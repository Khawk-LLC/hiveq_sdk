## 5. Context API — `ctx` (runtime type: `SigmaContext`)

`hf.Context` is a public type-alias for hints; the engine passes a `SigmaContext`.

### 5.1 Subscriptions  (call in START; return `None`)
```python
ctx.subscribe_bars(symbols: List[str], asset_type: AssetType = None, interval: str = "1m")
ctx.subscribe_quotes(symbols: Optional[List[str]], asset_type: AssetType = AssetType.EQUITY)
ctx.subscribe_trades(symbols: List[str], asset_type: AssetType = AssetType.EQUITY)  # trades; with the `tbbo` schema, quotes arrive too (on_quote)
ctx.subscribe_data(data_id: str, signals: List[str] = None)               # custom / signal data → on_custom_data
#   data_id must match the 'id' field in data_configs (CSV or HIVEQ_QUANT_SIGNALS).
#   signals: optional list of signal names to filter; None subscribes to all.
ctx.subscribe_index(symbols: List[str])                                   # spot index value
ctx.subscribe_index_bars(symbols: List[str], interval: str = '1d')        # index OHLCV (daily only)
ctx.subscribe_option_snaps(symbol: str, option_type: Optional[str] = None,  # 'C'|'P'|'CALL'|'PUT'
                           strike: Optional[float] = None,
                           expiration_type: Optional[Union[str, datetime]] = None,  # '0dte' | 'YYYY-MM-DD' | datetime
                           underlying: Optional[str] = None, interval: str = "1s")
# Futures: subscribe by SYMBOL STRING in `symbols=` (the clear, single way):
ctx.subscribe_futures_bars(symbols: Optional[List[str]] = None, root: Optional[str] = None,
                           contract: Optional[str] = None, continuous: Optional[str] = None,
                           interval: str = "1m")
ctx.subscribe_futures_trades(symbols: Optional[List[str]] = None, root: Optional[str] = None,
                             contract: Optional[str] = None, continuous: Optional[str] = None)
#   NOTE: there is NO subscribe_futures_quotes. Bars and trades have dedicated
#   futures convenience methods; quotes do not. For futures NBBO/tbbo quotes,
#   use subscribe_quotes with the futures symbol string and asset_type=FUTURES:
#     ctx.subscribe_quotes(['ES.c.0'], asset_type=AssetType.FUTURES)
#   The futures symbol string encodes the contract:
#     "ES.c.0" — continuous, calendar/front-month roll, front (rank 0)
#     "ES.v.0" — continuous, volume roll, front
#     "ES.H25" — a specific dated contract (root + month code)
#   For continuous-contract ROLLOVER, set BacktestConfig(enable_auto_rollover=True)
#   and handle on_rollover (§7.12) — nothing extra in data_configs.
```

### 5.2 Order placement  (return `Optional[SigmaOrder]`; §7.3)
```python
ctx.buy_order(symbol: str, quantity: float, order_type: OrderType = None,
              limit_price: float = None, stop_price: float = None, time_in_force: str = None,
              market_center: str = None)
ctx.sell_order(symbol, quantity, order_type=None, limit_price=None, stop_price=None,
               time_in_force=None, market_center=None)
ctx.short_order(symbol, quantity, order_type=None, limit_price=None, stop_price=None,
                time_in_force=None, market_center=None)
ctx.place_order(symbol: str, side: OrderSide, quantity: float, order_type: OrderType,
                limit_price: Optional[float] = None, stop_price: Optional[float] = None,
                market_center: str = None)

# Convenience helpers (size/flatten off the current net position):
ctx.close_position(symbol: str, order_type: OrderType = None) -> Optional[SigmaOrder]
ctx.order_to_target(symbol: str, target_quantity: float,
                    order_type: OrderType = None, limit_price: float = None) -> Optional[SigmaOrder]
ctx.flatten_all(order_type: OrderType = None) -> List[SigmaOrder]
```
- `order_type` default = `OrderType.MARKET`.
- **Auto time-in-force** when `time_in_force=None`: `MOO/LOO → "OPG"`; `MOC/LOC → "DAY"`; `MARKET → "DAY"`; else (LIMIT/STOP) → `"GTC"`.
- **`LOO` and `LOC` require `limit_price`** (the engine rejects them otherwise). `MOO`/`MOC` ignore price. `STOP` needs `stop_price`; `STOP_LIMIT` needs both.
- **`market_center`** (opt): venue routing — `"NYSE"`/`"NASDAQ"`/`"ARCA"`/… or a MIC alias (`"XNYS"`/`"XNAS"`/…). Mainly for auction orders (§5.2.1); auction orders default to `"NASDAQ"` when omitted.
- **`close_position`** flattens the net position in one offsetting order (no-op if flat); **`order_to_target`** trades the delta to reach a signed target; **`flatten_all`** closes every open position in the strategy. These wrap `net_position` + `buy_order`/`sell_order` — see §16.1.
- **Transactional / idempotent:** all three **skip (return `None`) if a working order already exists for the symbol** (`has_open_order`). `net_position` reflects only *filled* quantity, so re-issuing while an order is in flight would double-trade — the guard prevents that. Safe to call every bar: they converge one fill at a time. To replace a resting order, `cancel_all_orders(symbol)` first.
- **The base `buy_order` / `sell_order` / `short_order` do NOT have the `has_open_order` skip guard** — only the three sizing helpers above (`close_position`, `order_to_target`, `flatten_all`) skip. If you need to force a trade even while another order is working on the same symbol, call the base methods directly.

> ⚠️ **Cancel + close race (silent no-op — READ THIS).** The natural intuition
> ```python
> ctx.cancel_all_orders(sym)      # bar N
> ctx.close_position(sym)         # bar N — silently returns None
> ```
> **does not work.** `cancel_all_orders` only *requests* the cancel; the resting
> orders are still `has_open_order` == True for the rest of that bar. On the very
> next line, `close_position` sees a working order, hits the idempotency guard,
> returns `None`, and **the position is not closed**. Symptoms downstream:
> `positions` table shows `avg_px_close == 0`, `trades` has `exit_ts == 1970-01-01`,
> `Total Trades == 0` in `return_stats`, and any P&L in the report comes from
> end-of-backtest liquidation of the stuck position — not from your strategy.
>
> **Correct pattern — two-phase exit across bars (mirrors a real OMS):**
> ```python
> # Bar N — request cancel and mark the intent
> if condition_to_exit and not state[sym].get('exit_pending'):
>     ctx.cancel_all_orders(sym)
>     state[sym]['exit_pending'] = True
>     return
>
> # Bar N+1+ — verify cancel propagated, THEN send the close order
> if state[sym].get('exit_pending'):
>     if ctx.has_open_order(sym):
>         return                                             # still waiting
>     qty = ctx.net_position(sym)
>     if qty != 0:
>         (ctx.sell_order if qty > 0 else ctx.buy_order)(sym, quantity=abs(qty))
>     state[sym]['exit_pending'] = False
> ```
> Since the base `sell_order` / `buy_order` bypass the idempotency guard, they
> will fire even if a working order is still lingering — but you should still
> wait for `has_open_order == False` so you don't cross your own sibling.

> ⚠️ **`time_in_force` on LIMIT/STOP — pass `"GTC"` explicitly for brackets.**
> Although the documented auto-TIF says LIMIT/STOP default to `"GTC"` when
> `time_in_force=None`, live runs have shown the engine tagging bracket-child
> LIMIT and STOP orders with `"DAY"` in the `orders` table. A `DAY` bracket
> child expires at end of the entry session, leaving the position unprotected.
> When placing bracket children (or any order meant to persist past today's
> session), pass `time_in_force="GTC"` explicitly rather than relying on the
> documented default.

> **⚠️ Tick-size rounding — round your OWN `limit_price`/`stop_price` to the instrument tick.** Exchanges reject prices that aren't on the instrument's tick grid (e.g. a computed `123.4567` on a `0.01` grid). When you place orders yourself with an explicit price, round it with `adjust_tick_size`:
> ```python
> from hiveq.flow.trading.price_utils import adjust_tick_size
> px = adjust_tick_size(symbol, raw_price)        # rounds to the instrument's minTick
> ctx.buy_order(symbol, qty, order_type=OrderType.LIMIT, limit_price=px)
> ```
> **Executors (§5.10) round to the tick internally — do NOT call this for executor-worked orders.** `adjust_tick_size(symbol, price)` returns the price unchanged if the tick can't be resolved; `get_min_tick(symbol)` returns the tick (or `None`). Both are in `hiveq.flow.trading.price_utils`.

### 5.2.1 Auction order types & exchange cutoff rules (MOO / MOC / LOO / LOC)

Auction orders participate in an exchange's opening or closing cross/auction, not the continuous book. Pass them via `order_type=OrderType.MOO|MOC|LOO|LOC` (LOO/LOC also need `limit_price`). Each exchange enforces its own **entry cutoff** (latest time an order is accepted) and **cancel/modify cutoff** (after which the order is locked). These differ by venue.

> ⚠️ **Two requirements for auction orders to fill in a backtest:**
> 1. **Trade-print data.** Auction orders cross against the official open/close prints (`MCOfficialOpen` / `MCOfficialClose`), which live only in tick-level **trade** data. Subscribe with `ctx.subscribe_trades(...)` and use schema **`eq_trades`** (equities) or **`fut_trades`** (futures). Minute/second **bars** (`bars_*`) and **quotes** (`tbbo`) carry **no auction print** — auction orders on those never fill (§9.1).
> 2. **Primary listing exchange.** The cross happens at the symbol's primary venue. When `market_center` is **omitted, auction orders default to NASDAQ** — correct for NASDAQ-listed names (e.g. AAPL), so omitting it is the portable choice. Pass `market_center=` (e.g. `'NYSE'`) only to override routing. ⚠️ Explicit `market_center` on a *direct* `buy_order`/`sell_order` requires a recent engine — older deployed executors raise `TypeError: ... unexpected keyword argument 'market_center'`; if you hit that, omit it (or route via the `AUCTION` executor, which has always accepted `market_center`).

**Two ways to send an auction order:**

1. **Direct order** — `ctx.buy_order(sym, qty, order_type=OrderType.MOC, market_center='NYSE')` (or `MOO`/`LOO`/`LOC`; `LOO`/`LOC` need `limit_price`). Pass `market_center` to route to a venue; when omitted, auction orders default to **NASDAQ**. The backtest applies a single global cutoff pair — **open `09:28` ET / close `15:55` ET** (`MarketSessionDefaults`), which match Nasdaq's MOO/MOC entry cutoffs. ⚠️ Per-venue cutoff enforcement (NYSE's earlier `15:50` vs Nasdaq) is not yet keyed off `market_center` in the backtest fill model — routing is honored but model to the Nasdaq close (`15:55`) / be conservative (submit before `15:50`) until that lands (`docs/ENGINE_GAPS_PLAN.md` G9 #3).

2. **Auction executor** — the institutional path; build an `AUCTION` executor and let the OMS work it:
```python
def on_timer(self, ctx, event):                      # fire before the venue cutoff (§16.5)
    params = ctx.build_executor_params(
        symbol='AAPL', quantity=100, side='SELL',
        executor_type='AUCTION', order_type='MOC',   # 'MOC' | 'MOO' | 'LOC' | 'LOO'
        market_center='NYSE',                        # NYSE | NASDAQ | ARCA | … (+ MIC aliases: XNYS/XNAS/…)
    )
    ctx.add_executor(params)
```
The executor `market_center` map is comprehensive (full venue enum + MIC aliases) and the order-type map covers `MOC`/`MOO`/`LOC`/`LOO`/`LIMIT`/`MARKET`.

**Official exchange cutoff reference (all times ET):**

_Nasdaq — Opening Cross (9:30) / Closing Cross (16:00):_
| order | entry cutoff | cancel/modify cutoff |
|---|---|---|
| MOO | **9:28** (rejected after) | 9:25 (locked for the cross after) |
| LOO | **9:29:30** (after: IOC rejected; non-IOC re-typed to Imbalance-Only) | 9:25 |
| MOC | **15:55** (rejected after) | 15:50 (locked after) |
| LOC | **15:58** (rejected after) | 15:50 |

_NYSE — Opening Auction (9:30) / Closing Auction (16:00):_
| order | entry cutoff | cancel/modify cutoff |
|---|---|---|
| MOO | accepted until the DMM opens the security | **9:29** (cancel/replace rejected after) |
| LOO | until the DMM opens the security | 9:29 |
| MOC | **15:50** (after: only contra-side of a published Significant Imbalance, until 16:00) | **15:50** (no modify/cancel after; documented-error exception 15:50–15:58 per Rule 7.35B) |
| LOC | **15:50** (same contra-side-only rule after) | 15:50 |

Key takeaways for strategy code: submit MOC/LOC **before 15:50** to be venue-agnostic (NYSE's cutoff is earlier than Nasdaq's); submit MOO **before 09:28**; never rely on canceling an auction order in the final minutes — past the cancel cutoff it is locked. Use a timer + `ctx.now()` (§16.5) to place these ahead of the cutoff.

### 5.3 Order management
```python
ctx.cancel_order(order_id: str) -> bool
ctx.modify_order(order_id: str, quantity: float = None, limit_price: float = None, stop_price: float = None) -> bool
ctx.cancel_all_orders(symbol: str = None) -> bool
ctx.clear_pending_order(symbol: str) -> None
ctx.get_order_state(order_id: str) -> Optional[OrderState]
```

### 5.4 Position / order queries
```python
ctx.net_position(symbol: str) -> float          # signed; + long, - short, 0 flat
ctx.quantity(symbol: str) -> float              # alias of net_position
ctx.is_flat(symbol: str = None) -> bool
ctx.is_net_long(symbol: str) -> bool
ctx.is_net_short(symbol: str) -> bool
ctx.has_open_order(symbol: str = None) -> bool
ctx.open_order_qty(symbol: str) -> float
```

### 5.5 Portfolio accessors
```python
ctx.portfolio() -> SigmaPortfolio               # strategy-scoped (§8)
ctx.global_portfolio() -> SigmaGlobalPortfolio  # account-wide aggregate (§8)
```

### 5.6 Instrument
```python
ctx.instrument(symbol: str) -> SigmaInstrument  # §7.10
```

### 5.7 Time
```python
ctx.now() -> Optional[datetime]                 # configured tz
ctx.now_utc() -> Optional[datetime]             # UTC
ctx.trading_day -> Optional[str]                # 'YYYY-MM-DD' (property; tz-safe, prefer over deriving from ts_event)
```

### 5.8 Timers
```python
ctx.set_timer(timer_id: str, timer_interval)    # timer_interval: pandas.Timedelta or datetime.timedelta
ctx.cancel_timer(timer_id: str)
# Fires EventType.TIMER; event.data() -> TimerEventData (§7.8)
```

### 5.9 Event logging
```python
ctx.add_event_log(message: str, sub_event_type: Optional[str] = None,
                  symbol: Optional[str] = None, state_variable: Optional[Dict[str, Any]] = None)
#   logged with event_log_type = EventLogType.USER_LOG
ctx.log_parameter_change(param_name: str, old_value, new_value, symbol: Optional[str] = None)
```

### 5.9.1 Strategy logger

Use the HiveQ logger — **not** the standard Python `logging` module — so your output is controlled by `hiveq_log_level` (§2.1):

```python
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()   # module-level; call once per strategy file
```

The executor sets all non-HiveQ loggers to `WARNING` by default, and `logging.basicConfig` is a no-op once the executor has already installed handlers — so `logging.getLogger(__name__)` is silent regardless of level. The HiveQ logger is the only one controlled by `hiveq_log_level`.

```python
logger.debug("...")    # visible only when hiveq_log_level='DEBUG'
logger.info("...")     # visible at INFO and above (the default)
logger.warning("...")
logger.error("...")
```

**Add copious `logger.debug(...)` calls throughout every callback from the very first version of the strategy** — in `on_start`, on every `on_bar`, at every decision branch, and around every order. Because the default level is `INFO`, these are silent in normal runs and add zero noise. When a strategy produces no trades or behaves unexpectedly, re-run with `config={'hiveq_log_level': 'DEBUG', 'oms_console_log': True}` and all debug context surfaces immediately — no code changes required. See §11.5 for the complete debugging workflow.

`ctx.add_event_log(...)` (above) is complementary: it writes a structured, queryable row into the event-log table (readable via `run.event_logs()`). Use it for milestone events (pattern detected, target set, regime change) rather than for fine-grained per-bar diagnostics. Use `logger.debug` for everything else.

### 5.10 Executors — managed order working  (POV / TWAP / VWAP / AUCTION …)

An executor is a server-side execution algo that owns the *entire* order lifecycle for a target — it slices the parent quantity into child orders, places them, **modifies/replaces and cancels** as the market moves, aggregates fills, and guarantees order-state consistency even when a downstream provider misbehaves (chasing/re-pricing, repeated replaces). It is well-tested and shared across strategies. You hand it a target (qty/side/type/limits) and it works toward it, keeping **one executor handle per target** instead of you re-sending orders every bar.

**Use an executor when it fits the strategy — not always.** Reach for one when execution quality/reliability matters:
- working a **sizeable order** that should be sliced (POV/TWAP/VWAP),
- **live/livesim** trading where downstream fills are async and orders must be chased/replaced reliably,
- routing to an **auction** with venue handling (§5.2.1),
- any case where you'd otherwise hand-write replace/cancel/retry logic.

For **simple cases — a single immediate market order, or a signal-style backtest** that just needs a position — plain `buy_order`/`sell_order` (or the §5.2 sizing helpers) are simpler and sufficient; an executor adds no value there. Don't wrap a one-shot market order in an executor.

> ⚠️ **Executors require a tick-by-tick data stream — they do NOT work on bars.** POV/TWAP/VWAP/PASSIVE/AUCTION etc. slice and reprice against the live tick stream, so the strategy must subscribe to ticks — **prefer trades: `ctx.subscribe_trades(...)` with schema `eq_trades` (equities) / `fut_trades` (futures)**. (`ctx.subscribe_quotes(...)` with the `tbbo` schema also drives executors, but tbbo tick coverage is limited — default to `eq_trades`/`fut_trades`.) **Not** `bars_1m`/`bars_*` (§9.1) — subscribing only to bars and starting an executor will not work.

```python
ctx.build_executor_params(symbol: str, quantity: int, side: str, executor_type: str,
    start_time=None, end_time=None, min_order_size: int = 1, max_order_size: int = 100,
    refresh_millis: int = 100, participate_pct: float = None, aggressive_mult: float = None,
    min_notional: float = None, max_notional: float = None, market_center: str = None,
    order_type: str = None, time_in_force: str = None, nbbo_size_pct: float = None,
    account: str = None, custom_fix_params: dict = None) -> ExecutorParams
ctx.add_executor(executor_params) -> Executor | None       # start it; returns the handle (executor.executorID)
ctx.stop_executor(executor) -> bool
ctx.stop_executor_by_id(executor_id: str) -> bool
ctx.replace_executor_params_by_id(executor_id: str, executor_params) -> bool   # re-target IN PLACE (don't stack a new one)
ctx.get_executor_params_by_id(executor_id: str) -> ExecutorParams | None
ctx.executor_state(executor) -> str   # "STARTED"|"NEW"|"PARTIALLY_FILLED"|"FILLED"|"STOPPING"|"STOPPED"|"UNDEFINED"|"INVALID"
```

**Canned executor types** (pass as `executor_type=`; resolved server-side):
| `executor_type` | what it does | key params |
|---|---|---|
| `POV` | Percentage-of-volume — participates at a target % of traded volume | `participate_pct` (required), `aggressive_mult` |
| `POV_PASSIVE` | POV that rests passively (posts liquidity, less aggressive) | `participate_pct`, `nbbo_size_pct` |
| `PASSIVE` | Posts passively at/near the bid/ask, repricing as the book moves | `nbbo_size_pct`, `aggressive_mult` |
| `TWAP` / `ALGO_TWAP` | Time-weighted — even slices across `[start_time, end_time]` | `start_time`, `end_time`, `min/max_order_size` |
| `ALGO_VWAP` | Volume-weighted — slices along a volume curve over the window | `start_time`, `end_time` |
| `AUCTION` | Routes to the opening/closing auction (`order_type='MOC'/'MOO'/'LOC'/'LOO'`) | `order_type`, `market_center` (§5.2.1) |
| `ALGO_COBRA` | Adaptive liquidity-seeking algo | type-specific |

> These are the executor-type strings referenced in the engine (`build_executor_params` docstring). The authoritative registry is server-side (PySigma); if you need one not listed, confirm the exact string with the engine rather than guessing. `executor_type` is a **string**, not an enum.

Common params: `quantity` is unsigned (direction is `side='BUY'|'SELL'`); `min_order_size`/`max_order_size` bound child clip size; `refresh_millis` is the work cadence; `min_notional`/`max_notional` bound child notionals; `market_center` routes the venue (§5.2.1); `account`/`custom_fix_params` for live/FIX.

**Lifecycle:** `build_executor_params(...)` → `add_executor(params)` (returns the handle) → the executor works the order over time, emitting `EXECUTOR_EVENT` to your `on_executor` callback → check progress with `ctx.executor_state(executor)` → adjust with `replace_executor_params_by_id(id, new_params)` (**re-target in place — never `add_executor` a second one for the same target**) → `stop_executor(executor)` when done/cancelling. See the executor-driven strategy pattern in §16.6.

---

