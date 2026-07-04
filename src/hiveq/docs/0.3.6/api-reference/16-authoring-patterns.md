## 16. Authoring patterns (idioms for capabilities without a built-in helper)

HiveQ Flow is deliberately lean on *authoring conveniences* (target-percent sizing, rolling-history windows, indicator libraries, bracket/OCO orders, calendar scheduling) — implement those with the idioms below. **Execution quality, however, is a first-class engine feature: when the strategy calls for it (sliced/large orders, live order-chasing, auctions), use the canned executors (§5.10, §16.6) — but keep simple one-shot orders direct.** Follow these patterns exactly for consistency.

### 16.1 Flatten / reverse / target a position
Use the built-in helpers (§5.2) — `ctx.close_position`, `ctx.order_to_target`, `ctx.flatten_all`:
```python
ctx.close_position(symbol)                # flatten one symbol (no-op if flat)
ctx.order_to_target(symbol, 100)          # signed target: +long / -short / 0 flat; trades the delta
ctx.order_to_target(symbol, -50)
ctx.flatten_all()                         # close every open position in this strategy
```
These wrap `net_position` + `buy_order`/`sell_order`; the manual equivalent is `delta = target - ctx.net_position(symbol)` then a buy/sell for `abs(delta)`. **They are transactional** — each skips (returns `None`) when a working order already exists for that symbol, so calling them on every bar will not stack duplicate orders (it converges one fill at a time). If you need to re-price a resting order, `ctx.cancel_all_orders(symbol)` first.
> Percent-of-equity sizing (`order_target_percent`) is **not available yet** — the portfolio API exposes P&L/exposure but not cash/equity/buying-power (planned; see `docs/ENGINE_GAPS_PLAN.md` G2). Size in fixed quantity, or off `initial_capital` and a price you track yourself.

### 16.2 Rolling history / lookback (no `ctx.history()`)
Maintain your own window in strategy state; there is no engine-side history buffer.
```python
from collections import deque
class MA:
    def __init__(self): self.win = {}
    def on_start(self, ctx, event): ...
    def on_bar(self, ctx, event):
        bar = event.data()
        w = self.win.setdefault(bar.symbol, deque(maxlen=20))
        w.append(bar.close)
        if len(w) == w.maxlen:
            sma20 = sum(w) / len(w)
```
`ctx.instrument(symbol).last_bar` returns the most recent bar if you only need the latest price.

#### State across session boundaries

**`__init__` runs once per backtest run** and the same strategy instance
receives every subsequent `on_start`, `on_bar`, `on_order`, `on_position`,
and `on_stop`. Use per-instance `self.*` state normally — there is no
per-session re-instantiation to defend against.

```python
from collections import deque


class DailyMeanReversion:
    def __init__(self):
        # __init__ fires ONCE for the whole backtest — all state lives here.
        self.daily_closes = {}     # symbol -> deque[float]
        self.current_day = {}      # symbol -> current trading day
        self.current_day_close = {}

    def on_start(self, ctx, event):
        # on_start fires once per CALENDAR day (§4), same instance —
        # ctx.subscribe_* dedupes internally, so re-subscribing is safe.
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                           asset_type=AssetType.EQUITY,
                           interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        day = ctx.trading_day
        if self.current_day.get(bar.symbol) != day:
            prior_close = self.current_day_close.get(bar.symbol)
            if prior_close is not None:
                self.daily_closes.setdefault(
                    bar.symbol, deque(maxlen=20)
                ).append(prior_close)
            self.current_day[bar.symbol] = day
        self.current_day_close[bar.symbol] = bar.close
```

State on `self` is run-local process memory: it persists throughout one
backtest run, but not across separate backtest submissions or separate
executor containers. For state that must survive across runs, persist it
through an external store or input data source.

> **Earlier versions of this doc** recommended module-level containers on
> the grounds that `__init__` re-runs per session. That claim is
> **retracted** — treat `__init__` as running exactly once per run. If
> you're on an older executor where you observe repeated `__init__`
> calls, that's a framework bug, not the documented contract.

### 16.3 Indicators (no built-in TA library)
Compute with `numpy`/`pandas` (both are dependencies). Examples over a `deque`/`np.array` of closes:
```python
import numpy as np
closes = np.array(w)
sma = closes.mean()
ema = pd.Series(closes).ewm(span=12, adjust=False).mean().iloc[-1]
delta = np.diff(closes); up = delta.clip(min=0).mean(); dn = -delta.clip(max=0).mean()
rsi = 100 - 100/(1 + up/dn) if dn else 100.0
```

### 16.4 Bracket / stop-loss + take-profit (no native bracket/OCO)
Place the entry, then on its fill (`on_order`) submit protective child orders, and cancel the siblings yourself when one fills or the position flattens.
```python
def on_order(self, ctx, event):
    o = event.data()
    if o.is_filled and o.symbol == self.entry_symbol and not self.protected:
        entry = o.avg_px
        ctx.sell_order(o.symbol, quantity=o.filled_qty, order_type=OrderType.STOP,  stop_price=entry*0.98)
        ctx.sell_order(o.symbol, quantity=o.filled_qty, order_type=OrderType.LIMIT, limit_price=entry*1.04)
        self.protected = True
    # when one leg fills, flatten remainder & cancel_all_orders(symbol) to emulate OCO
```
There is no trailing-stop order type — emulate it in `on_bar` by tracking the high-water mark and `modify_order` on the stop.

### 16.5 Scheduling at wall-clock times (no `schedule.on(...)`)
Use `ctx.set_timer` (relative `timedelta`) plus `ctx.now()` checks, and `TradingCalendar` for session/day math.
```python
from datetime import timedelta
from hiveq.flow.utils.date_calendar import TradingCalendar
def on_start(self, ctx, event):
    ctx.set_timer('poll', timedelta(minutes=1))
def on_timer(self, ctx, event):
    now = ctx.now()                       # configured-tz datetime (ET sessions per R6)
    if now.hour == 15 and now.minute >= 55:
        # e.g. submit MOC near the close
        ...
```
`TradingCalendar.get_trading_days(start, end)` and `TradingCalendar.get_session_boundaries(...)` are public design-time helpers for trading-day/session windows.

### 16.6 Executor-driven strategies (when execution quality matters)
When the strategy needs managed execution — sizeable orders to slice, live trading with order-chasing/replaces, or auction routing (see "when to use" in §5.10) — let an **executor** work the order instead of hand-managing `buy_order`/`sell_order` + replaces + cancels. The executor handles child-order slicing, repricing, cancel/replace, and fill aggregation. For a simple one-shot market entry this is overkill — use a direct order. The idiom: **hold one executor handle per (symbol, role); check its state before starting another; re-target in place.**

⚠️ **Executors need a tick stream, not bars** (§5.10/§9.1): subscribe with `ctx.subscribe_trades(...)` — **prefer schema `eq_trades`/`fut_trades`** (`tbbo` quotes work but have limited coverage) — and drive them from `on_trade`. They will not work on a `bars_*` subscription.

```python
class ExecAlgo:
    def __init__(self):
        self.entry = {}                                  # symbol -> Executor handle

    def on_start(self, ctx, event):
        # Executors work the TICK stream — subscribe to trades (eq_trades/fut_trades), not bars.
        ctx.subscribe_trades(ctx.strategy_config.symbols, asset_type=AssetType.EQUITY)

    def on_trade(self, ctx, event):
        tick = event.data()                              # -> SigmaTradeTick (§7.5)
        sym = tick.symbol
        ex = self.entry.get(sym)
        # Only START a new executor when none is working this target.
        if ex is None or ctx.executor_state(ex) in ('FILLED', 'STOPPED', 'INVALID'):
            if self._want_long(tick) and ctx.is_flat(sym):
                params = ctx.build_executor_params(
                    symbol=sym, quantity=1000, side='BUY',
                    executor_type='POV', participate_pct=10,   # work at 10% of volume
                )
                self.entry[sym] = ctx.add_executor(params)
        else:
            # Already working — adjust IN PLACE, never add a second executor.
            # eid = str(ex.executorID); ctx.replace_executor_params_by_id(eid, new_params)
            pass

    def on_executor(self, ctx, event):                   # EXECUTOR_EVENT lifecycle updates
        ...                                              # state transitions, partials, completion
```

**Key pattern:** keep **one executor per (symbol, slot)** where slots are **`entry` / `exit` / `risk`**. Check `executor_state` before (re)starting a slot, and replace params rather than stacking. Auction exits (`exit=MOC`/`MOO`) map to the `AUCTION` executor with venue routing (§5.2.1).
