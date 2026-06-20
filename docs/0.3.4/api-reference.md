<!--
CANONICAL MACHINE-READABLE API SPEC FOR HIVEQ FLOW.
Audience: code-generation agents AND human developers.
Every signature, type, enum value, and dict key below is verified against source.
If you are unsure of a value, use the documented dataset/schema codes in §9 rather than guessing — the platform fetches the data at run time (this thin client has no local data access).
-->

# HiveQ Flow — Canonical API Specification

- **package**: `hiveq-flow` · **import root**: `hiveq.flow` · **version**: `0.3.4`
- **python**: `>=3.11`
- **scope of this doc**: backtest authoring · reading results · remote deploy + observability. (Live trading is out of scope here.)
- **how to read this**: this is a reference spec, not a tutorial. Signatures are exact. Defaults are exact. Enum `.value` strings are exact. Use the field tables in §7 to know what `event.data()` returns — it is otherwise untyped.

---

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
```

---

## 1. Minimal working example (canonical)

```python
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

class BuyAndHold:
    def __init__(self):
        self.bought = False

    # PER-EVENT CALLBACKS (default contract). One focused method per event type.
    def on_start(self, ctx: hf.Context, event):
        ctx.subscribe_bars(ctx.strategy_config.symbols, asset_type=AssetType.EQUITY, interval='1m')

    def on_bar(self, ctx, event):
        bar = event.data()                           # -> SigmaBar (§7.1)
        if not self.bought and ctx.is_flat(bar.symbol):
            ctx.buy_order(bar.symbol, quantity=100)
            self.bought = True

    def on_order(self, ctx, event):                  # NOT on_order_filled — fills come here
        order = event.data()                         # -> SigmaOrder (§7.3)
        if order.is_filled:
            fill = order.last_fill                   # -> SigmaFill (§7.4)

# run_backtest returns a Run HANDLE (§10.0), not a PerformanceReport directly.
run = hf.run_backtest(
    strategy_configs=[StrategyConfig(name='BuyAndHold', type='BuyAndHold')],
    symbols=['AAPL'],
    start_date='2025-08-01',
    end_date='2025-08-02',
    data_configs=[{'type': 'hiveq_historical', 'dataset': 'HIVEQ_US_EQ', 'schema': ['bars_1m']}],
)
report = run.report()                                # -> PerformanceReport (§10.1)
print(report.return_stats.to_string())
```

> **First time you run this:** if no key is saved yet, a browser opens for the user to sign in and this call waits ~5 min — that is expected. Just tell the user a browser is opening to sign in (and give the bare link if it didn't); wait, don't debug it, and never show internal commands or set a key by hand. See §3.1.

---

## 2. Entry points  (module: `hiveq.flow`)

```python
run_backtest(
    strategy_configs: list[StrategyConfig],
    symbols: Optional[list[str]] = None,
    start_date: Optional[str] = None,          # 'YYYY-MM-DD'
    end_date: Optional[str] = None,            # 'YYYY-MM-DD'
    data_configs: Optional[list[dict]] = None, # §9
    backtest_config: Optional[BacktestConfig] = None,
    *,                                          # keyword-only below
    silent: bool = False,                       # True -> deploy + return Run immediately (no blocking)
    **kwargs,                                   # engine configuration: config={...} / engine_config=EngineConfig(...) — §2.1
) -> Run                                        # ALWAYS returns a Run handle (§10.0), never a bare report
#   silent=False (default): deploy to platform, block w/ live progress, return the finished Run.
#   silent=True           : deploy and return the Run immediately (run.run_id / run.task_id).
#   In every mode: run.report() -> PerformanceReport (§10.1); run.positions()/.trades()/... -> DataFrame.

get_run(run_id: str, task_id: Optional[str] = None) -> Run   # re-attach to an existing run (§10.0)

event_logs() -> pandas.DataFrame                # logs of the LAST run_backtest, fetched over REST (§10.2)
config() -> EngineConfig                        # the module EngineConfig (timezone + params)

login(*, timeout=300.0, open_browser=True) -> str   # Internal plumbing: browser sign-in (loopback), opens a browser and BLOCKS ~5min for the user. You may invoke it invisibly on the user's behalf (§3.1); NEVER show it to the user or put it in deliverable scripts (§3).
```

**Precedence note**: `symbols`, `start_date`, `end_date` may be passed as top-level args OR set on `BacktestConfig`. Top-level args, when provided, populate the effective config. Set them in exactly one place to avoid ambiguity.

### 2.1 Engine configuration (via `**kwargs`)

Engine behavior is tuned through `run_backtest`'s `**kwargs`, in either of two equivalent forms:

```python
# (a) inline override dict — merged into the engine params:
run = hf.run_backtest(..., config={'hiveq_log_level': 'DEBUG'})

# (b) a full EngineConfig (e.g. to set the timezone) — see §13:
from hiveq.flow import EngineConfig
run = hf.run_backtest(..., engine_config=EngineConfig(timezone='America/New_York',
                                                      params={'hiveq_log_level': 'DEBUG'}))
```

Recognized keys (all optional; sensible defaults apply):

| key | type | default | purpose |
|---|---|---|---|
| `hiveq_log_level` | str | `'INFO'` | Executor log verbosity: `DEBUG` / `INFO` / `WARNING` / `ERROR`. Use `DEBUG`, then read the full executor log (incl. tracebacks) with `run.logs()` (§10.0). |
| `oms_console_log` | bool | `False` | Echo order-management-system activity to the executor console. |
| `futures_datasets` | list[str] | `['HIVEQ_US_FUT']` | Datasets treated as futures (enables contract resolution + rollover events, §7.12). |
| `signals_datasets` | list[str] | `['HIVEQ_QUANT_SIGNALS']` | Datasets that key off `config['symbols']` rather than the run's symbol universe. |
| `hiveq_data_page_size` | int | `100_000` | Max records per data request (raise for very dense schemas, e.g. options). |

Credentials/identity (API key, user/org) are **not** set here — they resolve from the environment (§3) and are intentionally not part of the engine config you pass.

### 2.2 Function registry & remote functions  (module: `hiveq.flow`)

**What it is.** The *function registry* is a versioned store of reusable Python callables that live on the HiveQ platform rather than in any one script. You write a function once, `push_function(...)` it (cloudpickled, with its `requirements` and a semver `version`), and from then on it can be fetched and run from anywhere — other scripts, other machines, or inside platform jobs — by name and version. It's the building block for sharing indicators, signal functions, and utilities across strategies and teammates without copy-pasting code.

**Why it's useful.**
- **Reuse & versioning** — one canonical, immutable `name@version`; bump the version to publish changes (existing versions never change under you).
- **Portability** — `load_function(name)` pulls the exact callable back on any machine; you don't carry the source around.
- **Run anywhere** — `run_function(func, ...)` executes a callable on the platform (a `QUANT_SCRIPTS` task) with its deps installed in the sandbox, and returns the result.

Functions are captured with cloudpickle; the registry host follows the same platform host as everything else (`HIVEQ_AUTH_URL`, override with `HIVEQ_FUNCTION_REGISTRY_URL`).

```python
push_function(func, *, version, name=None, requirements=None, docstring=None,
              namespace=None, override=False, include_source=True) -> dict
#   register a callable. version is semver ('1.0.0'). name defaults to func.__name__,
#   docstring to func.__doc__. requirements: ['pandas>=2.0'] or {'packages': [...]}.
#   Goes to YOUR namespace by default; namespace='default' publishes to public.
#   -> {'function_id', 'namespace', 'name', 'version'}

load_function(name, version=None, namespace=None) -> Callable
#   fetch a registered function back as a callable (latest version by default).

run_function(func, *args, task_name=None, requirements=None, job_type=None,
             wait=True, timeout=None, poll_interval=2.0, **kwargs) -> Any
#   run func(*args, **kwargs) on the platform as a QUANT_SCRIPTS task. Blocks and
#   returns the function's RETURN VALUE (wait=False -> {'task_id', ...} immediately).
#   requirements: pip specs installed in the sandbox, e.g. ['pandas>=2.0'].

list_functions(namespace=None, name=None) -> list[dict]   # name = regex filter
function_versions(name, namespace=None) -> dict           # {'name','versions':[...],'latest'}
get_function_source(name, version=None, namespace=None) -> dict
delete_function(name, version=None, namespace=None) -> dict   # one version, or all
```

```python
import hiveq.flow as hf

def zscore(series, window=20):
    """Rolling z-score."""
    import numpy as np
    return (series[-1] - np.mean(series[-window:])) / np.std(series[-window:])

# register once...
hf.push_function(zscore, version="1.0.0", requirements=["numpy"])
# ...then fetch it back and run it on the platform (from any script/machine).
# Pin the version for reproducibility (omit version=... to take the latest):
fn = hf.load_function("zscore", version="1.0.0")
hf.run_function(fn, [1, 2, 3, 4, 5], window=3, requirements=["numpy"])   # -> value
```

> **TBD — access control.** Function-level ACLs (who can read/run a function, fine-grained sharing across namespaces) are **not fully implemented yet**. Today: functions live in your own namespace by default; `namespace="default"` is the shared/public namespace. Treat cross-namespace permissions as subject to change.

---

## 3. Credentials

A **HiveQ API key** is the only credential required, and the SDK provisions it **automatically** — the user never sets, exports, or manages it by hand. Identity (user, org) is resolved from the key server-side.

How it actually works: the **first** `run_backtest` (any deploy) calls the SDK's init, which looks for a key in the environment / `~/.hiveq/.env`. If none is found, it opens a browser sign-in (loopback), mints the key, **writes it to `~/.hiveq/.env`, and reuses it on every later run** — no prompt, no env var, no code. There is no "set up your key first" step.

### 3.1 First run — the sign-in you will see, and what to do (AI agents: READ THIS)

When you (the AI) run anything that initializes the client with no key saved yet — the first `run_backtest`, or the dedicated sign-in step below — the process prints a banner like this to stdout and then **blocks for up to 5 minutes** waiting for the user to finish signing in:

```
────────────────────────────────────────────────────────────────
  Opening your browser to sign in to HiveQ…

  If it doesn't open, paste this link into your browser:

      https://<host>/cli-login?redirect_uri=http://127.0.0.1:<port>/callback&state=…&source=cli

  Waiting for you to finish signing in…
────────────────────────────────────────────────────────────────
```

A tiny local server on `127.0.0.1:<port>` (same machine as you) receives the redirect, writes the key to `~/.hiveq/.env`, prints `✓ Signed in.`, and the run continues. **This wait is expected and correct** — it is not a hang, a crash, a missing dependency, or a bug to debug.

**The whole experience the user should have is:** *a browser opens, they sign in (or sign up), and their key is saved.* That is it. Internal command names (`hf.login()`, `hiveq-login`, `python -c …`), file paths like `~/.hiveq/.env`, env vars, and the loopback mechanics are **plumbing — never show them to the user.** They cannot act on them and they make a one-click flow look like a chore.

**What you MUST do:**
1. **Trigger the sign-in for the user** — silently, on their behalf. Run the first backtest (init signs in automatically), or run a sign-in step in the background so the ~5-min wait doesn't block your tool call. *How* you trigger it is yours to manage; the user never needs to know the command.
2. **Tell the user only the human part, in plain words.** A browser window is opening for them to sign in; once they're signed in, they're all set. If the browser didn't open on its own, give them just the bare sign-in link to click:
   > 👉 **A browser will open so you can sign in to HiveQ.** (If it doesn't open, click here: `<url>`) Once you've signed in, you're all set — nothing else to do.
3. **Wait** for it to complete (the process prints `✓ Signed in.` once the key is written), then continue. Every later run reuses the saved key automatically — sign-in happens **once**.

**What you MUST NOT do** (the real failure modes):
- Treat the wait as a hang, kill the process, and start bisecting / debugging the SDK.
- Conclude the key is "missing" and tell the user to create, paste, or `export` a key, or hand-edit a file.
- Show the user internal commands (`hf.login()`, `hiveq-login`, etc.), env vars, or file paths and ask them to run/set anything.
- Retry the run in a loop or downgrade to a "manual API-key setup" workaround. Signup IS the browser flow — there is no manual key.

> If (and only if) sign-in raises `HIVEQ_AUTH_URL is not set`, the sign-in host hasn't been configured on this machine. It's normally already set; in that one case ask the user for their HiveQ host in plain language — do not invent or hard-code one.

### 3.2 When generating deliverable code or instructions, DO NOT:
- tell the user to `export HIVEQ_API_KEY=...` (or set it in a shell/`.env`/`os.environ` by hand) — the sign-in does this;
- add `hf.login()` to a strategy/backtest script — `run_backtest` already signs in on first use, and you handle first-run sign-in invisibly per §3.1;
- add hedging comments like `# if not already saved via hf.login()` or `# make sure your API key is set`;
- hard-code, print, or ask the user to paste a key;
- surface `hf.login()` / `hiveq-login` to the user as something they should run.

`hf.login()` / `hiveq-login` is **internal plumbing you may invoke on the user's behalf** (§3.1) or use to pre-authenticate a headless/CI box. It must **never** appear inside generated strategy/backtest code, nor be presented to the user as a step — a finished script must be runnable as-is, with **zero** credential setup mentioned to the user.

---

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
| `on_order` | `ORDER`, `ORDER_SUBMITTED/ACCEPTED/REJECTED/FILLED/CANCELED` | `SigmaOrder` (§7.3) |
| `on_position` | `POSITION`, `POSITION_OPENED/CHANGED/CLOSED` | `SigmaPosition` (§7.2) |
| `on_timer` | `TIMER` | `TimerEventData` (§7.8) |
| `on_custom_data` | `CUSTOM_DATA` | `SigmaCustomData` (§7.9) |
| `on_index_price` (alias `on_index`) | `INDEX_PRICE` | `IndexPrice` (§7.11) |
| `on_rollover` | `ROLLOVER` | `Rollover` (§7.12) |
| `on_executor` | `EXECUTOR_EVENT` | executor payload (opaque; §7.13) |
| `on_security_event` | `SECURITY_EVENT` | security payload (opaque; §7.13) |

- **There is NO `on_order_filled`.** Fills are delivered to **`on_order`**; check `order.is_filled` / `order.status` / `order.last_fill`.
- **`on_stop` / `EventType.STOP`**: fires after the engine has STOPPED. Do **not** place orders in STOP — they are rejected.
- **Global single-dispatch (opt-in, NOT default):** if you specifically want one method, branch on `event.type` (EventType → payload map in §7.0) in a single `on_hiveq_event`. Two equivalent forms: (a) a **class** with `on_hiveq_event(self, ctx, event)` deployed via `StrategyConfig(name=..., type='YourClass')`; or (b) a **module-level** `def on_hiveq_event(ctx, event):` deployed with `run_backtest(strategy_configs=[], ...)` — the engine auto-discovers the captured function. (Empty `strategy_configs` is accepted *only* for this global form; otherwise it errors.) Use this only on explicit request — per-event callbacks are the default.

---

## 5. Context API — `ctx` (runtime type: `SigmaContext`)

`hf.Context` is a public type-alias for hints; the engine passes a `SigmaContext`.

### 5.1 Subscriptions  (call in START; return `None`)
```python
ctx.subscribe_bars(symbols: List[str], asset_type: AssetType = None, interval: str = "1m")
ctx.subscribe_quotes(symbols: Optional[List[str]], asset_type: AssetType = None)
ctx.subscribe_trades(symbols: List[str], asset_type: AssetType)      # trades; with the `tbbo` schema, quotes arrive too (on_quote)
ctx.subscribe_data(data_id: str, signals: List[str] = None)               # custom / signal data → on_custom_data
#   data_id must match the 'id' field in data_configs (CSV or HIVEQ_QUANT_SIGNALS).
#   signals: optional list of signal names to filter; None subscribes to all.
ctx.subscribe_index(symbols: List[str])                                   # spot index value
ctx.subscribe_index_bars(symbols: List[str], interval: str = '1d')        # index OHLCV (daily only)
ctx.subscribe_option_snaps(symbol: str, option_type: Optional[str] = None,  # 'C'|'P'|'CALL'|'PUT'
                           strike: Optional[float] = None,
                           expiration_type: Optional[str] = None,         # '0dte' | 'YYYY-MM-DD'
                           underlying: Optional[str] = None, interval: str = "1s")
# Futures: subscribe by SYMBOL STRING in `symbols=` (the clear, single way):
ctx.subscribe_futures_bars(symbols=["ES.c.0"], interval="1m")
ctx.subscribe_futures_trade_ticks(symbols=["ES.c.0"])
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

> These are the executor-type strings referenced in the engine (`build_executor_params` docstring + the `algo_instruction` slot map). The authoritative registry is server-side (PySigma); if you need one not listed, confirm the exact string with the engine rather than guessing. `executor_type` is a **string**, not an enum.

Common params: `quantity` is unsigned (direction is `side='BUY'|'SELL'`); `min_order_size`/`max_order_size` bound child clip size; `refresh_millis` is the work cadence; `min_notional`/`max_notional` bound child notionals; `market_center` routes the venue (§5.2.1); `account`/`custom_fix_params` for live/FIX.

**Lifecycle:** `build_executor_params(...)` → `add_executor(params)` (returns the handle) → the executor works the order over time, emitting `EXECUTOR_EVENT` to your `on_executor` callback → check progress with `ctx.executor_state(executor)` → adjust with `replace_executor_params_by_id(id, new_params)` (**re-target in place — never `add_executor` a second one for the same target**) → `stop_executor(executor)` when done/cancelling. See the executor-driven strategy pattern in §16.6.

---

## 6. The Event object

```
event.type        -> EventType          # branch on this
event.data()      -> payload object     # type depends on event.type (§7.0)
event.ts_event    -> int                # nanoseconds
event.time        -> Optional[datetime] # configured tz
event.time_utc    -> Optional[datetime] # UTC
```

---

## 7. Event payloads — what `event.data()` returns

### 7.0 EventType → payload map
| event.type | event.data() returns | section |
|---|---|---|
| `BAR` (and `BAR_1_MIN`…`BAR_1_DAY`) | `SigmaBar` | 7.1 |
| `TRADE` | `SigmaTradeTick` | 7.5 |
| `QUOTE` | `SigmaQuoteTick` | 7.6 |
| `SNAP` | `SigmaSnapData` (options) | 7.7 |
| `ORDER_FILLED`, `ORDER_*` | `SigmaOrder` | 7.3 |
| `POSITION_*` | `SigmaPosition` | 7.2 |
| `CUSTOM_DATA` | `SigmaCustomData` | 7.9 |
| `TIMER` | `TimerEventData` | 7.8 |
| `INDEX_PRICE` | `IndexPrice` | 7.11 |
| `ROLLOVER` | `Rollover` | 7.12 |
| `EXECUTOR_EVENT` | executor lifecycle payload (opaque) | 7.13 |
| `SECURITY_EVENT` | security/reference payload (opaque) | 7.13 |

### 7.1 SigmaBar
`symbol:str` · `open:float` · `high:float` · `low:float` · `close:float` · `volume:float` · `interval:str` · `ts_event:int(ns, close)` · `ts_init:int(ns, open)` · `time:datetime?` · `time_utc:datetime?`

### 7.2 SigmaPosition
`symbol:str` · `quantity:float`(signed) · `side:str`("LONG"|"SHORT"|"FLAT") · `avg_price:float` (aliases `entry_price`,`average_price`) · `market_value:float` · `realized_pnl:float` · `unrealized_pnl:float` · `total_pnl:float` · `day_pnl:float` · `notional:float` · `fees:float` · `is_open:bool` · `is_flat:bool` · `is_long:bool` · `is_short:bool` · `ts_event:int`

### 7.3 SigmaOrder
`symbol:str` · `side:OrderSide` · `quantity:float` · `order_type:OrderType` · `time_in_force:str` · `limit_price:float?` · `stop_price:float?` · `order_id:str` · `client_order_id:str` · `status:OrderStatus` · `filled_qty:float` · `leaves_qty:float` · `avg_px:float?` · `last_px:float?` · `last_qty:float?` · `reject_reason:str?` · `last_fill:SigmaFill?` · `commission:float` · `is_buy:bool` · `is_sell:bool` · `is_filled:bool` · `is_open:bool` · `account:str` · `executor_id:str`(empty if placed directly) · `market_center:str` · `ts_event:int` · `ts_init:int` · `time:datetime?` · `time_utc:datetime?`

### 7.4 SigmaFill  (via `order.last_fill`)
`trade_id:str` · `execution_id:str` · `last_qty:float`(alias `filled_qty`) · `last_px:float`(alias `avg_px`) · `commission:float` · `liquidity_side:str`("MAKER"|"TAKER") · `symbol:str` · `side:str`("BUY"|"SELL") · `ts_event:int`

### 7.5 SigmaTradeTick
`symbol:str` · `price:float` · `size:float` · `aggressor_side:str`("BUY"|"SELL"|"NO_AGGRESSOR") · `trade_id:str` · `exchange:str` · `ts_event:int` · `time/time_utc:datetime?`

### 7.6 SigmaQuoteTick
`symbol:str` · `bid_price:float` · `ask_price:float` · `bid_size:float` · `ask_size:float` · `mid_price:float` · `spread:float` · `exchange:str` · `ts_event:int` · `time/time_utc:datetime?`

### 7.7 SigmaSnapData (options snapshot)
`symbol:str`(root) · `chain:str`(OCC) · `underlying:str` · `option_type:str`("C"|"P") · `expiration_date:str` · `strike:float` · `bid_px:float` · `ask_px:float` · `price:float` · `bid_sz:int` · `ask_sz:int` · `size:int` · `mid_price:float` · `spread:float` · `date:str`('YYYY-MM-DD') · `ts_event:int` · `time/time_utc:datetime?` · method `column_data(name, default=None)`

### 7.8 TimerEventData
`timer_id:str` · `ts_event:int` · `ts_init:int` · `time/time_utc:datetime?`

### 7.9 SigmaCustomData
`symbol:str` · `event_id:str`(data source id) · `data:Dict[str,str]`(column→value) · `header:str`(CSV header row) · `row:str`(raw CSV row) · `ts_event:int` · `time/time_utc:datetime?` · method `column_data(name, default=None)`

`column_data(name, default=None)` returns the string value of the named CSV column for this row, or `default` if the column is absent. All values are strings — cast to `float`/`int`/`bool` in strategy code.

**CSV custom data** (`data_type='custom'`): every column in your CSV is accessible by name via `column_data()`. The engine uses `date` + `time` columns (or a `timestamp` column) to determine when each row fires during the backtest; all other columns are user-defined.

**HIVEQ_QUANT_SIGNALS**: rows arrive with a `signal_json` column containing a JSON-encoded string. Parse it with `json.loads(data.column_data("signal_json"))` to access signal fields.

### 7.10 SigmaInstrument  (`ctx.instrument(symbol)`)
`symbol:str` · `last_bar:Bar?` · `multiplier:float` · `exchange` · `min_tick:float` · `asset_type:AssetType` · `current_contract:str`(resolved contract for continuous) · `security_details` · `native_instrument_id` · `tradeStats:SigmaTradeStats?`(`symbol,open,high,low,close,volume`)

### 7.11 IndexPrice
`symbol:str` · `price:float` · `ts_event:int` · `ts_init:int`

### 7.12 Rollover
`continuous_symbol:str`("ES.c.0") · `prev_contract:str`("ESZ5") · `current_contract:str`("ESH6") · `ts_event:int`

### 7.13 EXECUTOR_EVENT / SECURITY_EVENT (advanced)
These fire for executor lifecycle transitions (`on_executor`) and security/reference updates (`on_security_event`). Their payloads are not part of the stable strategy-authoring surface — treat `event.data()` as opaque. For executors, prefer `ctx.executor_state(executor)` (§5.10) to read state rather than parsing the event. Most strategies do not handle these.

---

## 8. Portfolio API  (`SigmaPortfolio` and `SigmaGlobalPortfolio` share this surface)

```python
.position(symbol: str) -> Optional[SigmaPosition]
.net_position(symbol: str) -> float
.is_flat(symbol: str = None) -> bool
.is_net_long(symbol: str) -> bool
.is_net_short(symbol: str) -> bool
.positions() -> List[SigmaPosition]
.realized_pnl(symbol: str = None) -> float       # total when symbol omitted
.unrealized_pnl(symbol: str = None) -> float
.total_pnl(symbol: str = None) -> float          # realized + unrealized
.day_pnl(symbol: str = None) -> float
.net_exposure() -> float                          # signed
.gross_exposure() -> float                        # sum of |values|
.max_drawdown -> float       (property)
.fees -> float               (property)
.initial_capital -> float    (property)           # capital the run started with (fixed base)
.equity -> float             (property)           # account value / NAV = initial_capital + realized + unrealized − fees
.cash -> float               (property)           # uninvested cash (equity − market value of fully-funded holdings)
```
- `SigmaPortfolio` = current strategy only. `SigmaGlobalPortfolio` = summed across all strategies.
- **Account view** (`initial_capital` / `equity` / `cash`): tracked by the engine's cash ledger, not derived in Python. `equity` is asset-agnostic (correct for equities and futures). `cash` reflects that **futures encumber margin, not cash** — buying a future moves only fees out of `cash` (not notional), so `cash ≈ equity` for futures positions while equities reduce `cash` by the full notional. Identity: `equity == initial_capital + realized_pnl + unrealized_pnl` (minus fees). Use `equity` for percent-of-account sizing; there is no built-in `order_target_percent` — size yourself (e.g. `qty = int(pct * ctx.portfolio().equity / (price * ctx.instrument(sym).multiplier))`).

---

## 9. `data_configs` schema  (list of dicts)

### 9.1 `type='hiveq_historical'`
| key | type | notes |
|---|---|---|
| `type` | str | `'hiveq_historical'` |
| `dataset` | str | dataset code: `HIVEQ_US_EQ`, `HIVEQ_US_FUT`, `HIVEQ_US_OPT`, `HIVEQ_US_IND`, `HIVEQ_US_ETF`, `HIVEQ_QUANT_SIGNALS`, `HIVEQ_ECON`. (`HIVEQ_STRAT` holds published run *results* — an output you read after a run, not a strategy input.) |
| `schema` | list[str] \| str | one or more exact schema codes, e.g. `bars_1m`, `bars_1s`, `bars_1d`, `tbbo`, `eq_trades`, `fut_trades`, `snaps_1s`, `signals`. Bar data exists only at `bars_1s` / `bars_1m` / `bars_1d` per asset class — there is **no** `bars_5m` / `bars_1h`. **Trade prints** (incl. the opening/closing **auction** prints that MOO/MOC fill against, §5.2.1) come from `eq_trades` (equities) / `fut_trades` (futures); `tbbo` is **quotes** (bid/ask) and `bars_*` are aggregated — neither carries trade/auction prints. **Executors** (POV/TWAP/VWAP…, §5.10) also need a tick stream — **prefer `eq_trades`/`fut_trades`** (`tbbo` quotes work too but have limited coverage); never `bars_*`. |
| `id` | str (opt) | identifier referenced by `ctx.subscribe_data(data_id=...)` for signal/custom sources |
| `enabled` | bool (opt) | default `True` |

```python
# equities 1-minute bars
{'type':'hiveq_historical','dataset':'HIVEQ_US_EQ','schema':['bars_1m']}
# futures (subscribe to a continuous symbol like 'ES.c.0'); for rollover set
# BacktestConfig(enable_auto_rollover=True) — no data_configs flag needed.
{'type':'hiveq_historical','dataset':'HIVEQ_US_FUT','schema':['bars_1m']}
# quant signals (subscribe via ctx.subscribe_data(data_id=...))
{'type':'hiveq_historical','dataset':'HIVEQ_QUANT_SIGNALS','schema':['signals'],
 'id':'mysignals','symbols':['My_Signal_Name']}
```

**`HIVEQ_QUANT_SIGNALS`** delivers platform-hosted signal data to `on_custom_data`. Each row has a `signal_json` column containing a JSON-encoded payload with the signal fields. Parse it in your strategy:
```python
def on_custom_data(self, ctx, event):
    data = event.data()
    sig = json.loads(data.column_data("signal_json"))
    value = sig.get("my_field")
```

The `symbols` key in `data_configs` selects which signal stream to subscribe to. The `id` field must match the `data_id` argument in `ctx.subscribe_data(data_id=...)`.

> **Backtest with your own signals?** Use a CSV custom data source instead (§9.2) — it fires the same `on_custom_data` callback. Put your signal fields as columns in the CSV and read them with `column_data()`. No `signal_json` wrapping needed.

```python
```

### 9.2 `type='csv'`
| key | type | notes |
|---|---|---|
| `type` | str | `'csv'` |
| `data_type` | str | fill-mode hint for your own file: a `bars_*` value (OHLCV → bar fills), `'tbbo'`/`'trades'` (tick fills), or `'custom'` (user/signal data). For CSV the granularity is whatever your file contains. |
| `path` | str | path to CSV (relative or absolute) |
| `id` | str | identifier referenced in strategy subscriptions |
| `enabled` | bool (opt) | default `True` |

```python
{'type':'csv','data_type':'bars_1m','id':'1_MIN_BAR','path':'bars/AAPL_bars.csv'}
{'type':'csv','data_type':'custom','id':'UserData','path':'userdata/signals.csv'}
```
CSV bar columns: `timestamp,symbol,open,high,low,close,volume`.

**CSV custom/signal columns** (`data_type='custom'`):

The engine requires **three mandatory columns** (matched by header name, case-insensitive, any position):

| Column | Required | Format | Notes |
|---|---|---|---|
| `date` | **yes** | `YYYY-MM-DD` | Combined with `time` to determine when the row fires during backtest |
| `time` | **yes** | `HH:MM:SS` | Combined with `date` for the event timestamp (`ts_event`) |
| `sym` | **yes** | string | Symbol identifier; available as `data.symbol` and `data.column_data("sym")` |
| *(any other)* | no | string | User-defined columns; read with `data.column_data("col_name")` |

> **All three columns are mandatory.** The engine raises an error if any of `date`, `time`, or `sym` is missing from the CSV header. Column order does not matter — the engine locates them by name.

All column values arrive as strings in the strategy. Cast to the appropriate type in your code:
```python
zone_prob = float(data.column_data("zone_prob", default="0"))
enabled = data.column_data("gate", default="false").lower() == "true"
```

> **Note:** Values containing commas must use `|` (pipe) as a separator instead, since the engine uses simple comma-splitting (not RFC 4180 quoted CSV). Decode pipes back to commas in strategy code if needed.

#### Daily file pattern (`_yyyymmdd.csv`)

When the CSV path ends with **`_yyyymmdd.csv`**, the engine treats it as a date pattern and automatically resolves the file for each backtest day by replacing `yyyymmdd` with the date in `YYYYMMDD` format. This lets you organize signal data into one file per day:

```
signals/kx_signals_20250602.csv
signals/kx_signals_20250603.csv
signals/kx_signals_20250604.csv
...
```

Reference the pattern (not an individual file) in `data_configs`:
```python
{'type':'csv','data_type':'custom','id':'my_signals','path':'signals/kx_signals_yyyymmdd.csv'}
```

On each backtest day, the engine opens the matching file (e.g. `signals/kx_signals_20250602.csv` on June 2). If the file for a given day doesn't exist, no custom data events fire for that day. Each daily file must have the same header row with the three mandatory columns (`date`, `time`, `sym`).

Upload all daily files to the platform before running:
```bash
hiveq-data -u signals/                         # uploads all files in the directory
```

#### End-to-end: using a CSV signal file in a backtest

Strategies run on the HiveQ platform, not on your local machine. CSV data files must be uploaded to your persistent-data store **before** submitting the backtest. The `path` in `data_configs` must match the uploaded path exactly — the platform executor resolves it against your store at runtime.

1. **Create the CSV** with the three mandatory columns (`date`, `time`, `sym`) followed by your signal columns:
   ```
   date,time,sym,zone_prob,drift_price,iv_quintile_ewm,quote_gate_enabled
   2025-06-02,14:00:00,ES.c.0,0.65,5960.25,4.0,true
   2025-06-02,14:01:00,ES.c.0,0.63,5959.50,4.0,true
   ```

2. **Upload to the platform** — the file must exist in your persistent-data store before the strategy runs:
   ```bash
   hiveq-data -u signals/my_signals.csv
   ```
   This stores the file as `signals/my_signals.csv` on the platform.

3. **Wire in `data_configs`** — the `path` must match the uploaded path exactly, and the `id` must match the `subscribe_data` call:
   ```python
   data_configs=[
       {'type':'hiveq_historical','dataset':'HIVEQ_US_FUT','schema':['bars_1m']},
       {'type':'csv','data_type':'custom','id':'my_signals','path':'signals/my_signals.csv'},
   ]
   ```

4. **Subscribe in `on_start`**:
   ```python
   ctx.subscribe_data(data_id='my_signals')
   ```

5. **Read in `on_custom_data`** — each CSV row fires as a `SigmaCustomData` event at the time specified by its `date` + `time` columns:
   ```python
   def on_custom_data(self, ctx, event):
       data = event.data()
       zone_prob = float(data.column_data("zone_prob", default="0"))
       drift = float(data.column_data("drift_price", default="0"))
       # ... use in strategy logic
   ```

#### `hiveq-data` — managing data files on the platform

All strategies run on the HiveQ platform, not on your local machine. CSV files referenced in `data_configs` must be uploaded to your per-user persistent-data store **before** submitting the backtest. The platform executor resolves `path` against the store at runtime — the path you upload with is the path you must use in `data_configs`.

**Runtime store location:** `/home/hivequser/hiveq/persistent_data/`

A file uploaded as `signals/my_signals.csv` is available at runtime as `/home/hivequser/hiveq/persistent_data/signals/my_signals.csv`. Reference it in `data_configs` by its **relative path** (`signals/my_signals.csv`), not the full runtime path.

```bash
# Upload (prerequisite — must complete before running the strategy)
hiveq-data -u signals/my_signals.csv           # upload a single file
hiveq-data -u signals/                         # upload a whole directory (recursive)
hiveq-data -u signals/ --force                 # re-send everything (skip MD5 check)
hiveq-data -u signals/ --dry-run               # preview what would be sent

# Verify what's on the platform
hiveq-data -l                                  # list everything in your store
hiveq-data -l signals                          # list a subdirectory

# Remove files
hiveq-data --rm signals/old.csv                # a single file
hiveq-data --rm signals                        # a whole subdirectory
```

Uploads are **incremental** (rsync-like) — a file is sent only if it's new or its content changed, compared by MD5 against the server's listing. Requires `HIVEQ_API_KEY` in your environment or `~/.hiveq/.env`.

> **Path anchoring:** `hiveq-data -u` preserves the directory structure relative to the argument. Uploading `signals/my_signals.csv` stores it as `signals/my_signals.csv`. Uploading just `my_signals.csv` stores it at the root as `my_signals.csv`. Always verify the result with `hiveq-data -l` and match the stored path exactly in `data_configs`.

> **Do not use absolute or `Path(__file__)`-based paths** in `data_configs` — they resolve on your local machine but not on the platform. Always use relative paths that match the uploaded location.

**Example workflow:**
```bash
# 1. Upload
hiveq-data -u signals/kx_signals.csv

# 2. Verify
hiveq-data -l signals
#   signals/kx_signals.csv    28.2 KB  b9e0947a76f1  2026-06-20T12:21:27Z
```
```python
# 3. Reference in data_configs (path matches uploaded path exactly)
{'type':'csv','data_type':'custom','id':'my_signals','path':'signals/kx_signals.csv'}
```

### 9.3 Behavior derived from schema/dataset
- schema containing `bar` → bar-based fills; schema with `trade`/`tbbo` → tick-based fills.
- `dataset='HIVEQ_US_FUT'` → futures session defaults (18:00–17:00 ET) applied automatically.
- `dataset='HIVEQ_US_OPT'` + `snaps_*` schema → options snapshot handling.

---

## 10. Results

### 10.0 `Run` handle (returned by `run_backtest` / `get_run`)
`run_backtest(...)` and `get_run(...)` return a `Run` (module `hiveq.flow.runs`). It is the single accessor for a run's status and results, local or remote.

```python
run.run_id -> str
run.task_id -> Optional[str]
run.is_local -> bool
run.wait(timeout=None, poll_interval=1.0, progress=True) -> Run   # block until terminal; returns self (chainable)
run.status() -> dict                       # {'status': 'PENDING'|'RUNNING'|'DONE'|'FAILED'|..., ...}
run.report(include: Optional[list[str]] = None) -> PerformanceReport   # §10.1
run.positions() / run.orders() / run.trades() / run.daily_returns() / run.equity_curve() / run.metrics() / run.event_logs() -> pandas.DataFrame
run.summary() -> dict
run.overview() -> dict
run.tearsheet(output: Optional[str] = None) -> str   # writes a quantstats tearsheet file; returns the path. Format from extension: .html -> HTML, anything else -> PDF. Default name: <task_name|run_id>.pdf
run.logs() -> list[str]                    # the COMPLETE remote executor log (stdout/strategy errors), by task_id
run.download_logs(path: str) -> str        # stream the full gzipped executor log to `path` (.gz); returns path
```
- Default `run_backtest()` already blocked until done, so `run.report()` is ready immediately.
- **`hf.get_run(run_id, task_id=None)`** re-attaches to a run that was already submitted and returns its `Run` handle. `run_backtest` runs on the platform, so its results outlive your Python process — `get_run` is how you reconnect later (a new session, a different machine, or after a `silent=True` deploy) to check status or pull results without re-running anything. Pass the `run_id` from an earlier `run.run_id`. Typical use: `hf.get_run(run_id).wait().report()`.
- **`event_logs()` vs `logs()`**: `event_logs()` is the strategy's structured event-log table from the runs REST API (`ctx.add_event_log(...)` rows, keyed by run_id). `logs()` is the raw executor **stdout** — `print(...)` output and strategy-callback crashes (e.g. `STRATEGY_CALLBACK_ERROR`) that never reach `event_logs()` — fetched as the whole gzip log by **task_id** (`run_id != task_id`). Use `logs()` to debug a run that produced nothing; `download_logs(path)` for very large logs.

### 10.1 `PerformanceReport` (obtained via `run.report()`)
| attribute | type |
|---|---|
| `return_stats` | DataFrame (Sharpe, Sortino, vol, drawdown, win rate, …) |
| `returns_series` | Series (datetime-indexed equity returns; used for tearsheet) |
| `positions` | DataFrame |
| `fills` | DataFrame |
| `orders` | DataFrame |
| `trades` | DataFrame |
| `pnl_stats` | DataFrame |
| `daily_returns` | DataFrame (ascending by date) |
| `strategy_stats` | DataFrame (per-strategy) |
| `run_info` | DataFrame (dates, capital, counts, params) |
| `tca_report` | TCAReport (only if `BacktestConfig.enable_tca=True`) |
| `total_realized_pnl` / `total_unrealized_pnl` / `net_pnl` / `total_fees` | float |
| `create_tearsheet()` | `-> str` (quantstats HTML tearsheet; for Jupyter/Marimo) |
| `summary_stats()` | `-> dict \| None` (quantstats metric → value, e.g. Sharpe/CAGR/max drawdown) |

DataFrame attrs may be `None`/empty — always guard (`if report.fills is not None and not report.fills.empty`).

**Tearsheet / metrics (quantstats).** `create_tearsheet()` and `summary_stats()` are powered by **quantstats**, which ships with the SDK (no extra install). Both read `report.returns_series`, so they need a run that produced returns; on missing/empty returns `create_tearsheet()` returns a small "no data" HTML page and `summary_stats()` returns `None`.

```python
report = run.report()                 # or hf.get_run(run_id).wait().report()

# Tearsheet file on disk (equity curve, drawdowns, monthly returns, risk metrics).
# run.tearsheet() is the single entry point; the format is chosen from the output
# extension — `.html` writes a standalone HTML file, anything else writes a PDF.
path = run.tearsheet()                       # -> '<task_name|run_id>.pdf' in the cwd
path = run.tearsheet(output='my_report.pdf')
path = run.tearsheet(output='my_report.html')

# Only when you need the HTML *string* to render inline in a notebook:
html = report.create_tearsheet()
# In a Marimo notebook:
import marimo as mo; mo.md(html)
# In a Jupyter notebook:
from IPython.display import display, HTML; display(HTML(html))
# In a plain script create_tearsheet() returns a basic text report instead.

# Metrics as a dict (Sharpe, CAGR, max drawdown, …)
stats = report.summary_stats()        # None if the run has no returns_series
if stats:
    print(stats["Sharpe"], stats["Max Drawdown"])
```

### 10.2 `event_logs()` DataFrame columns (remote runs)
`time(datetime, tz-aware)` · `ts_event(str, ISO-8601 UTC)` · `strategy_id` · `trader_id` · `nav(float)` · `realized_pnl(float)` · `total_pnl(float)` · `symbol` · `event_log_type(str)` · `sub_event_type(str)` · `message(str)` · `state_variables(str, JSON)` · `trade_id(str?)`
> `event_logs()` returns rows for **remote** runs (fetched over REST). A **local** run returns an empty DataFrame (`runs.py` short-circuits when `run.is_local`). You can also pull logs for any job via §11.3.

---

## 11. Remote deploy + observability  (`hiveq.flow.jobs`)

One surface to deploy a job and pull its status/logs/results. A thin **direct-REST** client (built on `requests`) against the platform API — there is no `hiveq_orchestrator` package or any second install. Uses the same API key (§3).

```python
from hiveq.flow.jobs import (
    TaskType,
    submit,                                     # deploy
    poll_result, get_status, get_result, get_logs, get_logs_gz, get_client,  # observe
)
```

### 11.1 Deploy a backtest (high-level)
Prefer **`hf.run_backtest(..., silent=True)`** (§2) — it captures your strategy class automatically and returns a `Run` handle immediately (`run.run_id`, `run.task_id`). Observe via the Run (`run.wait()`, `run.status()`, `run.report()`, §10.0) — no need to touch the lower-level `jobs` API for the common case.

```python
run = hf.run_backtest(strategy_configs=[...], symbols=['AAPL'],
                      start_date='2025-08-01', end_date='2025-08-02',
                      data_configs=[{'type':'hiveq_historical','dataset':'HIVEQ_US_EQ','schema':['bars_1m']}],
                      silent=True)
report = run.wait().report()                    # block to completion, then read results
```
For the common backtest case use `run_backtest(..., silent=True)` above; the `jobs` API below remains valid for any job type and for low-level control.

### 11.2 Generic submit
```python
submit(task_type: TaskType|str, task_name: str, task,
       entry_method: str = 'run', job_type=None,
       metadata=None, requirements=None,
       allow_duplicate: bool = False, duplicate_action=None) -> dict   # -> {'task_id', 'payload_id', ...}
#   duplicate_action ∈ {'override', 'terminate', 'duplicate'} (only consulted when allow_duplicate=True)
```

### 11.3 Observe (works for any job type)
```python
get_status(task_id: str) -> dict                # {'status': 'PENDING'|'RUNNING'|'DONE'|'FAILED'|..., 'created_at', 'started_at', 'completed_at'}
get_logs(task_id: str = None, task_name: str = None, limit: int = 1000) -> dict   # remote executor logs, JSON tail/paginate
get_logs_gz(task_id=None, run_id=None, task_name=None, dest: str = None) -> str    # FULL log via GET /logs?format=gz (streamed)
get_result(task_id: str) -> dict                # result/output (non-blocking; for completed task)
poll_result(task_id: str, timeout: Optional[int] = None, poll_interval: float = 1.0) -> dict  # BLOCK until terminal state
get_client() -> _Client                         # low-level transport handle (advanced)
```
- The platform `GET /logs` accepts `task_id` | `run_id` | `task_name` (one required), plus `limit` / `offset` / `tail` / `format` (`json` default, or `gz` for the whole gzipped log). `get_logs(...)` returns the JSON tail; `get_logs_gz(...)` returns the **complete** log text (or, with `dest`, streams the `.gz` to that path). Prefer the `Run` handle — `run.logs()` / `run.download_logs(path)` (§10.0) — which key off the run's `task_id` for you.

### 11.4 Recommended observe loop (poll status + pull logs)
```python
from hiveq.flow.jobs import get_logs

# Preferred: deploy and observe via the Run handle (§10.0).
run = hf.run_backtest(strategy_configs=[...], symbols=['ES.c.0'],
                      start_date='2024-01-01', end_date='2024-03-01',
                      data_configs=[{'type':'hiveq_historical','dataset':'HIVEQ_US_FUT',
                                     'schema':['bars_1m']}],
                      silent=True)
run.wait()                                       # blocks w/ live progress until terminal
print('status:', run.status()['status'])
logs = get_logs(task_id=run.task_id, limit=500)  # for debugging / course-correction
report = run.report()                            # PerformanceReport on success (§10.1)
```
> Incremental mid-run P&L is not streamed today; status transitions are PENDING→RUNNING→DONE, with the report available on completion. Use `get_logs` for in-flight diagnostics.

---

## 12. Enums (exact members and `.value`)

```python
from hiveq.flow.config import EventType, AssetType, DataType, EventLogType, OMSType
from hiveq.flow.trading_types import OrderType, OrderSide, OrderStatus, MarketCenter
```

**EventType** (`.value` == name): `START STOP BAR BAR_1_MIN BAR_5_MIN BAR_15_MIN BAR_30_MIN BAR_1_HOUR BAR_1_DAY TICK TRADE QUOTE SNAP ORDER ORDER_SUBMITTED ORDER_ACCEPTED ORDER_REJECTED ORDER_FILLED ORDER_CANCELED ORDER_DENIED ORDER_EMULATED ORDER_EXPIRED ORDER_INITIALIZED ORDER_PENDING_CANCEL ORDER_PENDING_UPDATE ORDER_UPDATED ORDER_TRIGGERED ORDER_RELEASED ORDER_CANCEL_REJECTED ORDER_MODIFY_REJECTED POSITION POSITION_OPENED POSITION_CHANGED POSITION_CLOSED CUSTOM_DATA TIMER INDEX_PRICE ROLLOVER EXECUTOR_EVENT SECURITY_EVENT`

**AssetType**: `EQUITY OPTIONS FUTURES CRYPTO INDEX`

**OrderType**: `MARKET LIMIT STOP STOP_LIMIT MOO MOC LOO LOC` — `MOO/MOC/LOO/LOC` are auction orders with exchange-specific entry/cancel cutoffs (§5.2.1); `LOO/LOC` require `limit_price`.

**OrderSide**: `BUY SELL`

**OrderStatus**: `PENDING SUBMITTED ACCEPTED REJECTED CANCELED FILLED PARTIALLY_FILLED`

**OrderState** (from `ctx.get_order_state`): `INITIALIZED SUBMITTED ACCEPTED REJECTED FILLED CANCELED DENIED EXPIRED PENDING_CANCEL PENDING_REPLACE`

**DataType**: `BAR BAR_1_MIN BAR_5_MIN BAR_15_MIN BAR_30_MIN BAR_1_HOUR BAR_1_DAY TICK QUOTE`

**EventLogType**: `POSITION ORDER FILL CUSTOM_DATA USER_LOG ENTRY_TRADE EXIT_TRADE PARAM_CHANGE`

**OMSType**: `SIGMA` (`.value == "SIGMA"`)

**MarketCenter**: `NYSE NASDAQ ARCA BATS AMEX CME CBOE NYMEX CBOT`

**time_in_force** (valid strings): `"DAY" "GTC" "IOC" "FOK" "GTX" "GTD" "OPG" "ATC"`

---

## 13. Config dataclasses

```python
from hiveq.flow import StrategyConfig, BacktestConfig, EngineConfig
```

**StrategyConfig**
| field | type | default |
|---|---|---|
| `name` | str | (required) |
| `type` | str | (required — class name, R2) |
| `symbols` | Optional[List[str]] | None |
| `params` | Dict[str, Any] | {} |

**BacktestConfig** (key fields)
| field | type | default |
|---|---|---|
| `id` | Optional[str] | None |
| `symbols` | list | None |
| `start_date` / `end_date` | Optional[str] | None |
| `initial_capital` | float | 1_000_000.0 |
| `commission` | float | 0.001 |
| `slippage` | float | 0.0 |
| `venue` | str | "SIM" |
| `deploy` | bool | False |
| `benchmark` | Optional[str] | None |
| `risk_free_rate` | float | 0.02 |
| `equity_fee` | float | 0.0011 (per share) |
| `futures_fee` | float | 0.5 (per contract) |
| `crypto_fee` | float | 0.00005 |
| `session_start` / `session_end` | Optional[str] | None (ET "HH:MM", R6) |
| `enable_auto_rollover` | bool | False |
| `enable_tca` | bool | False |
| `export_orders_csv` | bool | False |
| `extra_config` | Dict[str, Any] | {} |

**EngineConfig**
| field | type | default |
|---|---|---|
| `oms` | str | "SIGMA" (use `OMSType.SIGMA.value`) |
| `timezone` | Optional[str] | None (IANA name; auto-detected if None) |
| `params` | Dict[str, Any] | {} (engine-behavior keys — see §2.1 for the recognized keys) |

Pass an `EngineConfig` (or a plain `config={...}` dict) to `run_backtest` via `**kwargs`; the tunable `params` keys are listed in **§2.1**.

---

## 14. Imports cheat-sheet

```python
import hiveq.flow as hf
from hiveq.flow import StrategyConfig, BacktestConfig, EngineConfig, Context, get_run
from hiveq.flow.runs import Run
from hiveq.flow.config import EventType, AssetType, DataType, EventLogType, OMSType
from hiveq.flow.trading_types import OrderType, OrderSide, OrderStatus, MarketCenter
from hiveq.flow.trading.price_utils import adjust_tick_size, get_min_tick   # round your own limit/stop prices (§5.2)
from hiveq.flow.utils.date_calendar import TradingCalendar   # trading-day / session helpers (US-only today)
from hiveq.flow.jobs import submit, poll_result, get_status, get_logs, get_logs_gz, get_result, get_client, TaskType
```

---

## 15. Common pitfalls (accurate)

- **Use per-event callbacks** (`on_start`/`on_bar`/`on_order`/…), not a single `on_hiveq_event` — that global form is opt-in only (§4).
- **There is no `on_order_filled` callback** — handle fills in `on_order` and check `order.is_filled`.
- **`run_backtest(...)` returns a `Run`, not a `PerformanceReport`** — call `run.report()` (§10.0). For non-blocking deploy use `run_backtest(..., silent=True)`.
- **No built-in indicators or rolling-history accessor** — maintain your own `collections.deque` windows and compute with `numpy`/`pandas` (§16). `ctx.instrument(symbol).last_bar` is the only built-in "latest price" accessor.
- **Sizing helpers exist** (`ctx.close_position`/`order_to_target`/`flatten_all`, §5.2/§16.1) but **percent-of-equity sizing and native brackets do not** — build stop-loss/take-profit as explicit child orders (§16.4).
- **Match the order mechanism to the need:** a single immediate market order or a signal backtest → direct `buy_order`/`sell_order` (§5.2). Sizeable/sliced orders, live order-chasing/replaces, or auction routing → an **executor** (§5.10/§16.6), which owns slicing/replaces/cancels/fill-aggregation. Don't wrap a one-shot order in an executor; don't hand-roll replace/retry logic when one fits. With executors, hold one handle per target and `replace_executor_params_by_id` to re-target — never `add_executor` a duplicate.
- `event.data()` is untyped — use §7.0 to know the concrete type for the branch you are in.
- `ts_event`/`ts_init` are **nanoseconds (int)**, not seconds. Use `.time`/`.time_utc` for datetime, or `ctx.trading_day` for the date.
- Do not place orders in `EventType.STOP` / `on_stop` — they are rejected (engine already STOPPED).
- `report.*` DataFrames can be `None`/empty — guard before use.
- For futures, subscribe by the continuous symbol string in `symbols=`, e.g. `subscribe_futures_bars(symbols=['ES.c.0'])` (or `subscribe_bars(['ES.c.0'], asset_type=AssetType.FUTURES)`). For continuous rollover set `BacktestConfig(enable_auto_rollover=True)` + handle `on_rollover` — there is no `data_configs` flag for it.
- Set `symbols`/`start_date`/`end_date` in one place (top-level args OR `BacktestConfig`), not both.

---

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

**Reference implementation:** `hiveq.flow.strategies.algo_instruction.AlgoInstructionStrategy` is the canonical executor-driven strategy. It keeps **one executor per (symbol, slot)** where slots are **`entry` / `exit` / `risk`**, is parameterized entirely by algo instructions (e.g. `entry=POV;exit=MOC;risk=TWAP`), checks `executor_state` before (re)starting a slot, and replaces params rather than stacking. Study it when building executor-based strategies. Auction exits (`exit=MOC`/`MOO`) map to the `AUCTION` executor with venue routing (§5.2.1).
