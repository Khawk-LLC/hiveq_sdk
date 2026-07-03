## 14. Imports cheat-sheet

```python
# Core SDK (strategy authoring, deploy, observe) — always prefer this
import hiveq.flow as hf
from hiveq.flow import StrategyConfig, BacktestConfig, EngineConfig, Context, get_run
from hiveq.flow.runs import Run
from hiveq.flow.config import EventType, AssetType, DataType, EventLogType, OMSType
from hiveq.flow.trading_types import OrderType, OrderSide, OrderStatus, MarketCenter
from hiveq.flow.trading.price_utils import adjust_tick_size, get_min_tick   # round your own limit/stop prices (§5.2)
from hiveq.flow.utils.date_calendar import TradingCalendar   # trading-day / session helpers (US-only today)
from hiveq.flow.jobs import submit, poll_result, get_status, get_logs, get_logs_gz, get_result, get_client, TaskType

# Ancillary: data facade (§14.1 — only when clearly the right tool or user-requested)
from hiveq import dd                           # keyword-style data load/save/subscribe
from hiveq.dd import DateRange, TimeRange      # date/time range helpers for dd.load()
from hiveq.symbol import translate             # old-world ticker translation (ES1! → ES.c.0)
from hiveq.driver.data_driver_interface import Cache  # cache enum for legacy load()

# Ancillary: data API SDK (§14.1 — only when clearly the right tool or user-requested)
import hiveq_data as hd
from hiveq_data import Historical, Publisher, LiveStream, InstrumentReference, Metadata, HiveQAPIError
```

---

## 14.1 Ancillary data packages (shipped as stubs with the SDK)

> **When to use:** Only when it is *clearly* the right tool for the job, or when the user **explicitly asks** to use these packages. The default for strategy authoring is always the core `hiveq.flow` API above. These are helper packages for direct data access and signal generation outside the engine's event loop.

The SDK ships import stubs for two ancillary packages so user code that references them can be deployed from a local machine without breaking. **The stubs make imports resolve — they do not execute.** The real implementations run on the HiveQ platform executor.

### `hiveq.dd` — Keyword-style data facade (from `data_driver`)

A convenience layer for loading historical data, reading/writing files, and publishing signals — used in research notebooks, data pipelines, and pre/post-trade scripts. **Not for use inside strategy callbacks** (use `ctx.subscribe_*` there).

```python
from hiveq import dd
from hiveq.dd import DateRange, TimeRange

# Historical pull (HiveQ Data API)
df = dd.load(dataset='HIVEQ_US_EQ', schema='bars_1s', symbols=['AAPL'],
             date=DateRange('2025-08-01', '2025-08-05'))

# File read/write
df = dd.load(path='data/{sym}.csv', symbols=['AAPL'])
dd.save(df, path='output.csv')

# Publish a signal
dd.save(df, schema='quant_features', key='my_signal')

# Live subscription (distributor)
dd.load(topic='market_data.equity.tbbo', keys=['AAPL'], mode='subscribe')
```

| Function | Purpose |
|---|---|
| `dd.load(dataset, schema, symbols, date, ...)` | Historical data pull |
| `dd.load(path, ...)` | CSV/HDF5 file read |
| `dd.load(topic, keys, ...)` | Live subscription |
| `dd.save(df, path, ...)` | File write |
| `dd.save(df, schema, key, ...)` | Signal publish |
| `dd.stop()` | Stop all live subscriptions |
| `DateRange(start, end)` | Calendar-day range with `.date_list`, `.chunks(n)` |
| `TimeRange(start, end)` | Intraday clock window |

**Other `hiveq` namespace modules from data_driver (stub-only):**

```python
from hiveq.driver.data_driver_interface import Cache  # NO_CACHE, ONLY_CACHE, CACHE_FORCE_PULL, PULL_UPDATE_CACHE, IN_MEMORY
from hiveq.symbol import translate              # old-world ticker translation (ES1! → ES.c.0)
from hiveq.datetime import DateRange, TimeRange # older alias (prefer hiveq.dd.DateRange)
```

### `hiveq_data` — HiveQ Data API SDK

The low-level REST/WebSocket SDK for programmatic data access — datasets, schemas, historical queries, live streaming, and publishing. Used in data pipelines, signal scripts, and integration code. **Not for use inside strategy callbacks.**

```python
import hiveq_data as hd

hd.configure(api_key="...", base_url="https://staging.hiveq.ai")

# Historical
client = hd.Historical(timezone='America/New_York')
data = client.get_data(
    dataset='HIVEQ_US_EQ', schema='bars_1s',
    symbols=['AAPL'], start='2025-08-01', end='2025-08-05',
)

# Instrument reference
ref = hd.InstrumentReference()
futures = ref.get_futures(root=['ES', 'NQ'])

# Publishing
pub = hd.Publisher()
pub.publish(schema='quant_features', data=[{'sym': 'AAPL', 'score': 0.85}], key='my_signal')

# Live streaming (async)
stream = hd.LiveStream()
await stream.connect()
await stream.subscribe(topic='market_data.equity.tbbo', key='AAPL', callback=on_tick)

# Metadata
meta = hd.Metadata()
datasets = meta.get_datasets()
schemas = meta.get_schemas(dataset='HIVEQ_US_EQ')
```

| Class / Function | Purpose |
|---|---|
| `hd.configure(api_key, base_url, ...)` | Set API credentials (once, module-level) |
| `hd.Historical(timezone=...)` | Historical data client |
| `.get_data(dataset, schema, symbols, root, chains, start, end, limit, filter_mode, ...)` | Fetch historical data |
| `hd.Publisher(async_mode=...)` | REST publisher |
| `.publish(schema, data, key, operation)` | Publish records |
| `hd.LiveStream(...)` | WebSocket live streaming client |
| `hd.InstrumentReference(...)` | Instrument reference lookups (futures, options, equities, indices) |
| `hd.Metadata(...)` | Dataset/schema discovery |
| `hd.HiveQAPIError` | Exception for API failures (has `.status_code`, `.response_json`) |

**When `hiveq.dd` vs `hiveq_data`:** `hiveq.dd` is the high-level convenience layer (one-liner loads, file I/O, symbol translation). `hiveq_data` is the lower-level SDK (explicit client construction, pagination, streaming, instrument reference). Use `dd` for quick data pulls; use `hiveq_data` when you need fine-grained control, the instrument reference API, or async live streaming.

---

