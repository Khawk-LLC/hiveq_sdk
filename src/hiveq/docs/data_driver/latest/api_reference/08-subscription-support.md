## Subscription support

The data driver supports asynchronous HiveQ subscriptions. Data is pushed from
the HiveQ distributor to the driver asynchronously. Use `dd.load()` to retrieve
it. A live subscription is still a normal data-driver load: configure a
`data_source_id`, pass symbols through `params_tuple`, and call `dd.load(...)`.
Subscriber classes such as `HiveQSubscriber` are internal to the transport layer
and should not be instantiated or exposed by user code. All subscription data is
held in memory and lost on shutdown.

Config for a subscription (a section whose transport has a `topic`):

```ini
[TbboSub]
primary = HiveQTbboSub

[HiveQTbboSub]
transport = HiveQ
topic     = market_data.equity.tbbo
keyField  = sym
wsHost    = localhost
wsPort    = 8765
```

To retrieve data, use the same `dd.load(...)` construct used for pulls:

```python
import collections, time
import hiveq.driver as dd
from hiveq import Cache
from hiveq.datetime import DateRange, TimeRange

dd_params = collections.namedtuple('Input', ['sym', 'date', 'time'])
params = dd_params(['AAPL'],
                   DateRange('2025-10-14', '2025-10-14'),
                   TimeRange('09:30:00', '16:00:00'))

df = dd.load('TbboSub', params_tuple=params)
for i in range(10):
    time.sleep(10)
    df = dd.load('TbboSub', params_tuple=params)

# stop() lets the driver stop its internal subscription threads; otherwise the
# process won't exit.
dd.stop()
```

### Subscription disconnects and support for pull query

The driver supports **pre-loading** historical data and **stitching** it under
the real-time subscription stream. If the connection drops, the subscribe is
retried (auto-reconnect + re-subscribe).

To pre-load, set **`pullDataSourceID`** to the data source id whose history
should seed the buffer.

```ini
[TbboSub]
primary = HiveQTbboSub

[HiveQTbboSub]
transport        = HiveQ
topic            = market_data.equity.tbbo
keyField         = sym
pullDataSourceID = Trades       ; history seed source (any transport)

[Trades]
primary = HiveQTrades
cache   = cache_trades

[HiveQTrades]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = eq_trades
```

> The HiveQ WebSocket handles keepalive and reconnect internally, so the old KDB
> heartbeat query/table settings are not needed.
>
> **Note:** with `pullDataSourceID`, historic pulls and live ticks may arrive
> unsorted. Ordering is not guaranteed by the driver — dedupe/sort yourself.

### Timeout

When `dd.load` is invoked for a subscription with `pullDataSourceID`, you can
specify a **`time_out`** (milliseconds). It makes the call blocking until the
timeout elapses or data is available.

```python
df = dd.load('TbboSub', params_tuple=params, cache=Cache.CACHE_FORCE_PULL)
# should load from cache
df = dd.load('TbboSub', params_tuple=params, cache=Cache.PULL_UPDATE_CACHE, time_out=10000)
```

If the pull returns empty before the timeout, `dd.load` returns an empty frame.

### ForceRefresh

By default all subscription data is cached in memory. For high-volume topics
(e.g. trades/tbbo) this can grow over time. Enable **`forceRefresh`** in the
subscription config to retain only data not yet pulled by the user.

```ini
[HiveQTbboSub]
transport    = HiveQ
topic        = market_data.equity.tbbo
forceRefresh = true
```

With `forceRefresh=true`, if `dd.load()` is called at 09:00 and the next pull is
at 09:30, only the 09:00–09:30 data is kept; once pulled, the buffer is cleared
and new data accumulates from 09:30 onward. Think of it as a moving window of
unread data.

### Dynamic Subscription Support

Add **`keyField`** — the params tuple field that holds the symbols looked for as
new data. New symbols passed on a subsequent `dd.load()` are subscribed live and
their history is stitched in.

```ini
[TbboSub]
primary = HiveQTbboSub

[HiveQTbboSub]
transport        = HiveQ
topic            = market_data.equity.tbbo
keyField         = sym
pullDataSourceID = Trades
forceRefresh     = true
```

```python
# Start with AAPL, then add MSFT mid-session — both stream on the same subscription.
df = dd.load('TbboSub', params_tuple=dd_params(['AAPL'], date, time), time_out=3000)
df = dd.load('TbboSub', params_tuple=dd_params(['AAPL', 'MSFT'], date, time))
```

> The HiveQ transport layer subscribes the new keys directly on the live socket;
> user code still only calls `dd.load(...)`. No separate "dynamic query" is
> needed (unlike the legacy KDB design).

### Filtering

For subscription filtering, the dataframe columns must match the parameter tuple
field names. Map filters in the config (`filterMap`) to map a param tuple name to
the actual column name.

For time-based filtering, the param tuple field must be a **`TimeRange`**. Time
values passed as plain strings are used for equality filtering, not range
filtering.

```python
import collections
from hiveq.datetime import DateRange, TimeRange
import hiveq.driver as dd
from hiveq import Cache

dd_input = collections.namedtuple('Input', ['date', 'sym', 'time'])
dd_param = dd_input(DateRange('2025-10-14', '2025-10-14'), ['AAPL'],
                    TimeRange('10:00:00', '12:00:00'))
df = dd.load('TbboSub', params_tuple=dd_param, cache=Cache.PULL_UPDATE_CACHE)
```

---

