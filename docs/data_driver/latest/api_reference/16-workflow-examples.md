## Workflow Examples

Common setup shared by "Multiple calls..." and "Subscription and pulls" below:

```python
import collections
import hiveq.driver as dd
from hiveq import Cache
from hiveq.datetime import DateRange

Params = collections.namedtuple('Params', ['date', 'sym'])
```

### Writing mode-agnostic code

"Mode" is how you run a historical backtest vs. production. For backtest, data is
already in a local cache and you filter by time/symbol; in production the config
points at HiveQ. Write the code once; switch only the config.

```python
import hiveq.driver as dd
if mode == 'prod':
    dd.init(config='dd-config-prod.ini')
else:
    dd.init(config='dd-config-bt.ini')
```

### Multiple calls vs. a single call and filtering

Always prefer a single call that batches symbols/filters in the params tuple,
then filter the returned dataframe — rather than many calls:

```python
params = Params(DateRange('2025-10-14', '2025-10-14'), ['AAPL', 'MSFT', 'NVDA'])
data = dd.load('EqBars', params_tuple=params, cache=Cache.PULL_UPDATE_CACHE)

aapl = data[data['sym'] == 'AAPL']
msft = data[data['sym'] == 'MSFT']
nvda = data[data['sym'] == 'NVDA']
```

```ini
[EqBars]
primary = HiveQBars1m

[HiveQBars1m]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = bars_1m
```

### Subscription and pulls

In production you often want startup (historical) data then realtime. Instead of
exposing a separate subscriber API, configure the live source and keep using
`dd.load(...)`. The HiveQ transport internally combines the initial pull with
the live subscription and stitches them (via `pullDataSourceID`):

```ini
[tickData]
primary = HiveQTradesSub

[HiveQTradesSub]
transport        = HiveQ
topic            = market_data.equity.tbbo
keyField         = sym
pullDataSourceID = tickStartUp

[tickStartUp]
primary = HiveQTrades

[HiveQTrades]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = eq_trades
```

```python
params = Params(DateRange('2025-10-14', '2025-10-14'), ['AAPL'])
ddat = dd.load('tickData', params_tuple=params, cache=Cache.PULL_UPDATE_CACHE)
```

> The historical pull seeds the buffer and is used throughout the lifetime of the
> subscription; ensure the pull source returns columns matching the live topic.

### Timeouts

```python
dd.load('TbboSub', params_tuple=params, time_out=10000)
```

1. Timeout values are in milliseconds.
2. If data returns within 1 ms, `dd.load()` returns the pull-query data.
3. If it takes more than 10000 ms, `dd.load` returns an empty dataframe.
4. The timeout is ignored on all subsequent calls.

### Dropping duplicates

While stitching pull + subscription data, duplicate rows can appear. Supply the
columns to dedupe on:

```python
df = dd.load('TbboSub', params_tuple=params, cache=Cache.NO_CACHE,
             drop_duplicates=['sym', 'seqno'])
```

### In-memory support

To avoid re-reading the cache on every `dd.load()`, keep the frame in memory:

```python
bars_df = dd.load('EqBars', params_tuple=params, cache=Cache.NO_CACHE, in_memory=True)
```

---

