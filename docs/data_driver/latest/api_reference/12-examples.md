## Examples

Common setup shared by the examples below (shown once — each example continues from this):

```python
import collections
import hiveq.driver as dd
from hiveq import Cache
from hiveq.datetime import DateRange

A = collections.namedtuple('A', ['date', 'sym'])
```

### Pulling data from the primary source

```ini
[Trades]
primary = HiveQTrades
cache   = cache_trades

[HiveQTrades]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = eq_trades

[cache_trades]
transport = CSV
file      = csv/trades/{sym}/{date:%Y.%m.%d}.csv
```

```python
a = A(DateRange('2025-10-14', '2025-10-14'), 'AAPL')
df = dd.load('Trades', params_tuple=a, cache=Cache.NO_CACHE)
```

### SplitSize DateRange example

By default `splitSize=1` (one request per date). To pull a whole range in larger
chunks, raise it:

```ini
[HiveQTrades]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = eq_trades
splitSize = 1000
```

```python
import datetime
dt1 = datetime.datetime(2025, 10, 13)
dt2 = datetime.datetime(2025, 10, 17)
a = A(DateRange(dt1, dt2), ['AAPL'])
df = dd.load('Trades', params_tuple=a, cache=Cache.NO_CACHE)
```

> `splitSize` chunks a `DateRange` into N-day requests that are concatenated.

### Date range input as string

```python
a = A(DateRange('2025-10-13', '2025-10-17'), ['AAPL'])
df = dd.load('Trades', params_tuple=a, cache=Cache.NO_CACHE)
```

### Cache force pull — always pull from source and update cache

```python
a = A(DateRange('2025-10-14', '2025-10-14'), ['AAPL'])
df = dd.load('Trades', params_tuple=a, cache=Cache.CACHE_FORCE_PULL)
```

### More input parameters

```python
A = collections.namedtuple('Input', ['date', 'sym', 'stime', 'etime'])   # rebinds A to a 4-field shape
a = A(DateRange('2025-10-14', '2025-10-15'), 'AAPL',
      TimeRange('09:30:00', '16:00:00'), None)
dd.load('CombinedScoreTime', params_tuple=a, cache=Cache.PULL_UPDATE_CACHE)
df = dd.load('CombinedScoreTime', params_tuple=a, cache=Cache.ONLY_CACHE)
```

```ini
[CombinedScoreTime]
primary = HiveQBars1m
cache   = cache_combinedScore

[HiveQBars1m]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = bars_1m

[cache_combinedScore]
transport = CSV
file      = csv/score/{sym}/{date:%Y.%m.%d}.csv
```

### Init function — pass initialization values

```python
import os
a = A(DateRange('2025-10-13', '2025-10-17'), ['AAPL', 'MSFT'])
dd.init(storeBasePath=os.getcwd() + '/dd/')
df = dd.load('Trades', params_tuple=a, cache=Cache.PULL_UPDATE_CACHE)
```

### Saving data

```python
date_range_param = A(DateRange('2025-10-13', '2025-10-17'), 'AAPL')

df = dd.load('DailyBars', params_tuple=date_range_param, cache=Cache.NO_CACHE)
df = dd.save('TestSave', df, params_tuple=date_range_param)
df = dd.load('TestSave', params_tuple=date_range_param, cache=Cache.ONLY_CACHE)
```

```ini
[TestSave]
primary = HiveQBars1d
cache   = CSVSave

[CSVSave]
transport = CSV
file      = csv/save/{sym}/csv_save-{date:%Y.%m.%d}.csv
```

---
