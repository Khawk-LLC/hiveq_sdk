## Loading Data

The driver loads data in real time from a source, or from a cache. A HiveQ pull
needs a date (and symbol) window, so pass a `params_tuple` (see the next
section); the cache file template uses the same fields.

```python
import collections
from hiveq import Cache
from hiveq.datetime import DateRange
import hiveq.driver as dd

Params = collections.namedtuple('Params', ['date', 'sym'])
params = Params(DateRange('2025-10-14', '2025-10-14'), ['AAPL'])

df = dd.load('AaplBars', params_tuple=params, cache=Cache.PULL_UPDATE_CACHE)
```

Config for the `AaplBars` data source:

```ini
[AaplBars]
primary = HiveQBars1m
cache   = AaplBarsCSV

[HiveQBars1m]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = bars_1m

[AaplBarsCSV]
transport = CSV
file      = csv/bars/{sym}/{date:%Y.%m.%d}.csv
```

---

