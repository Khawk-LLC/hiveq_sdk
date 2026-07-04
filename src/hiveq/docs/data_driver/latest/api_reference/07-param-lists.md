## Better parameter handling — passing params as a list

```ini
[EqBars]
primary = HiveQBars1m
cache   = EqBarsCSV

[EqBarsCSV]
transport = CSV
file      = csv/bars/{sym}/{date:%Y.%m.%d}.csv

[HiveQBars1m]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = bars_1m
```

```python
import collections
import hiveq.driver as dd
from hiveq import Cache
from hiveq.datetime import DateRange

ParamTuple = collections.namedtuple('Params', ['date', 'sym'])
params = ParamTuple(DateRange('2025-10-13', '2025-10-17'), ['AAPL', 'MSFT', 'NVDA'])

df = dd.load('EqBars', params_tuple=params, cache=Cache.PULL_UPDATE_CACHE)
```

The cache is updated by filtering the dataframe by symbol and then by date.

---

