## Python code to load and save the data

```python
import collections
import hiveq.driver as dd
from hiveq import Cache
from hiveq.datetime import DateRange

ParamTuple = collections.namedtuple('Params', ['date', 'sym'])
params = ParamTuple(DateRange('2025-10-14', '2025-10-14'), ['AAPL'])

df = dd.load('AaplBars', params_tuple=params, cache=Cache.PULL_UPDATE_CACHE)
```

The data is now available in `csv/bars/AAPL/2025.10.14.csv` (under the configured
`baseCSVPath`).

### Filtering for the cache

Ensure that the dataframe column names match the names passed in as the named
tuple. The cache driver uses those column names to filter and store data.

---

