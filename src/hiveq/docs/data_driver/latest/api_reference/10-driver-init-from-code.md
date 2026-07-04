## Driver init support from code

The API supports passing configuration from code instead of `dd-config.ini`.
Auth is via `HIVEQ_API_KEY` env (no user/password in code), so init is used for
config selection and storage paths.

```python
import os, collections
import hiveq.driver as dd
from hiveq import Cache
from hiveq.datetime import DateRange

A = collections.namedtuple('A', ['date', 'sym'])
a = A(DateRange('2025-10-13', '2025-10-17'), ['AAPL', 'MSFT'])

dd.init(config='dd-config.json', storeBasePath=os.getcwd() + '/dd/')
df = dd.load('EqBars', params_tuple=a, cache=Cache.NO_CACHE)
```

`config` accepts an inline dict, a `.py` file (defining `CONFIG`/`config`), a
`.json` file, or a legacy `.ini` path.

---

