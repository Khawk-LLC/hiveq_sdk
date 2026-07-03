## Date Range

`DateRange` supports a transform function to process/remove dates (e.g. drop
weekends/holidays):

```python
import datetime
import pandas as pd
from hiveq.datetime import DateRange

def transform_date_list(date_list):
    start_date = datetime.datetime.strptime(date_list[0], '%Y.%m.%d')
    end_date = datetime.datetime.strptime(date_list[-1], '%Y.%m.%d')
    dd_dt = pd.date_range(start_date, end_date - datetime.timedelta(days=1), freq='B')
    return dd_dt.strftime('%Y.%m.%d').values

d = DateRange('2025-10-01', '2025-10-31', transform_date_list)
print(d.date_list)
```

---

