## Storing data

Call the driver's `save` with a data source id. The id is looked up in config
and the frame is written to its configured target(s) — its `primary` and/or
`cache` section. Point those at a file transport to store the frame to disk.

```python
import hiveq.driver as dd
dd.save('UserData', df)   # written to UserData's configured target
```

Config entries:

```ini
[UserData]
cache = UserDataCSV

[UserDataCSV]
transport = CSV
file      = csv/userdata/study.csv
```

---

