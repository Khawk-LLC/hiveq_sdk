## FAQ

**How do I run a range query?** By default `splitSize=1`, so a query runs once
per date. Raise `splitSize` to pull the whole range in fewer requests.

**"Driver not found. Configure transport in config"** — the section is missing
the `transport` attribute. Each transport section needs `transport=HiveQ` (or
`CSV` / `HDF5` / `KDB`).

```ini
[HiveQBars1m]
transport = HiveQ
```

---

