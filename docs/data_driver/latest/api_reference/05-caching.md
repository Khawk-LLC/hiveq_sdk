## Caching

Caching data from the primary source and reading it back from the cache enables
faster access. There are multiple cache options:

1. **NO_CACHE** – loads data directly from the source.
2. **ONLY_CACHE** – loads data only from the cache.
3. **CACHE_FORCE_PULL** – loads from source and forcibly updates the cache.
4. **PULL_UPDATE_CACHE** – loads from the cache, but pulls from the source for
   anything not found in the cache (and updates it).
5. **IN_MEMORY** – keeps the loaded frame in process memory.

These ensure that while real-time data is loaded from the primary source, the
cache is automatically kept up to date.

```ini
# AAPL 1-minute bars
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

