## General Guidelines

1. When building a cache for the first time, use `CACHE_FORCE_PULL` (multi-symbol
   / multi-day queries are batched and faster) with an appropriate `splitSize`.
2. Once the cache exists and new symbols are added, use `PULL_UPDATE_CACHE` (it
   pulls per missing symbol+date).
3. To bulk-add many new symbols, use `CACHE_FORCE_PULL`.

---

