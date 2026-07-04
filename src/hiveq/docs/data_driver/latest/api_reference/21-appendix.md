## Appendix — config-driven section properties

A `data_source_id` section points at a `primary` (and optional `cache`) section;
those sections carry a `transport` and its properties.

**HiveQ — read/subscribe:** `dataset`, `schema` (required for reads), `splitSize`,
`filterMode`, `columns`, `limit`, `timezone` (default `America/New_York`),
`topic` (⇒ subscribe or WebSocket publish), `wsHost`/`wsPort` (default
`localhost:8765`), `replay`/`replayTo`, `forceRefresh`, `keyField`,
`pullDataSourceID`.
**HiveQ — WebSocket publish (real-time, preferred for live):** `topic` (required,
activates WS path), `keyField` (per-row key column), `key` (static fallback),
`wsHost`/`wsPort`.
**HiveQ — SDK publish (batch, preferred for backtest):** `publishSchema` (or
`schema`), `key` (required), `operation` (`add`/`modify`), `async` (`true`/`false`).
**CSV:** `file` (`{sym}`/`{date:%fmt}` templates), `append`, `filterMap`; root
`[default] baseCSVPath`.
**HDF5:** `store`, `key` (templated), `enableCompression`, `compression`,
`compression_level`, `minSizeItems`; root `[default] storeBasePath`.

### Cache modes (`hiveq.Cache`)

`NO_CACHE`, `ONLY_CACHE`, `CACHE_FORCE_PULL`, `PULL_UPDATE_CACHE`, `IN_MEMORY`
(see [Caching](05-caching.md)).

### params_tuple convention

Fields are identified **by type**: first `DateRange` → date window, first
`TimeRange` → intraday window, remaining list/str → symbols. Field **names** drive
`{date}`/`{sym}` templates and `filterMap`. Conventionally `date`, `time`, `sym`.

### `hiveq.dd` keyword facade (HiveQ-specific, NOT transport-agnostic)

For throwaway scripts only. `dd.load(dataset=, schema=, ...)` / `dd.save(schema=,
key=)` bind code directly to the HiveQ API and cannot be flipped to CSV/KDB by
config. Prefer the config-driven section API above.
