# HiveQ Data Driver (index)

The HiveQ data driver Python module standardizes data access and provides a
transport-agnostic API. Your code names a `data_source_id`, and config decides
the backend — load data from HiveQ, CSV, HDF5 (or legacy kdb+) by changing config
files instead of writing any code. KDB is retained (`transport=KDB`) for kdb+
users; see [Migrating from KDB](20-migrating-from-kdb.md).

> For the underlying **HiveQ Data API SDK** (`hiveq_data`) that the `transport=HiveQ`
> path uses to pull data — `configure` + `Historical.get_data` params, datasets/schemas,
> filter modes, pagination, and the config→SDK mapping — see
> [`../hiveq_data_api_reference.md`](../hiveq_data_api_reference.md) (a separate, complementary doc — not part of this split).

**HiveQ reference data** (real datasets/schemas; the examples below use a subset)

| | Values |
|---|---|
| Datasets / schemas | `HIVEQ_US_EQ` → `bars_1m`, `bars_1d`, `eq_trades`, `early_imbalance`; `HIVEQ_US_IND` → `indices_1m` |
| Symbols            | `AAPL`, `MSFT`, `NVDA` (equities); `ES1!` → `ES.c.0` (continuous future) |
| Session            | `2025-10-13` … `2025-10-17`, `09:30:00`–`16:00:00` US/Eastern |
| Live topics        | `market_data.equity.tbbo`, `signals.khawk.quant_features` |

**How to read this**: this reference is split by topic, one file per section below —
open the index, pick the section(s) relevant to your question, and read only those.
Don't load the whole tree for one question.

| File | What's in it |
|---|---|
| [01-installation.md](01-installation.md) | `pip install`, extras (`[hiveq]`, `[kdb]`), pandas prerequisite. |
| [02-configuration.md](02-configuration.md) | Config shape, how to supply it (inline dict/JSON/py/ini, auto-discovered `dd-config.ini`), the `[HiveQ]` credentials section. |
| [03-storing-data.md](03-storing-data.md) | `dd.save(id, df)` — write to a data source's configured target. |
| [04-loading-data.md](04-loading-data.md) | `dd.load(id, params_tuple=..., cache=...)` — the core read call. |
| [05-caching.md](05-caching.md) | The five `Cache` modes: `NO_CACHE`, `ONLY_CACHE`, `CACHE_FORCE_PULL`, `PULL_UPDATE_CACHE`, `IN_MEMORY`. |
| [06-python-load-save.md](06-python-load-save.md) | Minimal load→CSV round trip; column-name matching for cache filtering. |
| [07-param-lists.md](07-param-lists.md) | Passing multiple symbols as a list in `params_tuple`. |
| [08-subscription-support.md](08-subscription-support.md) | Live HiveQ subscriptions via `dd.load(...)`: `pullDataSourceID` stitching, `time_out`, `forceRefresh`, dynamic `keyField` subscription, filtering. Largest section. |
| [09-saving-output-publishing.md](09-saving-output-publishing.md) | The two publish paths — WebSocket (real-time/live) vs SDK/REST (batch/backtest) — plus CSV/HDF5 output. |
| [10-driver-init-from-code.md](10-driver-init-from-code.md) | `dd.init(config=..., storeBasePath=...)` — programmatic config instead of `dd-config.ini`. |
| [11-alerts.md](11-alerts.md) | `dd.alert(...)`. |
| [12-examples.md](12-examples.md) | Worked pull/save examples (splitSize, cache modes, init, saving). |
| [13-hdf5-driver.md](13-hdf5-driver.md) | HDF5 transport compression options. |
| [14-csv-driver.md](14-csv-driver.md) | CSV transport `baseCSVPath`. |
| [15-date-range.md](15-date-range.md) | `DateRange` transform functions (e.g. drop weekends/holidays). |
| [16-workflow-examples.md](16-workflow-examples.md) | Mode-agnostic code, batching vs multiple calls, subscription+pull combos, timeouts, dedup, in-memory. |
| [17-symbols.md](17-symbols.md) | Continuous-futures symbol translation (`ES1!` → `ES.c.0`). |
| [18-general-guidelines.md](18-general-guidelines.md) | When to use which cache mode for first-build vs incremental vs bulk-add. |
| [19-faq.md](19-faq.md) | Range queries, "Driver not found" error. |
| [20-migrating-from-kdb.md](20-migrating-from-kdb.md) | KDB→HiveQ config mapping table, `qpython3` install notes. |
| [21-appendix.md](21-appendix.md) | Full config-driven section property reference, cache modes, `params_tuple` convention, the `hiveq.dd` keyword facade. |
