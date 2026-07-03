## 9. `data_configs` schema  (list of dicts)

### 9.1 `type='hiveq_historical'`

> **Full data catalog — datasets, schemas, coverage caveats — lives in
> [`data-reference.md`](../data-reference.md).** That file is the single source
> of truth for "what data exists"; keep this section limited to the
> `data_configs` dict shape. If a dataset/schema code isn't in
> `data-reference.md`, don't guess — it doesn't exist.

| key | type | notes |
|---|---|---|
| `type` | str | `'hiveq_historical'` |
| `dataset` | str | dataset code — see `data-reference.md` §1 for the full list (`HIVEQ_US_EQ`, `HIVEQ_US_FUT`, `HIVEQ_QUANT_SIGNALS`, …). |
| `schema` | list[str] \| str | one or more exact schema codes — see `data-reference.md` §2 for the full list (`bars_1m`, `eq_trades`, `fut_trades`, `tbbo`, …) and coverage caveats (bar granularities, auction prints, executor tick-stream requirement). |
| `id` | str (opt) | identifier referenced by `ctx.subscribe_data(data_id=...)` for signal/custom sources |
| `enabled` | bool (opt) | default `True` |

```python
# equities 1-minute bars
{'type':'hiveq_historical','dataset':'HIVEQ_US_EQ','schema':['bars_1m']}
# futures (subscribe to a continuous symbol like 'ES.c.0'); for rollover set
# BacktestConfig(enable_auto_rollover=True) — no data_configs flag needed.
{'type':'hiveq_historical','dataset':'HIVEQ_US_FUT','schema':['bars_1m']}
# quant signals (subscribe via ctx.subscribe_data(data_id=...))
{'type':'hiveq_historical','dataset':'HIVEQ_QUANT_SIGNALS','schema':['signals'],
 'id':'mysignals','symbols':['My_Signal_Name']}
```

**`HIVEQ_QUANT_SIGNALS`** delivers platform-hosted signal data to `on_custom_data` — see `data-reference.md` §3 for the `signal_json` payload format.

> **Backtest with your own signals?** Use a CSV custom data source instead (§9.2) — it fires the same `on_custom_data` callback. Put your signal fields as columns in the CSV and read them with `column_data()`. No `signal_json` wrapping needed.

#### `data_type='custom'` + `filters` (query a ClickHouse-backed table directly)

A `hiveq_historical` entry can also route through `HiveQUserDataAdapter`
instead of the default signal path, by adding `data_type: 'custom'` and a
`filters` dict. `filters` is flattened verbatim into `<id>.<key>` config
read by the adapter — every key is optional except `adapter`/`dataset`/
`schema`, and omitting a key keeps that setting at its default (no effect
on existing configs).

| key | type | notes |
|---|---|---|
| `adapter` | str | Must be `'HiveQUserDataAdapter'`. |
| `dataset` | str | Dataset code to query, e.g. `'HIVEQ_QUANT_CLUSTERS'`. |
| `schema` | str | Schema code to query, e.g. `'clusters'`. |
| `symbols` | str, comma-separated | Symbol/identifier filter values, e.g. `'ES.c.0'` or `'ES.c.0,NQ.c.0'`. Opaque by default (signal name, zone tag, etc.) unless a resolution mode below is set. |
| `symbolFilterKey` | str | The request-body field name `symbols` is sent under — dataset-dependent (`'symbol'` for `HIVEQ_QUANT_SIGNALS`, `'sym'` for `HIVEQ_QUANT_CLUSTERS`/`_MODES`). **Not** a list — one field name applies to the whole `symbols` array. |
| `symbolsAreFuturesContracts` | bool | Legacy flag. `True`/`'true'` resolves `symbols` as `<ROOT>.c.<N>` continuous contracts (dated contract + Nanex translation, re-resolved per backtest day). Superseded by `symbolResolutionMode` below when both are set. |
| `symbolResolutionMode` | str | `'continuous_contract'` (same as `symbolsAreFuturesContracts: True`), `'root_symbol'` (strip the `.c.<N>` suffix, use the bare root, no per-date lookup or Nanex translation), or `'option'` (reserved, not yet implemented — logs a warning and falls back to unresolved symbols). Unset defaults to `continuous_contract`/`none` based on `symbolsAreFuturesContracts`. |
| `timestampColumn` | str | Overrides the row column used as the event timestamp. Default (unset) guesses from `ts_event` > `db_event` > `time`, in that preference order. Set this when a table's timestamp column doesn't match any of those three names. |
| `extraFilters` | str, comma-separated `col=val` pairs | Arbitrary hardcoded column=value literal filters merged into the query alongside `symbols`, e.g. `'exchange=CME,session=RTH'`. No built-in equivalent before this key — use it for any column/value constraint beyond symbol and date range. |

```python
{'type': 'hiveq_historical', 'dataset': 'HIVEQ_QUANT_CLUSTERS', 'data_type': 'custom',
 'id': 'es_mm_zones', 'filters': {
     'adapter': 'HiveQUserDataAdapter',
     'dataset': 'HIVEQ_QUANT_CLUSTERS',
     'schema': 'clusters',
     'symbols': 'ES.c.0',
     'symbolFilterKey': 'sym',
     'symbolResolutionMode': 'continuous_contract',
     'timestampColumn': 'time',
     'extraFilters': 'tag=ON',
 }}
```

### 9.2 `type='csv'`
| key | type | notes |
|---|---|---|
| `type` | str | `'csv'` |
| `data_type` | str | fill-mode hint for your own file: a `bars_*` value (OHLCV → bar fills), `'tbbo'`/`'trades'` (tick fills), or `'custom'` (user/signal data). For CSV the granularity is whatever your file contains. |
| `path` | str | path to CSV (relative or absolute) |
| `id` | str | identifier referenced in strategy subscriptions |
| `enabled` | bool (opt) | default `True` |

```python
{'type':'csv','data_type':'bars_1m','id':'1_MIN_BAR','path':'bars/AAPL_bars.csv'}
{'type':'csv','data_type':'custom','id':'UserData','path':'userdata/signals.csv'}
```
CSV bar columns: `timestamp,symbol,open,high,low,close,volume`.

**CSV custom/signal columns** (`data_type='custom'`):

The engine requires **three mandatory columns** (matched by header name, case-insensitive, any position):

| Column | Required | Format | Notes |
|---|---|---|---|
| `date` | **yes** | `YYYY-MM-DD` | Combined with `time` to determine when the row fires during backtest |
| `time` | **yes** | `HH:MM:SS` | Combined with `date` for the event timestamp (`ts_event`) |
| `sym` | **yes** | string | Symbol identifier; available as `data.symbol` and `data.column_data("sym")` |
| *(any other)* | no | string | User-defined columns; read with `data.column_data("col_name")` |

> **All three columns are mandatory.** The engine raises an error if any of `date`, `time`, or `sym` is missing from the CSV header. Column order does not matter — the engine locates them by name.

All column values arrive as strings in the strategy. Cast to the appropriate type in your code:
```python
zone_prob = float(data.column_data("zone_prob", default="0"))
enabled = data.column_data("gate", default="false").lower() == "true"
```

> **Note:** Values containing commas must use `|` (pipe) as a separator instead, since the engine uses simple comma-splitting (not RFC 4180 quoted CSV). Decode pipes back to commas in strategy code if needed.

#### Daily file pattern (`_yyyymmdd.csv`)

When the CSV path ends with **`_yyyymmdd.csv`**, the engine treats it as a date pattern and automatically resolves the file for each backtest day by replacing `yyyymmdd` with the date in `YYYYMMDD` format. This lets you organize signal data into one file per day:

```
signals/kx_signals_20250602.csv
signals/kx_signals_20250603.csv
signals/kx_signals_20250604.csv
...
```

Reference the pattern (not an individual file) in `data_configs`:
```python
{'type':'csv','data_type':'custom','id':'my_signals','path':'signals/kx_signals_yyyymmdd.csv'}
```

On each backtest day, the engine opens the matching file (e.g. `signals/kx_signals_20250602.csv` on June 2). If the file for a given day doesn't exist, no custom data events fire for that day. Each daily file must have the same header row with the three mandatory columns (`date`, `time`, `sym`).

Upload all daily files to the platform before running:
```bash
hiveq-data -u signals/                         # uploads all files in the directory
```

#### End-to-end: using a CSV signal file in a backtest

Strategies run on the HiveQ platform, not on your local machine. CSV data files must be uploaded to your persistent-data store **before** submitting the backtest. The `path` in `data_configs` must match the uploaded path exactly — the platform executor resolves it against your store at runtime.

1. **Create the CSV** with the three mandatory columns (`date`, `time`, `sym`) followed by your signal columns:
   ```
   date,time,sym,zone_prob,drift_price,iv_quintile_ewm,quote_gate_enabled
   2025-06-02,14:00:00,ES.c.0,0.65,5960.25,4.0,true
   2025-06-02,14:01:00,ES.c.0,0.63,5959.50,4.0,true
   ```

2. **Upload to the platform** — the file must exist in your persistent-data store before the strategy runs:
   ```bash
   hiveq-data -u signals/my_signals.csv
   ```
   This stores the file as `signals/my_signals.csv` on the platform.

3. **Wire in `data_configs`** — the `path` must match the uploaded path exactly, and the `id` must match the `subscribe_data` call:
   ```python
   data_configs=[
       {'type':'hiveq_historical','dataset':'HIVEQ_US_FUT','schema':['bars_1m']},
       {'type':'csv','data_type':'custom','id':'my_signals','path':'signals/my_signals.csv'},
   ]
   ```

4. **Subscribe in `on_start`**:
   ```python
   ctx.subscribe_data(data_id='my_signals')
   ```

5. **Read in `on_custom_data`** — each CSV row fires as a `SigmaCustomData` event at the time specified by its `date` + `time` columns:
   ```python
   def on_custom_data(self, ctx, event):
       data = event.data()
       zone_prob = float(data.column_data("zone_prob", default="0"))
       drift = float(data.column_data("drift_price", default="0"))
       # ... use in strategy logic
   ```

#### `hiveq-data` — managing data files on the platform

All strategies run on the HiveQ platform, not on your local machine. CSV files referenced in `data_configs` must be uploaded to your per-user persistent-data store **before** submitting the backtest. The platform executor resolves `path` against the store at runtime — the path you upload with is the path you must use in `data_configs`.

**Runtime store location:** `/home/hivequser/hiveq/persistent_data/`

A file uploaded as `signals/my_signals.csv` is available at runtime as `/home/hivequser/hiveq/persistent_data/signals/my_signals.csv`. Reference it in `data_configs` by its **relative path** (`signals/my_signals.csv`), not the full runtime path.

```bash
# Upload (prerequisite — must complete before running the strategy)
hiveq-data -u signals/my_signals.csv           # upload a single file
hiveq-data -u signals/                         # upload a whole directory (recursive)
hiveq-data -u signals/ --force                 # re-send everything (skip MD5 check)
hiveq-data -u signals/ --dry-run               # preview what would be sent

# Verify what's on the platform
hiveq-data -l                                  # list everything in your store
hiveq-data -l signals                          # list a subdirectory

# Remove files
hiveq-data --rm signals/old.csv                # a single file
hiveq-data --rm signals                        # a whole subdirectory
```

Uploads are **incremental** (rsync-like) — a file is sent only if it's new or its content changed, compared by MD5 against the server's listing. Requires `HIVEQ_API_KEY` in your environment or `~/.hiveq/.env`.

> **Path anchoring:** `hiveq-data -u` preserves the directory structure relative to the argument. Uploading `signals/my_signals.csv` stores it as `signals/my_signals.csv`. Uploading just `my_signals.csv` stores it at the root as `my_signals.csv`. Always verify the result with `hiveq-data -l` and match the stored path exactly in `data_configs`.

> **Do not use absolute or `Path(__file__)`-based paths** in `data_configs` — they resolve on your local machine but not on the platform. Always use relative paths that match the uploaded location.

**Example workflow:**
```bash
# 1. Upload
hiveq-data -u signals/kx_signals.csv

# 2. Verify
hiveq-data -l signals
#   signals/kx_signals.csv    28.2 KB  b9e0947a76f1  2026-06-20T12:21:27Z
```
```python
# 3. Reference in data_configs (path matches uploaded path exactly)
{'type':'csv','data_type':'custom','id':'my_signals','path':'signals/kx_signals.csv'}
```

### 9.3 Behavior derived from schema/dataset
See `data-reference.md` §4 (fill mode, futures session defaults, options snapshot handling).

---

