# HiveQ Data SDK — reference for the Data Driver (pull path)

This documents the part of the **`hiveq_data`** SDK (dist `hiveq-data`; repo
<https://github.com/Khawk-LLC/hiveq-data-sdk>) that the data driver depends on to
**pull market data**. The driver never calls the SDK directly in user code — the
`transport=HiveQ` sections route through `hiveq/driver/hiveq_transport.py`, which
calls the SDK for you. This file exists so an AI agent (or a human) extending the
HiveQ transport knows the exact SDK surface, parameters, and config mapping.

SDK version referenced: **0.2.4**. Endpoints are version `v0`.

---

## 1. The two SDK calls the driver makes to read data

```python
import hiveq_data

# (a) one-time credential/endpoint setup
hiveq_data.configure(api_key=..., base_url=...)

# (b) the actual historical pull
client = hiveq_data.Historical(timezone='America/New_York')
resp = client.get_data(dataset=..., schema=..., symbols=[...],
                       start=..., end=..., limit=..., offset=..., filter_mode=...)
```

`get_data` issues `POST /api/read/v0/data` and returns a **JSON dict**:

```json
{ "data": [ { "<col>": <val>, ... }, ... ],
  "meta": { "requestId": "...", "rowCount": N, "version": "v0", "total": N? },
  "success": true }
```

The transport takes `resp["data"]` (a list of row dicts) and wraps it in a
`pandas.DataFrame`. A response is capped at `limit` rows server-side, so the driver
**paginates** with `offset` (see §5).

---

## 2. `configure(...)` — credentials + endpoint

```python
def configure(api_key=None, base_url=None,
              user_id=None, org_id=None, user_name=None) -> None
```
- **`api_key`** — required for any API access. Sent as the **`X-API-Key`** header;
  the service derives user + org from the key.
- **`base_url`** — the Data API root for your environment, set via the `baseUrl`
  config prop — e.g. `https://vm.hiveq.ai` or `https://staging.hiveq.ai`. The
  transport is endpoint-agnostic: change this URL and the driver pulls from that
  environment (with a valid key for it). The SDK carries a built-in fallback string
  `https://api.hiveq.com`, but that is **not** a live endpoint; always point it at the
  real environment URL.
- `user_id` / `org_id` / `user_name` — deprecated header overrides (`X-User-ID` /
  `X-Org-ID` / `X-User-Name`); the driver does **not** send these.

**How the driver resolves these (precedence, high → low):**
- **api key** — saved credentials from `hiveq login`; advanced/manual overrides
  may still set `apiKey` in the data-source section → `apiKey` in **`[HiveQ]`** →
  `apiKey` in `[default]`.
- **base url** — `baseUrl` in the section → **`[HiveQ]`** → `[default]` →
  `https://staging.hiveq.ai` (driver default). Override per environment, e.g.
  `https://vm.hiveq.ai` for production. The SDK's own
  `https://api.hiveq.com` fallback is a placeholder, not a live endpoint.

Run `hiveq login` once before using HiveQ API-backed data access. Put HiveQ-wide
settings (`baseUrl`, `timezone`, `limit`, …) in a dedicated **`[HiveQ]`**
section so they live in one place; a per-source section still wins.
```ini
[HiveQ]
baseUrl = https://vm.hiveq.ai
```
**Exact names (they are case-sensitive):**
- Section header is **`[HiveQ]`** — capital `H` and `Q`.
- Manual credential property is **`apiKey`** (camelCase); endpoint is **`baseUrl`**.

> ⚠️ The API key is a secret. Prefer `hiveq login`; if you use a manual
> `apiKey` override, keep it only in a **gitignored** config — never in a
> tracked/committed config file.

**Read timeout:** each request uses a connect=10s / read=**300s** timeout, overridable
via the **`HIVEQ_READ_TIMEOUT`** env var (seconds). Large pages of tick data take
tens of seconds to serialize, so don't set this too low.

---

## 3. `Historical(...)` — the read client

```python
Historical(api_key=None, base_url=None,
           user_id=None, org_id=None, user_name=None,
           timezone=None)   # timezone: IANA str or zoneinfo.ZoneInfo
```
- **`timezone`** — when set, naive `start`/`end` are interpreted in this TZ and
  response `time` fields are converted from UTC back to it. The driver passes
  `America/New_York` by default (`DEFAULT_TIMEZONE`); override per-section with a
  `timezone` config prop (e.g. `UTC`).

### `get_data(...)` — fetch historical rows

```python
def get_data(dataset, schema,
             symbols=None, root=None, chains=None,
             start=None, end=None,
             limit=None, offset=None,
             filter_mode=None, **kwargs) -> dict   # JSON {data, meta, success}
```

| Param | Meaning |
|---|---|
| `dataset` | dataset id — `HIVEQ_US_EQ` (equities), `HIVEQ_US_FUT` (futures), `HIVEQ_US_OPT` (options), `HIVEQ_US_IND` (indices). **Required.** |
| `schema` | table within the dataset (see §4). **Required.** |
| `symbols` | symbol or list. Equities: required (`['AAPL','MSFT']`). Futures: full contract syms (`['ESH25']`) — or use `root`. Options: full OCC syms — or use `chains`. |
| `root` | futures root(s) (`['ES','NQ']`) — alternative to `symbols` for futures. |
| `chains` | option underlying(s) (`['SPY']`) — alternative to `symbols` for options. |
| `start`, `end` | window bounds. `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` (str/date/datetime). |
| `limit` | max rows in **this** response (page size). |
| `offset` | rows to skip — used for pagination. |
| `filter_mode` | `"continuous"` (default) or `"session"` — see §4. |
| `**kwargs` | `columns=[...]` (subset of columns); options-only: `expiration_date`, `strike`, `option_type` (`'C'`/`'P'`). |

The driver passes `dataset`, `schema`, `symbols`, `columns`, `limit`, `offset`,
`start`, `end`, and `filter_mode`. It does not currently use `root`/`chains`/the
options kwargs — add them in `__load_historical` if a section needs them.

---

## 4. Datasets, schemas, and filter modes

**Datasets** (from the SDK docstrings): `HIVEQ_US_EQ`, `HIVEQ_US_FUT`,
`HIVEQ_US_OPT`, `HIVEQ_US_IND`.

**Schemas** seen in use / SDK examples: `bars_1s`, `bars_1m`, `bars_1d`,
`eq_trades`, `trades`, `tbbo`, `snaps_1s`, `early_imbalance`, `indices_1m`.
The authoritative, live list comes from the SDK's `Metadata` client
(`Metadata().get_schemas(dataset=["*"])`) — not maintained here.

**`filter_mode`:**
- **`continuous`** (default) — a single time-range query that spans date
  boundaries. Works on any table.
- **`session`** — applies the same **intraday** time window (e.g. 09:30–16:00)
  to **each day** in `[start, end]`. Requires the schema to have both a Date and a
  DateTime filterable column. The driver auto-selects `session` when a section's
  params include a `TimeRange` (intraday window) — see §6.

---

## 5. Pagination (how the driver gets ALL rows)

The API caps each response at `limit` rows (1000 if `limit` is omitted). The driver
sets `limit = DEFAULT_LIMIT` (500,000, the API's `LIMIT_MAX`) and pages with `offset`:

```
offset = 0
loop:
    page = get_data(..., limit=PAGE, offset=offset)["data"]
    accumulate(page)
    if len(page) < PAGE:        # short page ⇒ end of data
        break
    offset += PAGE
```
A page that comes back **exactly full** (`len == limit`) means there may be more, so
it fetches the next page; a short page ends the loop. Override the page size with a
per-section `limit` config prop. (Bigger page = fewer requests but each is a larger,
slower transfer — keep it within the `HIVEQ_READ_TIMEOUT`.)

---

## 6. Config → SDK mapping (what `hiveq_transport` does)

A `transport=HiveQ` section's properties map to `get_data` arguments. Every prop is
resolved **data-source section → `[HiveQ]` → `[default]`**, so HiveQ-wide settings
(`apiKey`, `baseUrl`, `timezone`, `limit`, …) can be set once in `[HiveQ]`:

| Config prop | → `get_data` | Notes |
|---|---|---|
| `dataset` | `dataset` | required |
| `schema` | `schema` | required |
| `columns` | `columns` (kwarg) | comma-separated → list |
| `limit` | `limit` (page size) | default 500,000 |
| `filterMode` | `filter_mode` | else auto: `session` if a `TimeRange` is present |
| `splitSize` | (windowing) | days-per-request; default `1` ⇒ **one request per day**, concatenated |
| `timezone` | `Historical(timezone=)` | default `America/New_York` |
| `apiKey` | `configure(api_key=)` | section → `[HiveQ]` → `[default]` |
| `baseUrl` | `configure(base_url=)` | section → `[HiveQ]` → `[default]` → `https://staging.hiveq.ai` |

The **`params_tuple`** passed to `dd.load(...)` supplies the runtime window/symbols
(`hiveq_transport.__extract_params`):
- first **`DateRange`** field → `start` / `end` (split into per-day windows by `splitSize`),
- first **`TimeRange`** field → the intraday window (and switches `filter_mode` to `session`),
- remaining list/str field(s) → `symbols`.

Example section + call:
```ini
[AaplBars]
primary = AaplBars1m
[AaplBars1m]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = bars_1m
```
```python
Params = collections.namedtuple('Params', ['date', 'time', 'sym'])
p = Params(DateRange('2025-10-14','2025-10-14'),
           TimeRange('09:30:00','16:00:00'), ['AAPL'])
df = Driver().load('AaplBars', params_tuple=p)   # → get_data(HIVEQ_US_EQ, bars_1m, ['AAPL'], session)
```

---

## 7. Publish (the `save` path)

The HiveQ transport has **two publish paths**. Only the SDK/REST path uses the
`hiveq_data` SDK; the WebSocket path speaks directly to the distributor and does
**not** use the SDK at all.

### 7a. WebSocket publish — real-time (preferred for live)

When a config section has a **`topic`**, `dd.save()` bypasses the SDK entirely
and opens a WebSocket to the distributor message broker
(`ws://<wsHost>:<wsPort>`). Each DataFrame row is sent as a single JSON frame:

```json
{"action": "publish", "topic": "<topic>", "key": "<key>", "data": {<row dict>}}
```

The transport waits for a per-message ack before sending the next row. Rows flow
through the distributor to all live WebSocket subscribers on the same topic.

This path does **not** call `hiveq_data` — no API key or `baseUrl` is needed.
It only requires the distributor WebSocket to be reachable. Modelled on the
distributor's `examples/ws_publisher.py`.

**This is the preferred publish mode for live / production** — use it whenever
rows should be delivered in real time to live subscribers.

Config properties: `topic` (required), `keyField` (per-row key column),
`key` (static fallback), `wsHost` (default `localhost`), `wsPort` (default `8765`).

### 7b. SDK / REST publish — batch (preferred for backtest)

When the config section has **no `topic`** but has `publishSchema` (or `schema`)
+ `key`, `dd.save()` uses the `hiveq_data` SDK:

```python
import hiveq_data
publisher = hiveq_data.Publisher(async_mode=True)
publisher.publish(schema=schema, data=records, key=key, operation=operation)
```

This issues `POST /api/publish/v0/data`. The entire DataFrame is serialized as a
list of row dicts and sent in a single HTTP request.

| Param | Meaning |
|---|---|
| `schema` | Target schema to publish into. Config: `publishSchema` (falls back to `schema`). |
| `data` | List of row dicts (`df.to_dict('records')`). |
| `key` | Caller-supplied identifier for the batch. Config: `key`. |
| `operation` | `"add"` (insert, default) or `"modify"` (update by key). Config: `operation`. |

`Publisher(async_mode=)` — when `True` (default), the SDK runs the publish
asynchronously. Config: `async` (default `true`).

This path requires `HIVEQ_API_KEY` + `baseUrl` (resolved the same way as the
pull path: section → `[HiveQ]` → `[default]` → env). It does **not** go through
the distributor — live WebSocket subscribers will **not** see these rows.

**This is the preferred publish mode for backtest, analytics, and bulk uploads**
— any scenario where data is persisted to the HiveQ Data API rather than streamed
to live consumers.

---

## 8. Errors

SDK calls raise **`hiveq_data.HiveQAPIError`** on non-2xx responses, carrying
`status_code`, `response_text`, `response_json`, `request_url`, `request_method`.
The transport logs and re-raises; a `403` typically means the API key/IP is not
permitted, a `400` usually means a missing/invalid filter (e.g. no `start`/`end`).
