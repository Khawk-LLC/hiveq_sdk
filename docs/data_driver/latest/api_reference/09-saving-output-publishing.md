## Saving output (publishing)

The HiveQ transport has **two distinct publish paths**. Which one fires depends
on whether the config section carries a `topic` property. The two paths serve
different use cases — pick the one that matches your scenario:

| | WebSocket publish (real-time) | SDK / REST publish (batch) |
|---|---|---|
| **When to use** | **Live / production** — the preferred mode for real-time publishing. Rows flow instantly through the distributor message broker to all live subscribers on the topic. | **Backtest, analytics, bulk upload** — the preferred mode when you are not publishing to a live stream. Rows are persisted through the HiveQ Data API. |
| **Trigger** | Section has a **`topic`** property | Section has **no** `topic`; uses `publishSchema`/`schema` + `key` |
| **Wire** | Opens a WebSocket to the distributor (`ws://<wsHost>:<wsPort>`), sends one JSON frame per row: `{"action":"publish","topic","key","data":<row>}`, waits for a per-message ack | `hiveq_data.Publisher().publish(...)` → `POST /api/publish/v0/data` (HTTP) |
| **Latency** | Sub-second per row (WebSocket) | HTTP request/response per batch |
| **Destination** | Distributor message broker → live WebSocket subscribers | HiveQ Data API (persisted storage) |
| **Requires** | Distributor WebSocket up on `wsHost:wsPort` | `HIVEQ_API_KEY` + Data API endpoint (`baseUrl`) |

### Path 1 — WebSocket publish (real-time / live)

This is the **preferred publish mode for live / production** use cases. When a
config section has a `topic`, `dd.save()` connects to the distributor's
WebSocket and publishes each DataFrame row as a single message. The rows flow
through the distributor message broker and are delivered instantly to any live
subscriber on the same topic — the mirror image of the WebSocket subscribe path.

The protocol follows the distributor's `examples/ws_publisher.py`:
one `{"action":"publish", "topic":"<topic>", "key":"<key>", "data":{<row>}}`
JSON frame per row. The transport opens a short-lived WebSocket connection, sends
all rows sequentially, and checks the per-message ack from the distributor before
moving on to the next row. A nack or timeout is logged as a warning; `dd.save()`
returns `None` if any row fails.

**Key resolution:** each row's partition key is derived from the `keyField`
column in the DataFrame. If the row has a value in that column, it becomes the
message key (e.g. the symbol). If `keyField` is not set or the column is missing,
the static `key` property from config is used instead.

**Config:**

```ini
[SignalsPub]
primary = HiveQSignalsPub

[HiveQSignalsPub]
transport = HiveQ
topic     = signals.khawk.quant_features   ; topic present ⇒ WebSocket publish
keyField  = symbol                          ; per-row partition key (df column)
wsHost    = localhost                        ; distributor host
wsPort    = 8765                             ; distributor port
```

| Property | Required | Default | Description |
|---|---|---|---|
| `topic` | yes | — | Distributor topic to publish to. Presence of this property activates the WebSocket path. |
| `keyField` | no | — | DataFrame column used as the per-row message key. If absent or the column is missing in a row, falls back to `key`. |
| `key` | no | `""` | Static fallback key when `keyField` is not set. |
| `wsHost` | no | `localhost` | Distributor WebSocket host. |
| `wsPort` | no | `8765` | Distributor WebSocket port. |

**Code:**

```python
import hiveq.driver as dd

# The DataFrame columns ARE the published fields.
# Shape the frame in pandas before saving — what you publish is what subscribers receive.
dd.save('SignalsPub', df)   # one WS message per row, keyed by df['symbol']
```

**Subscribe side:** to consume the published messages live, configure a matching
subscriber section on the same topic (see [Subscription support](08-subscription-support.md)):

```ini
; Publisher
[SignalsPub]
primary = HiveQSignalsPub

[HiveQSignalsPub]
transport = HiveQ
topic     = signals.khawk.quant_features
keyField  = symbol
wsHost    = localhost
wsPort    = 8765

; Subscriber (same topic — receives the published rows)
[SignalsSub]
primary = HiveQSignalsSub

[HiveQSignalsSub]
transport = HiveQ
topic     = signals.khawk.quant_features
keyField  = sym
wsHost    = localhost
wsPort    = 8765
```

```python
import collections, time
import hiveq.driver as dd
from hiveq import Cache

# Publish
dd.save('SignalsPub', signals_df)

# Subscribe (another process or after a brief delay)
Params = collections.namedtuple('Params', ['sym'])
df = dd.load('SignalsSub', params_tuple=Params(['AAPL', 'MSFT']),
             time_out=5000, cache=Cache.NO_CACHE)
```

See `examples/publish_signals.py` for a complete round-trip demo (publish then
read back).

### Path 2 — SDK / REST publish (batch / backtest)

This is the **preferred publish mode for backtest, analytics, and bulk uploads**
— any scenario where you are persisting data to the HiveQ Data API rather than
streaming it to live subscribers. When the config section has **no `topic`** but
has `publishSchema` (or `schema`) + `key`, `dd.save()` publishes through the
`hiveq_data` SDK: `Publisher().publish(schema, data, key, operation)` →
`POST /api/publish/v0/data`.

The entire DataFrame is serialized as a list of row dicts and sent in a single
HTTP request. This does **not** go through the distributor or message broker —
live WebSocket subscribers will **not** see these rows.

**Config:**

```ini
[BacktestOutput]
primary = HiveQBacktestPub

[HiveQBacktestPub]
transport     = HiveQ
publishSchema = signals_backtest       ; schema to publish into
key           = bt_run_20251014        ; caller-supplied identifier for this batch
operation     = add                    ; "add" (default) or "modify"
async         = true                   ; async mode (default true)
```

| Property | Required | Default | Description |
|---|---|---|---|
| `publishSchema` | yes* | — | Schema to publish into. Falls back to `schema` if not set. |
| `key` | yes | — | Caller-supplied identifier for the published batch. |
| `operation` | no | `add` | `"add"` to insert new rows, `"modify"` to update existing rows by key. |
| `async` | no | `true` | Whether the SDK publisher runs in async mode. |

\* If `publishSchema` is not set, the transport falls back to `schema`.

**Code:**

```python
import hiveq.driver as dd

# Publish backtest results to the Data API (persisted, not live-streamed)
dd.save('BacktestOutput', results_df)
```

**Credentials:** the SDK publish path requires `HIVEQ_API_KEY` and `baseUrl`,
resolved the same way as the pull path (section → `[HiveQ]` → `[default]` →
env). See [Configuration](02-configuration.md).

### Saving output to CSV

Add date patterns to the output filename to stamp the current date:

```ini
transport = CSV
file      = csv/out/signals-{date:%Y.%m.%d}.csv
```

### Introducing HDF5 transport

With CSV, dtype metadata is not preserved. HDF5 eliminates tracking column data
types — just supply the `key` and the `store`.

```ini
[DailyBars]
primary = HiveQBars1d
cache   = HDF5DailyBars

[HiveQBars1d]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = bars_1d

[HDF5DailyBars]
transport = HDF5
store     = hdf5/dd.h5
key       = bars_1d-{sym}-{date:%Y.%m.%d}
```

---

