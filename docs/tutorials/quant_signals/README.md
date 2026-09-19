# Publishing signals and custom data

A follow-along guide for internal quants: how to publish a signal — or any custom
data — to a HiveQ topic with the data driver, prove it landed, and then consume
it from a backtest or a realtime LiveSim run.

**Going from backtest to LiveSim takes no code changes at all** — same strategy,
same callback. The platform converts the backtest's data sources into live
subscriptions for you; all that has to hold is that the live records carry the same
structure as the records the backtest read.

**The SDK runs anywhere; the data does not** — every step names the machine it
runs on.

The worked example is a signal called **`hiveq_signal_v1_test`** published to
`prod.quants.quant_features_v3`, but nothing here is specific to that topic or
schema (see the closing note in §9).

---

## 0. What is custom data?

Anything your research produces that another part of the platform needs to read:
a signal, a feature, a model score, a set of price levels.

Each of those is an observation about one instrument at one moment — *go long
SPY, conviction 0.9*, or *the decay factor for this cluster is 0.42*. You publish
them one record at a time, and the shape never changes: you assemble a record in
pandas, it is stored with the same fields it was published with, and a strategy
reads those fields back by name with `data.column_data("signal1")`. What you sent
is what the strategy sees.

Once published, anything with access can read it:

- **Your strategy in a backtest** — the records arrive at `on_custom_data` at their
  own timestamps, interleaved with market data, so you can measure what trading
  them would have done.
- **Your strategy running live** — the same code and the same callback, now fed
  by the live stream instead of stored history.
- **Another desk's execution strategy** — you publish a signal; a separate
  strategy subscribes and trades it, with no coordination beyond agreeing on a
  name.
- **Anything else that subscribes** — a standalone subscription script, a
  monitor, a notebook someone is watching it from. A subscriber does not have to
  be a strategy; it just has to know the channel and the key.

### You publish once; it is kept for you

The same publish does two jobs. Subscribers get the record **immediately**, and the
platform writes it on to the historical database **automatically** — you do not
run a second job, or a nightly load, or anything else to save it.

Which is why today's live signal is simply *there* when you backtest next month.
Everything you have ever published accumulates as history without you managing
it.

### You may not need to define anything

HiveQ already provides schemas for the shapes quants publish most often. If yours
fits one, there is nothing to design and nothing to request — publish into it and
the data is immediately queryable, backtestable and available to subscribers:

| what it holds | dataset / schema |
|---|---|
| Quant features — the worked example here | `HIVEQ_QUANT_FEATURES_V3` / `quant_features_v3` |
| Platform-hosted signals | `HIVEQ_QUANT_SIGNALS` / `signals` |
| Cluster analytics | `HIVEQ_QUANT_CLUSTERS` / `clusters` |
| Market-making zone snapshots | `HIVEQ_QUANT_CLUSTER_ZONE` / `cluster_zone` |
| Market-making grid modes | `HIVEQ_QUANT_CLUSTERS_MODES` / `clusters_modes` |

This guide publishes a signal named `hiveq_signal_v1_test` into
`quant_features_v3`, chosen because it is already provisioned and its 38 columns
cover most of what a signal needs to express. Every step that follows works the
same way for any other schema or channel.

---

## 1. Which machine am I on?

The same code behaves differently depending on where you run it. `hiveq.driver`
resolves either to a stub that raises or to the real driver, and data access is
granted by network rather than by credential.

| | Where | `hiveq.driver` is | Data access |
|---|---|---|---|
| **E1** | Any machine, `hiveq-sdk` only | an import **stub** | none — calls raise `PlatformOnlyError` |
| **E2** | Any container spawned in the HiveQ platform — marimo, Jupyter, VS Code | the real driver | full |
| **E3** | Your **company VM**, driver installed | the real driver | full — the network is provisioned for it |

**The split that matters:** `hiveq-sdk` runs anywhere, your laptop included,
because it only authors code and deploys it. **Data access is provisioned by
network** — so it happens on the VM, in a platform container, or in a deployed
job. Installing the driver on a laptop does not change that.

Work through them in that order. E1 is not a mistake to skip past — it is the
contract, and recognising its failure mode saves an afternoon.

> **If driver calls raise with the real driver installed**, check the line count
> of `hiveq/driver/data_driver.py`: 23 is the stub, ~300 the real driver. Both
> distributions ship the same `hiveq/driver/*.py` paths, so the second one
> installed overwrites the first while `pip list` still shows both.
> Reinstalling the one you want repairs it.

---

## 2. E1 — what happens if you just try it

Run the publish script on your laptop, before installing anything:

```sh
python3 publish_hiveq_signal_v1_test.py
```

The import succeeds. The first driver *call* is what stops:

```
hiveq._platform_only.PlatformOnlyError: hiveq.driver.init() cannot run on a
local machine -- it is an import stub.
```

Every entry point behaves this way — `init`, `load`, `save`, `stop`, `alert`,
`Driver()`. The stub exists so driver code can be written and type-checked
locally, then run where the driver lives.

---

## 3. The fields you can publish

Whatever schema you target, read its fields from the platform rather than
guessing. For the worked example, `quant_features_v3` has **38 columns, 17 of
them NOT NULL**. Fetched from
`POST /api/metadata/v0/schema-details`
(`dataset=HIVEQ_QUANT_FEATURES_V3`), staging database `staging_quant_001`.

### Identity — all NOT NULL

| Column | Type | What goes in it |
|---|---|---|
| `symbol` | String | **the signal's NAME** — `hiveq_signal_v1_test` |
| `ticker` | String | the instrument the signal is about — `SPY` |
| `norm_ticker` | String | normalised instrument |
| `root` | String | root symbol |
| `id` | String | record identifier |
| `table` | String | source table name |

> **The one trap that matters.** `symbol` is the *signal name*, not a tradeable
> instrument. The instrument lives in `ticker` / `norm_ticker` / `root`. Getting
> these backwards publishes records nobody can find.

### Time — all NOT NULL

| Column | Type | Meaning |
|---|---|---|
| `date` | Date | calendar date |
| `time` | DateTime64(3) | event timestamp |
| `ts_event` | DateTime64(3) | when the signal happened |
| `recv_event` | DateTime64(3) | when you received it |
| `db_event` | DateTime64(3) | when it was written |

Timestamps are **US/Eastern already**. Do not convert from UTC.

### Signal payload

| Column | Type | Null? |
|---|---|---|
| `signal1` | Float64 | NOT NULL |
| `signal2`, `signal3` | Float64 | nullable |
| `weight1` | Float64 | NOT NULL |
| `weight2`, `weight3` | Float64 | nullable |
| `side` | Int32 | NOT NULL |
| `score`, `size` | Float64 | nullable |

### Pricing context — all nullable

`price`, `stop_px`, `target_px`, `volume`

### Bookkeeping

| Column | Type | Null? |
|---|---|---|
| `freq` | Int64 | NOT NULL |
| `flag` | String | NOT NULL (empty string is fine) |
| `text` | String | NOT NULL |
| `version` | Float64 | nullable |

### Reserved

`reserved1` … `reserved10`, all `Nullable(Float64)`.

**Filterable** (usable as read filters): `date`, `time`, `symbol`, `ticker`,
`root`, `norm_ticker`, `flag`, `side`, `version`, `table`. There are no required
filters and no primary keys.

---

## 4. Realtime data or historical data

Your records exist in both forms, and they are reached differently. Which one a call
uses is decided entirely by config — the Python you write is identical. These are
the same two words the platform uses, so they are worth fixing in your head now.

- **Realtime — a topic.** Records handed to whoever is subscribed *at that moment*,
  arriving in milliseconds. This is how a signal is published and how a running
  strategy receives one. The LiveSim dialog calls these **real-time sources**.
- **Historical — a dataset.** The same records once they are in platform storage,
  queried by date and symbol. This is what a backtest reads, because a backtest
  replays the past and there is no live stream to listen to. In `data_configs`
  this is `type: 'hiveq_historical'`.

It is one record in two places, not two kinds of data. You publish it as realtime;
the platform files it away as historical a few seconds later. That is what makes
today's signal part of next month's backtest.

| Doing this | Reads | Set in config |
|---|---|---|
| **Publishing a signal** | realtime | `topic` |
| Backtesting on past records | historical | `dataset` + `schema` |
| Checking a live feed works | realtime | `topic` + `time_out` |

```python
'HiveQQuantFeaturesPub': {
    'transport': 'HiveQ',
    'topic': 'prod.quants.quant_features_v3',
    'keyField': 'symbol',                    # per-record key = the signal name
    'wsHost': 'hiveq-distributor-staging',
    'wsPort': 8765,
}
```

**The key is the signal name, not `id`.** Taken from a live dump of staging's
distributor: `key=1800_FearCycle_MR_ES_V1_Long_PF15a_Sim`, while `id` in that
same record was `Sigma_Pub`. Resolution order is `keyField` column → static `key` →
`''`. That last fallback is the trap: a mistyped `keyField` with no static `key`
publishes every record under an empty key, the distributor acks it, `save()` returns
the frame, and no subscriber sees it.

**The distributor address depends on where your code runs.** Neither form works
from the other side, so set it per environment:

| running in | `wsHost` : `wsPort` |
|---|---|
| a platform container (job, marimo, Jupyter, VS Code) | `hiveq-distributor-staging` : `8765` |
| the VM / provisioned network | `staging.hiveq.ai` : `8765` |

The internal service name only resolves inside a container; `localhost` is wrong
everywhere. No environment variable exposes the address, so name it in config —
or set `HIVEQ_DISTRIBUTOR_HOST` and let the scripts pick it up:

```sh
export HIVEQ_DISTRIBUTOR_HOST=<host>
export HIVEQ_DISTRIBUTOR_PORT=8765
```

If a connection from the VM times out, the endpoint is not yet open to your
network — check with the platform team rather than assuming your config is wrong.

**Do not publish a signal over REST.** A section with `publishSchema` and no
`topic` writes straight to storage and live subscribers never see it — that
route is for backtest output and bulk loads.

---

## 5. The files here

| File | Runs where | What it does |
|---|---|---|
| `deploy_publish_job.py` | anywhere (`hiveq-sdk`) | publishes to the topic as a `QUANT_SCRIPTS` job, then reads stored history |
| `realtime_subscribe_job.py` | anywhere (`hiveq-sdk`) | subscribe → publish → receive |
| `publish_hiveq_signal_v1_test.py` | platform container or VM | in-process topic publish |
| `verify_read_back.py` | platform container or VM | REST read of stored history |
| `backfill_signal_history.py` | anywhere (`hiveq-sdk`) | loads a day of signal history for a backtest to read |
| `backtest_on_signal.py` | anywhere (`hiveq-sdk`) | backtest that trades on the published signal |
| `tutorial.html` | — | the presentable version of this guide |
| `BACKTEST.md` | — | the backtest walkthrough in detail |

The job and backtest scripts run from any machine because they only deploy; the
two in-process scripts need the driver and a provisioned network.

Config is a plain dict inside each script — there is no `dd-config.ini` to copy
around or let drift. `dd.init` also takes a `.json`, a `.py` defining `CONFIG`,
or a `.ini` path.

One detail worth knowing: `params_tuple` fields are matched **by type, not
name** — the first `DateRange` is the date window, the first list/str the symbol
list. The field names are for your benefit only.

---

## 5b. Publishing without installing anything (`QUANT_SCRIPTS`)

`deploy_job` cloudpickles a function and runs it in a platform container, where
the real driver lives — so a laptop with only `hiveq-sdk` can publish. It is also
the only route that works off the platform network.

```python
def publish_signal():
    from hiveq.driver.data_driver import Driver     # imports INSIDE the function
    driver = Driver(config=CONFIG)                  # never the dd.* facade
    driver.save('QuantFeaturesPub', df)
    return {'published': len(df)}

if __name__ == '__main__':                          # the guard is required
    job = hf.deploy_job(publish_signal, task_name='publish-hiveq-signal-v1-test',
                        wait=True)
    print(job.result(), job.logs())
```

Three rules:

1. **Construct `Driver(config=...)`, not the module-level `dd.*` facade.** The
   facade's logger does `makedirs('<cwd>/logs')` and a job container's cwd
   `/app` is read-only, so the first `dd.*` call dies with
   `OSError: [Errno 30] Read-only file system: '/app/logs'`.
2. **Keep driver imports inside the function**, so they resolve against the
   container's driver, not the deploying machine's stubs.
3. **Credentials are `HIVEQ_API_KEY` only.** Do not set `HIVEQ_DATA_URL`,
   `HIVEQ_ORG_ID` or `HIVEQ_USER_ID`.

---

## 5c. Two properties a read section needs

**`'timezone': 'UTC'`** — and it does not mean what it says. It means *do not
convert*. The default `US/Eastern` converts UTC→Eastern, but this schema stores
Eastern already, so every record comes back 4 hours early:

| published | raw REST | driver default | driver `'UTC'` |
|---|---|---|---|
| `10:30:00` | `10:30:00` | `06:30:00` | `10:30:00` |

The default is not globally wrong — schemas that really store UTC need it. It is
wrong for this one. WebSocket reads are unaffected.

**`columns`** — without an explicit list you get the default projection, 10 of
the 38 columns, and your published values look missing when they are merely
unselected.

---

## 5d. Confirming a topic publish

Delivery is confirmed off the wire, in milliseconds. Storage catches up later —
a published marker was absent at +5.1s and present at +10.1s — so a REST read
run right after publishing will legitimately miss the record.

```sh
python3 realtime_subscribe_job.py
```

```
connected ws://hiveq-distributor-staging:8765 topic=prod.quants.quant_features_v3
subscription open; 0 row(s) buffered before publishing
Published 1 row(s) (0 failed) to topic=prod.quants.quant_features_v3
received 1 row(s) after publishing
round-trip CONFIRMED
```

Two things make this work rather than flake:

**Open the subscription first.** The distributor forwards only to subscribers
connected when a message is sent, so `buffered_before: 0` is correct rather than
a failure.

**`time_out` does not wait for records.** It blocks only until the initial connect
completes — `while not self._initial and elapsed <= time_out`. Once the
subscriber is up, every `load()` returns immediately with whatever is buffered,
so a single `load(time_out=8000)` after publishing is a race, not a wait. Poll
until your marker appears:

```python
deadline = time.time() + 15
while time.time() < deadline:
    after = driver.load('QuantFeaturesSub', params_tuple=params)
    if after is not None and (after['text'] == marker).any():
        break
    time.sleep(0.5)
```

### Pointing at a different distributor

In a deployed job the container's own `HIVEQ_DISTRIBUTOR_HOST` wins over whatever
was baked in at deploy time, and each job prints the address it used:

```
distributor: hiveq-distributor-staging:8765
connected ws://hiveq-distributor-staging:8765 topic=prod.quants.quant_features_v3
```

---

## 6. Installing the driver on your VM

The VM is the company-provisioned machine you remote into. Its network is the one
entitled to the data, so this is where a local driver install actually buys you
something.

The driver is not on public PyPI. **Ask the internal systems team for the
wheel**, copy it to the VM, and install it:

```sh
pip install ./HiveQDataDriver-<version>-py3-none-any.whl
```

Confirm you got the real driver rather than a stub — the line count is the tell:

```python
import inspect
from hiveq.driver.data_driver import Driver
print(len(inspect.getsource(Driver).splitlines()))    # 23 = stub, ~300 = real
```

### Then run the same example

Nothing changes about the code. The scripts from the earlier steps run in-process
on the VM — publish to the topic, subscribe to it, read stored history:

```sh
python3 publish_hiveq_signal_v1_test.py
python3 verify_read_back.py
```

### What works where

| | on the VM | off the provisioned network |
|---|---|---|
| REST read — `dataset` + `schema` | works | no — IP restricted |
| REST publish — `publishSchema` | works | no — IP restricted |
| Topic publish | works — `staging.hiveq.ai:8765` | no route to the distributor |
| Topic subscribe | works — same endpoint | no route |

On the VM everything runs in-process, including the topic round trip. Point
`wsHost`/`wsPort` at `staging.hiveq.ai:8765`; the internal name
`hiveq-distributor-staging` only resolves inside a platform container.

Off the provisioned network the driver installs and then reaches nothing: data is
IP restricted and the distributor is not routable. The public `/ws/relay`,
`/api/viewer/ws` and `/api/market/ws` routes are not a way in either — they proxy
to the viewer relay, a different protocol needing its own token. A local install
there buys you authoring and type-checking only.

---

## 7. Why the driver rather than hand-rolled REST

Its publisher replaces every non-finite float with `null` before serializing.
The stdlib JSON encoder emits bare `NaN`, **and the Data API coerces that to
`0`** — so a value you meant as "absent" reads back as `0.0`, silently.

---

## 8. Backtesting on the signal you published

Declare the published records as a custom data source and each one fires
`on_custom_data` at its own timestamp, interleaved with bars:

```sh
python3 backfill_signal_history.py     # load a day of signal history
python3 backtest_on_signal.py          # trade on it
```

`quant_features_v3` is **not** `HIVEQ_QUANT_SIGNALS` — two different registered
datasets, and what you publish lands in the first. Full walkthrough, including
the three ways the wiring fails silently, is in **`BACKTEST.md`**.

---

## 9. From backtest to LiveSim

A backtest reads stored history; LiveSim reads the live topic. Everything else
stays the same — including your code.

**No code changes.** The strategy that ran the backtest is the strategy that runs
live: same class, same `on_custom_data`, same `ctx.subscribe_data(...)`. You do
not write a subscriber and you do not fork the strategy for realtime. All that
has to hold is that the live records carry the same structure as the records the
backtest read — on promotion the platform converts the backtest's data sources
into container-managed real-time subscriptions and pre-fills them, which is what
**Reset to transformed defaults** restores. You are reviewing a configuration,
not authoring one.

**Backtests → History**, find the run, **Deploy to Livesim**. Screenshots of each
step are in `tutorial.html`; the images themselves are in `images/`.

Two things the dialog tells you:

- Workspace files from the source backtest are staged into a per-user
  `livesim_artifacts` volume.
- Backtest-only sources are converted to container-managed real-time
  subscriptions — **LiveSim does not replay files.**

### Add real-time source

| Field | Value here | Becomes |
|---|---|---|
| **Logical source ID** | `qf3_signals` | must match `ctx.subscribe_data("qf3_signals")` |
| **Topic key** | `hiveq_signal_v1_test` | the adapter's `subscribeKeys` — only records published under this key arrive |
| **Topic** | `prod.quants.quant_features_v3` | the adapter's `distributorTopic` |
| Filter payload rows | optional, e.g. `symbol=ES` | the adapter's `extraFilters` — an exact match after the key subscription |

`Preview sources` shows what will actually be written; `Reset to transformed
defaults` puts it back. Strategy params carry over from the source backtest and
can be overridden. Deploy schedules it onto a container
(`livesim-equities-1`), picked up within ~15s with no restart.

### The same three names, in three places

| | publisher config | backtest | LiveSim dialog |
|---|---|---|---|
| logical data id | — | `id` / `ctx.subscribe_data(data_id=)` | **Logical source ID** |
| partition key | `keyField` → the record's `symbol` | `symbols` + `symbolFilterKey` | **Topic key** |
| topic | `topic` | — (reads stored history) | **Topic** |

Get one out of step and nothing errors — the subscriber simply never receives
anything.

### None of this is specific to `quant_features_v3`

That topic is just one topic. **Any custom data you can shape as records can be
published to its own topic** — set `topic` and `keyField` in the publisher config
— and wired up exactly the same way: the same three names, the same **Add
real-time source** form, the same `on_custom_data` callback in the strategy.

The one asymmetry is the backtest side. Reading *stored history* needs a
registered dataset and schema, which `quant_features_v3` is. If your data is not
registered as one, backtest it from a CSV custom data source instead and keep the
topic for the live path — the strategy code does not change either way.

