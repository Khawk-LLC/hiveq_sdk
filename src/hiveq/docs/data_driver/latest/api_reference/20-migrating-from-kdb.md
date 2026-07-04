## Migrating from KDB

The KDB transport is retained (`transport=KDB`) so existing kdb+ configs keep
working. HiveQ is recommended — translate unless you must stay on kdb+. It is a
**config** change, not a code change.

| KDB-era config                  | HiveQ config |
|---------------------------------|--------------|
| `transport=KDB`                 | `transport=HiveQ` |
| `KDBQuery=.foo.get_trades[...]` | `dataset=` + `schema=` (+ `columns`, `filterMode`) |
| `KDBHost`/`KDBPort`             | none — endpoint is internal; auth via `HIVEQ_API_KEY` |
| `.u.sub[...]`                   | `topic=...` |
| `.u.pub` / save-to-KDB (live)   | `topic=` section (WebSocket → distributor message broker; preferred for live) |
| `.u.pub` / save-to-KDB (batch) | `publishSchema`/`key` section (SDK → REST API; preferred for backtest) |
| `pullDataSourceID`              | same property; HiveQ transport stitches history internally |
| KDB heartbeat query/table       | none — WebSocket keepalive is internal |
| `splitSize`                     | `splitSize` (DateRange chunking, days) |
| `KDBUserName`/`KDBPassword`     | none — `HIVEQ_API_KEY` only |

A `KDBQuery` does not mechanically map to a dataset/schema — **ask**, don't
invent. Do not emit `KDB*` properties or q syntax for new code.

> **qpython on modern stacks.** The original `qpython` 2.0.0 is Python-2 era and
> breaks on modern numpy/pandas. The driver's KDB code is already fixed; install
> the `[kdb]` extra — **`qpython3==1.0.1`** (same `qpython` import namespace, no
> code change):
>
> ```sh
> pip install '.[kdb]'
> # qpython3's Cython ext links against numpy at build; if a cached wheel mismatches:
> pip install numpy && pip install --no-binary qpython3 'qpython3==1.0.1'
> ```
>
> numpy 2.x removed `numpy.string_`/`numpy.NaN`/`ndarray.tostring()`;
> `hiveq/driver/_qpython_compat.py` shims them and is auto-imported before any
> qpython import, so `transport=KDB` works on numpy 2.x. `qpython` is imported
> only when `transport=KDB` is used.

---

