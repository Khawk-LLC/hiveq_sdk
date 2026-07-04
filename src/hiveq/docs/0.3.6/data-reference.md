# HiveQ Flow Data Reference

Use the SDK to print the datasets and schemas available to your account:

```bash
hiveq datasets
```

The command reads the HiveQ data API metadata service, backed by the platform's
table schema metadata. It is not a frozen list in the wheel.

For machine-readable output:

```bash
hiveq datasets --json
```

To inspect fields for a dataset/schema pair:

```bash
hiveq datasets fields HIVEQ_US_EQ bars_1m
```

To view a small sample:

```bash
hiveq datasets sample HIVEQ_US_EQ bars_1m --limit 5
hiveq datasets sample HIVEQ_US_EQ bars_1m --filters '{"symbol":"AAPL"}' --start 2026-06-01 --end 2026-06-01
```

When `--filters`, `--start`, or `--end` are omitted, the sample command derives
the minimal required filters from metadata and returns the first non-empty
sample it can find. Pass explicit filters when you need a specific symbol or
date range.

Use `--base-url` only when you need to point at a non-default HiveQ data API
host. The default resolves from `HIVEQ_DATA_URL`, `HIVEQ_BASE_URL`,
`HIVEQ_AUTH_URL`, then `https://staging.hiveq.ai`.

See [`api-reference/09-data-configs.md`](api-reference/09-data-configs.md) for
the `data_configs` shape used by strategy backtests.

## 1. Choosing schemas

The exact available datasets, schemas, fields, and sample rows come from
`hiveq datasets`, `hiveq datasets fields`, and `hiveq datasets sample`.

**Bars vs. trades — coverage matters for correctness:**
- `bars_*` (aggregated) carry **no trade/auction print** — an
  auction order (`MOO`/`MOC`/`LOO`/`LOC`) will never fill against them.
- **Auction orders and auction/POV/TWAP/VWAP executors need a tick-by-tick trade
  stream** — subscribe with `ctx.subscribe_trades(...)` using `eq_trades`
  (equities) or `fut_trades` (futures). Never `bars_*`.

## 2. Published signal and analytics payloads

Rows from signal, cluster, zone, and other analytics schemas arrive at
`on_custom_data` when wired as custom data. Signal rows may include a
`signal_json` column with a JSON-encoded payload. Confirm the actual fields with
`hiveq datasets fields <DATASET> <SCHEMA>`.

```python
def on_custom_data(self, ctx, event):
    data = event.data()
    sig = json.loads(data.column_data("signal_json"))
    value = sig.get("my_field")
```

The `symbols` key in the `data_configs` entry selects which stream to subscribe
to; the entry's `id` must match `ctx.subscribe_data(data_id=...)`.

> Backtesting with your **own** signals? Use a CSV custom data source instead
> ([`api-reference/09-data-configs.md`](api-reference/09-data-configs.md) §9.2) — it fires the same `on_custom_data` callback
> without the `signal_json` wrapping.

## 3. Behavior derived from dataset/schema

- schema containing `bar` → bar-based fills; schema with `trade` → tick-based fills.
- `dataset='HIVEQ_US_FUT'` → futures session defaults (18:00–17:00 ET) applied automatically.
- `dataset='HIVEQ_US_OPT'` + `snaps_*` schema → options snapshot handling.

## 4. Examples

```python
# equities 1-minute bars
{'type':'hiveq_historical','dataset':'HIVEQ_US_EQ','schema':['bars_1m']}
# futures (subscribe to a continuous symbol like 'ES.c.0')
{'type':'hiveq_historical','dataset':'HIVEQ_US_FUT','schema':['bars_1m']}
# tick trades for auction fills / executors
{'type':'hiveq_historical','dataset':'HIVEQ_US_EQ','schema':['eq_trades']}
{'type':'hiveq_historical','dataset':'HIVEQ_US_FUT','schema':['fut_trades']}
# quant signals (subscribe via ctx.subscribe_data(data_id=...))
{'type':'hiveq_historical','dataset':'HIVEQ_QUANT_SIGNALS','schema':['signals'],
 'id':'mysignals','symbols':['My_Signal_Name']}
# quant analytics
{'type':'hiveq_historical','dataset':'HIVEQ_QUANT_CLUSTERS','schema':['clusters'],
 'id':'clusters','symbols':['ES.c.0']}
```
