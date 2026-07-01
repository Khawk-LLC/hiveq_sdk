<!--
CANONICAL DATA CATALOG FOR HIVEQ FLOW.
Audience: code-generation agents AND human developers, plus the data team keeping
this catalog current. This file owns the "what data exists / what does it look like"
facts. `api-reference.md` §9 owns "how to wire it into data_configs" and links here.
-->

# HiveQ Flow — Data Reference

- **scope of this doc**: every `dataset` code and `schema` code accepted by
  `data_configs` (`type='hiveq_historical'`), what each contains, its
  granularity, and any coverage caveats. Update this file when a dataset or
  schema is added, removed, or its coverage changes — no other doc needs to
  change for a data-catalog update.
- See `api-reference.md` §9 for the `data_configs` dict shape and how to wire
  a dataset/schema into a strategy.

---

## 1. Datasets (`dataset` key)

| dataset | asset class | notes |
|---|---|---|
| `HIVEQ_US_EQ` | US equities | |
| `HIVEQ_US_FUT` | US futures | subscribe via a continuous symbol (e.g. `'ES.c.0'`); for rollover set `BacktestConfig(enable_auto_rollover=True)` — no `data_configs` flag needed. Futures session defaults (18:00–17:00 ET) are applied automatically. |
| `HIVEQ_US_OPT` | US options | pair with `snaps_*` schema for options snapshot handling. |
| `HIVEQ_US_IND` | US indices | spot index value / daily index bars only (`ctx.subscribe_index` / `ctx.subscribe_index_bars`). |
| `HIVEQ_US_ETF` | US ETFs | |
| `HIVEQ_QUANT_SIGNALS` | platform-hosted signals | delivers rows to `on_custom_data` (see §3). |
| `HIVEQ_ECON` | economic data | |
| `HIVEQ_STRAT` | published run results | **output only** — read after a run (not a strategy input). |

## 2. Schemas (`schema` key)

| schema | granularity | contains |
|---|---|---|
| `bars_1s` / `bars_1m` / `bars_1d` | 1 second / 1 minute / 1 day | aggregated OHLCV bars. **These are the only bar granularities** — there is no `bars_5m` / `bars_1h`; aggregate `bars_1m` in strategy code if you need coarser bars. |
| `eq_trades` | tick | equities trade prints, including the official opening/closing auction prints (`MCOfficialOpen` / `MCOfficialClose`). |
| `fut_trades` | tick | futures trade prints, including the official opening/closing auction prints. |
| `tbbo` | tick | quotes (bid/ask). Delivers `on_trade` **and** `on_quote`. Tick coverage is more limited than `eq_trades`/`fut_trades`. |
| `snaps_1s` | 1 second | options snapshot data (pair with `HIVEQ_US_OPT`). |
| `signals` | per-signal | platform-hosted signal rows (pair with `HIVEQ_QUANT_SIGNALS`, see §3). |

**Bars vs. trades vs. quotes — coverage matters for correctness, not just style:**
- `bars_*` (aggregated) and `tbbo` (quotes) carry **no trade/auction print** — an
  auction order (`MOO`/`MOC`/`LOO`/`LOC`) will never fill against them.
- **Auction orders and auction/POV/TWAP/VWAP executors need a tick-by-tick trade
  stream** — subscribe with `ctx.subscribe_trades(...)` using `eq_trades`
  (equities) or `fut_trades` (futures). `tbbo` quotes also drive executors but
  with more limited coverage; prefer `eq_trades`/`fut_trades`. Never `bars_*`.

## 3. `HIVEQ_QUANT_SIGNALS` payload format

Each row arrives at `on_custom_data` with a `signal_json` column containing a
JSON-encoded payload. Parse it in strategy code:

```python
def on_custom_data(self, ctx, event):
    data = event.data()
    sig = json.loads(data.column_data("signal_json"))
    value = sig.get("my_field")
```

The `symbols` key in the `data_configs` entry selects which signal stream to
subscribe to; the entry's `id` must match `ctx.subscribe_data(data_id=...)`.

> Backtesting with your **own** signals? Use a CSV custom data source instead
> (`api-reference.md` §9.2) — it fires the same `on_custom_data` callback
> without the `signal_json` wrapping.

## 4. Behavior derived from dataset/schema

- schema containing `bar` → bar-based fills; schema with `trade`/`tbbo` → tick-based fills.
- `dataset='HIVEQ_US_FUT'` → futures session defaults (18:00–17:00 ET) applied automatically.
- `dataset='HIVEQ_US_OPT'` + `snaps_*` schema → options snapshot handling.

## 5. Examples

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
```
