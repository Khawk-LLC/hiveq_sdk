# Backtesting on a published signal

The first tutorial published `hiveq_signal_v1_test` into `quant_features_v3`.
This one wires those rows into a backtest as a **custom data source**, so each
row fires `on_custom_data` at its own timestamp, interleaved with SPY bars.

```sh
python3 backfill_signal_history.py     # load a day of signal history
python3 backtest_on_signal.py          # trade on it
```

Both run from any machine — they only deploy to the platform.

---

## 0. `quant_features_v3` is not `HIVEQ_QUANT_SIGNALS`

Two different registered datasets, and a signal you publish lands in the first:

| dataset / schema | what it holds | shape |
|---|---|---|
| `HIVEQ_QUANT_FEATURES_V3` / `quant_features_v3` | **what you publish** | 38 flat columns |
| `HIVEQ_QUANT_SIGNALS` / `signals` | platform-hosted signals (`Prillach_MC_ES`, …) | rows carry a `signal_json` blob |

Checked directly: `hiveq_signal_v1_test` filtered out of
`HIVEQ_QUANT_SIGNALS`/`signals` returns `rowCount: 0`. Nothing promotes
published rows into that dataset.

The practical difference in your strategy is that `quant_features_v3` needs
**no JSON unwrapping** — the columns are already columns:

```python
# HIVEQ_QUANT_SIGNALS
sig = json.loads(data.column_data("signal_json").replace(r'\"', '"').replace('|', ','))
weight = sig.get("weight1")

# quant_features_v3
weight = float(data.column_data("weight1", default="0") or 0)
```

---

## 1. Why the backfill step exists

A backtest reads **stored history**, so the signal rows have to already be in
the table, on a date that also has market data. Both halves matter:

- SPY bars_1m exist on **2026-06-01** (467 bars). They do **not** exist on
  2026-09-07 or 2026-07-20 — coverage has holes, so check before choosing a date.
- The first tutorial's rows are dated 2026-09-07, where there are no bars.

So `backfill_signal_history.py` publishes eight rows across 2026-06-01.

**It uses the REST route (`publishSchema`), not the topic.** A live signal goes
on the topic; this is a historical fixture being loaded, which is what the REST
route is for. Publishing backdated rows onto a live topic would push them at
subscribers as though they were happening now.

| time (ET) | `signal1` | `score` | intent |
|---|---|---|---|
| 09:35 | 1 | 0.90 | enter |
| 10:05 | 1 | 0.95 | already long — no action |
| 10:35 | 0 | 0.20 | exit |
| 11:05 | 1 | 0.85 | enter |
| 12:00 | 1 | 0.60 | **below threshold — logged, not traded** |
| 13:00 | 0 | 0.10 | exit |
| 14:00 | 1 | 0.99 | enter |
| 15:00 | 0 | 0.05 | exit |

---

## 2. Wiring the dataset in

```python
data_configs=[
    {'type': 'hiveq_historical', 'dataset': 'HIVEQ_US_EQ',
     'schema': ['bars_1m']},
    {'type': 'hiveq_historical', 'dataset': 'HIVEQ_QUANT_FEATURES_V3',
     'schema': ['quant_features_v3'],
     'id': SIGNAL_ID,
     'symbols': [SIGNAL_NAME],
     'symbolFilterKey': 'symbol'},
]
```

Three things are easy to get wrong:

- **`id` must match `ctx.subscribe_data(data_id=...)`.** They are separate names
  and nothing checks them for you.
- **`symbolFilterKey: 'symbol'`** — the request field the `symbols` array is
  sent under. `quant_features_v3` filters on `symbol`.
- **`signals_datasets` must include the dataset**, or the engine looks for the
  run's symbol universe (`SPY`) inside it instead of your signal name:

```python
backtest_config=BacktestConfig(
    initial_capital=100_000,
    extra_config={'signals_datasets': ['HIVEQ_QUANT_SIGNALS',
                                       'HIVEQ_QUANT_FEATURES_V3']},
)
```

The default is `['HIVEQ_QUANT_SIGNALS']` only.

Subscribe to bars as well as the signal — bars initialise the venue so orders
are routable, and give the signal something to trade against.

---

## 3. What a correct run looks like

```
status: {'status': 'DONE', 'is_final': True, 'net_pnl': -29.66, 'return': -0.0003}
```

Every published row arrives at its own timestamp, and the threshold gate holds:

```
09:35  SIGNAL  signal SPY signal1=1 score=0.90     -> enter long, fill 09:42 @ 758.25
10:05  SIGNAL  signal SPY signal1=1 score=0.95     -> already long, no action
10:35  SIGNAL  signal SPY signal1=0 score=0.20     -> exit,  fill 10:43 @ 758.71
11:05  SIGNAL  signal SPY signal1=1 score=0.85     -> enter, fill 11:06 @ 758.67
12:00  SIGNAL  signal SPY signal1=1 score=0.60     -> logged only, below MIN_SCORE
13:00  SIGNAL  signal SPY signal1=0 score=0.10     -> exit,  fill 13:01 @ 757.77
14:00  SIGNAL  signal SPY signal1=1 score=0.99     -> enter, fill 14:00 @ 755.56
15:00  SIGNAL  signal SPY signal1=0 score=0.05     -> exit,  fill 15:00 @ 755.71
```

Two things to read carefully:

- **Fills lag the signal.** The 09:35 signal fills at 09:42 because that day has
  467 bars, not one a minute — the order rests until the next bar. Sparse data,
  not a broken order.
- **A non-zero P&L is the smoke test.** Zero trades means the wiring failed
  silently — usually `id` mismatched, `symbolFilterKey` wrong, or the dataset
  missing from `signals_datasets`. Check the `SIGNAL` event logs: if they are
  absent, no rows were delivered at all.

Read the logs with:

```python
run.event_logs()
```

---

## 4. Prefer a CSV for your own signals

If the signal is yours and not yet published anywhere, a CSV custom data source
is simpler — same `on_custom_data` callback, no publish step, no dataset
registration. Use `quant_features_v3` when the signal is genuinely published and
you want the backtest reading the same rows production reads.
