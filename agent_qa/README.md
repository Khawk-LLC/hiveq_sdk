# agent_qa — HiveQ's self-extending QA suite

Exhaustive, tiered, re-runnable validation of the HiveQ platform across both
execution modes (historical backtest and livesim), plus an agent that grows the
suite from new commits.

Not shipped in the wheel: `pyproject.toml` finds packages under `src/`, so this
top-level directory is development-only.

## Quick start

```bash
cd ~/PycharmProjects/hiveq-sdk
pip install -r agent_qa/requirements-qa.txt      # pyyaml; the rest comes with the SDK

python agent_qa/run_all.py --level l0            # preflight — run this first
python agent_qa/run_all.py --tier 1              # historical, both engines
python agent_qa/run_all.py --tier 2 --profile staging   # livesim
python agent_qa/run_all.py --list                # what would run
```

If `l0_01` fails with a 401, **stop and fix credentials** — every data-dependent
verdict below `l0` is meaningless until then. Run any normal SDK command once to
sign in against the environment your `--profile` targets. A local-stack key 401s
against staging and vice versa.

## The two axes

|  | `--engine inproc` | `--engine remote` |
|---|---|---|
| provides `hiveq.flow` | `hiveq-flow` (fat engine) | `hiveq-sdk` (thin client) |
| where callbacks run | this process | the platform executor |
| evidence channel | module state | `run.event_logs()` |
| `run.event_logs()` | empty *by design* | populated |
| catalog / livesim REST | unavailable | available |

The two packages **claim the same import namespace and must not be
co-installed**. The runner handles this by putting `hiveq-flow/src` at the front
of `PYTHONPATH` for `inproc` — the same shadowing trick `hiveq-flow`'s own
`qa_validation` uses. No second virtualenv is needed. Point elsewhere with
`--flow-src` or `AGENT_QA_FLOW_SRC`.

`--profile local|staging` selects the URL/credential preset. Tier 2 (livesim)
should run against `staging`: local Kafka is PLAINTEXT while the deployed stack
is SASL_SSL, so livesim transport bugs do not reproduce locally.

## `Probe` — why tests do not branch on engine

A test's callbacks run in this process under `inproc` and in the executor under
`remote`. Module state is visible only in the first case; `event_logs()` is
populated only in the second. So `Probe` writes to **both** and reads back from
whichever produced data:

```python
probe = Probe()                       # module level, next to the strategy

def on_bar(self, ctx, event):
    probe.bump("bar")                 # cheap per-event counter
    probe.sample("bar", close=event.data().close)   # first N payloads
    # probe.error("...")              # contract violation; never raise

def on_stop(self, ctx, event):
    probe.flush(ctx)                  # REQUIRED — one event-log write, not per-bar

data = probe.collect(run)             # after run
data.count("bar"); data.samples("bar"); data.errors; data.source
```

Counters accumulate in memory for free and flush **once**, so a per-bar probe
costs nothing. `data.source` reports which channel answered
(`memory` / `event_logs` / `empty`).

## The ladder

Each level assumes the ones below it pass. Filename prefix is the level;
`SURFACE` inside the file is the finer-grained id the coverage ledger keys on.

| Level | Asserts |
|---|---|
| `l0_env` | Credentials accepted on an **authenticated** route; profile actually took effect; every documented enum/config/context member exists in the installed package (catches spec-vs-code drift) |
| `l1_data` | Equity bars/trades/quotes; futures continuous + volume-continuous + raw contract; session windows per asset class; catalog-wide (dataset, schema) reachability and coverage accounting |
| `l2_callbacks` | Callback firing order; `event.type` per callback; ns timestamps; `ctx.now()`/`trading_day`; all asset classes routed to the right handler |
| `l3_timers` | Cadence, `timer_id` attribution, concurrent timers, `cancel_timer` |
| `l4_bars` | Real intervals only (`1s`/`1m`/`1d`); multi-interval on one symbol; successive `subscribe_bars` **accumulates** |
| `l5_customdata` | CSV `date`/`time`/`sym` contract, `column_data` + defaults, pipe-escaped commas, timestamp ordering |
| `l6_eventlogs` | `add_event_log` forms, `log_parameter_change`, and the engine-dependent `event_logs()` behaviour asserted as behaviour |
| `l7_metrics` | `net_pnl` recomputed from fills; `daily_return` is **percent**; equity-curve endpoints; risk ratios **null not zero** under the ≥10-active-day floor |
| `l8_oms` | Market fill via `on_order` (there is no `on_order_filled`); far limit rests; `cancel_all_orders`; `close_position` flattens; position events |
| `l9_livesim` | REST contract + validation guard rails; **backtest→livesim promotion**; param hot-update via `PATCH`; strict-creator ACL; teardown-guaranteed |

## Verdicts

```
RESULT: PASS|FAIL|GAP|SKIP <test-name> — <check>=ok; <check>=FAIL; <extra>
```

- **GAP** — the product or the data cannot answer yet (empty table, market
  closed, unentitled dataset). The run completed cleanly. Record it in
  `ledger/gaps.json` so the agent stops re-proposing it.
- **SKIP** — wrong engine/profile to ask. Not a failure and not a gap.
- **ERROR** — the process died. `install_crash_handler` converts an uncaught
  exception into an ERROR line carrying the cause, so this should always be
  diagnosable from the scorecard.

Only FAIL and ERROR gate the run. `quarantine/` never gates.

## Adding a test

Copy `agent/templates/backtest_test.py.tmpl` (or `livesim_test.py.tmpl`) into
`suites/l<N>_<topic>/l<N>_<nn>_<slug>.py`. Required:

- `probe.flush(ctx)` in `on_stop`
- `install_crash_handler(NAME, SURFACE)` first in `main()`
- `c.finish(NAME, surface=SURFACE, ...)`
- **no `hiveq_log_level`**, ever (`hiveq-flow/AGENTS.md`)
- never invent a dataset/schema name — use `FIXTURES.dataset_*`
- assert *relationships*, not magic counts, so a holiday does not fail the suite

Probe dates and symbols are data, in `fixtures.json` / `core/profiles.py`. When a
date goes stale, edit it there once rather than in every file.

## The agent

```bash
python agent_qa/agent/watch.py --summary     # what changed, which surfaces
```

`watch.py` is the deterministic half: it reads the per-repo commit watermark from
`ledger/commits.json`, diffs `hiveq` / `hiveq-flow` / `hiveq-sdk` / `sigma`,
classifies changed paths through `agent/surface_map.yaml`, and emits a ranked
work-list. Same commits always produce the same list.

The judgement half is the `qa-agent` Claude Code skill
(`.claude/skills/qa-agent/SKILL.md`): it reads the diffs and the relevant
`docs/llms.txt` sections, writes tests into `proposed/`, runs them, and emits a
dated report plus a review branch. It **never commits to `suites/`** — a human
promotes. A generated test that passes for the wrong reason would otherwise
become the spec.

The watermark advances only after a clean run, so a crash re-examines the same
commits rather than skipping them.

### What "self-learning" actually means here

Three ledgers under `ledger/`, all committed:

- `coverage.json` — which surfaces the suite really exercises, recorded from what
  *ran*, not from what exists on disk.
- `commits.json` — per-repo watermark.
- `gaps.json` — documented limitations. This is the part that makes the agent
  stop: a gap is knowledge, so the same impossible test is not re-proposed every
  night.

## Layout

```
core/       result (verdict protocol) · probe · profiles · guards · backtest
            catalog (live discovery + cache) · livesim_client · observe · ledger
suites/     the permanent, human-approved corpus
proposed/   agent output, awaiting review
quarantine/ known-flaky; runs but never gates
ledger/     coverage · commits · gaps · cached catalog
agent/      watch.py · surface_map.yaml · templates/
reports/    dated JSON run reports + agent markdown reports
fixtures/   CSV and other test data
```

## Relationship to `hiveq-flow/qa_validation`

`qa_validation` (t01–t12) is the in-process ancestor of this suite: same
`RESULT:` protocol, real data, no mocks, backtest only. Its properties are
carried into `l1`–`l8` here and widened to run under both engines, plus `l9`
livesim coverage it never had. Both can coexist; `qa_validation` remains the
fastest way to check the engine in isolation.
