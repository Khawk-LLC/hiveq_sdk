---
name: qa-agent
description: Grow the HiveQ QA suite from new commits. Diffs hiveq / hiveq-flow / hiveq-sdk / sigma since the last watermark, maps changed paths to QA surfaces, writes new tests into agent_qa/proposed/, runs them, and emits a dated report plus a review branch. Use when asked to run the QA agent, extend QA coverage from recent commits, or check what new behaviour is untested.
---

# QA agent

You extend `agent_qa/` — HiveQ's QA suite — in response to what actually changed
in the code. You do not rewrite the suite, you add to it.

**You never commit to `agent_qa/suites/`.** New tests go to `agent_qa/proposed/`
and a human promotes them. A generated test that passes for the wrong reason
becomes the spec if it lands in the permanent suite, and that is much worse than
a missing test.

## Steps

### 1. Get the work-list

```bash
cd ~/PycharmProjects/hiveq-sdk
python agent_qa/agent/watch.py --summary
```

This is deterministic — do not re-derive it by reading git yourself. It prints,
per repo, the commits since the last watermark, and a ranked work-list of QA
surfaces those commits touched. Each entry carries:

- `surface` — e.g. `l9.promotion`. The `l<N>` prefix is the suite level.
- `spec_sections` — which `docs/llms.txt` sections ground a test here.
- `files` / `commits` — the evidence.
- `covered_by` — existing tests. `[NEW]` means no coverage at all.
- `known_gaps` — **do not re-propose these.** They are documented product/data
  limitations, recorded in `agent_qa/ledger/gaps.json`.

Take the top 1–3 entries. Prefer `[NEW]` surfaces. Do not attempt the whole
list — a run that lands two good tests beats one that lands eight shallow ones.

### 2. Understand what changed

For each surface you picked:

- `git -C <repo> show <sha>` for the named commits. Read the actual diff. The
  commit *subject* is a hint; the diff is the specification.
- Read only the `spec_sections` from `hiveq-sdk/docs/llms.txt`. It is 1856
  lines — do not load it whole. Search for a line starting `## <N>.` to jump.
- Read the existing tests in `covered_by` so you extend rather than duplicate.

Then decide, explicitly: **what observable behaviour would differ if this commit
were reverted?** That sentence is the test. If you cannot write it, this surface
does not need a new test — say so and move on. This is the most common correct
outcome for refactor and logging commits, and reporting "nothing to test here"
is a real result, not a failure.

### 3. Write the test

Copy `agent_qa/agent/templates/backtest_test.py.tmpl` (or
`livesim_test.py.tmpl` for `l9.*`) into
`agent_qa/proposed/<surface-level>_<nn>_<slug>.py`.

Non-negotiables:

- **`probe.flush(ctx)` in `on_stop`** — without it there is no evidence channel
  on remote runs.
- **`install_crash_handler(NAME, SURFACE)`** first line of `main()`.
- **`c.finish(NAME, surface=SURFACE, ...)`** — the surface id is what keeps the
  coverage ledger and the work-list speaking the same language.
- **No `hiveq_log_level`** anywhere. Ever. (`hiveq-flow/AGENTS.md`.) Evidence
  goes through `add_event_log`, which works at the default `WARNING`.
- **Never invent a dataset or schema name.** Use `FIXTURES.dataset_*`, or check
  `agent_qa/ledger/catalog.local.json`. There is no `bars_5m` or `bars_1h`.
- **Absent data is a `GAP`, not a `FAIL`.** Use `guards.require_dataset` /
  `guards.require_market_open`, or pass `gap=True` to `finish`.
- **Assert relationships, not magic numbers.** "minute bars >> daily bars over
  the same window" survives a holiday; "exactly 390 bars" does not.
- Each check name should read as the property it defends
  (`equity_survives_futures_config`, not `check_3`).

### 4. Run it

```bash
python agent_qa/run_all.py --include-proposed --only 'proposed/*' --engine both
```

Then read the result honestly:

- **PASS on both engines** — good. Candidate for promotion.
- **PASS on one engine only** — usually correct and expected (livesim is
  remote-only; engine internals are inproc-only). Add the right `guards.require_*`
  so the other engine reports `SKIP` rather than failing.
- **FAIL** — decide which of two things it is, and say which:
  1. *the test is wrong* — fix it, re-run;
  2. *the product is wrong* — *keep the failing test*, and report it as a
     **finding**. This is the most valuable outcome the agent can produce. Do not
     weaken an assertion to make it green.
- **GAP** — add an entry to `ledger/gaps.json` via `ledger.record_gap(...)` with
  the reason and the commit, so future runs stop re-proposing it.

If the whole suite fails on auth (`l0_01` FAILs with a 401), stop. Report that
the environment needs a fresh API key and propose nothing — every verdict below
`l0` is meaningless until that is fixed.

### 5. Report

Write `agent_qa/reports/<YYYY-MM-DD>-qa-agent.md`:

- commits digest per repo (sha + subject)
- surfaces touched, and which you picked, and **why you skipped the rest**
- each test proposed: what it asserts, why that assertion, its result
- findings — product bugs the new tests caught, with the failing check name
- gaps recorded
- coverage delta

Then create the review branch:

```bash
git -C ~/PycharmProjects/hiveq-sdk checkout -b qa/agent-$(date +%Y-%m-%d)
git -C ~/PycharmProjects/hiveq-sdk add agent_qa/proposed agent_qa/reports agent_qa/ledger
git -C ~/PycharmProjects/hiveq-sdk commit -m "qa(agent): propose tests for <surfaces>"
```

### 6. Advance the watermark — last, and only if step 4 and 5 succeeded

```bash
python agent_qa/agent/watch.py --advance
```

If anything went wrong, **do not advance**. The same commits will be re-examined
next run, which is the intended failure mode: re-doing work is cheap, silently
skipping a commit is not.

## What good looks like

One run, three surfaces examined, one test proposed and passing, one surface
correctly declined as untestable, one product finding reported. That is a better
outcome than eight generated files.
