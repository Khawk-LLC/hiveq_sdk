# HiveQ SDK release-validation conversion report

Generated: 2026-08-08 (America/New_York)

## Outcome

- Source corpus: **165** Python files under `hiveq-flow/examples/bt` only.
- Canonical, deduplicated release validations: **46** (`t01`–`t46`).
- Source accounting: **36 ported primary validations**, **108 semantic/example
  duplicates mapped to canonical gates**, **17 diagnostic-only or unsupported
  files**, and **4 shared helpers**.
- Unaccounted/pending source files: **0**.
- Every normal remote validation writes a structured `SDK_RELEASE_CHECKPOINT`
  event-log row. The intentional callback-crash case instead validates the raw
  `run.logs()` error record because a post-crash checkpoint is not reliable.
  Every case applies an explicit oracle to data, orders/trades/results, expected
  errors, or a documented data-availability GAP.

The file-by-file mapping, including every duplicate target and every exclusion
reason, is in `examples_bt_inventory.json`. It can be regenerated and checked
with:

```bash
python release_validation/classify_examples_bt.py
```

## Distinct coverage added after the original 19-test set

The expanded suite includes venue-specific early/per-exchange imbalances,
session/holiday/DST behavior, option filters and actual option fills, multi-leg
options, executor lifecycle, TCA, stream/Data API parity, cluster analytics,
TBBO trade+quote payloads, repeated futures rollover, STOP/STOP_LIMIT manual
brackets, run isolation, callback-error logs, parameter-change event logs,
custom CSV/hosted signals, order modification/open quantity, and public result
surfaces. See `README.md` for the complete `t01`–`t46` contract table.

## Verification performed

Live sequential platform results are recorded in `platform_results.md`.

### Static/local checks

```text
python -m compileall -q release_validation       PASS
pytest -q                                        19 passed
installed-wheel import isolation                 46/46 imported
inventory target/path audit                      165/165 accounted, 0 missing
```

The repository's **19 pytest tests are not the conversion count**. They are the
existing local unit suite. The converted release-validation count is 46.

### Full installed-wheel scorecard

Command executed from `/tmp`, with `PYTHONPATH` removed:

```bash
env -u PYTHONPATH python \
  /home/rmadanagopal/PycharmProjects/hiveq-sdk/release_validation/run_all.py
```

Result after the final all-platform normalization:

```text
PASS: 46 validations follow the SdkTxx convention; documented exceptions={}
SUMMARY: {'BLOCKED': 46}
functional FAIL: 0
```

The former local-only enum check and classless dispatch check are now `SdkT16`
and `SdkT23`. The suite contains **46 platform strategies** and no local-only
validation exception. Platform execution remains blocked by the same sandbox
restriction.

All remote cases were blocked **before task submission** by this execution
sandbox:

```text
HTTPConnectionPool(host='vm.hiveq.ai', port=80)
/api/orchestrator/submit
Failed to establish a new connection: [Errno 1] Operation not permitted
```

This is not evidence that the remote validations passed or failed. It means the
platform never received them. `run_all.py` now reports this condition as
`BLOCKED` rather than incorrectly labeling every case `ERROR`.

## How to complete remote confirmation

Run the scorecard command above from a shell allowed to reach `vm.hiveq.ai`.
The runner executes each validation in an isolated process against the installed
wheel and prints one `RESULT: PASS|FAIL|GAP|BLOCKED` line per case. A fully
confirmed release has no `FAIL`, `ERROR`, or `BLOCKED` rows; `GAP` is reserved
for explicitly optional/unpopulated datasets and includes checkpoint evidence.

## Diagnostic-only source files

The 17 exclusions are not silently dropped. Their reasons are recorded in the
inventory. They comprise non-deterministic performance/profile/OOM harnesses,
an external Backtrader comparison, undocumented internal diagnostics, and old
configuration examples for platform-internal `UserSignalStrategy` behavior
that conflicts with the public SDK's ET authoring contract.
