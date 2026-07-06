# HiveQ Platform — End-to-End User Journey

This document walks through the full HiveQ platform experience a quant or a
quant-team developer goes through, from a clean machine to a scheduled,
production signal pipeline and (optionally) a native C++ HFT strategy. Every
command below was actually run and verified on a clean test machine
(`192.168.0.164`, Ubuntu/Linux Mint, Python 3.12) on 2026-07-04. Where a step
uncovered a real platform issue, that's called out explicitly rather than
glossed over — this doc is meant to be reproducible, not aspirational.

---

## 0. Prerequisites

- Python 3.11+ (a conda environment is recommended — see the note in §2).
- Docker, only if you'll also do the native C++ HFT path (§8).
- A HiveQ account (sign-in happens automatically the first time you need it).

---

## 1. Install the HiveQ SDK

The SDK (`hiveq-sdk`) is public: <https://github.com/Khawk-LLC/hiveq_sdk>. It
is not yet on PyPI, so install it from source:

```bash
git clone https://github.com/Khawk-LLC/hiveq_sdk.git
cd hiveq_sdk
python -m venv .venv && source .venv/bin/activate    # or a conda env
pip install build
bash scripts/install.sh
```

Verify:

```bash
python -c "import hiveq.flow as hf; print('hiveq.flow OK ->', hf.__file__)"
```

## 2. Log in

```bash
hiveq login
```

This opens a browser sign-in and writes your API key to `~/.hiveq/.env`
(`HIVEQ_API_KEY=...`) automatically — no copying a key by hand. Every other
command in this doc picks the key up from that file. `hiveq docs` prints the
path to the docs bundled inside the installed wheel; `hiveq datasets` lists
what's available to your account.

> **Environment note — resolved 2026-07-06.** On the test machine, `run_backtest`
> deployed from a fresh `venv` used to reliably fail with a `CodeArtifact 401`
> pulling `hiveq-flow` on the executor container, while the same script
> succeeded from a different, pre-existing conda environment on the same box
> with the same API key. The fix landed in `hiveq-sdk` (`requirements=['hiveq-flow']`
> → `requirements=[]` in `_deploy()`) paired with a `staging.hiveq.ai` release.
> Reinstalling the rebuilt wheel into the previously-failing venv and retrying
> confirmed it: 3/3 successful runs, real PnL, no environment workaround needed
> anymore.

## 3. Point Claude Code (or any AI assistant) at the docs to author a strategy

```bash
hiveq docs
# Docs: /path/to/venv/lib/python3.X/site-packages/hiveq/docs
```

Copy that `docs/` folder next to your project (or point your assistant's
working directory at it), then just ask for a strategy in plain language. On
the test machine, this literal prompt —

> "Read the HiveQ SDK docs and, using only what they teach, write a simple
> execution strategy: a single-symbol AAPL intraday momentum strategy that
> buys when price crosses above its short moving average and flattens when
> it crosses back below, deployed as a backtest."

— produced a correct, idiomatic strategy on the first try (one import fixed:
`AssetType` lives in `hiveq.flow.config`, not `hiveq.flow`). Running it
produced a real backtest with a full performance report:

```python
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
# ... (see examples/ in the SDK repo for the full pattern)

run = hf.run_backtest(
    strategy_configs=[StrategyConfig(name="SimpleMomentum", type="SimpleMomentum")],
    symbols=["AAPL"],
    start_date="2025-08-01",
    end_date="2025-08-06",
    data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
)
print(run.report().return_stats.to_string())
```

Result: 68 trades over the week, full return stats printed.

## 4. Install the HiveQ Data Driver

The **data driver** (`hiveq.driver`, package `HiveQDataDriver`,
<https://github.com/Khawk-LLC/hiveq_data_driver>, private) is what a quant
uses to pull historical and realtime data locally for research/signal
generation — distinct from the thin `hiveq-sdk` client, which only
deploys/observes runs. It depends on the private `hiveq-data` SDK
(<https://github.com/Khawk-LLC/hiveq-data-sdk>).

```bash
git clone git@github.com:Khawk-LLC/hiveq-data-sdk.git
git clone git@github.com:Khawk-LLC/hiveq_data_driver.git
pip install -e hiveq-data-sdk
pip install -e "hiveq_data_driver[hiveq]"
```

> **Dependency-pin finding (in progress / fixed by repo owner).** A fresh
> install of `hiveq-data-sdk` at the time of this test pulled
> `kafka-python==3.0.7` (its `pyproject.toml`/`requirements.txt` pin was
> unbounded, `kafka-python>=2.0.2`). That version removed
> `NoBrokersAvailable` from `kafka.errors`, breaking `import hiveq_data`
> entirely — reproduced in a brand-new throwaway venv on an unrelated
> machine, so it isn't specific to this test box. Pin `kafka-python<3` as a
> workaround if you hit this before the upstream fix lands.

Verify with a real pull:

```python
import hiveq.driver as dd
# credentials load from $HOME/.hiveq/.env automatically via the `hiveq.load()`
# facade; if you construct `Driver(...)` directly instead, export
# HIVEQ_API_KEY into the shell yourself first.
```

On the test machine this pulled 390 real rows of AAPL 1-minute bars for a
known trading day — confirmed working end to end.

## 5. Generate a historical signal (the user-journey test: one week)

Pull a week of bars and compute a signal:

```python
import hiveq.driver as dd
from hiveq import Cache
from hiveq.datetime import DateRange, TimeRange
# ... dd.load('AaplWeekBars', params_tuple=..., cache=Cache.NO_CACHE)
```

On the test machine this fetched 2,340 AAPL 1-minute bars across
2025-08-01→2025-08-08 (6 trading days) and computed a simple SMA-crossover
signal, producing 293 signal rows in the shape the platform's custom-data
CSV format expects:

```csv
date,time,sym,signal,weight,action
2025-08-01,10:09:00,AAPL,0.0013,0.13,BUY_SIGNAL
2025-08-01,10:12:00,AAPL,-0.0,0.0,SELL_SIGNAL
...
```

The `time` column **must** be plain `HH:MM:SS` (no full datetime, no
milliseconds) — the engine's parser is strict about this.

## 6. Upload the signal with `hiveq-data`

```bash
hiveq-data -u signals/                 # uploads the whole directory, incremental (MD5 diff)
hiveq-data -l signals                  # verify what's on the platform
```

> **Path-anchoring gotcha.** `hiveq-data -u path/to/file.csv` (a single file,
> not a directory) anchors to the file's *own parent directory*, so any
> leading folder in the path you gave it is stripped from the stored name.
> Upload the *containing directory* if you want that prefix preserved —
> `hiveq-data -u signals/` stores files as `signals/<name>`, but
> `hiveq-data -u signals/foo.csv` stores it as just `foo.csv` at the root.

> **Real platform finding — read this before wiring up a custom-data
> strategy.** The documented pattern (§9.2 of the API reference) says to
> reference an uploaded CSV in `data_configs` by its plain **relative** path
> (matching what you uploaded). As of this test, that does **not** work on
> `staging.hiveq.ai` — `on_custom_data` fires **zero times**, reproduced with
> three independent files including the SDK's own bundled example fixture.
> The fix: use the **full absolute persistent-data path**
> (`/home/hivequser/hiveq/persistent_data/<path>`) **combined with** the
> `_yyyymmdd.csv` daily-file-naming pattern — both together. Neither the
> relative path nor the full path alone (with a single non-pattern file)
> worked; only the combination did. Concretely:
>
> ```python
> # Split your signal into one file per trading day, e.g.:
> #   signals/my_signal_20250801.csv
> #   signals/my_signal_20250804.csv
> #   ...
> # Upload the whole directory:
> #   hiveq-data -u signals/
> # Then in data_configs:
> SIGNAL_PATH = "/home/hivequser/hiveq/persistent_data/signals/my_signal_yyyymmdd.csv"
> data_configs = [
>     {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]},
>     {"type": "csv", "data_type": "custom", "id": "my_signal", "path": SIGNAL_PATH},
> ]
> ```
>
> This is a documentation gap, not a broken feature — file a doc fix
> separately, but use the full-path + daily-pattern form until it's updated.

## 7. Write a simple execution strategy that consumes the signal (via prompt)

Same AI-assisted flow as §3, this time asking for a strategy that subscribes
to the uploaded signal and trades off its `action` column
(`BUY_SIGNAL`/`STRONG_BUY` → go long if flat; `SELL_SIGNAL`/`WEAK_SELL` →
flatten if long). After applying the full-path + daily-pattern fix from §6,
this produced a real, profitable-in-backtest result on the test machine:
**144 trades, +$5,971.71 net PnL, 95% win rate** over the same week's data.

```python
class SignalConsumer:
    def on_start(self, ctx, event):
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_data(data_id="weekly_signal")

    def on_custom_data(self, ctx, event):
        data = event.data()
        action = data.column_data("action", default="").strip().upper()
        if action in ("BUY_SIGNAL", "STRONG_BUY") and ctx.is_flat("AAPL"):
            ctx.buy_order("AAPL", quantity=100)
        elif action in ("SELL_SIGNAL", "WEAK_SELL") and ctx.net_position("AAPL") > 0:
            ctx.close_position("AAPL")
```

## 8. Deploy the signal-generation code to the platform, on a schedule

Use `deploy_job` for a plain fetch/compute/publish script (not a
`hiveq.flow` strategy) — this is the mechanism a quant or a quant-team dev
uses to put their signal-generation code into production, either as a
one-off validation run or on a recurring schedule:

```python
import hiveq.flow as hf
from hiveq.flow import Schedule, ScheduleFrequency

def generate_signal():
    from hiveq import dd
    df = dd.load(dataset="HIVEQ_US_EQ", schema="bars_1d", symbols=["AAPL"])
    dd.save(df, schema="quant_features", key="my_signal")
    return {"rows": len(df)}

# One-off, to validate under real platform conditions before scheduling:
job = hf.deploy_job(generate_signal, task_name="daily-signal-oneoff", wait=True)
job.result()

# Recurring — the platform runs this on its own, no cron/host needed from you:
job = hf.deploy_job(
    generate_signal,
    task_name="daily-signal-scheduled",
    schedule=Schedule(frequency=ScheduleFrequency.DAILY, start_time="16:05",
                      timezone="US/Eastern"),
)
job.status()   # -> {'status': 'scheduled', ...}
```

This gives the quant/dev team **full deploy, schedule, and monitoring
access**: `job.status()`, `job.logs()`, `job.result()` to observe a run, and
`job.terminate()` to cancel a schedule.

> **Two platform findings from this test.** (1) The `QUANT_SCRIPTS` sandbox
> executor's bundled `hiveq` package has **no `dd` module** — `from hiveq
> import dd` inside a deployed job fails with `ImportError`, even though it's
> the exact documented pattern; this executor image appears to be a
> different/older build than the one used for `run_backtest`. (2)
> `Job.terminate()` currently 404s (`POST /api/orchestrator/terminate` not
> found) — there is presently no way to cancel a registered schedule from
> the client. Both flagged for the platform team; schedule registration
> itself (`Schedule(...)` → `status: scheduled`) does work.

## 9. (Optional) Native C++ HFT path

For users who need native/compiled strategies instead of Python — and who
have been granted access to the private, trusted `hiveq_sdk_hft` package
(<https://github.com/Khawk-LLC/hiveq_sdk_hft>) plus scoped ECR pull access:

```bash
python -m pip install hiveq_sdk_hft-<version>-py3-none-any.whl
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com
export HIVEQ_API_KEY="your_api_key_here"
```

Write (or use a canned) strategy — the SDK ships three ready-made examples:
`minimal_strategy` (a trade/TBBO logger — the simplest possible plugin),
`source_routing_strategy` (bars + ticks with config-driven data-source
routing), and `depth_ladder_strategy` (a real multi-layer market-making
strategy that places and manages resting orders). A strategy directory needs
just three files: `CMakeLists.txt`, `MyStrategy.cpp`, `sigma_strategy.json`.

```bash
cd my_strategy/
hiveq-hft build       # builds inside the version-matched ECR builder image
hiveq-hft validate --plugin build/libMyStrategy.so --strategy-config sigma_strategy.json
hiveq-hft run-backtest \
  --plugin build/libMyStrategy.so \
  --strategy-config sigma_strategy.json \
  --run-config run.backtest.yaml \
  --start-date 2025-09-15 --end-date 2025-09-15
```

Verified on the test machine: `hiveq-hft build` compiled `minimal_strategy`
cleanly inside the cached `hiveq-sigma-native-builder:0.3.6` image, and
`hiveq-hft validate` passed ABI checks.

> **Real finding — native backtest execution hung.** Submitting
> `minimal_strategy` as a native backtest (`ES.c.0`, 2025-09-15,
> `fut_trades`) succeeded (`Task deployed successfully`, a real `run_id`),
> but the run then sat at `status: RUNNING`, `elapsed_ms: 0`,
> `current_day: 0/1` with **zero progress** for over 20 minutes — this is a
> hang, not just a slow tick-level backtest. Separately, calling
> `run.logs()` on that native `run_id` returned the raw HTML of the HiveQ
> web app's `index.html` instead of any log content — the logs endpoint for
> native backtests appears misrouted. The CLI's own `hiveq-hft logs`
> subcommand only supports LiveSim, not backtests, so there is currently
> **no working way to inspect a native backtest's progress or logs** at
> all. `build` and `validate` both work correctly; `run-backtest` execution
> itself does not currently complete. Flagged for the platform team.
>
> **Retested 2026-07-06** after a `staging.hiveq.ai` release and a fresh
> `hiveq-sdk` wheel — this specific hang is unaffected (a new attempt,
> `run_id=33dc0cc3-270c-4f0f-a8f8-c83e9d13f603`, reproduced the identical
> symptom). That release fixed a separate, unrelated issue (the
> `run_backtest` CodeArtifact 401, §2) — the native path needs its own fix.

For a platform-provided canned native strategy (e.g. `POVUserSignalStrategy`)
you don't need to build or upload anything — omit `--plugin` and provide only
the two config files; the executor uses the strategy already compiled into
its image.

---

## Summary of platform findings from this test pass (2026-07-04)

| # | Area | Finding | Status |
|---|---|---|---|
| 1 | `run_backtest` executor | CodeArtifact 401 pulling `hiveq-flow`, reproducible in a fresh venv but not a pre-existing conda env on the same box | **Fixed** 2026-07-06 (commit 763f9f6 + staging release); verified 3/3 |
| 2 | `hiveq-data-sdk` deps | Unbounded `kafka-python>=2.0.2` pulls incompatible 3.0.7 | Being fixed by repo owner |
| 3 | Custom CSV `data_configs` | Relative-path (documented) delivers zero rows; full path + `_yyyymmdd.csv` pattern together works | Re-verified 2026-07-06, still needs the full-path workaround — docs need a fix |
| 4 | `QUANT_SCRIPTS` sandbox | `from hiveq import dd` fails — module missing in that executor image | Re-verified 2026-07-06, still broken — flagged for platform team |
| 5 | `Job.terminate()` | 404s — no way to cancel a registered schedule from the client | Re-verified 2026-07-06, still broken. **Two schedules are now stuck on staging** (`weekly-signal-scheduled`, task IDs `4636a8d7-ce54-4b3e-b2b8-008066916eaf` and `c1bd5c10-ed56-492e-9659-ae78fe7bf294`) and will keep firing and failing daily at 16:05 ET until removed platform-side |
| 6 | Native (`hiveq-hft`) backtest | `run-backtest` submits fine, but execution hangs (`RUNNING`, `elapsed_ms=0` for 20+ min); `run.logs()` on a native run returns the web app's HTML, not logs; CLI `logs` subcommand only supports LiveSim | Retested 2026-07-06 after the staging release — **still hangs**, identical symptom (new `run_id=33dc0cc3-270c-4f0f-a8f8-c83e9d13f603`); this release did not touch the native path |
