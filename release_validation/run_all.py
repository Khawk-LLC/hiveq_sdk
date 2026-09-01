"""Run the installed-wheel baseline suite or the gated full validation."""

import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import html
import json
import os
import re
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
MAX_CONCURRENT_RETRIES = 30
RETRY_DELAY_SECONDS = 10
BETWEEN_TEST_DELAY_SECONDS = 2
DEFAULT_TEST_TIMEOUT_SECONDS = 14400
LONG_TEST_TIMEOUT_SECONDS = 14400
BASELINE_PREFIX = "baseline_"
LONG_RUNNING_PREFIX = "long_running_"
SUITE_CONCURRENCY = 2
# Remote platform submissions: a timeout means the client gave up, not that the
# run stopped -- an abandoned run can still be active server-side, so the suite
# halts rather than submitting another strategy on top of it. Set to True only
# for in-memory (is_local) runs, which are process-scoped and die with their
# process, where halting the whole suite over one hung test just yields a
# scorecard covering a prefix of the tests while reading like a full result.
LOCAL_RUNS = True

# Set RELEASE_VALIDATION_CONTINUE_ON_FAIL=1 to score every validation in one
# pass instead of halting at the first red row. Default behaviour is unchanged:
# the suite stops on FAIL/ERROR/BLOCKED so a broken platform is not hammered.
CONTINUE_ON_FAIL = os.environ.get("RELEASE_VALIDATION_CONTINUE_ON_FAIL") == "1"
REPORTS_DIR = HERE / "reports"
ARTIFACTS_DIR = HERE / "run_artifacts"
VALIDATION_ENV = HERE / ".env"


def validation_environment(export_orders: bool) -> dict[str, str]:
    """Return the process environment with release-validation values applied."""
    env = dict(os.environ)
    for raw_line in (
        VALIDATION_ENV.read_text(encoding="utf-8").splitlines()
        if VALIDATION_ENV.exists() else []
    ):
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("HIVEQ_"):
            env[key] = value.strip().strip('"').strip("'")
    # qa_common applies this to every BacktestConfig constructed by a
    # validation. Local (in-memory) result readers require the streamed order
    # CSV, so --export-orders is what makes run.orders()/run.fills() readable;
    # remote submissions publish orders from C++ and never write the file.
    if export_orders:
        env["RELEASE_VALIDATION_EXPORT_ORDERS_CSV"] = "1"
    else:
        env.pop("RELEASE_VALIDATION_EXPORT_ORDERS_CSV", None)
    return env


def artifact_snapshot() -> dict[Path, int]:
    return {
        path.parent: path.stat().st_mtime_ns
        for path in ARTIFACTS_DIR.glob("*/validation.json")
    }


def changed_artifacts(before: dict[Path, int]) -> list[Path]:
    after = artifact_snapshot()
    return sorted(
        path for path, modified in after.items()
        if path not in before or before[path] != modified
    )


def _relative_link(report: Path, target: Path, label: str) -> str:
    if not target.exists():
        return f'<span class="missing">{html.escape(label)} missing</span>'
    href = os.path.relpath(target, report.parent)
    return f'<a href="{html.escape(href, quote=True)}">{html.escape(label)}</a>'


def test_phase(test: Path) -> str:
    if test.name.startswith(BASELINE_PREFIX):
        return "baseline"
    if test.name.startswith(LONG_RUNNING_PREFIX):
        return "long-running"
    raise ValueError(f"validation has no suite prefix: {test.name}")


def test_number(test: Path) -> int:
    match = re.match(r"(?:baseline|long_running)_t([0-9]{2})_", test.name)
    if not match:
        raise ValueError(f"invalid validation filename: {test.name}")
    return int(match.group(1))


def baseline_passed(rows: list[dict]) -> bool:
    baseline = [row for row in rows if row["phase"] == "baseline"]
    return bool(baseline) and not any(
        row["status"] in {"FAIL", "ERROR", "BLOCKED"} for row in baseline
    )


def write_html_report(rows: list[dict], report: Path, suite_id: str,
                      requested_suite: str, expected_baseline: int) -> None:
    counts = {
        status: sum(row["status"] == status for row in rows)
        for status in sorted({row["status"] for row in rows})
    }
    body = []
    for phase in ("baseline", "long-running"):
      phase_rows = [row for row in rows if row["phase"] == phase]
      if not phase_rows:
        continue
      body.append(
          f'<tr class="phase"><th colspan="5">{html.escape(phase.title())}</th></tr>'
      )
      for row in phase_rows:
        artifacts = []
        for artifact_dir in row["artifacts"]:
            validation_path = artifact_dir / "validation.json"
            metadata = {}
            if validation_path.exists():
                try:
                    metadata = json.loads(validation_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    metadata = {}
            run_id = str(metadata.get("run_id") or artifact_dir.name)
            table_rows = metadata.get("table_rows") or {}
            links = " · ".join([
                _relative_link(report, validation_path, "validation"),
                _relative_link(report, artifact_dir / "orders.csv", f'orders ({table_rows.get("orders", "?")})'),
                _relative_link(report, artifact_dir / "trades.csv", f'trades ({table_rows.get("trades", "?")})'),
                _relative_link(report, artifact_dir / "positions.csv", f'positions ({table_rows.get("positions", "?")})'),
                _relative_link(report, artifact_dir / "event_logs.csv", f'event logs ({table_rows.get("event_logs", "?")})'),
            ])
            artifacts.append(
                f'<div class="run"><code>{html.escape(run_id)}</code><br>{links}</div>'
            )
        artifact_html = "".join(artifacts) or (
            '<span class="na">not applicable (static audit; no backtest run)</span>'
            if row["test"] == "baseline_t00_sdk_convention.py"
            else '<span class="missing">no run artifacts exported</span>'
        )
        log_link = _relative_link(report, row["log"], "runner output")
        body.append(
            "<tr>"
            f'<td><span class="status {html.escape(row["status"].lower())}">{html.escape(row["status"])}</span></td>'
            f'<td><code>{html.escape(row["test"])}</code></td>'
            f'<td>{row["duration"]:.1f}s</td>'
            f'<td>{artifact_html}</td>'
            f'<td>{log_link}<div class="detail">{html.escape(row["line"])}</div></td>'
            "</tr>"
        )
    baseline_rows = [row for row in rows if row["phase"] == "baseline"]
    long_rows = [row for row in rows if row["phase"] == "long-running"]
    gate_passed = baseline_passed(rows)
    gate_failed = any(
        row["status"] in {"FAIL", "ERROR", "BLOCKED"} for row in baseline_rows
    )
    if gate_failed:
        gate_text = "CLOSED — baseline failed; remaining validations were stopped"
        gate_class = "closed"
    elif len(baseline_rows) < expected_baseline:
        gate_text = "PENDING — baseline is still running"
        gate_class = "neutral"
    elif gate_passed:
        gate_text = (
            "OPEN — baseline passed; long-running validations allowed"
            if requested_suite == "all"
            else "PASSED — baseline complete; long-running validations not requested"
        )
        gate_class = "open"
    else:
        gate_text = "CLOSED — baseline did not pass; long-running validations not run"
        gate_class = "closed"
    document = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HiveQ release validation {html.escape(suite_id)}</title>
<style>
body{{font:14px system-ui,sans-serif;margin:24px;color:#17202a;background:#f7f9fb}}
h1{{margin-bottom:4px}} .summary{{margin:0 0 20px;color:#52606d}}
table{{width:100%;border-collapse:collapse;background:white;box-shadow:0 1px 4px #ccd3da}}
th,td{{padding:10px;border-bottom:1px solid #e4e7eb;text-align:left;vertical-align:top}}
th{{background:#edf2f7;position:sticky;top:0}} a{{color:#0757a6}}
.status{{font-weight:700}} .pass{{color:#18733c}} .fail,.error{{color:#b42318}}
.gap,.blocked,.skipped{{color:#9a6700}} .missing{{color:#b42318}} .run{{margin-bottom:8px}}
.detail{{max-width:680px;margin-top:5px;color:#52606d;font-size:12px;overflow-wrap:anywhere}}
.cards{{display:flex;gap:12px;margin:16px 0 20px;flex-wrap:wrap}} .card{{background:white;border:1px solid #d8dee4;border-radius:6px;padding:10px 14px}}
.gate.open{{border-left:5px solid #18733c}} .gate.closed{{border-left:5px solid #b42318}} .gate.neutral{{border-left:5px solid #52606d}}
.phase th{{position:static;background:#dce6f0;font-size:15px}}
code{{font-size:12px}}
</style></head><body>
<h1>HiveQ release validation</h1>
<p class="summary">Suite {html.escape(suite_id)} · mode {html.escape(requested_suite)} · {html.escape(str(counts))} · generated {html.escape(datetime.now().astimezone().isoformat(timespec="seconds"))}</p>
<div class="cards"><div class="card"><strong>Baseline</strong><br>{len(baseline_rows)} / {expected_baseline} tests</div>
<div class="card"><strong>Long-running</strong><br>{len(long_rows)} tests</div>
<div class="card gate {gate_class}"><strong>Long-running gate</strong><br>{html.escape(gate_text)}</div></div>
<table><thead><tr><th>Status</th><th>Test</th><th>Duration</th><th>Run artifacts</th><th>Execution</th></tr></thead>
<tbody>{''.join(body)}</tbody></table></body></html>"""
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(document, encoding="utf-8")
    (report.parent / "latest.html").write_text(document, encoding="utf-8")


def is_max_concurrent(output: str) -> bool:
    text = output.lower()
    return any(marker in text for marker in (
        "max concurrent", "maximum concurrent", "concurrency limit",
        "too many concurrent", "too many active", "active run limit",
        "429 client error: too many requests",
    ))


def artifacts_for_output(output: str, before: dict[Path, int]) -> list[Path]:
    """Associate artifacts with a concurrent test by run IDs in its output."""
    run_ids = set(re.findall(
        r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
        output,
    ))
    matched = sorted(
        artifact_dir for run_id in run_ids
        if (artifact_dir := ARTIFACTS_DIR / run_id).is_dir()
    )
    return matched or changed_artifacts(before)


def run_test(test: Path, env: dict[str, str], log_dir: Path) -> dict:
    phase = test_phase(test)
    started = time.monotonic()
    artifacts_before = artifact_snapshot()
    line = ""
    output = ""
    unsafe_to_continue = False
    for attempt in range(1, MAX_CONCURRENT_RETRIES + 1):
        try:
            timeout = (LONG_TEST_TIMEOUT_SECONDS if phase == "long-running"
                       else DEFAULT_TEST_TIMEOUT_SECONDS)
            proc = subprocess.run(
                [sys.executable, str(test)], cwd=str(PROJECT_ROOT), env=env,
                capture_output=True, text=True, timeout=timeout,
            )
            output = (proc.stdout or "") + (proc.stderr or "")
            if is_max_concurrent(output):
                print(
                    f"[WAIT ] {test.name}: platform concurrency limit "
                    f"(attempt {attempt}/{MAX_CONCURRENT_RETRIES})",
                    flush=True,
                )
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            result = [x for x in output.splitlines() if x.startswith("RESULT:")]
            if result:
                line = result[-1]
            elif proc.returncode == 0:
                line = (
                    f"RESULT: ERROR {test.stem} — exited 0 without a RESULT line; "
                    "finish() was never reached, so nothing was asserted"
                )
            elif (
                "Operation not permitted" in output
                and ("Failed to deploy task" in output or "/api/orchestrator/submit" in output)
            ):
                line = (
                    f"RESULT: BLOCKED {test.name} — remote platform connection denied "
                    "by the execution sandbox before submission"
                )
            else:
                tail = next(
                    (x.strip() for x in reversed(output.splitlines()) if x.strip()),
                    "no output",
                )
                line = f"RESULT: ERROR {test.name} — rc={proc.returncode}; tail={tail[:300]}"
            break
        except subprocess.TimeoutExpired as exc:
            output = (exc.stdout or "") + (exc.stderr or "")
            line = (
                f"RESULT: ERROR {test.name} — timeout after {timeout}s; "
                "continuing with the next validation"
            )
            unsafe_to_continue = not LOCAL_RUNS
            break
    if not line:
        line = (
            f"RESULT: ERROR {test.name} — platform concurrency limit persisted "
            f"after {MAX_CONCURRENT_RETRIES} retries"
        )
        unsafe_to_continue = True
    status = line.split()[1] if len(line.split()) > 1 else "ERROR"
    duration = time.monotonic() - started
    log_path = log_dir / f"{test.stem}.log"
    log_path.write_text(output or line, encoding="utf-8")
    return {
        "status": status, "line": line, "test": test.name,
        "duration": duration, "artifacts": artifacts_for_output(output, artifacts_before),
        "log": log_path, "unsafe_to_continue": unsafe_to_continue,
    }


def run_phase(tests: list[Path], rows: list[dict], env: dict[str, str],
              log_dir: Path, report: Path, suite_id: str,
              requested_suite: str, expected_baseline: int) -> bool:
    """Run one phase completely; return whether submissions remain safe."""
    pending: dict[Future, Path] = {}
    stop_submitting = False
    with ThreadPoolExecutor(max_workers=SUITE_CONCURRENCY) as executor:
        for test in tests[:SUITE_CONCURRENCY]:
            pending[executor.submit(run_test, test, env, log_dir)] = test
        next_test = iter(tests[SUITE_CONCURRENCY:])
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                test = pending.pop(future)
                row = future.result()
                unsafe_to_continue = row.pop("unsafe_to_continue")
                row["phase"] = test_phase(test)
                rows.append(row)
                rows.sort(key=lambda item: item["test"])
                write_html_report(
                    rows, report, suite_id, requested_suite, expected_baseline
                )
                print(f"[{row['status']:5s}] {row['test']} ({row['duration']:.0f}s)", flush=True)
                if unsafe_to_continue and not CONTINUE_ON_FAIL:
                    stop_submitting = True
                    print("Stopping new submissions; active-run state is not proven safe.", flush=True)
                if row["status"] in {"FAIL", "ERROR", "BLOCKED"} and not CONTINUE_ON_FAIL:
                    stop_submitting = True
                    print(
                        f"Stopping new submissions; {test.name} returned {row['status']}.",
                        flush=True,
                    )
                if not stop_submitting:
                    try:
                        next_item = next(next_test)
                    except StopIteration:
                        pass
                    else:
                        time.sleep(BETWEEN_TEST_DELAY_SECONDS)
                        pending[executor.submit(run_test, next_item, env, log_dir)] = next_item
    return not stop_submitting


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suite", choices=("baseline", "all"), default="all",
        help="run the quick baseline only, or baseline then gated long-running tests",
    )
    parser.add_argument(
        "--export-orders", dest="export_orders", action="store_true",
        default=LOCAL_RUNS,
        help="stream every order event to a local CSV so in-memory runs can "
             "read back orders/fills (required for LOCAL_RUNS validations)",
    )
    parser.add_argument(
        "--no-export-orders", dest="export_orders", action="store_false",
        help="disable local order-event capture",
    )
    args = parser.parse_args()
    env = validation_environment(args.export_orders)
    start_at = int(env.get("RELEASE_VALIDATION_START", "0"))
    skipped = {
        int(item.strip()) for item in env.get("RELEASE_VALIDATION_SKIP", "").split(",")
        if item.strip().isdigit()
    }
    # The SdkTxx convention audit is t00, so it is collected like any other
    # validation and runs first by number — no separate pre-step.
    rows: list[dict] = []
    suite_id = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    report = REPORTS_DIR / f"release-validation-{suite_id}.html"
    log_dir = REPORTS_DIR / "logs" / suite_id
    log_dir.mkdir(parents=True, exist_ok=True)
    tests = []
    candidates = [
        *HERE.glob("baseline_t[0-9][0-9]_*.py"),
        *HERE.glob("long_running_t[0-9][0-9]_*.py"),
    ]
    for test in sorted(candidates, key=lambda path: test_number(path)):
        number = test_number(test)
        if number < start_at:
            continue
        if number in skipped:
            print(f"[SKIP ] {test.name} (RELEASE_VALIDATION_SKIP)")
            continue
        tests.append(test)

    baseline_tests = [test for test in tests if test_phase(test) == "baseline"]
    long_tests = [test for test in tests if test_phase(test) == "long-running"]
    safe = run_phase(
        baseline_tests, rows, env, log_dir, report, suite_id, args.suite,
        len(baseline_tests),
    )
    gate_open = safe and baseline_passed(rows)
    if args.suite == "all" and gate_open:
        print("Baseline passed; starting long-running validations.", flush=True)
        run_phase(
            long_tests, rows, env, log_dir, report, suite_id, args.suite,
            len(baseline_tests),
        )
    elif args.suite == "all":
        for test in long_tests:
            rows.append({
                "status": "SKIPPED", "line": "Baseline gate closed; not run",
                "test": test.name, "duration": 0.0, "artifacts": [],
                "log": log_dir / f"{test.stem}.log", "phase": "long-running",
            })
        rows.sort(key=lambda item: item["test"])
        write_html_report(rows, report, suite_id, args.suite, len(baseline_tests))
    else:
        write_html_report(rows, report, suite_id, args.suite, len(baseline_tests))
    print("\n" + "=" * 78)
    print("HIVEQ SDK INSTALLED-WHEEL RELEASE SCORECARD")
    print("=" * 78)
    for row in rows:
        print(row["line"])
    counts = {status: sum(row["status"] == status for row in rows)
              for status in sorted({row["status"] for row in rows})}
    print(f"SUMMARY: {counts}")
    print(f"HTML REPORT: {report}")
    if any(row["status"] in {"FAIL", "ERROR"} for row in rows):
        return 1
    return 2 if any(row["status"] == "BLOCKED" for row in rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
