"""Run all installed-wheel release validations; use only after individual review."""

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
LONG_TEST_NUMBERS = {45, *range(48, 59)}
SUITE_CONCURRENCY = 2
# Remote platform submissions: a timeout means the client gave up, not that the
# run stopped -- an abandoned run can still be active server-side, so the suite
# halts rather than submitting another strategy on top of it. Set to True only
# for in-memory (is_local) runs, which are process-scoped and die with their
# process, where halting the whole suite over one hung test just yields a
# scorecard covering a prefix of the tests while reading like a full result.
LOCAL_RUNS = False
REPORTS_DIR = HERE / "reports"
ARTIFACTS_DIR = HERE / "run_artifacts"


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


def write_html_report(rows: list[dict], report: Path, suite_id: str) -> None:
    counts = {
        status: sum(row["status"] == status for row in rows)
        for status in sorted({row["status"] for row in rows})
    }
    body = []
    for row in rows:
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
            if row["test"] == "t00_sdk_convention.py"
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
.gap,.blocked{{color:#9a6700}} .missing{{color:#b42318}} .run{{margin-bottom:8px}}
.detail{{max-width:680px;margin-top:5px;color:#52606d;font-size:12px;overflow-wrap:anywhere}}
code{{font-size:12px}}
</style></head><body>
<h1>HiveQ release validation</h1>
<p class="summary">Suite {html.escape(suite_id)} · {html.escape(str(counts))} · generated {html.escape(datetime.now().astimezone().isoformat(timespec="seconds"))}</p>
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
    number = int(test.name[1:3])
    started = time.monotonic()
    artifacts_before = artifact_snapshot()
    line = ""
    output = ""
    unsafe_to_continue = False
    for attempt in range(1, MAX_CONCURRENT_RETRIES + 1):
        try:
            timeout = (LONG_TEST_TIMEOUT_SECONDS if number in LONG_TEST_NUMBERS
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


def main() -> int:
    env = dict(os.environ)
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
    for test in sorted(HERE.glob("t[0-9][0-9]_*.py")):
        number = int(test.name[1:3])
        if number < start_at:
            continue
        if number in skipped:
            print(f"[SKIP ] {test.name} (RELEASE_VALIDATION_SKIP)")
            continue
        tests.append(test)

    pending: dict[Future, Path] = {}
    stop_submitting = False
    with ThreadPoolExecutor(max_workers=SUITE_CONCURRENCY) as executor:
        for test in tests[:SUITE_CONCURRENCY]:
            pending[executor.submit(run_test, test, env, log_dir)] = test
        next_test = iter(tests[SUITE_CONCURRENCY:])
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                pending.pop(future)
                row = future.result()
                unsafe_to_continue = row.pop("unsafe_to_continue")
                rows.append(row)
                rows.sort(key=lambda item: item["test"])
                write_html_report(rows, report, suite_id)
                print(f"[{row['status']:5s}] {row['test']} ({row['duration']:.0f}s)", flush=True)
                if unsafe_to_continue:
                    stop_submitting = True
                    print(
                        "Stopping new submissions; active-run state is not proven safe.",
                        flush=True,
                    )
                if not stop_submitting:
                    try:
                        test = next(next_test)
                    except StopIteration:
                        pass
                    else:
                        time.sleep(BETWEEN_TEST_DELAY_SECONDS)
                        pending[executor.submit(run_test, test, env, log_dir)] = test
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
