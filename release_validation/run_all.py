"""Run all installed-wheel release validations; use only after individual review."""

from pathlib import Path
import os
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
MAX_CONCURRENT_RETRIES = 30
RETRY_DELAY_SECONDS = 10
BETWEEN_TEST_DELAY_SECONDS = 2


def is_max_concurrent(output: str) -> bool:
    text = output.lower()
    return any(marker in text for marker in (
        "max concurrent", "maximum concurrent", "concurrency limit",
        "too many concurrent", "too many active", "active run limit",
    ))


def main() -> int:
    env = dict(os.environ)
    audit = subprocess.run(
        [sys.executable, str(HERE / "audit_sdk_convention.py")],
        cwd=str(PROJECT_ROOT), env=env, capture_output=True, text=True,
    )
    if audit.returncode:
        print(audit.stdout + audit.stderr)
        return 1
    print(audit.stdout.strip())
    rows = []
    for test in sorted(HERE.glob("t[0-9][0-9]_*.py")):
        started = time.monotonic()
        line = ""
        for attempt in range(1, MAX_CONCURRENT_RETRIES + 1):
            try:
                proc = subprocess.run(
                    [sys.executable, str(test)], cwd=str(PROJECT_ROOT), env=env,
                    capture_output=True, text=True, timeout=900,
                )
                output = (proc.stdout or "") + (proc.stderr or "")
                if is_max_concurrent(output):
                    print(
                        f"[WAIT ] {test.name}: platform concurrency limit "
                        f"(attempt {attempt}/{MAX_CONCURRENT_RETRIES})"
                    )
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                result = [x for x in output.splitlines() if x.startswith("RESULT:")]
                if result:
                    line = result[-1]
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
            except subprocess.TimeoutExpired:
                line = f"RESULT: ERROR {test.name} — timeout; stopping to avoid overlapping an active platform run"
                break
        if not line:
            line = (
                f"RESULT: ERROR {test.name} — platform concurrency limit persisted "
                f"after {MAX_CONCURRENT_RETRIES} retries"
            )
        status = line.split()[1] if len(line.split()) > 1 else "ERROR"
        rows.append((status, line))
        print(f"[{status:5s}] {test.name} ({time.monotonic() - started:.0f}s)")
        if status in {"ERROR", "BLOCKED"}:
            print("Stopping sequential runner; active-run state is not proven safe for the next submission.")
            break
        time.sleep(BETWEEN_TEST_DELAY_SECONDS)
    print("\n" + "=" * 78)
    print("HIVEQ SDK INSTALLED-WHEEL RELEASE SCORECARD")
    print("=" * 78)
    for _, line in rows:
        print(line)
    counts = {status: sum(row_status == status for row_status, _ in rows)
              for status in sorted({row_status for row_status, _ in rows})}
    print(f"SUMMARY: {counts}")
    if any(status in {"FAIL", "ERROR"} for status, _ in rows):
        return 1
    return 2 if any(status == "BLOCKED" for status, _ in rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
