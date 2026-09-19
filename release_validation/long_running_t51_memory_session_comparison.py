"""Run the 100-symbol memory probe twice, sequentially, with two sessions."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent


def main() -> None:
    # Explicit per-probe deadline (t58 does the same). The impl default is 4h,
    # which the full-session probe outgrew: it was still on day 4 of 5 when the
    # client abandoned an otherwise healthy run. 6h was not enough either --
    # on 2026-09-16 the full-session probe was on day 5 of 5 when the deadline
    # hit, which aborted the whole test before the windowed session ran at all.
    # 8h per probe. Note both probes share this value and run sequentially, so
    # the worst case is 16h and run_all's LONG_TEST_TIMEOUT_SECONDS must exceed
    # it or the runner's cap silently wins.
    common = [
        sys.executable, str(HERE / "_memory_probe_impl.py"),
        "--start", "2026-08-10", "--end", "2026-08-14",
        "--timeout", "28800",
    ]
    runs = [
        common + ["--out", str(HERE / "probe_reports" / "memory_full_session.json")],
        common + [
            "--session-start", "14:00", "--session-end", "16:30",
            "--out", str(HERE / "probe_reports" / "memory_1400_1630.json"),
        ],
    ]
    for command in runs:
        print(f"T51: running {' '.join(command)}", flush=True)
        subprocess.run(command, cwd=HERE.parent, check=True)
    print("RESULT: PASS t51_memory_session_comparison — both runs completed sequentially")


if __name__ == "__main__":
    main()
