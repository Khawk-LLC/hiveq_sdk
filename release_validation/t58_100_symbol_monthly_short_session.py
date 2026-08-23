"""100-symbol equity universe over one month and a short ET session.

Companion to t51: it keeps the full 100-name trade/imbalance universe while
restricting delivery to 13:00–16:30 ET, exposing multi-day state and duplicate
delivery issues at a practical runtime.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent


def main() -> None:
    command = [
        sys.executable, str(HERE / "_memory_probe_impl.py"),
        "--start", "2026-07-06", "--end", "2026-07-10",
        "--session-start", "13:00", "--session-end", "16:30",
        "--timeout", "14400",
        "--out", str(HERE / "probe_reports" / "memory_100symbol_month_1300_1630.json"),
    ]
    print(f"T58: running {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=HERE.parent, check=True)
    print("RESULT: PASS t58_100_symbol_monthly_short_session — month completed", flush=True)


if __name__ == "__main__":
    main()
