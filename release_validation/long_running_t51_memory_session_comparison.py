"""Run the 100-symbol memory probe twice, sequentially, with two sessions."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent


def main() -> None:
    common = [
        sys.executable, str(HERE / "_memory_probe_impl.py"),
        "--start", "2026-08-10", "--end", "2026-08-14",
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
