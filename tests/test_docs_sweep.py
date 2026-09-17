"""Run the canonical-doc sweep as a test so the docs cannot drift from the SDK.

The sweep (release_validation/doc_sweep.py) statically validates every code
snippet, signature listing, enum member, config field and payload field table in
docs/llms.txt, docs/data_driver/llms.txt and docs/data_api/llms.txt against the
installed package. It needs no credentials and runs no backtests.

If this fails, read the printed findings: either the doc is wrong, or the SDK
changed and the doc needs updating. Do not silence it.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SWEEP = ROOT / "release_validation" / "doc_sweep.py"


def test_docs_match_installed_sdk():
    assert SWEEP.exists(), f"missing sweep tool: {SWEEP}"
    proc = subprocess.run(
        [sys.executable, str(SWEEP)],
        capture_output=True, text=True, cwd=str(ROOT),
        env={"PATH": "/usr/bin:/bin", "HOME": str(Path.home()),
             "HIVEQ_SUPPRESS_STUB_NOTICE": "1",
             "PYTHONPATH": ""},
    )
    assert proc.returncode == 0, (
        "doc sweep found drift between the docs and the installed SDK:\n\n"
        + proc.stdout + proc.stderr
    )
