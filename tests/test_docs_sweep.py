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


def _sweep_module():
    """Import release_validation/doc_sweep.py as a module (not on sys.path)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("doc_sweep", SWEEP)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_requirements_exemption_is_narrow():
    """§3.4 packages are exempt from the import check — nothing else is.

    A strategy imports its `requirements=` packages inside a callback; those
    are installed on the platform, so they cannot resolve locally. The
    exemption must cover exactly the declared specs and no other import.
    """
    sweep = _sweep_module()
    import ast

    declared = ast.parse(
        'MODULES = ["lightgbm==4.7.0", "scikit-learn>=1.5"]\n'
        'hf.run_backtest(cfgs, requirements=MODULES)\n'
    )
    assert sweep.platform_requirements(declared) == {"lightgbm", "scikit_learn"}

    inline = ast.parse('hf.run_backtest(cfgs, requirements=["xgboost==3.0.5"])')
    assert sweep.platform_requirements(inline) == {"xgboost"}

    # No `requirements=` anywhere -> no exemption, and the check still fires.
    plain = ast.parse('import lightgbm')
    assert sweep.platform_requirements(plain) == set()

    sweep.FINDINGS.clear()
    sweep.check_imports("t", [(1, "import definitely_not_a_real_package")])
    assert [f for f in sweep.FINDINGS if f[0] == "imports"], "undeclared import not reported"
    sweep.FINDINGS.clear()
