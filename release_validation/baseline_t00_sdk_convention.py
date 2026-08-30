"""Every release validation follows the SdkTxx submission convention.

A static audit of this directory — no platform run — so it costs nothing and
catches the drift that makes the rest of the suite untrustworthy: a validation
that submits nothing, one whose strategy class or `StrategyConfig.type` no
longer identifies which test it belongs to, a duplicated or missing number, or
one that never reads its own result back.

It is named ``baseline_t00`` so it runs before the validations it audits.
"""
from __future__ import annotations

import ast
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import finish                                          # noqa: E402

HERE = Path(__file__).resolve().parent

# t00 is this audit: static, strategy-free, and therefore not subject to the
# convention it enforces.
SELF_NAME = "baseline_t00_sdk_convention.py"
EXPECTED_NUMBERS = range(1, 72)
EXCEPTIONS = {
    "long_running_t51_memory_session_comparison.py": "sequential wrapper around the strategy-bearing memory probe",
    "long_running_t58_100_symbol_monthly_short_session.py": "wrapper around the strategy-bearing 100-symbol memory probe",
}


def validation_number(path: Path) -> int:
    return int(path.name.split("_t", 1)[1][:2])


def strategy_config_calls(tree: ast.AST):
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (
            isinstance(func, ast.Name) and func.id == "StrategyConfig"
            or isinstance(func, ast.Attribute) and func.attr == "StrategyConfig"
        ):
            continue
        yield node


def audit() -> tuple[dict[str, bool], list[str], int]:
    """Return (checks, issues, number of validations audited)."""
    files = sorted([
        *HERE.glob("baseline_t[0-9][0-9]_*.py"),
        *HERE.glob("long_running_t[0-9][0-9]_*.py"),
    ])
    files = [p for p in files if p.name != SELF_NAME]

    numbering: list[str] = []
    by_number: dict[int, list[str]] = {}
    for path in files:
        by_number.setdefault(validation_number(path), []).append(path.name)
    for number in EXPECTED_NUMBERS:
        matches = by_number.get(number, [])
        if len(matches) != 1:
            numbering.append(
                f"t{number:02d}: expected exactly one validation, found {matches}"
            )
    unexpected = sorted(n for n in by_number if n not in EXPECTED_NUMBERS)
    if unexpected:
        numbering.append(f"unexpected validation numbers: {unexpected}")

    naming: list[str] = []
    submits: list[str] = []
    evidence: list[str] = []
    config_type: list[str] = []

    for path in files:
        number = f"{validation_number(path):02d}"
        prefix = f"SdkT{number}"
        source = path.read_text()
        tree = ast.parse(source, filename=str(path))
        classes = [node.name for node in tree.body if isinstance(node, ast.ClassDef)]

        if path.name not in EXCEPTIONS:
            if not classes:
                naming.append(f"{path.name}: no SDK strategy class")
            for class_name in classes:
                if not class_name.startswith(prefix):
                    naming.append(
                        f"{path.name}: class {class_name!r} must start with {prefix!r}"
                    )
            if "run_backtest" not in source:
                submits.append(f"{path.name}: does not submit with run_backtest")
            if not any(token in source for token in (
                "completed_checkpoint", "checkpoint(", "run.logs()",
                "export_run_artifacts",
            )):
                evidence.append(f"{path.name}: no platform result/log evidence reader")

        for call in strategy_config_calls(tree):
            values = {kw.arg: kw.value for kw in call.keywords if kw.arg}
            type_node = values.get("type")
            if isinstance(type_node, ast.Constant) and isinstance(type_node.value, str):
                if not type_node.value.startswith(prefix):
                    config_type.append(
                        f"{path.name}: StrategyConfig.type={type_node.value!r} "
                        f"must start with {prefix!r}"
                    )

    checks = {
        "numbering": not numbering,
        "suite_prefix": all(
            path.name.startswith(("baseline_", "long_running_")) for path in files
        ),
        "strategy_class_naming": not naming,
        "submits_with_run_backtest": not submits,
        "reads_platform_evidence": not evidence,
        "strategy_config_type": not config_type,
    }
    issues = numbering + naming + submits + evidence + config_type
    return checks, issues, len(files)


def main() -> None:
    checks, issues, audited = audit()
    for issue in issues:
        print(f"  [ISSUE] {issue}")
    extra = f"audited={audited}; documented_exceptions={sorted(EXCEPTIONS)}"
    finish("t00_sdk_convention", checks, extra=extra)


if __name__ == "__main__":
    main()
