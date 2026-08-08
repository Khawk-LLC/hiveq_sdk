"""Fail if a release validation stops following the SDK submission convention."""
from __future__ import annotations

import ast
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXCEPTIONS = {}


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


def main() -> None:
    issues = []
    files = sorted(HERE.glob("t[0-9][0-9]_*.py"))
    for path in files:
        number = path.stem[1:3]
        prefix = f"SdkT{number}"
        source = path.read_text()
        tree = ast.parse(source, filename=str(path))
        classes = [node.name for node in tree.body if isinstance(node, ast.ClassDef)]

        if path.name not in EXCEPTIONS:
            if not classes:
                issues.append(f"{path.name}: no SDK strategy class")
            for class_name in classes:
                if not class_name.startswith(prefix):
                    issues.append(
                        f"{path.name}: class {class_name!r} must start with {prefix!r}"
                    )
            if "run_backtest" not in source:
                issues.append(f"{path.name}: does not submit with run_backtest")
            if not any(token in source for token in (
                "completed_checkpoint", "checkpoint(", "run.logs()"
            )):
                issues.append(f"{path.name}: no platform result/log evidence reader")

        for call in strategy_config_calls(tree):
            values = {kw.arg: kw.value for kw in call.keywords if kw.arg}
            type_node = values.get("type")
            if isinstance(type_node, ast.Constant) and isinstance(type_node.value, str):
                if not type_node.value.startswith(prefix):
                    issues.append(
                        f"{path.name}: StrategyConfig.type={type_node.value!r} "
                        f"must start with {prefix!r}"
                    )

    if issues:
        raise SystemExit("SDK convention audit failed:\n- " + "\n- ".join(issues))
    print(
        f"PASS: {len(files)} validations follow the SdkTxx convention; "
        f"documented exceptions={EXCEPTIONS}"
    )


if __name__ == "__main__":
    main()
