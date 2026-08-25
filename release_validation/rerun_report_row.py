"""Refresh one row of an existing HTML scorecard from a single test re-run.

A suite takes hours, so when a validation is fixed and re-run on its own the
scorecard still shows the stale row -- and a reader has no way to tell that the
red row was a defect in the validation rather than in the product. Rewriting the
row in place keeps `reports/latest.html` the single place to look, and the row is
stamped with the re-run time so it is never mistaken for part of the original
suite.

    python release_validation/rerun_report_row.py \
        --test t56_equity_calendar_daily_bars.py \
        --log  /path/to/rerun.log \
        --duration 1075.7

The status, checks and run ids all come from the re-run's own output: the
`RESULT:` line it printed and the run ids it logged. Nothing is asserted here
that the re-run did not report.
"""
from __future__ import annotations

import argparse
import html
import json
import re
import shutil
from datetime import datetime
from pathlib import Path

import run_all

HERE = Path(__file__).resolve().parent
REPORTS_DIR = HERE / "reports"
RUN_ID = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)


def artifact_cell(report: Path, run_ids: list[str]) -> str:
    cells = []
    for run_id in run_ids:
        artifact_dir = run_all.ARTIFACTS_DIR / run_id
        validation_path = artifact_dir / "validation.json"
        metadata = {}
        if validation_path.exists():
            try:
                metadata = json.loads(validation_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                metadata = {}
        rows = metadata.get("table_rows") or {}
        links = " · ".join([
            run_all._relative_link(report, validation_path, "validation"),
            run_all._relative_link(report, artifact_dir / "orders.csv",
                                   f'orders ({rows.get("orders", "?")})'),
            run_all._relative_link(report, artifact_dir / "trades.csv",
                                   f'trades ({rows.get("trades", "?")})'),
            run_all._relative_link(report, artifact_dir / "positions.csv",
                                   f'positions ({rows.get("positions", "?")})'),
            run_all._relative_link(report, artifact_dir / "event_logs.csv",
                                   f'event logs ({rows.get("event_logs", "?")})'),
        ])
        cells.append(
            f'<div class="run"><code>{html.escape(run_id)}</code><br>{links}</div>'
        )
    return "".join(cells) or '<span class="missing">no run artifacts exported</span>'


def build_row(report: Path, test: str, status: str, line: str, duration: float,
              run_ids: list[str], log_path: Path, stamp: str) -> str:
    detail = (
        f'<div class="detail">{html.escape(line)}'
        f'<br><em>re-run on its own {html.escape(stamp)}, after the validation '
        f'was fixed; not part of the original suite</em></div>'
    )
    return (
        "<tr>"
        f'<td><span class="status {html.escape(status.lower())}">'
        f'{html.escape(status)}</span></td>'
        f'<td><code>{html.escape(test)}</code></td>'
        f'<td>{duration:.1f}s</td>'
        f'<td>{artifact_cell(report, run_ids)}</td>'
        f'<td>{run_all._relative_link(report, log_path, "runner output")}{detail}</td>'
        "</tr>"
    )


def replace_row(document: str, test: str, row: str) -> str:
    pattern = re.compile(
        r"<tr>(?:(?!</tr>).)*?<code>" + re.escape(test) + r"</code>.*?</tr>",
        re.S,
    )
    document, count = pattern.subn(lambda _: row, document, count=1)
    if not count:
        raise SystemExit(f"{test}: no row for it in the report")
    return document


def recount(document: str) -> str:
    counts: dict[str, int] = {}
    for status in re.findall(r'<span class="status [a-z]+">([A-Z]+)</span>', document):
        counts[status] = counts.get(status, 0) + 1
    ordered = {key: counts[key] for key in sorted(counts)}
    return re.sub(
        r"(Suite [^·]*· )\{[^}]*\}", lambda m: m.group(1) + html.escape(str(ordered)),
        document, count=1,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test", required=True, help="validation file name, e.g. t55_x.py")
    parser.add_argument("--log", required=True, type=Path,
                        help="the re-run's captured output")
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--report", type=Path, default=REPORTS_DIR / "latest.html")
    parser.add_argument("--run-id", action="append", default=[],
                        help="override the run ids read from the log")
    args = parser.parse_args()

    output = args.log.read_text(encoding="utf-8", errors="replace")
    results = [x for x in output.splitlines() if x.startswith("RESULT:")]
    if not results:
        raise SystemExit(f"{args.log}: no RESULT line, so there is nothing to record")
    line = results[-1]
    status = line.split()[1]
    run_ids = args.run_id or [
        run_id for run_id in dict.fromkeys(RUN_ID.findall(output))
        if (run_all.ARTIFACTS_DIR / run_id).is_dir()
    ]

    # The re-run's own output lives beside the suite's logs so the row's
    # "runner output" link resolves to what actually produced this status.
    report = args.report.resolve()
    stamp = datetime.now().astimezone()
    suite = re.search(r"reports/logs/([0-9-]+)", output)
    log_dir = next((path for path in sorted(
        (REPORTS_DIR / "logs").glob("*"), reverse=True) if path.is_dir()), REPORTS_DIR)
    if suite:
        log_dir = REPORTS_DIR / "logs" / suite.group(1)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{Path(args.test).stem}.rerun.log"
    shutil.copyfile(args.log, log_path)

    row = build_row(report, args.test, status, line, args.duration, run_ids,
                    log_path, stamp.isoformat(timespec="seconds"))
    for target in {report, *REPORTS_DIR.glob("release-validation-*.html")}:
        if not target.exists():
            continue
        document = target.read_text(encoding="utf-8")
        if f"<code>{args.test}</code>" not in document:
            continue
        target.write_text(recount(replace_row(document, args.test, row)),
                          encoding="utf-8")
        print(f"updated {target.name}: {args.test} -> {status}")


if __name__ == "__main__":
    main()
