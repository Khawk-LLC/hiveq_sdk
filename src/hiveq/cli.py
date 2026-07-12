"""Top-level HiveQ command line interface."""

from __future__ import annotations

import argparse
import json
from importlib import resources
from pathlib import Path


def _print_table(headers: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    widths = [
        max(len(str(row[index])) for row in (*rows, headers))
        for index in range(len(headers))
    ]
    print("  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))


def _docs_path() -> Path:
    return Path(str(resources.files("hiveq").joinpath("docs")))


def _cmd_login(args: argparse.Namespace) -> int:
    from hiveq.flow import auth

    if args.force:
        auth.login()
    else:
        auth.ensure_login()
    return 0


def _cmd_docs(_args: argparse.Namespace) -> int:
    docs = _docs_path()
    print(f"Docs: {docs}")
    print(f"Flow API reference (single file, read in one go): {docs / 'llms.txt'}")
    print(f"Data-driver reference: {docs / 'data_driver' / 'llms.txt'}")
    return 0


def _cmd_datasets(args: argparse.Namespace) -> int:
    from hiveq.datasets import DatasetCatalogError, fetch_catalog

    category = None if args.category == "all" else args.category
    try:
        datasets = fetch_catalog(category=category, base_url=args.base_url)
    except DatasetCatalogError as exc:
        print(f"Could not read HiveQ dataset catalog: {exc}")
        return 1
    if args.json:
        print(json.dumps(datasets, indent=2, default=str))
        return 0

    rows = [
        (
            dataset.get("dataset", ""),
            dataset.get("description", ""),
            ", ".join(dataset.get("schemas") or []),
            dataset.get("details", ""),
        )
        for dataset in datasets
    ]

    print("Available HiveQ read-only datasets")
    print()
    _print_table(("Dataset", "Description", "Schemas", "Details"), rows)
    return 0


def _cmd_dataset_fields(args: argparse.Namespace) -> int:
    from hiveq.datasets import DatasetCatalogError, fetch_schema_details

    try:
        details = fetch_schema_details(
            args.dataset,
            args.schema,
            base_url=args.base_url,
            include_stats=args.stats,
        )
    except DatasetCatalogError as exc:
        print(f"Could not read HiveQ schema fields: {exc}")
        return 1
    if args.json:
        print(json.dumps(details, indent=2, default=str))
        return 0

    print(f"{details.get('schema_name', args.schema)} ({args.dataset})")
    if details.get("description"):
        print(str(details["description"]))
    if details.get("database") or details.get("table_name"):
        print(f"Table: {details.get('database', '')}.{details.get('table_name', '')}")
    print()
    rows = [
        (
            field.get("name", ""),
            field.get("type", ""),
            bool(field.get("filterable")),
            bool(field.get("required_filter")),
            bool(field.get("default_query")),
            field.get("description", "") or "",
        )
        for field in details.get("fields") or []
        if isinstance(field, dict)
    ]
    _print_table(
        ("Field", "Type", "Filter", "Required", "Default", "Description"),
        rows,
    )
    return 0


def _cmd_dataset_sample(args: argparse.Namespace) -> int:
    from hiveq.datasets import DatasetCatalogError, fetch_sample

    filters = json.loads(args.filters) if args.filters else {}
    try:
        rows = fetch_sample(
            args.dataset,
            args.schema,
            base_url=args.base_url,
            limit=args.limit,
            filters=filters,
            start=args.start,
            end=args.end,
        )
    except DatasetCatalogError as exc:
        print(f"Could not read HiveQ sample data: {exc}")
        print("For schemas that require date filters, pass --start YYYY-MM-DD --end YYYY-MM-DD.")
        return 1
    if args.json:
        print(json.dumps(rows, indent=2, default=str))
        return 0

    if not rows:
        print("No rows returned.")
        return 0
    headers = tuple(rows[0].keys())
    table_rows = [tuple(row.get(header, "") for header in headers) for row in rows]
    _print_table(headers, table_rows)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hiveq")
    sub = parser.add_subparsers(dest="command", required=True)

    login = sub.add_parser(
        "login",
        help="Sign in through the browser and save a HiveQ API key",
    )
    login.add_argument(
        "--force",
        action="store_true",
        help="open the browser and refresh the saved key even if one already exists",
    )
    login.set_defaults(func=_cmd_login)

    docs = sub.add_parser(
        "docs",
        help="Print the path to the docs bundled with this wheel",
    )
    docs.set_defaults(func=_cmd_docs)

    datasets = sub.add_parser(
        "datasets",
        help="Print the available read-only datasets and schemas",
    )
    datasets_sub = datasets.add_subparsers(dest="datasets_command")
    datasets.add_argument(
        "--category",
        choices=("all", "market_data", "signals"),
        default="all",
        help="filter datasets by category",
    )
    datasets.add_argument(
        "--json",
        action="store_true",
        help="print the catalog as JSON",
    )
    datasets.add_argument(
        "--base-url",
        help="HiveQ data API base URL; defaults to HIVEQ_DATA_URL/HIVEQ_BASE_URL/staging",
    )
    datasets.set_defaults(func=_cmd_datasets)

    fields = datasets_sub.add_parser(
        "fields",
        help="Print field metadata for a dataset/schema",
    )
    fields.add_argument("dataset")
    fields.add_argument("schema")
    fields.add_argument("--base-url")
    fields.add_argument("--stats", action="store_true", help="include schema stats in JSON output")
    fields.add_argument("--json", action="store_true")
    fields.set_defaults(func=_cmd_dataset_fields)

    sample = datasets_sub.add_parser(
        "sample",
        help="Print sample rows for a dataset/schema",
    )
    sample.add_argument("dataset")
    sample.add_argument("schema")
    sample.add_argument("--base-url")
    sample.add_argument("--limit", type=int, default=5)
    sample.add_argument("--start", help="start date/time filter, for example 2026-07-02")
    sample.add_argument("--end", help="end date/time filter, for example 2026-07-03")
    sample.add_argument("--filters", help="JSON object of read filters")
    sample.add_argument("--json", action="store_true")
    sample.set_defaults(func=_cmd_dataset_sample)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
