"""HiveQ read-only dataset discovery utilities."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlencode


class DatasetCatalogError(RuntimeError):
    """Raised when the HiveQ data catalog cannot be read from the API."""


def _api_base_url(base_url: str | None = None) -> str:
    if base_url:
        return base_url.rstrip("/")
    return (
        os.environ.get("HIVEQ_DATA_URL")
        or os.environ.get("HIVEQ_BASE_URL")
        or os.environ.get("HIVEQ_AUTH_URL")
        or "https://staging.hiveq.ai"
    ).rstrip("/")


def _api_key() -> str:
    _load_hiveq_env()
    value = os.environ.get("HIVEQ_API_KEY")
    if value:
        return value
    from hiveq.flow.auth import ensure_login

    return ensure_login()


def _load_hiveq_env() -> None:
    path = os.path.join(os.path.expanduser("~"), ".hiveq", ".env")
    if not os.path.isfile(path):
        return
    try:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if key.startswith("HIVEQ_") and key not in os.environ:
                    os.environ[key] = value.strip().strip('"').strip("'")
    except OSError:
        return


def _request(
    method: str,
    path: str,
    *,
    base_url: str | None = None,
    body: dict[str, Any] | None = None,
    query: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        import requests
    except Exception as exc:
        raise DatasetCatalogError("The requests package is required for data catalog access.") from exc

    url = f"{_api_base_url(base_url)}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": _api_key(),
    }
    org_id = os.environ.get("HIVEQ_ORG_ID")
    if org_id:
        headers["X-Org-ID"] = org_id
    try:
        response = requests.request(method, url, headers=headers, json=body, timeout=30)
    except requests.RequestException as exc:
        raise DatasetCatalogError(f"Unable to reach HiveQ data API at {_api_base_url(base_url)}.") from exc
    if response.status_code >= 400:
        raise DatasetCatalogError(
            f"HiveQ data API returned HTTP {response.status_code} for {path}: {response.text[:300]}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise DatasetCatalogError("HiveQ data API returned a non-JSON response.") from exc
    if payload.get("success") is False:
        raise DatasetCatalogError(str(payload.get("data") or payload))
    data = payload.get("data")
    if not isinstance(data, dict) and path != "/api/read/v0/data":
        raise DatasetCatalogError("HiveQ data API response did not contain an object in data.")
    return payload


def _category_for(entry: dict[str, Any]) -> str:
    category = str(entry.get("category") or "").strip()
    if category:
        return category
    tags = {str(tag).lower() for tag in entry.get("tags") or []}
    asset_class = str(entry.get("asset_class") or "").lower()
    if "signal" in tags or "quant" in tags or "signal" in asset_class:
        return "signals"
    return "market_data"


def _asset_class_for(entry: dict[str, Any]) -> str:
    value = str(entry.get("asset_class") or "").strip()
    if value:
        return value
    tags = {str(tag).lower() for tag in entry.get("tags") or []}
    for candidate in ("equity", "futures", "options", "indices", "economics", "strategy"):
        if candidate in tags:
            return candidate
    if "quantitative" in tags or "clusters" in tags or "signals" in tags:
        return "analytics"
    return "data"


def _entry_key(entry: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(entry.get("schema_name") or ""),
        str(entry.get("database") or ""),
        str(entry.get("table_name") or ""),
    )


def _datasets_for(entry: dict[str, Any]) -> list[str]:
    values = entry.get("datasets") or entry.get("data_sets") or []
    if isinstance(values, str):
        return [values]
    if isinstance(values, list):
        return [str(value) for value in values if value]
    return []


def fetch_catalog(
    *,
    category: str | None = None,
    base_url: str | None = None,
) -> list[dict[str, Any]]:
    """Return the entitled dataset/schema catalog from the HiveQ data API."""

    datasets_payload = _request("GET", "/api/metadata/v0/datasets", base_url=base_url)
    datasets = datasets_payload.get("data", {}).get("datasets", [])
    if not isinstance(datasets, list):
        raise DatasetCatalogError("Dataset response did not contain a dataset list.")

    enrichment: dict[str, dict[str, Any]] = {}
    try:
        all_schemas_payload = _request("GET", "/api/metadata/v0/all-schemas", base_url=base_url)
        for entry in all_schemas_payload.get("data", {}).get("schemas", []):
            if isinstance(entry, dict) and entry.get("schema_name"):
                enrichment["|".join(_entry_key(entry))] = entry
    except DatasetCatalogError:
        enrichment = {}

    catalog: list[dict[str, Any]] = []
    for dataset in sorted(str(value) for value in datasets if value):
        schemas_payload = _request(
            "POST",
            "/api/metadata/v0/schemas",
            base_url=base_url,
            body={"dataset": [dataset]},
        )
        schema_entries = schemas_payload.get("data", {}).get("schemas", [])
        if not isinstance(schema_entries, list):
            schema_entries = []

        schema_names: list[str] = []
        descriptions: list[str] = []
        categories: list[str] = []
        asset_classes: list[str] = []
        for schema_entry in schema_entries:
            if not isinstance(schema_entry, dict):
                continue
            schema_name = schema_entry.get("schema_name")
            if not schema_name:
                continue
            schema_names.append(str(schema_name))
            enriched = enrichment.get("|".join(_entry_key(schema_entry)), {})
            description = schema_entry.get("description") or enriched.get("description")
            if description:
                descriptions.append(str(description))
            entry_category = enriched.get("category") or schema_entry.get("category") or _category_for(schema_entry)
            if entry_category:
                categories.append(str(entry_category))
            asset_classes.append(_asset_class_for(enriched or schema_entry))

        entry_category = _most_common(categories) or "Data"
        normalized_category = _normalize_category(entry_category)
        if category is not None and normalized_category != category:
            continue

        description = _most_common(asset_classes) or entry_category
        catalog.append(
            {
                "dataset": dataset,
                "description": description.replace("_", " "),
                "schemas": schema_names,
                "category": normalized_category,
                "details": descriptions[0] if descriptions else "",
                "source": "api",
            }
        )
    return catalog


def _most_common(values: list[str]) -> str:
    if not values:
        return ""
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _normalize_category(value: str) -> str:
    lower = value.strip().lower().replace(" ", "_").replace("-", "_")
    if lower in {"analytics", "signals", "signal", "quant", "quantitative"}:
        return "signals"
    return "market_data"


def fetch_schema_details(
    dataset: str,
    schema: str,
    *,
    base_url: str | None = None,
    include_stats: bool = False,
) -> dict[str, Any]:
    """Return field metadata for one dataset/schema pair."""

    payload = _request(
        "POST",
        "/api/metadata/v0/schema-details",
        base_url=base_url,
        body={"dataset": dataset, "schema": schema, "include_stats": include_stats},
    )
    data = payload.get("data")
    if not isinstance(data, dict):
        raise DatasetCatalogError("Schema details response did not contain an object in data.")
    return data


def fetch_sample(
    dataset: str,
    schema: str,
    *,
    base_url: str | None = None,
    limit: int = 5,
    filters: dict[str, Any] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    """Return a small sample for one dataset/schema pair."""

    sample_filters = dict(filters or {})
    has_symbol = "symbol" in sample_filters or "symbols" in sample_filters
    if has_symbol:
        symbol = sample_filters.get("symbol")
        default_start, default_end = (None, None)
        if not start or not end:
            default_start, default_end = _symbol_sample_window(
                dataset,
                schema,
                str(symbol) if symbol else None,
                base_url=base_url,
            )
        if not default_start or not default_end:
            default_start, default_end = _default_sample_window(dataset, schema, base_url=base_url)
        _set_date_filters(sample_filters, start or default_start, end or default_end)
        return _read_sample_rows(dataset, schema, sample_filters, limit=limit, base_url=base_url)

    candidates = _default_sample_symbols(dataset, schema, base_url=base_url, limit=10)
    if candidates:
        for symbol in candidates:
            attempt_filters = dict(sample_filters)
            attempt_filters["symbol"] = symbol
            default_start, default_end = (None, None)
            if not start or not end:
                default_start, default_end = _symbol_sample_window(
                    dataset,
                    schema,
                    symbol,
                    base_url=base_url,
                )
            if not default_start or not default_end:
                default_start, default_end = _default_sample_window(dataset, schema, base_url=base_url)
            _set_date_filters(attempt_filters, start or default_start, end or default_end)
            rows = _read_sample_rows(dataset, schema, attempt_filters, limit=limit, base_url=base_url)
            if rows:
                return rows
        return []

    if not start or not end:
        default_start, default_end = _default_sample_window(dataset, schema, base_url=base_url)
        start = start or default_start
        end = end or default_end
    _set_date_filters(sample_filters, start, end)
    return _read_sample_rows(dataset, schema, sample_filters, limit=limit, base_url=base_url)


def _set_date_filters(filters: dict[str, Any], start: str | None, end: str | None) -> None:
    if start and "start" not in filters:
        filters["start"] = start
    if end and "end" not in filters:
        filters["end"] = end


def _read_sample_rows(
    dataset: str,
    schema: str,
    filters: dict[str, Any],
    *,
    limit: int,
    base_url: str | None = None,
) -> list[dict[str, Any]]:
    body: dict[str, Any] = {
        "dataset": dataset,
        "schema": schema,
        "filters": filters,
        "limit": limit,
    }

    payload = _request(
        "POST",
        "/api/read/v0/data",
        base_url=base_url,
        body=body,
    )
    data = payload.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        rows = data.get("rows") or data.get("data") or data.get("records")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    raise DatasetCatalogError("Sample response did not contain a row list in data.")


def _default_sample_window(
    dataset: str,
    schema: str,
    *,
    base_url: str | None = None,
) -> tuple[str | None, str | None]:
    try:
        details = fetch_schema_details(
            dataset,
            schema,
            base_url=base_url,
            include_stats=True,
        )
    except DatasetCatalogError:
        return None, None
    stats = details.get("stats")
    if not isinstance(stats, dict):
        return None, None
    date_range = stats.get("date_range")
    if not isinstance(date_range, dict):
        return None, None
    latest = date_range.get("max")
    earliest = date_range.get("min")
    if isinstance(latest, str) and latest:
        return latest, latest
    if isinstance(earliest, str) and earliest:
        return earliest, earliest
    return None, None


def _default_sample_symbols(
    dataset: str,
    schema: str,
    *,
    base_url: str | None = None,
    limit: int = 10,
) -> list[str]:
    try:
        payload = _request(
            "POST",
            "/api/metadata/v0/universe",
            base_url=base_url,
            body={"dataset": dataset, "schema": schema, "limit": limit},
        )
    except DatasetCatalogError:
        return []
    symbols = payload.get("data", {}).get("symbols")
    if not isinstance(symbols, list) or not symbols:
        return []

    values: list[str] = []
    for item in symbols:
        if isinstance(item, dict):
            value = item.get("symbol")
            if value:
                values.append(str(value))
        elif isinstance(item, str):
            values.append(item)
    return values


def _symbol_sample_window(
    dataset: str,
    schema: str,
    symbol: str | None,
    *,
    base_url: str | None = None,
) -> tuple[str | None, str | None]:
    if not symbol:
        return None, None
    try:
        payload = _request(
            "POST",
            "/api/metadata/v0/symbol-lookup",
            base_url=base_url,
            body={"dataset": dataset, "schema": schema, "symbols": [symbol]},
        )
    except DatasetCatalogError:
        return None, None

    symbols = payload.get("data", {}).get("symbols")
    if not isinstance(symbols, list) or not symbols:
        return None, None
    first = symbols[0]
    if not isinstance(first, dict):
        return None, None
    latest = first.get("last_date") or first.get("latest_date") or first.get("max_date")
    earliest = first.get("first_date") or first.get("earliest_date") or first.get("min_date")
    if isinstance(latest, str) and latest:
        return latest, latest
    if isinstance(earliest, str) and earliest:
        return earliest, earliest
    return None, None
