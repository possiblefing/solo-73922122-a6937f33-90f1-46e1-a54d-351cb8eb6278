"""CSV parsing / validation for replay logs.

Expected columns (header required, order irrelevant, extra columns ignored):

    arrival_ms,template,store

* ``arrival_ms`` - non-negative integer milliseconds relative to round start
* ``template``   - one of the catalog template names
* ``store``      - store identifier (non-empty)

At most 50 data rows are accepted.
"""

from __future__ import annotations

import csv
import io

MAX_ROWS = 50
REQUIRED_COLUMNS = ("arrival_ms", "template", "store")
# Accept a couple of sensible header aliases.
HEADER_ALIASES = {
    "arrival": "arrival_ms",
    "arrival_ms": "arrival_ms",
    "relative_arrival_ms": "arrival_ms",
    "template": "template",
    "query_template": "template",
    "store": "store",
    "store_id": "store",
}


class CSVValidationError(ValueError):
    pass


def parse_replay_csv(raw: bytes, valid_templates: set[str]) -> list[dict]:
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CSVValidationError(f"CSV must be UTF-8 encoded: {exc}") from exc

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise CSVValidationError("CSV is empty or missing a header row.")

    normalized = [
        HEADER_ALIASES.get(name.strip().lower(), name.strip().lower())
        for name in reader.fieldnames
    ]
    missing = [c for c in REQUIRED_COLUMNS if c not in normalized]
    if missing:
        raise CSVValidationError(
            f"Missing required column(s): {', '.join(missing)}. "
            f"Required: {', '.join(REQUIRED_COLUMNS)}"
        )

    rows: list[dict] = []
    for line_no, raw_row in enumerate(reader, start=2):
        row = {norm: (raw_row.get(orig) or "").strip()
               for orig, norm in zip(reader.fieldnames, normalized)}
        if not any(row.values()):
            continue  # tolerate blank lines

        arrival_raw = row.get("arrival_ms", "")
        try:
            arrival_ms = int(arrival_raw)
        except (TypeError, ValueError):
            raise CSVValidationError(
                f"Line {line_no}: arrival_ms must be an integer, got "
                f"{arrival_raw!r}"
            )
        if arrival_ms < 0:
            raise CSVValidationError(
                f"Line {line_no}: arrival_ms must be >= 0"
            )

        template = row.get("template", "")
        if template not in valid_templates:
            raise CSVValidationError(
                f"Line {line_no}: unknown template {template!r}. "
                f"Allowed: {sorted(valid_templates)}"
            )

        store = row.get("store", "")
        if not store:
            raise CSVValidationError(f"Line {line_no}: store must not be empty")

        rows.append({
            "arrival_ms": arrival_ms,
            "template": template,
            "store": store,
        })

    if not rows:
        raise CSVValidationError("CSV contains no data rows.")
    if len(rows) > MAX_ROWS:
        raise CSVValidationError(
            f"Too many rows: {len(rows)} (maximum {MAX_ROWS})."
        )

    rows.sort(key=lambda r: r["arrival_ms"])
    return rows
