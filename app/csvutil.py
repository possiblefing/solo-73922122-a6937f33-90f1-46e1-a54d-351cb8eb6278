"""CSV parsing / validation for replay logs and correction batches.

Replay CSV - expected columns (header required, order irrelevant, extra
columns ignored):

    arrival_ms,template,store

* ``arrival_ms`` - non-negative integer milliseconds relative to round start
* ``template``   - one of the catalog template names
* ``store``      - store identifier (non-empty)

At most 50 data rows are accepted.

Correction CSV - expected columns:

    sale_id,revision,amount

* ``sale_id``  - positive integer identifying an existing sales row
* ``revision`` - positive integer (stored rows start at revision 0)
* ``amount``   - non-negative decimal with at most two fraction digits

Revision and amount must always be submitted together; at most 20 data rows
and no duplicate sale_id inside one batch.
"""

from __future__ import annotations

import csv
import io
import re
from decimal import Decimal, InvalidOperation

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

# --- Correction-batch limits ------------------------------------------------

MAX_CORRECTION_ROWS = 20
CORRECTION_COLUMNS = ("sale_id", "revision", "amount")
CORRECTION_ALIASES = {
    "sale_id": "sale_id",
    "sale": "sale_id",
    "revision": "revision",
    "rev": "revision",
    "amount": "amount",
}
# sale_id fits BIGINT, revision fits INTEGER, amount fits NUMERIC(12,2).
_MAX_SALE_ID = 9_223_372_036_854_775_807
_MAX_REVISION = 2_147_483_647
_MAX_AMOUNT = Decimal("9999999999.99")
_INT_RE = re.compile(r"^\d+$")
_AMOUNT_RE = re.compile(r"^\d+(\.\d{1,2})?$")


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


def parse_correction_csv(raw: bytes) -> list[dict]:
    """Parse and validate a sales-correction CSV.

    Returns one dict per data row: ``line_no``, ``sale_id``, ``revision``
    (int) and ``amount`` (Decimal).  Raises :class:`CSVValidationError` on
    the first problem found.
    """
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CSVValidationError(f"CSV must be UTF-8 encoded: {exc}") from exc

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise CSVValidationError("CSV is empty or missing a header row.")

    normalized = [
        CORRECTION_ALIASES.get(name.strip().lower(), name.strip().lower())
        for name in reader.fieldnames
    ]
    missing = [c for c in CORRECTION_COLUMNS if c not in normalized]
    if missing:
        raise CSVValidationError(
            f"Missing required column(s): {', '.join(missing)}. "
            f"Required: {', '.join(CORRECTION_COLUMNS)}"
        )

    rows: list[dict] = []
    seen_sale_ids: set[int] = set()
    for line_no, raw_row in enumerate(reader, start=2):
        row = {norm: (raw_row.get(orig) or "").strip()
               for orig, norm in zip(reader.fieldnames, normalized)}
        if not any(row.values()):
            continue  # tolerate blank lines

        # Revision and amount must always be submitted together: a row that
        # carries only one of them is rejected outright.
        if not row.get("sale_id") or not row.get("revision") \
                or not row.get("amount"):
            raise CSVValidationError(
                f"Line {line_no}: sale_id, revision and amount must all "
                f"be submitted together (no empty field)."
            )

        sale_raw = row["sale_id"]
        if not _INT_RE.match(sale_raw):
            raise CSVValidationError(
                f"Line {line_no}: sale_id must be a positive integer, got "
                f"{sale_raw!r}"
            )
        sale_id = int(sale_raw)
        if sale_id < 1 or sale_id > _MAX_SALE_ID:
            raise CSVValidationError(
                f"Line {line_no}: sale_id out of range: {sale_raw!r}"
            )
        if sale_id in seen_sale_ids:
            raise CSVValidationError(
                f"Line {line_no}: duplicate sale_id {sale_id} in this batch"
            )
        seen_sale_ids.add(sale_id)

        rev_raw = row["revision"]
        if not _INT_RE.match(rev_raw):
            raise CSVValidationError(
                f"Line {line_no}: revision must be a positive integer, got "
                f"{rev_raw!r}"
            )
        revision = int(rev_raw)
        if revision < 1 or revision > _MAX_REVISION:
            raise CSVValidationError(
                f"Line {line_no}: revision must be between 1 and "
                f"{_MAX_REVISION}, got {rev_raw!r}"
            )

        amount_raw = row["amount"]
        if not _AMOUNT_RE.match(amount_raw):
            raise CSVValidationError(
                f"Line {line_no}: amount must be a non-negative number with "
                f"at most two fraction digits, got {amount_raw!r}"
            )
        try:
            amount = Decimal(amount_raw)
        except InvalidOperation:  # pragma: no cover - regex already guards
            raise CSVValidationError(
                f"Line {line_no}: invalid amount {amount_raw!r}"
            )
        if amount > _MAX_AMOUNT:
            raise CSVValidationError(
                f"Line {line_no}: amount exceeds the NUMERIC(12,2) limit: "
                f"{amount_raw!r}"
            )

        rows.append({
            "line_no": line_no,
            "sale_id": sale_id,
            "revision": revision,
            "amount": amount,
        })

    if not rows:
        raise CSVValidationError("CSV contains no data rows.")
    if len(rows) > MAX_CORRECTION_ROWS:
        raise CSVValidationError(
            f"Too many rows: {len(rows)} (maximum {MAX_CORRECTION_ROWS})."
        )
    return rows
