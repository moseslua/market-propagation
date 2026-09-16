"""Immutable raw storage and sealed columnar datasets.

Two layers live here.

:class:`RawStore` keeps content-addressed payload blobs plus one receipt per
covered occurrence. Content addressing gives deduplication of *bytes*; the
receipt manifest gives *occurrence* identity. Those are different things. A
payload that genuinely repeats with no source identifier produces one blob and
two distinct receipts, because collapsing the two occurrences would destroy the
event being studied.

Sealed datasets are Parquet files written once and never rewritten. Every
dataset carries a manifest naming the file, its sha256, the Arrow schema and the
row count; reading verifies the hash before returning anything. Structured
record families declare their columns and partition keys here rather than
inferring them, because neither Parquet nor a declarative query engine enforces
the research contract.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
import math
import os
import re
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .domain import UTC, Provenance, parse_utc_time

__all__ = [
    "DECIMAL_TYPE",
    "FORECAST_COLUMNS",
    "FORECAST_SCHEMA_VERSION",
    "TABLE_SCHEMAS",
    "TRADE_PANEL_COLUMNS",
    "DatasetRef",
    "RawStore",
    "duckdb_connection",
    "hash_bytes",
    "hash_file",
    "query_sealed",
    "read_parquet",
    "resolve_rows",
    "write_parquet",
]

# 38 digits with 12 fractional places holds every price, size and macro value in
# this study exactly. A value needing a thirteenth place is a schema change and
# is rejected rather than rounded silently.
DECIMAL_TYPE = pa.decimal128(38, 12)

STRING = pa.string()
BOOL = pa.bool_()
INT64 = pa.int64()
FLOAT64 = pa.float64()
TIMESTAMP = pa.timestamp("us", tz="UTC")

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


def _valid_receipt_id(receipt_id: str) -> bool:
    return bool(_RECEIPT_ID_RE.match(receipt_id))


_CREDENTIAL_KEY_RE = re.compile(
    r"(?:^|[_-])(?:api[_-]?key|apikey|authoriz\w*|auth|bearer|cookie|credential|passwo?rd"
    r"|private[_-]?key|secret|session[_-]?id|signature|token)(?:$|[_-])",
    re.IGNORECASE,
)


def hash_bytes(payload: bytes) -> str:
    """sha256 of a payload, as lowercase hex."""
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TypeError(f"hash_bytes expects bytes, got {type(payload).__name__}")
    return hashlib.sha256(bytes(payload)).hexdigest()


def hash_file(path: str | Path) -> str:
    """sha256 of a file, streamed so a large raw payload does not enter memory."""
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    """Write ``payload`` to ``path`` atomically, never overwriting existing bytes."""
    if path.exists():
        existing = hash_file(path)
        if existing == hash_bytes(payload):
            return
        raise FileExistsError(
            f"refusing to overwrite immutable file {path}: existing sha256 {existing} "
            f"differs from new content"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    # The handle is closed deliberately before the hard link, and the file must
    # outlive it: a context manager here would unlink the temporary file.
    handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False
    )
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if hash_file(path) != hash_bytes(payload):
                raise FileExistsError(
                    f"refusing to overwrite immutable file {path}: it appeared concurrently "
                    "with different content"
                ) from None
    finally:
        temporary.unlink(missing_ok=True)


# JSON codec that keeps decimals exact


class _Encoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Decimal):
            return {"__decimal__": str(o)}
        if isinstance(o, dt.datetime):
            if o.tzinfo is None or o.tzinfo.utcoffset(o) is None:
                raise ValueError(f"refusing to serialize naive datetime {o!r}")
            return {"__datetime__": o.astimezone(UTC).isoformat()}
        if isinstance(o, (set, frozenset)):
            return sorted(o)
        if is_dataclass(o) and not isinstance(o, type):
            return {f.name: getattr(o, f.name) for f in fields(o)}
        return super().default(o)


def _decoder(obj: dict) -> Any:
    if len(obj) == 1:
        if "__decimal__" in obj:
            return Decimal(obj["__decimal__"])
        if "__datetime__" in obj:
            return dt.datetime.fromisoformat(obj["__datetime__"]).astimezone(UTC)
    return obj


def _json_dumps(value: Any) -> str:
    return json.dumps(value, cls=_Encoder, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _json_loads(text: str) -> Any:
    return json.loads(text, object_hook=_decoder)


@dataclass(frozen=True, slots=True)
class TableSchema:
    """Declared columns, required columns, keys and identity for one dataset table."""

    name: str
    columns: tuple[tuple[str, pa.DataType], ...]
    required: tuple[str, ...]
    sort_by: tuple[str, ...]
    json_columns: tuple[str, ...] = ()
    schema_version: str = "1"

    def __post_init__(self) -> None:
        if not _NAME_RE.match(self.name):
            raise ValueError(f"invalid table name {self.name!r}")
        if not isinstance(self.schema_version, str) or not self.schema_version:
            raise ValueError(f"table {self.name!r} schema_version must be a non-empty str")
        names = [name for name, _ in self.columns]
        if len(set(names)) != len(names):
            raise ValueError(f"table {self.name!r} declares a duplicate column")
        for name in (*self.required, *self.sort_by, *self.json_columns):
            if name not in names:
                raise ValueError(f"table {self.name!r} references undeclared column {name!r}")
        for name in self.json_columns:
            declared = dict(self.columns)[name]
            if declared != STRING:
                raise ValueError(
                    f"table {self.name!r} json column {name!r} must be a string column"
                )

    @property
    def arrow_schema(self) -> pa.Schema:
        return pa.schema(list(self.columns))

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)


def _clock_columns(prefix: str = "") -> tuple[tuple[str, pa.DataType], ...]:
    head = f"{prefix}_" if prefix else ""
    return (
        (f"{head}source_time", TIMESTAMP),
        (f"{head}received_time", TIMESTAMP),
        (f"{head}usable_time", TIMESTAMP),
        (f"{head}availability_lower", TIMESTAMP),
        (f"{head}availability_upper", TIMESTAMP),
        (f"{head}availability_quality", STRING),
        (f"{head}availability_basis", STRING),
        (f"{head}monotonic_ns", INT64),
    )


_PROVENANCE_COLUMNS = (
    ("raw_hash", STRING),
    ("record_id", STRING),
    ("source", STRING),
    ("schema_version", STRING),
)

_CLOCK_PATHS = (
    ("source_time", "clock.source_time"),
    ("received_time", "clock.received_time"),
    ("usable_time", "clock.usable_time"),
    ("availability_lower", "clock.availability.lower"),
    ("availability_upper", "clock.availability.upper"),
    ("availability_quality", "clock.availability.quality"),
    ("availability_basis", "clock.availability.basis"),
    ("monotonic_ns", "clock.monotonic_ns"),
)

_PROVENANCE_PATHS = (
    ("raw_hash", "provenance.raw_hash"),
    ("record_id", "provenance.record_id"),
    ("source", "provenance.source"),
    ("schema_version", "provenance.schema_version"),
)


_RECORD_PATHS: dict[str, tuple[tuple[str, str], ...]] = {}


def _missing_required(rows: Sequence[Mapping[str, Any]], table: str) -> list[str]:
    """Required columns that are absent or null in any row.

    Required means the column is what identifies the row: without it a consumer
    cannot tell which market, event or release a row belongs to. An unknown
    optional column is left null deliberately; a null required column is a
    broken record, so the write must fail rather than seal it.
    """
    schema = TABLE_SCHEMAS[table]
    if not schema.required or not rows:
        return []
    missing: list[str] = []
    for name in schema.required:
        if any(row.get(name) is None for row in rows):
            missing.append(name)
    return missing


def _record_table(
    name: str,
    *,
    columns: Sequence[tuple[str, pa.DataType]],
    paths: Sequence[tuple[str, str]],
    sort_by: Sequence[str],
    required: Sequence[str],
    json_columns: Sequence[str] = (),
) -> TableSchema:
    _RECORD_PATHS[name] = tuple(paths)
    return TableSchema(
        name=name,
        columns=tuple(columns),
        required=tuple(required),
        sort_by=tuple(sort_by),
        json_columns=tuple(json_columns),
    )


_RELEASE_AUX_COLUMNS = (
    ("event_id", STRING),
    ("family", STRING),
    ("scheduled_at", TIMESTAMP),
    ("reference_period", STRING),
    ("observed_at", TIMESTAMP),
    ("values_json", STRING),
    ("revisions_json", STRING),
)
_RELEASE_AUX_PATHS = (
    ("event_id", "event_id"),
    ("family", "family"),
    ("scheduled_at", "scheduled_at"),
    ("reference_period", "reference_period"),
    ("observed_at", "observed_at"),
    ("values_json", "values"),
    ("revisions_json", "revisions"),
)

TABLE_SCHEMAS: dict[str, TableSchema] = {
    "book_events": _record_table(
        "book_events",
        columns=(
            ("venue", STRING),
            ("contract_id", STRING),
            ("kind", STRING),
            ("connection_id", STRING),
            ("sequence_scope", STRING),
            ("sequence", INT64),
            ("operation", STRING),
            ("side", STRING),
            ("price", DECIMAL_TYPE),
            ("size", DECIMAL_TYPE),
            ("bid_levels_json", STRING),
            ("ask_levels_json", STRING),
            *_clock_columns(),
            *_PROVENANCE_COLUMNS,
        ),
        paths=(
            ("venue", "venue"),
            ("contract_id", "contract_id"),
            ("kind", "kind"),
            ("connection_id", "connection_id"),
            ("sequence_scope", "sequence_scope"),
            ("sequence", "sequence"),
            ("operation", "operation"),
            ("side", "side"),
            ("price", "price"),
            ("size", "size"),
            ("bid_levels_json", "bids"),
            ("ask_levels_json", "asks"),
            *_CLOCK_PATHS,
            *_PROVENANCE_PATHS,
        ),
        sort_by=("venue", "contract_id", "sequence_scope", "sequence", "usable_time", "raw_hash"),
        required=("venue", "contract_id", "kind", "usable_time", "raw_hash"),
        json_columns=("bid_levels_json", "ask_levels_json"),
    ),
    "quotes": _record_table(
        "quotes",
        columns=(
            ("venue", STRING),
            ("contract_id", STRING),
            ("bid", DECIMAL_TYPE),
            ("ask", DECIMAL_TYPE),
            ("bid_size", DECIMAL_TYPE),
            ("ask_size", DECIMAL_TYPE),
            ("validity", STRING),
            ("last_price_change", TIMESTAMP),
            ("last_verified", TIMESTAMP),
            ("last_trade", TIMESTAMP),
            ("replay_order", STRING),
            *_clock_columns(),
            *_PROVENANCE_COLUMNS,
        ),
        paths=(
            ("venue", "venue"),
            ("contract_id", "contract_id"),
            ("bid", "bid"),
            ("ask", "ask"),
            ("bid_size", "bid_size"),
            ("ask_size", "ask_size"),
            ("validity", "validity"),
            ("last_price_change", "last_price_change"),
            ("last_verified", "last_verified"),
            ("last_trade", "last_trade"),
            ("replay_order", "replay_order"),
            *_CLOCK_PATHS,
            *_PROVENANCE_PATHS,
        ),
        sort_by=("venue", "contract_id", "usable_time", "source_time", "raw_hash", "record_id"),
        required=("venue", "contract_id", "validity", "raw_hash"),
    ),
    "trades": _record_table(
        "trades",
        columns=(
            ("venue", STRING),
            ("contract_id", STRING),
            ("trade_id", STRING),
            ("price", DECIMAL_TYPE),
            ("size", DECIMAL_TYPE),
            ("aggressor", STRING),
            ("is_block", BOOL),
            *_clock_columns(),
            *_PROVENANCE_COLUMNS,
        ),
        paths=(
            ("venue", "venue"),
            ("contract_id", "contract_id"),
            ("trade_id", "trade_id"),
            ("price", "price"),
            ("size", "size"),
            ("aggressor", "aggressor"),
            ("is_block", "is_block"),
            *_CLOCK_PATHS,
            *_PROVENANCE_PATHS,
        ),
        sort_by=("venue", "contract_id", "usable_time", "raw_hash", "record_id"),
        required=("venue", "contract_id", "price", "size", "raw_hash"),
    ),
    "contracts": _record_table(
        "contracts",
        columns=(
            ("venue", STRING),
            ("contract_id", STRING),
            ("event_id", STRING),
            ("family", STRING),
            ("reference_period", STRING),
            ("release_source", STRING),
            ("units", STRING),
            ("operator", STRING),
            ("threshold", DECIMAL_TYPE),
            ("lower", DECIMAL_TYPE),
            ("upper", DECIMAL_TYPE),
            ("rounding", STRING),
            ("vintage", STRING),
            ("timezone", STRING),
            ("deadline", TIMESTAMP),
            ("settlement", STRING),
            ("currency", STRING),
            ("exceptional_policy", STRING),
            ("open_time", TIMESTAMP),
            ("close_time", TIMESTAMP),
            ("resolve_time", TIMESTAMP),
            ("rule_hash", STRING),
            ("rule_available_at", TIMESTAMP),
            *_PROVENANCE_COLUMNS,
        ),
        paths=(
            ("venue", "venue"),
            ("contract_id", "contract_id"),
            ("event_id", "event_id"),
            ("family", "family"),
            ("reference_period", "reference_period"),
            ("release_source", "source"),
            ("units", "units"),
            ("operator", "operator"),
            ("threshold", "threshold"),
            ("lower", "lower"),
            ("upper", "upper"),
            ("rounding", "rounding"),
            ("vintage", "vintage"),
            ("timezone", "timezone"),
            ("deadline", "deadline"),
            ("settlement", "settlement"),
            ("currency", "currency"),
            ("exceptional_policy", "exceptional_policy"),
            ("open_time", "open_time"),
            ("close_time", "close_time"),
            ("resolve_time", "resolve_time"),
            ("rule_hash", "rule_hash"),
            ("rule_available_at", "rule_available_at"),
            *_PROVENANCE_PATHS,
        ),
        sort_by=("venue", "contract_id", "rule_hash"),
        required=("venue", "contract_id", "event_id", "rule_hash", "raw_hash"),
    ),
    "releases": _record_table(
        "releases",
        columns=(*_RELEASE_AUX_COLUMNS, *_clock_columns(), *_PROVENANCE_COLUMNS),
        paths=(*_RELEASE_AUX_PATHS, *_CLOCK_PATHS, *_PROVENANCE_PATHS),
        sort_by=("event_id", "usable_time", "raw_hash"),
        required=(
            "event_id",
            "family",
            "scheduled_at",
            "reference_period",
            "values_json",
            "raw_hash",
        ),
        json_columns=("values_json", "revisions_json"),
    ),
    "expectations": _record_table(
        "expectations",
        columns=(
            ("event_id", STRING),
            ("statistic", STRING),
            ("value", DECIMAL_TYPE),
            ("source_kind", STRING),
            ("revision_status", STRING),
            *_clock_columns(),
            *_PROVENANCE_COLUMNS,
        ),
        paths=(
            ("event_id", "event_id"),
            ("statistic", "statistic"),
            ("value", "value"),
            ("source_kind", "source_kind"),
            ("revision_status", "revision_status"),
            *_CLOCK_PATHS,
            *_PROVENANCE_PATHS,
        ),
        sort_by=("event_id", "statistic", "source_kind", "usable_time", "raw_hash"),
        required=("event_id", "statistic", "value", "source_kind", "usable_time", "raw_hash"),
    ),
    "resolutions": _record_table(
        "resolutions",
        columns=(
            ("contract_id", STRING),
            ("payout", DECIMAL_TYPE),
            ("known_at", TIMESTAMP),
            ("resolved_at", TIMESTAMP),
            ("rule_hash", STRING),
            ("exceptional", BOOL),
            *_PROVENANCE_COLUMNS,
        ),
        paths=(
            ("contract_id", "contract_id"),
            ("payout", "payout"),
            ("known_at", "known_at"),
            ("resolved_at", "resolved_at"),
            ("rule_hash", "rule_hash"),
            ("exceptional", "exceptional"),
            *_PROVENANCE_PATHS,
        ),
        sort_by=("contract_id", "rule_hash", "raw_hash"),
        required=("contract_id", "payout", "rule_hash", "raw_hash"),
    ),
    "event_panel": TableSchema(
        name="event_panel",
        columns=(
            ("event_id", STRING),
            ("cluster_id", STRING),
            ("family", STRING),
            ("contract_id", STRING),
            ("venue", STRING),
            ("cohort", STRING),
            ("event_time", TIMESTAMP),
            ("horizon_seconds", INT64),
            ("baseline_time", TIMESTAMP),
            ("endpoint_time", TIMESTAMP),
            ("baseline", FLOAT64),
            ("endpoint", FLOAT64),
            ("response", FLOAT64),
            ("spread_before", FLOAT64),
            ("spread_after", FLOAT64),
            ("depth_before", FLOAT64),
            ("depth_after", FLOAT64),
            ("baseline_age_seconds", FLOAT64),
            ("endpoint_age_seconds", FLOAT64),
            ("valid", BOOL),
            ("exclusion_reason", STRING),
            ("raw_hashes", STRING),
            ("replay_order", STRING),
            ("clock_quality", STRING),
            ("timing_uncertainty_seconds", FLOAT64),
            ("label_available_time", TIMESTAMP),
            ("training_cutoff", TIMESTAMP),
            ("split", STRING),
            ("operator", STRING),
            ("threshold", FLOAT64),
            ("rounding", STRING),
            ("units", STRING),
            ("orientation_sign", FLOAT64),
        ),
        required=(
            "event_id",
            "cluster_id",
            "family",
            "contract_id",
            "venue",
            "cohort",
            "event_time",
            "horizon_seconds",
            "baseline_time",
            "endpoint_time",
            "valid",
            "replay_order",
        ),
        sort_by=("event_id", "contract_id", "venue", "horizon_seconds"),
    ),
    "forecast": TableSchema(
        name="forecast",
        columns=(
            ("event_id", STRING),
            ("family", STRING),
            ("contract_id", STRING),
            ("event_time", TIMESTAMP),
            ("prediction_time", TIMESTAMP),
            ("horizon_seconds", INT64),
            ("target", FLOAT64),
            ("target_available_time", TIMESTAMP),
            ("max_input_available_time", TIMESTAMP),
            ("current_price", FLOAT64),
            ("own_lag", FLOAT64),
            ("shock", FLOAT64),
            ("delayed_shock", FLOAT64),
            ("neighbor_lag", FLOAT64),
            ("neighbor_lag_complement", FLOAT64),
            ("neighbor_lag_control", FLOAT64),
            ("valid", BOOL),
            ("cluster_id", STRING),
            ("cohort", STRING),
            ("orientation_sign", FLOAT64),
            ("exclusion_reason", STRING),
        ),
        required=(
            "event_id",
            "family",
            "contract_id",
            "event_time",
            "prediction_time",
            "horizon_seconds",
            "target",
            "valid",
            # The release a row belongs to is what the split authority groups
            # by, so a forecast table without it cannot reproduce the folds and
            # the loss a comparison reported.
            "cluster_id",
        ),
        # ``target`` is a future probability change, not part of a row's
        # identity: a numeric key would also order rows by its magnitude.
        sort_by=("event_id", "contract_id", "horizon_seconds", "prediction_time"),
        # Version 3 carries every predictor the fitted comparison reads and the
        # release, cohort and orientation columns admissibility depends on.
        # Version 2 held a narrower table that omitted ``neighbor_lag_control``
        # and ``cluster_id``; a network model cannot be refit from those bytes
        # and their splits cannot be reproduced, so they are refused. Version 1
        # declared ``target`` as text. The version travels in the file's own
        # metadata and is checked on the way back in, so an older dataset is
        # refused instead of being read as if it carried the current columns.
        schema_version="3",
    ),
    "historical_trades": _record_table(
        "historical_trades",
        columns=(
            ("venue", STRING),
            ("contract_id", STRING),
            ("token_id", STRING),
            ("outcome_seq", INT64),
            ("trade_id", STRING),
            ("price", DECIMAL_TYPE),
            ("raw_price_units", STRING),
            ("price_precision", STRING),
            ("raw_price", DECIMAL_TYPE),
            ("secondary_price", DECIMAL_TYPE),
            ("event_price", DECIMAL_TYPE),
            ("event_axis", STRING),
            ("direction", STRING),
            ("event_direction", INT64),
            ("size", DECIMAL_TYPE),
            ("size_quality", STRING),
            ("flags_json", STRING),
            *_clock_columns(),
            *_PROVENANCE_COLUMNS,
        ),
        paths=(
            ("venue", "venue"),
            ("contract_id", "contract_id"),
            ("token_id", "token_id"),
            ("outcome_seq", "outcome_seq"),
            ("trade_id", "trade_id"),
            ("price", "price"),
            ("raw_price_units", "raw_price_units"),
            ("price_precision", "price_precision"),
            ("raw_price", "raw_price"),
            ("secondary_price", "secondary_price"),
            ("event_price", "event_price"),
            ("event_axis", "event_axis"),
            ("direction", "direction"),
            ("event_direction", "event_direction"),
            ("size", "size"),
            ("size_quality", "size_quality"),
            ("flags_json", "flags"),
            *_CLOCK_PATHS,
            *_PROVENANCE_PATHS,
        ),
        # ``usable_time`` is deliberately absent from the sort key: for a
        # historical archive row it is null, and sorting by a column that is
        # always null would put the whole table in raw-hash order while looking
        # like a time ordering. ``source_time`` orders these rows.
        sort_by=("venue", "contract_id", "source_time", "raw_hash", "record_id"),
        # ``size`` is absent from ``required`` on purpose. The archive layers
        # omit quantity, and an unknown quantity is a real state that has to
        # survive a storage round trip as null rather than be refused or zeroed.
        # ``record_id`` is required here, unlike ``trades``: an external row's
        # occurrence locator is the only thing that distinguishes repeated
        # identical fills, so a row without one cannot be sealed.
        required=(
            "venue",
            "contract_id",
            "price",
            "raw_price_units",
            "price_precision",
            "size_quality",
            "raw_hash",
            "record_id",
        ),
        json_columns=("flags_json",),
    ),
    "trade_panel": TableSchema(
        name="trade_panel",
        columns=(
            ("event_id", STRING),
            ("cluster_id", STRING),
            ("family", STRING),
            ("venue", STRING),
            ("contract_id", STRING),
            ("cohort", STRING),
            ("event_time", TIMESTAMP),
            ("horizon_seconds", INT64),
            ("baseline_source_time", TIMESTAMP),
            ("endpoint_source_time", TIMESTAMP),
            ("baseline_time_basis", STRING),
            ("endpoint_time_basis", STRING),
            ("baseline", FLOAT64),
            ("endpoint", FLOAT64),
            ("response", FLOAT64),
            ("baseline_raw_price", FLOAT64),
            ("endpoint_raw_price", FLOAT64),
            ("price_scale", STRING),
            ("price_convention", STRING),
            ("event_axis", STRING),
            ("baseline_age_seconds", FLOAT64),
            ("endpoint_age_seconds", FLOAT64),
            ("baseline_trade_count", INT64),
            ("endpoint_trade_count", INT64),
            ("tie_group_size", INT64),
            ("tie_group_response_min", FLOAT64),
            ("tie_group_response_max", FLOAT64),
            ("endpoint_envelope_low", FLOAT64),
            ("endpoint_envelope_high", FLOAT64),
            ("post_release_trade_observed", BOOL),
            ("rule_version", STRING),
            ("rule_evidence_quality", STRING),
            ("valid", BOOL),
            ("exclusion_reason", STRING),
            ("clock_mode", STRING),
            ("availability_status", STRING),
            ("label_time_basis", STRING),
            ("size_quality", STRING),
            ("size_verified", BOOL),
            ("provenance_locators_json", STRING),
            ("flags_json", STRING),
        ),
        required=(
            "event_id",
            "cluster_id",
            "family",
            "venue",
            "contract_id",
            "cohort",
            "event_time",
            "horizon_seconds",
            "valid",
            "clock_mode",
            "availability_status",
            "post_release_trade_observed",
        ),
        # Quote-only columns are deliberately not declared. A trade tape has no
        # bid, ask, spread or depth, and a column that could only ever hold null
        # invites a later reader to cast a trade price into it.
        sort_by=("event_id", "contract_id", "horizon_seconds"),
        json_columns=("provenance_locators_json", "flags_json"),
    ),
    "historical_forecast": TableSchema(
        name="historical_forecast",
        columns=(
            ("event_id", STRING),
            ("cluster_id", STRING),
            ("family", STRING),
            ("venue", STRING),
            ("receiver_contract_id", STRING),
            ("donor_contract_id", STRING),
            ("release_time", TIMESTAMP),
            ("forecast_origin", TIMESTAMP),
            ("label_source_time", TIMESTAMP),
            ("max_input_source_time", TIMESTAMP),
            ("decision_date", TIMESTAMP),
            ("recipient_anchor_time", TIMESTAMP),
            ("recipient_anchor", FLOAT64),
            ("recipient_anchor_age_seconds", FLOAT64),
            ("recipient_anchor_tie_group_size", INT64),
            ("recipient_prior_baseline_time", TIMESTAMP),
            ("recipient_prior_baseline", FLOAT64),
            ("recipient_prior_baseline_age_seconds", FLOAT64),
            ("recipient_prior_baseline_tie_group_size", INT64),
            ("own_lag", FLOAT64),
            ("recipient_target_time", TIMESTAMP),
            ("recipient_target", FLOAT64),
            ("recipient_target_tie_group_size", INT64),
            ("target", FLOAT64),
            ("donor_baseline_time", TIMESTAMP),
            ("donor_baseline", FLOAT64),
            ("donor_cutoff_time", TIMESTAMP),
            ("donor_signal_time", TIMESTAMP),
            ("donor_signal", FLOAT64),
            ("neighbor_lag", FLOAT64),
            ("recipient_anchor_occurrences", STRING),
            ("recipient_prior_baseline_occurrences", STRING),
            ("recipient_target_occurrences", STRING),
            ("donor_signal_occurrences", STRING),
            ("valid", BOOL),
            ("exclusion_reason", STRING),
            ("clock_basis", STRING),
        ),
        required=(
            "event_id",
            "cluster_id",
            "family",
            "venue",
            "receiver_contract_id",
            "release_time",
            "forecast_origin",
            "valid",
            "clock_basis",
        ),
        # The forward target is a future probability change, so it is not part
        # of a row's identity: a numeric key would order rows by its magnitude.
        sort_by=("event_id", "receiver_contract_id", "forecast_origin"),
        json_columns=(
            "recipient_anchor_occurrences",
            "recipient_prior_baseline_occurrences",
            "recipient_target_occurrences",
            "donor_signal_occurrences",
        ),
    ),
}

#: Canonical trade-panel columns, in declared order.
#:
#: The panel builder takes its column list from here rather than keeping its own
#: copy, so a column cannot be declared in one place and silently dropped in the
#: other. ``response`` is in absolute probability units on the 0-1 scale.
TRADE_PANEL_COLUMNS: tuple[str, ...] = TABLE_SCHEMAS["trade_panel"].column_names

#: Canonical historical-forecast columns, in declared order.
#:
#: The source-time forecast builder takes its column list from here rather than
#: keeping its own copy, so a column cannot be declared in one place and silently
#: dropped in the other. The table is deliberately separate from ``forecast``:
#: that one is the availability-based quote pipeline's table, and a source-time
#: row has no availability interval to put in it.
HISTORICAL_FORECAST_COLUMNS: tuple[str, ...] = TABLE_SCHEMAS["historical_forecast"].column_names

#: Canonical forecast-table columns, in declared order.
#:
#: The normalizer (:mod:`market_propagation.point_in_time`) and the simulator
#: (:mod:`market_propagation.simulation`) both take their column list from this
#: one tuple rather than keeping their own copy, so a column cannot be declared
#: here and silently dropped there. Every column a model reads as a predictor,
#: and every column that identifies the release a row belongs to, is in it.
FORECAST_COLUMNS: tuple[str, ...] = TABLE_SCHEMAS["forecast"].column_names

#: Version written into a sealed forecast dataset and required on the way back.
FORECAST_SCHEMA_VERSION: str = TABLE_SCHEMAS["forecast"].schema_version


_BLOB_DIR = "blobs"
_RECEIPT_DIR = "receipts"


class RawStore:
    """Content-addressed raw payloads with one receipt per covered occurrence.

    Layout under ``root``::

        blobs/<hash[:2]>/<hash>.bin          payload bytes, never rewritten
        receipts/<receipt_id>.json           one receipt per occurrence

    ``put`` returns the :class:`Provenance` for the occurrence. When the caller
    supplies a ``record_id`` the call is idempotent: repeating it returns the
    same provenance, and repeating it with different bytes for the same identity
    raises rather than overwriting. When the caller supplies no ``record_id``
    every call is a new occurrence with its own generated identity, even for
    byte-identical payloads.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._blob_dir = self._root / _BLOB_DIR
        self._receipt_dir = self._root / _RECEIPT_DIR

    @property
    def root(self) -> Path:
        return self._root

    def _blob_path(self, raw_hash: str) -> Path:
        if not _HASH_RE.match(raw_hash):
            raise ValueError(f"not a sha256 hash: {raw_hash!r}")
        return self._blob_dir / raw_hash[:2] / f"{raw_hash}.bin"

    def _receipt_path(self, receipt_id: str) -> Path:
        if not _valid_receipt_id(receipt_id):
            raise ValueError(f"invalid receipt id {receipt_id!r}")
        return self._receipt_dir / f"{receipt_id}.json"

    @staticmethod
    def _reject_credentials(metadata: Mapping[str, Any] | None) -> None:
        if not metadata:
            return
        for key in metadata:
            if _CREDENTIAL_KEY_RE.search(str(key)):
                raise ValueError(
                    f"refusing to store metadata key {key!r}: raw payload archives must not "
                    "carry credentials"
                )

    def put(
        self,
        payload: bytes,
        *,
        source: str,
        received_time: dt.datetime,
        record_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        schema_version: str = "1",
    ) -> Provenance:
        """Store ``payload`` and record one occurrence of it.

        ``record_id`` is the source's own occurrence identifier when the feed
        provides one. Without it a fresh identity is generated, so two
        byte-identical payloads stay two occurrences.
        """
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise TypeError(f"put expects bytes, got {type(payload).__name__}")
        if not isinstance(source, str) or not source.strip():
            raise ValueError("put requires a non-empty source")
        if record_id is not None and (not isinstance(record_id, str) or not record_id.strip()):
            raise ValueError("put record_id must be a non-empty str or None")
        received = parse_utc_time(received_time, field_name="RawStore.put.received_time")
        self._reject_credentials(metadata)
        body = bytes(payload)
        raw_hash = hash_bytes(body)
        self._blob_dir.mkdir(parents=True, exist_ok=True)
        self._receipt_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_bytes(self._blob_path(raw_hash), body)

        if record_id is None:
            occurrence_id = f"generated-{uuid.uuid4().hex}"
            receipt_id = f"r-{uuid.uuid4().hex}"
        else:
            occurrence_id = record_id
            receipt_id = (
                "r-"
                + hashlib.blake2b(f"{source}\x1f{record_id}".encode(), digest_size=16).hexdigest()
            )
            existing = self._receipt_path(receipt_id)
            if existing.exists():
                prior = json.loads(existing.read_text(encoding="utf-8"))
                if prior["raw_hash"] != raw_hash:
                    raise ValueError(
                        f"record_id {record_id!r} from {source!r} was already stored with "
                        f"payload {prior['raw_hash']}; the same occurrence cannot have two "
                        "different payloads"
                    )
                return Provenance(raw_hash, occurrence_id, source, schema_version)

        receipt = {
            "receipt_id": receipt_id,
            "raw_hash": raw_hash,
            "record_id": occurrence_id,
            "source": source,
            "received_time": received.isoformat(),
            "stored_at": dt.datetime.now(tz=UTC).isoformat(),
            "metadata": dict(metadata or {}),
            "schema_version": schema_version,
        }
        _atomic_write_bytes(
            self._receipt_path(receipt_id),
            (json.dumps(receipt, sort_keys=True, indent=2) + "\n").encode("utf-8"),
        )
        return Provenance(raw_hash, occurrence_id, source, schema_version)

    def get(self, raw_hash: str) -> bytes:
        """Return stored payload bytes, verifying the content hash."""
        path = self._blob_path(raw_hash)
        if not path.exists():
            raise FileNotFoundError(f"no payload stored for {raw_hash!r} under {self._root}")
        payload = path.read_bytes()
        actual = hash_bytes(payload)
        if actual != raw_hash:
            raise ValueError(
                f"stored payload {path} hashes to {actual}, not {raw_hash}: store is corrupt"
            )
        return payload

    def receipt(self, record_id: str, *, source: str | None = None) -> dict[str, Any] | None:
        """Look up a receipt by occurrence identity, or ``None`` when absent."""
        if source is None:
            matches = [r for r in self.receipts() if r["record_id"] == record_id]
            if len(matches) > 1:
                raise ValueError(
                    f"record_id {record_id!r} appears in several sources; pass source="
                )
            return matches[0] if matches else None
        receipt_id = (
            "r-" + hashlib.blake2b(f"{source}\x1f{record_id}".encode(), digest_size=16).hexdigest()
        )
        path = self._receipt_path(receipt_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def receipts(self, *, raw_hash: str | None = None) -> list[dict[str, Any]]:
        """All receipts, deterministically ordered, optionally for one payload."""
        if raw_hash is not None and not _HASH_RE.match(raw_hash):
            raise ValueError(f"not a sha256 hash: {raw_hash!r}")
        out: list[dict[str, Any]] = []
        if not self._receipt_dir.exists():
            return out
        for path in sorted(self._receipt_dir.glob("*.json")):
            entry = json.loads(path.read_text(encoding="utf-8"))
            if raw_hash is None or entry["raw_hash"] == raw_hash:
                out.append(entry)
        out.sort(key=lambda r: (r["received_time"], r["raw_hash"], r["record_id"]))
        return out

    def stored_hashes(self) -> list[str]:
        """Every stored payload hash, sorted."""
        if not self._blob_dir.exists():
            return []
        return sorted(path.stem for path in self._blob_dir.glob("*/*.bin"))


@dataclass(frozen=True, slots=True)
class DatasetRef:
    """Identity of one sealed dataset file.

    ``content_hash`` is the sha256 of the file's bytes. ``coverage_epoch`` names
    the data vintage the rows came from, so two datasets built from different
    acquisition cutoffs are distinguishable even when their schema matches.
    """

    path: str
    table: str
    schema_version: str
    coverage_epoch: str
    content_hash: str
    row_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path:
            raise ValueError("DatasetRef.path must be a non-empty str")
        if self.table not in TABLE_SCHEMAS:
            raise ValueError(
                f"DatasetRef.table {self.table!r} is not a declared table; "
                f"known tables: {', '.join(sorted(TABLE_SCHEMAS))}"
            )
        if not isinstance(self.schema_version, str) or not self.schema_version:
            raise ValueError("DatasetRef.schema_version must be a non-empty str")
        if not isinstance(self.coverage_epoch, str) or not self.coverage_epoch:
            raise ValueError("DatasetRef.coverage_epoch must be a non-empty str")
        if not _HASH_RE.match(str(self.content_hash)):
            raise ValueError(
                f"DatasetRef.content_hash must be a sha256 hash: {self.content_hash!r}"
            )
        if (
            isinstance(self.row_count, bool)
            or not isinstance(self.row_count, int)
            or self.row_count < 0
        ):
            raise ValueError(
                f"DatasetRef.row_count must be a non-negative int, got {self.row_count!r}"
            )

    @property
    def file(self) -> Path:
        return Path(self.path)

    @property
    def manifest(self) -> dict[str, Any]:
        """The manifest as written next to the dataset."""
        return {
            "table": self.table,
            "schema_version": self.schema_version,
            "coverage_epoch": self.coverage_epoch,
            "content_hash": self.content_hash,
            "row_count": self.row_count,
            "file": self.file.name,
            "columns": list(TABLE_SCHEMAS[self.table].column_names),
        }


def _dotted(record: Any, path: str) -> Any:
    current = record
    for part in path.split("."):
        if current is None:
            return None
        if isinstance(current, Mapping):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


def _project(record: Any, paths: Sequence[tuple[str, str]]) -> dict[str, Any]:
    return {name: _dotted(record, path) for name, path in paths}


def _row_from(record: Any, schema: TableSchema, table: str) -> dict[str, Any]:
    if isinstance(record, Mapping):
        unknown = set(record) - set(schema.column_names)
        if unknown:
            raise ValueError(
                f"row for table {table!r} carries undeclared columns: {sorted(unknown)}"
            )
        return {name: record.get(name) for name in schema.column_names}
    paths = _RECORD_PATHS.get(table)
    if paths is not None and is_dataclass(record) and not isinstance(record, type):
        return _project(record, paths)
    raise TypeError(
        f"table {table!r} rows must be mappings or a supported record type, got "
        f"{type(record).__name__}"
    )


def _json_column_value(name: str, schema: TableSchema, value: Any) -> Any:
    if value is None or name not in schema.json_columns:
        return value
    return _json_dumps(value)


def resolve_rows(rows: Any, table: str) -> list[dict[str, Any]]:
    """Normalize supported input into a list of mapping rows for ``table``.

    Accepts a :class:`pandas.DataFrame`, an iterable of mappings, or an iterable
    of the record dataclass that owns the table. Structured columns declared as
    JSON in the table schema are encoded here, so a caller does not hand-encode
    them.
    """
    if table not in TABLE_SCHEMAS:
        raise ValueError(
            f"unknown table {table!r}; known tables: {', '.join(sorted(TABLE_SCHEMAS))}"
        )
    schema = TABLE_SCHEMAS[table]
    if isinstance(rows, pd.DataFrame):
        unknown = set(rows.columns) - set(schema.column_names)
        if unknown:
            raise ValueError(
                f"frame for table {table!r} carries undeclared columns: {sorted(unknown)}"
            )
        frame = rows
        out: list[dict[str, Any]] = []
        for position in range(len(frame)):
            out.append(
                {
                    name: _json_column_value(name, schema, frame[name].iloc[position])
                    for name in schema.column_names
                    if name in frame.columns
                }
            )
        return out
    out = []
    for record in rows:
        row = _row_from(record, schema, table)
        out.append({name: _json_column_value(name, schema, value) for name, value in row.items()})
    return out


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


def _is_number(value: Any) -> bool:
    """Whether ``value`` is a real number, never a bool.

    A numpy or pandas scalar reaches a sealed table whenever a caller passes a
    DataFrame column value across, so those count. A bool does not: it is a
    distinct Arrow type and silently sealing it as 0.0 or 1.0 would invent a
    measurement.
    """
    if isinstance(value, (bool, np.bool_)):
        return False
    return isinstance(value, (int, float, Decimal, np.integer, np.floating))


def _arrow_array(values: Sequence[Any], dtype: pa.DataType, *, column: str, table: str) -> pa.Array:
    cleaned = [_clean(value) for value in values]
    if pa.types.is_decimal(dtype):
        converted: list[Any] = []
        for value in cleaned:
            if value is None:
                converted.append(None)
            elif isinstance(value, Decimal):
                converted.append(value)
            elif isinstance(value, bool):
                raise TypeError(f"{table}.{column}: bool is not a decimal value")
            elif isinstance(value, (int, str)):
                converted.append(Decimal(value))
            else:
                raise TypeError(
                    f"{table}.{column}: expected an exact decimal for a decimal column, got "
                    f"{type(value).__name__}; convert floats explicitly at the boundary"
                )
        return pa.array(converted, type=dtype)
    if pa.types.is_timestamp(dtype):
        converted = []
        for value in cleaned:
            if value is None:
                converted.append(None)
            elif isinstance(value, dt.datetime):
                if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                    raise ValueError(
                        f"{table}.{column}: naive datetime {value!r} in a UTC timestamp column"
                    )
                converted.append(value.astimezone(UTC))
            else:
                raise TypeError(
                    f"{table}.{column}: expected a timezone-aware datetime, got {type(value).__name__}"
                )
        return pa.array(converted, type=dtype)
    if pa.types.is_boolean(dtype):
        converted = []
        for value in cleaned:
            if value is None or isinstance(value, bool):
                converted.append(value)
            elif hasattr(value, "item") and isinstance(value.item(), bool):
                converted.append(bool(value))
            else:
                raise TypeError(f"{table}.{column}: expected bool, got {type(value).__name__}")
        return pa.array(converted, type=dtype)
    if pa.types.is_integer(dtype):
        converted = []
        for value in cleaned:
            if value is None:
                converted.append(None)
            elif isinstance(value, bool):
                raise TypeError(f"{table}.{column}: bool is not an integer value")
            elif isinstance(value, int):
                converted.append(value)
            elif isinstance(value, float):
                if not float(value).is_integer():
                    raise TypeError(
                        f"{table}.{column}: expected int, got non-integral float {value!r}"
                    )
                converted.append(int(value))
            elif hasattr(value, "item") and isinstance(value.item(), int):
                converted.append(int(value.item()))
            else:
                raise TypeError(f"{table}.{column}: expected int, got {type(value).__name__}")
        return pa.array(converted, type=dtype)
    if pa.types.is_floating(dtype):
        converted = []
        for value in cleaned:
            if value is None:
                converted.append(None)
            elif _is_number(value):
                converted.append(float(value))
            else:
                raise TypeError(f"{table}.{column}: expected a number, got {type(value).__name__}")
        return pa.array(converted, type=dtype)
    converted = []
    for value in cleaned:
        if value is None or isinstance(value, str):
            converted.append(value)
        elif isinstance(value, (int, Decimal)) and not isinstance(value, bool):
            converted.append(str(value))
        else:
            raise TypeError(f"{table}.{column}: expected str, got {type(value).__name__}")
    return pa.array(converted, type=dtype)


def _build_table(rows: Sequence[Mapping[str, Any]], table: str) -> pa.Table:
    schema = TABLE_SCHEMAS[table]
    columns: dict[str, pa.Array] = {}
    for name, dtype in schema.columns:
        values = [row.get(name) for row in rows]
        columns[name] = _arrow_array(values, dtype, column=name, table=table)
    arrow = pa.schema(list(schema.columns))
    return pa.Table.from_arrays([columns[name] for name in schema.column_names], schema=arrow)


def _comparable(value: Any) -> tuple[int, float, str]:
    """A total-order key for one sort column value.

    Numeric values compare numerically. A numpy or pandas scalar is unwrapped
    first, because comparing ``numpy.int64(380)`` against ``numpy.int64(60)`` as
    text would order ``380`` before ``60`` and silently change the row order of a
    sealed dataset.
    """
    if value is None or value is pd.NaT or value is pd.NA:
        return (1, 0.0, "")
    if hasattr(value, "item") and not isinstance(value, (str, bytes, dt.datetime)):
        with contextlib.suppress(AttributeError, ValueError):
            value = value.item()
    if isinstance(value, dt.datetime):
        return (0, value.timestamp(), "")
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        numeric = float(value)
        if math.isnan(numeric):
            return (1, 0.0, "")
        return (0, numeric, "")
    return (0, 0.0, str(value))


def _sort_rows(rows: list[dict[str, Any]], table: str) -> list[dict[str, Any]]:
    schema = TABLE_SCHEMAS[table]
    keys = list(schema.sort_by)
    if not keys or not rows:
        return rows
    return sorted(rows, key=lambda row: tuple(_comparable(row.get(name)) for name in keys))


def write_parquet(
    rows: Any,
    path: str | Path,
    *,
    table: str,
    coverage_epoch: str = "fixture",
    metadata: Mapping[str, Any] | None = None,
) -> DatasetRef:
    """Seal ``rows`` as one immutable Parquet dataset and return its reference.

    ``rows`` is a :class:`pandas.DataFrame` or an iterable of mappings/records
    for a declared table. Rows are sorted deterministically, so the bytes depend
    only on the content. Writing the same content twice is a no-op; writing
    different content to an existing path raises.
    """
    if table not in TABLE_SCHEMAS:
        raise ValueError(
            f"unknown table {table!r}; known tables: {', '.join(sorted(TABLE_SCHEMAS))}"
        )
    if not isinstance(coverage_epoch, str) or not coverage_epoch.strip():
        raise ValueError("coverage_epoch must be a non-empty str")
    target = Path(path)
    resolved = resolve_rows(rows, table)
    missing = _missing_required(resolved, table)
    if missing:
        raise ValueError(
            f"table {table!r} rows are missing required columns: {sorted(missing)}; "
            "a row that cannot be identified cannot be sealed"
        )
    normalized = _sort_rows(resolved, table)
    arrow = _build_table(normalized, table)

    schema = TABLE_SCHEMAS[table]
    extra = {
        f"market_propagation.meta.{k}": str(v) for k, v in (metadata or {}).items() if v is not None
    }
    arrow = arrow.replace_schema_metadata(
        {
            b"market_propagation.table": table.encode(),
            b"market_propagation.schema_version": schema.schema_version.encode(),
            b"market_propagation.coverage_epoch": coverage_epoch.encode(),
            b"market_propagation.row_count": str(len(normalized)).encode(),
            b"market_propagation.declared_columns": _json_dumps(list(schema.column_names)).encode(),
            b"market_propagation.sort_by": _json_dumps(list(schema.sort_by)).encode(),
            **{key.encode(): value.encode() for key, value in extra.items()},
        }
    )

    if target.exists():
        existing = pq.read_schema(target)
        if existing.remove_metadata() != arrow.schema.remove_metadata():
            raise FileExistsError(
                f"refusing to replace sealed dataset {target}: its Arrow schema differs from "
                "the declared schema for this table"
            )
        _verify_manifest_metadata(existing, table)
        buffer = _table_bytes(arrow)
        _atomic_write_bytes(target, buffer)
    else:
        _atomic_write_bytes(target, _table_bytes(arrow))

    content_hash = hash_file(target)
    dataset = DatasetRef(
        path=str(target),
        table=table,
        schema_version=schema.schema_version,
        coverage_epoch=coverage_epoch,
        content_hash=content_hash,
        row_count=len(normalized),
    )
    _write_manifest(dataset)
    return dataset


def _table_bytes(arrow: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(
        arrow,
        sink,
        compression="snappy",
        version="2.6",
        write_statistics=True,
        data_page_size=1 << 20,
    )
    return sink.getvalue().to_pybytes()


def _verify_manifest_metadata(schema: pa.Schema, table: str) -> None:
    meta = schema.metadata or {}
    declared = meta.get(b"market_propagation.table")
    if declared is None:
        raise ValueError(
            f"sealed dataset for table {table!r} carries no market_propagation.table metadata"
        )
    if declared.decode() != table:
        raise ValueError(f"sealed dataset declares table {declared.decode()!r}, expected {table!r}")


def _manifest_path(path: Path) -> Path:
    return path.with_name(path.name + ".manifest.json")


def _write_manifest(dataset: DatasetRef) -> None:
    _atomic_write_bytes(
        _manifest_path(dataset.file),
        (_json_dumps(dataset.manifest) + "\n").encode("utf-8"),
    )


def _decode_json_cell(text: Any) -> Any:
    """Decode one JSON column cell, leaving a stored null as a null.

    A null JSON column arrives from pandas as ``NaN`` rather than ``None``, and
    ``NaN`` is not a string, so a decode that tests only for ``None`` hands a float
    to ``json.loads`` and raises. Absence has to survive the round trip as absence:
    a caller cannot otherwise tell "nothing was recorded" from "the stored text was
    malformed", and a row whose locators were genuinely never recorded would be
    unreadable rather than reported as having none.
    """
    if text is None:
        return None
    if isinstance(text, float) and math.isnan(text):
        return None
    if text is pd.NaT or text is pd.NA:
        return None
    return _json_loads(text)


def read_parquet(path: str | Path, *, table: str | None = None) -> pd.DataFrame:
    """Read a sealed dataset after verifying its schema and content hash.

    ``table`` is inferred from the file's own metadata when not given. A hash
    mismatch, a missing manifest, or a schema that disagrees with the declared
    columns raises: a research panel that cannot be verified is not returned as
    if it were verified.
    """
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"sealed dataset not found: {target}")
    arrow_schema = pq.read_schema(target)
    meta = arrow_schema.metadata or {}
    declared_table = meta.get(b"market_propagation.table")
    if declared_table is None:
        raise ValueError(f"{target} carries no market_propagation.table metadata")
    declared_table_name = declared_table.decode()
    if table is not None and table != declared_table_name:
        raise ValueError(f"{target} declares table {declared_table_name!r}, not {table!r}")
    table = declared_table_name
    if table not in TABLE_SCHEMAS:
        raise ValueError(f"{target} declares unknown table {table!r}")
    expectations = TABLE_SCHEMAS[table]
    sealed_version = meta.get(b"market_propagation.schema_version")
    if sealed_version is None:
        raise ValueError(
            f"{target} carries no market_propagation.schema_version metadata; a sealed dataset "
            "of unknown version is not read"
        )
    if sealed_version.decode() != expectations.schema_version:
        raise ValueError(
            f"{target} was sealed as table {table!r} version {sealed_version.decode()!r}, but "
            f"this build declares version {expectations.schema_version!r}; the sealed bytes keep "
            "their old column types and are refused rather than reinterpreted"
        )
    expected_schema = expectations.arrow_schema
    actual = arrow_schema.remove_metadata()
    if actual != expected_schema:
        mismatched = [
            name
            for name in expectations.column_names
            if name not in actual.names
            or actual.field(name).type != expected_schema.field(name).type
        ]
        raise ValueError(
            f"{target} schema does not match table {table!r} version "
            f"{expectations.schema_version}; mismatched columns: {mismatched or 'unknown'}"
        )

    manifest_file = _manifest_path(target)
    if not manifest_file.exists():
        raise ValueError(f"{target} has no manifest at {manifest_file}; refusing to trust it")
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    actual_hash = hash_file(target)
    if manifest.get("content_hash") != actual_hash:
        raise ValueError(
            f"{target} content hash {actual_hash} does not match its manifest "
            f"{manifest.get('content_hash')!r}: the sealed dataset was modified"
        )
    if manifest.get("table") != table:
        raise ValueError(
            f"{target} manifest declares table {manifest.get('table')!r}, not {table!r}"
        )

    frame = pq.read_table(target).to_pandas()
    for name in expectations.json_columns:
        if name in frame.columns:
            frame[name] = frame[name].map(_decode_json_cell)
    return frame


def _quoted(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def duckdb_connection(**kwargs: Any):
    """A local in-memory DuckDB connection for queries over sealed datasets."""
    import duckdb

    return duckdb.connect(database=":memory:", **kwargs)


def query_sealed(
    dataset: DatasetRef | str | Path,
    sql: str,
    *,
    parameters: Sequence[Any] | None = None,
    connection: Any = None,
) -> pd.DataFrame:
    """Run ``sql`` with the sealed dataset exposed under its table name.

    The dataset's hash is verified first, so a query cannot silently run over a
    file that changed after it was sealed.
    """
    reference = dataset if isinstance(dataset, DatasetRef) else None
    if reference is None:
        path = Path(dataset)
        schema = pq.read_schema(path)
        declared = (schema.metadata or {}).get(b"market_propagation.table")
        if declared is None:
            raise ValueError(f"{path} carries no market_propagation.table metadata")
        table = declared.decode()
        dataset_path = path
    else:
        table = reference.table
        dataset_path = reference.file
    if table not in TABLE_SCHEMAS or not _NAME_RE.match(table):
        raise ValueError(f"refusing to expose table name {table!r} to SQL")
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("query_sealed requires a non-empty SQL string")
    if not dataset_path.exists():
        raise FileNotFoundError(f"sealed dataset not found: {dataset_path}")
    if reference is not None:
        actual = hash_file(dataset_path)
        if actual != reference.content_hash:
            raise ValueError(
                f"sealed dataset {dataset_path} hash {actual} does not match its DatasetRef "
                f"{reference.content_hash}: refusing to query modified content"
            )

    owned = connection is None
    con = duckdb_connection() if owned else connection
    try:
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet({_quoted(dataset_path)})"
        )
        if parameters is None:
            return con.execute(sql).fetchdf()
        return con.execute(sql, list(parameters)).fetchdf()
    finally:
        if owned:
            con.close()
