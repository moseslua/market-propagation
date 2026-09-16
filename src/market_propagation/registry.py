"""Experiment registry: recorded runs and one-shot locked-test reservations.

The registry is the durable evidence behind this study's reproducibility and
cohort-reuse claims, so it records only what a caller states. Every hash is
supplied; a missing hash, an undeclared field, an empty or repeated event
cohort, and a non-finite number are rejected at this boundary rather than stored
as a placeholder that would later read as known provenance.

Writes that repeat stored state are idempotent, with one exception: a locked-test
reservation is one-shot, so reserving a specification again raises instead of
replaying its token and authorizing a second evaluation. A write that changes
state under the same identity raises instead of overwriting evidence. SQLite
transactions plus unique constraints keep one reservation per specification and
one claim per event across every specification version: a conflict rolls back
completely, so a rejected reservation leaves no partial cohort behind.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import sqlite3
import tempfile
import uuid
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from .domain import UTC, parse_utc_time

__all__ = [
    "FINISH_STATUSES",
    "RUN_FIELDS",
    "ExperimentRegistry",
    "ExperimentRegistryError",
    "RegistryConflictError",
]

# Declared run-record fields. Anything else is rejected, so a mistyped
# provenance key cannot be silently dropped from the stored evidence.
RUN_FIELDS = (
    "run_id",
    "spec_hash",
    "data_hash",
    "source_hash",
    "environment_hash",
    "event_ids",
    "seed",
    "metrics",
    "synthetic",
    "created_at",
)
_HASH_FIELDS = ("spec_hash", "data_hash", "source_hash", "environment_hash")
_REQUIRED_FIELDS = (*_HASH_FIELDS, "event_ids", "metrics", "synthetic")
FINISH_STATUSES = ("complete", "failed")
_RESERVED = "reserved"
_CLAIM_CHUNK = 500


class _Missing:
    """Sentinel that compares unequal to every stored record value."""


_MISSING = _Missing()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
    token TEXT PRIMARY KEY,
    spec_hash TEXT NOT NULL UNIQUE,
    dataset_hash TEXT NOT NULL,
    event_ids TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('reserved', 'complete', 'failed')),
    result TEXT,
    reserved_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS event_claims (
    event_id TEXT PRIMARY KEY,
    token TEXT NOT NULL,
    spec_hash TEXT NOT NULL,
    dataset_hash TEXT NOT NULL
);
"""


class ExperimentRegistryError(ValueError):
    """A record or request violates the registry contract, so nothing is stored."""


class RegistryConflictError(ExperimentRegistryError):
    """A write contradicts state the registry already holds."""


def _canonical_json(value: Any, *, field_name: str) -> str:
    """Canonical JSON text, or a rejection that names the offending field.

    ``allow_nan=False`` is what keeps a NaN out of the store: IEEE non-finite
    values are legal Python floats and illegal JSON numbers, and a stored NaN
    would read back as a measurement rather than as missing.
    """
    try:
        return json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ExperimentRegistryError(
            f"{field_name}: not finite JSON, refusing to store it: {exc}"
        ) from exc


def _text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ExperimentRegistryError(f"{field_name}: expected str, got {type(value).__name__}")
    text = value.strip()
    if not text:
        raise ExperimentRegistryError(
            f"{field_name}: empty value; the registry records what the caller observed "
            "and never supplies a provenance value on its own"
        )
    return text


def _event_ids(value: object, *, field_name: str) -> list[str]:
    """Validate an event cohort and return it as a sorted unique list.

    Cohort identity is the event set, so duplicates are rejected rather than
    collapsed and the stored order is canonical. An empty cohort claims nothing
    and is rejected.
    """
    if isinstance(value, (str, bytes, Mapping)) or not isinstance(value, Sequence):
        raise ExperimentRegistryError(
            f"{field_name}: expected a sequence of event ids, got {type(value).__name__}"
        )
    ids = [_text(item, field_name=f"{field_name}[{index}]") for index, item in enumerate(value)]
    repeated = sorted(event_id for event_id, count in Counter(ids).items() if count > 1)
    if repeated:
        raise ExperimentRegistryError(
            f"{field_name}: repeated event ids {repeated}; a cohort claims each event once"
        )
    if not ids:
        raise ExperimentRegistryError(
            f"{field_name}: empty cohort; a locked test with no events is not run"
        )
    return sorted(ids)


def _mapping(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ExperimentRegistryError(
            f"{field_name}: expected a mapping, got {type(value).__name__}"
        )
    for key in value:
        if not isinstance(key, str):
            raise ExperimentRegistryError(f"{field_name}: keys must be strings, got {key!r}")
    return dict(value)


def _seed(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExperimentRegistryError(f"seed: expected int or None, got {type(value).__name__}")
    return value


def _instant(value: object, *, field_name: str) -> str:
    if not isinstance(value, (dt.datetime, str)):
        raise ExperimentRegistryError(
            f"{field_name}: expected an ISO-8601 string or aware datetime, "
            f"got {type(value).__name__}"
        )
    try:
        return parse_utc_time(value, field_name=field_name).isoformat()
    except (TypeError, ValueError) as exc:
        raise ExperimentRegistryError(str(exc)) from exc


def _now() -> str:
    return dt.datetime.now(UTC).isoformat()


def _run_payload(record: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    """Normalize a run record and report whether its ``created_at`` was stamped."""
    unknown = sorted(set(record) - set(RUN_FIELDS))
    if unknown:
        raise ExperimentRegistryError(f"run record carries undeclared fields: {unknown}")
    missing = sorted(name for name in _REQUIRED_FIELDS if name not in record)
    if missing:
        raise ExperimentRegistryError(
            f"run record is missing required provenance fields: {missing}; "
            "an unavailable hash is not defaulted"
        )
    payload: dict[str, Any] = dict(record)
    for name in _HASH_FIELDS:
        payload[name] = _text(record[name], field_name=name)
    payload["event_ids"] = _event_ids(record["event_ids"], field_name="event_ids")
    payload["seed"] = _seed(record.get("seed"))
    payload["metrics"] = _mapping(record["metrics"], field_name="metrics")
    if not isinstance(record["synthetic"], bool):
        raise ExperimentRegistryError(
            "synthetic: expected bool, got "
            f"{type(record['synthetic']).__name__}; the registry never assumes a run is real data"
        )
    run_id = record.get("run_id")
    payload["run_id"] = uuid.uuid4().hex if run_id is None else _text(run_id, field_name="run_id")
    stamped = record.get("created_at") is None
    payload["created_at"] = (
        _now() if stamped else _instant(record["created_at"], field_name="created_at")
    )
    _canonical_json(payload["metrics"], field_name="metrics")
    return payload, stamped


class ExperimentRegistry:
    """Durable runs and locked-test reservations in one SQLite file.

    A registry opened twice on the same path sees the same state: every read
    decodes stored rows, so ``snapshot`` is the durable truth rather than a
    process-local cache.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        if self._path == ":memory:":
            target = self._path
        else:
            target = str(Path(self._path).expanduser())
            Path(target).parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = sqlite3.connect(target, isolation_level=None)
        self._conn.executescript(_SCHEMA)

    def __enter__(self) -> ExperimentRegistry:
        self._connection()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the connection. Idempotent; later calls raise."""
        connection, self._conn = self._conn, None
        if connection is not None:
            connection.close()

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise ExperimentRegistryError(f"registry {self._path} is closed")
        return self._conn

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """Immediate-write transaction: commits as a unit or rolls back whole."""
        connection = self._connection()
        connection.execute("BEGIN IMMEDIATE")
        with connection:
            yield connection

    def record_run(self, record: Mapping[str, Any]) -> str:
        """Store one run record and return its run id.

        An explicit ``run_id`` with identical content is idempotent. The same
        ``run_id`` with changed content raises. An omitted ``created_at`` is
        stamped once, and a later replay that still omits it reuses the stored
        stamp instead of inventing a new one.
        """
        if not isinstance(record, Mapping):
            raise ExperimentRegistryError(
                f"run record: expected a mapping, got {type(record).__name__}"
            )
        payload, stamped = _run_payload(record)
        run_id = payload["run_id"]
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT payload FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is not None:
                stored = json.loads(row[0])
                if stamped:
                    payload["created_at"] = stored.get("created_at", payload["created_at"])
                if stored == payload:
                    return run_id
                changed = sorted(
                    key for key in payload if stored.get(key, _MISSING) != payload[key]
                )
                raise RegistryConflictError(
                    f"run {run_id!r} is already recorded with different content in {changed}; "
                    "a recorded run is never rewritten"
                )
            connection.execute(
                "INSERT INTO runs (run_id, payload) VALUES (?, ?)",
                (run_id, _canonical_json(payload, field_name="run record")),
            )
        return run_id

    def _run_payloads(self) -> list[dict[str, Any]]:
        rows = [
            json.loads(row[0]) for row in self._connection().execute("SELECT payload FROM runs")
        ]
        return sorted(rows, key=lambda payload: (payload["created_at"], payload["run_id"]))

    def reserve_locked_test(
        self, spec_hash: str, event_ids: Sequence[str], *, dataset_hash: str
    ) -> str:
        """Claim the one locked-test cohort for ``spec_hash`` and return its token.

        A specification version is evaluated once. Every later reservation for
        the same ``spec_hash`` raises, whether the arguments repeat the first
        call exactly or differ, and whether the stored attempt is still reserved
        or already finished: a returned token authorizes an evaluation, so a
        replay would buy a second one. ``snapshot`` is the read-only way to
        recover the recorded reservation.

        The reservation is written with its event claims in a single
        transaction, so a conflict over any event leaves neither a reservation
        nor a claim behind.
        """
        spec = _text(spec_hash, field_name="spec_hash")
        dataset = _text(dataset_hash, field_name="dataset_hash")
        events = _event_ids(event_ids, field_name="event_ids")
        events_json = _canonical_json(events, field_name="event_ids")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT token FROM reservations WHERE spec_hash = ?", (spec,)
            ).fetchone()
            if row is not None:
                raise RegistryConflictError(
                    f"specification {spec} already holds reservation {row[0]}; a specification "
                    "version is evaluated once, so a second reservation is refused and the "
                    "recorded attempt is read back with snapshot()"
                )
            claimed = self._claimed_events(connection, events)
            if claimed:
                raise RegistryConflictError(
                    f"events {sorted(claimed)} are already claimed by another specification; "
                    "a locked-test cohort is consumed once and never reused"
                )
            token = uuid.uuid4().hex
            connection.execute(
                "INSERT INTO reservations "
                "(token, spec_hash, dataset_hash, event_ids, status, result, reserved_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?, NULL, ?, NULL)",
                (token, spec, dataset, events_json, _RESERVED, _now()),
            )
            connection.executemany(
                "INSERT INTO event_claims (event_id, token, spec_hash, dataset_hash) VALUES (?, ?, ?, ?)",
                [(event_id, token, spec, dataset) for event_id in events],
            )
        return token

    def finish_locked_test(self, token: str, *, status: str, result: Mapping[str, Any]) -> None:
        """Close a reservation as ``complete`` or ``failed``, once.

        A failed attempt keeps its reservation and its event claims, so the
        cohort stays consumed and a corrected specification still needs a fresh
        one. A finished reservation is never reopened: replaying the identical
        outcome is a no-op, anything else raises.
        """
        token_text = _text(token, field_name="token")
        if status not in FINISH_STATUSES:
            raise ExperimentRegistryError(
                f"status: expected one of {list(FINISH_STATUSES)}, got {status!r}; "
                "a reservation is never returned to the reserved state"
            )
        stored_result = _canonical_json(_mapping(result, field_name="result"), field_name="result")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, result FROM reservations WHERE token = ?", (token_text,)
            ).fetchone()
            if row is None:
                raise ExperimentRegistryError(f"no reservation for token {token_text!r}")
            existing_status, existing_result = row
            if existing_status != _RESERVED:
                if existing_status == status and existing_result == stored_result:
                    return
                raise RegistryConflictError(
                    f"reservation {token_text} already finished as {existing_status!r}; "
                    "a finished locked test is never reopened or rewritten"
                )
            connection.execute(
                "UPDATE reservations SET status = ?, result = ?, finished_at = ? WHERE token = ?",
                (status, stored_result, _now(), token_text),
            )

    @staticmethod
    def _claimed_events(connection: sqlite3.Connection, events: Sequence[str]) -> list[str]:
        claimed: list[str] = []
        for start in range(0, len(events), _CLAIM_CHUNK):
            chunk = tuple(events[start : start + _CLAIM_CHUNK])
            marks = ",".join("?" * len(chunk))
            claimed.extend(
                row[0]
                for row in connection.execute(
                    f"SELECT event_id FROM event_claims WHERE event_id IN ({marks})", chunk
                )
            )
        return claimed

    def snapshot(self) -> dict[str, Any]:
        """Decoded durable state: runs, reservations and event claims."""
        connection = self._connection()
        reservations: list[dict[str, Any]] = []
        for (
            token,
            spec_hash,
            dataset_hash,
            event_ids,
            status,
            result,
            reserved_at,
            finished_at,
        ) in connection.execute(
            "SELECT token, spec_hash, dataset_hash, event_ids, status, result, "
            "reserved_at, finished_at FROM reservations"
        ):
            reservations.append(
                {
                    "token": token,
                    "spec_hash": spec_hash,
                    "dataset_hash": dataset_hash,
                    "event_ids": json.loads(event_ids),
                    "status": status,
                    "result": None if result is None else json.loads(result),
                    "reserved_at": reserved_at,
                    "finished_at": finished_at,
                }
            )
        reservations.sort(key=lambda item: (item["reserved_at"], item["token"]))
        claims = [
            {
                "event_id": event_id,
                "token": token,
                "spec_hash": spec_hash,
                "dataset_hash": dataset_hash,
            }
            for event_id, token, spec_hash, dataset_hash in connection.execute(
                "SELECT event_id, token, spec_hash, dataset_hash FROM event_claims ORDER BY event_id"
            )
        ]
        return {"runs": self._run_payloads(), "reservations": reservations, "event_claims": claims}

    def export_jsonl(self, path: str | Path) -> None:
        """Write one canonical JSON line per recorded run, atomically.

        The bytes land by replacing a temporary file in the destination
        directory, so a reader never sees a half-written registry export.
        """
        target = Path(path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        body = "".join(
            _canonical_json(payload, field_name="run record") + "\n"
            for payload in self._run_payloads()
        ).encode("utf-8")
        handle, temporary = tempfile.mkstemp(
            dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(handle, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(temporary)
            raise
