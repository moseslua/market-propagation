"""Point-in-time release expectations, validated before any surprise is computed.

The propagation comparison needs a surprise term: without one, a shock that moved
both contracts at once is indistinguishable from transmission between them, and
the network rung would report a common cause as a neighbour effect. A surprise is
the release's first-print actual minus a forecast that was public *before* the
release, so the whole term rests on the forecast's provenance rather than on its
number. This module validates that provenance and refuses the substitutions that
would manufacture a surprise out of evidence that cannot support one.

Five refusals are load-bearing, and each of them is a way a real study invents a
predictor it never had:

**A forecast published at or after the release is not a forecast.** It is a
restatement of the outcome, and admitting it would let the surprise column carry
the answer.

**A revised actual is not the first print.** The release dataset carries both, and
a forecast scored against a revision measures the revision rather than the news
the release delivered. A statistic that only exists in the revisions block is
refused even though its name resolves in the same document.

**A unit mismatch is a thousand-fold error, not a rounding difference.**
``payrolls_change_jobs`` and ``payrolls_change_thousands`` are one release
statistic in two units, so the unit is compared against the release's own
declared statistic and never inferred from the magnitude of the number.

**An unnamed consensus is an assertion.** The record has to name who published the
expectation and by what method it was verified, because a numeric expectation
with no identity cannot be re-derived by a reader.

**Evidence bytes are identified, not described.** Every record carries the sha256
of the exact bytes it was read from, and the loader re-reads those bytes and
compares. A payload edited after the record was written stops validating instead
of inheriting its earlier verdict.

No expectation source is packaged with this repository, and this module states
that as an absence rather than filling it: :func:`load_expectations` raises
:class:`ExpectationSourceError` when the configured source is missing, and every
caller reports the missing input instead of measuring against a substituted news
vector.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ..domain import UTC, Clock, Expectation, Provenance
from ..storage import read_parquet

#: The clock basis a point-in-time expectation is read on. The publication instant
#: is its source time and there is no receipt, so the availability interval stays
#: unknown: an archived forecast was not observed arriving.
CLOCK_BASIS_POINT_IN_TIME = "point_in_time_publication"

#: The unit vocabulary a statistic may be stated in. Units are declared, never
#: inferred: the same release statistic appears in the archive in more than one
#: unit, so a magnitude cannot stand in for a unit.
UNITS: tuple[str, ...] = (
    "count",
    "thousands_of_count",
    "percent",
    "percent_change",
    "usd",
    "hours",
    "index_level",
)

#: Refusal codes. Each names one distinct way an expectation fails to be
#: point-in-time evidence, so a blocked run reports which one applied.
REFUSAL_EVIDENCE_BYTES_MISMATCH = "evidence_bytes_do_not_match_the_recorded_digest"
REFUSAL_EVIDENCE_UNREADABLE = "recorded_evidence_bytes_could_not_be_read"
REFUSAL_POST_RELEASE_FORECAST = "expectation_published_at_or_after_the_release"
REFUSAL_REVISED_ACTUAL_TARGET = "expectation_targets_a_revised_actual_not_the_first_print"
REFUSAL_UNIT_MISMATCH = "unit_does_not_match_the_release_statistic"
REFUSAL_UNIT_UNDECLARED = "release_declares_no_unit_for_the_statistic"
REFUSAL_REFERENCE_PERIOD_MISMATCH = "reference_period_does_not_match_the_release"
REFUSAL_UNKNOWN_STATISTIC = "statistic_is_not_declared_by_the_release"
REFUSAL_MARKET_IMPLIED = "market_implied_expectation_cannot_validate_the_market"
REFUSAL_UNNAMED_CONSENSUS = "consensus_identity_is_not_named"
REFUSAL_UNNAMED_VERIFIER = "verification_method_is_not_named"
REFUSAL_UNVERIFIED_SOURCE_KIND = "source_kind_is_not_an_admissible_route"
REFUSAL_INCOMPLETE_NEWS_VECTOR = "news_vector_is_incomplete"
REFUSAL_NO_SURPRISE_SCALE = "no_unit_scale_is_stated_for_the_statistic"
REFUSAL_SOURCE_ABSENT = "expectation_source_is_absent"
REFUSAL_SOURCE_MALFORMED = "expectation_source_is_malformed"

#: The refusals a caller can branch on, in the order this module applies them.
REFUSALS: tuple[str, ...] = (
    REFUSAL_EVIDENCE_UNREADABLE,
    REFUSAL_EVIDENCE_BYTES_MISMATCH,
    REFUSAL_POST_RELEASE_FORECAST,
    REFUSAL_REVISED_ACTUAL_TARGET,
    REFUSAL_UNKNOWN_STATISTIC,
    REFUSAL_REFERENCE_PERIOD_MISMATCH,
    REFUSAL_UNIT_MISMATCH,
    REFUSAL_UNIT_UNDECLARED,
    REFUSAL_MARKET_IMPLIED,
    REFUSAL_UNVERIFIED_SOURCE_KIND,
    REFUSAL_UNNAMED_CONSENSUS,
    REFUSAL_UNNAMED_VERIFIER,
)


class ExpectationError(ValueError):
    """A refusal, carrying the code that says which one applied."""

    def __init__(self, refusal: str, detail: str) -> None:
        if refusal not in REFUSALS and refusal not in (
            REFUSAL_INCOMPLETE_NEWS_VECTOR,
            REFUSAL_NO_SURPRISE_SCALE,
            REFUSAL_SOURCE_ABSENT,
            REFUSAL_SOURCE_MALFORMED,
        ):
            raise ValueError(f"{refusal!r} is not a declared refusal code")
        super().__init__(f"{refusal}: {detail}")
        self.refusal = refusal
        self.detail = detail


class ExpectationSourceError(ExpectationError):
    """The expectation source itself is absent or unreadable."""


def _text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty str, got {value!r}")
    return value


def _instant(value: object, *, field_name: str) -> dt.datetime:
    if isinstance(value, dt.datetime):
        moment = value
    elif isinstance(value, str):
        try:
            moment = dt.datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} is not an ISO-8601 instant: {value!r}") from exc
    else:
        raise TypeError(f"{field_name} must be an ISO-8601 string or datetime, got {value!r}")
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _decimal(value: object, *, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} is not a decimal: {value!r}") from exc


#: The unit each declared statistic suffix states. The archive states a release
#: statistic's unit in its own name, so this table reads that declaration rather
#: than a second one invented here. A suffix that is absent from this mapping means
#: the release declares no unit this module can read, which is a refusal rather
#: than a default.
_DECLARED_UNIT_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("_index_level", "index_level"),
    ("_thousands", "thousands_of_count"),
    ("_jobs", "count"),
    ("_hours", "hours"),
    ("_usd", "usd"),
    ("_mom_pct", "percent_change"),
    ("_yoy_pct", "percent_change"),
    ("_rate_pct", "percent"),
    ("_pct", "percent_change"),
)


def declared_unit(statistic: str) -> str | None:
    """The unit a release statistic's own declared name states, or ``None``.

    ``payrolls_change_jobs`` and ``payrolls_change_thousands`` are one statistic in
    two units, so a magnitude can never stand in for a unit and a name with no
    declared suffix yields no unit at all.
    """
    name = str(statistic)
    for suffix, unit in _DECLARED_UNIT_SUFFIXES:
        if name.endswith(suffix):
            return unit
    return None


@dataclass(frozen=True, slots=True)
class ReleaseFacts:
    """One archived release as the expectation validator needs to read it.

    ``statistics`` holds the first-print actuals the release published and
    ``revised_statistics`` the statistics that exist only in its revisions block.
    They are kept apart because a forecast scored against a revision measures the
    revision rather than the news the release delivered.
    """

    event_id: str
    family: str
    release_time: dt.datetime
    reference_period: str
    statistics: Mapping[str, Decimal]
    revised_statistics: tuple[str, ...]
    raw_hash: str

    def __post_init__(self) -> None:
        for name in ("event_id", "family", "reference_period", "raw_hash"):
            _text(getattr(self, name), field_name=f"ReleaseFacts.{name}")
        object.__setattr__(self, "statistics", dict(self.statistics))
        object.__setattr__(self, "revised_statistics", tuple(sorted(set(self.revised_statistics))))

    def first_print(self, statistic: str) -> Decimal | None:
        """The first-print actual for one statistic, or ``None`` when it has none."""
        return self.statistics.get(str(statistic))

    @property
    def statistics_units(self) -> dict[str, str | None]:
        """Every declared statistic with the unit its own name states."""
        return {name: declared_unit(name) for name in sorted(self.statistics)}


def _decode_statistics(value: Any, *, field_name: str) -> dict[str, Decimal]:
    """A release's declared statistics as exact decimals."""
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} is not readable JSON: {exc}") from exc
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping, got {type(value).__name__}")
    return {
        str(name): _decimal(item, field_name=f"{field_name}[{name!r}]")
        for name, item in value.items()
    }


def load_release_facts(path: str | Path) -> dict[str, ReleaseFacts]:
    """Read the sealed release dataset as the facts an expectation is validated against.

    The read verifies the dataset's declared schema and its content hash before any
    row is used, so a release table whose bytes changed is refused rather than read.
    """
    frame = read_parquet(path)
    facts: dict[str, ReleaseFacts] = {}
    for row in frame.to_dict("records"):
        event_id = str(row["event_id"])
        facts[event_id] = ReleaseFacts(
            event_id=event_id,
            family=str(row["family"]),
            release_time=_instant(row["scheduled_at"], field_name=f"{event_id}.scheduled_at"),
            reference_period=str(row["reference_period"]),
            statistics=_decode_statistics(row.get("values_json"), field_name=f"{event_id}.values"),
            revised_statistics=tuple(
                _decode_statistics(row.get("revisions_json"), field_name=f"{event_id}.revisions")
            ),
            raw_hash=str(row.get("raw_hash") or ""),
        )
    return facts


@dataclass(frozen=True, slots=True)
class ExpectationRecord:
    """One expectation as it was recorded, before any of it is believed.

    The record is deliberately a separate type from the validated
    :class:`~market_propagation.domain.Expectation`: an unvalidated row must not be
    able to reach a fit by being constructed.
    """

    event_id: str
    statistic: str
    unit: str
    reference_period: str
    value: Decimal
    published_at: dt.datetime
    source_kind: str
    revision_status: str
    consensus_id: str
    verified_by: str
    source_url: str
    evidence_path: str
    evidence_sha256: str

    @classmethod
    def from_mapping(cls, item: Mapping[str, Any], *, where: str) -> ExpectationRecord:
        """One record from a decoded source document, with every field required."""
        if not isinstance(item, Mapping):
            raise ExpectationSourceError(
                REFUSAL_SOURCE_MALFORMED, f"{where} is not a mapping, got {type(item).__name__}"
            )
        missing = sorted(
            name
            for name in (
                "event_id",
                "statistic",
                "unit",
                "reference_period",
                "value",
                "published_at",
                "source_kind",
                "revision_status",
                "consensus_id",
                "verified_by",
                "source_url",
                "evidence_path",
                "evidence_sha256",
            )
            if item.get(name) in (None, "")
        )
        if missing:
            raise ExpectationSourceError(
                REFUSAL_SOURCE_MALFORMED,
                f"{where} is missing required field(s) {missing}; a record that does not state "
                "them cannot be validated and is not defaulted",
            )
        return cls(
            event_id=_text(item["event_id"], field_name=f"{where}.event_id"),
            statistic=_text(item["statistic"], field_name=f"{where}.statistic"),
            unit=_text(item["unit"], field_name=f"{where}.unit"),
            reference_period=_text(
                item["reference_period"], field_name=f"{where}.reference_period"
            ),
            value=_decimal(item["value"], field_name=f"{where}.value"),
            published_at=_instant(item["published_at"], field_name=f"{where}.published_at"),
            source_kind=_text(item["source_kind"], field_name=f"{where}.source_kind"),
            revision_status=_text(item["revision_status"], field_name=f"{where}.revision_status"),
            consensus_id=_text(item["consensus_id"], field_name=f"{where}.consensus_id"),
            verified_by=_text(item["verified_by"], field_name=f"{where}.verified_by"),
            source_url=_text(item["source_url"], field_name=f"{where}.source_url"),
            evidence_path=_text(item["evidence_path"], field_name=f"{where}.evidence_path"),
            evidence_sha256=_text(item["evidence_sha256"], field_name=f"{where}.evidence_sha256"),
        )


def _evidence_digest(record: ExpectationRecord, *, evidence_root: str | Path) -> str:
    """The sha256 of the bytes the record says it was read from.

    The path is resolved under the declared root so a record cannot point the
    validator at a different file than the one it was recorded against.
    """
    root = Path(evidence_root)
    candidate = (root / record.evidence_path).resolve()
    if root.resolve() not in candidate.parents and candidate != root.resolve():
        raise ExpectationError(
            REFUSAL_EVIDENCE_UNREADABLE,
            f"record {record.event_id!r} names evidence path {record.evidence_path!r}, which "
            f"resolves outside the declared evidence root {str(root)!r}",
        )
    try:
        payload = candidate.read_bytes()
    except OSError as exc:
        raise ExpectationError(
            REFUSAL_EVIDENCE_UNREADABLE,
            f"record {record.event_id!r} names evidence {record.evidence_path!r}, which could not "
            f"be read: {type(exc).__name__}: {exc}",
        ) from exc
    return hashlib.sha256(payload).hexdigest()


def validate_expectation(
    record: ExpectationRecord,
    facts: ReleaseFacts,
    *,
    evidence_root: str | Path,
) -> Expectation:
    """One recorded expectation as a validated point-in-time expectation, or a refusal.

    Every check is applied before any value is read, because the value is the part a
    study is tempted to keep when the provenance fails.
    """
    digest = _evidence_digest(record, evidence_root=evidence_root)
    if digest != record.evidence_sha256:
        raise ExpectationError(
            REFUSAL_EVIDENCE_BYTES_MISMATCH,
            f"record for {record.event_id!r} records evidence digest "
            f"{record.evidence_sha256!r} and the bytes at {record.evidence_path!r} hash to "
            f"{digest!r}; the payload changed after the record was written",
        )
    if record.source_kind not in Expectation.SOURCE_KINDS:
        raise ExpectationError(
            REFUSAL_UNVERIFIED_SOURCE_KIND,
            f"record for {record.event_id!r} states source_kind {record.source_kind!r}, and the "
            f"admissible routes are {list(Expectation.SOURCE_KINDS)}",
        )
    if record.source_kind == Expectation.MARKET_IMPLIED:
        raise ExpectationError(
            REFUSAL_MARKET_IMPLIED,
            f"record for {record.event_id!r} is market-implied, and a market-implied expectation "
            "cannot validate the market it is derived from",
        )
    if not record.consensus_id.strip():
        raise ExpectationError(
            REFUSAL_UNNAMED_CONSENSUS,
            f"record for {record.event_id!r} names no consensus identity",
        )
    if not record.verified_by.strip():
        raise ExpectationError(
            REFUSAL_UNNAMED_VERIFIER,
            f"record for {record.event_id!r} names no verification method",
        )
    if record.published_at >= facts.release_time:
        raise ExpectationError(
            REFUSAL_POST_RELEASE_FORECAST,
            f"record for {record.event_id!r} was published at {record.published_at.isoformat()} "
            f"and the release was scheduled for {facts.release_time.isoformat()}; a forecast "
            "published at or after the release restates the outcome",
        )
    if record.revision_status != "initial":
        raise ExpectationError(
            REFUSAL_REVISED_ACTUAL_TARGET,
            f"record for {record.event_id!r} states revision_status "
            f"{record.revision_status!r}; a forecast scored against a revision measures the "
            "revision rather than the news the release delivered",
        )
    if record.statistic not in facts.statistics:
        if record.statistic in facts.revised_statistics:
            raise ExpectationError(
                REFUSAL_REVISED_ACTUAL_TARGET,
                f"record for {record.event_id!r} names {record.statistic!r}, which the release "
                "publishes only in its revisions block and not as a first print",
            )
        raise ExpectationError(
            REFUSAL_UNKNOWN_STATISTIC,
            f"record for {record.event_id!r} names statistic {record.statistic!r}, which the "
            f"release does not declare; it declares {sorted(facts.statistics)}",
        )
    if record.reference_period != facts.reference_period:
        raise ExpectationError(
            REFUSAL_REFERENCE_PERIOD_MISMATCH,
            f"record for {record.event_id!r} states reference period "
            f"{record.reference_period!r} and the release states {facts.reference_period!r}",
        )
    stated = declared_unit(record.statistic)
    if stated is None:
        raise ExpectationError(
            REFUSAL_UNIT_UNDECLARED,
            f"the release declares no unit for statistic {record.statistic!r}, so a stated unit "
            "cannot be checked against it",
        )
    if record.unit != stated:
        raise ExpectationError(
            REFUSAL_UNIT_MISMATCH,
            f"record for {record.event_id!r} states unit {record.unit!r} and the release's "
            f"statistic {record.statistic!r} states {stated!r}",
        )
    return Expectation(
        event_id=record.event_id,
        statistic=record.statistic,
        value=record.value,
        source_kind=record.source_kind,
        clock=Clock.historical(record.published_at),
        provenance=Provenance(
            raw_hash=digest,
            record_id=f"{record.event_id}:{record.statistic}",
            source=record.source_url,
        ),
        revision_status=record.revision_status,
    )


def surprise(expectation: Expectation, facts: ReleaseFacts) -> Decimal | None:
    """The release's first-print actual minus its validated expectation, or ``None``.

    The difference is stated in the release's own unit, which the validation already
    fixed; a missing first print yields no surprise rather than a zero.
    """
    actual = facts.first_print(expectation.statistic)
    if actual is None:
        return None
    return actual - expectation.value


def load_expectations(
    path: str | Path,
    *,
    facts: Mapping[str, ReleaseFacts],
    statistics: Sequence[str],
    evidence_root: str | Path,
) -> dict[str, dict[str, Expectation]]:
    """Read, validate and assemble the declared news vector, or state what is missing.

    ``statistics`` is the declared news vector: the release statistics the study
    needs an expectation for. A release whose vector is incomplete is refused rather
    than fitted with a subset, because a news model on a partial vector attributes
    the missing term's variation to whatever columns are present.
    """
    source_path = Path(path)
    if not source_path.exists():
        raise ExpectationSourceError(
            REFUSAL_SOURCE_ABSENT,
            f"the expectation source {str(source_path)!r} does not exist; no surprise is "
            "estimable without a point-in-time expectation, and an absent forecast is not a "
            "zero-valued surprise",
        )
    try:
        document = json.loads(source_path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ExpectationSourceError(
            REFUSAL_SOURCE_MALFORMED,
            f"the expectation source {str(source_path)!r} is not readable JSON: {exc}",
        ) from exc
    if not isinstance(document, Mapping) or not isinstance(document.get("records"), Iterable):
        raise ExpectationSourceError(
            REFUSAL_SOURCE_MALFORMED,
            f"the expectation source {str(source_path)!r} must be a mapping with a 'records' list",
        )
    wanted = tuple(dict.fromkeys(str(name) for name in statistics))
    validated: dict[str, dict[str, Expectation]] = {}
    for position, item in enumerate(document["records"]):
        record = ExpectationRecord.from_mapping(item, where=f"records[{position}]")
        release = facts.get(record.event_id)
        if release is None:
            raise ExpectationError(
                REFUSAL_UNKNOWN_STATISTIC,
                f"records[{position}] names release {record.event_id!r}, which the sealed release "
                "dataset does not carry",
            )
        validated.setdefault(record.event_id, {})[record.statistic] = validate_expectation(
            record, release, evidence_root=evidence_root
        )
    for event_id, declared in sorted(validated.items()):
        missing = [name for name in wanted if name not in declared]
        if missing:
            raise ExpectationError(
                REFUSAL_INCOMPLETE_NEWS_VECTOR,
                f"release {event_id!r} carries no validated expectation for {missing}; the "
                f"declared news vector is {list(wanted)} and a partial vector is not fitted",
            )
    return validated


def news_vector(
    expectations: Mapping[str, Mapping[str, Expectation]],
    facts: Mapping[str, ReleaseFacts],
    *,
    statistics: Sequence[str],
) -> dict[str, dict[str, Decimal]]:
    """Each release's validated surprise vector, keyed by release then statistic."""
    wanted = tuple(dict.fromkeys(str(name) for name in statistics))
    out: dict[str, dict[str, Decimal]] = {}
    for event_id, declared in sorted(expectations.items()):
        release = facts[event_id]
        vector: dict[str, Decimal] = {}
        for name in wanted:
            expectation = declared.get(name)
            if expectation is None:
                raise ExpectationError(
                    REFUSAL_INCOMPLETE_NEWS_VECTOR,
                    f"release {event_id!r} has no validated expectation for {name!r}",
                )
            value = surprise(expectation, release)
            if value is not None:
                vector[name] = value
        out[event_id] = vector
    return out
