"""Deterministic offline replay sample built from the packaged synthetic fixture.

``fixtures/replay.json`` is authored, not observed: every date, statistic, price
and size in it is invented so the whole pipeline can be exercised offline and
reproducibly. It carries ten illustrative releases, five in each family, and one
fully specified, deliberately unresolved target contract per release. That target
pays on the *next* reference period's print of its own family, first published
about a month after the release whose information it answers, so its payoff is a
later outcome and is unresolved at that release. The release-to-target relation
lives in the fixture's explicit ``exposure`` list and nowhere else, and it is that
list which labels the built panel's cohort. The fixture is not the study's
scientific cohort, which is the ten releases in ``configs/cohort.yaml``, and no
record in it may be cited as observed data.

:func:`build_sample` runs the real pipeline over it. The fixture's own records
are archived in a :class:`~market_propagation.storage.RawStore` under occurrence
identities qualified by the fixture's digest, real
:class:`~market_propagation.domain.BookEvent` / ``Trade`` / ``Release`` /
``Contract`` records are constructed from those bytes, the stream is replayed
independently under both orders, the two folds are compared, and each fold's
quotes are reduced to an event panel that is sealed as Parquet. Both folds are
built because they answer different questions: ``source`` is the historical
economic event study and ``usable`` is the information-feasible view.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from importlib import resources
from pathlib import Path
from typing import Any

import pandas as pd

from .domain import (
    Availability,
    BookEvent,
    BookOperation,
    BookSide,
    Clock,
    Contract,
    Operator,
    Provenance,
    Release,
    Rounding,
    Trade,
    parse_decimal,
    parse_utc_time,
)
from .point_in_time import build_event_panel
from .replay import ORDER_SOURCE, ORDER_USABLE, compare_replay_orders, replay
from .storage import RawStore, _atomic_write_bytes, hash_bytes, write_parquet

__all__ = [
    "DISAGREEMENTS_NAME",
    "RAW_DIRECTORY_NAME",
    "SOURCE_PANEL_NAME",
    "USABLE_PANEL_NAME",
    "SampleArtifacts",
    "build_sample",
]

#: Names of the files one build writes under its output directory. Callers build
#: exact paths from these rather than repeating the spelling.
SOURCE_PANEL_NAME = "source_panel.parquet"
USABLE_PANEL_NAME = "usable_panel.parquet"
DISAGREEMENTS_NAME = "replay_disagreements.json"
RAW_DIRECTORY_NAME = "raw"

_FIXTURE_PACKAGE = "market_propagation"
_FIXTURE_RESOURCE = ("fixtures", "replay.json")
_FIXTURE_SOURCE = "synthetic_replay_fixture"
_FIXTURE_NAME = "replay_sample"
_FIXTURE_SCHEMA_VERSION = "2"
_EVENT_COUNT_BY_FAMILY = {"cpi": 5, "employment": 5}
_STREAM_RECORDS = ("book_event", "trade")
_BOOK_EVENT_KINDS = ("snapshot", "delta")
#: The cohort names ``build_event_panel`` accepts. The fixture states one relation
#: per release and it is always a downstream one, so the sample passes exactly this
#: vocabulary through rather than minting a label of its own.
_COHORT_NAMES = frozenset({"downstream", "direct", "control"})
_OPERATORS = frozenset(member.value for member in Operator)
_ROUNDINGS = frozenset(member.value for member in Rounding)
_OPERATIONS = frozenset(member.value for member in BookOperation)
_SIDES = frozenset(member.value for member in BookSide)
_PERIOD_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")


@dataclass(frozen=True, slots=True)
class SampleArtifacts:
    """Everything one sample build produced, with the raw store it archived into.

    ``source_panel`` and ``usable_panel`` are the same event panel reduced from
    the two replay folds, so their differences are the fold's differences and
    nothing else. ``disagreements`` is the whole-books comparison between the
    folds, as returned by ``compare_replay_orders``. ``raw_hashes`` addresses the
    archived bytes of every fixture record this build used, and ``raw_root`` is
    the store they live in.
    """

    source_panel: pd.DataFrame
    usable_panel: pd.DataFrame
    disagreements: dict[str, Any]
    raw_hashes: tuple[str, ...]
    fixture_hash: str
    quote_count: int
    release_count: int
    raw_root: Path


def build_sample(output_dir: str | Path, *, max_age_seconds: float = 120) -> SampleArtifacts:
    """Build the offline replay sample from the packaged fixture into ``output_dir``.

    Writes :data:`SOURCE_PANEL_NAME` and :data:`USABLE_PANEL_NAME` with their
    ``.manifest.json`` sidecars, :data:`DISAGREEMENTS_NAME`, and the archived
    fixture records under :data:`RAW_DIRECTORY_NAME`. Outputs are sealed: a
    second identical build leaves every byte unchanged, and a fixture that
    changed raises rather than replacing a sealed panel. The packaged fixture is
    only ever read, never written to.

    ``max_age_seconds`` bounds quote staleness in both panels; it is measured
    against ``last_verified``, so an unchanged standing quote is not stale.
    """
    if isinstance(max_age_seconds, bool) or not isinstance(max_age_seconds, (int, float)):
        raise TypeError(f"max_age_seconds must be a number, got {type(max_age_seconds).__name__}")
    if float(max_age_seconds) <= 0:
        raise ValueError(
            f"max_age_seconds must be positive, got {max_age_seconds!r}; a zero bound would "
            "exclude every quote and report an empty panel as a measured one"
        )

    payload = _packaged_fixture()
    digest = hash_bytes(payload)
    document = _verified_document(payload)
    events = tuple(document["events"])
    contracts = tuple(document["contracts"])
    streams = tuple(document["streams"])
    exposure = tuple(document["exposure"])
    fixture = document["fixture"]

    target = Path(output_dir)
    store = RawStore(target / RAW_DIRECTORY_NAME)
    scheduled_by_event = {
        str(event["event_id"]): parse_utc_time(
            event["scheduled_at_utc"], field_name=f"{event['event_id']}.scheduled_at_utc"
        )
        for event in events
    }
    # The fixture's exposure list is the only statement of which release a target
    # contract answers, so the stream's contract and the panel's cohort both come
    # from it rather than from the contract's own ``event_id``, which names the
    # later print the contract pays on.
    contract_id_by_release = {
        str(entry["release_event_id"]): str(entry["contract_id"]) for entry in exposure
    }
    cohorts = {
        (str(entry["release_event_id"]), str(entry["contract_id"])): str(entry["cohort"])
        for entry in exposure
    }
    archived = _archive(
        store, digest, payload, fixture, events, contracts, streams, scheduled_by_event
    )

    releases = _releases(events, archived)
    contract_records = _contracts(contracts, archived)
    stream_records = _stream_records(streams, contract_id_by_release, scheduled_by_event, archived)
    horizons = tuple(int(horizon) for horizon in fixture["horizons_seconds"])

    source_result = replay(stream_records, order=ORDER_SOURCE)
    usable_result = replay(stream_records, order=ORDER_USABLE)
    disagreements = compare_replay_orders(stream_records)

    panels = {
        order: build_event_panel(
            result.quotes,
            releases,
            contract_records,
            order=order,
            horizons_seconds=horizons,
            max_age_seconds=float(max_age_seconds),
            cohorts=cohorts,
        )
        for order, result in (
            (ORDER_SOURCE, source_result),
            (ORDER_USABLE, usable_result),
        )
    }
    references = {
        order: write_parquet(
            panels[order],
            target / name,
            table="event_panel",
            coverage_epoch=digest,
            metadata={
                "fixture_name": str(fixture["name"]),
                "fixture_hash": digest,
                "replay_order": order,
            },
        )
        for order, name in (
            (ORDER_SOURCE, SOURCE_PANEL_NAME),
            (ORDER_USABLE, USABLE_PANEL_NAME),
        )
    }
    _seal(
        target / DISAGREEMENTS_NAME,
        _disagreement_record(
            fixture, digest, references, disagreements, max_age_seconds=float(max_age_seconds)
        ),
    )

    return SampleArtifacts(
        source_panel=panels[ORDER_SOURCE],
        usable_panel=panels[ORDER_USABLE],
        disagreements=disagreements,
        raw_hashes=tuple(sorted({provenance.raw_hash for provenance in archived.values()})),
        fixture_hash=digest,
        quote_count=int(source_result.coverage["quote_count"]),
        release_count=len(releases),
        raw_root=store.root,
    )


def _packaged_fixture() -> bytes:
    """The packaged fixture's own bytes, read through ``importlib.resources``."""
    resource = resources.files(_FIXTURE_PACKAGE).joinpath(*_FIXTURE_RESOURCE)
    try:
        return resource.read_bytes()
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"packaged fixture {'/'.join(_FIXTURE_RESOURCE)} is missing from {_FIXTURE_PACKAGE}"
        ) from exc


def _verified_document(payload: bytes) -> Mapping[str, Any]:
    """Parse the fixture and refuse anything this sample cannot state honestly.

    The fixture is external input even though it ships with the package, so its
    shape is checked here rather than trusted: an invented release date, a rule
    field left as a placeholder, or a packaged outcome would all reach the study
    unnoticed otherwise.
    """
    try:
        document = json.loads(payload.decode("utf-8"), parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"packaged fixture is not decodable JSON: {exc}") from exc
    fixture = _require_mapping(document, where="fixture document").get("fixture")
    fixture = _require_mapping(fixture, where="fixture")
    if fixture.get("synthetic") is not True:
        raise ValueError(
            "the packaged fixture must declare 'synthetic': true; illustrative records that "
            "do not say so are indistinguishable from observed data"
        )
    if fixture.get("name") != _FIXTURE_NAME:
        raise ValueError(f"fixture.name must be {_FIXTURE_NAME!r}, got {fixture.get('name')!r}")
    if fixture.get("schema_version") != _FIXTURE_SCHEMA_VERSION:
        raise ValueError(
            f"fixture.schema_version must be {_FIXTURE_SCHEMA_VERSION!r}, got "
            f"{fixture.get('schema_version')!r}"
        )
    for name in (
        "statement",
        "occurrence_id_policy",
        "price_policy",
        "resolution_policy",
        "exposure_policy",
        "scientific_cohort_config",
    ):
        _require_text(_required(fixture, name, where="fixture"), where=f"fixture.{name}")
    horizons = _horizons(fixture)

    events = _verified_events(document.get("events"))
    counts: dict[str, int] = {}
    for event in events:
        family = str(event["family"])
        counts[family] = counts.get(family, 0) + 1
    if counts != _EVENT_COUNT_BY_FAMILY:
        raise ValueError(f"the fixture must carry {_EVENT_COUNT_BY_FAMILY}, got {counts}")
    declared = _require_mapping(
        fixture.get("event_count_by_family"), where="fixture.event_count_by_family"
    )
    if {str(name): int(value) for name, value in declared.items()} != counts:
        raise ValueError(
            f"fixture.event_count_by_family {dict(declared)!r} disagrees with the {len(events)} "
            "events it carries"
        )
    if fixture.get("event_count") != len(events):
        raise ValueError(
            f"fixture.event_count {fixture.get('event_count')!r} disagrees with the "
            f"{len(events)} events it carries"
        )

    event_ids = tuple(str(event["event_id"]) for event in events)
    exposure = _verified_exposure(document.get("exposure"), events, horizons)
    _verified_contracts(document.get("contracts"), events, exposure, horizons)
    _verified_streams(document.get("streams"), event_ids, exposure)

    resolutions = document.get("resolutions")
    if not isinstance(resolutions, Sequence) or isinstance(resolutions, (str, bytes)):
        raise ValueError("fixture document must carry a 'resolutions' array")
    if resolutions:
        raise ValueError(
            "the packaged fixture carries realized outcomes; its target contracts must stay "
            "unresolved so no label time can be read from a sample"
        )
    return document


def _horizons(fixture: Mapping[str, Any]) -> tuple[int, ...]:
    rows = _require_sequence(
        _required(fixture, "horizons_seconds", where="fixture"),
        where="fixture.horizons_seconds",
    )
    out: list[int] = []
    for index, value in enumerate(rows):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(
                f"fixture.horizons_seconds[{index}] must be a positive integer, got {value!r}"
            )
        if out and value <= out[-1]:
            raise ValueError(f"fixture.horizons_seconds must strictly increase, got {rows!r}")
        out.append(value)
    if not out:
        raise ValueError("fixture.horizons_seconds must name at least one horizon")
    return tuple(out)


def _verified_events(value: Any) -> tuple[Mapping[str, Any], ...]:
    rows = _require_sequence(value, where="events")
    if not rows:
        raise ValueError("the fixture carries no events; an empty sample measures nothing")
    out: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        where = f"events[{index}]"
        record = _require_mapping(row, where=where)
        for name in ("event_id", "family", "reference_period"):
            _require_text(_required(record, name, where=where), where=f"{where}.{name}")
        event_id = str(record["event_id"])
        if event_id in seen:
            raise ValueError(f"event id {event_id!r} appears twice; releases must stay separable")
        seen.add(event_id)
        scheduled = parse_utc_time(
            _required(record, "scheduled_at_utc", where=where),
            field_name=f"{where}.scheduled_at_utc",
        )
        observed = parse_utc_time(
            _required(record, "observed_at", where=where), field_name=f"{where}.observed_at"
        )
        received = parse_utc_time(
            _required(record, "received_at", where=where), field_name=f"{where}.received_at"
        )
        if observed < scheduled:
            raise ValueError(
                f"{where}.observed_at {observed.isoformat()} precedes its scheduled instant "
                f"{scheduled.isoformat()}"
            )
        if received < observed:
            raise ValueError(
                f"{where}.received_at {received.isoformat()} precedes its publication instant "
                f"{observed.isoformat()}"
            )
        _verified_availability(record.get("availability"), where=f"{where}.availability")
        for section in ("values", "revisions"):
            values = _require_mapping(
                _required(record, section, where=where), where=f"{where}.{section}"
            )
            for name, item in values.items():
                parse_decimal(item, field_name=f"{where}.{section}[{name!r}]")
        if not record["values"]:
            raise ValueError(f"{where}.values must not be empty; a release publishes something")
        out.append(record)
    return tuple(out)


def _verified_exposure(
    value: Any,
    events: tuple[Mapping[str, Any], ...],
    horizons: tuple[int, ...],
) -> tuple[Mapping[str, Any], ...]:
    """The fixture's own statement of which release each target contract answers.

    This list is the only place the release-to-target relation is stated, and the
    sample labels its panel rows from it. Each entry is therefore checked to
    describe a genuinely downstream claim: the release's information must be
    answered by a *later* reference period whose first print lands after the
    release plus the longest panel horizon, so the payoff is still unresolved when
    the release is published. A contract whose reference period is the release's
    own would be direct material wearing a downstream label, and is refused.
    """
    rows = _require_sequence(value, where="exposure")
    if not rows:
        raise ValueError(
            "the fixture carries no exposure list; without it a target contract's release "
            "cannot be told from its outcome, and every cohort label would be a guess"
        )
    window_end = dt.timedelta(seconds=max(horizons))
    scheduled_by_event = {
        str(event["event_id"]): parse_utc_time(
            event["scheduled_at_utc"], field_name=f"{event['event_id']}.scheduled_at_utc"
        )
        for event in events
    }
    period_by_event = {str(event["event_id"]): str(event["reference_period"]) for event in events}
    family_by_event = {str(event["event_id"]): str(event["family"]) for event in events}

    out: list[Mapping[str, Any]] = []
    seen_contracts: set[str] = set()
    seen_releases: set[str] = set()
    for index, row in enumerate(rows):
        where = f"exposure[{index}]"
        record = _require_mapping(row, where=where)
        for name in (
            "basis",
            "cohort",
            "contract_id",
            "outcome_event_id",
            "outcome_published_at",
            "outcome_reference_period",
            "release_event_id",
        ):
            _require_text(_required(record, name, where=where), where=f"{where}.{name}")
        if not isinstance(record.get("outcome_event_packaged"), bool):
            raise ValueError(
                f"{where}.outcome_event_packaged must be true or false; whether this fixture "
                "also carries the outcome print as an event is a fact, not a default"
            )
        cohort = str(record["cohort"])
        if cohort not in _COHORT_NAMES:
            raise ValueError(f"{where}.cohort {cohort!r} is not one of {sorted(_COHORT_NAMES)}")
        release_id = str(record["release_event_id"])
        if release_id not in scheduled_by_event:
            raise ValueError(f"{where}.release_event_id {release_id!r} is not a fixture event")
        if release_id in seen_releases:
            raise ValueError(
                f"{where} repeats release {release_id!r}; one target contract per release is "
                "what makes a cohort label mean one thing"
            )
        seen_releases.add(release_id)
        contract_id = str(record["contract_id"])
        if contract_id in seen_contracts:
            raise ValueError(
                f"{where}.contract_id {contract_id!r} appears twice; two releases cannot share "
                "one target contract"
            )
        seen_contracts.add(contract_id)

        outcome_id = str(record["outcome_event_id"])
        if outcome_id == release_id:
            raise ValueError(
                f"{where}.outcome_event_id {outcome_id!r} is the release itself; a payoff "
                "determined by the release it is measured against is direct, not downstream"
            )
        period = str(record["outcome_reference_period"])
        release_period = period_by_event[release_id]
        _require_period(period, where=f"{where}.outcome_reference_period")
        if period <= release_period:
            raise ValueError(
                f"{where}.outcome_reference_period {period!r} is not later than the release's "
                f"reference period {release_period!r}; an outcome at or before the release is "
                "not a later outcome"
            )
        published = parse_utc_time(
            record["outcome_published_at"], field_name=f"{where}.outcome_published_at"
        )
        scheduled = scheduled_by_event[release_id]
        if published <= scheduled + window_end:
            raise ValueError(
                f"{where}.outcome_published_at {published.isoformat()} is not after its release "
                f"{scheduled.isoformat()} plus the longest horizon ({window_end}); the payoff "
                "would be resolved inside a measured window rather than downstream of it"
            )
        if record["outcome_event_packaged"]:
            if outcome_id not in scheduled_by_event:
                raise ValueError(
                    f"{where} declares outcome {outcome_id!r} packaged but the fixture carries "
                    "no such event"
                )
            if family_by_event[outcome_id] != family_by_event[release_id]:
                raise ValueError(
                    f"{where} pairs a {family_by_event[release_id]!r} release with a "
                    f"{family_by_event[outcome_id]!r} outcome"
                )
            if period_by_event[outcome_id] != period:
                raise ValueError(
                    f"{where}.outcome_reference_period {period!r} disagrees with the packaged "
                    f"outcome event's own {period_by_event[outcome_id]!r}"
                )
            if scheduled_by_event[outcome_id] != published:
                raise ValueError(
                    f"{where}.outcome_published_at {published.isoformat()} disagrees with the "
                    f"packaged outcome event's scheduled instant "
                    f"{scheduled_by_event[outcome_id].isoformat()}"
                )
        elif outcome_id in scheduled_by_event:
            raise ValueError(
                f"{where} marks outcome {outcome_id!r} un-packaged but the fixture carries that "
                "event; the flag would hide a print the records do state"
            )
        out.append(record)

    if len(out) != len(events):
        raise ValueError(
            f"the fixture states {len(out)} exposures for {len(events)} releases; every release "
            "needs exactly one target contract relation"
        )
    return tuple(out)


def _require_period(value: str, *, where: str) -> None:
    if not _PERIOD_RE.match(value):
        raise ValueError(f"{where} {value!r} is not a YYYY-MM reference period")


def _verified_contracts(
    value: Any,
    events: tuple[Mapping[str, Any], ...],
    exposure: tuple[Mapping[str, Any], ...],
    horizons: tuple[int, ...],
) -> tuple[Mapping[str, Any], ...]:
    rows = _require_sequence(value, where="contracts")
    family_by_event = {str(event["event_id"]): str(event["family"]) for event in events}
    scheduled_by_event = {
        str(event["event_id"]): parse_utc_time(
            event["scheduled_at_utc"], field_name=f"{event['event_id']}.scheduled_at_utc"
        )
        for event in events
    }
    by_contract = {str(entry["contract_id"]): entry for entry in exposure}
    # A contract's own ``event_id`` is the later print it pays on. That print is a
    # packaged release where the fixture carries it, and beyond the packaged
    # calendar for the tail release of each family, which the exposure flag states.
    known_events = set(family_by_event) | {
        str(entry["outcome_event_id"]) for entry in exposure if not entry["outcome_event_packaged"]
    }
    out: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        where = f"contracts[{index}]"
        record = _require_mapping(row, where=where)
        for name in (
            "contract_id",
            "venue",
            "event_id",
            "family",
            "reference_period",
            "source",
            "units",
            "statement",
            "operator",
            "threshold",
            "rounding",
            "vintage",
            "timezone",
            "deadline",
            "settlement",
            "currency",
            "exceptional_policy",
            "open_time",
            "close_time",
            "resolve_time",
            "rule_available_at",
            "rule_text",
        ):
            _require_text(_required(record, name, where=where), where=f"{where}.{name}")
        contract_id = str(record["contract_id"])
        if contract_id in seen:
            raise ValueError(f"{where}.contract_id {contract_id!r} appears twice")
        seen.add(contract_id)
        entry = by_contract.get(contract_id)
        if entry is None:
            raise ValueError(
                f"{where}.contract_id {contract_id!r} has no exposure entry; a contract whose "
                "release is unstated cannot be labelled downstream"
            )
        release_id = str(entry["release_event_id"])
        outcome_id = str(record["event_id"])
        if outcome_id not in known_events:
            raise ValueError(
                f"{where}.event_id {outcome_id!r} is neither a fixture event nor a declared "
                "outcome print"
            )
        if outcome_id != str(entry["outcome_event_id"]):
            raise ValueError(
                f"{where}.event_id {outcome_id!r} is not the outcome event "
                f"{entry['outcome_event_id']!r} its exposure entry names; the contract must be "
                "the claim on that later print"
            )
        if str(record["family"]) != family_by_event[release_id]:
            raise ValueError(
                f"{where}.family {record['family']!r} disagrees with the family "
                f"{family_by_event[release_id]!r} of the release it answers"
            )
        if str(record["reference_period"]) != str(entry["outcome_reference_period"]):
            raise ValueError(
                f"{where}.reference_period {record['reference_period']!r} disagrees with its "
                f"exposure entry's {entry['outcome_reference_period']!r}"
            )
        operator = str(record["operator"])
        if operator not in _OPERATORS:
            raise ValueError(f"{where}.operator {operator!r} is not one of {sorted(_OPERATORS)}")
        if operator == Operator.RANGE.value:
            raise ValueError(
                f"{where} states a range rule; this sample's target contracts are scalar "
                "threshold rules"
            )
        if str(record["rounding"]) not in _ROUNDINGS:
            raise ValueError(
                f"{where}.rounding {record['rounding']!r} is not one of {sorted(_ROUNDINGS)}"
            )
        parse_decimal(_required(record, "threshold", where=where), field_name=f"{where}.threshold")
        for name in ("deadline", "open_time", "close_time", "resolve_time", "rule_available_at"):
            parse_utc_time(_required(record, name, where=where), field_name=f"{where}.{name}")

        # The release this contract answers is the panel's own anchor, so the
        # contract's readability and trading window are checked against that
        # release, not against the later print it pays on.
        scheduled = scheduled_by_event[release_id]
        window_end = dt.timedelta(seconds=max(horizons))
        readable = parse_utc_time(
            record["rule_available_at"], field_name=f"{where}.rule_available_at"
        )
        if readable > scheduled:
            raise ValueError(
                f"{where}.rule_available_at {readable.isoformat()} is after the release "
                f"{scheduled.isoformat()} it answers; a rule the market could not have read "
                "before that release cannot anchor a panel that treats it as known"
            )
        open_time = parse_utc_time(record["open_time"], field_name=f"{where}.open_time")
        if open_time > scheduled:
            raise ValueError(
                f"{where}.open_time {open_time.isoformat()} is after the release "
                f"{scheduled.isoformat()} it answers; a claim that did not yet trade cannot "
                "carry a downstream response to that release"
            )
        close_time = parse_utc_time(record["close_time"], field_name=f"{where}.close_time")
        if close_time <= open_time:
            raise ValueError(f"{where}.close_time must follow its open_time")
        if close_time <= scheduled + window_end:
            raise ValueError(
                f"{where}.close_time {close_time.isoformat()} is not after the release "
                f"{scheduled.isoformat()} plus the longest horizon; the contract would be closed "
                "before the window whose response it is supposed to measure"
            )
        published = parse_utc_time(
            str(entry["outcome_published_at"]), field_name=f"{where}.outcome_published_at"
        )
        resolve_time = parse_utc_time(record["resolve_time"], field_name=f"{where}.resolve_time")
        deadline = parse_utc_time(record["deadline"], field_name=f"{where}.deadline")
        if not close_time <= published <= resolve_time:
            raise ValueError(
                f"{where} trades or settles across its outcome print {published.isoformat()}: "
                f"close_time {close_time.isoformat()}, resolve_time {resolve_time.isoformat()}"
            )
        if deadline < resolve_time:
            raise ValueError(
                f"{where}.deadline {deadline.isoformat()} precedes its resolve_time "
                f"{resolve_time.isoformat()}"
            )
        out.append(record)
    if seen != set(by_contract):
        raise ValueError(
            f"the fixture carries contracts for {len(seen)} of its {len(by_contract)} exposure "
            "entries; missing "
            f"{sorted(set(by_contract) - seen)}"
        )
    return tuple(out)


def _verified_streams(
    value: Any, event_ids: tuple[str, ...], exposure: tuple[Mapping[str, Any], ...]
) -> tuple[Mapping[str, Any], ...]:
    rows = _require_sequence(value, where="streams")
    contract_by_release = {
        str(entry["release_event_id"]): str(entry["contract_id"]) for entry in exposure
    }
    seen_events: set[str] = set()
    seen_records: set[str] = set()
    for index, row in enumerate(rows):
        where = f"streams[{index}]"
        stream = _require_mapping(row, where=where)
        event_id = _require_text(
            _required(stream, "event_id", where=where), where=f"{where}.event_id"
        )
        if event_id not in event_ids:
            raise ValueError(f"{where}.event_id {event_id!r} is not one of the fixture's events")
        if event_id not in contract_by_release:
            raise ValueError(f"{where}.event_id {event_id!r} has no target contract")
        if event_id in seen_events:
            raise ValueError(
                f"event {event_id!r} carries more than one stream; one documented stream per "
                "target contract is what makes a sequence gap mean something"
            )
        seen_events.add(event_id)
        for name in ("venue", "connection_id", "sequence_scope"):
            _require_text(_required(stream, name, where=where), where=f"{where}.{name}")
        records = _require_sequence(
            _required(stream, "records", where=where), where=f"{where}.records"
        )
        if not records:
            raise ValueError(f"{where}.records must not be empty")
        sequences: set[int] = set()
        for position, entry in enumerate(records):
            spot = f"{where}.records[{position}]"
            record = _require_mapping(entry, where=spot)
            kind = _require_text(_required(record, "record", where=spot), where=f"{spot}.record")
            if kind not in _STREAM_RECORDS:
                raise ValueError(f"{spot}.record {kind!r} is not one of {list(_STREAM_RECORDS)}")
            record_id = _require_text(
                _required(record, "record_id", where=spot), where=f"{spot}.record_id"
            )
            if record_id in seen_records:
                raise ValueError(
                    f"record id {record_id!r} appears twice in the fixture; two records cannot "
                    "share one occurrence identity"
                )
            seen_records.add(record_id)
            _seconds(
                _required(record, "source_offset_seconds", where=spot),
                where=f"{spot}.source_offset_seconds",
            )
            _seconds(
                _required(record, "latency_seconds", where=spot),
                where=f"{spot}.latency_seconds",
                allow_negative=False,
            )
            _verified_availability(record.get("availability"), where=f"{spot}.availability")
            if kind == "trade":
                for name in ("price", "size", "aggressor"):
                    _require_text(_required(record, name, where=spot), where=f"{spot}.{name}")
                parse_decimal(record["price"], field_name=f"{spot}.price")
                parse_decimal(record["size"], field_name=f"{spot}.size")
                if not isinstance(record.get("is_block"), bool):
                    raise ValueError(f"{spot}.is_block must be true or false")
                continue
            _verified_book_event(record, where=spot)
            sequence = record["sequence"]
            if sequence is not None:
                if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
                    raise ValueError(f"{spot}.sequence must be a non-negative integer or null")
                if sequence in sequences:
                    raise ValueError(
                        f"{spot}.sequence {sequence} appears twice on one stream; a sequence "
                        "number is a position in that stream"
                    )
                sequences.add(sequence)
    if seen_events != set(event_ids):
        raise ValueError(
            f"every fixture event needs a stream: missing {sorted(set(event_ids) - seen_events)}"
        )
    if seen_records.__len__() == 0:
        raise ValueError("the fixture carries no stream records")
    return tuple(rows)


def _verified_book_event(record: Mapping[str, Any], *, where: str) -> None:
    kind = _require_text(_required(record, "kind", where=where), where=f"{where}.kind")
    if kind not in _BOOK_EVENT_KINDS:
        raise ValueError(
            f"{where}.kind {kind!r} is not one of {list(_BOOK_EVENT_KINDS)}; this sample's "
            "fixture carries snapshots and deltas only, because a lifecycle message changes "
            "which panel rows are admissible and needs its own authoring decision"
        )
    for name in ("side", "price", "size", "operation", "bids", "asks"):
        if name not in record:
            raise ValueError(f"{where}.{name} is required, as null where it does not apply")
    if kind == "snapshot":
        for name in ("side", "price", "size", "operation"):
            if record[name] is not None:
                raise ValueError(
                    f"{where}.{name} must be null on a snapshot; a snapshot carries whole-book "
                    "levels"
                )
        for name in ("bids", "asks"):
            _levels(record[name], where=f"{where}.{name}")
        return
    side = _require_text(_required(record, "side", where=where), where=f"{where}.side")
    if side not in _SIDES:
        raise ValueError(f"{where}.side {side!r} is not one of {sorted(_SIDES)}")
    parse_decimal(_required(record, "price", where=where), field_name=f"{where}.price")
    operation = _require_text(
        _required(record, "operation", where=where), where=f"{where}.operation"
    )
    if operation not in _OPERATIONS:
        raise ValueError(f"{where}.operation {operation!r} is not one of {sorted(_OPERATIONS)}")
    if operation == BookOperation.DELETE.value:
        if record["size"] is not None:
            raise ValueError(f"{where}.size must be null when the operation deletes a level")
    else:
        parse_decimal(_required(record, "size", where=where), field_name=f"{where}.size")
    for name in ("bids", "asks"):
        if record[name]:
            raise ValueError(f"{where}.{name} belongs to a snapshot, not a delta")


def _verified_availability(value: Any, *, where: str) -> Mapping[str, Any]:
    availability = _require_mapping(value, where=where)
    _seconds(
        _required(availability, "uncertainty_seconds", where=where),
        where=f"{where}.uncertainty_seconds",
        allow_negative=False,
    )
    quality = _require_text(
        _required(availability, "quality", where=where), where=f"{where}.quality"
    )
    if quality not in Availability.QUALITIES:
        raise ValueError(
            f"{where}.quality {quality!r} is not one of {list(Availability.QUALITIES)}"
        )
    _require_text(_required(availability, "basis", where=where), where=f"{where}.basis")
    return availability


def _archive(
    store: RawStore,
    digest: str,
    payload: bytes,
    fixture: Mapping[str, Any],
    events: tuple[Mapping[str, Any], ...],
    contracts: tuple[Mapping[str, Any], ...],
    streams: tuple[Mapping[str, Any], ...],
    scheduled_by_event: Mapping[str, dt.datetime],
) -> dict[tuple[str, str], Provenance]:
    """Archive the fixture document and every record in it under stable identities.

    The identity of each occurrence is derived from the fixture digest and the
    record's own id, so re-running a build refers to the same occurrence rather
    than minting a new one, and a fixture whose bytes changed produces new
    occurrences instead of rewriting what an earlier build recorded.
    """
    out: dict[tuple[str, str], Provenance] = {}
    # The document itself is archived verbatim, so the fixture hash addresses the
    # real packaged bytes and not only the records parsed out of them.
    out[("document", _FIXTURE_RESOURCE[-1])] = store.put(
        payload,
        source=_FIXTURE_SOURCE,
        received_time=parse_utc_time(
            _require_text(
                _required(fixture, "authored_at", where="fixture"), where="fixture.authored_at"
            ),
            field_name="fixture.authored_at",
        ),
        record_id=f"{digest}-document",
        metadata={"fixture_hash": digest, "section": "document"},
    )
    for event in events:
        event_id = str(event["event_id"])
        out[("event", event_id)] = _store_record(
            store,
            digest,
            section="event",
            identity=event_id,
            record=event,
            received=parse_utc_time(event["received_at"], field_name=f"{event_id}.received_at"),
        )
    for contract in contracts:
        contract_id = str(contract["contract_id"])
        out[("contract", contract_id)] = _store_record(
            store,
            digest,
            section="contract",
            identity=contract_id,
            record=contract,
            received=parse_utc_time(
                contract["received_at"], field_name=f"{contract_id}.received_at"
            ),
        )
    for stream in streams:
        scheduled = scheduled_by_event[str(stream["event_id"])]
        for record in stream["records"]:
            record_id = str(record["record_id"])
            out[("stream", record_id)] = _store_record(
                store,
                digest,
                section="stream",
                identity=record_id,
                record=record,
                received=_record_clock(scheduled, record, where=record_id).usable_time,
            )
    return out


def _store_record(
    store: RawStore,
    digest: str,
    *,
    section: str,
    identity: str,
    record: Mapping[str, Any],
    received: dt.datetime,
) -> Provenance:
    return store.put(
        _canonical_json(record).encode("utf-8"),
        source=_FIXTURE_SOURCE,
        received_time=received,
        record_id=f"{digest}-{section}-{identity}",
        metadata={"fixture_hash": digest, "section": section},
    )


def _releases(
    events: tuple[Mapping[str, Any], ...], archived: Mapping[tuple[str, str], Provenance]
) -> tuple[Release, ...]:
    out: list[Release] = []
    for event in events:
        event_id = str(event["event_id"])
        availability = _verified_availability(
            event["availability"], where=f"{event_id}.availability"
        )
        clock = Clock.captured(
            parse_utc_time(event["observed_at"], field_name=f"{event_id}.observed_at"),
            parse_utc_time(event["received_at"], field_name=f"{event_id}.received_at"),
            uncertainty_seconds=_seconds(
                availability["uncertainty_seconds"],
                where=f"{event_id}.availability.uncertainty_seconds",
                allow_negative=False,
            ),
            quality=str(availability["quality"]),
            basis=str(availability["basis"]),
        )
        out.append(
            Release(
                event_id=event_id,
                family=str(event["family"]),
                scheduled_at=parse_utc_time(
                    event["scheduled_at_utc"], field_name=f"{event_id}.scheduled_at_utc"
                ),
                reference_period=str(event["reference_period"]),
                values={
                    str(name): parse_decimal(value, field_name=f"{event_id}.values[{name!r}]")
                    for name, value in event["values"].items()
                },
                clock=clock,
                provenance=archived[("event", event_id)],
                revisions={
                    str(name): parse_decimal(value, field_name=f"{event_id}.revisions[{name!r}]")
                    for name, value in event["revisions"].items()
                },
            )
        )
    return tuple(out)


def _contracts(
    contracts: tuple[Mapping[str, Any], ...], archived: Mapping[tuple[str, str], Provenance]
) -> tuple[Contract, ...]:
    out: list[Contract] = []
    for record in contracts:
        contract_id = str(record["contract_id"])
        rule_text = str(record["rule_text"])
        out.append(
            Contract(
                venue=str(record["venue"]),
                contract_id=contract_id,
                event_id=str(record["event_id"]),
                family=str(record["family"]),
                reference_period=str(record["reference_period"]),
                source=str(record["source"]),
                units=str(record["units"]),
                operator=str(record["operator"]),
                threshold=parse_decimal(record["threshold"], field_name=f"{contract_id}.threshold"),
                lower=None,
                upper=None,
                rounding=str(record["rounding"]),
                vintage=str(record["vintage"]),
                timezone=str(record["timezone"]),
                deadline=parse_utc_time(record["deadline"], field_name=f"{contract_id}.deadline"),
                settlement=str(record["settlement"]),
                currency=str(record["currency"]),
                exceptional_policy=str(record["exceptional_policy"]),
                open_time=parse_utc_time(
                    record["open_time"], field_name=f"{contract_id}.open_time"
                ),
                close_time=parse_utc_time(
                    record["close_time"], field_name=f"{contract_id}.close_time"
                ),
                resolve_time=parse_utc_time(
                    record["resolve_time"], field_name=f"{contract_id}.resolve_time"
                ),
                # The rule hash covers the rule text alone, so it pins the claim
                # rather than the record that carried it.
                rule_hash=hash_bytes(rule_text.encode("utf-8")),
                provenance=archived[("contract", contract_id)],
                rule_available_at=parse_utc_time(
                    record["rule_available_at"], field_name=f"{contract_id}.rule_available_at"
                ),
            )
        )
    return tuple(out)


def _stream_records(
    streams: tuple[Mapping[str, Any], ...],
    contract_id_by_event: Mapping[str, str],
    scheduled_by_event: Mapping[str, dt.datetime],
    archived: Mapping[tuple[str, str], Provenance],
) -> list[BookEvent | Trade]:
    out: list[BookEvent | Trade] = []
    for stream in streams:
        event_id = str(stream["event_id"])
        contract_id = contract_id_by_event[event_id]
        scheduled = scheduled_by_event[event_id]
        venue = str(stream["venue"])
        for record in stream["records"]:
            record_id = str(record["record_id"])
            clock = _record_clock(scheduled, record, where=record_id)
            provenance = archived[("stream", record_id)]
            if record["record"] == "trade":
                out.append(
                    Trade(
                        venue=venue,
                        contract_id=contract_id,
                        trade_id=record_id,
                        price=parse_decimal(record["price"], field_name=f"{record_id}.price"),
                        size=parse_decimal(record["size"], field_name=f"{record_id}.size"),
                        clock=clock,
                        provenance=provenance,
                        aggressor=str(record["aggressor"]),
                        is_block=record["is_block"],
                    )
                )
                continue
            common: dict[str, Any] = {
                "venue": venue,
                "contract_id": contract_id,
                "kind": str(record["kind"]),
                "clock": clock,
                "provenance": provenance,
                "connection_id": str(stream["connection_id"]),
                "sequence_scope": str(stream["sequence_scope"]),
                "sequence": record["sequence"],
            }
            if record["kind"] == "snapshot":
                out.append(
                    BookEvent(
                        **common,
                        bids=_levels(record["bids"], where=f"{record_id}.bids"),
                        asks=_levels(record["asks"], where=f"{record_id}.asks"),
                    )
                )
                continue
            out.append(
                BookEvent(
                    **common,
                    side=str(record["side"]),
                    price=parse_decimal(record["price"], field_name=f"{record_id}.price"),
                    size=(
                        None
                        if record["size"] is None
                        else parse_decimal(record["size"], field_name=f"{record_id}.size")
                    ),
                    operation=str(record["operation"]),
                )
            )
    return out


def _record_clock(scheduled: dt.datetime, record: Mapping[str, Any], *, where: str) -> Clock:
    """The clock for one stream record, from its offset and documented latency.

    ``source_offset_seconds`` places the record's own stamp relative to the
    release it belongs to, and ``latency_seconds`` is the receipt delay after
    that stamp. The usable time is therefore the receipt, which is what keeps a
    record delivered after an instant from being read as evidence at it.
    """
    source = scheduled + dt.timedelta(
        seconds=_seconds(record["source_offset_seconds"], where=f"{where}.source_offset_seconds")
    )
    received = source + dt.timedelta(
        seconds=_seconds(
            record["latency_seconds"], where=f"{where}.latency_seconds", allow_negative=False
        )
    )
    availability = _verified_availability(record["availability"], where=f"{where}.availability")
    return Clock.captured(
        source,
        received,
        uncertainty_seconds=_seconds(
            availability["uncertainty_seconds"],
            where=f"{where}.availability.uncertainty_seconds",
            allow_negative=False,
        ),
        quality=str(availability["quality"]),
        basis=str(availability["basis"]),
    )


def _disagreement_record(
    fixture: Mapping[str, Any],
    digest: str,
    references: Mapping[str, Any],
    disagreements: Mapping[str, Any],
    *,
    max_age_seconds: float,
) -> dict[str, Any]:
    """The auditable summary written beside the two sealed panels."""
    return {
        "fixture": {
            "name": str(fixture["name"]),
            "hash": digest,
            "synthetic": bool(fixture["synthetic"]),
            "statement": str(fixture["statement"]),
        },
        "max_age_seconds": max_age_seconds,
        "horizons_seconds": [int(value) for value in fixture["horizons_seconds"]],
        "panels": {
            order: {
                "file": Path(reference.path).name,
                "content_hash": reference.content_hash,
                "row_count": reference.row_count,
            }
            for order, reference in references.items()
        },
        "decimal_encoding": "exact decimal strings",
        "disagreements": disagreements,
    }


def _seal(path: Path, payload: Mapping[str, Any]) -> None:
    """Write one JSON artifact that a repeat build cannot change."""
    _atomic_write_bytes(path, (_canonical_json(payload) + "\n").encode("utf-8"))


def _require_mapping(value: Any, *, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{where} must be a JSON object, got {type(value).__name__}")
    return value


def _require_sequence(value: Any, *, where: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{where} must be a JSON array, got {type(value).__name__}")
    return value


def _require_text(value: Any, *, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where} must be non-empty text, got {value!r}")
    return value


def _required(record: Mapping[str, Any], name: str, *, where: str) -> Any:
    if name not in record or record[name] is None:
        raise ValueError(
            f"{where}.{name} is required; an unknown rule field is not a placeholder to fill in"
        )
    return record[name]


def _seconds(value: Any, *, where: str, allow_negative: bool = True) -> float:
    amount = float(parse_decimal(value, field_name=where))
    if not allow_negative and amount < 0:
        raise ValueError(f"{where} must not be negative, got {value!r}")
    return amount


def _levels(value: Any, *, where: str) -> tuple[tuple[Decimal, Decimal], ...]:
    pairs = _require_sequence(value, where=where)
    out: list[tuple[Decimal, Decimal]] = []
    for item in pairs:
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)) or len(item) != 2:
            raise ValueError(f"{where}: each level must be a (price, size) pair, got {item!r}")
        out.append(
            (
                parse_decimal(item[0], field_name=f"{where}.price"),
                parse_decimal(item[1], field_name=f"{where}.size"),
            )
        )
    return tuple(out)


def _canonical_json(value: Any) -> str:
    """Deterministic JSON, with decimals and instants kept exact rather than lossy."""
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError(f"refusing to serialize naive datetime {value!r}")
        return value.astimezone(dt.UTC).isoformat()
    raise TypeError(
        f"cannot serialize {type(value).__name__} into a sample artifact without inventing a "
        "representation for it"
    )
