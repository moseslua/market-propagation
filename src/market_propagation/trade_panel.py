"""Explicitly masked source-time transaction panels for the external-history path.

The estimand is the transaction-price response of one contract to one economic
release, as plan section 5.2 defines it. For an event at ``tau_e``, horizon ``h``,
baseline print ``s_minus`` and endpoint print ``s_plus(h)``:

``R(h) = p_i(s_plus) - p_i(s_minus)``

Three of the definition's clauses are easy to lose when a panel is built by
carrying a last price forward, so each one is enforced structurally here rather
than left to a reader's discipline:

* ``s_minus`` is the latest print *strictly* before ``tau_e`` and ``s_plus`` is
  the latest print at or before ``tau_e + h``. A row needs ``s_plus > tau_e``, so
  an event whose contract never traded again is ``no_post_release_trade``, never
  a zero produced by holding the baseline. A genuine observed zero needs two
  distinct valid prints at the same price, which is a different fact and stays
  visible as one.
* A response is only claimed when the contract's rule evidence and lifecycle
  support it. A trade-history join never waives rule evidence, and a direct
  contract that closed before publication yields a null response, never zero.
* Availability is not invented. These rows come from venue-recorded transaction
  times, so ``source`` mode records ``source_time_only`` and a ``usable`` fold
  records ``unidentifiable`` without establishing an interval. A trade tape also
  holds no quotes, so the panel declares no bid, ask, spread or depth column at
  all rather than a column that could only ever be null.

Every row carries all of :data:`market_propagation.storage.TRADE_PANEL_COLUMNS`.
A row is masked in place with the reason it fails, never dropped, because
missingness measured against the preselected candidate universe is itself a
reported outcome.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from .domain import HistoricalTrade, parse_utc_time
from .storage import TRADE_PANEL_COLUMNS, DatasetRef, read_parquet, write_parquet

#: The pipeline configuration this module reads its measurement window from.
CONFIG_PATH = "configs/external_history_v1.yaml"

#: Cohort stamped on every row. The panel measures transaction responses, so a
#: downstream table can join transaction panel rows and quote panel rows by
#: cohort instead of by which module wrote them.
DEFAULT_COHORT = "external_transaction_response"

CLOCK_MODE_SOURCE = "source"
CLOCK_MODE_USABLE = "usable"
CLOCK_MODE_ASSUMED_DELAY = "assumed_delay"

#: The permitted clock modes, in the order the study's plan lists them.
CLOCK_MODES: tuple[str, ...] = (CLOCK_MODE_SOURCE, CLOCK_MODE_USABLE, CLOCK_MODE_ASSUMED_DELAY)

#: Availability recorded for external trade rows. No receipt evidence exists for
#: an archive row, so no usable interval is established and none is invented.
USABLE_AVAILABILITY_STATUS = "unidentifiable"

#: Availability recorded for a source-time row. The venue's recorded time is the
#: only time there is, and it is retrospective alignment rather than evidence of
#: what a live participant knew.
SOURCE_AVAILABILITY_STATUS = "source_time_only"

# Masking vocabulary. Each name matches ``masking.exclusion_reasons`` in
# ``configs/external_history_v1.yaml`` except ``availability_unidentifiable``,
# which is the clock-mode mask: it marks a ``usable`` fold whose availability is
# unknown rather than any fault in the price evidence.
REASON_MISSING_BASELINE = "missing_baseline"
REASON_BASELINE_BEYOND_CAP = "baseline_beyond_cap"
REASON_NO_POST_RELEASE_TRADE = "no_post_release_trade"
REASON_ENDPOINT_BEYOND_CAP = "endpoint_beyond_cap"
REASON_CONTRACT_CLOSED_BEFORE_RELEASE = "contract_closed_before_release"
REASON_RULE_EVIDENCE_MISSING = "rule_evidence_missing"
REASON_RULE_VERSION_UNKNOWN = "rule_version_unknown"
REASON_AMBIGUOUS_OUTCOME_AXIS = "ambiguous_outcome_axis"
REASON_AVAILABILITY_UNIDENTIFIABLE = "availability_unidentifiable"

#: The reason vocabulary this module can write, in precedence order. The first
#: applicable reason is the row's ``exclusion_reason``; the whole list is kept in
#: ``flags_json`` so a lower-precedence fault is recorded rather than hidden.
EXCLUSION_REASONS: tuple[str, ...] = (
    REASON_AVAILABILITY_UNIDENTIFIABLE,
    REASON_RULE_EVIDENCE_MISSING,
    REASON_RULE_VERSION_UNKNOWN,
    REASON_CONTRACT_CLOSED_BEFORE_RELEASE,
    REASON_MISSING_BASELINE,
    REASON_BASELINE_BEYOND_CAP,
    REASON_NO_POST_RELEASE_TRADE,
    REASON_ENDPOINT_BEYOND_CAP,
    REASON_AMBIGUOUS_OUTCOME_AXIS,
)

#: Units of the ``baseline``, ``endpoint`` and ``response`` columns. They are the
#: declared event-axis price in absolute probability units on the 0-1 scale.
PRICE_SCALE = "probability_units_0_1"

#: How the price columns were obtained. The event-axis convention is documented
#: by the venue's own mapping, and no widening, smoothing, interpolation or
#: post-close filling is applied to reach a value.
PRICE_CONVENTION = "documented_event_axis_price_without_imputation"

#: Time bases recorded beside the source times, and the basis of the response
#: label. A source-time label is retrospective alignment; an assumed-delay label
#: is conditional on the stated delay; a usable label does not exist for these
#: rows at all.
TIME_BASIS_SOURCE = "source_time"
TIME_BASIS_ASSUMED_DELAY = "source_time_plus_assumed_delay"
LABEL_BASIS_SOURCE = "retrospective_source_time_alignment"
LABEL_BASIS_ASSUMED_DELAY = "assumption_conditional_source_time_plus_assumed_delay"

#: Names the denominator the observed and missing fractions are measured against.
PRESELECTED_DENOMINATOR = "candidate_pairs_in_declared_observation_window"

#: How the candidate universe was selected. The declared listing grid is chosen
#: from pre-event information only; window activity is chosen by what traded,
#: which is post-event information the plan prohibits as a universe selector. A
#: panel reports which of the two it used so an activity-selected universe cannot
#: be read as a declared one.
UNIVERSE_DECLARED_GRID = "declared_listing_grid"
UNIVERSE_WINDOW_ACTIVITY = "window_activity"

#: The permitted universe selectors.
UNIVERSES: tuple[str, ...] = (UNIVERSE_DECLARED_GRID, UNIVERSE_WINDOW_ACTIVITY)


def _text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty str, got {value!r}")
    return value


def _seconds(value: object, *, field_name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(
            f"{field_name} must be a whole number of seconds >= {minimum}, got {value!r}"
        )
    return value


@dataclass(frozen=True, slots=True)
class EventSpec:
    """One economic release the panel is aligned on.

    ``rule_version`` and ``rule_evidence_quality`` are the contract's rule
    evidence, not the release's. They are absent when no archived rule revision
    covers the contract, which makes a row invalid with ``rule_evidence_missing``
    or ``rule_version_unknown`` even when both price legs were observed: a price
    change whose payoff semantics are unverified is not the study's response.

    ``closed_before_release`` marks a direct contract that had already settled, so
    its price cannot move on the release. The observed legs are still reported,
    because they are prints the tape holds, and the row carries a null response
    with ``contract_closed_before_release`` rather than the structural zero a
    carried-forward price would produce.
    """

    event_id: str
    cluster_id: str
    family: str
    event_time: dt.datetime
    rule_version: str | None = None
    rule_evidence_quality: str | None = None
    closed_before_release: bool = False

    def __post_init__(self) -> None:
        _text(self.event_id, field_name="EventSpec.event_id")
        _text(self.cluster_id, field_name="EventSpec.cluster_id")
        _text(self.family, field_name="EventSpec.family")
        object.__setattr__(
            self,
            "event_time",
            parse_utc_time(self.event_time, field_name="EventSpec.event_time"),
        )
        for name in ("rule_version", "rule_evidence_quality"):
            value = getattr(self, name)
            if value is not None:
                _text(value, field_name=f"EventSpec.{name}")
        if not isinstance(self.closed_before_release, bool):
            raise TypeError(
                "EventSpec.closed_before_release must be a bool, got "
                f"{type(self.closed_before_release).__name__}"
            )

    def as_dict(self) -> dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}


def _config_seconds(block: Mapping[str, Any], key: str, default: int, *, minimum: int = 0) -> int:
    return _seconds(block.get(key, default), field_name=f"response.{key}", minimum=minimum)


def _config_flag(block: Mapping[str, Any], key: str, default: bool) -> bool:
    value = block.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"response.{key} must be a boolean, got {value!r}")
    return value


def _config_horizons(block: Mapping[str, Any], key: str, default: Sequence[int]) -> tuple[int, ...]:
    value = block.get(key, default)
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(
            f"response.{key} must be a sequence of whole horizons in seconds, got {value!r}"
        )
    horizons = tuple(_seconds(entry, field_name=f"response.{key}", minimum=1) for entry in value)
    if not horizons:
        raise ValueError(f"response.{key} must name at least one horizon")
    return tuple(sorted(set(horizons)))


@dataclass(frozen=True, slots=True)
class PanelSettings:
    """The masking and window rules a panel is built under.

    These are the pipeline's provisional trade-age defaults, deliberately read
    from the configuration instead of hard-coded at the call site, so a panel
    records the rules it was measured under. ``assumed_delay_seconds`` is the
    declared delay of the ``assumed_delay`` sensitivity scenario; it is null when
    no delay is declared, because an undeclared delay is not an assumption.
    """

    pre_window_seconds: int = 1800
    post_window_seconds: int = 3600
    baseline_max_age_seconds: int = 120
    endpoint_max_age_seconds: int = 120
    horizons_seconds: tuple[int, ...] = (60, 300, 900, 1800, 3600)
    primary_horizon_seconds: int = 300
    require_post_release_trade: bool = True
    report_endpoint_envelope: bool = True
    assumed_delay_seconds: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "pre_window_seconds",
            "post_window_seconds",
            "baseline_max_age_seconds",
            "endpoint_max_age_seconds",
        ):
            object.__setattr__(
                self, name, _seconds(getattr(self, name), field_name=f"PanelSettings.{name}")
            )
        object.__setattr__(
            self,
            "horizons_seconds",
            _config_horizons({"horizons_seconds": self.horizons_seconds}, "horizons_seconds", ()),
        )
        object.__setattr__(
            self,
            "primary_horizon_seconds",
            _seconds(
                self.primary_horizon_seconds,
                field_name="PanelSettings.primary_horizon_seconds",
                minimum=1,
            ),
        )
        for name in ("require_post_release_trade", "report_endpoint_envelope"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(
                    f"PanelSettings.{name} must be a bool, got {type(getattr(self, name)).__name__}"
                )
        if self.assumed_delay_seconds is not None:
            object.__setattr__(
                self,
                "assumed_delay_seconds",
                _seconds(
                    self.assumed_delay_seconds,
                    field_name="PanelSettings.assumed_delay_seconds",
                    minimum=1,
                ),
            )

    @property
    def assumed_delay_declared(self) -> bool:
        return self.assumed_delay_seconds is not None

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> PanelSettings:
        """Read the panel rules from the pipeline configuration.

        A configuration without a ``response`` block raises instead of falling
        back to the defaults here. The block is where the study declares its
        window, caps and horizons, and a second definition in this module would
        silently become the measurement rule a run was actually built under.
        """
        if not isinstance(config, Mapping):
            raise TypeError(
                "PanelSettings.from_config expects the parsed configuration mapping, got "
                f"{type(config).__name__}"
            )
        block = config.get("response")
        if not isinstance(block, Mapping):
            raise ValueError(
                f"the configuration carries no `response` mapping; the observation window, the "
                f"trade-age caps and the horizons must come from {CONFIG_PATH} and this module "
                "defines no fallback window"
            )
        clock = config.get("clock")
        clock = clock if isinstance(clock, Mapping) else {}
        defaults = {field.name: field.default for field in fields(cls)}
        delay = clock.get("assumed_delay_seconds")
        return cls(
            pre_window_seconds=_config_seconds(
                block, "pre_window_seconds", defaults["pre_window_seconds"]
            ),
            post_window_seconds=_config_seconds(
                block, "post_window_seconds", defaults["post_window_seconds"]
            ),
            baseline_max_age_seconds=_config_seconds(
                block, "baseline_max_age_seconds", defaults["baseline_max_age_seconds"]
            ),
            endpoint_max_age_seconds=_config_seconds(
                block, "endpoint_max_age_seconds", defaults["endpoint_max_age_seconds"]
            ),
            horizons_seconds=_config_horizons(
                block, "horizons_seconds", defaults["horizons_seconds"]
            ),
            primary_horizon_seconds=_config_seconds(
                block,
                "primary_horizon_seconds",
                defaults["primary_horizon_seconds"],
                minimum=1,
            ),
            require_post_release_trade=_config_flag(
                block, "require_post_release_trade", defaults["require_post_release_trade"]
            ),
            report_endpoint_envelope=_config_flag(
                block, "report_endpoint_envelope", defaults["report_endpoint_envelope"]
            ),
            assumed_delay_seconds=(
                None
                if delay is None
                else _seconds(delay, field_name="clock.assumed_delay_seconds", minimum=1)
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            field.name: (
                list(getattr(self, field.name))
                if isinstance(getattr(self, field.name), tuple)
                else getattr(self, field.name)
            )
            for field in fields(self)
        }


def load_panel_settings(config_path: str | Path = CONFIG_PATH) -> PanelSettings:
    """Load the panel rules from a pipeline configuration file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"pipeline configuration not found: {path}")
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"pipeline configuration at {path} is not valid YAML: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(
            f"pipeline configuration at {path} must be a mapping, got {type(payload).__name__}"
        )
    return PanelSettings.from_config(payload)


def settings_digest(settings: PanelSettings) -> str:
    """Stable sha256 of the settings, so a panel names the rules it was built under."""
    payload = json.dumps(settings.as_dict(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


#: Rule-evidence quality recorded when the source holds a usable record for the
#: event. The name states where the evidence came from, because that is all a
#: record in a local source establishes.
RULE_EVIDENCE_ARCHIVED_RECORD = "archived_rule_record_in_rule_evidence_source"

#: Rule-evidence quality recorded when the source holds no record for the event.
#: The version stays null beside it, so the row is masked with
#: ``rule_version_unknown`` rather than admitted on the strength of its prices.
RULE_EVIDENCE_ABSENT = "absent_no_archived_rule_record_for_event"

#: Rule-evidence quality recorded when the source's records for the event disagree
#: about the rule version. One column cannot summarize a conflict, so the version
#: stays null and the conflict is named rather than resolved by a rule of thumb.
RULE_EVIDENCE_CONFLICT = "conflicting_rule_versions_recorded_for_event"

_RULE_VERSION_KEYS = ("rule_version", "rule_hash")
_RULE_QUALITY_KEYS = ("rule_evidence_quality", "evidence_quality", "quality")

#: Lifecycle evidence the coverage document states for a candidate. Closure is read
#: from the venue's own close instant and never from the last trade, the absence of
#: prints or the last observation time, none of which date a market closing.
_CLOSE_TIME_KEYS = ("close_time", "close_at", "closed_at")
_CLOSED_COUNT_KEY = "direct_closed_pre_release_count"


def _records_with(
    document: Any, *, event_id: str, required_keys: Sequence[str] = ()
) -> list[Mapping[str, Any]]:
    """Every mapping anywhere in ``document`` that names ``event_id``.

    The walk is recursive because the two evidence sources nest their records
    differently, and a caller should not have to know a source's internal layout to
    read one event's evidence from it.
    """
    found: list[Mapping[str, Any]] = []
    stack: list[Any] = [document]
    while stack:
        current = stack.pop()
        if isinstance(current, Mapping):
            if current.get("event_id") == event_id and all(key in current for key in required_keys):
                found.append(current)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return found


def _rule_evidence_for_event(
    document: Mapping[str, Any] | None, *, event_id: str
) -> tuple[str | None, str]:
    """The rule version recorded for one event, with the quality of that answer.

    A local evidence source is not a verified rule vintage on its own: the pair
    returned here is what the panel records, and a release with no usable record
    comes back with a null version so its rows are masked rather than admitted on a
    trade-history join.
    """
    if document is None:
        return (None, RULE_EVIDENCE_ABSENT)
    versions: set[str] = set()
    quality: str | None = None
    for record in _records_with(document, event_id=event_id):
        names = [
            key
            for key in _RULE_VERSION_KEYS
            if isinstance(record.get(key), str) and record[key].strip()
        ]
        if not names:
            continue
        # A record that names a version is stronger evidence than one that names
        # only a rule digest, so the digest is read only to keep a version from
        # going missing, and never in place of one that was stated.
        for key in names:
            versions.add(record[key].strip())
        if quality is None:
            for name in _RULE_QUALITY_KEYS:
                recorded = record.get(name)
                if isinstance(recorded, str) and recorded.strip():
                    quality = recorded
                    break
    if not versions:
        return (None, RULE_EVIDENCE_ABSENT)
    if len(versions) > 1:
        return (None, RULE_EVIDENCE_CONFLICT)
    return (next(iter(versions)), quality or RULE_EVIDENCE_ARCHIVED_RECORD)


def _closed_before_release(
    document: Mapping[str, Any] | None, *, event_id: str, event_time: dt.datetime
) -> bool:
    """Whether the coverage evidence states that the event's contracts had closed.

    Both a stated close instant at or before the release and a non-zero count of
    direct contracts closed before it are required. A record that states neither is
    no evidence of closure, and the absence of evidence is reported as open rather
    than converted into a closure claim, because a market that stops trading is not
    a market that has settled.
    """
    if document is None:
        return False
    for record in _records_with(document, event_id=event_id):
        count = record.get(_CLOSED_COUNT_KEY)
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            continue
        candidates = record.get("candidates")
        if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
            continue
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                continue
            for key in _CLOSE_TIME_KEYS:
                stated = candidate.get(key)
                if stated is None:
                    continue
                if isinstance(stated, str) and not stated.strip():
                    continue
                try:
                    closed_at = parse_utc_time(stated, field_name=f"coverage.{key}")
                except (TypeError, ValueError):
                    # An unreadable close instant is not a closure, and refusing the
                    # whole dataset over one malformed field would lose the evidence
                    # that did parse. It is skipped, and the count is what gates the
                    # claim.
                    continue
                if closed_at <= event_time:
                    return True
    return False


def event_spec_from_row(
    row: Mapping[str, Any],
    *,
    rule_evidence: Mapping[str, Any] | None = None,
    audit_coverage: Mapping[str, Any] | None = None,
) -> EventSpec:
    """One release row as an :class:`EventSpec`, with the evidence that covers it.

    ``scheduled_at`` must carry an explicit offset. A naive instant is refused
    rather than read as UTC, because guessing the zone of a release time moves the
    whole observation window around it.

    ``cluster_id`` is the event itself. The plan makes the economic release the
    equal-weight aggregation unit and requires every market and venue answering one
    release to stay in one split, so one release is one cluster by default and a
    coarser grouping is a declared choice a caller makes outside this loader.
    """
    if not isinstance(row, Mapping):
        raise TypeError(
            f"release row must be a mapping, got {type(row).__name__}; a sealed release dataset "
            "is read as rows, never as a positional record"
        )
    event_id = _text(row.get("event_id"), field_name="releases.event_id")
    family = _text(row.get("family"), field_name="releases.family")
    event_time = parse_utc_time(row.get("scheduled_at"), field_name="releases.scheduled_at")
    rule_version, rule_quality = _rule_evidence_for_event(rule_evidence, event_id=event_id)
    return EventSpec(
        event_id=event_id,
        cluster_id=event_id,
        family=family,
        event_time=event_time,
        rule_version=rule_version,
        rule_evidence_quality=rule_quality,
        closed_before_release=_closed_before_release(
            audit_coverage, event_id=event_id, event_time=event_time
        ),
    )


def _load_optional_document(path: str | Path | None, *, purpose: str) -> Mapping[str, Any] | None:
    if path is None:
        return None
    target = Path(path)
    if not target.exists():
        # An absent evidence file is a reported absence, not a failed run: the panel
        # masks the rows the missing evidence would have admitted.
        return None
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{purpose} at {target} is not valid JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(
            f"{purpose} at {target} must be a JSON object, got {type(payload).__name__}"
        )
    return payload


def load_event_specs(
    release_path: str | Path,
    *,
    rule_evidence_path: str | Path | None = None,
    audit_coverage_path: str | Path | None = None,
) -> tuple[EventSpec, ...]:
    """Every archived release as an :class:`EventSpec`, in scheduled order.

    The release dataset is read through :func:`market_propagation.storage.read_parquet`
    so its content hash and declared schema version are verified before any row is
    used; an unverified or schema-disagreeing dataset raises instead of being read.

    Both evidence paths are optional. With neither supplied, one spec per release is
    still returned with the evidence fields marked absent, which makes every row of
    that release masked rather than silently treated as rule-verified.
    """
    releases = read_parquet(release_path)
    rule_evidence = _load_optional_document(rule_evidence_path, purpose="rule evidence")
    audit_coverage = _load_optional_document(audit_coverage_path, purpose="audit coverage")
    specs = [
        event_spec_from_row(row, rule_evidence=rule_evidence, audit_coverage=audit_coverage)
        for row in releases.to_dict("records")
    ]
    if not specs:
        raise ValueError(
            f"the release dataset at {release_path} holds no row; a panel with no release has no "
            "observation window to measure, and this loader defines no default event"
        )
    duplicated = sorted({spec.event_id for spec in specs if _count(specs, spec.event_id) > 1})
    if duplicated:
        raise ValueError(
            f"the release dataset names {duplicated} more than once; one release would then be "
            "measured under two windows and its rows would collide on (event, contract, horizon)"
        )
    return tuple(sorted(specs, key=lambda spec: (spec.event_time, spec.event_id)))


def _count(specs: Sequence[EventSpec], event_id: str) -> int:
    return sum(1 for spec in specs if spec.event_id == event_id)


@dataclass(frozen=True, slots=True)
class _PairBuild:
    """Rows for one event/contract pair, with the pair-level facts counts need."""

    rows: tuple[dict[str, Any], ...]
    pre_event_observed: bool
    post_release_only: bool
    mixed_raw_price_units: bool
    window_traded: bool


@dataclass(frozen=True, slots=True)
class _PairOutcome:
    """One candidate pair as the counts see it."""

    event_id: str
    cluster_id: str
    family: str
    pre_event_observed: bool
    post_release_only: bool
    lifecycle_eligible: bool
    rule_verified: bool
    baseline_observed: bool
    endpoint_observed: bool
    rows: tuple[dict[str, Any], ...]
    declared: bool = False
    window_traded: bool = False


def _source_time(print: HistoricalTrade) -> dt.datetime | None:
    return print.clock.source_time


def _axis_time(print: HistoricalTrade, assumed_delay: int) -> dt.datetime:
    """The print's position on the observation axis.

    An assumed delay perturbs only when a print becomes available, never the
    recorded transaction time: ``source`` and ``usable`` order prints by the venue
    time, and the delay scenario shifts the point a print could have been seen.
    """
    time = _source_time(print)
    if time is None:
        raise ValueError("a print without a source time has no position on the transaction axis")
    return time + dt.timedelta(seconds=assumed_delay)


def _print_rank(print: HistoricalTrade) -> tuple[dt.datetime, str, str]:
    """Order prints by recorded time, then by occurrence identity.

    ``provenance.record_id`` is the occurrence locator, so repeated identical
    fills stay distinct and the ordering is total. The order selects which print a
    leg is measured from; it is never evidence that one print reached anyone
    first.
    """
    time = _source_time(print)
    if time is None:
        raise ValueError("a print without a source time cannot be ranked on the transaction axis")
    return (time, print.provenance.record_id, print.trade_id or "")


def _tail_group(prints: Sequence[HistoricalTrade]) -> list[HistoricalTrade]:
    """Prints sharing the finest recorded timestamp of the last print.

    Archive rows carry the venue's own timestamp precision, so prints that share
    it are one declared tie group. Quantities are frequently unavailable and no
    ordering evidence exists inside a tie, so the group is one observation.
    """
    if not prints:
        return []
    finest = max(_source_time(print) for print in prints)  # type: ignore[type-var]
    return [print for print in prints if _source_time(print) == finest]


def _mean(values: Sequence[Decimal]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _event_axis_price(print: HistoricalTrade) -> Decimal | None:
    return print.event_price if print.event_axis is not None else None


def _leg_axis_mean(group: Sequence[HistoricalTrade]) -> Decimal | None:
    """Unweighted mean event-axis price of one leg's tie group, or null when unknown.

    A group where any print lacks a documented event axis has an unknown mean, and
    a partial average would report a projection the archive does not support.
    """
    if not group:
        return None
    prices = [_event_axis_price(print) for print in group]
    if any(price is None for price in prices):
        return None
    return _mean([price for price in prices if price is not None])


def _axis_summary(*groups: Sequence[HistoricalTrade]) -> str | None:
    """The one documented event axis a row's prices are on, or null when not one.

    A row where any priced leg lacks a documented axis, or where the legs were
    projected onto different axes, has no single event axis. The column stays null
    for it rather than naming an axis only part of the row used, and the masking
    step records the fault separately.
    """
    axes: set[str] = set()
    for group in groups:
        for print in group:
            if print.event_axis is None:
                return None
            axes.add(print.event_axis)
    if len(axes) == 1:
        return next(iter(axes))
    return None


def _axes_disagree(groups: Sequence[Sequence[HistoricalTrade]]) -> bool:
    """Whether the groups project onto more than one documented event axis."""
    axes = {print.event_axis for group in groups for print in group if print.event_axis is not None}
    return len(axes) > 1


def _raw_price_mean(group: Sequence[HistoricalTrade], *, units_consistent: bool) -> Decimal | None:
    """Unweighted mean of the group's venue raw prices, or null when unknown.

    A group where any print's raw price is unknown has an unknown mean: averaging
    the known ones would report a value the archive does not support.
    """
    if not group or not units_consistent:
        return None
    prices = [print.raw_price for print in group]
    if any(price is None for price in prices):
        return None
    return _mean([price for price in prices if price is not None])


def _size_evidence(group: Sequence[HistoricalTrade]) -> tuple[str | None, bool | None]:
    """Size quality and verifiedness of one group's prints.

    A group whose prints disagree cannot be summarised by one declared quality, so
    the quality stays null and only the verifiedness is asserted, which keeps the
    pair of columns from contradicting each other.
    """
    if not group:
        return (None, None)
    qualities = {print.size_quality for print in group}
    if len(qualities) != 1:
        return (None, False)
    quality = qualities.pop()
    return (quality, quality == "verified_source_quantity")


def _locators(group: Sequence[HistoricalTrade]) -> dict[str, list[str]]:
    return {
        "occurrences": sorted({print.provenance.record_id for print in group}),
        "raw_hashes": sorted({print.provenance.raw_hash for print in group}),
        "sources": sorted({print.provenance.source for print in group}),
    }


def _panel_row(*, table_columns: Sequence[str], **values: Any) -> dict[str, Any]:
    missing = [name for name in table_columns if name not in values]
    extra = sorted(set(values) - set(table_columns))
    if missing or extra:
        raise ValueError(
            f"trade panel row does not match the declared columns: missing {missing}, "
            f"undeclared {extra}"
        )
    return {name: values[name] for name in table_columns}


def _row(
    *,
    event: EventSpec,
    venue: str,
    contract_id: str,
    cohort: str,
    clock_mode: str,
    assumed_delay: int,
    settings: PanelSettings,
    horizon: int,
    reasons: Sequence[str],
    baseline_group: Sequence[HistoricalTrade],
    endpoint_group: Sequence[HistoricalTrade],
    baseline_count: int,
    endpoint_count: int,
    post_release_trade_observed: bool,
    row_flags: Sequence[str],
    units_consistent: bool,
) -> dict[str, Any]:
    tau = event.event_time
    target = tau + dt.timedelta(seconds=horizon)
    baseline_time = _source_time(baseline_group[0]) if baseline_group else None
    endpoint_time = _source_time(endpoint_group[0]) if endpoint_group else None
    valid = not reasons

    time_basis = (
        TIME_BASIS_ASSUMED_DELAY if clock_mode == CLOCK_MODE_ASSUMED_DELAY else TIME_BASIS_SOURCE
    )
    if clock_mode == CLOCK_MODE_SOURCE:
        label_basis: str | None = LABEL_BASIS_SOURCE
    elif clock_mode == CLOCK_MODE_ASSUMED_DELAY:
        label_basis = LABEL_BASIS_ASSUMED_DELAY
    else:
        label_basis = None

    baseline_value: float | None = None
    endpoint_value: float | None = None
    response: float | None = None
    baseline_raw: float | None = None
    endpoint_raw: float | None = None
    response_min: float | None = None
    response_max: float | None = None
    envelope_low: float | None = None
    envelope_high: float | None = None
    # A leg's price is an observation, so it is reported whenever that leg was
    # observed, even on a row the release's own evidence masks out. The masking
    # applies to the estimand: ``response``, the tie statistics and the envelope
    # are populated only for a valid row, so a masked row reports what was seen
    # without offering a change a reader could mistake for the study's response.
    baseline_mean = _leg_axis_mean(baseline_group)
    endpoint_mean = _leg_axis_mean(endpoint_group)
    baseline_value = None if baseline_mean is None else float(baseline_mean)
    endpoint_value = None if endpoint_mean is None else float(endpoint_mean)
    if valid and baseline_mean is not None and endpoint_mean is not None:
        responses = [print.event_price - baseline_mean for print in endpoint_group]  # type: ignore[operator]
        response = float(endpoint_mean - baseline_mean)
        response_min = float(min(responses))
        response_max = float(max(responses))
        if settings.report_endpoint_envelope:
            envelope_low = response_min
            envelope_high = response_max
    baseline_raw_mean = _raw_price_mean(baseline_group, units_consistent=units_consistent)
    endpoint_raw_mean = _raw_price_mean(endpoint_group, units_consistent=units_consistent)
    baseline_raw = None if baseline_raw_mean is None else float(baseline_raw_mean)
    endpoint_raw = None if endpoint_raw_mean is None else float(endpoint_raw_mean)

    sizes = _size_evidence(endpoint_group)
    size_quality, size_verified = sizes

    flags_json: dict[str, Any] = {
        "flags": sorted(set(row_flags)),
        "exclusion_reasons": list(reasons),
        "require_post_release_trade": settings.require_post_release_trade,
    }
    if clock_mode == CLOCK_MODE_ASSUMED_DELAY:
        flags_json["assumed_delay_seconds"] = assumed_delay

    availability = (
        SOURCE_AVAILABILITY_STATUS
        if clock_mode == CLOCK_MODE_SOURCE
        else USABLE_AVAILABILITY_STATUS
    )

    return _panel_row(
        table_columns=TRADE_PANEL_COLUMNS,
        event_id=event.event_id,
        cluster_id=event.cluster_id,
        family=event.family,
        venue=venue,
        contract_id=contract_id,
        cohort=cohort,
        event_time=tau,
        horizon_seconds=horizon,
        baseline_source_time=baseline_time,
        endpoint_source_time=endpoint_time,
        baseline_time_basis=time_basis if baseline_time is not None else None,
        endpoint_time_basis=time_basis if endpoint_time is not None else None,
        baseline=baseline_value,
        endpoint=endpoint_value,
        response=response,
        baseline_raw_price=baseline_raw,
        endpoint_raw_price=endpoint_raw,
        price_scale=PRICE_SCALE,
        price_convention=PRICE_CONVENTION,
        event_axis=_axis_summary(baseline_group, endpoint_group),
        baseline_age_seconds=(
            None if baseline_time is None else (tau - baseline_time).total_seconds()
        ),
        endpoint_age_seconds=(
            None if endpoint_time is None else (target - endpoint_time).total_seconds()
        ),
        baseline_trade_count=baseline_count,
        endpoint_trade_count=endpoint_count,
        tie_group_size=len(endpoint_group) if endpoint_group else None,
        tie_group_response_min=response_min,
        tie_group_response_max=response_max,
        endpoint_envelope_low=envelope_low,
        endpoint_envelope_high=envelope_high,
        post_release_trade_observed=post_release_trade_observed,
        rule_version=event.rule_version,
        rule_evidence_quality=event.rule_evidence_quality,
        valid=valid,
        exclusion_reason=reasons[0] if reasons else None,
        clock_mode=clock_mode,
        availability_status=availability,
        label_time_basis=label_basis,
        size_quality=size_quality,
        size_verified=size_verified,
        provenance_locators_json={
            "baseline": _locators(baseline_group),
            "endpoint": _locators(endpoint_group),
        },
        flags_json=flags_json,
    )


def _pair_build(
    event: EventSpec,
    key: tuple[str, str],
    prints: Sequence[HistoricalTrade],
    *,
    settings: PanelSettings,
    clock_mode: str,
    cohort: str,
    assumed_delay: int,
    declared: bool = False,
) -> _PairBuild | None:
    """Every horizon row for one event/contract pair, or ``None`` when not a candidate.

    A pair is a candidate when the release's declared listing grid names it. When
    no grid is supplied the pair is a candidate when the declared observation
    window holds at least one print, and the panel reports that the universe was
    selected by activity rather than by pre-event information. A declared pair
    with no print in the window is still built, masked, so the missing cell stays
    in the denominator instead of disappearing with its evidence.
    """
    venue, contract_id = key
    tau = event.event_time
    dated = sorted((print for print in prints if _source_time(print) is not None), key=_print_rank)
    pre_floor = tau - dt.timedelta(seconds=settings.pre_window_seconds)
    post_ceiling = tau + dt.timedelta(seconds=settings.post_window_seconds)
    pre_window = [print for print in dated if pre_floor <= _axis_time(print, assumed_delay) < tau]
    post_window = [
        print for print in dated if tau < _axis_time(print, assumed_delay) <= post_ceiling
    ]
    window_traded = bool(pre_window or post_window)
    if not window_traded and not declared:
        return None

    units = {print.raw_price_units for print in dated}
    units_consistent = len(units) <= 1
    untimed_before = [print for print in dated if _axis_time(print, assumed_delay) < pre_floor]
    post_release_only = bool(post_window) and not pre_window

    rows: list[dict[str, Any]] = []
    for horizon in settings.horizons_seconds:
        target = tau + dt.timedelta(seconds=horizon)
        reasons: list[str] = []
        row_flags: list[str] = []

        if post_release_only:
            row_flags.append("candidate_from_post_release_activity_only")
        if not units_consistent:
            row_flags.append("mixed_raw_price_units_in_pair")

        if clock_mode == CLOCK_MODE_USABLE:
            # The price evidence below is read on the venue's source axis. A
            # usable fold cannot be claimed from it, so the row is masked and its
            # estimand columns stay null rather than carrying a source-time price
            # into a usable-time result.
            reasons.append(REASON_AVAILABILITY_UNIDENTIFIABLE)
        if event.rule_evidence_quality is None:
            reasons.append(REASON_RULE_EVIDENCE_MISSING)
        elif event.rule_version is None:
            reasons.append(REASON_RULE_VERSION_UNKNOWN)
        if event.closed_before_release:
            reasons.append(REASON_CONTRACT_CLOSED_BEFORE_RELEASE)

        baseline_group = _tail_group(pre_window)
        if not baseline_group:
            reasons.append(REASON_MISSING_BASELINE)
            if untimed_before:
                row_flags.append("baseline_print_older_than_declared_pre_window")
        elif (tau - _source_time(baseline_group[0])).total_seconds() > (  # type: ignore[operator]
            settings.baseline_max_age_seconds
        ):
            reasons.append(REASON_BASELINE_BEYOND_CAP)

        endpoint_pool = [
            print for print in post_window if _axis_time(print, assumed_delay) <= target
        ]
        endpoint_group = _tail_group(endpoint_pool)
        post_release_trade_observed = bool(endpoint_group)
        if not endpoint_group:
            reasons.append(REASON_NO_POST_RELEASE_TRADE)
        else:
            if horizon > settings.post_window_seconds:
                # The horizon reaches past the declared observation window, so the
                # search above was truncated. The row records the truncated print
                # and the reason rather than a value for a window never observed.
                reasons.append(REASON_ENDPOINT_BEYOND_CAP)
                row_flags.append("horizon_beyond_declared_post_window")
            endpoint_age = (
                target - _source_time(endpoint_group[0])  # type: ignore[operator]
            ).total_seconds()
            if endpoint_age > settings.endpoint_max_age_seconds:
                reasons.append(REASON_ENDPOINT_BEYOND_CAP)

        used = [*baseline_group, *endpoint_group]
        if any(_event_axis_price(print) is None for print in used):
            reasons.append(REASON_AMBIGUOUS_OUTCOME_AXIS)
        elif _axes_disagree((baseline_group, endpoint_group)):
            # Both legs are projected, but onto different documented axes, so their
            # difference is not a response on either one.
            reasons.append(REASON_AMBIGUOUS_OUTCOME_AXIS)
            row_flags.append("event_axes_disagree_between_baseline_and_endpoint")
        if len(endpoint_group) > 1:
            row_flags.append("endpoint_tie_group_aggregated")
        if len(baseline_group) > 1:
            row_flags.append("baseline_tie_group_aggregated")
        for print in used:
            row_flags.extend(print.flags)

        rows.append(
            _row(
                event=event,
                venue=venue,
                contract_id=contract_id,
                cohort=cohort,
                clock_mode=clock_mode,
                assumed_delay=assumed_delay,
                settings=settings,
                horizon=horizon,
                reasons=reasons,
                baseline_group=baseline_group,
                endpoint_group=endpoint_group,
                baseline_count=len(pre_window),
                endpoint_count=len(
                    [print for print in post_window if _axis_time(print, assumed_delay) <= target]
                ),
                post_release_trade_observed=post_release_trade_observed,
                row_flags=row_flags,
                units_consistent=units_consistent,
            )
        )
    return _PairBuild(
        rows=tuple(rows),
        pre_event_observed=bool(pre_window),
        post_release_only=post_release_only,
        mixed_raw_price_units=not units_consistent,
        window_traded=window_traded,
    )


def _count_block(outcomes: Sequence[_PairOutcome], *, settings: PanelSettings) -> dict[str, Any]:
    rows = [row for outcome in outcomes for row in outcome.rows]
    primary = [row for row in rows if row["horizon_seconds"] == settings.primary_horizon_seconds]
    observed = sum(1 for row in primary if row["post_release_trade_observed"] is True)
    denominator = len(primary)
    reasons = Counter(
        row["exclusion_reason"]
        for row in rows
        if row["valid"] is False and row["exclusion_reason"] is not None
    )
    clusters = {outcome.cluster_id for outcome in outcomes}
    return {
        "candidate_pairs": len(outcomes),
        "declared_candidate_pairs": sum(1 for item in outcomes if item.declared),
        "pairs_without_any_window_trade": sum(1 for item in outcomes if not item.window_traded),
        "pre_event_observed_pairs": sum(1 for item in outcomes if item.pre_event_observed),
        "candidate_pairs_from_post_release_activity_only": sum(
            1 for item in outcomes if item.post_release_only
        ),
        "lifecycle_eligible_pairs": sum(1 for item in outcomes if item.lifecycle_eligible),
        "rule_verified_pairs": sum(1 for item in outcomes if item.rule_verified),
        "baseline_observed_pairs": sum(1 for item in outcomes if item.baseline_observed),
        "endpoint_observed_pairs": sum(1 for item in outcomes if item.endpoint_observed),
        "release_clusters": sorted(clusters),
        "distinct_release_clusters": len(clusters),
        "rows": len(rows),
        "valid_rows": sum(1 for row in rows if row["valid"] is True),
        "invalid_rows": sum(1 for row in rows if row["valid"] is False),
        "primary_horizon_seconds": settings.primary_horizon_seconds,
        "primary_horizon_rows": denominator,
        "primary_horizon_valid_rows": sum(1 for row in primary if row["valid"] is True),
        "primary_horizon_endpoint_observed_rows": observed,
        # A null is not zero: with nothing at the primary horizon there is no
        # fraction to report, and reporting 0.0 would assert a measured absence.
        "observed_fraction": (observed / denominator) if denominator else None,
        "missing_fraction": (1.0 - observed / denominator) if denominator else None,
        "exclusion_reason_counts": dict(sorted(reasons.items())),
    }


def _counts(
    outcomes: Sequence[_PairOutcome],
    settings: PanelSettings,
    *,
    cohort: str,
    clock_mode: str,
    universe: str,
) -> dict[str, Any]:
    by_event: dict[str, dict[str, Any]] = {}
    for event_id in sorted({outcome.event_id for outcome in outcomes}):
        group = [outcome for outcome in outcomes if outcome.event_id == event_id]
        block = _count_block(group, settings=settings)
        block["cluster_id"] = group[0].cluster_id
        block["family"] = group[0].family
        by_event[event_id] = block
    return {
        "cohort": cohort,
        "clock_mode": clock_mode,
        "candidate_universe": universe,
        "preselected_denominator": PRESELECTED_DENOMINATOR,
        "primary_horizon_seconds": settings.primary_horizon_seconds,
        "horizons_seconds": list(settings.horizons_seconds),
        "overall": _count_block(outcomes, settings=settings),
        "by_event": by_event,
    }


def _panel_flags(
    outcomes: Sequence[_PairOutcome],
    *,
    settings: PanelSettings,
    clock_mode: str,
    undated_prints: int,
    mixed_raw_price_units: bool,
    universe: str,
) -> tuple[str, ...]:
    flags: set[str] = {f"clock_mode_{clock_mode}", f"candidate_universe_from_{universe}"}
    if clock_mode == CLOCK_MODE_USABLE:
        flags.add("usable_time_unavailable_no_interval_established")
    if clock_mode == CLOCK_MODE_ASSUMED_DELAY:
        flags.add(f"assumed_delay_{settings.assumed_delay_seconds}_seconds")
    if settings.report_endpoint_envelope:
        flags.add("endpoint_envelope_reported")
    else:
        flags.add("endpoint_envelope_suppressed")
    if not settings.require_post_release_trade:
        # The estimand needs a new post-release print. Disabling the requirement
        # cannot yield a zero from a carried-forward baseline, so the builder keeps
        # the requirement and records that the declared policy was not applied as
        # written rather than changing the estimand silently.
        flags.add("post_release_trade_requirement_disabled_by_settings")
    if undated_prints:
        flags.add("undated_prints_not_placeable_on_the_transaction_axis")
    if mixed_raw_price_units:
        flags.add("mixed_raw_price_units_in_pair")
    if not outcomes:
        flags.add("no_candidate_pairs")
    primary_rows = sum(
        1
        for outcome in outcomes
        for row in outcome.rows
        if row["horizon_seconds"] == settings.primary_horizon_seconds
    )
    if not primary_rows:
        flags.add("primary_horizon_not_reported")
    return tuple(sorted(flags))


def _declared_grid(
    candidates: Mapping[str, Iterable[tuple[str, str]]], specs: Sequence[EventSpec]
) -> dict[str, tuple[tuple[str, str], ...]]:
    """Normalise the declared per-release candidate grid and refuse a partial one.

    A grid that omits a declared release would fall back to activity selection for
    that release alone, which is exactly the preselection the grid exists to
    remove, so a missing release is an error rather than a default.
    """
    if not isinstance(candidates, Mapping):
        raise TypeError(f"candidates must be a mapping, got {type(candidates).__name__}")
    grid: dict[str, tuple[tuple[str, str], ...]] = {}
    for event_id in {spec.event_id for spec in specs}:
        if event_id not in candidates:
            raise ValueError(
                f"candidates declares no grid for release {event_id!r}; an undeclared release has "
                "no denominator, and falling back to window activity would reselect the universe "
                "from post-event information"
            )
        pairs: list[tuple[str, str]] = []
        for pair in candidates[event_id]:
            if isinstance(pair, str):
                raise TypeError(
                    "candidates entries must be (venue, contract_id) pairs, got a bare string; a "
                    "contract id without its venue is not a candidate key"
                )
            venue, contract_id = pair
            pairs.append(
                (
                    _text(venue, field_name=f"candidates[{event_id!r}] venue"),
                    _text(contract_id, field_name=f"candidates[{event_id!r}] contract_id"),
                )
            )
        grid[event_id] = tuple(sorted(set(pairs)))
    return grid


def build_trade_panel(
    trades: Iterable[HistoricalTrade],
    events: Iterable[EventSpec],
    *,
    settings: PanelSettings,
    clock_mode: str = CLOCK_MODE_SOURCE,
    cohort: str = DEFAULT_COHORT,
    candidates: Mapping[str, Iterable[tuple[str, str]]] | None = None,
) -> TradePanel:
    """Build the masked source-time transaction panel.

    One row is emitted per candidate contract and horizon, and a row is masked in
    place rather than dropped, so missingness against the candidate universe is
    itself reportable. ``clock_mode`` selects the observation axis: ``source``
    reads the venue times directly, ``usable`` refuses to read them as a usable
    interval and masks every row, and ``assumed_delay`` reads them on the axis
    shifted by the delay the settings declare.

    ``candidates`` is the declared per-release grid of ``(venue, contract_id)``
    pairs, chosen from listing intervals before the release. Supplying it makes
    the grid the denominator: every declared pair gets its rows even when it never
    traded, so a missing cell cannot leave the grid with its evidence. Omitting it
    falls back to the window-activity universe, which the counts label as such
    because it selects the universe from post-release information.
    """
    if clock_mode not in CLOCK_MODES:
        raise ValueError(f"clock_mode must be one of {CLOCK_MODES}, got {clock_mode!r}")
    if not isinstance(settings, PanelSettings):
        raise TypeError(f"settings must be a PanelSettings, got {type(settings).__name__}")
    _text(cohort, field_name="cohort")
    if clock_mode == CLOCK_MODE_ASSUMED_DELAY and not settings.assumed_delay_declared:
        raise ValueError(
            "clock_mode 'assumed_delay' needs a declared delay and the settings declare none; an "
            "assumed delay is an explicit scenario, never a measured receipt time"
        )
    assumed_delay = settings.assumed_delay_seconds or 0

    registry: dict[tuple[str, str], list[HistoricalTrade]] = {}
    undated_prints = 0
    for entry in trades:
        if not isinstance(entry, HistoricalTrade):
            raise TypeError(
                f"build_trade_panel expects HistoricalTrade records, got {type(entry).__name__}; "
                "a quote has no transaction time and cannot stand in for one"
            )
        if _source_time(entry) is None:
            undated_prints += 1
        registry.setdefault((entry.venue, entry.contract_id), []).append(entry)

    specs: list[EventSpec] = []
    for spec in events:
        if not isinstance(spec, EventSpec):
            raise TypeError(
                f"build_trade_panel expects EventSpec records, got {type(spec).__name__}"
            )
        specs.append(spec)

    outcomes: list[_PairOutcome] = []
    mixed_raw_price_units = False
    grid = _declared_grid(candidates, specs) if candidates is not None else None
    universe = UNIVERSE_DECLARED_GRID if grid is not None else UNIVERSE_WINDOW_ACTIVITY
    for event in sorted(specs, key=lambda spec: (spec.event_time, spec.event_id)):
        keys = grid[event.event_id] if grid is not None else sorted(registry)
        for key in keys:
            build = _pair_build(
                event,
                key,
                registry.get(key, ()),
                settings=settings,
                clock_mode=clock_mode,
                cohort=cohort,
                assumed_delay=assumed_delay,
                declared=grid is not None,
            )
            if build is None:
                continue
            mixed_raw_price_units = mixed_raw_price_units or build.mixed_raw_price_units
            outcomes.append(
                _PairOutcome(
                    event_id=event.event_id,
                    cluster_id=event.cluster_id,
                    family=event.family,
                    pre_event_observed=build.pre_event_observed,
                    post_release_only=build.post_release_only,
                    lifecycle_eligible=not event.closed_before_release,
                    rule_verified=(
                        event.rule_version is not None and event.rule_evidence_quality is not None
                    ),
                    baseline_observed=any(
                        row["baseline_source_time"] is not None for row in build.rows
                    ),
                    endpoint_observed=any(
                        row["post_release_trade_observed"] is True for row in build.rows
                    ),
                    rows=build.rows,
                    declared=grid is not None,
                    window_traded=build.window_traded,
                )
            )

    rows = tuple(row for outcome in outcomes for row in outcome.rows)
    return TradePanel(
        rows=rows,
        clock_mode=clock_mode,
        cohort=cohort,
        settings=settings,
        counts=_counts(outcomes, settings, cohort=cohort, clock_mode=clock_mode, universe=universe),
        flags=_panel_flags(
            outcomes,
            settings=settings,
            clock_mode=clock_mode,
            undated_prints=undated_prints,
            mixed_raw_price_units=mixed_raw_price_units,
            universe=universe,
        ),
    )


@dataclass(frozen=True, slots=True)
class TradePanel:
    """One built panel: masked rows, the rules they were built under, and counts.

    ``rows`` are plain mappings keyed by
    :data:`market_propagation.storage.TRADE_PANEL_COLUMNS`, so the panel seals
    through the declared schema without a second column list anywhere.
    """

    rows: tuple[dict[str, Any], ...]
    clock_mode: str
    cohort: str
    settings: PanelSettings
    counts: Mapping[str, Any]
    flags: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.clock_mode not in CLOCK_MODES:
            raise ValueError(f"clock_mode must be one of {CLOCK_MODES}, got {self.clock_mode!r}")
        _text(self.cohort, field_name="TradePanel.cohort")
        if not isinstance(self.settings, PanelSettings):
            raise TypeError(f"settings must be a PanelSettings, got {type(self.settings).__name__}")
        expected = list(TRADE_PANEL_COLUMNS)
        rows: list[dict[str, Any]] = []
        for position, row in enumerate(self.rows):
            if not isinstance(row, Mapping):
                raise TypeError(
                    f"TradePanel row {position} must be a mapping, got {type(row).__name__}"
                )
            if list(row) != expected:
                raise ValueError(
                    "TradePanel row "
                    f"{position} does not carry exactly the declared trade panel columns in "
                    f"order: missing {[name for name in expected if name not in row]}, "
                    f"undeclared {sorted(set(row) - set(expected))}"
                )
            rows.append(dict(row))
        object.__setattr__(self, "rows", tuple(rows))
        if not isinstance(self.counts, Mapping):
            raise TypeError(f"counts must be a mapping, got {type(self.counts).__name__}")
        object.__setattr__(self, "counts", dict(self.counts))
        object.__setattr__(self, "flags", tuple(str(flag) for flag in self.flags))

    @property
    def coverage_epoch(self) -> str:
        """Vintage of the panel: the cohort, the clock mode and the measurement rules."""
        return f"{self.cohort}:{self.clock_mode}:{settings_digest(self.settings)[:12]}"

    def write(self, path: str | Path) -> DatasetRef:
        """Seal the panel as the ``trade_panel`` table.

        Sealing the same content twice rewrites identical bytes, so re-running a
        build over unchanged inputs leaves one verifiable dataset rather than an
        ambiguity about which run a panel came from.
        """
        return write_parquet(
            list(self.rows),
            path,
            table="trade_panel",
            coverage_epoch=self.coverage_epoch,
            metadata={
                "cohort": self.cohort,
                "clock_mode": self.clock_mode,
                "settings_digest": settings_digest(self.settings),
                "candidate_pairs": self.counts.get("overall", {}).get("candidate_pairs"),
                "endpoint_observed_pairs": self.counts.get("overall", {}).get(
                    "endpoint_observed_pairs"
                ),
            },
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "cohort": self.cohort,
            "clock_mode": self.clock_mode,
            "coverage_epoch": self.coverage_epoch,
            "row_count": len(self.rows),
            "columns": list(TRADE_PANEL_COLUMNS),
            "settings": self.settings.as_dict(),
            "settings_digest": settings_digest(self.settings),
            "counts": self.counts,
            "flags": list(self.flags),
            "rows": [dict(row) for row in self.rows],
        }
