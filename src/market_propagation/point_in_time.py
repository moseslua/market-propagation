"""Point-in-time features, label availability, and the historical event panel.

Two families live here and they admit different clocks.

* :func:`features_asof` answers "what was usable at ``at``". It admits only
  quotes whose availability interval is known, whose latest bound is at or
  before ``at``, and which were replayed in usable order. A quote with an
  unknown usable time is excluded, because admitting it would push source time
  into a feature set that claims to be information-feasible.
* :func:`build_event_panel` is the historical economic panel. It explicitly
  admits source time, keeps clock-quality and timing-uncertainty columns, and
  labels every row with the replay order that produced it, so a source-order
  response is never mistaken for a forecast.

Masking
-------
Rows are retained when they cannot be used. A missing market, an ineligible
contract, a closed or contaminated window, a rule published after the event, and
a stale quote all produce a row with ``valid=False`` and an ``exclusion_reason``.
Dropping those rows would silently change the sample and hide the reason.

Freshness uses ``last_verified``: an unchanged standing quote is not stale, and
price-change age says nothing about observation validity. When ``last_verified``
is unknown the row fails closed rather than being trusted.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pandas as pd

from .domain import (
    UTC,
    Contract,
    Quote,
    QuoteValidity,
    Release,
    Resolution,
    market_key,
)
from .replay import ORDER_SOURCE, ORDER_USABLE, resolve_order
from .storage import (
    BOOL,
    FLOAT64,
    FORECAST_COLUMNS,
    INT64,
    STRING,
    TABLE_SCHEMAS,
    TIMESTAMP,
)

__all__ = [
    "ASOF_ALL_EVENTS",
    "FORECAST_COLUMNS",
    "PANEL_COLUMNS",
    "PANEL_CONTRACT_COLUMNS",
    "AsOfQuote",
    "available_labels",
    "build_event_panel",
    "features_asof",
]

ASOF_ALL_EVENTS = "*"

_PANEL_BASE_COLUMNS: tuple[str, ...] = (
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
    "baseline",
    "endpoint",
    "response",
    "spread_before",
    "spread_after",
    "depth_before",
    "depth_after",
    "baseline_age_seconds",
    "endpoint_age_seconds",
    "valid",
    "exclusion_reason",
    "raw_hashes",
    "replay_order",
    "clock_quality",
    "timing_uncertainty_seconds",
    "label_available_time",
    "training_cutoff",
    "split",
)

# Payoff semantics travel with every panel row: a pooled regression must know
# which contracts share a payoff orientation before it pools them.
PANEL_CONTRACT_COLUMNS: tuple[str, ...] = (
    "operator",
    "threshold",
    "rounding",
    "units",
    "orientation_sign",
)

PANEL_COLUMNS: tuple[str, ...] = _PANEL_BASE_COLUMNS + PANEL_CONTRACT_COLUMNS


def _forecast_columns_by_type() -> dict[str, tuple[str, ...]]:
    """Forecast column names grouped by their declared Arrow type.

    The declared schema is the only place a forecast column and its type are
    written down, so this boundary reads the grouping off it. A column added to
    the table is materialized with the right dtype here without a second list to
    keep in step.
    """
    declared = dict(TABLE_SCHEMAS["forecast"].columns)
    groups = {
        "string": STRING,
        "timestamp": TIMESTAMP,
        "float": FLOAT64,
        "int": INT64,
        "bool": BOOL,
    }
    return {
        group: tuple(name for name in FORECAST_COLUMNS if declared[name] == arrow_type)
        for group, arrow_type in groups.items()
    }


_FORECAST_COLUMNS_BY_TYPE = _forecast_columns_by_type()

_CLOCK_QUALITY_RANK = {"clock_synced": 2, "clock_unsynced": 1, "unknown": 0}


def _order_time(quote: Quote, order: str) -> dt.datetime | None:
    return quote.source_time if order == ORDER_SOURCE else quote.clock.usable_time


def _rank(quote: Quote, order: str) -> tuple:
    """Position of a quote in the fold, used to keep replay semantics in features.

    The occurrence identity is the tie-break. Two quotes therefore never compare
    equal, and permuting the input cannot change which quote wins.
    """
    time = _order_time(quote, order)
    return (
        1 if time is None else 0,
        0.0 if time is None else time.timestamp(),
        *quote.occurrence_key,
    )


def _latest_by_book_key(quotes: Sequence[Quote], order: str) -> dict[tuple[str, str], Quote]:
    """Latest quote per book key over *all* quotes, regardless of time.

    Used only to decide which books a gap or closure invalidates: a book whose
    last word is an invalid quote stays invalid rather than falling back to an
    earlier valid one.
    """
    latest: dict[tuple[str, str], Quote] = {}
    for quote in quotes:
        key = (quote.market_key, quote.replay_order or order)
        current = latest.get(key)
        if current is None or _rank(quote, order) > _rank(current, order):
            latest[key] = quote
    return latest


@dataclass(frozen=True, slots=True)
class AsOfQuote:
    """One market's point-in-time state, with the reason it is or is not usable."""

    key: str
    candidate: Quote | None
    permitted: bool
    admission_reason: str | None
    feature_reason: str | None
    max_input_available_time: dt.datetime | None
    age_seconds: float | None
    invalidated_at: dt.datetime | None = None

    @property
    def admitted(self) -> bool:
        return self.permitted and self.candidate is not None

    @property
    def valid(self) -> bool:
        """Usable as a feature: admitted, valid, and carrying no feature reason."""
        return (
            self.admitted
            and self.candidate is not None
            and self.candidate.valid
            and self.feature_reason is None
        )

    @property
    def reason(self) -> str | None:
        if self.feature_reason is not None:
            return self.feature_reason
        if self.admission_reason is not None:
            return self.admission_reason
        if self.candidate is None:
            return QuoteValidity.MISSING.value
        return self.candidate.reason


def _book_key_of(quote: Quote) -> tuple[str, str]:
    return (quote.replay_order or "", quote.market_key)


def _asof_states(
    quotes: Sequence[Quote],
    at: dt.datetime,
    *,
    order: str,
    max_age_seconds: float | None,
) -> dict[tuple[str, str], AsOfQuote]:
    """Point-in-time state for every book key seen, at ``at``.

    A book is admitted only when its own as-of candidate is valid. When the
    candidate is invalid the book is excluded, even if an earlier quote in the
    same fold was valid: a gap or closure cannot be bypassed by looking further
    back.
    """
    candidate_by_key: dict[tuple[str, str], Quote] = {}
    invalidated: dict[tuple[str, str], Quote] = {}
    unplaceable: dict[tuple[str, str], Quote] = {}
    for quote in quotes:
        key = _book_key_of(quote)
        time = _order_time(quote, order)
        if time is None:
            # A record with no time on this fold's axis has no position in the
            # fold. It is still reported, with the reason it cannot be placed,
            # rather than silently disappearing from the feature map.
            unplaceable.setdefault(key, quote)
            continue
        if _rank(quote, order) <= _rank_at(at, order):
            current = candidate_by_key.get(key)
            if current is None or _rank(quote, order) > _rank(current, order):
                candidate_by_key[key] = quote
        else:
            current = invalidated.get(key)
            if current is None or _rank(quote, order) < _rank(current, order):
                invalidated[key] = quote
    for key, quote in invalidated.items():
        if key not in candidate_by_key:
            candidate_by_key[key] = quote
    for key, quote in unplaceable.items():
        if key not in candidate_by_key:
            candidate_by_key[key] = quote

    # A gap or closure cannot be bypassed by looking further back. When the
    # latest state of a book at `at` is invalid, in that same fold, the book is
    # excluded even if an earlier quote was valid.
    latest_by_key = _latest_by_book_key(quotes, order)
    for key, latest in latest_by_key.items():
        if latest.valid:
            continue
        at_latest = _order_time(latest, order)
        if at_latest is None or at_latest > at:
            continue
        candidate = candidate_by_key.get(key)
        if (
            candidate is not None
            and candidate.valid
            and _rank(candidate, order) < _rank(latest, order)
        ):
            candidate_by_key[key] = latest

    states: dict[tuple[str, str], AsOfQuote] = {}
    for key, candidate in candidate_by_key.items():
        states[key] = _state_for(key, candidate, at, order=order, max_age_seconds=max_age_seconds)
    return states


def _rank_at(at: dt.datetime, order: str) -> tuple:
    """Sort key that admits a quote whose order time is exactly ``at``.

    The key terminator is the highest code point, so every real occurrence
    identity sorts below it and ``order_time == at`` stays inclusive.
    """
    return (0, at.timestamp(), _LAST, _LAST, _LAST)


def _state_for(
    key: tuple[str, str],
    candidate: Quote | None,
    at: dt.datetime,
    *,
    order: str,
    max_age_seconds: float | None,
    require_verified: bool = True,
) -> AsOfQuote:
    market = key[1]
    if candidate is None:
        return AsOfQuote(market, None, False, QuoteValidity.MISSING.value, None, None, None)
    if candidate.replay_order is not None and candidate.replay_order != order:
        # A quote stamped by the other fold is not usable evidence on this one.
        # Reading a source-ordered quote as if it were information-feasible is
        # exactly the leakage this exclusion prevents.
        return AsOfQuote(market, candidate, False, "replay_order_mismatch", None, None, None)
    usable = candidate.clock.usable_time
    if usable is None:
        return AsOfQuote(
            market,
            candidate,
            False,
            "usable_time_unknown" if order == ORDER_USABLE else "order_time_unknown",
            None,
            None,
            None,
        )
    if usable > at:
        return AsOfQuote(
            market,
            candidate,
            False,
            "not_yet_available",
            None,
            None,
            None,
        )
    verified = candidate.last_verified
    if verified is None:
        return AsOfQuote(
            market,
            candidate,
            True,
            None,
            "last_verified_unknown" if require_verified else None,
            usable,
            None,
        )
    age = (at - verified).total_seconds()
    reason: str | None = None
    if age < 0:
        reason = "last_verified_after_asof_time"
    elif max_age_seconds is not None and age > max_age_seconds:
        reason = "stale_last_verified"
    if not candidate.valid and reason is None:
        reason = candidate.reason
    return AsOfQuote(market, candidate, True, None, reason, usable, age)


def features_asof(
    quotes: Iterable[Quote],
    at: dt.datetime,
    *,
    max_age_seconds: float | None = None,
    order: str = ORDER_USABLE,
) -> dict[str, dict]:
    """Point-in-time feature state per market at ``at``.

    Keyed by the venue-qualified market key ``f"{venue}|{contract_id}"``, because
    a contract identifier is only unique inside its venue. Values carry the
    observed state and its provenance: ``bid``, ``ask``, ``midpoint``,
    ``spread``, ``depth``, ``valid``, ``reason``, ``max_input_available_time``
    and ``raw_hash``, plus ``venue``, ``contract_id``, ``bid_size``,
    ``ask_size``, ``source_time``, ``last_verified``, ``last_trade``,
    ``last_price_change``, ``age_seconds``, ``clock_quality``,
    ``timing_uncertainty_seconds``, ``replay_order`` and ``record_id``.

    ``reason`` is ``None`` only when the market is usable. ``at`` must be
    timezone-aware; a market whose latest admissible quote is invalid is
    reported invalid rather than falling back to an earlier valid book. Default
    ``order='usable'`` excludes source-replayed quotes and quotes with no known
    usable time.
    """
    moment = _aware(at, field_name="features_asof(at)")
    resolved = resolve_order(order)
    materialized = list(quotes)
    states = _asof_states(materialized, moment, order=resolved, max_age_seconds=max_age_seconds)
    out: dict[str, dict] = {}
    for key in sorted(states):
        state = states[key]
        candidate = state.candidate
        market = state.key
        if candidate is None:
            out[market] = {
                "venue": market.split("|", 1)[0],
                "contract_id": market.split("|", 1)[1],
                "bid": None,
                "ask": None,
                "bid_size": None,
                "ask_size": None,
                "midpoint": None,
                "spread": None,
                "depth": None,
                "valid": False,
                "reason": state.reason,
                "max_input_available_time": None,
                "raw_hash": None,
                "record_id": None,
                "source_time": None,
                "last_price_change": None,
                "last_verified": None,
                "last_trade": None,
                "age_seconds": None,
                "clock_quality": "unknown",
                "timing_uncertainty_seconds": None,
                "replay_order": resolved,
            }
            continue
        state_valid = state.valid
        out[market] = {
            "venue": candidate.venue,
            "contract_id": candidate.contract_id,
            "bid": candidate.bid if state_valid else None,
            "ask": candidate.ask if state_valid else None,
            "bid_size": candidate.bid_size if state_valid else None,
            "ask_size": candidate.ask_size if state_valid else None,
            "midpoint": candidate.midpoint if state_valid else None,
            "spread": candidate.spread if state_valid else None,
            "depth": candidate.depth if state_valid else None,
            "valid": state_valid,
            "reason": state.reason,
            "max_input_available_time": state.max_input_available_time,
            "raw_hash": candidate.provenance.raw_hash,
            "record_id": candidate.provenance.record_id,
            "source_time": candidate.source_time,
            "last_price_change": candidate.last_price_change,
            "last_verified": candidate.last_verified,
            "last_trade": candidate.last_trade,
            "age_seconds": state.age_seconds,
            "clock_quality": candidate.clock.availability.quality,
            "timing_uncertainty_seconds": candidate.clock.timing_uncertainty_seconds,
            "replay_order": candidate.replay_order or resolved,
        }
        if not state.admitted and state.reason is not None:
            out[market]["reason"] = state.reason
    return out


def _aware(value: object, *, field_name: str) -> dt.datetime:
    if not isinstance(value, dt.datetime):
        raise TypeError(f"{field_name}: expected datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{field_name}: naive datetime {value!r}; an explicit offset is required")
    return value.astimezone(UTC)


def available_labels(resolutions: Iterable[Resolution], cutoff: dt.datetime) -> list[Resolution]:
    """Resolutions whose outcome knowledge was available at ``cutoff``.

    ``known_at`` governs. A resolution with no ``known_at`` is unavailable: an
    unknown knowledge time cannot be placed before a cutoff. This holds
    regardless of venue settlement, so a contract that settled early while its
    payout became known later stays unavailable. ``cutoff=None`` is an error
    rather than an implicit "everything is available".

    Output is sorted by ``(known_at, contract_id, rule_hash)``.
    """
    moment = _aware(cutoff, field_name="available_labels(cutoff)")
    out: list[Resolution] = []
    for resolution in resolutions:
        if not isinstance(resolution, Resolution):
            raise TypeError(
                f"available_labels expects Resolution records, got {type(resolution).__name__}"
            )
        known_at = resolution.known_at
        if known_at is None or known_at > moment:
            continue
        out.append(resolution)
    out.sort(key=lambda item: (item.known_at, item.contract_id, item.rule_hash))
    return out


def _panel_row(**values: Any) -> dict[str, Any]:
    missing = set(PANEL_COLUMNS) - set(values)
    if missing:
        raise ValueError(f"panel row is missing columns: {sorted(missing)}")
    return {name: values[name] for name in PANEL_COLUMNS}


def _float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _raw_hashes(values: Sequence[Quote | None]) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for quote in values:
        if quote is None:
            continue
        digest = quote.provenance.raw_hash
        if digest not in seen:
            seen.add(digest)
            ordered.append(digest)
    return ",".join(ordered)


def _age_seconds(at: dt.datetime, quote: Quote | None) -> float | None:
    if quote is None or quote.last_verified is None:
        return None
    return (at - quote.last_verified).total_seconds()


def _contract_columns(contract: Contract | None) -> dict[str, Any]:
    if contract is None:
        return {
            "operator": None,
            "threshold": None,
            "rounding": None,
            "units": None,
            "orientation_sign": None,
        }
    return {
        "operator": str(contract.operator),
        "threshold": _float(contract.threshold),
        # An unpublished rounding rule stays null rather than rendering as the
        # string "None", which a grouped consumer would read as a value.
        "rounding": None if contract.rounding is None else str(contract.rounding),
        "units": contract.units,
        "orientation_sign": float(contract.orientation_sign),
    }


def build_event_panel(
    quotes: Iterable[Quote],
    releases: Iterable[Release],
    contracts: Iterable[Contract],
    *,
    order: str,
    horizons_seconds: Sequence[int] = (60, 300, 900, 1800, 3600),
    max_age_seconds: float = 120,
    cohorts: Mapping[tuple[str, str], str] | None = None,
    clusters: Mapping[str, str] | None = None,
    contamination_windows: Mapping[str, Iterable[tuple[dt.datetime, dt.datetime]]] | None = None,
    resolutions: Iterable[Resolution] | None = None,
    exclude_quotes_before: dt.datetime | None = None,
) -> pd.DataFrame:
    """Build the historical economic panel: one row per event, contract, horizon.

    ``order`` is required because the two folds answer different questions.
    ``order='source'`` produces the historical event study, where the
    observation time is the source time and responses are not information
    feasible. ``order='usable'`` produces the information-feasible view. The
    fold used is stamped in ``replay_order`` on every row.

    ``cohorts`` maps ``(event_id, contract_id)`` to ``'downstream'``, ``'direct'``
    or ``'control'``. Without it, a contract is included for the release sharing
    its ``event_id`` and labelled ``'direct'``.

    ``clusters`` maps ``event_id`` to a cluster identifier, defaulting to the
    event itself, so cross-contract dependence inside one release stays visible.

    ``contamination_windows`` maps ``event_id`` to ``(start, end)`` intervals
    that truncate the window; the key :data:`ASOF_ALL_EVENTS` (``'*'``) applies to
    every event. A window that overlaps the endpoint masks the row.

    ``resolutions`` fills ``label_available_time`` from each contract's known
    outcome. ``exclude_quotes_before`` drops quotes earlier than a wall-clock
    floor, which is what a moving live/historical cutoff needs: a backfill that
    only covers recent history must not be treated as cover for older events.

    A row is masked, never dropped, with ``exclusion_reason`` naming why. The
    first reason in this order wins: ``missing_baseline``, ``missing_endpoint``,
    ``baseline_not_before_event``, ``contract_not_open``, ``closed_in_window``,
    ``contaminated``, ``rule_unavailable``, ``ineligible_contract``,
    ``unknown_rule_semantics``, ``quote_invalid``, ``quote_stale``,
    ``side_missing``, ``availability_unknown``.

    ``unknown_rule_semantics`` marks a contract whose own rule facts the venue
    never published, so the record cannot enter the primary panel as though its
    payoff semantics were verified.
    """
    resolved = resolve_order(order)
    materialized_quotes: list[Quote] = []
    dropped_type = 0
    for entry in quotes:
        if _is_quote(entry):
            materialized_quotes.append(entry)
        else:
            dropped_type += 1
    if dropped_type:
        raise TypeError(
            "build_event_panel received "
            f"{dropped_type} entries that are not Quote records; a trade price is not a quote "
            "and cannot be used as one"
        )
    if exclude_quotes_before is not None:
        floor = _aware(exclude_quotes_before, field_name="build_event_panel(exclude_quotes_before)")
    else:
        floor = None
    if floor is not None:
        materialized_quotes = [
            quote
            for quote in materialized_quotes
            if _order_time(quote, resolved) is None or _order_time(quote, resolved) >= floor
        ]

    release_by_event: dict[str, Release] = {}
    for release in releases:
        if not isinstance(release, Release):
            raise TypeError(
                f"build_event_panel expects Release records, got {type(release).__name__}"
            )
        release_by_event.setdefault(release.event_id, release)

    contract_by_key: dict[str, Contract] = {}
    for contract in contracts:
        if not isinstance(contract, Contract):
            raise TypeError(
                f"build_event_panel expects Contract records, got {type(contract).__name__}"
            )
        contract_by_key.setdefault(contract.market_key, contract)

    resolution_by_key: dict[str, Resolution] = {}
    for resolution in resolutions or ():
        if not isinstance(resolution, Resolution):
            raise TypeError(
                f"build_event_panel expects Resolution records, got {type(resolution).__name__}"
            )
        resolution_by_key.setdefault(resolution.contract_id, resolution)

    pairs: list[tuple[str, str, str]] = []
    if cohorts is not None:
        for (event_id, contract_id), cohort in cohorts.items():
            if cohort not in ("downstream", "direct", "control"):
                raise ValueError(
                    f"unknown cohort {cohort!r} for {event_id!r}/{contract_id!r}; expected "
                    "'downstream', 'direct' or 'control'"
                )
            pairs.append((event_id, contract_id, cohort))
    else:
        for contract in sorted(contract_by_key.values(), key=lambda item: item.market_key):
            if contract.event_id in release_by_event:
                pairs.append((contract.event_id, contract.contract_id, "direct"))
    pairs.sort()

    rows: list[dict[str, Any]] = []
    for event_id, contract_id, cohort in pairs:
        release = release_by_event.get(event_id)
        if release is None:
            continue
        contract = _contract_for(contract_by_key, venue=None, contract_id=contract_id)
        event_time = _event_time(release, resolved)
        if event_time is None:
            continue
        contract_quotes = [
            quote
            for quote in materialized_quotes
            if quote.contract_id == contract_id
            and (contract is None or quote.venue == contract.venue)
        ]
        contract_quotes.sort(key=lambda quote: _rank(quote, resolved))
        for horizon in horizons_seconds:
            rows.append(
                _panel_pair_row(
                    release=release,
                    contract=contract,
                    contract_id=contract_id,
                    venue=(contract.venue if contract is not None else ""),
                    cohort=cohort,
                    horizon=int(horizon),
                    order=resolved,
                    max_age_seconds=float(max_age_seconds),
                    quotes=contract_quotes,
                    event_time=event_time,
                    cluster_id=(clusters or {}).get(event_id, event_id),
                    contamination=_contamination_for(contamination_windows, event_id, resolved),
                    resolution=resolution_by_key.get(contract_id),
                )
            )
    return _panel_frame(rows)


def _is_quote(value: object) -> bool:
    return isinstance(value, Quote)


def _contract_for(
    contracts: Mapping[str, Contract], *, venue: str | None, contract_id: str
) -> Contract | None:
    """The contract for a ``(venue, contract_id)`` pair, or the sole match."""
    if venue is not None:
        return contracts.get(market_key(venue, contract_id))
    matches = [
        contract for key, contract in contracts.items() if contract.contract_id == contract_id
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def _event_time(release: Release, order: str) -> dt.datetime | None:
    """The event time the panel uses, from the fold's own clock.

    A source-order panel anchors on the observed publication time; a
    usable-order panel anchors on the time the release was usable to this
    system. Using one fold's time with the other fold's quotes would compare
    incomparable things, so the choice follows ``order``.
    """
    return release.clock.source_time if order == ORDER_SOURCE else release.clock.usable_time


def _contamination_for(
    windows: Mapping[str, Iterable[tuple[dt.datetime, dt.datetime]]] | None,
    event_id: str,
    order: str,
) -> tuple[tuple[dt.datetime, dt.datetime], ...]:
    if not windows:
        return ()
    raw = list(windows.get(event_id, ())) + list(windows.get(ASOF_ALL_EVENTS, ()))
    out: list[tuple[dt.datetime, dt.datetime]] = []
    for entry in raw:
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            raise TypeError(f"contamination window for {event_id!r} must be a (start, end) pair")
        start = _aware(entry[0], field_name="contamination_window.start")
        end = _aware(entry[1], field_name="contamination_window.end")
        if start > end:
            raise ValueError(f"contamination window for {event_id!r} ends before it starts")
        out.append((start, end))
    return tuple(out)


def _panel_pair_row(
    *,
    release: Release,
    contract: Contract | None,
    contract_id: str,
    venue: str,
    cohort: str,
    horizon: int,
    order: str,
    max_age_seconds: float,
    quotes: Sequence[Quote],
    event_time: dt.datetime,
    cluster_id: str,
    contamination: Sequence[tuple[dt.datetime, dt.datetime]],
    resolution: Resolution | None,
) -> dict[str, Any]:
    endpoint_time = event_time + timedelta(seconds=horizon)
    baseline_quote = _baseline_quote(quotes, event_time, order=order)
    endpoint_quote = _latest_at_or_before(quotes, endpoint_time, order=order)

    label_available_time = None
    if resolution is not None and resolution.known_at is not None:
        label_available_time = resolution.known_at

    reasons: list[str] = []
    if contract is None:
        reasons.append("ineligible_contract")
    if contract is not None:
        unknown_facts = [
            name for name in Contract.UNKNOWN_WHEN_ABSENT if getattr(contract, name) is None
        ]
        if unknown_facts:
            # A contract whose own rule semantics were never published cannot be
            # pooled as though its payoff were known: the row is retained with
            # the reason, and the contract columns stay null beside it.
            reasons.append("unknown_rule_semantics")
    if (
        contract is not None
        and contract.rule_available_at is not None
        and contract.rule_available_at > event_time
    ):
        reasons.append("rule_unavailable")
    if not _open_during(contract, event_time, endpoint_time):
        reasons.append("contract_not_open")

    baseline_time = None
    if baseline_quote is None:
        reasons.append("missing_baseline")
    else:
        baseline_time = _order_time(baseline_quote, order)
        if baseline_time is None:
            # The quote exists but has no position on this fold's axis, which is
            # a different fact from a baseline that arrived too late.
            reasons.append(
                "availability_unknown" if order == ORDER_USABLE else "order_time_unknown"
            )
        elif baseline_time >= event_time:
            reasons.append("baseline_not_before_event")

    if endpoint_quote is None:
        reasons.append("missing_endpoint")
    if _closed_in_window(quotes, event_time, endpoint_time, order=order):
        reasons.append("closed_in_window")
    if any(start <= endpoint_time and end >= event_time for start, end in contamination):
        reasons.append("contaminated")

    baseline_state = _admit(
        baseline_quote, event_time, order=order, max_age_seconds=max_age_seconds
    )
    endpoint_state = _admit(
        endpoint_quote, endpoint_time, order=order, max_age_seconds=max_age_seconds
    )
    reasons.extend(baseline_state)
    reasons.extend(endpoint_state)

    valid = not reasons and baseline_quote is not None and endpoint_quote is not None
    exclusion_reason = reasons[0] if reasons else None

    baseline_mid = baseline_quote.midpoint if (valid and baseline_quote is not None) else None
    endpoint_mid = endpoint_quote.midpoint if (valid and endpoint_quote is not None) else None
    response = (
        None if baseline_mid is None or endpoint_mid is None else float(endpoint_mid - baseline_mid)
    )
    clock_quality = _worst_quality(baseline_quote, endpoint_quote)
    uncertainty = _max_uncertainty(baseline_quote, endpoint_quote)

    return _panel_row(
        event_id=release.event_id,
        cluster_id=cluster_id,
        family=release.family,
        contract_id=contract_id,
        venue=venue,
        cohort=cohort,
        event_time=event_time,
        horizon_seconds=horizon,
        baseline_time=baseline_time,
        endpoint_time=endpoint_time,
        baseline=_float(baseline_mid),
        endpoint=_float(endpoint_mid),
        response=response,
        spread_before=_float(baseline_quote.spread) if valid and baseline_quote else None,
        spread_after=_float(endpoint_quote.spread) if valid and endpoint_quote else None,
        depth_before=_float(baseline_quote.depth) if valid and baseline_quote else None,
        depth_after=_float(endpoint_quote.depth) if valid and endpoint_quote else None,
        baseline_age_seconds=(_age_seconds(event_time, baseline_quote) if valid else None),
        endpoint_age_seconds=(_age_seconds(endpoint_time, endpoint_quote) if valid else None),
        valid=valid,
        exclusion_reason=exclusion_reason,
        raw_hashes=_raw_hashes([baseline_quote, endpoint_quote]),
        replay_order=order,
        clock_quality=clock_quality,
        timing_uncertainty_seconds=uncertainty,
        label_available_time=label_available_time,
        training_cutoff=None,
        split=None,
        **_contract_columns(contract),
    )


def _has_order_time(quote: Quote, at: dt.datetime, *, order: str) -> bool:
    """Whether a quote has a position on this fold's axis at or before ``at``."""
    time = _order_time(quote, order)
    return time is not None and time <= at


def _admit(
    quote: Quote | None, at: dt.datetime, *, order: str, max_age_seconds: float
) -> list[str]:
    """Reasons this quote cannot anchor a panel column at ``at``.

    Freshness is measured against ``last_verified``. When that is unknown the
    quote fails closed: an unverified book is not evidence of a live market.
    """
    if quote is None:
        return []
    if not _has_order_time(quote, at, order=order):
        return ["availability_unknown" if order == ORDER_USABLE else "order_time_unknown"]
    if not quote.valid:
        return [quote.reason or QuoteValidity.MISSING.value]
    verified = quote.last_verified
    if verified is None:
        return ["last_verified_unknown"]
    reasons: list[str] = []
    if (at - verified).total_seconds() > max_age_seconds:
        reasons.append("quote_stale")
    if not quote.has_both_sides:
        reasons.append("side_missing")
    return reasons


def _baseline_quote(
    quotes: Sequence[Quote], event_time: dt.datetime, *, order: str
) -> Quote | None:
    """Latest quote strictly before the event.

    Only quotes at or after ``event_time`` are refused. When no quote precedes
    the event there is no baseline, and the row is masked rather than anchored on
    a post-event quote.
    """
    before = [quote for quote in quotes if (_order_time(quote, order) or _MIN_TIME) < event_time]
    if not before:
        return None
    return max(before, key=lambda quote: _rank(quote, order))


def _latest_at_or_before(quotes: Sequence[Quote], at: dt.datetime, *, order: str) -> Quote | None:
    candidates = [quote for quote in quotes if (_order_time(quote, order) or _MIN_TIME) <= at]
    if not candidates:
        return None
    return max(candidates, key=lambda quote: _rank(quote, order))


_MIN_TIME = dt.datetime.min.replace(tzinfo=UTC)

_LAST = chr(0x10FFFF)


def _closed_in_window(
    quotes: Sequence[Quote],
    event_time: dt.datetime,
    endpoint_time: dt.datetime,
    *,
    order: str,
) -> bool:
    """Whether a closure landed inside the window.

    Closure is fold-relative: a source-order fold sees a close at its source
    time, a usable-order fold at the time the close became usable. Reading one
    fold's closure on the other fold's axis would truncate a window the fold
    never truncated.
    """
    for quote in quotes:
        if quote.validity is not QuoteValidity.CLOSED:
            continue
        at = _order_time(quote, order)
        if at is None:
            continue
        if event_time <= at <= endpoint_time:
            return True
    return False


def _open_during(
    contract: Contract | None, event_time: dt.datetime, endpoint_time: dt.datetime
) -> bool:
    if contract is None:
        return True
    if contract.close_time is not None and event_time >= contract.close_time:
        return False
    if contract.close_time is not None and contract.close_time <= endpoint_time:
        return False
    return not (contract.open_time is not None and contract.open_time > event_time)


def _worst_quality(*quotes: Quote | None) -> str | None:
    """The least trustworthy clock quality across the supplied quotes.

    A quote whose availability interval is unknown still has a clock quality, and
    that quality is ``unknown``. Reporting ``None`` would read as "not measured",
    which is a different and more flattering claim than "measured as unknown".
    """
    present = [quote for quote in quotes if quote is not None]
    if not present:
        return None
    return min(
        (quote.clock.availability.quality for quote in present),
        key=lambda quality: _CLOCK_QUALITY_RANK.get(quality, -1),
    )


def _max_uncertainty(*quotes: Quote | None) -> float | None:
    widths = [
        width
        for width in (
            quote.clock.timing_uncertainty_seconds for quote in quotes if quote is not None
        )
        if width is not None
    ]
    return max(widths) if widths else None


def _panel_frame(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame({name: pd.Series([], dtype=object) for name in PANEL_COLUMNS})
    frame = pd.DataFrame(list(rows))
    string_columns = (
        "event_id",
        "cluster_id",
        "family",
        "contract_id",
        "venue",
        "cohort",
        "exclusion_reason",
        "raw_hashes",
        "replay_order",
        "clock_quality",
        "split",
        "operator",
        "rounding",
        "units",
    )
    time_columns = (
        "event_time",
        "baseline_time",
        "endpoint_time",
        "label_available_time",
        "training_cutoff",
    )
    float_columns = (
        "baseline",
        "endpoint",
        "response",
        "spread_before",
        "spread_after",
        "depth_before",
        "depth_after",
        "baseline_age_seconds",
        "endpoint_age_seconds",
        "timing_uncertainty_seconds",
        "threshold",
        "orientation_sign",
    )
    for name in string_columns:
        frame[name] = frame[name].astype("object")
    for name in time_columns:
        frame[name] = pd.to_datetime(frame[name], utc=True)
    for name in float_columns:
        frame[name] = pd.to_numeric(frame[name], errors="coerce")
    frame["horizon_seconds"] = pd.to_numeric(frame["horizon_seconds"]).astype("int64")
    frame["valid"] = frame["valid"].astype(bool)
    return frame


def forecast_frame(
    rows: Iterable[Mapping[str, Any]],
) -> pd.DataFrame:
    """Materialize forecast-table rows with the declared columns and dtypes.

    The declared columns come from
    ``market_propagation.storage.TABLE_SCHEMAS['forecast']``, so the neighbour
    predictors and the ``cluster_id`` / ``cohort`` / ``orientation_sign`` /
    ``exclusion_reason`` fields are materialized here alongside the label. A row
    carrying a column outside that declaration is refused rather than silently
    dropped, which is what keeps a truth-only quantity from entering a forecast
    table by accident.

    ``target`` is the future probability change the models predict, in absolute
    probability units, so it is materialized as a number. A missing label stays
    missing: it is never filled with zero, because zero is a real observed
    movement and defaulting to it would fabricate a flat outcome for a row whose
    endpoint was never quoted. A non-numeric label is an error at this boundary
    rather than a value coerced to text.

    Forecast rows admit only ``max_input_available_time``: a caller cannot pass
    a source time as a prediction input through this helper, because the column
    does not exist.
    """
    materialized = []
    for row in rows:
        unknown = set(row) - set(FORECAST_COLUMNS)
        if unknown:
            raise ValueError(f"forecast row carries undeclared columns: {sorted(unknown)}")
        materialized.append({name: row.get(name) for name in FORECAST_COLUMNS})
    if not materialized:
        return pd.DataFrame({name: pd.Series([], dtype=object) for name in FORECAST_COLUMNS})
    frame = pd.DataFrame(materialized)
    # Every dtype grouping below comes from the declared schema, so a new
    # predictor reaches this boundary with the right type and no second list to
    # keep in step.
    for name in _FORECAST_COLUMNS_BY_TYPE["string"]:
        frame[name] = frame[name].astype("object")
    for name in _FORECAST_COLUMNS_BY_TYPE["timestamp"]:
        frame[name] = pd.to_datetime(frame[name], utc=True)
    for name in _FORECAST_COLUMNS_BY_TYPE["float"]:
        if name == "target":
            continue
        frame[name] = pd.to_numeric(frame[name], errors="coerce")
    try:
        frame["target"] = pd.to_numeric(frame["target"], errors="raise")
    except (TypeError, ValueError) as error:
        raise ValueError(
            "forecast.target must be the numeric future probability change, not a label; "
            "a target-kind label cannot be read as a prediction outcome"
        ) from error
    for name in _FORECAST_COLUMNS_BY_TYPE["int"]:
        frame[name] = pd.to_numeric(frame[name]).astype("int64")
    for name in _FORECAST_COLUMNS_BY_TYPE["bool"]:
        frame[name] = frame[name].astype(bool)
    return frame
