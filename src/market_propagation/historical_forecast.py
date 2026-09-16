"""Source-time forecast rows for the neighbour comparison.

The trade panel measures one contract's response to a release. The propagation
question needs a different row: a recipient's *forward* increment, the recipient's
own history at the forecast origin, and a lagged return from one matched donor.
This module builds that row and nothing else.

Three decisions are load-bearing.

**The neighbour signal stops before the target starts.** The donor's return is
measured from the release to ``tau - L`` and the recipient's target from ``tau``
onward, with ``L`` a declared guard. A donor window that reached into the target
window would let the same prints inform both sides of the comparison, and the
donor would predict the recipient partly because it *contains* the recipient's own
future.

**A missing observation is null, never zero, and a real zero stays a zero.** A
recipient that printed the same price twice produced a genuine observed ``0.0``,
which is evidence. A recipient that did not print at all produced nothing, which
is a different fact. Collapsing the second into the first would manufacture
observations for exactly the illiquid contracts the study is already thinnest on.

**Ties are grouped before features are built.** Archive rows share the venue's own
timestamp precision, so several prints can carry one instant. Choosing between
them by row order would invent a chronological order the record does not have, so
a shared timestamp becomes one tie group whose value is the unweighted mean of its
event-axis prices.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from . import storage
from .domain import HistoricalTrade

CLOCK_BASIS_SOURCE = "source"

REASON_NO_RECIPIENT_ANCHOR = "no_recipient_anchor"
REASON_ANCHOR_BEYOND_CAP = "recipient_anchor_beyond_cap"
REASON_NO_FORWARD_TARGET = "no_forward_target_trade"
REASON_TARGET_BEYOND_CAP = "forward_target_beyond_cap"
REASON_NO_DONOR_ANCHOR = "no_donor_anchor"
REASON_NO_DONOR_SIGNAL = "no_donor_signal"
REASON_NO_ADMISSIBLE_NEIGHBOR = "no_admissible_neighbor"
REASON_NEIGHBOR_GRAPH_NOT_SUPPLIED = "neighbor_graph_not_supplied"

#: Reason precedence. The first applicable reason is the row's
#: ``exclusion_reason``. Recipient-side reasons come first because they decide
#: whether the row measured anything at all; a donor-side reason says the row
#: cannot enter the network rung while leaving its own observation intact.
REASON_PRECEDENCE: tuple[str, ...] = (
    REASON_NO_RECIPIENT_ANCHOR,
    REASON_ANCHOR_BEYOND_CAP,
    REASON_NO_FORWARD_TARGET,
    REASON_TARGET_BEYOND_CAP,
    REASON_NEIGHBOR_GRAPH_NOT_SUPPLIED,
    REASON_NO_ADMISSIBLE_NEIGHBOR,
    REASON_NO_DONOR_ANCHOR,
    REASON_NO_DONOR_SIGNAL,
)

#: Reasons that decide whether the recipient observed anything. A row carrying
#: only donor-side reasons is still a valid recipient observation, which is what
#: lets the news model use it while the network comparison restricts to the rows
#: that additionally carry a neighbour signal.
RECIPIENT_REASONS: tuple[str, ...] = (
    REASON_NO_RECIPIENT_ANCHOR,
    REASON_ANCHOR_BEYOND_CAP,
    REASON_NO_FORWARD_TARGET,
    REASON_TARGET_BEYOND_CAP,
)

DEFAULT_CONFIG_PATH = "configs/external_history_v1.yaml"

_HORIZON_END = 3600
_PRE_WINDOW = 1800


@dataclass(frozen=True, slots=True)
class ForecastSettings:
    """The study's declared clock, as the execution plan fixes it.

    ``forecast_origin_seconds`` is ``tau``, ``lag_guard_seconds`` is ``L`` and
    ``future_horizon_seconds`` is ``H``. The defaults are the v2 choices the plan
    makes explicit: a five-minute origin, a one-minute donor guard, and a
    five-minute forward target, which together measure the five-to-ten-minute
    increment rather than the first five minutes' absorption.
    """

    forecast_origin_seconds: int = 300
    lag_guard_seconds: int = 60
    future_horizon_seconds: int = 300
    anchor_max_age_seconds: int = 120
    target_max_age_seconds: int = 120
    donor_max_age_seconds: int = 120

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> ForecastSettings:
        response = config.get("response", {}) if isinstance(config, Mapping) else {}
        return cls(
            anchor_max_age_seconds=int(response.get("baseline_max_age_seconds", 120)),
            target_max_age_seconds=int(response.get("endpoint_max_age_seconds", 120)),
            donor_max_age_seconds=int(response.get("endpoint_max_age_seconds", 120)),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "forecast_origin_seconds": self.forecast_origin_seconds,
            "lag_guard_seconds": self.lag_guard_seconds,
            "future_horizon_seconds": self.future_horizon_seconds,
            "anchor_max_age_seconds": self.anchor_max_age_seconds,
            "target_max_age_seconds": self.target_max_age_seconds,
            "donor_max_age_seconds": self.donor_max_age_seconds,
            "clock_basis": CLOCK_BASIS_SOURCE,
        }


def load_forecast_settings(config_path: str | None = DEFAULT_CONFIG_PATH) -> ForecastSettings:
    """Read the age caps from the pipeline configuration, keeping the clock fixed."""
    if config_path is None:
        return ForecastSettings()
    from pathlib import Path

    import yaml

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"forecast settings configuration not found: {path}")
    return ForecastSettings.from_config(yaml.safe_load(path.read_text(encoding="utf-8")))


@dataclass(frozen=True, slots=True)
class ForecastPanel:
    """One built forecast panel: rows, the clock they were built under, and counts."""

    rows: tuple[dict[str, Any], ...]
    settings: ForecastSettings
    counts: Mapping[str, Any] = field(default_factory=dict)
    flags: tuple[str, ...] = ()

    def write(self, path: str | Any, *, coverage_epoch: str = "historical_forecast") -> Any:
        return storage.write_parquet(
            [dict(row) for row in self.rows],
            path,
            table="historical_forecast",
            coverage_epoch=coverage_epoch,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "rows": [dict(row) for row in self.rows],
            "settings": self.settings.as_dict(),
            "counts": dict(self.counts),
            "flags": list(self.flags),
        }


def _source_time(print: HistoricalTrade) -> dt.datetime | None:
    return print.clock.source_time


def _axis_price(print: HistoricalTrade) -> Decimal | None:
    return print.event_price if print.event_axis is not None else None


def _rank(print: HistoricalTrade) -> tuple[dt.datetime, str]:
    """Order prints by recorded time then occurrence id, never by input position."""
    return (_source_time(print), print.provenance.record_id)  # type: ignore[arg-type]


def _ordered(prints: Iterable[HistoricalTrade]) -> list[HistoricalTrade]:
    dated = [print for print in prints if _source_time(print) is not None]
    return sorted(dated, key=_rank)


def _tail_group(prints: Sequence[HistoricalTrade]) -> list[HistoricalTrade]:
    """Prints sharing the finest recorded timestamp of the last print."""
    if not prints:
        return []
    last = _source_time(prints[-1])
    return [print for print in prints if _source_time(print) == last]


def _group_value(group: Sequence[HistoricalTrade]) -> float | None:
    """Unweighted mean event-axis price of a tie group, or null when any is unknown."""
    prices = [_axis_price(print) for print in group]
    if not prices or any(price is None for price in prices):
        return None
    return float(sum(prices, Decimal(0)) / Decimal(len(prices)))


def _occurrences(group: Sequence[HistoricalTrade]) -> list[str]:
    return sorted(print.provenance.record_id for print in group)


def _last_at_or_before(prints: Sequence[HistoricalTrade], instant: dt.datetime) -> list:
    pool = [print for print in prints if _source_time(print) <= instant]  # type: ignore[operator]
    return _tail_group(pool)


def _last_strictly_before(prints: Sequence[HistoricalTrade], instant: dt.datetime) -> list:
    pool = [print for print in prints if _source_time(print) < instant]  # type: ignore[operator]
    return _tail_group(pool)


def _blank_row() -> dict[str, Any]:
    """A row with every declared column present, so no key is silently omitted."""
    row: dict[str, Any] = dict.fromkeys(storage.HISTORICAL_FORECAST_COLUMNS, None)
    row["clock_basis"] = CLOCK_BASIS_SOURCE
    return row


def _declared_receivers(
    receivers: Mapping[str, Iterable[str]], events: Sequence[Any]
) -> dict[str, tuple[str, ...]]:
    """Normalise the declared per-release receiver set and refuse a partial one.

    A release the declaration does not cover would fall back to the activity
    universe for that release alone, which is the preselection the declaration
    exists to remove, so a missing release is an error rather than a default.
    """
    if not isinstance(receivers, Mapping):
        raise TypeError(f"receivers must be a mapping, got {type(receivers).__name__}")
    declared: dict[str, tuple[str, ...]] = {}
    for event in events:
        event_id = str(event.event_id)
        if event_id not in receivers:
            raise ValueError(
                f"receivers declares no set for release {event_id!r}; an undeclared release has "
                "no receiver universe, and falling back to the contracts that traded would "
                "select it from post-release activity"
            )
        names: list[str] = []
        for name in receivers[event_id]:
            if not isinstance(name, str) or not name.strip():
                raise ValueError(
                    f"receivers[{event_id!r}] must hold non-empty contract id strings, got {name!r}"
                )
            names.append(name)
        declared[event_id] = tuple(sorted(set(names)))
    return declared


def build_forecast_rows(
    trades: Iterable[HistoricalTrade],
    events: Iterable[Any],
    graph: Any | None,
    *,
    receivers: Mapping[str, Iterable[str]] | None = None,
    settings: ForecastSettings | None = None,
    venue: str = "kalshi",
) -> ForecastPanel:
    """Build one forecast row per release and receiver contract.

    ``events`` supplies ``event_id``, ``cluster_id``, ``family`` and
    ``event_time``. ``graph`` supplies, per receiver, the matched donor id or a
    decision naming why there is none. A ``graph`` of ``None`` is a declared policy
    that the neighbour evidence was not supplied, which every row reports as
    ``neighbor_graph_not_supplied``: it is not the same fact as a graph that was
    built and found no admissible donor, and the two are never merged.

    ``receivers`` is the declared per-release receiver set, chosen before the
    release. Supplying it makes the declaration the row universe: a declared
    receiver whose only prints fall outside the window still gets its masked row, so
    a quiet contract is a missing observation rather than an absent row. Omitting it
    falls back to the contracts that traded inside the window, which the counts and
    flags label as an activity-selected universe.
    """
    settings = settings or ForecastSettings()
    if graph is not None and not callable(getattr(graph, "donors_for", None)):
        raise TypeError(
            "graph must expose donors_for(receiver_contract_id); a mapping or an ad-hoc object "
            "cannot answer which donors are admissible, and reading it as an absent neighbour "
            "would report a missing input as a measured absence"
        )
    specs = list(events)
    declared = _declared_receivers(receivers, specs) if receivers is not None else None
    ordered = _ordered(trades)
    by_contract: dict[str, list[HistoricalTrade]] = {}
    for print in ordered:
        by_contract.setdefault(print.contract_id, []).append(print)

    rows: list[dict[str, Any]] = []
    for event in specs:
        release = event.event_time
        tau = release + dt.timedelta(seconds=settings.forecast_origin_seconds)
        donor_cutoff = tau - dt.timedelta(seconds=settings.lag_guard_seconds)
        target_end = tau + dt.timedelta(seconds=settings.future_horizon_seconds)
        # The floor reaches back past the prior baseline's own staleness cap, so a
        # prior print that exists but is stale is observed as stale rather than
        # reported as absent.
        window_floor = min(
            release - dt.timedelta(seconds=_PRE_WINDOW),
            tau
            - dt.timedelta(
                seconds=settings.future_horizon_seconds + settings.target_max_age_seconds
            ),
        )
        receivers_here = declared[str(event.event_id)] if declared is not None else None
        for contract_id in receivers_here if receivers_here is not None else sorted(by_contract):
            prints = by_contract.get(contract_id, [])
            window = [
                print
                for print in prints
                if window_floor <= _source_time(print)  # type: ignore[operator]
                and _source_time(print) <= target_end  # type: ignore[operator]
            ]
            if not window and receivers_here is None:
                continue
            donor_id: str | None = None
            if graph is not None:
                donors = graph.donors_for(contract_id)
                donor_id = donors[0] if donors else None
            rows.append(
                _forecast_row(
                    event=event,
                    contract_id=contract_id,
                    donor_id=donor_id,
                    graph_supplied=graph is not None,
                    window=window,
                    donor_prints=by_contract.get(donor_id or "", []),
                    release=release,
                    tau=tau,
                    donor_cutoff=donor_cutoff,
                    target_end=target_end,
                    settings=settings,
                    venue=venue,
                )
            )
    counts = _counts(rows, settings, declared=declared)
    flags = {"source_clock_alignment_is_retrospective"}
    flags.add(
        "receiver_universe_from_declared_set"
        if declared is not None
        else "receiver_universe_from_window_activity"
    )
    if graph is None:
        flags.add("neighbor_graph_not_supplied")
    if not rows:
        flags.add("no_forecast_rows_built")
    return ForecastPanel(
        rows=tuple(rows), settings=settings, counts=counts, flags=tuple(sorted(flags))
    )


def _forecast_row(
    *,
    event: Any,
    contract_id: str,
    donor_id: str | None,
    graph_supplied: bool,
    window: Sequence[HistoricalTrade],
    donor_prints: Sequence[HistoricalTrade],
    release: dt.datetime,
    tau: dt.datetime,
    donor_cutoff: dt.datetime,
    target_end: dt.datetime,
    settings: ForecastSettings,
    venue: str,
) -> dict[str, Any]:
    reasons: list[str] = []
    row = _blank_row()
    row.update(
        {
            "event_id": str(event.event_id),
            "cluster_id": str(getattr(event, "cluster_id", event.event_id)),
            "family": str(event.family),
            "venue": venue,
            "receiver_contract_id": contract_id,
            "donor_contract_id": donor_id,
            "release_time": release,
            "forecast_origin": tau,
            "donor_cutoff_time": donor_cutoff,
            "label_source_time": target_end,
        }
    )

    anchor_group = _last_at_or_before(window, tau)
    anchor_time = _source_time(anchor_group[0]) if anchor_group else None
    if not anchor_group:
        reasons.append(REASON_NO_RECIPIENT_ANCHOR)
    elif (tau - anchor_time).total_seconds() > settings.anchor_max_age_seconds:  # type: ignore[operator]
        reasons.append(REASON_ANCHOR_BEYOND_CAP)
    anchor_value = _group_value(anchor_group)
    row.update(
        {
            "recipient_anchor_time": anchor_time,
            "recipient_anchor": anchor_value,
            "recipient_anchor_age_seconds": (
                None if anchor_time is None else (tau - anchor_time).total_seconds()
            ),
            "recipient_anchor_tie_group_size": len(anchor_group) or None,
            "recipient_anchor_occurrences": _occurrences(anchor_group),
        }
    )

    # The recipient's own pre-origin increment, over a window of the same declared
    # length as the target and ending at the origin. It is the ladder's own-lag
    # feature: the target is the increment from tau forward, and this is the
    # increment of one horizon immediately before tau, so the own rung's
    # autoregressive term is the recipient's own past rather than another
    # contract's future.
    prior_origin = tau - (target_end - tau)
    prior_group = _last_at_or_before(window, prior_origin)
    prior_time = _source_time(prior_group[0]) if prior_group else None
    prior_value = _group_value(prior_group)
    row.update(
        {
            "recipient_prior_baseline_time": prior_time,
            "recipient_prior_baseline": prior_value,
            "recipient_prior_baseline_age_seconds": (
                None if prior_time is None else (prior_origin - prior_time).total_seconds()
            ),
            "recipient_prior_baseline_tie_group_size": len(prior_group) or None,
            "recipient_prior_baseline_occurrences": _occurrences(prior_group),
            "own_lag": (
                None if prior_value is None or anchor_value is None else anchor_value - prior_value
            ),
        }
    )

    forward = [print for print in window if tau < _source_time(print) <= target_end]  # type: ignore[operator]
    target_group = _tail_group(forward)
    target_time = _source_time(target_group[0]) if target_group else None
    if not target_group:
        # The anchor is never carried forward to manufacture a zero. A recipient
        # that did not trade after the origin has no forward increment.
        reasons.append(REASON_NO_FORWARD_TARGET)
    else:
        age = (target_end - target_time).total_seconds()  # type: ignore[operator]
        if age > settings.target_max_age_seconds:
            reasons.append(REASON_TARGET_BEYOND_CAP)
    target_value = _group_value(target_group)
    row.update(
        {
            "recipient_target_time": target_time,
            "recipient_target": target_value,
            "recipient_target_tie_group_size": len(target_group) or None,
            "recipient_target_occurrences": _occurrences(target_group),
        }
    )

    donor_signal: list = []
    if donor_id is None:
        # A graph that was never supplied is a missing input; a graph that was built
        # and found no admissible donor is a measured absence. Only the second is a
        # result of the neighbour comparison.
        reasons.append(
            REASON_NO_ADMISSIBLE_NEIGHBOR if graph_supplied else REASON_NEIGHBOR_GRAPH_NOT_SUPPLIED
        )
    else:
        donor_baseline = _last_strictly_before(donor_prints, release)
        candidate = _last_at_or_before(donor_prints, donor_cutoff)
        candidate_time = _source_time(candidate[0]) if candidate else None
        # The donor's return is measured from the release, so a print the donor
        # made before the release is the baseline again and would report a
        # spurious zero. The plan requires a post-release donor observation, and
        # its absence is unavailable rather than zero.
        post_release = candidate_time is not None and candidate_time >= release
        stale = candidate_time is not None and (
            (donor_cutoff - candidate_time).total_seconds() > settings.donor_max_age_seconds
        )
        donor_signal = candidate if (post_release and not stale) else []
        baseline_value = _group_value(donor_baseline)
        signal_time = _source_time(donor_signal[0]) if donor_signal else None
        signal_value = _group_value(donor_signal)
        if not donor_baseline:
            reasons.append(REASON_NO_DONOR_ANCHOR)
        if not donor_signal:
            reasons.append(REASON_NO_DONOR_SIGNAL)
        row.update(
            {
                "donor_baseline_time": (
                    _source_time(donor_baseline[0]) if donor_baseline else None
                ),
                "donor_baseline": baseline_value,
                "donor_signal_time": signal_time,
                "donor_signal": signal_value,
                "neighbor_lag": (
                    None
                    if baseline_value is None or signal_value is None
                    else signal_value - baseline_value
                ),
                "donor_signal_occurrences": _occurrences(donor_signal),
            }
        )

    observed = (
        anchor_value is not None
        and target_value is not None
        and not (set(reasons) & set(RECIPIENT_REASONS))
    )
    if anchor_value is not None and target_value is not None:
        row["target"] = target_value - anchor_value
    row["valid"] = bool(observed)
    first = next((name for name in REASON_PRECEDENCE if name in reasons), None)
    row["exclusion_reason"] = first
    # The input cutoff is the latest instant any feature read, so a caller can
    # purge training rows whose label window overlaps a later row's features.
    reads = [
        value
        for value in (anchor_time, _source_time(donor_signal[0]) if donor_signal else None)
        if value is not None
    ]
    row["max_input_source_time"] = max(reads) if reads else None
    return row


def _counts(
    rows: Sequence[Mapping[str, Any]],
    settings: ForecastSettings,
    *,
    declared: Mapping[str, tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    reasons: dict[str, int] = {}
    for row in rows:
        reason = row.get("exclusion_reason")
        if reason:
            reasons[str(reason)] = reasons.get(str(reason), 0) + 1
    return {
        "rows": len(rows),
        "valid_rows": sum(1 for row in rows if row.get("valid") is True),
        "rows_with_target": sum(1 for row in rows if row.get("target") is not None),
        "rows_with_neighbor_lag": sum(1 for row in rows if row.get("neighbor_lag") is not None),
        "exclusion_reason_counts": dict(sorted(reasons.items())),
        "distinct_receivers": len({str(row["receiver_contract_id"]) for row in rows}),
        "distinct_events": len({str(row["event_id"]) for row in rows}),
        "receiver_universe": (
            "declared_per_release_set" if declared is not None else "window_activity"
        ),
        "declared_receiver_slots": (
            None if declared is None else sum(len(names) for names in declared.values())
        ),
        "clock_basis": CLOCK_BASIS_SOURCE,
        "offsets_seconds": settings.as_dict(),
    }
