"""Synthetic transaction tapes, so the study's real decision rule can be calibrated.

The calibration cannot run on the archive. No candidate contract there carries an
attested rule vintage, so the exposure graph admits no edge, the forecast panel has
no valid row, and there is nothing for the promotion rule to decide. It can run on a
process whose truth is known by construction, which is the one place a rule vintage
is knowable rather than argued.

This module turns the simulator's declared observation process into the three things
the production builders consume:

* ``HistoricalTrade`` prints, one per observed venue update, carrying the source-time
  clock and an event-axis price. Nothing here is aggregate: the builders derive their
  own anchors, endpoints and staleness from these rows.
* ``ContractPredicate`` contracts with a declared rule interval. In simulation the
  interval is true by construction, which is exactly why the graph's rule gate can be
  exercised here and cannot be exercised on the archive.
* Releases and a decision calendar, so an edge can run from one meeting to the next.

Three things this module deliberately does not do.

It does not shortcut the production path. The tapes go to
``historical_forecast.build_forecast_rows`` and ``neighbors.build_neighbor_graph``
unmodified, so a defect in the observation, feature or graph code shows up as a
calibration failure rather than being papered over by a parallel implementation that
agrees with itself.

It does not present synthetic rule evidence as real. Every predicate it emits names
``SYNTHETIC_RULE_METHOD`` as its verifier, and every tape carries
``interpretation_limits``. A calibrated result says what this declared process can
detect. It says nothing about any venue.

It does not carry hidden truth into the feature schema. Sensitivity, latent moves,
transmitted signal and the communication edge stay on ``SimulatedTapes.truth``, which
no builder reads.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pandas as pd

from .domain import Availability, Clock, HistoricalTrade, Provenance
from .neighbors import ContractPredicate
from .simulation import (
    _CONTROL_VENUE,
    _FAST_VENUE,
    _TARGET_VENUE,
    _event_time,
    simulate_quotes,
    simulate_scenario,
)

__all__ = [
    "EVENT_AXIS",
    "SYNTHETIC_RULE_METHOD",
    "SimulatedRelease",
    "SimulatedTapeError",
    "SimulatedTapes",
    "simulate_tapes",
    "simulated_release_shocks",
]

#: Named verifier on every synthetic rule interval. A reader who finds this string in
#: a predicate knows the interval was declared by the generator, not attested by a
#: source, and must not treat the row as archive evidence.
SYNTHETIC_RULE_METHOD = "synthetic_declared_rule_interval_for_calibration"

#: The payout axis every simulated print is projected onto. A print with an
#: ``event_price`` and no ``event_axis`` is rejected by ``HistoricalTrade``, and the
#: panel reads a price only when the axis is named, so both are always set together.
EVENT_AXIS = "yes_probability"

#: Meeting each venue's contracts are declared to be about, relative to the release
#: they are observed at.
#:
#: The simulator builds one release window containing the fast venue's private
#: component, and copies that component to the target venue after the declared lag
#: wherever an edge exists. It has no notion of a contract listed across several
#: meetings, so the exposure graph, which runs an edge from the previous meeting's
#: contract to this one's, finds no donor that trades inside the receiver's window.
#:
#: Declaring the fast venue's path about the earlier meeting and the target venue's
#: path about the later one gives the graph exactly that edge: a donor and a receiver
#: that both trade at one release, with the donor carrying the leading information.
#: This is an attribution of the generated rows. It is not a claim that the generator
#: models a meeting calendar, and the certificate says so.
MEETING_LEAD_BY_VENUE: Mapping[str, int] = {
    _FAST_VENUE: 0,
    _TARGET_VENUE: 2,
    _CONTROL_VENUE: 0,
}

#: Columns the tape builder needs from the simulator's quote panel. Declared here so a
#: simulator change that drops one fails loudly at the boundary instead of silently
#: producing an empty or half-populated tape.
_REQUIRED_QUOTE_COLUMNS: tuple[str, ...] = (
    "contract_id",
    "venue",
    "event_id",
    "family",
    "event_time",
    "observation_time",
    "source_time",
    "midpoint",
    "exclusion_reason",
)

#: Slack added to the tape's own release-to-meeting span when computing how wide a
#: contract's listing must be. The graph is built once at the first release's origin and
#: then serves every release, so a contract is only usable if it is listed from before
#: that origin through after the last measured window. A margin fixed in calendar days
#: silently refuses every meeting far enough ahead of the first release.
_LISTING_SLACK_DAYS = 30


class SimulatedTapeError(RuntimeError):
    """The simulator's output cannot be turned into a tape under the accepted contract."""


@dataclass(frozen=True, slots=True)
class SimulatedRelease:
    """One simulated release, in the shape the forecast builder reads."""

    event_id: str
    cluster_id: str
    family: str
    event_time: dt.datetime


@dataclass(frozen=True, slots=True)
class SimulatedTapes:
    """One scenario's tapes, contracts, releases and calendar.

    ``trades`` are the prints the builders read. ``truth`` is the generator's own
    mechanism, which no builder reads: it exists so a recovery check can ask whether
    the process really contained the declared edge, independently of whether the
    estimator found it.
    """

    scenario: str
    seed: int
    n_events: int
    venue: str
    trades: tuple[HistoricalTrade, ...]
    contracts: tuple[ContractPredicate, ...]
    releases: tuple[SimulatedRelease, ...]
    calendar: tuple[dt.date, ...]
    declared_receivers: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    truth: Mapping[str, Any] = field(default_factory=dict)
    counts: Mapping[str, Any] = field(default_factory=dict)
    flags: tuple[str, ...] = ()
    interpretation_limits: str = (
        "Every row is generated by market_propagation.simulation. A result computed "
        "from these tapes describes the stated synthetic process and is not evidence "
        "about any real venue."
    )

    def receivers(self) -> dict[str, tuple[str, ...]]:
        """The declared per-release receiver set: this venue's contracts, per event.

        Built from every quote row the generator declared for the release, before the
        halt filter, so a receiver that never printed still gets its masked row. The
        sample the decision rule sees is then the sample that was declared rather than
        whatever happened to trade.
        """
        return {key: tuple(names) for key, names in self.declared_receivers.items()}

    def digest(self) -> str:
        """Content identity over the prints, so a rerun can be compared to a stored run."""
        hasher = hashlib.sha256()
        for trade in self.trades:
            hasher.update(
                "\x1f".join(
                    (
                        trade.venue,
                        trade.contract_id,
                        trade.provenance.record_id,
                        trade.clock.source_time.isoformat() if trade.clock.source_time else "",
                        str(trade.event_price),
                    )
                ).encode("utf-8")
            )
            hasher.update(b"\x1e")
        return hasher.hexdigest()

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "seed": self.seed,
            "n_events": self.n_events,
            "venue": self.venue,
            "trades": len(self.trades),
            "contracts": len(self.contracts),
            "releases": len(self.releases),
            "calendar_dates": len(self.calendar),
            "counts": dict(self.counts),
            "flags": list(self.flags),
            "digest": self.digest(),
            "truth": dict(self.truth),
            "interpretation_limits": self.interpretation_limits,
        }


def _split_tape_id(contract_id: str) -> tuple[str, str, str, int] | None:
    """``(venue, family, role, meeting)`` from a tape contract id, or ``None``.

    One parser for the tape id format, so the meeting a contract is about is read the
    same way everywhere it is needed rather than re-derived by ad-hoc splitting.
    """
    parts = contract_id.rsplit("-", 3)
    if len(parts) != 4 or not parts[3].isdigit():
        return None
    return parts[0], parts[1], parts[2], int(parts[3])


def _release_index(event_id: str) -> int:
    tail = event_id.rsplit("-", 1)[-1]
    if not tail.isdigit():
        raise SimulatedTapeError(f"release id {event_id!r} does not end in an ordinal")
    return int(tail)


def _release_id(index: int) -> str:
    return f"rel-{index:04d}"


def _tape_id(row: Mapping[str, Any]) -> str:
    """The tape contract id for one quote row.

    The release ordinal comes from the row's own event, and the meeting the contract
    is declared to be about is that release shifted by its venue's lead. The role is
    carried through unchanged, so one role's fast and target contracts still share a
    predicate and the graph can pair them across decisions.
    """
    meeting = _release_index(str(row["event_id"])) + MEETING_LEAD_BY_VENUE.get(str(row["venue"]), 0)
    role = str(row["contract_id"]).rsplit("-", 2)[-2]
    return f"{row['venue']}-{row['family']}-{role}-{meeting:04d}"


def _require_columns(frame: pd.DataFrame) -> None:
    missing = [name for name in _REQUIRED_QUOTE_COLUMNS if name not in frame.columns]
    if missing:
        raise SimulatedTapeError(
            "the simulator's quote panel is missing required columns "
            f"{missing}; available columns are {sorted(frame.columns)}"
        )


def _rule_hash(rate_definition: str) -> str:
    return hashlib.sha256(f"simulated-rule\x1f{rate_definition}".encode()).hexdigest()


def _print_row(row: Mapping[str, Any], contract_id: str) -> tuple[HistoricalTrade, dt.datetime]:
    """One observed venue update, as one trade print on the event axis.

    The price is the update's own observed value. The clock is the simulator's source
    time, kept distinct from the receipt, which stays null because a generated print
    was never received from anywhere. Availability is explicitly unknown, so the
    print can never leak into a point-in-time feature through a receipt it does not
    have.
    """
    price = Decimal(str(float(row["midpoint"])))
    observation = row["observation_time"]
    source_time = row["source_time"]
    received = None if observation is None else pd.Timestamp(observation).to_pydatetime()
    released = None if source_time is None else pd.Timestamp(source_time).to_pydatetime()
    event_time = pd.Timestamp(row["event_time"]).to_pydatetime()
    record_id = f"{contract_id}@{released.isoformat() if released else 'unknown'}"
    return HistoricalTrade(
        venue=str(row["venue"]),
        contract_id=contract_id,
        price=price,
        raw_price_units="dollars",
        price_precision="float64_source_precision",
        size_quality="unavailable_in_cleaned_layer",
        clock=Clock(
            source_time=released,
            received_time=received,
            availability=Availability.unknown(basis="synthetic_source_time_axis"),
        ),
        provenance=Provenance(
            raw_hash=hashlib.sha256(f"{contract_id}\x1f{released}\x1f{price}".encode()).hexdigest(),
            record_id=record_id,
            source="market_propagation.simulation",
        ),
        trade_id=record_id,
        event_price=price,
        event_axis=EVENT_AXIS,
        flags=("synthetic_process",),
    ), event_time


def _predicate(
    row: Mapping[str, Any],
    *,
    contract_id: str,
    meeting: int,
    margin: dt.timedelta,
    window_floor: dt.datetime,
) -> ContractPredicate:
    """The declared payoff of one simulated contract, with its rule interval.

    Donor and receiver match on ``rate_definition``, which is built from the venue's
    role, so a contract's only predicate-identical predecessor is the same role one
    meeting earlier. Nothing else in the simulation is allowed to match, which keeps
    the graph's edge set equal to the mechanism the calibration intends to test.

    The decision date is the meeting the contract is declared to be about, taken from
    its own id, never the release it was observed at. Reading it off the observation
    would date every contract to the release, collapse the calendar, and leave the
    graph no predecessor to pair with.
    """
    family = str(row["family"])
    role = contract_id.rsplit("-", 2)[-2]
    decision = _event_time(meeting)
    rate_definition = f"simulated_{family}_{role}_level"
    return ContractPredicate(
        contract_id=contract_id,
        venue=str(row["venue"]),
        series=f"SIM-{family.upper()}",
        decision_date=decision.date(),
        rate_definition=rate_definition,
        threshold=Decimal("0.25"),
        inequality="above",
        yes_axis=EVENT_AXIS,
        orientation=1,
        open_time=decision - margin,
        close_time=decision + margin,
        rule_hash=_rule_hash(rate_definition),
        rule_in_force_from=window_floor,
        rule_in_force_to=None,
        rule_verified_by=SYNTHETIC_RULE_METHOD,
    )


def simulate_tapes(
    name: str,
    *,
    seed: int = 20260913,
    n_events: int = 8,
    pre_seconds: float | None = None,
    post_seconds: float | None = None,
    venue: str = "beta",
    family: str | None = None,
) -> SimulatedTapes:
    """Turn one scenario's declared observation process into a transaction tape.

    ``venue`` names which simulator venue supplies the declared receivers. The other
    venues' contracts are still emitted, because a donor has to exist for an edge to
    be admissible at all.

    ``family`` restricts the tape to one release family. The simulator alternates
    family by release, so the release immediately before a CPI release is an
    employment release, and the graph reads the preceding date off one declared
    calendar. Left unrestricted, every receiver's predecessor is the other family and
    the graph refuses on ``no_matched_threshold``, which measures the emitter's
    universe rather than the decision rule. Restricting to one family makes the
    calendar a sequence of same-series releases, which is what an edge actually runs
    across.

    The same ``(name, seed, n_events, family)`` tuple always produces the same prints,
    in the same order. Determinism is a property of the tape rather than of the
    caller, so a calibration certificate can name a seed and be replayed.
    """
    quotes = simulate_quotes(
        name,
        seed=seed,
        n_events=n_events,
        pre_seconds=pre_seconds,
        post_seconds=post_seconds,
    )
    _require_columns(quotes)

    # The declared receiver universe is read before the halt filter, so a contract
    # that never printed is still declared for its release and gets a masked row
    # rather than dropping out and tilting the sample toward what happened to trade.
    declared = quotes if family is None else quotes.loc[quotes["family"] == family]
    declared_receivers: dict[str, set[str]] = {}
    declared_meetings: set[int] = set()
    declared_releases: set[dt.datetime] = set()
    for row in declared.to_dict("records"):
        if str(row["venue"]) != venue:
            continue
        contract_id = _tape_id(row)
        declared_receivers.setdefault(str(row["event_id"]), set()).add(contract_id)
        parsed = _split_tape_id(contract_id)
        if parsed is not None:
            declared_meetings.add(parsed[3])
        declared_releases.add(pd.Timestamp(row["event_time"]).to_pydatetime())
    if not declared_receivers:
        raise SimulatedTapeError(
            f"scenario {name!r} declares no {venue!r} receiver"
            f"{'' if family is None else f' for family {family!r}'}"
        )
    # Every declared rule version is in force, and every contract listed, from before
    # the earliest release through after the last measured window, so neither the rule
    # gate nor liveness is what refuses an edge and the neighbour contrast is what the
    # calibration measures. The span is measured from the earliest release rather than
    # from the earliest meeting, because the graph is built at that release's origin and
    # a margin tied to the meeting span leaves the furthest meetings unlisted there.
    meeting_times = [_event_time(meeting) for meeting in declared_meetings]
    earliest_release = min(declared_releases)
    margin = (max(meeting_times) - earliest_release) + dt.timedelta(days=_LISTING_SLACK_DAYS)
    window_floor = earliest_release - margin

    observed = quotes.loc[quotes["exclusion_reason"].isna()]
    if family is not None:
        observed = observed.loc[observed["family"] == family]
    observed = observed.copy()
    if observed.empty:
        raise SimulatedTapeError(
            f"scenario {name!r} produced no admissible prints at n_events={n_events}"
            f"{'' if family is None else f' for family {family!r}'}; a tape with no "
            "prints cannot exercise the observation process"
        )
    # Ordering is the panel's own contract: by source time, then by occurrence
    # identity. Sorting here rather than trusting the simulator's row order keeps a
    # future simulator reordering from silently changing tie groups.
    observed = observed.sort_values(
        ["source_time", "contract_id", "observation_time"], kind="stable"
    )

    trades: list[HistoricalTrade] = []
    releases: dict[str, SimulatedRelease] = {}
    contracts: dict[str, ContractPredicate] = {}
    for row in observed.to_dict("records"):
        contract_id = _tape_id(row)
        meeting = _split_tape_id(contract_id)
        if meeting is None:  # pragma: no cover - _tape_id always builds this shape
            raise SimulatedTapeError(f"tape id {contract_id!r} is not a contract id")
        trade, event_time = _print_row(row, contract_id)
        trades.append(trade)
        event_id = str(row["event_id"])
        releases.setdefault(
            event_id,
            SimulatedRelease(
                event_id=event_id,
                cluster_id=str(row.get("cluster_id") or event_id),
                family=str(row["family"]),
                event_time=event_time,
            ),
        )
        contracts.setdefault(
            contract_id,
            _predicate(
                row,
                contract_id=contract_id,
                meeting=meeting[3],
                margin=margin,
                window_floor=window_floor,
            ),
        )

    ordered_releases = tuple(releases[key] for key in sorted(releases))
    if not ordered_releases:
        raise SimulatedTapeError(f"scenario {name!r} produced no releases")
    earliest = min(release.event_time for release in ordered_releases)
    calendar = tuple(sorted({contract.decision_date for contract in contracts.values()}))

    truth = {
        "scenario": name,
        "synthetic": True,
        "process": "synthetic_software_simulation",
        "generator": "market_propagation.simulation",
        "rule_evidence": "declared",
        "family": family,
        "rule_evidence_note": (
            "Rule intervals on these contracts are declared by the generator, not "
            "attested by any source. They exist so the graph's rule gate can be "
            "exercised; they are never archive evidence."
        ),
        "declared_receivers_venue": venue,
        "releases": len(ordered_releases),
        "print_window_start": earliest.isoformat(),
    }

    counts = {
        "quote_rows": len(quotes),
        "prints": len(trades),
        "excluded_quote_rows": int(len(quotes) - len(observed)),
        "contracts": len(contracts),
        "releases": len(ordered_releases),
        "calendar_dates": len(calendar),
        "venues": sorted({trade.venue for trade in trades}),
    }
    flags = {
        "synthetic_process",
        "rule_evidence_declared_not_attested",
        "prints_are_observations_not_aggregates",
        "meeting_attribution_is_declared_not_modelled",
    }
    return SimulatedTapes(
        scenario=name,
        seed=seed,
        n_events=n_events,
        venue=venue,
        trades=tuple(trades),
        contracts=tuple(contracts[key] for key in sorted(contracts)),
        releases=ordered_releases,
        calendar=calendar,
        declared_receivers={key: tuple(sorted(names)) for key, names in declared_receivers.items()},
        truth=truth,
        counts=counts,
        flags=tuple(sorted(flags)),
    )


def simulated_release_shocks(
    name: str,
    *,
    seed: int = 20260913,
    n_events: int = 8,
) -> dict[str, float]:
    """The generator's per-release common shock, in the estimator's news control role.

    This process publishes no release package, so there is no archived actual and no
    expectation to subtract. The simulator instead draws one shock per release and
    applies it to every contract as ``orientation * sensitivity[role] * shock``, so
    the shock is recoverable from the generator's own truth-only ``latent_move``
    column and its declared per-release sensitivity.

    That is the declared common-news control and nothing more. It is not the
    transmission mechanism under test: the private component and the communication
    edge stay on ``SimulatedTapes.truth`` and never reach the estimator, or the
    calibration would be measuring a control it had handed the answer to.
    """
    frame = simulate_scenario(name, seed=seed, n_events=n_events)
    ground_truth = frame.attrs["ground_truth"]
    sensitivity = ground_truth["per_event_sensitivity"]
    shocks: dict[str, float] = {}
    for event_id, group in frame.groupby("event_id", sort=True):
        per_role = sensitivity.get(str(event_id)) or {}
        for contract_id, orientation, latent in zip(
            group["contract_id"], group["orientation_sign"], group["latent_move"], strict=True
        ):
            orientation = float(orientation)
            latent = float(latent)
            if orientation == 0.0 or not math.isfinite(latent):
                continue
            role = str(contract_id).rsplit("-", 2)[-2]
            strength = per_role.get(role)
            if not strength:
                continue
            shocks[str(event_id)] = latent / (orientation * float(strength))
            break
    return shocks


def tape_digest(tapes: Iterable[SimulatedTapes]) -> str:
    """One identity over a sequence of tapes, for a certificate's input hash."""
    hasher = hashlib.sha256()
    for tape in tapes:
        hasher.update(f"{tape.scenario}:{tape.seed}:{tape.digest()}".encode())
        hasher.update(b"\x1e")
    return hasher.hexdigest()


def receivers_by_release(tapes: SimulatedTapes) -> dict[str, Sequence[str]]:
    """Convenience re-export so a caller does not re-derive the declared universe."""
    return tapes.receivers()
