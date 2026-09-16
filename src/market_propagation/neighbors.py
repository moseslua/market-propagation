"""Admissible-neighbour topology for the primary policy-rate exposure graph.

An edge links a contract on an earlier policy decision date to a contract on the
immediately following decision date, and only when the two state the same payoff
predicate. Three measured facts about this archive decide whether such a graph is
populated, and all of them are results rather than faults:

* The archive's ``open_time`` is not a listing instant. Measured on
  ``KXFEDDECISION-25JAN`` it is ``2024-12-18T15:00Z`` while ``created_time`` is
  ``2024-12-13T13:17:29Z``, and 2024-12-18 is the preceding meeting's date: the
  field reports when the *previous* meeting resolved. An earlier claim here that
  Kalshi lists one decision date at a time was read off that field and is
  withdrawn. The calendar is supplied independently, and a declared date that
  holds no contract is ``calendar_date_holds_no_contract`` rather than a reason
  to reach back a further meeting.
* No contract on this archive carries an attested rule vintage, so the receiver's
  own rule check refuses before any donor is sought. On the declared calendar 623
  edges are structurally admissible and withheld by that requirement alone.
* The same-expiry strikes that are simultaneously live are mutually exclusive
  outcomes of one meeting. They share a payoff rather than an economic exposure,
  and linking them would report mechanical dependence as propagation.

Every contract the builder is handed therefore gets exactly one
:class:`GraphDecision`: the single donor selected for it, or the reason no donor
is admissible. A receiver is never dropped silently, and the most correlated
strike is never substituted for the missing donor. The reason vocabulary names the
distinct faults rather than folding them into one: ``no_admissible_neighbor``
where the declared calendar holds no earlier decision date,
``calendar_date_holds_no_contract`` where it does and this graph holds no contract
at it, ``no_matched_threshold`` where contracts at that date exist but none states
the receiver's predicate, ``rule_vintage_unverified`` where a contract's rule
version is not stated to be in force across the measured window, and the
lifecycle reasons where a predicate-identical contract is not listed yet, already
closed, or not listed for the whole window.

Only :data:`RELATION_ECONOMIC_EXPOSURE` edges are built here.
:data:`RELATION_PAYOFF_IDENTITY` and :data:`RELATION_MECHANICAL_DEPENDENCE` name
the two relations the study keeps apart from it, and they are declared here rather
than folded in so a consumer cannot read one relation as another.

The graph is a derived topology over records the caller already holds, so it
points at them by contract id and carries no provenance record of its own: a
:class:`~market_propagation.domain.Provenance` addresses a stored payload, and
this module never reads one. The release-relative windows in
:mod:`market_propagation.trade_panel` are likewise not reused, because they size a
response measurement rather than an eligibility window; an edge here is admissible
according to the contract's own documented listing interval, and borrowing a
window meant for a different question would record a rule this graph does not
enforce.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .domain import Clock, parse_decimal, parse_utc_time

#: Version of the topology rules. It travels beside the digest rather than inside
#: it, so bumping the rules does not pretend that an unchanged graph changed.
GRAPH_VERSION = "neighbor_graph_v2"

#: Predicates are only comparable when every one of these agrees. The venue, the
#: contract id and the series name are deliberately absent: ticker resemblance is
#: not settlement semantics, so a contract is matched on the rule it states.
MATCH_FIELDS: tuple[str, ...] = (
    "rate_definition",
    "threshold",
    "inequality",
    "yes_axis",
    "orientation",
)

#: The three relations the study keeps distinct. An exposure edge is the only one
#: this module builds; the other two are declared so no consumer can read a
#: mechanical dependence or a payoff identity as the exposure an edge claims.
RELATION_ECONOMIC_EXPOSURE = "economic_exposure"
RELATION_PAYOFF_IDENTITY = "payoff_identity"
RELATION_MECHANICAL_DEPENDENCE = "mechanical_dependence"

#: The permitted edge relations.
RELATIONS: tuple[str, ...] = (
    RELATION_ECONOMIC_EXPOSURE,
    RELATION_PAYOFF_IDENTITY,
    RELATION_MECHANICAL_DEPENDENCE,
)

#: Reasons a receiver has no donor. They are distinct facts, not one failure with
#: different wording: the declared calendar holding no earlier decision date, that
#: date holding no contract in this graph, the contract's own rule version not
#: being verified as in force across the measured window, the earlier date holding
#: no contract with the receiver's predicate, the donor not being listed yet, and
#: the donor having closed are different states of the world.
REASON_NO_ADMISSIBLE_NEIGHBOR = "no_admissible_neighbor"
REASON_CALENDAR_DATE_HOLDS_NO_CONTRACT = "calendar_date_holds_no_contract"
REASON_RULE_VINTAGE_UNVERIFIED = "rule_vintage_unverified"
REASON_DONOR_NOT_OPEN = "donor_not_open_at_origin"
REASON_RECEIVER_NOT_OPEN = "receiver_not_open_at_origin"
REASON_RECEIVER_NOT_LIVE_THROUGH_WINDOW = "receiver_not_live_through_window"
REASON_RESOLVED_BEFORE_ORIGIN = "resolved_before_forecast_origin"
REASON_DONOR_NOT_LIVE_THROUGH_WINDOW = "donor_not_live_through_window"
REASON_THRESHOLD_UNMATCHED = "no_matched_threshold"

#: The reason vocabulary this module can write, in the precedence order it applies.
REASONS: tuple[str, ...] = (
    REASON_RECEIVER_NOT_OPEN,
    REASON_RECEIVER_NOT_LIVE_THROUGH_WINDOW,
    REASON_RESOLVED_BEFORE_ORIGIN,
    REASON_RULE_VINTAGE_UNVERIFIED,
    REASON_NO_ADMISSIBLE_NEIGHBOR,
    REASON_CALENDAR_DATE_HOLDS_NO_CONTRACT,
    REASON_THRESHOLD_UNMATCHED,
    REASON_DONOR_NOT_OPEN,
    REASON_DONOR_NOT_LIVE_THROUGH_WINDOW,
)

#: The permitted inequalities. ``None`` is a third state and not a synonym for
#: either: a contract that publishes no direction is matched only against another
#: contract that publishes none.
INEQUALITIES: tuple[str, ...] = ("above", "below")

#: Lifecycle states, kept private because a caller reads the public reasons. They
#: exist so one liveness comparison serves both the donor and the receiver paths
#: instead of two comparisons that could drift apart.
_NOT_LISTED = "not_listed"
_CLOSED = "closed"


def _text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty str, got {value!r}")
    return value


def _optional_text(value: object | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _text(value, field_name=field_name)


def _optional_instant(value: object | None, *, field_name: str) -> dt.datetime | None:
    if value is None:
        return None
    return parse_utc_time(value, field_name=field_name)


def _stamp(moment: dt.datetime | None) -> str:
    return "an unpublished instant" if moment is None else moment.isoformat()


def _interval(predicate: ContractPredicate) -> str:
    """A contract's rule in-force interval as text, with an open bound stated as open."""
    opening = _stamp(predicate.rule_in_force_from)
    closing = (
        "open" if predicate.rule_in_force_to is None else predicate.rule_in_force_to.isoformat()
    )
    return f"[{opening}, {closing})"


def _rendered(value: object) -> str:
    """One match field's value as the text an edge records.

    A null field renders as an empty string. The edge's job is to state which
    fields agreed, and an absent threshold that agrees with an absent threshold is
    a real agreement rather than a missing one, so it is recorded as one.
    """
    return "" if value is None else str(value)


@dataclass(frozen=True, slots=True)
class ContractPredicate:
    """The payoff predicate and lifecycle of one candidate contract.

    ``decision_date`` is the meeting the contract is about, so an edge runs across
    decision dates and never within one: two contracts on the same meeting are
    never each other's donor, and the strikes of one meeting are excluded by that
    construction rather than by a separate rule.

    ``open_time`` and ``close_time`` are the venue's documented listing window and
    are the whole of the liveness evidence this module has. A record that does not
    publish one of the bounds is not given one, and :meth:`is_live_at` then reads
    only the bound the venue did publish.

    ``rule_in_force_from`` and ``rule_in_force_to`` are the interval a verified rule
    version was live, with ``rule_verified_by`` naming the method that established
    it. They are filled from a
    :class:`~market_propagation.ingest.audit.RuleVersionEvidence` record rather than
    derived here: a contract's own ``open_time`` dates the market, not the rule text
    a later fetch returned. A contract that states a hash and no interval is
    unverified, which :meth:`rule_verified_over` reports rather than assuming.
    """

    contract_id: str
    venue: str
    series: str
    decision_date: dt.date
    rate_definition: str
    threshold: Decimal | None
    inequality: str | None
    yes_axis: str
    orientation: int
    open_time: dt.datetime | None
    close_time: dt.datetime | None
    rule_hash: str | None
    rule_in_force_from: dt.datetime | None = None
    rule_in_force_to: dt.datetime | None = None
    rule_verified_by: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "contract_id",
            "venue",
            "series",
            "rate_definition",
            "yes_axis",
        ):
            object.__setattr__(
                self, name, _text(getattr(self, name), field_name=f"ContractPredicate.{name}")
            )
        if not isinstance(self.decision_date, dt.date) or isinstance(
            self.decision_date, dt.datetime
        ):
            raise TypeError(
                "ContractPredicate.decision_date must be a date, got "
                f"{type(self.decision_date).__name__}"
            )
        if self.threshold is not None:
            object.__setattr__(
                self,
                "threshold",
                parse_decimal(self.threshold, field_name="ContractPredicate.threshold"),
            )
        if self.inequality is not None and self.inequality not in INEQUALITIES:
            raise ValueError(
                f"ContractPredicate.inequality must be one of {INEQUALITIES} or None, got "
                f"{self.inequality!r}"
            )
        if isinstance(self.orientation, bool) or self.orientation not in (-1, 1):
            raise ValueError(
                f"ContractPredicate.orientation must be +1 or -1, got {self.orientation!r}"
            )
        object.__setattr__(
            self, "open_time", _optional_instant(self.open_time, field_name="open_time")
        )
        object.__setattr__(
            self, "close_time", _optional_instant(self.close_time, field_name="close_time")
        )
        object.__setattr__(
            self, "rule_hash", _optional_text(self.rule_hash, field_name="rule_hash")
        )
        object.__setattr__(
            self,
            "rule_in_force_from",
            _optional_instant(self.rule_in_force_from, field_name="rule_in_force_from"),
        )
        object.__setattr__(
            self,
            "rule_in_force_to",
            _optional_instant(self.rule_in_force_to, field_name="rule_in_force_to"),
        )
        object.__setattr__(
            self,
            "rule_verified_by",
            _optional_text(self.rule_verified_by, field_name="rule_verified_by"),
        )
        if (
            self.rule_in_force_from is not None
            and self.rule_in_force_to is not None
            and self.rule_in_force_to <= self.rule_in_force_from
        ):
            raise ValueError(
                f"ContractPredicate rule interval {self.rule_in_force_from.isoformat()} to "
                f"{self.rule_in_force_to.isoformat()} is empty, and an empty interval certifies "
                "no window"
            )
        if (
            self.open_time is not None
            and self.close_time is not None
            and self.open_time > self.close_time
        ):
            raise ValueError(
                f"ContractPredicate.open_time {self.open_time} is after its close_time "
                f"{self.close_time}"
            )

    def match_key(self) -> tuple[object, ...]:
        """Values that must be identical for a match, in :data:`MATCH_FIELDS` order.

        The key is built from the field names rather than restated, so adding a
        match field cannot leave this comparison behind.
        """
        return tuple(getattr(self, name) for name in MATCH_FIELDS)

    def match_values(self) -> dict[str, object]:
        """Field values that must agree for a match, keyed by name."""
        return {name: getattr(self, name) for name in MATCH_FIELDS}

    def is_live_at(self, at: dt.datetime) -> bool:
        """Whether the contract is inside its documented listing window at ``at``.

        The comparison mirrors :meth:`market_propagation.domain.Contract.is_open_at`
        exactly. Two liveness rules in one repository would disagree silently, and
        the one a panel already reads is the one an edge has to agree with. A bound
        the venue never published is not a constraint the venue imposed, so an
        unpublished close leaves the contract live rather than imputing a closure.
        """
        return _lifecycle_failure(self, at) is None

    def rule_verified_over(self, start: dt.datetime, end: dt.datetime) -> bool:
        """Whether a verified rule version is stated to be in force across a window.

        A rule hash alone binds a verdict to exact text; it does not say when that
        text was live. The interval is the part that can certify a window, so a
        contract that states a hash and no interval is unverified rather than
        implicitly valid, and a contract whose interval opens after the window or
        closes inside it is not certified for that window. ``verified_by`` must name
        the method, because an unattributed interval is an assertion rather than
        evidence.
        """
        if self.rule_hash is None or self.rule_verified_by is None:
            return False
        if self.rule_in_force_from is None or start < self.rule_in_force_from:
            return False
        return self.rule_in_force_to is None or end < self.rule_in_force_to

    @property
    def lifecycle_clock(self) -> Clock:
        """The contract's listing instant as the domain's clock record.

        An archived listing has no receipt, so ``Clock.historical`` keeps the
        availability interval unknown instead of deriving a usable one from the
        venue's schedule. That is the honest state for a record nobody observed
        arriving, and reusing the domain record keeps a consumer from inventing a
        second time axis beside the one it already reads.
        """
        return Clock.historical(self.open_time)


def _lifecycle_failure(predicate: ContractPredicate, at: dt.datetime) -> str | None:
    if predicate.open_time is not None and at < predicate.open_time:
        return _NOT_LISTED
    if predicate.close_time is not None and at >= predicate.close_time:
        return _CLOSED
    return None


def _match_pairs(predicate: ContractPredicate) -> tuple[tuple[str, str], ...]:
    return tuple((name, _rendered(getattr(predicate, name))) for name in MATCH_FIELDS)


@dataclass(frozen=True, slots=True)
class NeighborEdge:
    """One admissible exposure edge, carried from its donor to its receiver.

    ``match_fields`` is the evidence for the edge rather than a restatement of it:
    it holds every field of :data:`MATCH_FIELDS` with the value both contracts
    stated, so a reader can see what agreed without reopening either contract.
    """

    donor_contract_id: str
    receiver_contract_id: str
    donor_decision_date: dt.date
    receiver_decision_date: dt.date
    match_fields: tuple[tuple[str, str], ...]
    relation: str
    rule_hash: str | None
    donor_rule_in_force_from: dt.datetime | None = None
    donor_rule_in_force_to: dt.datetime | None = None

    def __post_init__(self) -> None:
        for name in ("donor_contract_id", "receiver_contract_id"):
            object.__setattr__(
                self, name, _text(getattr(self, name), field_name=f"NeighborEdge.{name}")
            )
        if self.donor_contract_id == self.receiver_contract_id:
            raise ValueError("NeighborEdge must not join a contract to itself")
        if self.donor_decision_date >= self.receiver_decision_date:
            raise ValueError(
                "NeighborEdge.donor_decision_date "
                f"{self.donor_decision_date} must precede the receiver's "
                f"{self.receiver_decision_date}"
            )
        pairs = tuple(
            (_text(name, field_name="NeighborEdge.match_fields"), str(value))
            for name, value in self.match_fields
        )
        if tuple(name for name, _ in pairs) != MATCH_FIELDS:
            raise ValueError(
                "NeighborEdge.match_fields must record every field of MATCH_FIELDS in order, got "
                f"{[name for name, _ in pairs]}"
            )
        object.__setattr__(self, "match_fields", pairs)
        if self.relation not in RELATIONS:
            raise ValueError(
                f"NeighborEdge.relation must be one of {RELATIONS}, got {self.relation!r}"
            )
        object.__setattr__(
            self, "rule_hash", _optional_text(self.rule_hash, field_name="rule_hash")
        )
        object.__setattr__(
            self,
            "donor_rule_in_force_from",
            _optional_instant(self.donor_rule_in_force_from, field_name="donor_rule_in_force_from"),
        )
        object.__setattr__(
            self,
            "donor_rule_in_force_to",
            _optional_instant(self.donor_rule_in_force_to, field_name="donor_rule_in_force_to"),
        )

    def as_dict(self) -> dict[str, Any]:
        """The edge as JSON-representable data, with dates on their ISO form."""
        return {
            "donor_contract_id": self.donor_contract_id,
            "receiver_contract_id": self.receiver_contract_id,
            "donor_decision_date": self.donor_decision_date.isoformat(),
            "receiver_decision_date": self.receiver_decision_date.isoformat(),
            "match_fields": [[name, value] for name, value in self.match_fields],
            "relation": self.relation,
            "rule_hash": self.rule_hash,
            "donor_rule_in_force_from": (
                self.donor_rule_in_force_from.isoformat()
                if self.donor_rule_in_force_from is not None
                else None
            ),
            "donor_rule_in_force_to": (
                self.donor_rule_in_force_to.isoformat()
                if self.donor_rule_in_force_to is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class GraphDecision:
    """What the graph decided for one receiver, whether or not a donor was found.

    ``donor_contract_id`` and ``reason`` are the two outcomes of one decision, so
    exactly one of them is set. A selected donor with a null reason is as much a
    decision as a blocked receiver with a reason, and recording both keeps a
    consumer from inferring the second from the absence of an edge.
    """

    receiver_contract_id: str
    donor_contract_id: str | None
    reason: str | None
    detail: str
    donor_candidate_count: int = 0

    def __post_init__(self) -> None:
        _text(self.receiver_contract_id, field_name="GraphDecision.receiver_contract_id")
        object.__setattr__(
            self,
            "donor_contract_id",
            _optional_text(self.donor_contract_id, field_name="GraphDecision.donor_contract_id"),
        )
        if self.reason is not None and self.reason not in REASONS:
            raise ValueError(
                f"GraphDecision.reason must be one of {REASONS} or None, got {self.reason!r}"
            )
        if (self.donor_contract_id is None) == (self.reason is None):
            raise ValueError(
                "GraphDecision must state either the donor it selected or the reason it selected "
                f"none, got donor {self.donor_contract_id!r} with reason {self.reason!r}"
            )
        _text(self.detail, field_name="GraphDecision.detail")
        if isinstance(self.donor_candidate_count, bool) or self.donor_candidate_count < 0:
            raise ValueError(
                "GraphDecision.donor_candidate_count must be a non-negative int, got "
                f"{self.donor_candidate_count!r}"
            )

    @property
    def blocked(self) -> bool:
        """Whether the decision refused to name a donor."""
        return self.reason is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "receiver_contract_id": self.receiver_contract_id,
            "donor_contract_id": self.donor_contract_id,
            "reason": self.reason,
            "detail": self.detail,
            "blocked": self.blocked,
            "donor_candidate_count": self.donor_candidate_count,
        }


@dataclass(frozen=True, slots=True)
class NeighborGraph:
    """One built graph: its admissible edges and the decision behind every receiver.

    ``decisions`` covers ``generated_from`` exactly once per contract. A graph with
    no edges is a valid graph, and an empty one is still asked for a decision per
    receiver it was handed, so a caller reading only the edges never mistakes an
    unbuilt graph for a fully blocked one.
    """

    version: str
    edges: tuple[NeighborEdge, ...]
    decisions: tuple[GraphDecision, ...]
    generated_from: tuple[str, ...]
    calendar: tuple[dt.date, ...] = ()

    def __post_init__(self) -> None:
        _text(self.version, field_name="NeighborGraph.version")
        dates: list[dt.date] = []
        for position, date in enumerate(self.calendar):
            if not isinstance(date, dt.date) or isinstance(date, dt.datetime):
                raise TypeError(
                    f"NeighborGraph.calendar entry {position} must be a date, got "
                    f"{type(date).__name__}"
                )
            dates.append(date)
        if len(set(dates)) != len(dates):
            raise ValueError("NeighborGraph.calendar must name each decision date once")
        object.__setattr__(self, "calendar", tuple(sorted(dates)))
        ids = tuple(sorted(self.generated_from))
        if len(set(ids)) != len(ids):
            raise ValueError("NeighborGraph.generated_from must name each contract once")
        object.__setattr__(self, "generated_from", ids)
        edges: list[NeighborEdge] = []
        for edge in self.edges:
            if not isinstance(edge, NeighborEdge):
                raise TypeError(
                    f"NeighborGraph.edges must hold NeighborEdge, got {type(edge).__name__}"
                )
            edges.append(edge)
        edges.sort(
            key=lambda edge: (
                edge.receiver_decision_date,
                edge.receiver_contract_id,
                edge.donor_contract_id,
            )
        )
        receivers = [edge.receiver_contract_id for edge in edges]
        if len(set(receivers)) != len(receivers):
            raise ValueError(
                "NeighborGraph holds more than one donor for a receiver; the study uses the single "
                "admissible donor, so a second edge is a selection rule that has not been declared"
            )
        object.__setattr__(self, "edges", tuple(edges))
        decisions: list[GraphDecision] = []
        for decision in self.decisions:
            if not isinstance(decision, GraphDecision):
                raise TypeError(
                    "NeighborGraph.decisions must hold GraphDecision, got "
                    f"{type(decision).__name__}"
                )
            decisions.append(decision)
        decisions.sort(key=lambda decision: decision.receiver_contract_id)
        decided = [decision.receiver_contract_id for decision in decisions]
        if len(set(decided)) != len(decided):
            raise ValueError("NeighborGraph must decide each receiver once")
        if set(decided) != set(ids):
            raise ValueError(
                "NeighborGraph.decisions must cover generated_from exactly: undecided "
                f"{sorted(set(ids) - set(decided))}, undeclared {sorted(set(decided) - set(ids))}"
            )
        named = {edge.donor_contract_id for edge in edges} | set(receivers)
        if not named <= set(ids):
            raise ValueError(
                "NeighborGraph edges name contracts outside generated_from: "
                f"{sorted(named - set(ids))}"
            )
        declared = set(self.calendar)
        outside = sorted(
            date
            for edge in edges
            for date in (edge.donor_decision_date, edge.receiver_decision_date)
            if date not in declared
        )
        if outside:
            raise ValueError(
                f"NeighborGraph edges name decision dates outside the declared calendar: {outside}"
            )
        object.__setattr__(self, "decisions", tuple(decisions))

    def donors_for(self, receiver_contract_id: str) -> tuple[str, ...]:
        """The donor contract ids admissible for one receiver, empty when none is."""
        return tuple(
            edge.donor_contract_id
            for edge in self.edges
            if edge.receiver_contract_id == receiver_contract_id
        )

    def decision_for(self, receiver_contract_id: str) -> GraphDecision:
        """The decision recorded for one receiver."""
        for decision in self.decisions:
            if decision.receiver_contract_id == receiver_contract_id:
                return decision
        raise KeyError(f"no decision was recorded for receiver {receiver_contract_id!r}")

    @property
    def blocked_count(self) -> int:
        """How many receivers the graph refused a donor for."""
        return sum(1 for decision in self.decisions if decision.blocked)

    def as_dict(self) -> dict[str, Any]:
        """The graph as JSON-representable data, with its digest beside the edges."""
        reasons: dict[str, int] = {}
        for decision in self.decisions:
            if decision.reason is not None:
                reasons[decision.reason] = reasons.get(decision.reason, 0) + 1
        return {
            "version": self.version,
            "digest": graph_digest(self),
            "calendar": [date.isoformat() for date in self.calendar],
            "generated_from": list(self.generated_from),
            "edges": [edge.as_dict() for edge in self.edges],
            "decisions": [decision.as_dict() for decision in self.decisions],
            "counts": {
                "edges": len(self.edges),
                "decisions": len(self.decisions),
                "blocked": self.blocked_count,
                "reasons": dict(sorted(reasons.items())),
            },
        }


def _lifecycle_decision(
    receiver: ContractPredicate, *, origin: dt.datetime, failure: str
) -> GraphDecision:
    if failure == _NOT_LISTED:
        return GraphDecision(
            receiver_contract_id=receiver.contract_id,
            donor_contract_id=None,
            reason=REASON_RECEIVER_NOT_OPEN,
            detail=(
                f"receiver {receiver.contract_id} is not listed at the forecast origin "
                f"{_stamp(origin)}: its documented listing instant is {_stamp(receiver.open_time)}"
            ),
        )
    return GraphDecision(
        receiver_contract_id=receiver.contract_id,
        donor_contract_id=None,
        reason=REASON_RESOLVED_BEFORE_ORIGIN,
        detail=(
            f"receiver {receiver.contract_id} stopped trading at {_stamp(receiver.close_time)}, "
            f"before the forecast origin {_stamp(origin)}, so it cannot receive a signal"
        ),
    )


def _decide(
    receiver: ContractPredicate,
    *,
    origin: dt.datetime,
    window_end: dt.datetime,
    calendar: tuple[dt.date, ...],
    by_date: Mapping[dt.date, tuple[ContractPredicate, ...]],
) -> GraphDecision:
    """The decision for one receiver, or the reason there is none to make.

    The checks run in a fixed order, so one receiver has one reason rather than a
    set: the receiver has to be listed at the origin and still listed at the end of
    the measured window, its own rule version has to be verified across the window,
    the declared calendar has to hold a preceding decision date and a contract at
    it, the donor has to state the receiver's predicate, and the donor has to be
    listed across the window under a verified rule version too.
    """
    failure = _lifecycle_failure(receiver, origin)
    if failure is not None:
        return _lifecycle_decision(receiver, origin=origin, failure=failure)
    late = _lifecycle_failure(receiver, window_end)
    if late is not None:
        return GraphDecision(
            receiver_contract_id=receiver.contract_id,
            donor_contract_id=None,
            reason=REASON_RECEIVER_NOT_LIVE_THROUGH_WINDOW,
            detail=(
                f"receiver {receiver.contract_id} is {late} at the end of the measured window "
                f"{_stamp(window_end)}, so it cannot carry the window's increment however it "
                "traded inside the window"
            ),
        )
    if not receiver.rule_verified_over(origin, window_end):
        return GraphDecision(
            receiver_contract_id=receiver.contract_id,
            donor_contract_id=None,
            reason=REASON_RULE_VINTAGE_UNVERIFIED,
            detail=(
                f"no verified rule version is stated to be in force for receiver "
                f"{receiver.contract_id} across {_stamp(origin)} to {_stamp(window_end)}: the "
                f"rule hash {receiver.rule_hash!r} and the interval "
                f"{_interval(receiver)} are not enough, because a hash binds the text and only "
                "the interval can certify the window"
            ),
        )
    earlier = [date for date in calendar if date < receiver.decision_date]
    if not earlier:
        siblings = len(by_date.get(receiver.decision_date, ())) - 1
        return GraphDecision(
            receiver_contract_id=receiver.contract_id,
            donor_contract_id=None,
            reason=REASON_NO_ADMISSIBLE_NEIGHBOR,
            detail=(
                f"the declared decision calendar holds no date before "
                f"{receiver.decision_date.isoformat()}, so no donor can exist for "
                f"{receiver.contract_id}; {siblings} same-expiry sibling contract(s) are listed "
                "with it and are not substituted, because mutually exclusive outcomes of one "
                "meeting share a payoff rather than the economic exposure an edge is defined by"
            ),
        )
    donor_date = max(earlier)
    candidates = by_date.get(donor_date)
    if not candidates:
        return GraphDecision(
            receiver_contract_id=receiver.contract_id,
            donor_contract_id=None,
            reason=REASON_CALENDAR_DATE_HOLDS_NO_CONTRACT,
            detail=(
                f"the declared decision calendar names {donor_date.isoformat()} as the decision "
                f"date immediately before {receiver.decision_date.isoformat()}, and this graph "
                f"holds no contract for it, so there is nothing to match {receiver.contract_id} "
                "against; the predecessor is never taken from an earlier date, because that edge "
                "would span two meetings"
            ),
        )
    identical = tuple(
        candidate for candidate in candidates if candidate.match_key() == receiver.match_key()
    )
    if not identical:
        differing = sorted(
            {
                name
                for candidate in candidates
                for name in MATCH_FIELDS
                if getattr(candidate, name) != getattr(receiver, name)
            }
        )
        rejected = ", ".join(candidate.contract_id for candidate in candidates)
        return GraphDecision(
            receiver_contract_id=receiver.contract_id,
            donor_contract_id=None,
            reason=REASON_THRESHOLD_UNMATCHED,
            detail=(
                f"{len(candidates)} contract(s) at the preceding decision date "
                f"{donor_date.isoformat()} exist and none states the payout predicate of "
                f"{receiver.contract_id}: the match field(s) {differing} differ, so the "
                f"candidate(s) {rejected} are rejected rather than substituted; a near-match is "
                "not a match, and the nearest strike is the most correlated contract rather than "
                "an admissible donor"
            ),
        )
    eligible = tuple(
        candidate
        for candidate in identical
        if _lifecycle_failure(candidate, origin) is None
        and _lifecycle_failure(candidate, window_end) is None
    )
    if not eligible:
        closed = tuple(
            candidate for candidate in identical if _lifecycle_failure(candidate, origin) == _CLOSED
        )
        if closed:
            # A dated closure outranks a listing that has not happened yet, matching
            # the panel's own precedence: which instant a market stopped trading is
            # a published fact, while a future listing is a schedule.
            listed = ", ".join(
                f"{candidate.contract_id} closed {_stamp(candidate.close_time)}"
                for candidate in closed
            )
            return GraphDecision(
                receiver_contract_id=receiver.contract_id,
                donor_contract_id=None,
                reason=REASON_RESOLVED_BEFORE_ORIGIN,
                detail=(
                    f"every predicate-identical contract at the preceding decision date "
                    f"{donor_date.isoformat()} stopped trading before the forecast origin "
                    f"{_stamp(origin)}: {listed}"
                ),
            )
        if not _lifecycle_failure(identical[0], origin):
            # Listed at the origin but not through the window: the donor's own
            # increment runs past its close, so the lagged read is not measurable.
            return GraphDecision(
                receiver_contract_id=receiver.contract_id,
                donor_contract_id=None,
                reason=REASON_DONOR_NOT_LIVE_THROUGH_WINDOW,
                detail=(
                    f"the predicate-identical contract(s) at the preceding decision date "
                    f"{donor_date.isoformat()} are listed at the forecast origin {_stamp(origin)} "
                    f"but not at the end of the measured window {_stamp(window_end)}: "
                    + ", ".join(
                        f"{candidate.contract_id} closed {_stamp(candidate.close_time)}"
                        for candidate in identical
                    )
                ),
            )
        scheduled = ", ".join(
            f"{candidate.contract_id} lists {_stamp(candidate.open_time)}"
            for candidate in identical
        )
        return GraphDecision(
            receiver_contract_id=receiver.contract_id,
            donor_contract_id=None,
            reason=REASON_DONOR_NOT_OPEN,
            detail=(
                f"every predicate-identical contract at the preceding decision date "
                f"{donor_date.isoformat()} is not listed yet at the forecast origin "
                f"{_stamp(origin)}: {scheduled}"
            ),
        )
    verified = tuple(
        candidate for candidate in eligible if candidate.rule_verified_over(origin, window_end)
    )
    if not verified:
        return GraphDecision(
            receiver_contract_id=receiver.contract_id,
            donor_contract_id=None,
            reason=REASON_RULE_VINTAGE_UNVERIFIED,
            detail=(
                "every predicate-identical contract at the preceding decision date "
                f"{donor_date.isoformat()} is open across the measured window and none states a "
                "verified rule version in force across it: "
                + ", ".join(
                    f"{candidate.contract_id} rule hash {candidate.rule_hash!r} interval "
                    f"{_interval(candidate)}"
                    for candidate in eligible
                )
            ),
        )
    donor = verified[0]
    return GraphDecision(
        receiver_contract_id=receiver.contract_id,
        donor_contract_id=donor.contract_id,
        reason=None,
        detail=(
            f"donor {donor.contract_id} is predicate-identical to {receiver.contract_id}, open "
            f"from {_stamp(origin)} to {_stamp(window_end)} under rule version "
            f"{donor.rule_hash!r} verified by {donor.rule_verified_by!r}, and it is the immediate "
            f"predecessor decision date {donor_date.isoformat()}; it was selected from "
            f"{len(verified)} eligible candidate(s) by lowest contract id"
        ),
        donor_candidate_count=len(verified),
    )


def build_neighbor_graph(
    contracts: Iterable[ContractPredicate],
    *,
    at: dt.datetime,
    calendar: Iterable[dt.date],
    window_end: dt.datetime | None = None,
    graph_version: str = GRAPH_VERSION,
) -> NeighborGraph:
    """Build the exposure graph admissible at the forecast origin ``at``.

    ``calendar`` is the declared decision calendar: the meeting dates the exposure
    graph is built over, supplied independently of the contracts. It is required
    because a calendar derived from the supplied contracts cannot establish that a
    preceding date is missing rather than absent from the set: with a derived
    calendar a predecessor whose contracts were never handed in looks like no
    predecessor at all, and the edge silently spans two meetings. The predecessor is
    therefore the latest declared date below the receiver's, and a declared date the
    graph holds no contract for is ``calendar_date_holds_no_contract`` rather than a
    reason to fall back to an earlier date.

    ``window_end`` closes the measured window an edge has to hold across. Both the
    donor and the receiver must be listed at ``at`` and still listed at
    ``window_end``, and both must carry a verified rule version in force across the
    whole interval; a contract listed at the origin that closes inside the window
    cannot carry the window's increment, however it traded inside it.

    Every contract is a receiver. A contract that is old enough to be someone
    else's donor is still exposed to its own preceding decision date, and the
    panel needs that receiver's reason as much as it needs the younger one's.
    """
    origin = parse_utc_time(at, field_name="build_neighbor_graph(at)")
    end = (
        origin
        if window_end is None
        else parse_utc_time(window_end, field_name="build_neighbor_graph(window_end)")
    )
    if end < origin:
        raise ValueError(
            f"build_neighbor_graph window_end {end.isoformat()} is before the forecast origin "
            f"{origin.isoformat()}; a window that ends before it starts measures nothing"
        )
    version = _text(graph_version, field_name="graph_version")
    declared: list[dt.date] = []
    for position, date in enumerate(calendar):
        if not isinstance(date, dt.date) or isinstance(date, dt.datetime):
            raise TypeError(
                f"build_neighbor_graph calendar entry {position} must be a date, got "
                f"{type(date).__name__}"
            )
        declared.append(date)
    if len(set(declared)) != len(declared):
        raise ValueError("build_neighbor_graph calendar must name each decision date once")
    decision_calendar = tuple(sorted(declared))
    by_id: dict[str, ContractPredicate] = {}
    for position, predicate in enumerate(contracts):
        if not isinstance(predicate, ContractPredicate):
            raise TypeError(
                f"build_neighbor_graph contract {position} must be a ContractPredicate, got "
                f"{type(predicate).__name__}"
            )
        if predicate.contract_id in by_id:
            raise ValueError(
                f"contract id {predicate.contract_id!r} appears twice; a duplicate identity "
                "cannot be resolved by order, because order is not a documented rule"
            )
        by_id[predicate.contract_id] = predicate

    unexpected = sorted({p.decision_date for p in by_id.values()} - set(decision_calendar))
    if unexpected:
        raise ValueError(
            "contract(s) name decision date(s) outside the declared calendar "
            f"{[date.isoformat() for date in unexpected]}; the calendar is the exposure axis and "
            "a date it does not declare cannot be one"
        )

    grouped: dict[dt.date, list[ContractPredicate]] = {}
    for predicate in by_id.values():
        grouped.setdefault(predicate.decision_date, []).append(predicate)
    by_date = {
        date: tuple(sorted(group, key=lambda candidate: candidate.contract_id))
        for date, group in grouped.items()
    }

    decisions: list[GraphDecision] = []
    edges: list[NeighborEdge] = []
    for contract_id in sorted(by_id):
        receiver = by_id[contract_id]
        decision = _decide(
            receiver,
            origin=origin,
            window_end=end,
            calendar=decision_calendar,
            by_date=by_date,
        )
        decisions.append(decision)
        if decision.donor_contract_id is None:
            continue
        donor = by_id[decision.donor_contract_id]
        edges.append(
            NeighborEdge(
                donor_contract_id=donor.contract_id,
                receiver_contract_id=receiver.contract_id,
                donor_decision_date=donor.decision_date,
                receiver_decision_date=receiver.decision_date,
                match_fields=_match_pairs(donor),
                relation=RELATION_ECONOMIC_EXPOSURE,
                rule_hash=donor.rule_hash,
                donor_rule_in_force_from=donor.rule_in_force_from,
                donor_rule_in_force_to=donor.rule_in_force_to,
            )
        )
    return NeighborGraph(
        version=version,
        edges=tuple(edges),
        decisions=tuple(decisions),
        generated_from=tuple(sorted(by_id)),
        calendar=decision_calendar,
    )


def graph_digest(graph: NeighborGraph) -> str:
    """Stable sha256 over the canonically sorted JSON of the graph's edges.

    The edges are serialized one per line and sorted as text before hashing, so two
    runs over the same contracts produce the same digest whatever order the
    contracts and edges were built in, and a change to any edge changes it.

    The decisions are deliberately outside the digest. A decision records why a
    receiver has no donor as well as which donor it has, and two runs over the same
    edges that explain their blocked receivers at different levels of detail are the
    same topology; hashing the prose too would make the digest sensitive to wording
    rather than to the graph. A caller wanting to identify a run's evidence reads
    :meth:`NeighborGraph.as_dict`, which carries the decisions beside the digest.
    """
    if not isinstance(graph, NeighborGraph):
        raise TypeError(f"graph_digest expects a NeighborGraph, got {type(graph).__name__}")
    canonical = sorted(
        json.dumps(edge.as_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        for edge in graph.edges
    )
    payload = "[" + ",".join(canonical) + "]"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
