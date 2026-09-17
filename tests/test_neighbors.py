"""Acceptance tests for the primary policy-rate exposure neighbour graph.

The graph's job is to refuse the substitutions that would quietly turn a missing
neighbour into a propagation edge. Each test here defends one of them:

* A receiver's donor must state the identical payout predicate. A near-match at
  the preceding decision date is ``no_matched_threshold`` with no edge, because the
  nearest strike is the most correlated contract and therefore the one substitution
  the design forbids.
* The donor's and the receiver's own listing windows are read from the venue's own
  instants. A donor that has not listed yet and a contract that already stopped
  trading are different facts and get different reasons, and neither is answered by
  the same-expiry strike listed beside the receiver.
* The measured real-world pattern is asserted as the expected outcome rather than
  tolerated as a fault. In the archived listing calendar the predecessor of a
  decision date is already resolved at the next date's release, so a
  predicate-identical pair is ``resolved_before_forecast_origin`` rather than an
  edge, and five mutually exclusive strikes of one meeting with no
  later-decision contract produce zero edges and five ``no_admissible_neighbor``
  decisions.

Two listing calendars are fixtured, because the archive only exhibits one of them.
The archive opens a single decision date at a time and closes it at its meeting, so
consecutive decision dates are never live together and no matched pair is
admissible. The graph still has to express the populated case, so the window set
where two decision dates overlap is stated openly here rather than imitated from
the measured one.

The remaining tests cover the immediate-predecessor rule across a three-date
calendar, digest stability and sensitivity, the empty graph that must round-trip
through ``as_dict`` rather than raise, and the decision-record invariant that every
receiver is decided exactly once.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from decimal import Decimal

import pytest

from market_propagation.domain import UTC
from market_propagation.neighbors import (
    GRAPH_VERSION,
    MATCH_FIELDS,
    ORIGIN_ARCHIVED_AND_LIVE,
    ORIGIN_ARCHIVED_ONLY,
    ORIGIN_LIVE_ONLY,
    REASON_CALENDAR_DATE_HOLDS_NO_CONTRACT,
    REASON_DONOR_NOT_LIVE_THROUGH_WINDOW,
    REASON_DONOR_NOT_OPEN,
    REASON_NO_ADMISSIBLE_NEIGHBOR,
    REASON_RECEIVER_NOT_LIVE_THROUGH_WINDOW,
    REASON_RECEIVER_NOT_OPEN,
    REASON_RESOLVED_BEFORE_ORIGIN,
    REASON_RULE_VINTAGE_UNVERIFIED,
    REASON_THRESHOLD_UNMATCHED,
    RELATION_ECONOMIC_EXPOSURE,
    ContractPredicate,
    NeighborGraph,
    build_neighbor_graph,
    graph_digest,
    observation_origin,
)

JAN = dt.date(2025, 1, 29)
MAR = dt.date(2025, 3, 19)
MAY = dt.date(2025, 5, 7)

#: The declared decision calendar the graph is built over. It is supplied
#: independently of the contracts, because a calendar derived from the contracts
#: cannot tell a missing predecessor from one that was never handed in, and the
#: graph would then join a receiver to a donor two meetings back.
CALENDAR = (JAN, MAR, MAY)


def graph_over_calendar(
    contracts: list[ContractPredicate],
    *,
    at: dt.datetime,
    window_end: dt.datetime | None = None,
) -> NeighborGraph:
    """Build a graph over :data:`CALENDAR`, which is the declared axis of exposure."""
    return build_neighbor_graph(contracts, at=at, calendar=CALENDAR, window_end=window_end)


#: The forecast origin the populated cases are built at: a release before the
#: January meeting, when a January contract and a March contract are both listed.
ORIGIN = dt.datetime(2025, 1, 10, 13, 30, tzinfo=UTC)

#: The measured March release instant, used for the cases the archive actually
#: presents.
MARCH_RELEASE = dt.datetime(2025, 3, 12, 12, 30, tzinfo=UTC)

RATE = "upper_bound_federal_funds_target_rate"
DECISION = "KXFEDDECISION"

#: The rule vintage the fixtures state as verified. It opens before every fixtured
#: window and is left open-ended, which is the shape a live rule interval takes.
IN_FORCE_FROM = dt.datetime(2024, 12, 1, tzinfo=UTC)
VERIFIED_BY = "fixture_rule_text_read_from_the_venue_record"

#: Windows in which two decision dates are live together, so an edge is admissible.
#: Each contract closes at its own meeting. This is the shape the graph must be able
#: to express, and it is the default here because the populated case is the one the
#: tests have to construct.
OVERLAPPING: dict[dt.date, tuple[dt.datetime, dt.datetime]] = {
    JAN: (
        dt.datetime(2024, 12, 15, 15, 0, tzinfo=UTC),
        dt.datetime(2025, 1, 29, 19, 0, tzinfo=UTC),
    ),
    MAR: (
        dt.datetime(2025, 1, 2, 15, 0, tzinfo=UTC),
        dt.datetime(2025, 3, 19, 19, 0, tzinfo=UTC),
    ),
    MAY: (
        dt.datetime(2025, 1, 2, 15, 0, tzinfo=UTC),
        dt.datetime(2025, 5, 7, 19, 0, tzinfo=UTC),
    ),
}

#: The measured listing calendar: Kalshi opens one decision date at a time, each
#: after the previous meeting resolved. Consecutive decision dates are therefore
#: never live together, which is why the archive yields no admissible pair.
MEASURED: dict[dt.date, tuple[dt.datetime, dt.datetime]] = {
    JAN: (
        dt.datetime(2025, 1, 2, 15, 0, tzinfo=UTC),
        dt.datetime(2025, 1, 31, 15, 0, tzinfo=UTC),
    ),
    MAR: (
        dt.datetime(2025, 1, 29, 15, 0, tzinfo=UTC),
        dt.datetime(2025, 3, 21, 15, 0, tzinfo=UTC),
    ),
    MAY: (
        dt.datetime(2025, 3, 20, 15, 0, tzinfo=UTC),
        dt.datetime(2025, 5, 9, 15, 0, tzinfo=UTC),
    ),
}


def contract(
    contract_id: str,
    decision_date: dt.date,
    *,
    threshold: str | None = "4.50",
    inequality: str | None = "above",
    rate_definition: str = RATE,
    yes_axis: str = "yes_pays_if_target_upper_bound_above_threshold",
    orientation: int = 1,
    open_time: dt.datetime | None = None,
    close_time: dt.datetime | None = None,
    windows: dict[dt.date, tuple[dt.datetime, dt.datetime]] = OVERLAPPING,
    series: str = DECISION,
    venue: str = "kalshi",
    rule_hash: str | None = "rules-2025-01-29",
    rule_in_force_from: dt.datetime | None = IN_FORCE_FROM,
    rule_in_force_to: dt.datetime | None = None,
    rule_verified_by: str | None = VERIFIED_BY,
    observation_origin: str = ORIGIN_ARCHIVED_ONLY,
) -> ContractPredicate:
    """One candidate contract on one of the fixtured listing calendars.

    An explicit ``open_time`` or ``close_time`` overrides the calendar, so a test
    that needs an unpublished bound or a boundary instant states it instead of
    relying on the fixture. The rule interval defaults to one that covers every
    fixtured window, so a test that needs an unverified vintage states that instead.
    """
    listed = closed = None
    if open_time is None or close_time is None:
        listed, closed = windows[decision_date]
    return ContractPredicate(
        contract_id=contract_id,
        venue=venue,
        series=series,
        decision_date=decision_date,
        rate_definition=rate_definition,
        threshold=None if threshold is None else Decimal(threshold),
        inequality=inequality,
        yes_axis=yes_axis,
        orientation=orientation,
        open_time=listed if open_time is None else open_time,
        close_time=closed if close_time is None else close_time,
        rule_hash=rule_hash,
        observation_origin=observation_origin,
        rule_in_force_from=rule_in_force_from,
        rule_in_force_to=rule_in_force_to,
        rule_verified_by=rule_verified_by,
    )


def matched_pair(*, origin: dt.datetime = ORIGIN) -> NeighborGraph:
    """A January and a March contract stating one identical payout predicate."""
    return graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=origin,
    )


def test_observation_origin_names_the_paths_a_contract_was_seen_on() -> None:
    """The three observed states stay distinguishable, and an unseen contract is refused.

    Provenance is what lets a consumer condition on how a contract was observed without
    the population being redefined afterwards, so the states must not collapse into a
    single admissible bit, and a contract neither path observed must not borrow a real
    path. None of this narrows eligibility: archived-only and live-only contracts are
    both admissible, because the population is their union rather than either alone.
    """
    assert observation_origin(seen_archive=True, seen_live=False) == ORIGIN_ARCHIVED_ONLY
    assert observation_origin(seen_archive=False, seen_live=True) == ORIGIN_LIVE_ONLY
    assert observation_origin(seen_archive=True, seen_live=True) == ORIGIN_ARCHIVED_AND_LIVE
    with pytest.raises(ValueError, match="neither"):
        observation_origin(seen_archive=False, seen_live=False)

    for origin in (ORIGIN_ARCHIVED_ONLY, ORIGIN_LIVE_ONLY, ORIGIN_ARCHIVED_AND_LIVE):
        assert contract("C", JAN, observation_origin=origin).observation_origin == origin
    with pytest.raises(ValueError, match="observation_origin"):
        contract("C", JAN, observation_origin="not_an_observation_path")

    # Provenance is evidence about the contract, not part of its payoff: two contracts
    # that state one predicate match whatever paths observed them, so provenance cannot
    # silently become a membership or matching rule.
    assert MATCH_FIELDS == ("rate_definition", "threshold", "inequality", "yes_axis", "orientation")
    graph = graph_over_calendar(
        [
            contract("DONOR", JAN, observation_origin=ORIGIN_ARCHIVED_ONLY),
            contract("RECEIVER", MAR, observation_origin=ORIGIN_LIVE_ONLY),
        ],
        at=ORIGIN,
    )
    assert graph.donors_for("RECEIVER") == ("DONOR",)


def test_matched_pair_produces_exactly_one_edge_with_the_correct_direction() -> None:
    graph = matched_pair()

    assert graph.version == GRAPH_VERSION
    assert [(edge.donor_contract_id, edge.receiver_contract_id) for edge in graph.edges] == [
        ("KXFEDDECISION-25JAN-C25", "KXFEDDECISION-25MAR-C25")
    ]
    edge = graph.edges[0]
    assert edge.donor_decision_date == JAN
    assert edge.receiver_decision_date == MAR
    assert edge.relation == RELATION_ECONOMIC_EXPOSURE
    assert edge.rule_hash == "rules-2025-01-29"
    # The edge carries the evidence for the match: every field of MATCH_FIELDS with
    # the value both contracts stated.
    assert tuple(name for name, _ in edge.match_fields) == MATCH_FIELDS
    assert dict(edge.match_fields)["rate_definition"] == RATE
    assert dict(edge.match_fields)["threshold"] == "4.50"

    # Every contract is a receiver, including the one that served as donor: the
    # January contract has no decision date before it to be exposed to.
    assert graph.generated_from == ("KXFEDDECISION-25JAN-C25", "KXFEDDECISION-25MAR-C25")
    assert graph.donors_for("KXFEDDECISION-25MAR-C25") == ("KXFEDDECISION-25JAN-C25",)
    assert graph.donors_for("KXFEDDECISION-25JAN-C25") == ()
    january = graph.decision_for("KXFEDDECISION-25JAN-C25")
    assert january.donor_contract_id is None
    assert january.reason == REASON_NO_ADMISSIBLE_NEIGHBOR
    march = graph.decision_for("KXFEDDECISION-25MAR-C25")
    assert march.donor_contract_id == "KXFEDDECISION-25JAN-C25"
    assert march.reason is None
    assert march.blocked is False
    assert graph.blocked_count == 1


def test_the_edge_joins_the_immediate_predecessor_and_never_skips_a_decision_date() -> None:
    """With three dates live together, the newest predecessor wins and no date is skipped."""
    graph = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN),
            contract("KXFEDDECISION-25MAR-C25", MAR),
            contract("KXFEDDECISION-25MAY-C25", MAY),
        ],
        at=ORIGIN,
    )

    assert [(edge.donor_contract_id, edge.receiver_contract_id) for edge in graph.edges] == [
        ("KXFEDDECISION-25JAN-C25", "KXFEDDECISION-25MAR-C25"),
        ("KXFEDDECISION-25MAR-C25", "KXFEDDECISION-25MAY-C25"),
    ]
    # January is not May's donor: the predecessor is the immediately preceding date,
    # not any earlier date that happens to match.
    assert graph.donors_for("KXFEDDECISION-25MAY-C25") == ("KXFEDDECISION-25MAR-C25",)
    assert [decision.blocked for decision in graph.decisions] == [True, False, False]


def test_a_near_match_threshold_is_not_a_match() -> None:
    """A neighbouring strike at the preceding date yields no edge and says why."""
    graph = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C50", JAN, threshold="5.00"),
            contract("KXFEDDECISION-25MAR-C25", MAR, threshold="4.50"),
        ],
        at=ORIGIN,
    )

    assert graph.edges == ()
    assert graph.donors_for("KXFEDDECISION-25MAR-C25") == ()
    decision = graph.decision_for("KXFEDDECISION-25MAR-C25")
    assert decision.donor_contract_id is None
    assert decision.reason == REASON_THRESHOLD_UNMATCHED
    # The record names the field that differed and refuses the substitution, so a
    # reader can tell a refused near-match from an absent contract.
    assert "threshold" in decision.detail
    assert "near-match" in decision.detail
    assert "KXFEDDECISION-25JAN-C50" in decision.detail


def test_every_field_of_the_match_key_is_compared() -> None:
    """Each field in MATCH_FIELDS can block a pair, not just the threshold."""
    for field, alternative in (
        ("rate_definition", "effective_federal_funds_rate"),
        ("threshold", "4.75"),
        ("inequality", "below"),
        ("yes_axis", "yes_pays_if_target_upper_bound_below_threshold"),
        ("orientation", -1),
    ):
        graph = graph_over_calendar(
            [
                contract("DONOR", JAN, **{field: alternative}),
                contract("RECEIVER", MAR),
            ],
            at=ORIGIN,
        )
        assert graph.edges == (), f"{field} must be compared"
        decision = graph.decision_for("RECEIVER")
        assert decision.reason == REASON_THRESHOLD_UNMATCHED
        assert field in decision.detail

    # A threshold the venue does not publish is its own state, not a wildcard: an
    # absent threshold agrees with an absent threshold and with nothing else.
    absent = graph_over_calendar(
        [
            contract("DONOR", JAN, threshold=None, inequality=None),
            contract("RECEIVER", MAR, threshold="4.50"),
        ],
        at=ORIGIN,
    )
    assert absent.edges == ()
    assert absent.decision_for("RECEIVER").reason == REASON_THRESHOLD_UNMATCHED
    both_absent = graph_over_calendar(
        [
            contract("DONOR", JAN, threshold=None, inequality=None),
            contract("RECEIVER", MAR, threshold=None, inequality=None),
        ],
        at=ORIGIN,
    )
    assert both_absent.donors_for("RECEIVER") == ("DONOR",)


def test_a_donor_that_has_not_listed_yet_is_not_open() -> None:
    """A predicate-identical predecessor listed later is no donor, and says so."""
    graph = graph_over_calendar(
        [
            contract("KXFEDDECISION-25MAR-C25", MAR, open_time=ORIGIN + dt.timedelta(days=1)),
            contract("KXFEDDECISION-25MAY-C25", MAY),
        ],
        at=ORIGIN,
    )

    assert graph.edges == ()
    decision = graph.decision_for("KXFEDDECISION-25MAY-C25")
    assert decision.donor_contract_id is None
    assert decision.reason == REASON_DONOR_NOT_OPEN
    assert "not listed yet" in decision.detail
    assert (ORIGIN + dt.timedelta(days=1)).isoformat() in decision.detail
    # The blocked reason belongs to the donor's state, so the March contract is not
    # itself admitted as a receiver at that origin either.
    assert graph.decision_for("KXFEDDECISION-25MAR-C25").reason == REASON_RECEIVER_NOT_OPEN


def test_a_receiver_that_is_not_listed_yet_is_not_open() -> None:
    graph = graph_over_calendar(
        [contract("KXFEDDECISION-25MAY-C25", MAY, windows=MEASURED)],
        at=MARCH_RELEASE,
    )

    assert graph.edges == ()
    decision = graph.decision_for("KXFEDDECISION-25MAY-C25")
    assert decision.reason == REASON_RECEIVER_NOT_OPEN
    assert "not listed at the forecast origin" in decision.detail
    assert MEASURED[MAY][0].isoformat() in decision.detail


def test_a_donor_that_closed_before_the_origin_is_resolved_before_the_forecast() -> None:
    graph = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=dt.datetime(2025, 2, 5, 12, 0, tzinfo=UTC),
    )

    assert graph.edges == ()
    decision = graph.decision_for("KXFEDDECISION-25MAR-C25")
    assert decision.reason == REASON_RESOLVED_BEFORE_ORIGIN
    assert "stopped trading" in decision.detail
    # A closed contract is not a receiver either: it cannot take a signal.
    assert graph.decision_for("KXFEDDECISION-25JAN-C25").reason == REASON_RESOLVED_BEFORE_ORIGIN
    assert graph.blocked_count == 2


def test_the_open_boundary_is_inclusive_and_the_close_boundary_exclusive() -> None:
    """The liveness rule is the one the panel already reads, boundaries included."""
    live = contract("LIVE", MAR, close_time=ORIGIN + dt.timedelta(seconds=1))
    graph = graph_over_calendar([contract("KXFEDDECISION-25JAN-C25", JAN), live], at=ORIGIN)
    assert graph.decision_for("LIVE").donor_contract_id == "KXFEDDECISION-25JAN-C25"

    closed = contract("CLOSED", MAR, close_time=ORIGIN)
    blocked = graph_over_calendar(
        [contract("KXFEDDECISION-25JAN-C25", JAN), closed],
        at=ORIGIN,
    )
    assert blocked.edges == ()
    assert blocked.decision_for("CLOSED").reason == REASON_RESOLVED_BEFORE_ORIGIN

    listed = contract("LISTED", MAR, open_time=ORIGIN)
    open_here = graph_over_calendar(
        [contract("KXFEDDECISION-25JAN-C25", JAN), listed],
        at=ORIGIN,
    )
    assert open_here.decision_for("LISTED").donor_contract_id == "KXFEDDECISION-25JAN-C25"


def test_the_measured_calendar_yields_no_admissible_pair() -> None:
    """The archived 10-of-10 outcome: no donor for the preceding decision date.

    Kalshi opens the March series only after the January meeting resolved, so at the
    March release the January series is closed and every March strike is blocked.
    The January series lists exactly one strike, so the March contracts are blocked
    for two distinct reasons and neither is repaired by linking the same-expiry
    strikes listed beside them.
    """
    graph = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-H0", JAN, threshold="4.50", windows=MEASURED),
            contract("KXFEDDECISION-25MAR-C25", MAR, threshold="4.25", windows=MEASURED),
            contract("KXFEDDECISION-25MAR-H0", MAR, threshold="4.50", windows=MEASURED),
        ],
        at=MARCH_RELEASE,
    )

    assert graph.edges == ()
    # January is resolved, so it cannot receive; the C25 receiver finds no January
    # contract stating its predicate; the H0 receiver's identical January contract is
    # resolved. Three different states of the world, three different reasons.
    assert graph.decision_for("KXFEDDECISION-25JAN-H0").reason == REASON_RESOLVED_BEFORE_ORIGIN
    assert graph.decision_for("KXFEDDECISION-25MAR-C25").reason == REASON_THRESHOLD_UNMATCHED
    assert graph.decision_for("KXFEDDECISION-25MAR-H0").reason == REASON_RESOLVED_BEFORE_ORIGIN
    assert "KXFEDDECISION-25JAN-H0" in graph.decision_for("KXFEDDECISION-25MAR-H0").detail


def test_same_expiry_strikes_with_no_later_contract_are_a_blocked_graph() -> None:
    """Five live strikes of one meeting, no later-decision contract, zero edges."""
    strikes = ["C25", "H0", "C50", "H25", "C75"]
    graph = graph_over_calendar(
        [
            contract(
                f"KXFEDDECISION-25MAR-{strike}",
                MAR,
                threshold=str(Decimal("4.25") + Decimal(index) / Decimal(4)),
                inequality=None,
                windows=MEASURED,
            )
            for index, strike in enumerate(strikes)
        ],
        at=MARCH_RELEASE,
    )

    assert graph.edges == (), "same-expiry strikes are not each other's donors"
    assert len(graph.decisions) == len(strikes)
    # The declared predecessor date is the January meeting, and this graph holds no
    # contract for it, so the receiver is blocked on the missing predecessor rather
    # than silently linked to an earlier date.
    assert {decision.reason for decision in graph.decisions} == {
        REASON_CALENDAR_DATE_HOLDS_NO_CONTRACT
    }
    assert all(decision.donor_contract_id is None for decision in graph.decisions)
    assert graph.blocked_count == len(strikes)
    assert all(JAN.isoformat() in decision.detail for decision in graph.decisions)
    assert graph.generated_from == tuple(
        sorted(f"KXFEDDECISION-25MAR-{strike}" for strike in strikes)
    )
    assert graph.donors_for("KXFEDDECISION-25MAR-C25") == ()


def test_a_declared_date_holding_no_contract_is_not_skipped() -> None:
    """A missing predecessor blocks the receiver instead of reaching two meetings back.

    The January meeting is declared and the May contract is exposed to the March
    meeting. Holding no March contract means there is no donor for May, and the
    January contract is not a substitute: an edge from it would span two decisions.
    """
    graph = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN),
            contract("KXFEDDECISION-25MAY-C25", MAY),
        ],
        at=ORIGIN,
    )

    assert graph.edges == ()
    may = graph.decision_for("KXFEDDECISION-25MAY-C25")
    assert may.reason == REASON_CALENDAR_DATE_HOLDS_NO_CONTRACT
    assert may.donor_contract_id is None
    assert MAR.isoformat() in may.detail
    assert graph.calendar == CALENDAR


def test_a_contract_outside_the_declared_calendar_is_refused() -> None:
    """A contract on a date the calendar does not declare cannot be an exposure."""
    with pytest.raises(ValueError, match="outside the declared calendar"):
        build_neighbor_graph(
            [
                contract(
                    "KXFEDDECISION-25JUL-C25",
                    dt.date(2025, 7, 30),
                    open_time=dt.datetime(2025, 6, 1, 15, 0, tzinfo=UTC),
                    close_time=dt.datetime(2025, 7, 31, 15, 0, tzinfo=UTC),
                )
            ],
            at=ORIGIN,
            calendar=CALENDAR,
        )


def test_an_edge_needs_a_rule_version_verified_in_force_across_the_window() -> None:
    """A rule hash without an interval is not a certified vintage, so there is no edge."""
    unstated = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN, rule_in_force_from=None),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=ORIGIN,
    )
    assert unstated.edges == ()
    assert unstated.decision_for("KXFEDDECISION-25MAR-C25").reason == REASON_RULE_VINTAGE_UNVERIFIED

    # A hash nobody signed for is the same missing fact as no hash at all.
    unsigned = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN, rule_verified_by=None),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=ORIGIN,
    )
    assert unsigned.edges == ()
    assert unsigned.decision_for("KXFEDDECISION-25MAR-C25").reason == REASON_RULE_VINTAGE_UNVERIFIED

    # An interval that opens after the origin does not reach back over the window.
    late = graph_over_calendar(
        [
            contract(
                "KXFEDDECISION-25JAN-C25",
                JAN,
                rule_in_force_from=ORIGIN + dt.timedelta(days=1),
            ),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=ORIGIN,
    )
    assert late.edges == ()
    assert late.decision_for("KXFEDDECISION-25MAR-C25").reason == REASON_RULE_VINTAGE_UNVERIFIED


def test_a_contract_that_closes_inside_the_window_is_not_live_through_it() -> None:
    """An edge has to hold for the whole measured window, not only at the origin."""
    window_end = ORIGIN + dt.timedelta(minutes=10)
    donor_closes = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN, close_time=ORIGIN + dt.timedelta(minutes=1)),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=ORIGIN,
        window_end=window_end,
    )
    assert donor_closes.edges == ()
    assert (
        donor_closes.decision_for("KXFEDDECISION-25MAR-C25").reason
        == REASON_DONOR_NOT_LIVE_THROUGH_WINDOW
    )

    receiver_closes = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN),
            contract("KXFEDDECISION-25MAR-C25", MAR, close_time=ORIGIN + dt.timedelta(minutes=1)),
        ],
        at=ORIGIN,
        window_end=window_end,
    )
    assert receiver_closes.edges == ()
    assert (
        receiver_closes.decision_for("KXFEDDECISION-25MAR-C25").reason
        == REASON_RECEIVER_NOT_LIVE_THROUGH_WINDOW
    )

    # Listed across the whole window, the same pair still produces its edge.
    held = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=ORIGIN,
        window_end=window_end,
    )
    assert [(edge.donor_contract_id, edge.receiver_contract_id) for edge in held.edges] == [
        ("KXFEDDECISION-25JAN-C25", "KXFEDDECISION-25MAR-C25")
    ]
    assert held.edges[0].donor_rule_in_force_from == IN_FORCE_FROM


def test_equivalent_donors_are_selected_by_a_declared_rule_and_counted() -> None:
    """Two predicate-identical donors resolve deterministically, and the tie is visible."""
    graph = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-B25", JAN),
            contract("KXFEDDECISION-25JAN-A25", JAN),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=ORIGIN,
    )

    march = graph.decision_for("KXFEDDECISION-25MAR-C25")
    assert march.donor_contract_id == "KXFEDDECISION-25JAN-A25"
    assert march.donor_candidate_count == 2
    assert "lowest contract id" in march.detail
    assert march.as_dict()["donor_candidate_count"] == 2


def test_an_empty_graph_is_valid_and_round_trips() -> None:
    graph = graph_over_calendar([], at=ORIGIN)

    assert graph.edges == ()
    assert graph.decisions == ()
    assert graph.generated_from == ()
    assert graph.donors_for("anything") == ()
    payload = graph.as_dict()
    assert payload["edges"] == []
    assert payload["decisions"] == []
    assert payload["counts"] == {"edges": 0, "decisions": 0, "blocked": 0, "reasons": {}}
    # The empty payload is JSON-representable, which is what a run writes out.
    assert json.loads(json.dumps(payload, sort_keys=True)) == payload
    assert graph_digest(graph) == hashlib.sha256(b"[]").hexdigest()


def test_every_decision_round_trips_through_as_dict() -> None:
    payload = matched_pair().as_dict()

    assert json.loads(json.dumps(payload, sort_keys=True)) == payload
    assert payload["version"] == GRAPH_VERSION
    assert payload["digest"] == graph_digest(matched_pair())
    assert [edge["donor_contract_id"] for edge in payload["edges"]] == ["KXFEDDECISION-25JAN-C25"]
    assert [decision["receiver_contract_id"] for decision in payload["decisions"]] == [
        "KXFEDDECISION-25JAN-C25",
        "KXFEDDECISION-25MAR-C25",
    ]
    blocked = payload["decisions"][0]
    assert blocked["reason"] == REASON_NO_ADMISSIBLE_NEIGHBOR
    assert blocked["blocked"] is True
    assert blocked["detail"]


def test_the_digest_is_stable_across_runs_and_changes_when_an_edge_changes() -> None:
    first = matched_pair()
    assert graph_digest(first) == graph_digest(matched_pair())
    # Input order is not part of the graph, so it is not part of its identity.
    reordered = graph_over_calendar(
        [
            contract("KXFEDDECISION-25MAR-C25", MAR),
            contract("KXFEDDECISION-25JAN-C25", JAN),
        ],
        at=ORIGIN,
    )
    assert graph_digest(reordered) == graph_digest(first)

    # One changed edge changes the digest.
    changed = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN, rule_hash="rules-2025-02-01"),
            contract("KXFEDDECISION-25MAR-C25", MAR),
        ],
        at=ORIGIN,
    )
    assert graph_digest(changed) != graph_digest(first)

    # A blocked graph has its own digest, distinct from the populated one it came
    # from and from the empty graph.
    blocked = graph_over_calendar(
        [contract("KXFEDDECISION-25MAR-C25", MAR)],
        at=MARCH_RELEASE,
    )
    assert graph_digest(blocked) == hashlib.sha256(b"[]").hexdigest()
    assert graph_digest(first) != graph_digest(blocked)


def test_a_decision_is_recorded_for_every_receiver_exactly_once() -> None:
    """The graph never silently skips a receiver, matched or blocked."""
    graph = graph_over_calendar(
        [
            contract("KXFEDDECISION-25JAN-C25", JAN),
            contract("KXFEDDECISION-25MAR-C25", MAR),
            contract("KXFEDDECISION-25MAY-C50", MAY, threshold="5.00"),
        ],
        at=ORIGIN,
    )

    assert isinstance(graph, NeighborGraph)
    assert len(graph.decisions) == len(graph.generated_from) == 3
    assert {decision.receiver_contract_id for decision in graph.decisions} == set(
        graph.generated_from
    )
    # The blocked May receiver is present with its reason, not absent from the graph.
    assert graph.decision_for("KXFEDDECISION-25MAY-C50").reason == REASON_THRESHOLD_UNMATCHED
    assert graph.as_dict()["counts"] == {
        "edges": 1,
        "decisions": 3,
        "blocked": 2,
        "reasons": {
            REASON_NO_ADMISSIBLE_NEIGHBOR: 1,
            REASON_THRESHOLD_UNMATCHED: 1,
        },
    }
