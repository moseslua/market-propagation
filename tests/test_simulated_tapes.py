"""The tape emitter's contract, and its integration with the production builders.

The point of these tapes is that they reach the real graph, panel and ladder rather
than a parallel implementation that agrees with itself. So the tests below assert the
things that would be false if the emitter quietly bypassed the production path: the
graph admits edges through its rule gate, and the panel builds the declared universe
including receivers that never printed.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

import pytest

from market_propagation import historical_forecast, neighbors, simulated_tapes

NULL_SCENARIO = "shared_news_delay"
RECOVERY_SCENARIO = "communication"
N_EVENTS = 5
#: Simulated receivers are made of this family only, so the declared calendar is a
#: sequence of same-series releases. See ``simulate_tapes`` on why.
FAMILY = "cpi"


def _tapes(scenario: str = RECOVERY_SCENARIO, seed: int = 11):
    return simulated_tapes.simulate_tapes(scenario, seed=seed, n_events=N_EVENTS, family=FAMILY)


def _graph(tapes: simulated_tapes.SimulatedTapes):
    origin = tapes.releases[1].event_time + dt.timedelta(seconds=300)
    return neighbors.build_neighbor_graph(
        tapes.contracts,
        at=origin,
        calendar=tapes.calendar,
        window_end=origin + dt.timedelta(seconds=300),
    )


def test_the_same_seed_reproduces_the_same_tape() -> None:
    first = _tapes(seed=11)
    second = _tapes(seed=11)
    other = _tapes(seed=12)

    assert first.digest() == second.digest()
    assert first.digest() != other.digest()
    assert simulated_tapes.tape_digest([first]) == simulated_tapes.tape_digest([second])


def test_every_print_carries_a_source_time_and_a_named_price_axis() -> None:
    tapes = _tapes()

    assert tapes.trades, "a tape with no prints cannot exercise the observation process"
    for trade in tapes.trades:
        assert trade.clock.source_time is not None
        assert trade.event_axis == simulated_tapes.EVENT_AXIS
        assert trade.event_price is not None
        assert trade.has_event_axis
        # A generated print was never received from anywhere, so a receipt would be an
        # invented fact and an unknown availability interval is the honest state.
        assert not trade.clock.availability.is_known
        assert 0 <= trade.event_price <= 1


def test_the_tape_declares_synthetic_rule_evidence_rather_than_implying_an_archive() -> None:
    tapes = _tapes()

    assert "synthetic_process" in tapes.flags
    assert "rule_evidence_declared_not_attested" in tapes.flags
    assert tapes.truth["rule_evidence"] == "declared"
    for contract in tapes.contracts:
        assert contract.rule_verified_by == simulated_tapes.SYNTHETIC_RULE_METHOD


def test_the_graph_admits_edges_through_its_rule_gate() -> None:
    """The whole reason the calibration runs here and cannot run on the archive."""
    tapes = _tapes()

    graph = _graph(tapes)

    assert graph.edges, "no edge is admissible, so the rule gate is not being exercised"
    refused = [
        decision
        for decision in graph.decisions
        if decision.reason == neighbors.REASON_RULE_VINTAGE_UNVERIFIED
    ]
    assert not refused


def test_the_panel_builds_exactly_the_declared_universe() -> None:
    tapes = _tapes()
    graph = _graph(tapes)
    settings = historical_forecast.ForecastSettings(
        forecast_origin_seconds=300, lag_guard_seconds=60, future_horizon_seconds=300
    )
    declared: dict[str, Any] = dict(tapes.receivers())
    printed = {trade.contract_id for trade in tapes.trades}
    # A receiver that never printed, declared before the release. The panel has to keep
    # its row: a quiet contract is a missing observation, not an absent row, and
    # dropping it is how the sample drifts toward whatever happened to trade.
    silent = "beta-cpi-C0-9999"
    assert silent not in printed
    first_release = min(declared)
    declared[first_release] = (*declared[first_release], silent)

    panel = historical_forecast.build_forecast_rows(
        tapes.trades,
        tapes.releases,
        graph,
        receivers=declared,
        settings=settings,
        venue=tapes.venue,
    )

    rows = [dict(row) for row in panel.rows]
    built = {(row["event_id"], row["receiver_contract_id"]) for row in rows}
    expected = {(event_id, contract) for event_id, names in declared.items() for contract in names}
    assert built == expected

    silent_rows = [row for row in rows if row["receiver_contract_id"] == silent]
    assert len(silent_rows) == 1
    assert silent_rows[0]["recipient_anchor"] is None


def test_a_receiver_without_a_declared_release_set_is_refused() -> None:
    """Falling back to the traded contracts would reselect the universe by activity."""
    tapes = _tapes()
    partial: Mapping[str, Any] = dict(tapes.receivers())
    partial.pop(next(iter(partial)))

    with pytest.raises(ValueError, match="declares no set for release"):
        historical_forecast.build_forecast_rows(
            tapes.trades,
            tapes.releases,
            None,
            receivers=partial,
        )


def test_the_elected_donor_trades_across_the_receivers_release() -> None:
    """The attribution the graph's adjacent-meeting edge depends on.

    The graph elects a donor from the preceding meeting, and the panel can read a donor
    return only from a donor print inside the receiver's release window. Each contract is
    therefore declared about one meeting and observed at the release before it, which
    lets the fast venue's path and the target venue's path trade together while remaining
    different decisions. Without that attribution every elected donor has stopped
    printing a meeting earlier, the neighbour and control columns come back null on every
    row, and the calibration has nothing to measure.
    """
    tapes = _tapes()
    graph = _graph(tapes)
    declared = tapes.receivers()
    wanted = {name for names in declared.values() for name in names}
    donors = {
        edge.receiver_contract_id: edge.donor_contract_id
        for edge in graph.edges
        if edge.receiver_contract_id in wanted
    }
    assert donors

    printed: dict[str, list[dt.datetime]] = {}
    for trade in tapes.trades:
        printed.setdefault(trade.contract_id, []).append(trade.clock.source_time)

    for release in tapes.releases:
        for name in declared[release.event_id]:
            donor = donors.get(name)
            if donor is None:
                continue
            across = [
                moment
                for moment in printed.get(donor, ())
                if release.event_time <= moment <= release.event_time + dt.timedelta(seconds=240)
            ]
            assert across, (
                f"donor {donor} of receiver {name} printed nothing across "
                f"{release.event_id}; a neighbour return can only be read from a print "
                "inside the window, so the column would be null"
            )


def test_every_declared_receiver_is_dated_ahead_of_its_release() -> None:
    """A receiver is a contract about a meeting still ahead of the release.

    Reading the decision date off the observation instead of off the contract would date
    every contract to the release it printed at, collapse the calendar to one date per
    release, and leave the graph no predecessor to pair with. The rule would then be
    calibrated against the emitter rather than against the mechanism.
    """
    tapes = _tapes()
    decisions = {contract.contract_id: contract.decision_date for contract in tapes.contracts}
    receivers = tapes.receivers()
    assert len({decisions[name] for names in receivers.values() for name in names}) > 1
    for release in tapes.releases:
        for name in receivers[release.event_id]:
            assert decisions[name] > release.event_time.date()
