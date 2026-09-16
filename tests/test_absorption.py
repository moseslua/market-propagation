"""Acceptance tests for the scalar absorption estimands.

The repository already carries a horizon-indexed response curve. What it does not
carry is the scalar that curve implies — when the move was absorbed — and reading
one off the other is where a study can go wrong quietly. Each test here defends one
of the ways that happens:

* ``H`` defines ``R`` and ``A`` is a fraction of ``R``, so the terminal horizon
  must be stated and one ramp must return different absorption times for different
  ``H``.
* A zero terminal reaction makes the fraction undefined, so the pair is refused by
  name rather than divided by. A zero response is an observed fact, so it survives
  on the refusal.
* A path whose move is already past a fraction at its first observed horizon has no
  locatable crossing. The bound is reported and no instant is invented between the
  release and that horizon.
* A path that reverses is reported as it is. The tests pin the reversal and assert
  the answer is *not* the one a monotone envelope would have produced.
* A path anchored at or after the release measures a forward increment, not the
  release's own absorption, and is refused rather than read as absorption.
* The panel summary keeps refused pairs in its denominator and counts them by
  reason, and its medians are taken over release clusters rather than over pairs.
"""

from __future__ import annotations

import datetime as dt
import inspect

import pandas as pd
import pytest

from market_propagation import absorption as ab

TAU = dt.datetime(2025, 3, 12, 12, 30, 0, tzinfo=dt.UTC)
CONTRACT = "KXCPI-25MAR-T0.4"
HORIZONS = (60, 300, 900)
TERMINAL = 900


def _path(
    *responses: tuple[int, float | None],
    event: str = "cpi-2025-02",
    cluster: str = "cpi-2025-02",
    contract: str = CONTRACT,
    baseline_offset: float | None = -30.0,
    family: str | None = "cpi",
) -> ab.AbsorptionPath:
    return ab.AbsorptionPath(
        event_id=event,
        cluster_id=cluster,
        contract_id=contract,
        responses=tuple(responses),
        baseline_offset_seconds=baseline_offset,
        family=family,
    )


def _linear(hundredths_of_h: float, horizon: int) -> float:
    """A ramp whose response is proportional to the horizon, normalised at 900s."""
    return hundredths_of_h * horizon / TERMINAL


def _panel(
    pairs: list[tuple[str, str, str, float | None, list[tuple[int, float | None]]]],
    *,
    family: str = "cpi",
) -> pd.DataFrame:
    """A sealed-shape trade panel: the columns a path is read from, real values.

    Entry shape is ``(event_id, cluster_id, contract_id, baseline_offset, rows)``,
    where each row is a ``(horizon_seconds, response)`` pair.
    """
    rows = []
    for event_id, cluster_id, contract_id, offset, values in pairs:
        baseline = None if offset is None else TAU + dt.timedelta(seconds=offset)
        for horizon, response in values:
            rows.append(
                {
                    "event_id": event_id,
                    "cluster_id": cluster_id,
                    "family": family,
                    "venue": "kalshi",
                    "contract_id": contract_id,
                    "event_time": TAU,
                    "horizon_seconds": horizon,
                    "baseline_source_time": baseline,
                    "response": response,
                }
            )
    return pd.DataFrame(rows)


def test_an_exactly_linear_ramp_recovers_the_defining_property_of_each_time() -> None:
    """On ``A(h) = h / H`` the crossing is at ``H/2`` and ``0.9 * H`` by definition."""
    path = _path(*((h, _linear(100.0, h)) for h in HORIZONS))

    estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)

    assert estimate.status == ab.STATUS_ESTIMATED
    assert estimate.h50_seconds == pytest.approx(TERMINAL / 2)
    assert estimate.h90_seconds == pytest.approx(0.9 * TERMINAL)
    assert dict(estimate.cumulative_fractions) == {
        60: pytest.approx(60 / TERMINAL),
        300: pytest.approx(300 / TERMINAL),
        900: pytest.approx(1.0),
    }


def test_the_terminal_horizon_has_no_default_and_defines_both_times() -> None:
    """``H`` defines ``R``, so it is stated with no default and it changes the answer."""
    parameter = inspect.signature(ab.estimate_absorption).parameters["terminal_horizon_seconds"]
    assert parameter.default is inspect.Parameter.empty
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY

    path = _path(*((h, _linear(100.0, h)) for h in (60, 300, 900, 1800)))

    with pytest.raises(TypeError):
        ab.estimate_absorption(path)  # type: ignore[call-arg]

    at_900 = ab.estimate_absorption(path, terminal_horizon_seconds=900)
    at_1800 = ab.estimate_absorption(path, terminal_horizon_seconds=1800)

    assert at_900.h50_seconds == pytest.approx(450.0)
    assert at_1800.h50_seconds == pytest.approx(900.0)
    assert at_900.h90_seconds == pytest.approx(810.0)
    assert at_1800.h90_seconds == pytest.approx(1620.0)


def test_a_zero_terminal_reaction_is_refused_by_name_and_never_divided_by() -> None:
    """``R == 0`` leaves the fraction undefined; the observed zero is kept beside it."""
    path = _path((60, 0.0), (300, 0.0), (TERMINAL, 0.0))

    estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)

    assert estimate.status == ab.STATUS_REFUSED
    assert estimate.reason == ab.REASON_TERMINAL_RESPONSE_ZERO
    assert estimate.reason in estimate.reasons
    assert estimate.terminal_response == 0.0
    assert estimate.h50_seconds is None
    assert estimate.h90_seconds is None
    assert estimate.cumulative_fractions == ()


def test_a_terminal_horizon_that_was_not_observed_is_refused_by_name() -> None:
    """An absent endpoint is not a zero reaction, and neither one yields a fraction."""
    unobserved = _path((60, 0.1), (300, 1.0), (TERMINAL, None))
    undeclared = _path((60, 0.1), (300, 1.0))

    no_endpoint = ab.estimate_absorption(unobserved, terminal_horizon_seconds=TERMINAL)
    other_horizon = ab.estimate_absorption(undeclared, terminal_horizon_seconds=TERMINAL)

    assert no_endpoint.reason == ab.REASON_TERMINAL_HORIZON_UNOBSERVED
    assert other_horizon.reason == ab.REASON_TERMINAL_HORIZON_UNDECLARED
    assert no_endpoint.terminal_response is None
    assert no_endpoint.h50_seconds is None


def test_a_crossing_that_the_declared_horizons_cannot_locate_reports_a_bound_only() -> None:
    """Past the fraction at the first observed horizon, no instant is invented."""
    path = _path((300, 95.0), (TERMINAL, 100.0))

    estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)

    assert estimate.status == ab.STATUS_ESTIMATED
    assert estimate.h50_seconds is None
    assert estimate.h90_seconds is None
    assert estimate.h50_reason == ab.REASON_REACHED_BEFORE_FIRST_OBSERVED_HORIZON
    assert estimate.h90_reason == ab.REASON_REACHED_BEFORE_FIRST_OBSERVED_HORIZON
    assert estimate.h50_bound_horizon_seconds == 300.0
    assert estimate.h90_bound_horizon_seconds == 300.0


def test_a_non_monotone_path_is_reported_rather_than_smoothed() -> None:
    """``A`` reversing against ``R`` is reported, and the answer is not the envelope's."""
    path = _path((60, 20.0), (300, 70.0), (600, 40.0), (TERMINAL, 100.0))

    estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)

    assert estimate.reversals == ((300, 600),)
    assert ab.FLAG_NON_MONOTONE in estimate.flags
    assert estimate.max_drawdown_fraction == pytest.approx(0.3)
    assert dict(estimate.cumulative_fractions)[300] == pytest.approx(0.7)
    assert dict(estimate.cumulative_fractions)[600] == pytest.approx(0.4)
    assert estimate.h50_seconds == pytest.approx(204.0)
    # A running-maximum envelope would report 800.0 here. The tape's own path is
    # what is answered from, so the reported crossing is the later, real one.
    assert estimate.h90_seconds == pytest.approx(850.0)
    assert estimate.h90_seconds != pytest.approx(800.0)


def test_an_overshoot_past_the_terminal_reaction_is_reported() -> None:
    """A move that goes past ``R`` is a feature of the tape, not something to clip."""
    path = _path((60, 40.0), (300, 140.0), (TERMINAL, 100.0))

    estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)

    assert ab.FLAG_OVERSHOOT in estimate.flags
    assert estimate.overshoot_fraction == pytest.approx(0.4)
    assert max(value for _, value in estimate.cumulative_fractions) == pytest.approx(1.4)


def test_a_baseline_at_or_after_the_release_is_refused_rather_than_read_as_absorption() -> None:
    """The forecast origin's own reaction is a different estimand, not this one."""
    at_release = _path((300, 50.0), (TERMINAL, 100.0), baseline_offset=0.0)
    after_release = _path((300, 50.0), (TERMINAL, 100.0), baseline_offset=300.0)
    unobserved = _path((300, 50.0), (TERMINAL, 100.0), baseline_offset=None)

    for path in (at_release, after_release):
        estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)
        assert estimate.status == ab.STATUS_REFUSED
        assert estimate.reason == ab.REASON_BASELINE_NOT_PRE_RELEASE
    assert (
        ab.estimate_absorption(unobserved, terminal_horizon_seconds=TERMINAL).reason
        == ab.REASON_PAIR_BASELINE_UNOBSERVED
    )


def test_an_unobserved_horizon_is_absent_from_the_curve_and_never_filled() -> None:
    """A horizon with no endpoint contributes no fraction and is named as a gap."""
    path = _path((60, 10.0), (300, None), (TERMINAL, 100.0))

    estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)

    assert 300 not in dict(estimate.cumulative_fractions)
    assert 300 in estimate.unobserved_horizons_seconds
    assert ab.FLAG_SPANS_UNOBSERVED_HORIZON in estimate.flags
    assert estimate.h50_reason is None
    assert estimate.h50_seconds == pytest.approx(60 + (0.5 - 0.1) / 0.9 * 840)


def test_a_fraction_met_exactly_on_a_declared_horizon_reports_that_horizon() -> None:
    """An exact hit is the declared horizon itself, not an interpolation of it."""
    path = _path((60, 10.0), (300, 50.0), (TERMINAL, 100.0))

    estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)

    assert estimate.h50_seconds == 300.0
    assert estimate.h90_seconds == pytest.approx(780.0)


def test_no_absorption_time_is_ever_extrapolated_past_the_declared_horizons() -> None:
    """Every located crossing lies inside the observed part of the declared set."""
    shapes = [
        ((60, 1.0), (300, 99.0), (TERMINAL, 100.0)),
        ((60, 0.5), (300, 40.0), (TERMINAL, 100.0)),
        ((60, 90.0), (300, 95.0), (TERMINAL, 100.0)),
        ((300, 50.0), (TERMINAL, 100.0)),
        ((60, -10.0), (300, 40.0), (TERMINAL, 100.0)),
    ]

    for shape in shapes:
        path = _path(*shape)
        estimate = ab.estimate_absorption(path, terminal_horizon_seconds=TERMINAL)
        observed = estimate.observed_horizons_seconds
        for value in (estimate.h50_seconds, estimate.h90_seconds):
            if value is None:
                continue
            assert min(observed) <= value <= TERMINAL


def test_horizons_past_the_terminal_horizon_are_recorded_not_folded_in() -> None:
    """A reaction read against a different ``R`` is a different number."""
    path = _path((60, 10.0), (300, 100.0), (1800, 150.0))

    estimate = ab.estimate_absorption(path, terminal_horizon_seconds=300)

    assert estimate.horizons_beyond_terminal_seconds == (1800,)
    assert 1800 not in dict(estimate.cumulative_fractions)
    assert estimate.h50_seconds == pytest.approx(60 + (0.4 / 0.9) * 240)


def test_the_panel_summary_counts_refusals_by_reason_instead_of_dropping_them() -> None:
    """A refused pair stays in the denominator and is named at the stage it failed."""
    frame = _panel(
        [
            ("rel-1", "cl-1", "K1", -30.0, [(60, 10.0), (300, 90.0), (TERMINAL, 100.0)]),
            ("rel-2", "cl-2", "K2", -30.0, [(60, 0.0), (300, 0.0), (TERMINAL, 0.0)]),
            ("rel-3", "cl-3", "K3", 300.0, [(60, 10.0), (300, 90.0), (TERMINAL, 100.0)]),
        ]
    )

    summary = ab.summarise_absorption_panel(frame, terminal_horizon_seconds=TERMINAL, samples=50)
    overall = summary["overall"]

    assert overall["pairs"] == 3
    assert overall["estimateable_pairs"] == 1
    assert overall["refused_pairs"] == 2
    assert overall["estimateable_pairs"] + overall["refused_pairs"] == overall["pairs"]
    assert overall["refusals_by_reason"] == {
        ab.REASON_TERMINAL_RESPONSE_ZERO: 1,
        ab.REASON_BASELINE_NOT_PRE_RELEASE: 1,
    }
    assert overall["refusals_by_stage"]["pair"] == overall["refusals_by_reason"]
    assert overall["median_h50_seconds"] is not None


def test_a_pair_whose_crossing_is_bound_only_is_estimateable_with_a_bound() -> None:
    """``R`` exists, so the pair is estimateable; only the crossing is unavailable."""
    frame = _panel(
        [
            ("rel-1", "cl-1", "K1", -30.0, [(300, 95.0), (TERMINAL, 100.0)]),
            ("rel-2", "cl-2", "K2", -30.0, [(60, 0.0), (TERMINAL, 0.0)]),
        ]
    )

    overall = ab.summarise_absorption_panel(frame, terminal_horizon_seconds=TERMINAL, samples=50)[
        "overall"
    ]

    assert overall["pairs"] == 2
    assert overall["estimateable_pairs"] == 1
    assert overall["refused_pairs"] == 1
    # Estimateable is about ``R``, not about locating a crossing.
    assert overall["pairs_with_both_times"] == 0
    assert overall["pairs_with_h50"] == 0
    assert overall["pairs_with_h50_bounded_only"] == 1
    assert overall["refusals_by_reason"] == {
        ab.REASON_REACHED_BEFORE_FIRST_OBSERVED_HORIZON: 2,
        ab.REASON_TERMINAL_RESPONSE_ZERO: 1,
    }


def test_the_summary_reports_pairs_it_cannot_estimate_as_null_and_never_as_zero() -> None:
    """With every pair refused there is no median, and no median is not zero."""
    frame = _panel(
        [
            ("rel-1", "cl-1", "K1", -30.0, [(60, 0.0), (TERMINAL, 0.0)]),
            ("rel-2", "cl-2", "K2", -30.0, [(60, 0.0), (TERMINAL, 0.0)]),
        ]
    )

    overall = ab.summarise_absorption_panel(frame, terminal_horizon_seconds=TERMINAL, samples=50)[
        "overall"
    ]

    assert overall["median_h50_seconds"] is None
    assert overall["median_h90_seconds"] is None
    assert overall["refusals_by_reason"] == {ab.REASON_TERMINAL_RESPONSE_ZERO: 2}
    assert overall["uncertainty"]["status"] == "not_estimated"
    assert overall["uncertainty"]["reason"] == ab.REASON_NO_CLUSTER_YIELDS_AN_ABSORPTION_TIME


def test_the_summary_medians_over_release_clusters_rather_than_over_pairs() -> None:
    """One release with two contracts must not outweigh a release with one."""
    # cluster cl-1: two contracts, each crossing 50% at 100s. cluster cl-2: one
    # contract crossing at 400s. Pair-level median is 100; release-level is 250.
    frame = _panel(
        [
            ("rel-1", "cl-1", "K1", -30.0, [(60, 40.0), (300, 100.0), (TERMINAL, 100.0)]),
            ("rel-1", "cl-1", "K2", -30.0, [(60, 40.0), (300, 100.0), (TERMINAL, 100.0)]),
            ("rel-2", "cl-2", "K3", -30.0, [(60, 10.0), (300, 40.0), (TERMINAL, 100.0)]),
        ]
    )

    overall = ab.summarise_absorption_panel(frame, terminal_horizon_seconds=TERMINAL, samples=50)[
        "overall"
    ]

    assert overall["pairs"] == 3
    assert overall["estimateable_pairs"] == 3
    assert overall["clusters_total"] == 2
    assert overall["clusters_with_an_h50"] == 2
    assert overall["median_h50_seconds"] == pytest.approx(250.0)
    assert overall["median_h50_seconds"] != pytest.approx(100.0)


def test_a_single_release_cluster_reports_no_interval_rather_than_a_precise_one() -> None:
    """One cluster cannot identify a cluster-robust interval, and says so."""
    frame = _panel(
        [
            ("rel-1", "cl-1", "K1", -30.0, [(60, 40.0), (300, 100.0), (TERMINAL, 100.0)]),
            ("rel-1", "cl-1", "K2", -30.0, [(60, 10.0), (300, 40.0), (TERMINAL, 100.0)]),
        ]
    )

    uncertainty = ab.summarise_absorption_panel(
        frame, terminal_horizon_seconds=TERMINAL, samples=50
    )["overall"]["uncertainty"]

    assert uncertainty["n_clusters"] == 1
    assert uncertainty["reason"] == ab.REASON_TOO_FEW_RELEASE_CLUSTERS
    assert uncertainty["samples"]["median_h50_seconds"]["lower"] is None
    assert uncertainty["samples"]["median_h50_seconds"]["upper"] is None


def test_the_summary_reports_every_family_and_the_panel_together() -> None:
    """Per-family estimates sit beside the overall one, and an undeclared family is named."""
    cpi = _panel([("rel-1", "cl-1", "K1", -30.0, [(60, 40.0), (TERMINAL, 100.0)])], family="cpi")
    employment = _panel(
        [("rel-2", "cl-2", "K2", -30.0, [(60, 10.0), (TERMINAL, 100.0)])], family="employment"
    )
    undeclared = _panel(
        [("rel-3", "cl-3", "K3", -30.0, [(60, 10.0), (TERMINAL, 100.0)])], family=""
    ).drop(columns=["family"])

    summary = ab.summarise_absorption_panel(
        pd.concat([cpi, employment, undeclared], ignore_index=True),
        terminal_horizon_seconds=TERMINAL,
        samples=50,
    )

    assert sorted(summary["by_family"]) == ["cpi", "employment", ab.UNSPECIFIED_FAMILY]
    assert summary["pairs"] == 3
    assert summary["terminal_horizon_seconds"] == TERMINAL
    assert summary["overall"]["pairs"] == 3
    assert summary["horizons_seconds"] == [60, TERMINAL]


def test_a_panel_that_anchors_each_horizon_separately_is_refused() -> None:
    """Two baselines for one pair is a forward-increment path wearing this name."""
    frame = _panel([("rel-1", "cl-1", "K1", -30.0, [(60, 10.0), (TERMINAL, 100.0)])])
    moved = frame.copy()
    moved.loc[moved["horizon_seconds"] == TERMINAL, "baseline_source_time"] = TAU - dt.timedelta(
        seconds=120
    )

    with pytest.raises(ab.AbsorptionError):
        ab.paths_from_panel(moved)


def test_reading_a_path_needs_the_columns_the_estimand_is_defined_on() -> None:
    """A frame without the baseline column cannot say what the fractions are of."""
    frame = _panel([("rel-1", "cl-1", "K1", -30.0, [(60, 10.0), (TERMINAL, 100.0)])])

    with pytest.raises(ab.AbsorptionError):
        ab.paths_from_panel(frame.drop(columns=["baseline_source_time"]))


def test_one_panel_row_per_horizon_is_required_and_a_duplicate_is_refused() -> None:
    """Two rows for one horizon would force a choice the record does not rank."""
    with pytest.raises(ab.AbsorptionError):
        _path((60, 10.0), (60, 20.0), (TERMINAL, 100.0))
