"""Regression tests for the network forecast-gate falsification audit.

These tests check the audit's contract rather than asserting a numerical
recovery rate: whether the communication process is recovered at a given event
count is a finite-sample question, and a test that pinned one seed's answer
would be exactly the lucky-seed reading the audit exists to prevent.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

import market_propagation.falsification as falsification
import market_propagation.models as models
import market_propagation.simulation as simulation

#: Small settings that keep the audit fast while still exercising the pipeline.
TINY = {"n_events": 60, "repetitions": 3}


def test_network_falsification_reports_the_shared_estimator_contract():
    """The audit must name the production estimator, metric and threshold.

    A null rate is only usable by the forecast gate if it came from the same
    pipeline and the same bar, so those fields are part of the payload's
    contract rather than decoration.
    """
    result = falsification.network_falsification(**TINY)
    assert result["estimator"] == models.FORECAST_ESTIMATOR["estimator"]
    assert result["metric"] == models.FORECAST_ESTIMATOR["metric"]
    assert result["metric"] == models.PROMOTION_GATE["metric"]
    assert result["split"] == models.FORECAST_ESTIMATOR["split"]
    assert result["minimum_mae_gain"] == pytest.approx(
        models.PROMOTION_GATE["default_minimum_mae_gain"]
    )
    assert result["estimator_identity"] == dict(falsification.NETWORK_FALSIFICATION_ESTIMATOR)


def test_network_falsification_threads_the_time_settings_through():
    """Horizon and prediction delay reach the comparison, not just the report."""
    result = falsification.network_falsification(
        n_events=60, repetitions=2, horizon_seconds=300, prediction_delay_seconds=60
    )
    assert result["horizon_seconds"] == [300]
    assert result["prediction_delay_seconds"] == pytest.approx(60.0)
    for kind in ("null", "recovery"):
        for run in result[kind]["runs"]:
            assert run["n_events_test"] > 0
            assert run["common_sample_violations"] == []
    # The same settings are recorded on the comparison the gate would read.
    comparison = models.nested_comparison(
        simulation.primary_target(
            simulation.simulate_scenario(
                "communication",
                seed=result["seeds"][0],
                n_events=60,
                horizons=[300],
                forecast_delay=60,
            )
        ),
        seed=result["seeds"][0],
        kinds=("news", "network"),
    ).as_record()
    assert comparison["promotion"]["time_settings"] == {
        "horizon_seconds": [300],
        "prediction_delay_seconds": pytest.approx(60.0),
    }


def test_null_and_recovery_share_one_seed_per_repetition():
    """Matched nuisance draws are the point of the paired design.

    Each repetition must drive both processes from one seed and expose the same
    comparison sample, so the difference between them isolates the declared
    mechanism rather than a resampled noise draw.
    """
    result = falsification.network_falsification(**TINY)
    assert result["seeds"] == list(
        falsification.falsification_seeds(seed=falsification.DEFAULT_SEED, repetitions=3)
    )
    null_runs = result["null"]["runs"]
    recovery_runs = result["recovery"]["runs"]
    assert len(null_runs) == len(recovery_runs) == result["repetitions"]
    for null_run, recovery_run in zip(null_runs, recovery_runs, strict=True):
        assert null_run["seed"] == recovery_run["seed"]
        assert null_run["scenario"] == "shared_news_delay"
        assert recovery_run["scenario"] == "communication"
        assert null_run["n_events_total"] == recovery_run["n_events_total"]


def test_null_and_communication_contrast_is_real_over_fixed_seeds():
    """The two processes must genuinely differ under the audit's own seeds.

    Gains are recomputed from the production comparison, so the contrast is
    detectable only if the frames are distinct processes. The check is that the
    paired difference is not identically zero and that every reported statistic
    is finite, without asserting which process wins.
    """
    result = falsification.network_falsification(**TINY)
    paired = result["paired_contrast"]["per_seed"]
    assert len(paired) == result["repetitions"]
    differences = [row["paired_difference"] for row in paired]
    assert all(math.isfinite(value) for value in differences)
    assert any(value != 0.0 for value in differences), (
        "the null and communication frames produced identical gains at every seed, so the "
        "audit is not contrasting two processes"
    )
    for row in paired:
        assert row["null_gain"] == pytest.approx(
            next(run["gain"] for run in result["null"]["runs"] if run["seed"] == row["seed"])
        )
        assert row["recovery_gain"] == pytest.approx(
            next(run["gain"] for run in result["recovery"]["runs"] if run["seed"] == row["seed"])
        )
    assert result["null"]["mean_gain"] == pytest.approx(
        float(np.mean([run["gain"] for run in result["null"]["runs"]]))
    )
    assert result["recovery"]["mean_gain"] == pytest.approx(
        float(np.mean([run["gain"] for run in result["recovery"]["runs"]]))
    )


def test_counts_match_the_reported_rates_and_flags():
    """A rate must be the counted successes over the drawn repetitions."""
    result = falsification.network_falsification(**TINY)
    repetitions = result["repetitions"]

    null_flags = sum(1 for run in result["null"]["runs"] if run["meets_minimum_gain"])
    recovery_flags = sum(1 for run in result["recovery"]["runs"] if run["meets_minimum_gain"])
    assert result["null"]["false_positive_count"] == null_flags
    assert result["recovery"]["recovery_count"] == recovery_flags
    assert result["null"]["false_positive_rate"] == pytest.approx(null_flags / repetitions)
    assert result["recovery"]["power"] == pytest.approx(recovery_flags / repetitions)
    assert result["false_positive_rate_at_null"] == result["null"]["false_positive_rate"]

    for block in ("null", "recovery"):
        interval = result[block]["interval"]
        assert (
            interval["lower"]
            <= result[block]["false_positive_rate" if block == "null" else "power"]
        )
        assert (
            interval["upper"]
            >= result[block]["false_positive_rate" if block == "null" else "power"]
        )
    assert result["null"]["interval"]["method"] == "wilson score interval"


def test_a_finite_repetition_count_cannot_assert_power():
    """The audit must refuse to certify a rate its sample cannot support.

    With a handful of repetitions no one-sided bound can clear 0.8 power or sit
    under a 0.1 false-positive ceiling, so the audit stays explicitly
    inconclusive and names both shortfalls rather than reporting a bare rate.
    """
    result = falsification.network_falsification(**TINY)
    assert result["status"] == "inconclusive"
    assert result["inconclusive_reasons"]
    joined = " ".join(result["inconclusive_reasons"])
    assert "null false-positive rate is not bounded below" in joined
    assert "recovery is not bounded above" in joined
    assert result["recovery"]["meets_target_power"] is False
    assert result["null"]["bounded_below_ceiling"] is False
    # Status is exactly the absence of a stated shortfall.
    assert result["status"] == ("ok" if not result["inconclusive_reasons"] else "inconclusive")


def test_power_is_counted_from_recovery_and_never_from_mechanism_metadata():
    """Reported power must be an observed count, not a declared property.

    The communication scenario declares a transmission edge by construction, so
    reading ``attrs`` would always report signal. The audit's power has to track
    the counted threshold crossings instead, and must not exceed one.
    """
    result = falsification.network_falsification(**TINY)
    declared = simulation.simulate_scenario(
        "communication", seed=result["seeds"][0], n_events=60
    ).attrs["ground_truth"]
    assert declared["communication"] is True
    assert declared["communication_edges"][0]["gain"] > 0.0

    counted = result["recovery"]["recovery_count"] / result["repetitions"]
    assert result["recovery"]["power"] == pytest.approx(counted)
    assert 0.0 <= result["recovery"]["power"] <= 1.0
    assert 0.0 <= result["null"]["false_positive_rate"] <= 1.0
    assert "observed recovery count" in result["power_source"]
    # At these event counts the declared edge does not translate into a
    # saturated rate, which is what makes the count informative.
    assert result["recovery"]["recovery_count"] <= result["repetitions"]


def test_audit_is_deterministic_for_fixed_seeds():
    """The same request must reproduce the same rates and seed list."""
    first = falsification.network_falsification(**TINY)
    second = falsification.network_falsification(**TINY)
    assert first["seeds"] == second["seeds"]
    assert first["null"]["false_positive_count"] == second["null"]["false_positive_count"]
    assert first["recovery"]["recovery_count"] == second["recovery"]["recovery_count"]
    assert first["status"] == second["status"]


def test_release_level_paired_interval_is_clustered_by_release():
    """The paired contrast is reported over releases, not over raw rows."""
    result = falsification.network_falsification(**TINY)
    for kind in ("null", "recovery"):
        interval = result["release_level_intervals"][kind]
        assert interval["status"] in {"ok", "degenerate", "inconclusive"}
        assert interval["n_clusters"] > 0
        if interval["lower"] is not None:
            assert interval["lower"] <= interval["upper"]
    assert result["event_counts"]["per_repetition_test_events"]
    assert all(count > 0 for count in result["event_counts"]["per_repetition_test_events"])
    assert result["event_counts"]["per_repetition_test_clusters"]


def test_invalid_settings_are_rejected():
    with pytest.raises(falsification.FalsificationError):
        falsification.network_falsification(n_events=2, repetitions=3)
    with pytest.raises(falsification.FalsificationError):
        falsification.network_falsification(n_events=60, repetitions=1)
    with pytest.raises(falsification.FalsificationError):
        falsification.network_falsification(n_events=60, repetitions=3, minimum_mae_gain=0.0)
    with pytest.raises(falsification.FalsificationError):
        falsification.network_falsification(n_events=60, repetitions=3, max_false_positive_rate=1.5)
    with pytest.raises(falsification.FalsificationError):
        falsification.network_falsification(n_events=60, repetitions=3, target_power=0.0)


def test_falsification_payload_satisfies_the_promotion_gate_identity():
    """A real audit payload must line up with the gate's identity check.

    This closes the loop between the two modules: the fields the gate insists on
    are exactly the fields the audit emits, so a genuine audit is admissible once
    its repetition count is sufficient, and no hand-written dict is needed.
    """
    result = falsification.network_falsification(**TINY)
    assert result["horizon_seconds"]
    frame = simulation.primary_target(
        simulation.simulate_scenario(
            "communication",
            seed=result["seeds"][0],
            n_events=result["n_events"],
            horizons=result["horizon_seconds"],
            forecast_delay=result["prediction_delay_seconds"],
        )
    )
    record = models.nested_comparison(
        frame,
        seed=result["seeds"][0],
        kinds=("news", "network"),
        minimum_mae_gain=result["minimum_mae_gain"],
    ).as_record()
    promotion = record["promotion"]
    assert promotion["minimum_mae_gain"] == pytest.approx(result["minimum_mae_gain"])
    assert promotion["time_settings"]["horizon_seconds"] == result["horizon_seconds"]
    assert promotion["time_settings"]["prediction_delay_seconds"] == pytest.approx(
        result["prediction_delay_seconds"]
    )
    assert record["sample"]["n_events_total"] == result["n_events"]

    # The identity fields all match, so an audit that had certified its rates
    # would be accepted.
    certified = dict(result, status="ok")
    accepted = models.nested_comparison(
        frame,
        seed=result["seeds"][0],
        kinds=("news", "network"),
        minimum_mae_gain=result["minimum_mae_gain"],
        null_assessment=certified,
    ).as_record()
    assert accepted["promotion"]["null_assessment"]["mismatch_reasons"] == []
    assert accepted["promotion"]["criteria_met"]["null_assessment_matches_and_ok"] is True

    # The same payload at its real, uncertified status is refused.
    refused = models.nested_comparison(
        frame,
        seed=result["seeds"][0],
        kinds=("news", "network"),
        minimum_mae_gain=result["minimum_mae_gain"],
        null_assessment=result,
    ).as_record()
    assert refused["promotion"]["criteria_met"]["null_assessment_matches_and_ok"] is False
    assert refused["promotion"]["status"] == "gated"
