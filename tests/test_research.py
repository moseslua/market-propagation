"""Acceptance tests for the simulator, forecast ladder and statistical suite.

These defend the plan's statistical acceptance criteria: scenario processes are
genuinely distinct and carry explicit ground truth, the model ladder keeps one
common held-out sample with train-only transforms and validation-only selection,
predictions stay bounded and distinct from terminal binary scores, the
common-news-plus-delay null does not promote a network model while genuine
communication does, and uncertainty is event-clustered rather than row-counted.

Every scenario assertion is a property of the generated process, so a scenario
cannot pass by being an alias of another one.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import pytest

import market_propagation.coherence as coherence
import market_propagation.evaluation as evaluation
import market_propagation.models as models
import market_propagation.simulation as simulation

REQUIRED_SCENARIOS = {
    "shared_news_delay",
    "communication",
    "heterogeneous_sensitivity",
    "omitted_shock",
    "opposing_sign",
    "spread_only",
    "later_reversal",
    "rule_mismatch",
    "resolution_pause",
    "coarse_sampling",
    "dropped_messages",
}


@pytest.fixture(scope="module")
def talk() -> pd.DataFrame:
    return simulation.simulate_scenario("communication", seed=101, n_events=12)


@pytest.fixture(scope="module")
def null() -> pd.DataFrame:
    return simulation.simulate_scenario("shared_news_delay", seed=101, n_events=12)


def _projection_panel(frame: pd.DataFrame) -> pd.DataFrame:
    """Event panel rows for local projections: valid primary-target rows only."""
    panel = simulation.primary_target(frame)
    panel = panel.loc[panel["valid"]].copy()
    panel["response"] = panel["target"]
    panel["response"] = panel["response"] * panel["orientation_sign"]
    return panel


def test_all_named_scenarios_are_registered():
    assert set(simulation.scenario_names()) == REQUIRED_SCENARIOS
    assert len(simulation.scenario_names()) == len(set(simulation.scenario_names()))


def test_forecast_table_has_the_frozen_columns(null):
    for column in simulation.FORECAST_COLUMNS:
        assert column in null.columns
    assert not null.columns.duplicated().any()
    assert "exclusion_reason" in null.columns and "cohort" in null.columns
    assert null["valid"].dtype == bool
    assert null["prediction_time"].dt.tz is not None


def test_no_feature_comes_from_after_the_forecast_cutoff(null):
    assert (null["max_input_available_time"] <= null["prediction_time"]).all()


def test_target_is_a_bounded_future_change(null):
    assert null["target"].abs().max() <= 1.0
    valid = null.loc[null["valid"]]
    span = (valid["target_available_time"] - valid["prediction_time"]).dt.total_seconds()
    assert (span == valid["horizon_seconds"]).all()


def test_truth_only_quantities_are_not_feature_columns():
    assert set(simulation.TRUTH_ONLY_COLUMNS).isdisjoint(simulation.FORECAST_COLUMNS)
    assert "transmitted_signal" in simulation.TRUTH_ONLY_COLUMNS


def test_simulation_is_seed_deterministic():
    first = simulation.simulate_scenario("communication", seed=7, n_events=6)
    second = simulation.simulate_scenario("communication", seed=7, n_events=6)
    pd.testing.assert_frame_equal(first, second)
    other = simulation.simulate_scenario("communication", seed=8, n_events=6)
    assert not np.allclose(first["target"], other["target"])


def test_output_is_invariant_to_row_permutation():
    frame = simulation.simulate_scenario("communication", seed=9, n_events=8)
    shuffled = frame.sample(frac=1.0, random_state=1).reset_index(drop=True)
    key = ["event_id", "contract_id", "horizon_seconds"]
    restored = shuffled.sort_values(key, kind="stable").reset_index(drop=True)
    expected = frame.sort_values(key, kind="stable").reset_index(drop=True)
    columns = list(frame.columns)
    pd.testing.assert_frame_equal(restored[columns], expected[columns])


def test_null_transmission_is_absent_in_the_generated_rows():
    """The null's defining property must be visible in the data, not just declared.

    Reading ``attrs['ground_truth']['communication'] is False`` only restates
    what the spec says. The load-bearing fact is that the generated rows carry no
    delivered signal under the null while the communication process does, so the
    falsification contrast is measured against a genuinely edge-free process.
    """
    null = simulation.simulate_scenario("shared_news_delay", seed=101, n_events=12)
    talk = simulation.simulate_scenario("communication", seed=101, n_events=12)
    assert np.nanmax(np.abs(null["transmitted_signal"])) == 0.0
    assert np.nanmax(np.abs(talk["transmitted_signal"])) > 0.0
    # The target venue's endpoint carries the delivered signal, which is what the
    # forecast comparison is asked to recover.
    assert not np.allclose(null["target"], talk["target"])


def test_scenarios_are_distinct_processes_not_aliases():
    """Every registered scenario produces observably different rows.

    Distinctness is checked on generated data rather than on declared metadata,
    so two scenarios that merely describe themselves differently but generate the
    same process would still fail this test.
    """
    signatures: dict[str, np.ndarray] = {}
    for name in simulation.scenario_names():
        frame = simulation.simulate_scenario(name, seed=11, n_events=10)
        observed = np.nan_to_num(
            np.asarray(
                [
                    float(frame["target"].median()),
                    float(frame["target"].std(ddof=0)),
                    float((~frame["valid"]).mean()),
                    float(len(frame)),
                ],
                dtype=np.float64,
            )
        )
        signatures[name] = observed
    names = sorted(signatures)
    for index, left in enumerate(names):
        for right in names[index + 1 :]:
            assert not np.array_equal(signatures[left], signatures[right]), (
                f"scenarios {left!r} and {right!r} generated indistinguishable rows at one seed"
            )


def test_controlled_comparison_shares_nuisance_draws():
    null = simulation.simulate_scenario("shared_news_delay", seed=13, n_events=8)
    talk = simulation.simulate_scenario("communication", seed=13, n_events=8)
    # The observed common news, the prediction clocks and the withheld common
    # shock are identical across the pair, so the comparison isolates the
    # declared mechanism rather than the noise. These are the draws the
    # matched-nuisance contract covers.
    for column in (
        "shock",
        "delayed_shock",
        "prediction_time",
        "event_time",
        "event_id",
        "latent_move",
        "omitted_common_shock",
    ):
        pd.testing.assert_series_equal(null[column], talk[column])
    # Both frames expose exactly the same rows, so the comparison sample is
    # matched and only the target process differs.
    keys = ["event_id", "contract_id", "horizon_seconds"]
    assert len(null) == len(talk)
    pd.testing.assert_frame_equal(
        null[keys].reset_index(drop=True), talk[keys].reset_index(drop=True)
    )
    # The declared transmission edge changes the target; the null has none.
    assert not np.allclose(null["target"], talk["target"])
    assert null.attrs["ground_truth"]["communication"] is False
    assert talk.attrs["ground_truth"]["communication"] is True
    assert talk.attrs["ground_truth"]["communication_edges"][0]["gain"] > 0.0


def test_spread_only_leaves_midpoints_quiet_and_spreads_wide():
    frame = simulation.simulate_scenario("spread_only", seed=17, n_events=24)
    news = simulation.simulate_scenario("shared_news_delay", seed=17, n_events=24)
    quiet = frame.loc[frame["valid"], "target"].abs().median()
    responding = news.loc[news["valid"], "target"].abs().median()
    assert quiet < responding
    # the latent still drifts, so the target is small rather than exactly zero
    assert frame.loc[frame["valid"], "target"].abs().max() < 0.05
    assert frame.attrs["ground_truth"]["spread_only"] is True
    quotes = simulation.simulate_quotes("spread_only", seed=17, n_events=6)
    offset = (quotes["observation_time"] - quotes["event_time"]).dt.total_seconds()
    inside = quotes.loc[offset.between(0, 120), "spread"]
    outside = quotes.loc[offset.between(-150, -60), "spread"]
    assert inside.median() > outside.median() * 2.0


def test_opposing_sign_moves_opposite_directions_on_one_shock():
    frame = simulation.simulate_scenario("opposing_sign", seed=19, n_events=30)
    up = simulation.primary_target(frame, cohort="downstream")
    down = simulation.primary_target(frame, cohort="complement")
    merged = up.merge(down, on=["event_id", "horizon_seconds"], suffixes=("_up", "_down"))
    horizon = merged.loc[merged["horizon_seconds"] == 900]
    assert len(horizon) >= 20
    assert np.corrcoef(horizon["target_up"], horizon["target_down"])[0, 1] < 0.0


def test_later_reversal_flips_sign_between_finite_horizons():
    frame = simulation.simulate_scenario("later_reversal", seed=23, n_events=30)
    target = simulation.primary_target(frame)
    valid = target.loc[target["valid"]]
    early = valid.loc[valid["horizon_seconds"] == 60].groupby("event_id")["target"].mean()
    late = valid.loc[valid["horizon_seconds"] == 900].groupby("event_id")["target"].mean()
    shared = early.index.intersection(late.index)
    assert len(shared) >= 5
    flips = np.sign(early.loc[shared]) != np.sign(late.loc[shared])
    assert flips.mean() > 0.5
    assert early.loc[shared].mean() < late.loc[shared].mean()


def test_resolution_pause_masks_spanning_windows():
    frame = simulation.simulate_scenario("resolution_pause", seed=29, n_events=8)
    invalid = frame.loc[~frame["valid"]]
    assert not invalid.empty
    assert invalid["exclusion_reason"].str.contains("halted_during_window").any()
    quotes = simulation.simulate_quotes("resolution_pause", seed=29, n_events=4)
    halted = quotes.loc[~quotes["valid"]]
    assert not halted.empty and set(halted["exclusion_reason"]) == {"halted"}


def test_coarse_sampling_quantizes_grid_and_tick():
    quotes = simulation.simulate_quotes("coarse_sampling", seed=31, n_events=8)
    offset = (quotes["observation_time"] - quotes["event_time"]).dt.total_seconds()
    assert np.allclose(np.mod(offset, 30.0), 0.0)
    assert np.allclose(quotes["midpoint"], np.round(quotes["midpoint"] / 0.05) * 0.05, atol=1e-9)


def test_dropped_messages_shows_observed_gaps():
    quotes = simulation.simulate_quotes("dropped_messages", seed=37, n_events=8)
    beta = quotes.loc[quotes["venue"] == "beta"]
    assert beta["dropped_previous"].any()


def test_quotes_are_well_formed_and_separate_from_the_forecast_table():
    quotes = simulation.simulate_quotes("communication", seed=41, n_events=4)
    assert (quotes["bid"] <= quotes["ask"]).all()
    assert set(quotes["cohort"]) <= {"downstream", "complement", "control", "direct"}
    assert set(quotes["bid"]).isdisjoint({"target", "own_lag", "shock"})


def test_invalid_arguments_are_rejected():
    with pytest.raises(ValueError):
        simulation.simulate_scenario("does_not_exist")
    with pytest.raises(ValueError):
        simulation.simulate_scenario("communication", n_events=0)
    with pytest.raises(ValueError):
        simulation.simulate_scenario("communication", post_seconds=1.0)


def test_primary_target_requires_a_scenario_frame(null):
    with pytest.raises(ValueError):
        simulation.primary_target(null, cohort="nonsense")
    with pytest.raises(ValueError):
        simulation.primary_target(null.drop(columns=["cohort"]))


def test_ladder_is_nested_and_neighbours_are_network_only():
    specs = models.FEATURE_SPECS
    assert set(specs["no_change"]) < set(specs["own"]) < set(specs["news"]) < set(specs["network"])
    assert "neighbor_lag" in specs["network"]
    assert "neighbor_lag" not in specs["news"]
    assert {"shock", "delayed_shock", "own_lag"}.issubset(specs["news"])
    # The network set adds exactly the admissible neighbour columns that are not
    # exact logical restatements of a column already in the design.
    collapsed = set(models.REDUNDANT_FEATURE_COLUMNS["network"])
    assert set(specs["network"]) - set(specs["news"]) == {"neighbor_lag", "neighbor_lag_control"}
    assert collapsed & set(specs["network"]) == set()
    assert collapsed.issubset(set(simulation.NEIGHBOR_COLUMNS))


def test_current_price_conditions_every_non_trivial_kind():
    """The target is a future change, so the current level is a real regressor.

    Conditioning on the baseline level lets the ladder express a level-dependent
    response instead of forcing every kind through one fixed anchor. The gate
    therefore has to be able to see the coefficient, not just the bounded link.
    """
    for kind in ("own", "news", "network"):
        assert "current_price" in models.FEATURE_SPECS[kind], kind
    assert "current_price" not in models.FEATURE_SPECS["no_change"]


def test_current_price_is_fitted_and_changes_the_prediction(talk):
    """A fitted coefficient on the baseline level must reach the forecast.

    Fitting ``current_price`` and then ignoring it at prediction time would make
    the new covariate decorative, so the check is that varying only the level
    moves the predicted change.
    """
    model = models.fit(talk, {"kind": "news", "validation": talk})
    assert "current_price" in model.feature_names
    assert model.coefficients["current_price"] != 0.0
    assert set(model.feature_means) == set(model.feature_names)
    low = talk.assign(current_price=talk["current_price"] - 0.05)
    high = talk.assign(current_price=talk["current_price"] + 0.05)
    low_report = models.evaluate(model, low)
    high_report = models.evaluate(model, high)
    assert low_report["n_rows"] == high_report["n_rows"] > 0
    assert not np.allclose(
        [entry["predicted"] for entry in low_report["predictions"]],
        [entry["predicted"] for entry in high_report["predictions"]],
    )


def test_exact_logical_complement_is_collapsed_out_of_the_design(talk):
    """An exact restatement of another column must not be fitted beside it.

    The raw neighbour column stays in the forecast table for audit, but the
    design keeps one coefficient for the pair. A genuinely distinct control is
    preserved rather than dropped with it.
    """
    model = models.fit(talk, {"kind": "network"})
    assert "neighbor_lag_complement" not in model.feature_names
    assert "neighbor_lag" in model.feature_names
    assert "neighbor_lag_control" in model.feature_names
    # The collapse is a declared contract, not a numeric accident.
    assert set(model.feature_names) == set(models.FEATURE_SPECS["network"])
    assert "neighbor_lag_complement" in talk.columns

    # A caller that declares every raw neighbour column still gets the redundant
    # one collapsed out, and the collapse is recorded rather than silent.
    declared = models.fit(
        talk,
        {
            "kind": "network",
            "columns": list(simulation.NEIGHBOR_COLUMNS[:1]) + list(simulation.NEIGHBOR_COLUMNS),
        },
    )
    assert "neighbor_lag_complement" not in declared.feature_names
    assert "neighbor_lag_control" in declared.feature_names
    assert any("collapsed exact logical complement" in note for note in declared.notes)


def test_fit_records_transforms_parameters_and_cutoff(talk):
    model = models.fit(talk, {"kind": "network"})
    record = model.as_record()
    assert record["feature_names"] == list(models.FEATURE_SPECS["network"])
    assert set(record["feature_means"]) == set(models.FEATURE_SPECS["network"])
    assert all(scale > 0.0 for scale in record["feature_scales"].values())
    assert record["train_cutoff"]
    assert record["n_train_events"] == talk["event_id"].nunique()
    assert "future change" in record["target_definition"]
    assert record["bounded_link"]["scale"] > 0.0


def test_no_change_model_has_no_coefficients(talk):
    model = models.fit(talk, {"kind": "no_change"})
    assert model.feature_names == ()
    assert model.coefficients == {}
    assert not model.is_fitted


def test_predictions_are_bounded_and_clipping_is_reported(talk):
    model = models.fit(talk, {"kind": "network"})
    report = models.evaluate(model, talk)
    for entry in report["predictions"]:
        assert 0.0 <= entry["predicted_level"] <= 1.0
        assert abs(entry["predicted"]) <= 1.0
    assert report["clipping"]["data_selected"] is False
    assert report["clipping"]["tanh_scale"] > 0.0


def test_evaluate_scores_probability_points_not_terminal_payouts(talk):
    model = models.fit(talk, {"kind": "own"})
    report = models.evaluate(model, talk)
    assert "probability points" in report["prediction_scope"]
    assert report["mae"] >= 0.0 and report["mse"] >= 0.0
    assert report["mae"] <= report["rmse"] + 1e-12


def test_evaluate_rejects_a_column_the_model_was_not_fit_on(talk):
    model = models.fit(talk, {"kind": "own"})
    assert model.feature_names == ("own_lag", "current_price")
    with pytest.raises(models.ForecastDataError):
        models.evaluate(model, talk.drop(columns=["own_lag"]))
    report = models.evaluate(model, talk.drop(columns=["neighbor_lag"]))
    assert report["n_rows"] > 0


def test_nested_comparison_uses_one_identical_held_out_sample(talk):
    record = models.nested_comparison(talk, seed=3).as_record()
    assert record["common_sample_violations"] == []
    event_sets = {kind: tuple(p["event_ids"]) for kind, p in record["evaluations"].items()}
    assert len(set(event_sets.values())) == 1
    assert len({row["n_rows"] for row in record["scores"]}) == 1


def test_nested_comparison_selects_penalty_on_validation_only(talk):
    result = models.nested_comparison(talk, seed=4)
    assert result.models["network"]["selected_on"] == "validation"
    assert result.models["network"]["parameters"]["alpha"] in models.DEFAULT_ALPHAS
    assert result.folds.train_cutoff < result.folds.validation_cutoff


def test_folds_partition_whole_releases(talk):
    result = models.nested_comparison(talk, seed=5)
    train = set(result.folds.train_events)
    validation = set(result.folds.validation_events)
    test = set(result.folds.test_events)
    assert not train & validation and not train & test and not validation & test
    assert train | validation | test == set(talk["event_id"])
    for _kind in ("train", "validation", "test"):
        for model in result.models.values():
            assert set(model["train_event_ids"]).isdisjoint(test)


def test_news_and_network_share_every_baseline_covariate_and_grid(talk):
    result = models.nested_comparison(talk, seed=6, kinds=("news", "network"))
    news = set(result.models["news"]["feature_names"])
    network = set(result.models["network"]["feature_names"])
    assert network - news == {"neighbor_lag", "neighbor_lag_control"}
    assert news < network
    # Neither kind is handed a tuning advantage: both select from the same grid
    # on the same validation rows.
    for kind in ("news", "network"):
        assert result.models[kind]["selected_on"] == "validation"
        assert result.models[kind]["parameters"]["alpha"] in models.DEFAULT_ALPHAS
    assert set(result.models["news"]["validation_scores"]) == set(
        result.models["network"]["validation_scores"]
    )


def test_nested_comparison_defers_to_the_single_split_authority(talk):
    """Fold assignment, cutoff and purge have exactly one authority.

    The comparison must not carry a second split implementation, so its folds
    have to agree with ``evaluation.chronological_splits`` on the same sample:
    same release units, same boundaries and same purge rule.
    """
    result = models.nested_comparison(talk, seed=31, kinds=("news", "network"))
    reference = evaluation.chronological_splits(talk)
    assert result.folds.policy["split_authority"] == (
        "market_propagation.evaluation.chronological_splits"
    )
    for fold in ("train", "validation", "test"):
        assert set(getattr(result.folds, f"{fold}_events")) == set(
            reference[fold]["event_id"].unique()
        )
    assert result.folds.purged_rows == reference["test"].attrs["policy"]["purged_rows"]
    assert (
        result.folds.train_cutoff
        == pd.Timestamp(reference["train"].attrs["policy"]["train_cutoff"]).to_pydatetime()
    )
    assert (
        result.folds.validation_cutoff
        == pd.Timestamp(
            reference["validation"].attrs["policy"]["validation_cutoff"]
        ).to_pydatetime()
    )


def test_whole_cluster_stays_in_one_fold_across_different_event_labels(talk):
    """Cross-venue equivalents share a release even when ``event_id`` differs.

    Splitting on an event label alone would leak one release across folds, so
    the check is that a mirrored row carrying a new ``event_id`` but the same
    ``cluster_id`` never lands in a different fold from its twin.
    """
    mirror = talk.copy()
    mirror["event_id"] = mirror["event_id"] + "-mirror"
    mirror["contract_id"] = mirror["contract_id"] + "-mirror"
    combined = pd.concat([talk, mirror], ignore_index=True)
    result = models.nested_comparison(combined, seed=33, kinds=("news", "network"))
    assigned: dict[str, str] = {}
    for fold in ("train", "validation", "test"):
        for cluster in getattr(result.folds, f"{fold}_events"):
            assert assigned.setdefault(str(cluster), fold) == fold
    # The fold event sets are drawn from one assignment, and a mirrored row can
    # never pull its twin's release into a second fold.
    train = set(result.folds.train_events)
    validation = set(result.folds.validation_events)
    test = set(result.folds.test_events)
    assert not train & validation and not train & test and not validation & test
    assert len(assigned) == len(train) + len(validation) + len(test)
    assert set(assigned.values()) == {"train", "validation", "test"}


def test_minimum_mae_gain_is_propagated_from_the_caller(talk):
    """The gate threshold is the caller's value, not a buried constant.

    The registered study threshold is 0.005, so a comparison asked for that
    value must report it, and a stricter request must change the verdict a
    marginal reduction would receive.
    """
    lenient = models.nested_comparison(
        talk, seed=41, kinds=("news", "network"), minimum_mae_gain=0.005
    ).as_record()
    assert lenient["promotion"]["minimum_mae_gain"] == pytest.approx(0.005)
    assert lenient["promotion"]["criteria_met"]["meets_minimum_mae_gain"] == (
        lenient["promotion"]["mae_reduction"] >= 0.005
    )

    strict = models.nested_comparison(
        talk, seed=41, kinds=("news", "network"), minimum_mae_gain=0.9
    ).as_record()
    assert strict["promotion"]["minimum_mae_gain"] == pytest.approx(0.9)
    assert strict["promotion"]["criteria_met"]["meets_minimum_mae_gain"] is False
    assert strict["promotion"]["status"] == "gated"
    # Same held-out evidence under both thresholds: only the bar moved.
    assert strict["promotion"]["mae_reduction"] == lenient["promotion"]["mae_reduction"]
    assert strict["promotion"]["baseline_mae"] == lenient["promotion"]["baseline_mae"]

    with pytest.raises(models.ForecastDataError):
        models.nested_comparison(talk, seed=41, minimum_mae_gain=0.0)


def test_communication_encodes_a_real_transmission_edge():
    """The communication frame carries a genuine, declared transmission mechanism.

    No end-to-end MAE separation is asserted here. Whether the network model
    recovers the edge at a given event count is a finite-sample question answered
    by ``network_falsification`` over a fixed seed list, not by one seed's score,
    and asserting it from a single draw is exactly how a lucky seed becomes a
    finding. What is checked here is the mechanism the audit is asked to recover:
    a non-trivial delivered signal that is absent under the null.
    """
    talk = simulation.simulate_scenario("communication", seed=71, n_events=60)
    null = simulation.simulate_scenario("shared_news_delay", seed=71, n_events=60)
    truth = talk.attrs["ground_truth"]
    assert truth["communication"] is True
    assert truth["private_component"]["sigma"] > 0.0
    edge = truth["communication_edges"][0]
    assert edge["gain"] > 0.0 and edge["lag_seconds"] > 0.0
    # the transmitted signal is present in the truth-only metadata and non-trivial
    assert np.nanmax(np.abs(talk["transmitted_signal"])) > 0.0
    assert np.nanmax(np.abs(null["transmitted_signal"])) == 0.0
    # transmission changes the target venue, so the two scenarios are not aliases
    assert not np.allclose(null["target"], talk["target"])
    assert np.nanmax(np.abs(null["target"] - talk["target"])) > 0.0


def test_locked_test_rows_cannot_change_the_fit_or_the_selection(talk):
    """Corrupting the held-out fold must not move transforms, coefficients or alpha.

    This is the leakage regression the estimator identity rests on: scales and
    the validation-selected penalty are fit without the locked test, so rewriting
    test-row features changes only how the frozen model scores, never what it is.
    """
    base = models.nested_comparison(talk, seed=51, kinds=("news", "network"))
    test_events = set(base.folds.test_events)
    assert test_events
    scrambled = talk.copy()
    held_out = scrambled["event_id"].isin(test_events)
    assert bool(held_out.any())
    for column in base.sample["common_columns"]:
        scrambled.loc[held_out, column] = scrambled.loc[held_out, column] * -7.5 + 0.31
    again = models.nested_comparison(scrambled, seed=51, kinds=("news", "network"))

    assert base.folds.train_events == again.folds.train_events
    assert base.folds.validation_events == again.folds.validation_events
    assert base.folds.test_events == again.folds.test_events
    for kind in ("news", "network"):
        left = base.models[kind]
        right = again.models[kind]
        assert left["feature_means"] == right["feature_means"]
        assert left["feature_scales"] == right["feature_scales"]
        assert left["coefficients"] == right["coefficients"]
        assert left["intercept"] == right["intercept"]
        assert left["parameters"]["alpha"] == right["parameters"]["alpha"]
        assert left["selected_on"] == "validation"
        # The frozen model is unchanged, so only the held-out score may move.
        assert base.evaluations[kind]["mae"] != again.evaluations[kind]["mae"]


def test_no_hardcoded_gate_threshold_remains():
    """The old incidental 0.001 bar must be gone from the gate contract.

    The registered study threshold is 0.005; a leftover 0.001 would silently
    weaken every gate decision, so the gate's declared default is checked and the
    retired key is asserted absent rather than merely unused.
    """
    assert models.PROMOTION_GATE["default_minimum_mae_gain"] == pytest.approx(0.005)
    assert "smallest_relevant_mae_reduction" not in models.PROMOTION_GATE
    # The threshold is a parameter, not a module-level constant that a caller
    # could forget to read.
    assert not any(name == "SMALLEST_RELEVANT_MAE_REDUCTION" for name in dir(models))


def test_null_reduction_alone_never_promotes_the_network_model():
    """A held-out reduction under the null is not evidence of propagation.

    This is the scientific safeguard behind the common-news-plus-delay null: the
    network model may beat the shared-news baseline on a no-communication process,
    and the gate must still refuse to promote it without a null assessment.
    """
    null = simulation.simulate_scenario("shared_news_delay", seed=73, n_events=60)
    record = models.nested_comparison(null, seed=9, kinds=("news", "network")).as_record()
    assert not record["common_sample_violations"]
    assert record["promotion"]["criteria_met"]["beats_baseline"] in {True, False}
    assert record["promotion"]["criteria_met"]["null_assessment_matches_and_ok"] is False
    assert record["promotion"]["status"] == "gated"
    assert record["promotion"]["null_assessment"]["status"] == "not_supplied"


def _matching_null_assessment(record: dict[str, Any], *, rate: float = 0.05) -> dict[str, Any]:
    """A null assessment that genuinely matches one comparison's identity."""
    promotion = record["promotion"]
    return {
        "status": "ok",
        "estimator": models.FORECAST_ESTIMATOR["estimator"],
        "metric": models.FORECAST_ESTIMATOR["metric"],
        "minimum_mae_gain": promotion["minimum_mae_gain"],
        "horizon_seconds": list(promotion["time_settings"]["horizon_seconds"]),
        "prediction_delay_seconds": promotion["time_settings"]["prediction_delay_seconds"],
        "n_events": record["sample"]["n_events_total"],
        "false_positive_rate_at_null": rate,
    }


def test_promotion_requires_a_matching_null_assessment():
    """The gate unlocks only on an assessment from this same pipeline.

    An assessment must name this comparison's estimator, metric, MAE-gain
    threshold, horizon, prediction delay and event count. A foreign one is
    rejected, so the network model cannot be promoted on statistics that
    describe a different question.
    """
    frame = simulation.simulate_scenario("communication", seed=71, n_events=60)

    # At the registered bar this frame does not clear the MAE criterion, so the
    # gate is closed on that ground rather than on the assessment.
    at_registered = models.nested_comparison(
        frame, seed=8, kinds=("news", "network"), minimum_mae_gain=0.005
    ).as_record()
    assert at_registered["promotion"]["criteria_met"]["meets_minimum_mae_gain"] is False
    assert at_registered["promotion"]["status"] == "gated"

    # A bar this frame does clear isolates the assessment criterion: the same
    # frame then differs only in which null assessment it was handed.
    at_clearable = models.nested_comparison(
        frame, seed=8, kinds=("news", "network"), minimum_mae_gain=0.0001
    ).as_record()
    assert at_clearable["promotion"]["criteria_met"]["meets_minimum_mae_gain"] is True
    assert at_clearable["promotion"]["status"] == "gated"
    assert at_clearable["promotion"]["null_assessment"]["status"] == "not_supplied"

    promoted = models.nested_comparison(
        frame,
        seed=8,
        kinds=("news", "network"),
        minimum_mae_gain=0.0001,
        null_assessment=_matching_null_assessment(at_clearable),
    ).as_record()
    assert promoted["promotion"]["null_assessment"]["mismatch_reasons"] == []
    assert promoted["promotion"]["criteria_met"]["null_assessment_matches_and_ok"] is True
    assert promoted["promotion"]["status"] == "promoted"
    assert promoted["promotion"]["null_assessment"]["false_positive_rate"] == 0.05
    assert promoted["promotion"]["null_assessment"]["matches_this_estimator"] is True


def test_unrelated_null_assessment_cannot_unlock_the_gate():
    """A response-slope power report is not a forecast-null audit.

    This is the trap the repair exists to close, so the frame is chosen to clear
    the MAE criterion: with that bar satisfied, only the assessment identity
    stands between the candidate and promotion, and a slope-unit rate must not
    supply it.
    """
    frame = simulation.simulate_scenario("communication", seed=71, n_events=60)
    record = models.nested_comparison(
        frame, seed=8, kinds=("news", "network"), minimum_mae_gain=0.0001
    ).as_record()
    assert record["promotion"]["criteria_met"]["meets_minimum_mae_gain"] is True

    slope_report = evaluation.power_assessment(
        n_events=60, repetitions=20, samples=20, sample_grid=(12,)
    )
    assert slope_report["estimator"] == "between_release_response_slope"
    assert slope_report["metric"] == "response_slope"
    gated = models.nested_comparison(
        frame,
        seed=8,
        kinds=("news", "network"),
        minimum_mae_gain=0.0001,
        null_assessment=slope_report,
    ).as_record()
    assert gated["promotion"]["status"] == "gated"
    assert gated["promotion"]["criteria_met"]["null_assessment_matches_and_ok"] is False
    reasons = " ".join(gated["promotion"]["null_assessment"]["mismatch_reasons"])
    assert "estimator" in reasons and "metric" in reasons

    slope_null = evaluation.null_false_positive_rate(
        scenario="shared_news_delay", n_events=60, repetitions=20, samples=20, sample_grid=(12,)
    )
    assert slope_null["forecast_gate_use"].startswith("prohibited")
    still_gated = models.nested_comparison(
        frame,
        seed=8,
        kinds=("news", "network"),
        minimum_mae_gain=0.0001,
        null_assessment=slope_null,
    ).as_record()
    assert still_gated["promotion"]["status"] == "gated"


def test_null_assessment_must_match_threshold_settings_and_event_count():
    """Each identity field is load-bearing on its own."""
    frame = simulation.simulate_scenario("communication", seed=71, n_events=60)
    record = models.nested_comparison(
        frame, seed=8, kinds=("news", "network"), minimum_mae_gain=0.0001
    ).as_record()
    good = _matching_null_assessment(record)
    accepted = models.nested_comparison(
        frame,
        seed=8,
        kinds=("news", "network"),
        minimum_mae_gain=0.0001,
        null_assessment=good,
    ).as_record()
    assert accepted["promotion"]["status"] == "promoted"
    assert accepted["promotion"]["null_assessment"]["mismatch_reasons"] == []

    for field, wrong in (
        ("minimum_mae_gain", 0.001),
        ("horizon_seconds", [60]),
        ("prediction_delay_seconds", 60.0),
        ("n_events", record["sample"]["n_events_total"] + 1),
        ("status", "inconclusive"),
        ("metric", "response_slope"),
    ):
        broken = dict(good)
        broken[field] = wrong
        gated = models.nested_comparison(
            frame,
            seed=8,
            kinds=("news", "network"),
            minimum_mae_gain=0.0001,
            null_assessment=broken,
        ).as_record()
        assert gated["promotion"]["status"] == "gated", field
        assert gated["promotion"]["criteria_met"]["null_assessment_matches_and_ok"] is False
        reasons = " ".join(gated["promotion"]["null_assessment"]["mismatch_reasons"])
        assert reasons, field


def test_null_and_communication_are_held_to_the_same_gate():
    """Both processes are compared under one rule, without asserting a winner.

    The common-news-plus-delay null is expected to produce an apparent
    cross-market edge, so a test that simply requires the null's reduction to be
    smaller would be asserting a property this simulator does not have. What is
    checked here is that the two processes are evaluated identically and that
    neither can be promoted without the null assessment.
    """
    seeds = (73, 101)
    for seed in seeds:
        talk = simulation.simulate_scenario("communication", seed=seed, n_events=60)
        null = simulation.simulate_scenario("shared_news_delay", seed=seed, n_events=60)
        for frame in (talk, null):
            record = models.nested_comparison(frame, seed=9, kinds=("news", "network")).as_record()
            assert record["common_sample_violations"] == []
            assert record["promotion"]["status"] == "gated"
            assert record["promotion"]["criteria_met"]["null_assessment_matches_and_ok"] is False
            assert record["metric"] == "probability_point_mae"
            assert record["promotion"]["time_settings"]["horizon_seconds"]


def test_promotion_gate_names_one_primary_metric():
    gate = models.PROMOTION_GATE
    assert gate["metric"] == "probability_point_mae"
    assert gate["baseline_kind"] == "news" and gate["candidate_kind"] == "network"
    assert gate["direction"] == "lower_is_better"
    assert gate["default_minimum_mae_gain"] == pytest.approx(0.005)
    # The comparison's default threshold is the registered one, and the identity
    # constants the gate checks against are the production estimator's.
    assert models.FORECAST_ESTIMATOR["metric"] == gate["metric"]
    assert models.FORECAST_ESTIMATOR["split"] == "evaluation.chronological_splits"


def test_unknown_kind_and_empty_input_are_rejected(talk):
    with pytest.raises(models.ModelFitError):
        models.fit(talk, {"kind": "mystery"})
    with pytest.raises(models.ForecastDataError):
        models.fit(talk.iloc[0:0], {"kind": "own"})


def test_local_projections_recover_a_positive_news_response():
    frame = simulation.simulate_scenario("communication", seed=81, n_events=40)
    panel = _projection_panel(frame)
    result = models.local_projections(panel, shock_column="shock", bootstrap_samples=80)
    assert result["status"] == "ok"
    slopes = [
        cell["shock_slope"] for cell in result["cells"].values() if cell["shock_slope"] is not None
    ]
    assert slopes and all(value > 0.0 for value in slopes)


def test_local_projections_without_a_shock_report_no_slope(null):
    panel = _projection_panel(null)
    result = models.local_projections(panel)
    assert result["primary_metric"] == "mean_response"
    assert result["identification"].startswith("descriptive only")
    assert all(cell["shock_slope"] is None for cell in result["cells"].values())
    assert all(cell["shock_slope_status"] == "not_requested" for cell in result["cells"].values())


def test_local_projections_keep_event_level_means():
    frame = simulation.simulate_scenario("communication", seed=83, n_events=20)
    panel = _projection_panel(frame)
    result = models.local_projections(panel, shock_column="shock", bootstrap_samples=40)
    for cell in result["cells"].values():
        assert len(cell["event_means"]) == cell["n_events"]
        assert cell["n_clusters"] <= cell["n_events"]


def test_local_projections_flag_singleton_clusters():
    frame = simulation.simulate_scenario("communication", seed=87, n_events=10)
    panel = _projection_panel(frame)
    single = panel.loc[panel["horizon_seconds"] == 60]
    singleton = models.local_projections(single, shock_column="shock", bootstrap_samples=40)
    assert singleton["singleton_clusters"] == singleton["n_clusters_used"]
    assert "singleton" in singleton["singleton_cluster_note"]
    pooled = models.local_projections(panel, shock_column="shock", bootstrap_samples=40)
    assert pooled["singleton_clusters"] == 0


def test_local_projections_return_inconclusive_cells_for_too_few_events():
    frame = simulation.simulate_scenario("communication", seed=89, n_events=6)
    panel = _projection_panel(frame)
    result = models.local_projections(panel, shock_column="shock", min_events=50)
    assert result["status"] == "partially_inconclusive"
    for cell in result["cells"].values():
        assert cell["status"] == "inconclusive"
        assert cell["shock_slope"] is None
        assert cell["reason"]


def test_local_projections_exclude_unsignable_orientations():
    frame = simulation.simulate_scenario("opposing_sign", seed=91, n_events=20)
    panel = simulation.primary_target(frame)
    panel = panel.loc[panel["valid"]].copy()
    panel["response"] = panel["target"]
    # all-unsignable: a range or two-sided bucket cohort cannot be pooled by sign
    unsignable = panel.assign(orientation_sign=0.0)
    with pytest.raises(models.LocalProjectionError):
        models.local_projections(unsignable, shock_column="shock", bootstrap_samples=30)
    # mixed: unsignable rows are excluded and counted, the rest still pool
    mixed = panel.assign(orientation_sign=1.0)
    mixed.loc[mixed.index[:3], "orientation_sign"] = 0.0
    result = models.local_projections(mixed, shock_column="shock", bootstrap_samples=30)
    assert result["excluded"]["orientation_excluded"] == 3
    assert result["n_rows_used"] == len(mixed) - 3
    assert result["orientation_column"] == "orientation_sign"


def test_orientation_alignment_is_recorded_and_reportable():
    frame = simulation.simulate_scenario("opposing_sign", seed=93, n_events=20)
    panel = _projection_panel(frame)
    aligned = models.local_projections(panel, shock_column="shock", bootstrap_samples=30)
    raw = models.local_projections(
        panel, shock_column="shock", orient_response=False, bootstrap_samples=30
    )
    assert aligned["orient_response"] is True and raw["orient_response"] is False
    assert aligned["orientation_note"] != raw["orientation_note"]
    assert "orientation sign" in aligned["orientation_note"]


def test_local_projections_report_simultaneous_band_and_pointwise_comparison():
    frame = simulation.simulate_scenario("communication", seed=97, n_events=40)
    panel = _projection_panel(frame)
    result = models.local_projections(panel, shock_column="shock", bootstrap_samples=80)
    for curve in result["curves"].values():
        band = curve["simultaneous_band"]
        assert band["kind"] == "shock_slope"
        assert band["status"] == "ok"
        assert band["critical_value"] is not None
        assert band["method"]
        assert band["pointwise_intervals"]
        assert "cost of covering the whole curve" in band["comparison"]


def test_settling_and_overshoot_flags_are_finite():
    frame = simulation.simulate_scenario("later_reversal", seed=99, n_events=40)
    panel = _projection_panel(frame)
    result = models.local_projections(panel, shock_column="shock", bootstrap_samples=40)
    for curve in result["curves"].values():
        settling = curve["settling"]
        assert settling["tolerance"] > 0.0
        assert "not evidence of a permanent effect" in settling["note"]
        if settling["slope_status"] == "ok":
            assert settling["overshoot"] is not None
            assert settling["overshoot"] >= 0.0


def test_leave_one_event_out_sensitivity_is_present():
    frame = simulation.simulate_scenario("communication", seed=103, n_events=20)
    panel = _projection_panel(frame)
    result = models.local_projections(panel, shock_column="shock", bootstrap_samples=40)
    cells = [cell for cell in result["cells"].values() if cell["status"] == "ok"]
    assert cells
    assert any(cell["leave_one_event_out_slope"] for cell in cells)


def test_local_projections_reject_a_panel_without_a_response():
    frame = simulation.simulate_scenario("communication", seed=107, n_events=6)
    with pytest.raises(models.LocalProjectionError):
        models.local_projections(simulation.primary_target(frame))


def test_local_projections_reject_an_unknown_shock_column(null):
    panel = _projection_panel(null)
    with pytest.raises(models.LocalProjectionError):
        models.local_projections(panel, shock_column="not_a_column")


def _event_panel(frame: pd.DataFrame) -> pd.DataFrame:
    panel = _projection_panel(frame)
    panel["baseline_age_seconds"] = 1.0 + (panel["event_time"].rank() % 7)
    return panel


def test_chronological_splits_assign_whole_releases():
    frame = simulation.simulate_scenario("communication", seed=111, n_events=30)
    folds = evaluation.chronological_splits(_event_panel(frame))
    assert set(folds) == {"train", "validation", "test"}
    seen: dict[str, str] = {}
    for name, fold in folds.items():
        assert fold["split"].unique().tolist() == [name]
        assert fold["training_cutoff"].notna().all()
        for event in fold["event_id"].unique():
            assert seen.setdefault(event, name) == name
    assert set(seen.values()) == {"train", "validation", "test"}


def test_chronological_splits_keep_cross_venue_equivalents_together():
    frame = simulation.simulate_scenario("communication", seed=113, n_events=24)
    panel = _event_panel(frame)
    mirror = panel.copy()
    mirror["contract_id"] = mirror["contract_id"] + "-mirror"
    mirror["venue"] = "beta-mirror"
    mirror["response"] = mirror["response"] * -1.0
    combined = pd.concat([panel, mirror], ignore_index=True)
    folds = evaluation.chronological_splits(combined)
    for fold in folds.values():
        assert fold["event_id"].nunique() == fold["cluster_id"].nunique()
        per_cluster = fold.groupby("cluster_id")["split"].nunique()
        assert (per_cluster == 1).all()


def test_chronological_splits_report_purge_and_embargo_basis():
    frame = simulation.simulate_scenario("communication", seed=117, n_events=30)
    folds = evaluation.chronological_splits(_event_panel(frame))
    policy = folds["test"].attrs["policy"]
    assert policy["purged_rows"] >= 0
    assert "label availability" in policy["embargo_basis"]
    assert policy["embargo_seconds"] >= 0.0
    assert policy["units_train"] and policy["units_test"]


def test_chronological_splits_reject_invalid_fractions():
    frame = simulation.simulate_scenario("communication", seed=119, n_events=20)
    panel = _event_panel(frame)
    with pytest.raises(evaluation.ForecastEvaluationError):
        evaluation.chronological_splits(panel, train_fraction=0.7, validation_fraction=0.5)
    with pytest.raises(evaluation.ForecastEvaluationError):
        evaluation.chronological_splits(panel, train_fraction=0.0)


def test_chronological_splits_reject_a_single_release():
    frame = simulation.simulate_scenario("communication", seed=121, n_events=1)
    with pytest.raises(evaluation.ForecastEvaluationError):
        evaluation.chronological_splits(_event_panel(frame))


def test_weighted_event_slope_recovers_a_known_response_exactly():
    shocks = np.array([-2.0, -1.0, 0.0, 1.0, 2.0])
    values = 3.0 * shocks - 1.0
    estimate = evaluation.weighted_event_slope(values, shocks)
    assert estimate["status"] == "ok"
    assert estimate["slope"] == pytest.approx(3.0 * float(shocks.std(ddof=0)), rel=1e-9)
    assert estimate["intercept"] == pytest.approx(-1.0, abs=1e-9)
    assert estimate["rank"] == 2


def test_weighted_event_slope_is_inconclusive_without_shock_variation():
    constant = evaluation.weighted_event_slope([1.0, 2.0, 3.0], [0.5, 0.5, 0.5])
    assert constant["status"] == "inconclusive"
    assert constant["slope"] is None
    assert "constant" in constant["reason"]
    too_few = evaluation.weighted_event_slope([1.0], [1.0])
    assert too_few["status"] == "inconclusive"
    assert too_few["slope"] is None


def test_weighted_event_slope_matches_an_unweighted_reference():
    shocks = np.array([-1.5, -0.5, 0.5, 1.5, 2.5, -2.0])
    values = 0.5 * shocks + np.array([0.1, -0.1, 0.05, -0.05, 0.2, -0.2])
    estimate = evaluation.weighted_event_slope(values, shocks)
    design = np.column_stack([np.ones(shocks.size), shocks])
    reference = np.linalg.lstsq(design, values, rcond=None)[0]
    scale = float(shocks.std(ddof=0))
    assert estimate["slope"] == pytest.approx(reference[1] * scale, rel=1e-9)


def test_event_level_slope_aggregates_rows_to_releases():
    frame = pd.DataFrame(
        {
            "event_id": ["a", "a", "b", "b", "c", "c"],
            "cluster_id": ["a", "a", "b", "b", "c", "c"],
            "response": [0.1, 0.3, -0.1, 0.1, 0.4, 0.6],
            "shock": [1.0, 1.0, 0.0, 0.0, -1.0, -1.0],
        }
    )
    result = evaluation.event_level_slope(frame)
    assert result["status"] == "ok"
    assert set(result["event_values"]) == {"a", "b", "c"}
    assert pytest.approx(result["event_values"]["a"]) == 0.2
    assert "between-release" in result["identification"]


def test_cluster_bootstrap_replicates_are_seed_deterministic():
    values = pd.Series([0.1, 0.2, 0.3, 0.4, 0.5, 0.6])
    clusters = pd.Series(["a", "a", "b", "b", "c", "c"])
    first = evaluation.cluster_bootstrap(values, clusters, seed=5, samples=60)
    second = evaluation.cluster_bootstrap(values, clusters, seed=5, samples=60)
    assert first["replicates"]["statistic"] == second["replicates"]["statistic"]
    assert first["samples"]["statistic"] == second["samples"]["statistic"]


def test_cluster_bootstrap_draws_depend_on_cluster_labels_not_metric_name():
    # Values are keyed by unit label, so the replicate payload reveals the draw.
    values = pd.Series({"u0": 0.0, "u1": 1.0, "u2": 2.0, "u3": 3.0})
    clusters = pd.Series({"u0": "a", "u1": "a", "u2": "b", "u3": "b"})

    def statistic(sample: np.ndarray) -> float:
        return float(np.sum(values.loc[list(sample)].to_numpy(dtype=np.float64)))

    first = evaluation.clustered_bootstrap(
        {"left": statistic(np.asarray(list(values.index), dtype=object))},
        lambda s: {"left": statistic(s)},
        clusters,
        seed=9,
        samples=30,
    )
    second = evaluation.clustered_bootstrap(
        {"right": statistic(np.asarray(list(values.index), dtype=object))},
        lambda s: {"right": statistic(s)},
        clusters,
        seed=9,
        samples=30,
    )
    assert first["replicates"]["left"] == second["replicates"]["right"]
    assert len(set(first["replicates"]["left"])) > 1
    changed = evaluation.clustered_bootstrap(
        {"left": statistic(np.asarray(list(values.index), dtype=object))},
        lambda s: {"left": statistic(s)},
        clusters,
        seed=10,
        samples=30,
    )
    assert changed["replicates"]["left"] != first["replicates"]["left"]


def test_cluster_bootstrap_resamples_clusters_not_rows():
    clusters = pd.Series(["a"] * 5 + ["b"] * 5)
    seen: list[int] = []

    def statistic(sample: np.ndarray) -> dict[str, float]:
        seen.append(len(set(sample.tolist())))
        return {"n": float(len(sample))}

    evaluation.clustered_bootstrap({"n": 10.0}, statistic, clusters, seed=3, samples=10)
    assert seen
    assert all(count in {5, 10} for count in seen)


def test_cluster_bootstrap_reports_inconclusive_for_a_single_cluster():
    result = evaluation.cluster_bootstrap(
        pd.Series([1.0, 2.0]), pd.Series(["only", "only"]), seed=1, samples=20
    )
    assert result["degenerate"] is True
    assert result["samples"]["statistic"]["status"] == "inconclusive"
    assert result["samples"]["statistic"]["lower"] is None


def test_curve_uncertainty_is_simultaneous_not_pointwise():
    cells = []
    for offset in range(4):
        replicates = {str(i): float(i % 7) + offset for i in range(60)}
        cells.append(
            {
                "horizon_seconds": 60 * (offset + 1),
                "shock_slope": float(offset),
                "bootstrap": {
                    "replicates": {"shock_slope": [replicates[str(i)] for i in range(60)]}
                },
                "shock_slope_ci": {"lower": -1.0, "upper": 1.0},
            }
        )
    band = evaluation.curve_uncertainty(cells, kind="shock_slope", coverage=0.95)
    assert band["status"] == "ok"
    assert band["critical_value"] > 0.0
    assert len(band["simultaneous_lower"]) == len(cells)
    assert band["pointwise_intervals"][0]["status"] == "ok"


def test_curve_uncertainty_is_inconclusive_with_too_few_aligned_cells():
    thin = evaluation.curve_uncertainty(
        [
            {
                "horizon_seconds": 60,
                "shock_slope": 0.1,
                "bootstrap": {"replicates": {"shock_slope": [0.1, 0.2]}},
            },
            {
                "horizon_seconds": 300,
                "shock_slope": None,
                "bootstrap": {"replicates": {"shock_slope": []}},
            },
        ],
        kind="shock_slope",
    )
    assert thin["status"] == "inconclusive"
    assert thin["simultaneous_lower"] is None
    assert thin["usable_cells"] == 1
    assert {entry["reason"] for entry in thin["excluded"]} == {"no_statistic"}


def test_forecast_scores_are_probability_point_errors():
    scores = evaluation.forecast_scores([0.1, -0.2], [0.0, -0.1])
    assert scores["mae"] == pytest.approx(0.1)
    assert scores["mse"] == pytest.approx(0.01)
    assert scores["rmse"] == pytest.approx(math.sqrt(0.01))
    assert "probability-point" in scores["metric_convention"]


def test_directional_accuracy_ignores_unresolved_rows():
    scores = evaluation.forecast_scores([0.0, 0.3, -0.3], [0.0, 0.2, -0.5])
    assert scores["directional_n"] == 2
    assert scores["directional_accuracy"] == pytest.approx(1.0)
    assert scores["bad_direction_rate"] == pytest.approx(0.0)


def test_forecast_scores_aggregate_to_events_when_labels_given():
    scores = evaluation.forecast_scores([0.1, 0.2, 0.3], [0.0, 0.0, 0.0], event=["a", "a", "b"])
    assert scores["n_events"] == 2
    assert scores["event_mae"]["a"] == pytest.approx(0.15)


def test_forecast_scores_reject_mismatched_lengths():
    with pytest.raises(evaluation.ForecastEvaluationError):
        evaluation.forecast_scores([0.1, 0.2], [0.1])


def test_resolution_scores_are_distinct_from_forecast_scores():
    result = evaluation.resolution_scores(
        [1.0, 0.0, 1.0, 0.0],
        [0.9, 0.1, 0.8, 0.2],
        cluster=["a", "a", "b", "b"],
    )
    assert result["brier"] < 0.05
    assert result["n_effective_units"] == 2
    assert result["n"] == 4
    assert result["unit_label"] == "cluster"
    assert "not independent binary trials" in result["unit_note"]
    assert "brier" not in evaluation.forecast_scores([1.0], [0.5])


def test_resolution_scores_exclude_unresolved_labels():
    result = evaluation.resolution_scores(
        [1.0, 0.0], [0.9, 0.1], known=[True, False], cluster=["a", "b"]
    )
    assert result["n"] == 1
    assert result["excluded"]["unknown_or_exceptional"] == 1


def test_resolution_scores_reject_exceptional_payouts():
    with pytest.raises(evaluation.ScoreConventionError):
        evaluation.resolution_scores([0.5, 0.5], [0.5, 0.5])
    flagged = evaluation.resolution_scores(
        [1.0, 0.5], [0.5, 0.5], exceptional=[False, True], cluster=["a", "b"]
    )
    assert flagged["excluded"]["unknown_or_exceptional"] == 1


def test_log_loss_boundary_convention_is_reported_and_finite():
    result = evaluation.resolution_scores([1.0, 0.0], [0.0, 1.0], cluster=["a", "b"])
    assert result["boundary_convention"]["n_clipped"] == 2
    assert math.isfinite(result["log_loss"])
    assert result["boundary_convention"]["epsilon"] > 0.0


def test_resolution_scores_return_inconclusive_without_a_usable_unit():
    result = evaluation.resolution_scores([1.0], [0.5], known=[False])
    assert result["status"] == "inconclusive"
    assert result["brier"] is None
    assert result["reason"]


def test_placebo_suite_reports_unavailable_inputs_explicitly():
    frame = simulation.simulate_scenario("communication", seed=131, n_events=24)
    panel = _event_panel(frame)
    result = evaluation.placebo_tests(panel, samples=40, n_permutations=40)
    for name, payload in result["placebos"].items():
        assert "status" in payload, name
        if payload["status"] == "unavailable":
            assert payload["reason"]
    assert result["unavailable"]
    assert "exchangeability" in result["exchangeability_note"]


def test_placebo_tests_report_the_baseline_slope_and_endpoint_axis():
    frame = simulation.simulate_scenario("communication", seed=137, n_events=30)
    panel = _event_panel(frame)
    result = evaluation.placebo_tests(panel, samples=40, n_permutations=40)
    assert result["baseline_slope"]["status"] in {"ok", "inconclusive"}
    endpoints = result["placebos"]["endpoint_sensitivity"]
    assert endpoints["status"] in {"ok", "unavailable", "inconclusive"}
    if endpoints["status"] == "ok":
        assert endpoints["by_horizon"]


def test_reversed_direction_is_the_exact_negative():
    frame = simulation.simulate_scenario("communication", seed=139, n_events=30)
    panel = _event_panel(frame)
    result = evaluation.placebo_tests(panel, samples=40, n_permutations=40)
    reversed_result = result["placebos"]["reversed_direction"]
    if reversed_result["status"] == "ok":
        assert reversed_result["matches_negation"] is True


def test_label_permutation_refuses_a_global_shuffle():
    frame = simulation.simulate_scenario("communication", seed=149, n_events=20)
    panel = _event_panel(frame).drop(columns=["family"])
    result = evaluation.label_permutation_placebo(panel, n_permutations=30)
    assert result["status"] == "unavailable"
    assert "exchangeable" in result["reason"]


def test_label_permutation_within_regime_returns_a_null_distribution():
    frame = simulation.simulate_scenario("communication", seed=151, n_events=40)
    panel = _event_panel(frame)
    result = evaluation.label_permutation_placebo(panel, n_permutations=60, seed=5)
    assert result["status"] == "ok"
    assert result["null_quantiles"]["p50"] is not None
    assert 0.0 < result["p_value_two_sided"] <= 1.0
    assert "within one regime" in result["exchangeability"]


def test_sensitivity_analysis_covers_the_prespecified_axes():
    frame = simulation.simulate_scenario("communication", seed=157, n_events=30)
    panel = _event_panel(frame)
    result = evaluation.sensitivity_analysis(panel, age_column="baseline_age_seconds", samples=40)
    assert {"endpoints", "quote_age", "time_of_day", "leave_one_event_out"} <= set(result)
    if result["status"] == "ok":
        assert result["leave_one_event_out"]["n_events"] >= 3
        assert result["leave_one_event_out"]["slope_without_event"]


def test_sensitivity_analysis_names_missing_inputs():
    frame = simulation.simulate_scenario("communication", seed=163, n_events=20)
    panel = _event_panel(frame).drop(columns=["event_time"])
    result = evaluation.sensitivity_analysis(panel, samples=20)
    assert result["status"] == "inconclusive" or result["time_of_day"]["status"] == "unavailable"


def test_weighted_slope_is_used_by_the_placebo_suite():
    frame = simulation.simulate_scenario("communication", seed=167, n_events=40)
    panel = _event_panel(frame)
    direct = evaluation.event_level_slope(panel)
    assert direct["status"] == "ok"
    reference = evaluation.weighted_event_slope(
        np.array(list(direct["event_values"].values()), dtype=np.float64),
        np.array(list(direct["event_shocks"].values()), dtype=np.float64),
    )
    assert direct["slope"] == pytest.approx(reference["slope"], rel=1e-12)


def test_power_assessment_reports_false_positive_and_power_rates():
    result = evaluation.power_assessment(
        n_events=24,
        true_slope=0.0,
        relevant_slope=0.03,
        residual_sigma=0.03,
        repetitions=40,
        samples=40,
        sample_grid=(12, 24),
    )
    assert result["status"] == "ok"
    assert 0.0 <= result["false_positive_rate"] <= 1.0
    assert 0.0 <= result["power_at_relevant_slope"] <= 1.0
    assert result["sample_requirement"]["events_for_target_power"] in {None, 12, 24}
    assert "universal event-count rule" in result["sample_requirement"]["note"]


def test_power_assessment_calibrates_residual_scale_from_a_scenario():
    result = evaluation.power_assessment(
        n_events=20,
        true_slope=0.0,
        relevant_slope=0.05,
        repetitions=30,
        samples=30,
        scenario="shared_news_delay",
        calibration_n_events=40,
        sample_grid=(12,),
    )
    assert result["residual_sigma_source"] == "scenario calibration"
    assert result["calibration"]["status"] == "ok"
    assert result["calibration"]["event_residual_sigma"] > 0.0


def test_null_false_positive_rate_uses_a_named_null_scenario():
    result = evaluation.null_false_positive_rate(
        scenario="shared_news_delay",
        n_events=20,
        repetitions=30,
        samples=30,
        relevant_slope=0.0,
        sample_grid=(12,),
    )
    assert result["null_scenario"] == "shared_news_delay"
    assert result["calibration"]["scenario"] == "shared_news_delay"
    assert result["status"] in {"ok", "inconclusive"}


def test_power_assessment_rejects_degenerate_settings():
    with pytest.raises(evaluation.ForecastEvaluationError):
        evaluation.power_assessment(repetitions=5)
    with pytest.raises(evaluation.ForecastEvaluationError):
        evaluation.power_assessment(residual_sigma=0.0)


def test_simulation_calibration_returns_event_level_scale():
    result = evaluation.simulation_calibration("communication", n_events=30)
    assert result["status"] == "ok"
    assert result["event_residual_sigma"] > 0.0
    assert result["n_events_used"] >= 2
    assert "nuisance parameters" in result["note"]


def test_classify_outcome_separates_evidence_bound_and_inconclusive():
    evidence = evaluation.classify_outcome(0.05, 0.09, relevant_effect=0.02)
    assert evidence["classification"] == "evidence_for"
    bound = evaluation.classify_outcome(-0.005, 0.015, relevant_effect=0.02)
    assert bound["classification"] == "informative_bound"
    unclear = evaluation.classify_outcome(-0.05, 0.06, relevant_effect=0.02)
    assert unclear["classification"] == "inconclusive"
    with pytest.raises(evaluation.ForecastEvaluationError):
        evaluation.classify_outcome(0.2, 0.1, relevant_effect=0.02)


def test_classification_is_reported_for_a_real_interval():
    frame = simulation.simulate_scenario("communication", seed=173, n_events=40)
    panel = _event_panel(frame)
    result = evaluation.placebo_tests(panel, samples=60, n_permutations=40)
    base = result["baseline_slope"]
    if base["status"] == "ok" and base["interval"]["lower"] is not None:
        verdict = evaluation.classify_outcome(
            base["interval"]["lower"], base["interval"]["upper"], relevant_effect=0.01
        )
        assert verdict["classification"] in {"evidence_for", "informative_bound", "inconclusive"}
        assert verdict["reason"]


def test_quote_feasibility_layers_are_honest_about_missing_instruments():
    quotes = simulation.simulate_quotes("communication", seed=179, n_events=4)
    spread = evaluation.observed_spread_summary(quotes)
    assert spread["status"] == "ok"
    assert spread["spread_min"] <= spread["spread_max"]
    unfiltered = evaluation.feasibility_distribution(quotes)
    assert unfiltered["status"] == "inconclusive"
    assert unfiltered["instrument_filter"] is None
    assert unfiltered["edge"] is None
    filtered = evaluation.feasibility_distribution(quotes, instrument_filter="does-not-exist")
    assert filtered["status"] == "inconclusive"
    assert filtered["edge"] is None
    combined = evaluation.evaluate_quotes(quotes)
    assert combined["status"] == "inconclusive"
    assert combined["edge"] is None
    assert "execution model" in combined["edge_reason"]


#: Module-level so the `timezone` rule field inside the class body cannot
#: shadow the datetime module when the deadline default is evaluated.
_DEADLINE = datetime(2026, 2, 12, 13, 30, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class RuleRecord:
    """Minimal rule carrier for matching tests, mirroring the Contract fields."""

    venue: str = "kalshi"
    contract_id: str = "cpi-above-030"
    event_id: str = "rel-0001"
    family: str = "cpi"
    reference_period: str = "2026-02"
    source: str = "bls-cpi"
    units: str = "percent_change"
    operator: str = "above"
    threshold: Decimal | None = Decimal("0.30")
    lower: Decimal | None = None
    upper: Decimal | None = None
    rounding: str = "nearest"
    vintage: str = "initial"
    timezone: str = "America/New_York"
    deadline: datetime | None = _DEADLINE
    settlement: str = "cash"
    currency: str = "USD"
    exceptional_policy: str = "reject"
    rule_hash: str = "rule-cpi-above-030"
    open_time: datetime | None = None
    close_time: datetime | None = None
    resolve_time: datetime | None = None


def _rule(**changes: Any) -> RuleRecord:
    return RuleRecord(**changes)


def brute_force_distance(
    payouts: np.ndarray, bids: np.ndarray, asks: np.ndarray, *, step: float = 0.005
) -> float:
    """Explicit enumeration of the simplex for a small family."""
    atoms = payouts.shape[1]
    best = float("inf")
    grid = np.arange(0.0, 1.0 + 1e-9, step)
    for combo in itertools.product(grid, repeat=atoms):
        if abs(sum(combo) - 1.0) > 1e-9:
            continue
        projected = payouts @ np.array(combo)
        violation = max(
            float(np.max(np.maximum(bids - projected, 0.0))),
            float(np.max(np.maximum(projected - asks, 0.0))),
        )
        best = min(best, violation)
    return best


def test_strict_and_non_strict_thresholds_differ_at_the_boundary():
    atoms = [1.0, 2.0, 3.0]
    strict = coherence.threshold_payoffs([2.0], ["above"], atoms)
    non_strict = coherence.threshold_payoffs([2.0], ["at_least"], atoms)
    assert strict[0].tolist() == [0.0, 0.0, 1.0]
    assert non_strict[0].tolist() == [0.0, 1.0, 1.0]


def test_below_and_at_most_are_the_complements_of_above_and_at_least():
    atoms = [1.0, 2.0, 3.0]
    above = coherence.threshold_payoffs([2.0], ["above"], atoms)[0]
    at_most = coherence.threshold_payoffs([2.0], ["at_most"], atoms)[0]
    below = coherence.threshold_payoffs([2.0], ["below"], atoms)[0]
    at_least = coherence.threshold_payoffs([2.0], ["at_least"], atoms)[0]
    np.testing.assert_allclose(above + at_most, np.ones(3))
    np.testing.assert_allclose(below + at_least, np.ones(3))


def test_nested_thresholds_are_monotone_in_the_threshold():
    atoms = [1.0, 2.0, 3.0, 4.0]
    payouts = coherence.threshold_payoffs([2.0, 3.0], ["at_least", "at_least"], atoms)
    for column in range(payouts.shape[1]):
        assert payouts[0, column] >= payouts[1, column]


def test_rounding_is_applied_before_the_comparison():
    strict = coherence.threshold_payoffs([2.0], ["above"], [1.996, 2.004], None)[0]
    assert strict.tolist() == [0.0, 1.0]
    nearest = coherence.threshold_payoffs([2.0], ["above"], [1.996, 2.004], "nearest")[0]
    assert nearest.tolist() == [0.0, 0.0]
    up = coherence.threshold_payoffs([2.0], ["above"], [1.996, 2.004], "up")[0]
    assert up.tolist() == [0.0, 1.0]
    down = coherence.threshold_payoffs([2.0], ["above"], [1.996, 2.004], "down")[0]
    assert down.tolist() == [0.0, 0.0]


def test_rounding_accepts_one_mode_per_contract():
    payouts = coherence.threshold_payoffs(
        [2.0, 2.0], ["above", "above"], [1.996, 2.004], ["none", "nearest"]
    )
    assert payouts[0].tolist() == [0.0, 1.0]
    assert payouts[1].tolist() == [0.0, 0.0]


def test_decimal_thresholds_avoid_binary_float_surprises():
    value = Decimal("0.1") + Decimal("0.2")
    payouts = coherence.threshold_payoffs([Decimal("0.3")], ["at_least"], [value])
    assert payouts[0].tolist() == [1.0]


def test_payouts_stay_in_the_unit_range():
    payouts = coherence.threshold_payoffs([2.0], ["above"], [1.0, 2.0, 3.0])
    assert payouts.dtype == np.float64
    assert ((payouts >= 0.0) & (payouts <= 1.0)).all()


def test_threshold_payoffs_reject_malformed_input():
    with pytest.raises(coherence.CoherenceInputError):
        coherence.threshold_payoffs([], [], [1.0])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.threshold_payoffs([1.0, 2.0], ["above"], [1.0])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.threshold_payoffs([1.0], ["sideways"], [1.0, 2.0])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.threshold_payoffs([1.0], ["above"], [])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.threshold_payoffs([None], ["above"], [1.0])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.threshold_payoffs([1.0], ["range"], [1.0])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.threshold_payoffs([1.0], ["above"], [float("nan")])


def test_complementary_pair_inside_the_box_is_feasible_at_zero_distance():
    payouts = np.array([[1.0, 0.0], [0.0, 1.0]])
    result = coherence.coherence_distance(payouts, [0.29, 0.69], [0.31, 0.71])
    assert result["feasible"] is True
    assert result["distance"] == pytest.approx(0.0, abs=1e-12)
    assert sum(result["probabilities"]) == pytest.approx(1.0)
    np.testing.assert_allclose(
        payouts @ np.array(result["probabilities"]), result["projected"], atol=1e-12
    )
    assert sum(result["projected"]) == pytest.approx(1.0, abs=1e-9)


def test_feasible_box_while_midpoints_violate_the_identity():
    payouts = np.array([[1.0, 0.0], [1.0, 0.0], [0.0, 1.0]])
    result = coherence.coherence_distance(payouts, [0.29, 0.29, 0.69], [0.31, 0.31, 0.71])
    assert result["feasible"] is True
    assert result["distance"] == pytest.approx(0.0, abs=1e-12)
    midpoint = result["box"]["midpoints"]
    assert sum(midpoint) != pytest.approx(1.0, abs=0.1)
    # a coherent projection exists inside the box, which is the whole point
    projected = np.array(result["projected"])
    assert projected[0] == pytest.approx(projected[1], abs=1e-12)
    assert projected[0] + projected[2] == pytest.approx(1.0, abs=1e-9)


def test_genuinely_infeasible_box_reports_a_positive_distance():
    payouts = np.array([[1.0, 0.0], [0.0, 1.0]])
    result = coherence.coherence_distance(payouts, [0.6, 0.6], [0.7, 0.7])
    assert result["feasible"] is False
    assert result["distance"] > 0.0
    assert result["residual_max"] == pytest.approx(result["distance"], abs=1e-9)


def test_fractional_payouts_are_supported():
    payouts = np.array([[0.5, 1.0], [0.5, 0.0]])
    result = coherence.coherence_distance(payouts, [0.4, 0.4], [0.6, 0.6])
    assert result["feasible"] is True
    assert result["distance"] == pytest.approx(0.0, abs=1e-12)
    assert all(0.0 <= value <= 1.0 for value in result["projected"])


def test_min_max_distance_matches_explicit_enumeration():
    cases = (
        (np.array([[1.0, 0.0], [0.0, 1.0]]), [0.29, 0.69], [0.31, 0.71]),
        (np.array([[1.0, 0.0], [0.0, 1.0]]), [0.6, 0.6], [0.7, 0.7]),
        (np.array([[0.5, 1.0], [0.5, 0.0]]), [0.4, 0.4], [0.6, 0.6]),
        (np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]), [0.2, 0.3], [0.25, 0.35]),
    )
    for payouts, bids, asks in cases:
        result = coherence.coherence_distance(payouts, bids, asks)
        enumerated = brute_force_distance(payouts, np.array(bids), np.array(asks), step=0.005)
        assert result["distance"] <= enumerated + 1e-9
        assert result["distance"] >= enumerated - 0.01


def test_bucket_family_with_exceptional_payout_matches_enumeration():
    payouts = np.array(
        [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
            [0.25, 0.25, 0.5],
        ]
    )
    bids = [0.2, 0.2, 0.2, 0.30]
    asks = [0.3, 0.3, 0.4, 0.45]
    result = coherence.coherence_distance(payouts, bids, asks)
    enumerated = brute_force_distance(payouts, np.array(bids), np.array(asks), step=0.005)
    assert result["feasible"] is True
    assert result["distance"] == pytest.approx(0.0, abs=1e-12)
    assert enumerated == pytest.approx(0.0, abs=1e-9)


def test_projection_never_leaves_the_box_beyond_the_reported_distance():
    payouts = np.array([[1.0, 0.0], [0.0, 1.0]])
    bids = np.array([0.55, 0.55])
    asks = np.array([0.65, 0.65])
    result = coherence.coherence_distance(payouts, bids.tolist(), asks.tolist())
    projected = np.array(result["projected"])
    violation = np.maximum(np.maximum(bids - projected, 0.0), np.maximum(projected - asks, 0.0))
    assert float(violation.max()) == pytest.approx(result["distance"], abs=1e-9)


def test_midpoint_diagnostic_is_reported_separately_from_the_box():
    payouts = np.array([[1.0, 0.0], [0.0, 1.0]])
    result = coherence.coherence_distance(payouts, [0.4, 0.4], [0.5, 0.5])
    assert result["midpoint_distance"] is not None
    assert result["midpoint_probabilities"] is not None
    assert result["box"]["bids"] == [0.4, 0.4]
    assert result["box"]["asks"] == [0.5, 0.5]


def test_solver_provenance_is_returned():
    result = coherence.coherence_distance(
        np.array([[1.0, 0.0], [0.0, 1.0]]), [0.4, 0.4], [0.6, 0.6]
    )
    assert result["solver"]["method"] == "highs"
    assert result["solver"]["status"] == 0
    assert result["n_contracts"] == 2 and result["n_atoms"] == 2
    assert result["feasible_tolerance"] >= 0.0


def test_coherence_distance_rejects_bad_inputs():
    payouts = np.array([[1.0, 0.0], [0.0, 1.0]])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(payouts, None, [0.6, 0.6])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(payouts, [0.4], [0.6, 0.6])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(payouts, [0.7, 0.4], [0.6, 0.6])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(payouts, [float("nan"), 0.4], [0.6, 0.6])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(np.array([1.0, 0.0]), [0.4, 0.4], [0.6, 0.6])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(np.array([[[1.0]]]), [0.4], [0.6])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(np.array([[1.0, 1.5]]), [0.4], [0.6])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(np.array([[1.0, -0.5]]), [0.4], [0.6])
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(payouts, [0.4, 0.4], [0.6, 0.6], feasible_tolerance=-1.0)


def test_exceptional_values_outside_the_unit_range_are_refused():
    with pytest.raises(coherence.CoherenceInputError):
        coherence.coherence_distance(np.array([[1.2, 0.0]]), [0.4], [0.6])


def test_single_contract_is_feasible_only_inside_the_box():
    infeasible = coherence.coherence_distance(np.array([[1.0]]), [0.4], [0.6])
    assert infeasible["feasible"] is False
    assert infeasible["distance"] == pytest.approx(0.4)
    assert infeasible["probabilities"] == [pytest.approx(1.0)]
    feasible = coherence.coherence_distance(np.array([[1.0]]), [0.9], [1.0])
    assert feasible["feasible"] is True
    assert feasible["distance"] == pytest.approx(0.0, abs=1e-12)
    assert feasible["projected"] == [pytest.approx(1.0)]


def test_identical_full_rules_match():
    result = coherence.exact_match(_rule(), _rule())
    assert result["matches"] is True
    assert result["blocking_differences"] == []
    assert result["differences"] == []
    assert result["missing_fields"] == []
    assert result["cross_venue"] is False
    assert set(result["compared_fields"]) == set(coherence.COHERENCE_FIELDS)


def test_every_rule_field_difference_blocks_a_match():
    for field in coherence.COHERENCE_FIELDS:
        if field in {
            "venue",
            "contract_id",
            "rule_hash",
            "open_time",
            "close_time",
            "resolve_time",
        }:
            continue
        changes: dict[str, Any] = {field: "changed"}
        if field in {"threshold", "lower", "upper"}:
            changes[field] = Decimal("0.99")
        elif field == "operator":
            changes[field] = "below"
        elif field == "rounding":
            changes[field] = "down"
        left, right = _rule(), _rule(**changes)
        result = coherence.exact_match(left, right)
        assert result["matches"] is False, field
        assert field in result["blocking_differences"], field


def test_threshold_strictness_mismatch_blocks_a_match():
    result = coherence.exact_match(_rule(operator="above"), _rule(operator="at_least"))
    assert result["matches"] is False
    assert "operator" in result["blocking_differences"]


def test_same_venue_identity_difference_blocks_a_match():
    result = coherence.exact_match(
        _rule(contract_id="cpi-above-030"), _rule(contract_id="cpi-above-099")
    )
    assert result["matches"] is False
    assert "contract_id" in result["blocking_differences"]


def test_cross_venue_identity_difference_is_a_note_not_a_rule_difference():
    left = _rule(venue="kalshi", contract_id="kalshi-cpi-above-030")
    right = _rule(venue="polymarket", contract_id="polymarket-cpi-above-030")
    result = coherence.exact_match(left, right)
    assert result["cross_venue"] is True
    assert result["matches"] is True
    assert result["blocking_differences"] == []
    recorded = {entry["field"] for entry in result["differences"]}
    assert {"venue", "contract_id"} <= recorded
    assert all(entry["blocking"] is False for entry in result["differences"])


def test_missing_metadata_on_both_records_is_not_a_verified_match():
    left = RuleRecord(reference_period="", source="", units="")
    right = RuleRecord(reference_period="", source="", units="")
    result = coherence.exact_match(left, right)
    assert result["matches"] is False
    assert "reference_period" in result["missing_fields"]
    assert any("missing-required-semantic-field" in reason for reason in result["missing_reasons"])


def test_absent_field_differs_from_a_present_one():
    left = RuleRecord(deadline=None)
    right = _rule()
    result = coherence.exact_match(left, right)
    assert result["matches"] is False
    assert "deadline" in result["blocking_differences"]
    entry = next(item for item in result["differences"] if item["field"] == "deadline")
    assert entry["kind"] == "presence"
    assert entry["blocking"] is True


def test_inapplicable_threshold_is_a_note_not_a_missing_rule():
    left = RuleRecord(operator="range", threshold=None, lower=Decimal("0.1"), upper=Decimal("0.3"))
    right = RuleRecord(operator="range", threshold=None, lower=Decimal("0.1"), upper=Decimal("0.3"))
    result = coherence.exact_match(left, right)
    assert result["matches"] is True
    assert "threshold" not in result["missing_fields"]
    assert any("inapplicable" in note for note in result["notes"])


def test_missing_scalar_threshold_is_a_missing_rule():
    left = RuleRecord(threshold=None)
    right = RuleRecord(threshold=None)
    result = coherence.exact_match(left, right)
    assert result["matches"] is False
    assert "threshold" in result["missing_fields"]
    assert result["blocking_differences"] == []
    assert any("missing-required-semantic-field" in reason for reason in result["missing_reasons"])


def test_titles_alone_never_certify_a_match():
    left = _rule(rule_hash="same-hash", threshold=Decimal("0.30"))
    right = _rule(rule_hash="same-hash", threshold=Decimal("0.40"))
    result = coherence.exact_match(left, right)
    assert result["matches"] is False
    assert "threshold" in result["blocking_differences"]
    assert "rule_hash" in result["matched_fields"]


def test_partial_records_fail_closed_rather_than_matching():
    barren = RuleRecord(
        reference_period="",
        source="",
        units="",
        vintage="",
        rounding="",
        timezone="",
        settlement="",
        currency="",
        exceptional_policy="",
        threshold=None,
    )
    result = coherence.exact_match(barren, barren)
    assert result["matches"] is False
    assert len(result["missing_fields"]) >= 8
    assert len(result["missing_reasons"]) >= 8


def test_comparison_is_total_and_never_raises_on_mixed_types():
    left = _rule(threshold=Decimal("0.30"))
    right = _rule(threshold="0.30")
    result = coherence.exact_match(left, right)
    assert result["matches"] is False
    assert "threshold" in result["blocking_differences"]


def test_report_values_are_serialisable():
    deadline = datetime(2026, 2, 12, 13, 30, tzinfo=UTC)
    result = coherence.exact_match(
        _rule(deadline=deadline), _rule(deadline=deadline - timedelta(days=1))
    )
    entry = next(item for item in result["differences"] if item["field"] == "deadline")
    assert isinstance(entry["left"], str) and entry["left"].startswith("2026-02-12")
    assert isinstance(entry["right"], str)


def test_matched_and_blocking_field_reports_are_consistent():
    result = coherence.exact_match(_rule(source="bls-ppi"), _rule())
    assert result["matches"] is False
    assert "source" in result["blocking_differences"]
    assert "source" not in result["matched_fields"]
    assert "family" in result["matched_fields"]
    assert len(set(result["matched_fields"])) == len(result["matched_fields"])
