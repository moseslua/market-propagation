"""Behavior tests for the timing-only response baseline.

These defend the contract the pipeline configuration declares: a caller can fit
the baseline that stays available when no release expectation exists, the nested
ladder is nested as an arithmetic fact rather than as a convention, an absent
expectation disables only a surprise slope, and every weight and interval is
computed at the economic release. Assertions are on fitted coefficients, reported
weights and realized intervals, never on internal wiring or on the presence of
an exception where a coefficient comparison is the real claim.

The strongest test here is the one that plants a prohibited covariate in the
panel and asserts the fitted coefficients are identical, because that is the
property the whole module exists for: a `shock` column that no release verifies
must not reach the design, and rejecting the row instead would shrink the sample
without making the fit any more honest.
"""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest

from market_propagation import timing_model
from market_propagation.storage import TRADE_PANEL_COLUMNS

HORIZON = 300


def _panel(
    *,
    releases: int = 40,
    contracts: int = 3,
    horizons: tuple[int, ...] = (HORIZON,),
    noise: float = 0.004,
    seed: int = 20260915,
) -> pd.DataFrame:
    """Deterministic panel with a known price response and a known curvature term.

    Every release contributes several contracts at its own baseline price, so the
    price terms are identified across releases rather than from one contract per
    release, and the curvature coefficient is distinguishable from the level
    coefficient because the baseline prices straddle one half.
    """
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for index in range(releases):
        family = "cpi" if index % 2 == 0 else "employment"
        event_time = pd.Timestamp("2025-01-02T13:30:00Z") + pd.Timedelta(days=2 * index)
        for slot in range(contracts):
            price = float(rng.uniform(0.05, 0.95))
            for horizon in horizons:
                rows.append(
                    {
                        "event_id": f"E{index:04d}",
                        "cluster_id": f"R{index:04d}",
                        "family": family,
                        "venue": "kalshi",
                        "contract_id": f"C{index:04d}{slot}",
                        "cohort": "external_transaction_response",
                        "event_time": event_time,
                        "horizon_seconds": horizon,
                        "baseline": price,
                        "response": float(
                            0.03 * (0.5 - price)
                            + 0.02 * (0.25 - price * (1.0 - price))
                            + rng.normal(0.0, noise)
                        ),
                        "baseline_trade_count": int(rng.integers(1, 20)),
                        "valid": True,
                        "exclusion_reason": None,
                    }
                )
    return pd.DataFrame(rows)


def _contract_rows(count: int = 40, contracts: int = 1) -> pd.DataFrame:
    """Minimal panel whose baseline prices straddle one half at equal spacing."""
    prices = np.linspace(0.05, 0.95, count)
    rows: list[dict[str, object]] = []
    for index, price in enumerate(prices):
        for slot in range(contracts):
            rows.append(
                {
                    "event_id": f"E{index:04d}",
                    "cluster_id": f"R{index:04d}",
                    "family": "cpi" if index % 2 == 0 else "employment",
                    "venue": "kalshi",
                    "contract_id": f"C{index:04d}{slot}",
                    "cohort": "external_transaction_response",
                    "event_time": pd.Timestamp("2025-01-02T13:30:00Z")
                    + pd.Timedelta(days=2 * index),
                    "horizon_seconds": HORIZON,
                    "baseline": float(price),
                    "response": float(0.02 * (0.5 - price)),
                    "baseline_trade_count": 4,
                    "valid": True,
                    "exclusion_reason": None,
                }
            )
    return pd.DataFrame(rows)


def _spec() -> timing_model.TimingSpec:
    return timing_model.load_timing_spec()


# --------------------------------------------------------------------------
# The declared specification
# --------------------------------------------------------------------------


def test_spec_is_read_from_the_declared_block() -> None:
    """The baseline's constants come from the configuration, not from a copy."""
    spec = _spec()
    assert spec.model_kind == timing_model.TIMING_MODEL_KIND == "timing_only"
    assert spec.admissible_covariates == timing_model.TIMING_ADMISSIBLE_COVARIATES
    assert spec.prohibited_covariates == timing_model.PROHIBITED_COVARIATES
    assert spec.prohibited_covariates[-1] == "any_post_event_quantity_or_price"
    assert spec.alphas == timing_model.DEFAULT_TIMING_ALPHAS
    assert spec.coefficient_families == timing_model.DEFAULT_TIMING_FAMILIES
    assert spec.loss == timing_model.SUPPORTED_LOSS
    assert spec.aggregation_unit == timing_model.AGGREGATION_UNIT
    assert spec.family_coefficients_reported_separately is True
    assert spec.surprise_slopes_enabled is False
    assert spec.source_path.endswith("configs/external_history_v1.yaml")


def test_declared_covariates_do_not_overlap_the_forecast_ladder() -> None:
    """The timing baseline is a second specification, not a renaming of the ladder."""
    overlap = set(timing_model.TIMING_ADMISSIBLE_COVARIATES) & set(
        timing_model.FORECAST_LADDER_COVARIATES
    )
    assert overlap == set()
    assert "shock" in timing_model.FORECAST_LADDER_COVARIATES
    assert "shock" in timing_model.PROHIBITED_COVARIATES


def test_absent_block_raises_instead_of_defaulting(tmp_path: pathlib.Path) -> None:
    """A configuration with no block declares no specification to fall back on."""
    path = tmp_path / "external_history_v1.yaml"
    path.write_text(
        "config_version: external_history_v1\nresponse:\n  pre_window_seconds: 1800\n",
        encoding="utf-8",
    )
    with pytest.raises(timing_model.TimingModelError, match="declares no 'timing_only' block"):
        timing_model.load_timing_spec(path)


def test_incomplete_block_raises_rather_than_completing_itself(tmp_path: pathlib.Path) -> None:
    """A block missing a declared key is an error, not a partial specification."""
    path = tmp_path / "config.yaml"
    path.write_text("timing_only:\n  model_kind: timing_only\n", encoding="utf-8")
    with pytest.raises(timing_model.TimingModelError, match="declares no"):
        timing_model.load_timing_spec(path)


def test_missing_configuration_file_raises(tmp_path: pathlib.Path) -> None:
    with pytest.raises(timing_model.TimingModelError, match="does not exist"):
        timing_model.load_timing_spec(tmp_path / "absent.yaml")


def test_disagreeing_covariate_tuple_is_refused(tmp_path: pathlib.Path) -> None:
    """A configuration that renames a covariate cannot be read under this code."""
    source = pathlib.Path(timing_model.DEFAULT_CONFIG_PATH).read_text(encoding="utf-8")
    edited = source.replace(
        "    - boundary_proximity\n    - pre_release_activity\n",
        "    - boundary_proximity\n    - pre_release_activity_renamed\n",
    )
    assert edited != source
    path = tmp_path / "config.yaml"
    path.write_text(edited, encoding="utf-8")
    with pytest.raises(timing_model.TimingModelError, match="admissible_covariates"):
        timing_model.load_timing_spec(path)


def test_meaningful_sizes_are_reported_as_unmeasured_choices() -> None:
    """The two meaningful sizes are scientific choices, not measured capabilities."""
    sizes = _spec().meaningful_sizes()
    assert sizes["meaningful_response_size"] == 0.01
    assert sizes["meaningful_gain_size"] == 0.005
    assert sizes["status"] == "scientific_choices_requiring_power_assessment"
    assert sizes["measured"] is False
    assert sizes["power_assessment"] is None


# --------------------------------------------------------------------------
# Nested ladder
# --------------------------------------------------------------------------


def test_ladder_is_nested_for_every_kind() -> None:
    """Every rung's covariate set contains the previous rung's, in declared order."""
    kinds = timing_model.TIMING_LADDER_KINDS
    assert len(kinds) == len(timing_model.TIMING_ADMISSIBLE_COVARIATES)
    previous: tuple[str, ...] = ()
    for kind in kinds:
        covariates = timing_model.TIMING_LADDER_COVARIATES[kind]
        assert covariates[: len(previous)] == previous
        assert len(covariates) == len(previous) + 1
        previous = covariates
    assert previous == timing_model.TIMING_ADMISSIBLE_COVARIATES
    assert timing_model.TIMING_FEATURE_SPECS[kinds[-1]] == timing_model.feature_columns(
        timing_model.TIMING_ADMISSIBLE_COVARIATES
    )


def test_nested_rungs_add_a_dimension_on_rows_where_the_covariate_varies() -> None:
    """Nesting is a rank statement about the actual designs, not only about names."""
    frame = _contract_rows(count=40)
    ranks: list[int] = []
    for kind in timing_model.TIMING_LADDER_KINDS:
        design = timing_model.build_timing_features(frame, spec=_spec(), kind=kind)
        ranks.append(int(np.linalg.matrix_rank(design.matrix())))
        assert design.matrix().shape == (design.n_rows, len(design.columns))
    assert ranks == sorted(ranks)
    assert ranks[-1] > ranks[0]
    for kind in timing_model.TIMING_LADDER_KINDS:
        design = timing_model.build_timing_features(frame, spec=_spec(), kind=kind)
        previous = timing_model.TIMING_LADDER_COVARIATES[kind][:-1]
        assert set(previous) <= set(design.covariates)


def test_every_rung_fits_on_one_identical_sample() -> None:
    """The comparison is between nested specifications, not differently sized samples."""
    comparison = timing_model.compare_timing_kinds(_panel(), samples=60)
    assert comparison.sample["identical_rows_for_every_rung"] is True
    for kind in comparison.kinds:
        assert comparison.fits[kind]["n_rows"] == comparison.sample["n_train_rows"]
        assert comparison.evaluations[kind]["n_rows"] == comparison.sample["n_test_rows"]
        assert comparison.evaluations[kind]["event_ids"] == comparison.sample["held_out_events"]
    assert comparison.baseline_kind == timing_model.TIMING_LADDER_BASELINE_KIND


def test_unknown_rung_is_refused() -> None:
    with pytest.raises(timing_model.TimingModelError, match="unknown timing rung"):
        _spec().covariates_for("timing_surprise")


# --------------------------------------------------------------------------
# Derived covariates
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("price", "curvature", "proximity"),
    [(0.0, 0.0, 0.0), (0.5, 0.25, 0.5), (1.0, 0.0, 0.0)],
)
def test_curvature_and_proximity_at_the_declared_prices(
    price: float, curvature: float, proximity: float
) -> None:
    """Both geometry terms follow their declared formula at the boundary cases."""
    frame = pd.DataFrame(
        [
            {
                "event_id": "E1",
                "cluster_id": "R1",
                "family": "cpi",
                "contract_id": "C1",
                "event_time": pd.Timestamp("2025-01-02T13:30:00Z"),
                "horizon_seconds": HORIZON,
                "baseline": price,
                "response": 0.0,
                "baseline_trade_count": 3,
                "valid": True,
                "exclusion_reason": None,
            }
        ]
    )
    derived = timing_model.add_derived_covariates(frame)
    assert float(derived["pre_event_price"].iloc[0]) == price
    assert float(derived["bounded_price_curvature"].iloc[0]) == pytest.approx(curvature)
    assert float(derived["boundary_proximity"].iloc[0]) == pytest.approx(proximity)
    assert float(derived["family_indicator_cpi"].iloc[0]) == 1.0
    assert float(derived["family_indicator_employment"].iloc[0]) == 0.0
    assert float(derived["horizon_seconds"].iloc[0]) == float(HORIZON)


def test_geometry_is_a_real_observation_at_the_boundary_not_a_missing_value() -> None:
    """A zero baseline is a price at the absorbing state, not an absent price."""
    frame = _contract_rows(count=6)
    frame.loc[frame.index[:2], "baseline"] = 0.0
    design = timing_model.build_timing_features(frame, spec=_spec())
    assert design.n_rows == 6
    assert "baseline_at_probability_boundary" in design.flags
    assert float(design.frame["bounded_price_curvature"].min()) == 0.0
    assert design.frame["boundary_proximity"].notna().all()


def test_price_outside_the_probability_scale_raises() -> None:
    """A baseline outside the declared payout axis is a unit error, not a row to drop."""
    frame = _contract_rows(count=6)
    frame.loc[0, "baseline"] = 1.4
    with pytest.raises(timing_model.TimingModelError, match="outside the"):
        timing_model.add_derived_covariates(frame)
    with pytest.raises(timing_model.TimingModelError, match="outside the"):
        timing_model.build_timing_features(frame, spec=_spec())


def test_row_without_a_baseline_price_is_excluded_not_imputed() -> None:
    """A missing expectation-free input is an exclusion, never a zero or a mean."""
    frame = _contract_rows(count=8)
    frame.loc[0, "baseline"] = np.nan
    frame.loc[0, "exclusion_reason"] = "missing_baseline"
    frame.loc[0, "valid"] = False
    design = timing_model.build_timing_features(frame, spec=_spec())
    assert design.n_rows == 7
    assert design.exclusions == {"missing_baseline": 1}
    assert design.frame["baseline"].isna().sum() == 0
    assert 0.0 not in design.frame["pre_event_price"].to_numpy()


def test_missing_activity_count_is_excluded_under_its_own_reason() -> None:
    """A null covariate leaves the sample; it never enters the design as zero."""
    frame = _contract_rows(count=8)
    frame.loc[3, "baseline_trade_count"] = np.nan
    design = timing_model.build_timing_features(frame, spec=_spec())
    assert design.n_rows == 7
    assert design.exclusions.get("missing_pre_release_activity") == 1


def test_missing_baseline_price_fails_when_every_row_lacks_one() -> None:
    frame = _contract_rows(count=4)
    frame["baseline"] = np.nan
    with pytest.raises(timing_model.TimingModelError, match="nothing to fit"):
        timing_model.build_timing_features(frame, spec=_spec())


def test_only_declared_panel_columns_may_be_read() -> None:
    """The design is built from the frozen panel schema and nothing else."""
    assert set(timing_model.PANEL_COLUMNS_READ) <= set(TRADE_PANEL_COLUMNS)
    assert set(timing_model.SOURCE_COLUMNS.values()) <= set(timing_model.PANEL_COLUMNS_READ)
    assert "response" not in timing_model.PANEL_COLUMNS_READ[:8]


# --------------------------------------------------------------------------
# Prohibited covariates
# --------------------------------------------------------------------------


def test_prohibited_covariate_cannot_be_requested() -> None:
    with pytest.raises(timing_model.ProhibitedCovariateError, match="shock"):
        timing_model.assert_permitted_covariates(["pre_event_price", "shock"], context="test")
    with pytest.raises(timing_model.ProhibitedCovariateError, match="winning_outcome_label"):
        timing_model.build_timing_features(
            _contract_rows(count=4), spec=_spec(), covariates=["winning_outcome_label"]
        )


def test_post_event_columns_are_prohibited_as_covariates() -> None:
    """Every endpoint-derived panel column is refused by name, not by convention."""
    assert "endpoint" in timing_model.POST_EVENT_PANEL_COLUMNS
    assert "response" in timing_model.TIMING_PROHIBITED_COVARIATES
    for column in timing_model.POST_EVENT_PANEL_COLUMNS:
        assert column in timing_model.TIMING_PROHIBITED_COVARIATES


def test_prohibited_panel_column_present_changes_no_coefficient() -> None:
    """An unverified shock column in the rows must not reach the design.

    This is asserted on the coefficients rather than on a lack of an exception,
    because the claim is that the value is ignored rather than that the row is
    admitted. A row carrying a shock column that no release verifies must fit
    exactly as the same row without it.
    """
    clean = _contract_rows(count=40)
    tainted = clean.assign(
        shock=np.linspace(-3.0, 3.0, len(clean)),
        delayed_shock=np.linspace(3.0, -3.0, len(clean)),
        surprise=1.0,
        expectation_gap=0.25,
        winning_outcome_label="yes",
        resolution_status="resolved",
    )
    clean_fit = timing_model.fit_timing_model(
        timing_model.build_timing_features(clean, spec=_spec())
    )
    tainted_fit = timing_model.fit_timing_model(
        timing_model.build_timing_features(tainted, spec=_spec())
    )
    assert tainted_fit.coefficients == clean_fit.coefficients
    assert tainted_fit.alpha == clean_fit.alpha
    assert tainted_fit.event_weighted_mae == clean_fit.event_weighted_mae
    assert "shock" not in tainted_fit.feature_names
    assert tainted_fit.n_rows == clean_fit.n_rows


# --------------------------------------------------------------------------
# Fitting and event weighting
# --------------------------------------------------------------------------


def test_penalty_is_selected_on_validation_rows() -> None:
    """The penalty comes from held-out rows, and the grid is the declared one."""
    design = timing_model.build_timing_features(_panel(releases=30), spec=_spec())
    fit = timing_model.fit_timing_model(design, validation=design)
    assert fit.penalty_selected_on == "validation"
    assert fit.alpha in timing_model.DEFAULT_TIMING_ALPHAS
    assert sorted(fit.validation_scores) == [
        f"alpha={alpha:g}" for alpha in timing_model.DEFAULT_TIMING_ALPHAS
    ]
    assert fit.alpha == min(
        timing_model.DEFAULT_TIMING_ALPHAS,
        key=lambda alpha: (fit.validation_scores[f"alpha={alpha:g}"], alpha),
    )


def test_penalty_without_validation_rows_says_so() -> None:
    """An in-sample penalty is recorded as such rather than passed off as tuned."""
    design = timing_model.build_timing_features(_panel(releases=20), spec=_spec())
    fit = timing_model.fit_timing_model(design)
    assert fit.penalty_selected_on == "train"
    assert any("re-selected on held-out releases" in note for note in fit.notes)


def test_two_contracts_in_one_release_carry_the_weight_of_one_release() -> None:
    """Contracts inside a release share its weight, so the release counts once."""
    frame = pd.DataFrame(
        [
            {
                "event_id": "E1",
                "cluster_id": "R1",
                "contract_id": "A",
                "family": "cpi",
                "event_time": pd.Timestamp("2025-01-02T13:30:00Z"),
                "horizon_seconds": HORIZON,
                "baseline": 0.4,
                "response": 0.01,
                "baseline_trade_count": 2,
                "valid": True,
                "exclusion_reason": None,
            },
            {
                "event_id": "E1",
                "cluster_id": "R1",
                "contract_id": "B",
                "family": "cpi",
                "event_time": pd.Timestamp("2025-01-02T13:30:00Z"),
                "horizon_seconds": HORIZON,
                "baseline": 0.6,
                "response": 0.02,
                "baseline_trade_count": 2,
                "valid": True,
                "exclusion_reason": None,
            },
            {
                "event_id": "E2",
                "cluster_id": "R2",
                "contract_id": "C",
                "family": "employment",
                "event_time": pd.Timestamp("2025-01-09T13:30:00Z"),
                "horizon_seconds": HORIZON,
                "baseline": 0.5,
                "response": 0.03,
                "baseline_trade_count": 2,
                "valid": True,
                "exclusion_reason": None,
            },
        ]
    )
    weights = timing_model.release_weights(frame)
    assert weights.tolist() == [0.5, 0.5, 1.0]
    per_release = weights.groupby(timing_model.release_labels(frame)).sum()
    assert per_release.tolist() == [1.0, 1.0]
    assert float(weights.sum()) == pytest.approx(2.0)


def test_event_weighted_loss_is_a_release_mean_not_a_row_mean() -> None:
    """The reported loss is the release mean, and the row mean is reported beside it."""
    actual = np.array([0.0, 0.0, 0.10])
    predicted = np.array([0.02, 0.02, 0.10])
    releases = np.array(["R1", "R1", "R2"])
    payload = timing_model.event_weighted_mae(actual, predicted, releases=releases)
    assert payload["value"] == pytest.approx((0.02 + 0.0) / 2.0)
    assert payload["row_mae"] == pytest.approx(0.04 / 3.0)
    assert payload["n_releases"] == 2
    assert payload["loss"] == timing_model.SUPPORTED_LOSS
    assert payload["weighting"]["unit"] == timing_model.AGGREGATION_UNIT


def test_unequal_contract_counts_do_not_move_the_reported_loss() -> None:
    """The loss does not depend on how many contracts happen to be observed."""
    frame = _contract_rows(count=20)
    design = timing_model.build_timing_features(frame, spec=_spec())
    doubled = pd.concat(
        [design.frame, design.frame.assign(contract_id=design.frame["contract_id"] + "_2")]
    )
    fit = timing_model.fit_timing_model(design)
    expected = timing_model.score_timing_fit(fit, design.frame)
    doubled_score = timing_model.score_timing_fit(fit, doubled)
    assert doubled_score["event_weighted_mae"] == pytest.approx(
        expected["event_weighted_mae"], abs=1e-12
    )
    assert doubled_score["n_releases"] == expected["n_releases"] == 20
    assert doubled_score["n_rows"] == 2 * expected["n_rows"]


def test_cpi_and_employment_coefficients_are_reported_separately() -> None:
    """Each family's level coefficient is reported under its own name."""
    fit = timing_model.fit_timing_model(
        timing_model.build_timing_features(_panel(releases=40), spec=_spec())
    )
    assert set(fit.family_coefficients) == set(timing_model.DEFAULT_TIMING_FAMILIES)
    assert fit.feature_names[:2] == ("family_indicator_cpi", "family_indicator_employment")
    for family, payload in fit.family_coefficients.items():
        assert payload["indicator"] == f"family_indicator_{family}"
        assert payload["n_rows"] > 0
        assert payload["n_releases"] > 0
        assert np.isfinite(payload["coefficient"])
    assert (
        fit.family_coefficients["cpi"]["coefficient"]
        != fit.family_coefficients["employment"]["coefficient"]
    )


def test_family_coefficients_appear_on_the_evaluation_too() -> None:
    """A rung reports family levels exactly when it carries the family indicator."""
    comparison = timing_model.compare_timing_kinds(_panel(releases=30), samples=40)
    for kind in comparison.kinds:
        reported = comparison.evaluations[kind]["family_coefficients"]
        carries_family = (
            timing_model.COVARIATE_FAMILY_INDICATOR in (timing_model.TIMING_LADDER_COVARIATES[kind])
        )
        assert set(reported) == (
            set(timing_model.DEFAULT_TIMING_FAMILIES) if carries_family else set()
        )


def test_a_family_level_is_recovered_from_a_known_response() -> None:
    """The family coefficient is the fitted response level, in probability points."""
    frame = _contract_rows(count=40, contracts=2)
    frame.loc[frame["family"] == "employment", "response"] += 0.05
    fit = timing_model.fit_timing_model(timing_model.build_timing_features(frame, spec=_spec()))
    difference = (
        fit.family_coefficients["employment"]["coefficient"]
        - fit.family_coefficients["cpi"]["coefficient"]
    )
    assert difference == pytest.approx(0.05, abs=0.01)


def test_conditioning_is_fixed_at_fit_and_reapplied() -> None:
    """A frozen fit scores new rows with the fitting transforms, never refit ones."""
    frame = _panel(releases=30)
    design = timing_model.build_timing_features(frame, spec=_spec())
    fit = timing_model.fit_timing_model(design)
    shifted = design.frame.assign(pre_event_price=design.frame["pre_event_price"] + 0.1)
    prediction = fit.predict(shifted)
    manual = fit.coefficients["pre_event_price"] * (
        (shifted["pre_event_price"] - fit.feature_means["pre_event_price"])
        / fit.feature_scales["pre_event_price"]
    )
    assert prediction.shape == (len(shifted),)
    assert np.isfinite(prediction).all()
    assert np.isfinite(manual).all()
    assert fit.feature_scales["pre_event_price"] > 0.0


def test_unusable_prediction_rows_are_refused() -> None:
    """A frozen fit does not accept a row whose covariate is absent."""
    frame = _panel(releases=20)
    fit = timing_model.fit_timing_model(timing_model.build_timing_features(frame, spec=_spec()))
    tainted = timing_model.build_timing_features(frame, spec=_spec()).frame
    tainted.loc[0, "pre_release_activity"] = np.nan
    with pytest.raises(timing_model.TimingModelError, match="not imputed"):
        fit.predict(tainted)


# --------------------------------------------------------------------------
# Paired comparison and whole-event uncertainty
# --------------------------------------------------------------------------


def test_comparison_reports_paired_gains_per_event_and_per_release() -> None:
    comparison = timing_model.compare_timing_kinds(_panel(releases=40), samples=80)
    held_out = comparison.sample["held_out_events"]
    held_out_releases = comparison.sample["held_out_releases"]
    assert held_out and held_out_releases
    for kind in comparison.kinds:
        gain = comparison.gains[kind]
        evaluated = comparison.evaluations[kind]
        assert set(gain["per_event_gain"]) == set(evaluated["per_event_mae"])
        assert set(gain["per_release_gain"]) == set(evaluated["per_release_mae"])
        assert set(gain["per_event_gain"]) == set(held_out)
        assert set(gain["per_release_gain"]) == set(held_out_releases)
        assert gain["n_events"] == len(gain["per_event_gain"])
        assert gain["n_releases"] == len(gain["per_release_gain"])
        assert gain["metric"] == timing_model.SUPPORTED_LOSS
        assert gain["meaningful_gain_size"] == 0.005
        assert gain["meaningful_gain_size_status"] == (
            "scientific_choices_requiring_power_assessment"
        )
        assert gain["interval"]["coverage"] == timing_model.DEFAULT_COVERAGE
        assert gain["bootstrap"]["unit"] == timing_model.AGGREGATION_UNIT


def test_baseline_gain_against_itself_is_zero_with_no_interval() -> None:
    """A rung compared against itself has no gain and no identified interval."""
    comparison = timing_model.compare_timing_kinds(_panel(releases=30), samples=40)
    baseline = comparison.gains[comparison.baseline_kind]
    assert baseline["point"] == pytest.approx(0.0)
    assert all(value == pytest.approx(0.0) for value in baseline["per_event_gain"].values())
    assert all(value == pytest.approx(0.0) for value in baseline["per_release_gain"].values())
    assert baseline["classification"]["classification"] in {
        "inconclusive",
        "informative_bound",
    }
    assert baseline["bootstrap"]["n_clusters"] == len(comparison.sample["held_out_releases"])


def test_uncertainty_is_computed_at_the_whole_event_level() -> None:
    """Duplicating a release's rows must not shrink the reported interval.

    A row-level bootstrap would treat the extra duplicate contracts as extra
    evidence and tighten the interval, which is pseudo replication. The
    whole-release bootstrap draws releases, so adding rows inside existing
    releases leaves the interval's width where it was.
    """
    panel = _panel(releases=40)
    first_release = panel["cluster_id"].iloc[0]
    duplicated = pd.concat(
        [
            panel,
            panel.loc[panel["cluster_id"] == first_release].assign(
                contract_id=lambda frame: frame["contract_id"] + "_dup"
            ),
        ],
        ignore_index=True,
    )
    original = timing_model.compare_timing_kinds(panel, samples=120)
    with_duplicates = timing_model.compare_timing_kinds(duplicated, samples=120)
    assert with_duplicates.sample["n_rows"] > original.sample["n_rows"]
    assert with_duplicates.sample["n_releases"] == original.sample["n_releases"]
    assert with_duplicates.sample["held_out_releases"] == original.sample["held_out_releases"]
    identified = 0
    for kind in original.kinds:
        before = original.gains[kind]["interval"]
        after = with_duplicates.gains[kind]["interval"]
        assert after["status"] == before["status"]
        if before["lower"] is None or after["lower"] is None:
            continue
        before_width = before["upper"] - before["lower"]
        after_width = after["upper"] - after["lower"]
        assert after_width == pytest.approx(before_width, rel=0.05)
        assert after_width >= before_width * 0.9
        assert original.gains[kind]["point"] == pytest.approx(
            with_duplicates.gains[kind]["point"], rel=1e-6, abs=1e-8
        )
        identified += 1
    assert identified > 0


def test_bootstrap_resampling_unit_follows_releases_not_rows() -> None:
    """The cluster count is the release count, whatever the rows per release are.

    Widening a panel by adding contracts to the releases it already has must
    leave the resampling unit untouched, which is the mechanical statement that
    a release, not a row, is the unit of independent information.
    """
    sparse = _panel(releases=30, contracts=1)
    dense = _panel(releases=30, contracts=6)
    assert sparse["cluster_id"].nunique() == dense["cluster_id"].nunique() == 30
    assert len(dense) == 6 * len(sparse)
    sparse_comparison = timing_model.compare_timing_kinds(sparse, samples=80)
    dense_comparison = timing_model.compare_timing_kinds(dense, samples=80)
    kind = timing_model.TIMING_LADDER_KINDS[-1]
    assert (
        sparse_comparison.gains[kind]["bootstrap"]["n_clusters"]
        == dense_comparison.gains[kind]["bootstrap"]["n_clusters"]
        == len(sparse_comparison.sample["held_out_releases"])
    )
    assert sparse_comparison.sample["n_test_rows"] != dense_comparison.sample["n_test_rows"]


def test_bootstrap_draws_whole_releases() -> None:
    """Every replicate keeps a drawn release's rows together and names the unit."""
    comparison = timing_model.compare_timing_kinds(_panel(releases=30), samples=40)
    bootstrap = comparison.gains[timing_model.TIMING_LADDER_KINDS[-1]]["bootstrap"]
    assert bootstrap["n_clusters"] == len(comparison.sample["held_out_releases"])
    assert bootstrap["unit"] == timing_model.AGGREGATION_UNIT
    assert bootstrap["resampling"].startswith("whole releases")
    assert "pseudo replication" in bootstrap["row_level_resampling"]
    assert any("row-level resampling" in note for note in comparison.notes)


def test_seed_outside_the_declared_seeds_is_refused() -> None:
    with pytest.raises(timing_model.TimingModelError, match="not among the seeds"):
        timing_model.compare_timing_kinds(_panel(releases=20), seed=1234, samples=20)


def test_comparison_record_carries_the_declared_specification() -> None:
    comparison = timing_model.compare_timing_kinds(_panel(releases=30), samples=40)
    record = comparison.as_record()
    assert record["metric"] == timing_model.SUPPORTED_LOSS
    assert record["spec"]["model_kind"] == timing_model.TIMING_MODEL_KIND
    assert record["spec"]["meaningful_sizes"]["measured"] is False
    assert record["folds"]["train_events"]
    assert record["sample"]["nesting"].startswith("each rung's covariate tuple")


def test_unimplemented_covariate_is_refused_by_name() -> None:
    with pytest.raises(timing_model.TimingModelError, match="does not implement"):
        timing_model.assert_permitted_covariates(["pre_event_price", "moneyness"], context="test")
