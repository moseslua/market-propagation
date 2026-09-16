"""Behavior tests for the covariate local-projection estimator.

These defend the Level-1 response-estimation contract: a caller can fit the
model the study specification names with genuine pre-event covariates and
shock-by-liquidity interactions, a pre-event input is an asserted and enforced
fact rather than a column name, and simultaneous uncertainty resamples whole
release curves. Assertions are on estimated effects, reported uncertainty and
invalid-input behavior, never on internal wiring.

The estimator centers and scales every regressor on the cell's own rows only as
an internal conditioning step, then reports coefficients, standard errors and
contrasts on the caller's supplied input scale. So the recovered coefficients
below are the structural parameters themselves, in the units of the panel's own
columns, and they are the same estimand at every horizon. The separately named
``standardized_coefficients`` are diagnostic metadata on each horizon's own
scale and are asserted only against the affine relation that defines them.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from market_propagation import models

#: Structural response used by the recovery tests:
#: response = INTERCEPT + SHOCK_SLOPE * shock + DEPTH_SLOPE * depth
#:            + INTERACTION * shock * depth + noise.
INTERCEPT = 0.42
SHOCK_SLOPE = 0.31
DEPTH_SLOPE = 0.09
INTERACTION = 0.24

HORIZONS = (60, 300, 900)


def _known_response_panel(
    *,
    releases: int = 32,
    contracts: int = 3,
    horizons: tuple[int, ...] = HORIZONS,
    noise: float = 0.004,
    seed: int = 20260913,
) -> pd.DataFrame:
    """Deterministic event panel with a known shock-depth interaction.

    Every release contributes several contracts, each with its own pre-event
    depth, so the interaction is identified across contracts *and* releases
    rather than from one contract per release. Each row is observed at several
    horizons, so a curve can be resampled as a whole. ``baseline_time`` is
    strictly before ``event_time`` for every row, which is what makes the
    conventional pre-state columns admissible.
    """
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for index in range(releases):
        release = f"E{index:04d}"
        event_time = pd.Timestamp("2026-03-02T13:30:00Z") + pd.Timedelta(days=index)
        shock = float(rng.normal())
        for contract in range(contracts):
            depth = float(0.4 + 1.8 * rng.random())
            baseline = float(0.3 + 0.4 * rng.random())
            for horizon in horizons:
                response = (
                    INTERCEPT
                    + SHOCK_SLOPE * shock
                    + DEPTH_SLOPE * depth
                    + INTERACTION * shock * depth
                    + float(rng.normal(scale=noise))
                )
                rows.append(
                    {
                        "event_id": release,
                        "cluster_id": release,
                        "family": "employment",
                        "contract_id": f"{release}-C{contract}",
                        "horizon_seconds": horizon,
                        "response": response,
                        "valid": True,
                        "orientation_sign": 1.0,
                        "event_time": event_time,
                        "baseline_time": event_time - pd.Timedelta(seconds=90),
                        "shock": shock,
                        "depth_before": depth,
                        "spread_before": 0.01 + 0.02 * float(rng.random()),
                        "baseline": baseline,
                    }
                )
    panel = pd.DataFrame(rows)
    panel["event_time"] = pd.to_datetime(panel["event_time"], utc=True)
    panel["baseline_time"] = pd.to_datetime(panel["baseline_time"], utc=True)
    return panel


def _structural_truth() -> dict[str, float]:
    """The structural coefficients on the panel's own supplied input scale.

    Because the estimator reports the caller's units, these are the generating
    parameters directly: the shock coefficient is the effect at zero pre-event
    depth, the depth coefficient is the effect at zero shock, and the product is
    the interaction. No per-horizon rescaling is involved.
    """
    return {
        "intercept": INTERCEPT,
        "shock_slope": SHOCK_SLOPE,
        "depth_before": DEPTH_SLOPE,
        "shock_slope_x_depth_before": INTERACTION,
    }


def test_covariate_projection_recovers_a_known_linear_response():
    panel = _known_response_panel()
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=80,
    )
    assert result["status"] == "ok"

    truth = _structural_truth()
    cell = result["cells"]["employment|h=60"]
    assert cell["status"] == "ok"
    coefficients = cell["coefficients"]
    for term in ("intercept", "shock_slope", "depth_before", "shock_slope_x_depth_before"):
        assert coefficients[term] == pytest.approx(truth[term], abs=0.02), term

    # The reported coefficients are the caller's input units, so the same
    # structural parameters must come back at every horizon rather than each
    # horizon reporting its own rescaled number.
    for horizon in HORIZONS:
        other = result["cells"][f"employment|h={horizon}"]["coefficients"]
        assert other["shock_slope"] == pytest.approx(SHOCK_SLOPE, abs=0.02)
        assert other["shock_slope_x_depth_before"] == pytest.approx(INTERACTION, abs=0.02)

    # The estimated uncertainty has to cover the process that generated the data,
    # not merely bracket the point estimate.
    interval = cell["shock_slope_ci"]
    assert interval["status"] == "ok"
    assert interval["lower"] < truth["shock_slope"] < interval["upper"]


def test_the_interaction_changes_the_shock_effect_with_pre_event_depth():
    panel = _known_response_panel()
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=60,
    )
    cell = result["cells"]["employment|h=300"]
    effects = cell["shock_marginal_effects"]
    assert effects["at_plus_one_sd_liquidity"]["value"] > effects["at_mean_liquidity"]["value"]
    assert effects["at_mean_liquidity"]["value"] > effects["at_minus_one_sd_liquidity"]["value"]
    assert effects["by_liquidity_variable"]["depth_before"]["at_plus_one_sd"][
        "value"
    ] == pytest.approx(effects["at_plus_one_sd_liquidity"]["value"])
    assert all(
        effect["standard_error"] is not None
        for effect in (
            effects["at_minus_one_sd_liquidity"],
            effects["at_mean_liquidity"],
            effects["at_plus_one_sd_liquidity"],
        )
    )

    test = cell["interaction_test"]
    assert test["status"] == "ok"
    assert test["terms"] == ["shock_slope_x_depth_before"]
    assert test["degrees_of_freedom"] == 1
    assert test["p_value"] < 0.01, "a genuine interaction must be detected, not reported as noise"
    assert test["note"]


def test_the_fitted_specification_is_named_and_never_adds_event_fixed_effects():
    panel = _known_response_panel(releases=12, contracts=2)
    specification = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before", "baseline"),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
    )["cells"]["employment|h=60"]["specification"]
    assert specification["terms_in_order"] == (
        "intercept",
        "shock_slope",
        "depth_before",
        "baseline",
        "shock_slope_x_depth_before",
    )
    assert specification["event_fixed_effects"] is False
    assert "absorb" in specification["event_fixed_effects_note"]
    assert specification["covariate_availability"]["depth_before"]["availability_column"] == (
        "baseline_time"
    )
    assert specification["covariate_availability"]["baseline"]["availability_column"] == (
        "baseline_time"
    )


def test_multiple_contracts_per_release_keep_one_release_one_cluster():
    panel = _known_response_panel(releases=10, contracts=4)
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
    )
    cell = result["cells"]["employment|h=60"]
    assert cell["n_events"] == 10
    assert cell["n_clusters"] == 10
    assert cell["n_rows"] == 40
    assert cell["cluster_event_counts"] == {f"E{index:04d}": 1 for index in range(10)}
    assert result["n_clusters_used"] == 10


def test_conventional_pre_event_columns_must_precede_the_release():
    panel = _known_response_panel(releases=12, contracts=2)
    # The last three releases are observed at or after their own release, which
    # makes those baseline summaries post-event readings rather than controls.
    late = panel["event_id"].isin(["E0009", "E0010", "E0011"])
    panel.loc[late, "baseline_time"] = panel.loc[late, "event_time"]

    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
    )
    assert result["excluded"]["baseline_not_before_event:depth_before"] == int(late.sum())
    assert result["n_events_used"] == 9
    assert result["status"] == "ok"


def test_a_panel_with_no_pre_event_baseline_is_refused():
    panel = _known_response_panel(releases=8, contracts=2)
    panel["baseline_time"] = panel["event_time"]
    with pytest.raises(models.LocalProjectionError, match="observed before the release"):
        models.local_projections(
            panel,
            shock_column="shock",
            pre_covariates=("depth_before",),
            bootstrap_samples=20,
        )


def test_custom_covariates_require_a_declared_availability_column():
    panel = _known_response_panel(releases=12, contracts=2)
    panel["pre_event_trade_count"] = 5.0 + panel["depth_before"] * 3.0

    with pytest.raises(models.LocalProjectionError, match="declares no observation time"):
        models.local_projections(
            panel,
            shock_column="shock",
            pre_covariates=("pre_event_trade_count",),
            bootstrap_samples=20,
        )

    with pytest.raises(models.LocalProjectionError, match="is not in the panel"):
        models.local_projections(
            panel,
            shock_column="shock",
            pre_covariates=("pre_event_trade_count",),
            pre_event_time_columns={"pre_event_trade_count": "quote_time"},
            bootstrap_samples=20,
        )

    declared = panel.assign(quote_time=panel["baseline_time"])
    result = models.local_projections(
        declared,
        shock_column="shock",
        pre_covariates=("pre_event_trade_count",),
        pre_event_time_columns={"pre_event_trade_count": "quote_time"},
        bootstrap_samples=20,
    )
    assert result["status"] == "ok"
    cell = result["cells"]["employment|h=60"]
    assert cell["coefficients"]["pre_event_trade_count"] is not None
    assert result["pre_event_time_columns"] == {"pre_event_trade_count": "quote_time"}


def test_a_custom_covariate_observed_after_the_release_is_refused():
    panel = _known_response_panel(releases=8, contracts=2)
    panel["flow_after"] = 1.0 + panel["depth_before"]
    panel["flow_time"] = panel["event_time"] + pd.Timedelta(seconds=30)
    with pytest.raises(models.LocalProjectionError, match="observed before the release"):
        models.local_projections(
            panel,
            shock_column="shock",
            pre_covariates=("flow_after",),
            pre_event_time_columns={"flow_after": "flow_time"},
            bootstrap_samples=20,
        )


def test_post_event_columns_are_refused_rather_than_used_as_controls():
    panel = _known_response_panel(releases=8, contracts=2)
    panel["spread_after"] = 0.05
    with pytest.raises(models.LocalProjectionError, match="declared post-event"):
        models.local_projections(
            panel,
            shock_column="shock",
            liquidity_columns=("spread_after",),
            post_event_columns=("spread_after",),
            pre_event_time_columns={"spread_after": "baseline_time"},
            bootstrap_samples=20,
        )


def test_liquidity_columns_without_a_shock_are_refused():
    panel = _known_response_panel(releases=6, contracts=2)
    with pytest.raises(models.LocalProjectionError, match="require a shock_column"):
        models.local_projections(panel, liquidity_columns=("depth_before",))
    with pytest.raises(models.LocalProjectionError, match="require a shock_column"):
        models.local_projections(panel, pre_covariates=("depth_before",))


def test_an_unknown_requested_covariate_is_never_dropped_silently():
    panel = _known_response_panel(releases=6, contracts=2)
    with pytest.raises(models.LocalProjectionError, match="not in the panel"):
        models.local_projections(
            panel,
            shock_column="shock",
            pre_covariates=("prior_volatility",),
            bootstrap_samples=20,
        )


def test_a_missing_covariate_row_is_excluded_and_never_zero_filled():
    panel = _known_response_panel(releases=12, contracts=2)
    gap = panel.index[:6]
    expected = panel.loc[gap, "depth_before"]
    panel.loc[gap, "depth_before"] = np.nan
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
    )
    assert result["excluded"]["missing_depth_before"] == len(gap)
    assert result["n_rows_used"] == len(panel) - len(gap)
    assert result["status"] == "ok"
    # A zero-filled covariate would leave these rows in and pull the covariate
    # mean down; the excluded rows must be absent, not present with a zero.
    assert not np.isclose(expected.mean(), 0.0)
    retained = result["cells"]["employment|h=60"]["coefficients"]["depth_before"]
    assert retained is not None


def test_a_rank_deficient_design_is_inconclusive_with_a_reason():
    panel = _known_response_panel(releases=16, contracts=2)
    panel["depth_before"] = 0.75  # no variation: the interaction cannot be identified
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=20,
    )
    cell = result["cells"]["employment|h=60"]
    assert cell["status"] == "inconclusive"
    assert "rank deficient" in cell["reason"]
    assert cell["interaction_test"] is None
    assert cell["shock_slope"] is None
    assert result["status"] == "partially_inconclusive"
    assert cell["reason"] in result["reasons"]


def test_too_few_releases_is_inconclusive_rather_than_estimated():
    panel = _known_response_panel(releases=3, contracts=2)
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=20,
    )
    cell = result["cells"]["employment|h=60"]
    assert cell["status"] == "inconclusive"
    assert "min_events=4" in cell["reason"]
    assert cell["coefficients"] is None
    assert result["status"] == "partially_inconclusive"


def test_a_design_with_too_few_releases_for_its_parameters_is_inconclusive():
    panel = _known_response_panel(releases=5, contracts=2)
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before", "baseline", "spread_before"),
        liquidity_columns=("depth_before", "spread_before"),
        bootstrap_samples=20,
    )
    cell = result["cells"]["employment|h=60"]
    assert cell["status"] == "inconclusive"
    assert "more releases than parameters" in cell["reason"]
    assert cell["coefficient_standard_errors"] is None
    assert cell["interaction_test"] is None


def test_shared_release_draws_align_a_curve_that_has_a_missing_horizon():
    panel = _known_response_panel(releases=20, contracts=2)
    # Six releases are never quoted at the 300s endpoint. They must drop out of
    # that horizon while the curve keeps resampling the same releases.
    missing = panel["event_id"].isin([f"E{index:04d}" for index in range(6)])
    panel = panel.loc[~(missing & (panel["horizon_seconds"] == 300))]

    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=60,
    )
    curve = result["curves"]["employment"]
    assert curve["horizons_seconds"] == [60, 300, 900]
    assert curve["shared_release_draws"]["n_releases_in_curve"] == 20

    keys = set()
    counts = set()
    for horizon in curve["horizons_seconds"]:
        bootstrap = result["cells"][f"employment|h={horizon}"]["bootstrap"]
        keys.add(bootstrap["draw_key"])
        counts.add(len(bootstrap["replicates"]["shock_slope"]))
        assert bootstrap["shared_curve_draws"] is True
        assert bootstrap["curve_releases_resampled"] == 20
    # One draw for the whole curve: identical key and one replicate per draw at
    # every horizon, so the band is aligned by the releases actually resampled.
    assert len(keys) == 1
    assert counts == {60}
    assert result["cells"]["employment|h=300"]["bootstrap"]["cell_releases_used"] == 14
    assert result["cells"]["employment|h=60"]["bootstrap"]["cell_releases_used"] == 20

    band = curve["simultaneous_band"]
    assert band["status"] == "ok"
    assert band["critical_value"] is not None
    assert band["n_usable_cells"] == 3
    assert all(value is not None for value in band["simultaneous_lower"])


def test_shared_draws_are_reproducible_and_change_with_the_seed():
    panel = _known_response_panel(releases=12, contracts=2)
    first = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
        seed=11,
    )
    repeat = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
        seed=11,
    )
    changed = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
        seed=12,
    )
    key = first["curves"]["employment"]["shared_release_draws"]["draw_key"]
    assert repeat["curves"]["employment"]["shared_release_draws"]["draw_key"] == key
    assert changed["curves"]["employment"]["shared_release_draws"]["draw_key"] != key
    assert (
        first["cells"]["employment|h=60"]["bootstrap"]["replicates"]
        == repeat["cells"]["employment|h=60"]["bootstrap"]["replicates"]
    )
    assert (
        first["cells"]["employment|h=60"]["bootstrap"]["replicates"]
        != changed["cells"]["employment|h=60"]["bootstrap"]["replicates"]
    )


def test_a_curve_keeps_families_separate_with_covariates():
    panel = _known_response_panel(releases=12, contracts=2)
    # A second release class with its own shock response and no interaction; it
    # must be pooled separately, never averaged into the first family's curve.
    other = panel.assign(family="cpi", response=1.5 * panel["shock"])
    combined = pd.concat([panel, other], ignore_index=True)
    result = models.local_projections(
        combined,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
    )
    assert set(result["curves"]) == {"employment", "cpi"}
    for family in ("employment", "cpi"):
        curve = result["curves"][family]
        assert set(curve["coefficients_by_horizon"]) == {"60", "300", "900"}
        assert set(curve["interaction_test_by_horizon"]) == {"60", "300", "900"}
        for cell in result["cells"].values():
            if cell["family"] == family:
                assert cell["coefficients"]["shock_slope"] is not None
    employment = result["cells"]["employment|h=60"]["coefficients"]
    cpi = result["cells"]["cpi|h=60"]["coefficients"]
    assert cpi["shock_slope_x_depth_before"] == pytest.approx(0.0, abs=0.02)
    assert employment["shock_slope_x_depth_before"] == pytest.approx(INTERACTION, abs=0.02)
    # Same input units on both families, so the difference is a real effect
    # difference rather than a difference in each cell's standardization.
    assert employment["shock_slope"] == pytest.approx(SHOCK_SLOPE, abs=0.02)
    assert cpi["shock_slope"] == pytest.approx(1.5, abs=0.02)


def test_no_covariate_requests_keep_the_existing_mean_response_reports():
    panel = _known_response_panel(releases=16, contracts=2)
    result = models.local_projections(panel, bootstrap_samples=30)
    assert result["primary_metric"] == "mean_response"
    assert result["identification"].startswith("descriptive only")
    assert result["pre_covariates"] == []
    assert result["liquidity_columns"] == []
    assert result["shock_column"] is None
    assert result["units"] == "probability points"
    assert "no shock column" in result["specification"]
    for cell in result["cells"].values():
        assert cell["shock_slope"] is None
        assert cell["shock_slope_ci"] is None
        assert cell["shock_slope_status"] == "not_requested"
        assert cell["mean_response"] == pytest.approx(
            float(panel.loc[panel["horizon_seconds"] == cell["horizon_seconds"], "response"].mean())
        )
        assert "specification" not in cell
    for family_cells in result["curves"].values():
        assert family_cells["curve_metric"] == "mean_response"
        assert all(value is not None for value in family_cells["mean_response"])


def test_a_shock_without_covariates_keeps_the_event_level_slope():
    panel = _known_response_panel(releases=16, contracts=2)
    result = models.local_projections(panel, shock_column="shock", bootstrap_samples=30)
    assert result["primary_metric"] == "shock_slope"
    assert result["pre_covariates"] == []
    assert result["liquidity_columns"] == []
    assert result["units"] == "probability points per unit of the supplied shock column"
    cell = result["cells"]["employment|h=60"]
    assert cell["status"] == "ok"
    assert cell["shock_slope"] is not None
    assert cell["shock_slope_ci"]["status"] == "ok"
    assert "specification" not in cell
    assert "rank" in cell
    # The shock column is supplied in raw units, so the event-level path reports
    # the raw between-release slope directly rather than a standardized one.
    cell_shocks = panel.loc[panel["horizon_seconds"] == 60].groupby("event_id")["shock"].mean()
    cell_responses = (
        panel.loc[panel["horizon_seconds"] == 60].groupby("event_id")["response"].mean()
    )
    raw_reference = np.linalg.lstsq(
        np.column_stack([np.ones(len(cell_shocks)), cell_shocks.to_numpy(dtype=float)]),
        cell_responses.loc[cell_shocks.index].to_numpy(dtype=float),
        rcond=None,
    )[0][1]
    assert cell["shock_slope"] == pytest.approx(raw_reference, abs=1e-6)

    interfaced = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=30,
    )
    with_interface = interfaced["cells"]["employment|h=60"]
    assert with_interface["shock_slope"] != cell["shock_slope"]
    assert with_interface["specification"]["covariate_columns"] == ["depth_before"]


def test_conventional_pre_state_and_liquidity_columns_are_accepted_together():
    panel = _known_response_panel(releases=16, contracts=3)
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("baseline", "spread_before", "depth_before"),
        liquidity_columns=("depth_before", "spread_before"),
        bootstrap_samples=30,
    )
    cell = result["cells"]["employment|h=900"]
    assert cell["status"] == "ok"
    assert cell["specification"]["terms_in_order"] == (
        "intercept",
        "shock_slope",
        "baseline",
        "spread_before",
        "depth_before",
        "shock_slope_x_depth_before",
        "shock_slope_x_spread_before",
    )
    test = cell["interaction_test"]
    assert test["degrees_of_freedom"] == 2
    assert set(test["terms"]) == {
        "shock_slope_x_depth_before",
        "shock_slope_x_spread_before",
    }
    assert cell["leave_one_event_out_slope"] is not None
