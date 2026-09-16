"""The input-unit contract for local-projection coefficients and curves.

The estimator centers and scales its regressors on each cell's own rows only to
condition the solve. Everything the caller reads back is on the supplied input
scale: the coefficients, their standard errors, the marginal-effect contrasts and
therefore the simultaneous band, the overshoot and the settling diagnostics.
Because that scale is fixed at the caller's, one horizon-comparable curve is
possible; the standardized values are reported only under explicitly named
diagnostic fields and are never the primary series.

These tests defend that contract against the two ways it can break. First, a
release that is missing at one horizon must not shrink that horizon's regressor
dispersion into a different effect for the same input. Second, a standardized
fit with interaction terms must be back-transformed including the mean shifts of
every factor, not by dividing coefficients by standard deviations alone.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from market_propagation import models

#: The generating response in the constant-effect regressions:
#: response = TRUE_SLOPE * shock, with no horizon or release dependence.
TRUE_SLOPE = 0.02

#: Structural response for the back-transformation regression:
#: response = INTERCEPT + SHOCK_SLOPE * shock + DEPTH_SLOPE * depth
#:            + INTERACTION * shock * depth + noise.
INTERCEPT = 0.42
SHOCK_SLOPE = 0.31
DEPTH_SLOPE = 0.09
INTERACTION = 0.24

#: Exact-response coefficients for the cross-horizon covariate regression, where
#: an exactly linear response identifies the structural values on any sample.
DEPTH_SLOPE_TRUE = 0.1
INTERACTION_TRUE = 0.05

#: The exact shock grid the audit probe uses. The 300s horizon keeps only the
#: releases inside one unit, which is what changes the shock's own dispersion
#: between horizons while the input-unit effect stays constant.
PROBE_SHOCKS = (-3.0, -2.0, -1.0, -0.5, 0.5, 1.0, 2.0, 3.0)


def _panel_row(
    *,
    index: int,
    shock: float,
    depth: float,
    response: float,
    horizon: int,
    days: int,
) -> dict[str, object]:
    event_time = pd.Timestamp("2025-01-01T13:30:00Z") + pd.Timedelta(days=index)
    return {
        "event_id": f"E{index:04d}",
        "cluster_id": f"E{index:04d}",
        "family": "cpi",
        "horizon_seconds": horizon,
        "response": response,
        "shock": shock,
        "depth_before": depth,
        "valid": True,
        "event_time": event_time,
        "baseline_time": event_time - pd.Timedelta(seconds=days),
    }


def _probe_panel() -> pd.DataFrame:
    """The audit probe's panel: an exactly constant effect per input unit.

    Eight releases at 60s, but only the four releases inside one shock unit at
    300s. The response carries no noise, so any departure of a reported
    coefficient from ``TRUE_SLOPE`` is the estimator's own rescaling rather than
    sampling variation.
    """
    rows: list[dict[str, object]] = []
    for index, shock in enumerate(PROBE_SHOCKS):
        for horizon in (60, 300):
            if horizon == 300 and abs(shock) > 1.0:
                continue
            rows.append(
                _panel_row(
                    index=index,
                    shock=shock,
                    depth=1.0,
                    response=TRUE_SLOPE * shock,
                    horizon=horizon,
                    days=90,
                )
            )
    return pd.DataFrame(rows)


def _raw_ols_reference(
    panel: pd.DataFrame, *, horizon: int
) -> tuple[dict[str, float], dict[str, float]]:
    """Independent raw-unit reference: OLS plus release-clustered CR1 errors.

    Computed here from the panel's own columns with no standardization at all, so
    it is the quantity the estimator is supposed to reproduce rather than a
    restatement of how the estimator computes it.
    """
    cell = panel.loc[panel["horizon_seconds"] == horizon]
    shock = cell["shock"].to_numpy(dtype=float)
    depth = cell["depth_before"].to_numpy(dtype=float)
    response = cell["response"].to_numpy(dtype=float)
    clusters = cell["cluster_id"].to_numpy()
    design = np.column_stack([np.ones_like(shock), shock, depth, shock * depth])
    solution, *_ = np.linalg.lstsq(design, response, rcond=None)
    residuals = response - design @ solution
    bread = np.linalg.pinv(design.T @ design)
    meat = np.zeros((design.shape[1], design.shape[1]), dtype=float)
    for cluster in np.unique(clusters):
        member = clusters == cluster
        score = design[member].T @ residuals[member]
        meat += np.outer(score, score)
    n_rows = float(design.shape[0])
    n_parameters = float(design.shape[1])
    n_clusters = float(len(np.unique(clusters)))
    correction = (n_clusters / (n_clusters - 1.0)) * ((n_rows - 1.0) / (n_rows - n_parameters))
    covariance = correction * bread @ meat @ bread
    names = ("intercept", "shock_slope", "depth_before", "shock_slope_x_depth_before")
    coefficients = {name: float(solution[position]) for position, name in enumerate(names)}
    standard_errors = {
        name: float(np.sqrt(np.diag(covariance))[position]) for position, name in enumerate(names)
    }
    return coefficients, standard_errors


def _interaction_panel(*, releases: int = 28, seed: int = 20260913) -> pd.DataFrame:
    """Nonzero means and non-unit scales on both regressors, with an interaction.

    The shock is centred near 6 with a spread near 3 and depth near -2 with a
    spread near 0.7, so dividing a standardized coefficient by a standard
    deviation alone cannot recover the supplied-unit coefficient: the mean shifts
    of both factors have to be carried through the back-transformation too.
    """
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for index in range(releases):
        shock = float(6.0 + 3.0 * rng.normal())
        depth = float(-2.0 + 0.7 * rng.normal())
        response = (
            INTERCEPT
            + SHOCK_SLOPE * shock
            + DEPTH_SLOPE * depth
            + INTERACTION * shock * depth
            + float(rng.normal(scale=0.01))
        )
        for horizon in (60, 300):
            rows.append(
                _panel_row(
                    index=index,
                    shock=shock,
                    depth=depth,
                    response=response,
                    horizon=horizon,
                    days=90,
                )
            )
    return pd.DataFrame(rows)


def test_constant_effect_probe_reports_the_input_unit_at_both_horizons():
    panel = _probe_panel()
    result = models.local_projections(
        panel,
        shock_column="shock",
        orient_response=False,
        bootstrap_samples=20,
    )
    curve = result["curves"]["cpi"]
    assert curve["horizons_seconds"] == [60, 300]

    slopes = curve["shock_slope"]
    # The response is TRUE_SLOPE per input unit at both horizons, so a reported
    # coefficient that differs by horizon is the estimator renormalizing, not the
    # data. This is the exact artifact the audit probe measured.
    assert slopes[0] == pytest.approx(TRUE_SLOPE, abs=1e-9)
    assert slopes[1] == pytest.approx(TRUE_SLOPE, abs=1e-9)
    assert slopes[0] == pytest.approx(slopes[1], abs=1e-9)

    # Missing extremes at one horizon must not manufacture curve heterogeneity.
    settling = curve["settling"]
    assert abs(settling["overshoot"]) < 1e-9, settling["overshoot"]
    assert abs(settling["signed_overshoot"]) < 1e-9, settling["signed_overshoot"]
    assert settling["slope_status"] == "ok"

    band = curve["simultaneous_band"]
    assert band["status"] == "ok"
    for lower, upper in zip(band["simultaneous_lower"], band["simultaneous_upper"], strict=True):
        assert lower == pytest.approx(TRUE_SLOPE, abs=1e-6)
        assert upper == pytest.approx(TRUE_SLOPE, abs=1e-6)
    assert result["units"] == "probability points per unit of the supplied shock column"

    # The standardized values still differ by horizon, which is exactly why they
    # cannot be the reported curve. Their presence keeps the old convention
    # available as named diagnostic metadata instead of as the primary series.
    standardized = [
        result["cells"][f"cpi|h={horizon}"]["standardized_shock_slope"] for horizon in (60, 300)
    ]
    assert standardized[0] != pytest.approx(standardized[1], abs=1e-6)
    assert standardized[0] != pytest.approx(TRUE_SLOPE, abs=1e-6)


def test_covariate_projection_keeps_one_input_unit_across_horizons():
    """The same cross-horizon comparability holds on the row-level design path."""

    def covariate_row(index: int, shock: float, depth: float) -> dict[str, object]:
        response = TRUE_SLOPE * shock + DEPTH_SLOPE_TRUE * depth + INTERACTION_TRUE * shock * depth
        return _panel_row(
            index=index,
            shock=shock,
            depth=depth,
            response=response,
            horizon=60,
            days=90,
        )

    # Sixteen releases at 60s, and the eight inside two shock units at 300s, so
    # both horizons clear the design's parameter count while the shock's own
    # dispersion differs sharply between them.
    shocks = (
        -8.0,
        -6.0,
        -4.0,
        -3.0,
        -2.0,
        -1.5,
        -1.0,
        -0.5,
        0.5,
        1.0,
        1.5,
        2.0,
        3.0,
        4.0,
        6.0,
        8.0,
    )
    rows: list[dict[str, object]] = []
    for index, shock in enumerate(shocks):
        for contract in range(2):
            depth = float(0.5 + 0.3 * contract + 0.1 * index)
            for horizon in (60, 300):
                if horizon == 300 and abs(shock) > 2.05:
                    continue
                row = covariate_row(index, shock, depth)
                row["horizon_seconds"] = horizon
                rows.append(row)
    panel = pd.DataFrame(rows)
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=20,
    )
    cell_60 = result["cells"]["cpi|h=60"]
    cell_300 = result["cells"]["cpi|h=300"]
    assert cell_60["status"] == "ok" and cell_300["status"] == "ok"

    for cell in (cell_60, cell_300):
        coefficients = cell["coefficients"]
        # An exact linear response identifies the structural coefficients on any
        # sample, so every horizon must return the supplied-unit values.
        assert coefficients["shock_slope"] == pytest.approx(TRUE_SLOPE, abs=1e-9)
        assert coefficients["depth_before"] == pytest.approx(DEPTH_SLOPE_TRUE, abs=1e-9)
        assert coefficients["shock_slope_x_depth_before"] == pytest.approx(
            INTERACTION_TRUE, abs=1e-9
        )

    assert "as supplied by the caller" in cell_60["coefficient_units"]
    assert "not comparable across horizons" in cell_60["standardized_coefficient_units"]

    # The regression authority: the same input-unit coefficient is reported at
    # both horizons, while each cell's own shock scale, and therefore its
    # standardized diagnostic, differs by roughly a factor of three.
    assert cell_60["shock_scale"] > 2.0 * cell_300["shock_scale"]
    assert cell_60["standardized_shock_slope"] > 2.0 * cell_300["standardized_shock_slope"]
    curve = result["curves"]["cpi"]
    assert curve["coefficients_by_horizon"]["60"]["shock_slope"] == pytest.approx(
        TRUE_SLOPE, abs=1e-9
    )
    assert curve["coefficients_by_horizon"]["300"]["shock_slope"] == pytest.approx(
        TRUE_SLOPE, abs=1e-9
    )
    assert "not comparable across horizons" in curve["standardized_coefficient_units"]
    assert curve["simultaneous_band"]["status"] == "ok"


def test_standardized_interaction_back_transforms_to_the_supplied_units():
    panel = _interaction_panel()
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=40,
    )
    cell = result["cells"]["cpi|h=60"]
    assert cell["status"] == "ok"

    reference, reference_errors = _raw_ols_reference(panel, horizon=60)
    for name, expected in reference.items():
        assert cell["coefficients"][name] == pytest.approx(expected, abs=1e-9), name
        assert cell["coefficient_standard_errors"][name] == pytest.approx(
            reference_errors[name], abs=1e-9
        ), name

    # The structural parameters are recovered on the supplied scale, so both
    # regressor means being nonzero has not shifted them.
    assert cell["coefficients"]["shock_slope"] == pytest.approx(SHOCK_SLOPE, abs=0.01)
    assert cell["coefficients"]["depth_before"] == pytest.approx(DEPTH_SLOPE, abs=0.02)
    assert cell["coefficients"]["shock_slope_x_depth_before"] == pytest.approx(
        INTERACTION, abs=0.01
    )

    # The two reported families are related by the exact affine map between their
    # design columns: the raw design, expressed in the standardized columns, maps
    # standardized coefficients onto raw ones. A back-transformation that ignored
    # the mean shifts would fail this.
    cell_rows = panel.loc[panel["horizon_seconds"] == 60]
    shock_raw = cell_rows["shock"].to_numpy(dtype=float)
    depth_raw = cell_rows["depth_before"].to_numpy(dtype=float)
    shock_z = (shock_raw - shock_raw.mean()) / shock_raw.std(ddof=0)
    depth_z = (depth_raw - depth_raw.mean()) / depth_raw.std(ddof=0)
    design_raw = np.column_stack(
        [np.ones_like(shock_raw), shock_raw, depth_raw, shock_raw * depth_raw]
    )
    design_standardized = np.column_stack(
        [np.ones_like(shock_z), shock_z, depth_z, shock_z * depth_z]
    )
    back_transform = np.linalg.lstsq(design_raw, design_standardized, rcond=None)[0]
    names = ("intercept", "shock_slope", "depth_before", "shock_slope_x_depth_before")
    raw_vector = np.array([cell["coefficients"][name] for name in names])
    standardized_vector = np.array([cell["standardized_coefficients"][name] for name in names])
    mapped = back_transform @ standardized_vector
    for position, name in enumerate(names):
        assert raw_vector[position] == pytest.approx(float(mapped[position]), abs=1e-9), name

    # The naive back-transformation is genuinely wrong here: dividing the
    # standardized shock coefficient by the shock spread alone, without removing
    # the interaction term's mean-shift contribution, lands far from the reported
    # input-unit coefficient. So the check above has teeth.
    shock_spread = cell["shock_scale"]
    depth_mean = float(depth_raw.mean())
    naive_shock = standardized_vector[1] / shock_spread
    assert naive_shock != pytest.approx(cell["coefficients"]["shock_slope"], abs=0.05)
    # The mean-shift correction is exactly the interaction times liquidity's mean.
    assert cell["coefficients"]["shock_slope"] == pytest.approx(
        naive_shock - cell["coefficients"]["shock_slope_x_depth_before"] * depth_mean,
        abs=1e-9,
    )

    # Uncertainty is on the same input scale as the point estimate: the reported
    # standard errors equal the independent raw-unit reference above, and they are
    # not the standardized standard errors relabelled.
    assert cell["coefficient_standard_errors"]["shock_slope"] == pytest.approx(
        reference_errors["shock_slope"], abs=1e-9
    )
    assert cell["coefficient_standard_errors"]["shock_slope_x_depth_before"] == pytest.approx(
        reference_errors["shock_slope_x_depth_before"], abs=1e-9
    )
    # The raw and standardized standard errors differ sharply, so the equality
    # above is a real test of the covariance transform and not a tautology.
    assert cell["coefficient_standard_errors"]["shock_slope"] != pytest.approx(
        abs(standardized_vector[1]) / shock_spread, rel=0.1
    )


def test_the_interaction_gate_is_invariant_to_the_units_reported():
    """Back-transformation must not move the inference gate.

    The Wald statistic is invariant under the invertible affine reparameterization
    between the standardized and supplied-unit designs, so the reported gate
    statistic and p-value must equal what the caller's own units would produce.
    """
    panel = _interaction_panel()
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=40,
    )
    cell = result["cells"]["cpi|h=60"]
    test = cell["interaction_test"]
    assert test["status"] == "ok"

    cell_rows = panel.loc[panel["horizon_seconds"] == 60]
    shock = cell_rows["shock"].to_numpy(dtype=float)
    depth = cell_rows["depth_before"].to_numpy(dtype=float)
    response = cell_rows["response"].to_numpy(dtype=float)
    clusters = cell_rows["cluster_id"].to_numpy()
    shock_z = (shock - shock.mean()) / shock.std(ddof=0)
    depth_z = (depth - depth.mean()) / depth.std(ddof=0)

    def wald(design: np.ndarray, interaction_position: int) -> float:
        solution, *_ = np.linalg.lstsq(design, response, rcond=None)
        residuals = response - design @ solution
        bread = np.linalg.pinv(design.T @ design)
        meat = np.zeros((design.shape[1], design.shape[1]), dtype=float)
        for cluster in np.unique(clusters):
            member = clusters == cluster
            score = design[member].T @ residuals[member]
            meat += np.outer(score, score)
        n_rows, n_parameters, n_clusters = (
            float(design.shape[0]),
            float(design.shape[1]),
            float(len(np.unique(clusters))),
        )
        correction = (n_clusters / (n_clusters - 1.0)) * ((n_rows - 1.0) / (n_rows - n_parameters))
        covariance = correction * bread @ meat @ bread
        value = float(solution[interaction_position])
        variance = float(covariance[interaction_position, interaction_position])
        return value * value / variance

    raw_design = np.column_stack([np.ones_like(shock), shock, depth, shock * depth])
    standardized_design = np.column_stack(
        [np.ones_like(shock_z), shock_z, depth_z, shock_z * depth_z]
    )
    assert test["statistic"] == pytest.approx(wald(raw_design, 3), rel=1e-9)
    assert wald(raw_design, 3) == pytest.approx(wald(standardized_design, 3), rel=1e-9)


def test_marginal_effect_contrasts_are_reported_in_the_supplied_units():
    panel = _interaction_panel()
    result = models.local_projections(
        panel,
        shock_column="shock",
        pre_covariates=("depth_before",),
        liquidity_columns=("depth_before",),
        bootstrap_samples=40,
    )
    cell = result["cells"]["cpi|h=60"]
    effects = cell["shock_marginal_effects"]
    depth_mean = float(panel.loc[panel["horizon_seconds"] == 60, "depth_before"].mean())

    # The interact action is the slope in shock with depth at its mean, which is
    # the raw shock coefficient plus the interaction times that mean. Because
    # depth's mean is not zero, it deliberately differs from the raw shock
    # coefficient and is not merely a relabelled standardized zero.
    at_mean = effects["at_mean_liquidity"]
    assert at_mean["value"] == pytest.approx(
        cell["coefficients"]["shock_slope"]
        + cell["coefficients"]["shock_slope_x_depth_before"] * depth_mean,
        abs=1e-9,
    )
    assert at_mean["value"] != pytest.approx(cell["coefficients"]["shock_slope"], abs=0.01)
    assert at_mean["standard_error"] > 0.0
    assert "per unit of the shock column" in effects["definition"]

    # One-standard-deviation contrasts move depth by one of its own input units,
    # and the per-variable contrast coincides with the all-together contrast when
    # a single liquidity variable was requested.
    depth_spread = float(panel.loc[panel["horizon_seconds"] == 60, "depth_before"].std(ddof=0))
    delta = cell["coefficients"]["shock_slope_x_depth_before"]
    assert effects["at_plus_one_sd_liquidity"]["value"] == pytest.approx(
        at_mean["value"] + delta * depth_spread, abs=1e-9
    )
    assert effects["at_minus_one_sd_liquidity"]["value"] == pytest.approx(
        at_mean["value"] - delta * depth_spread, abs=1e-9
    )
    single = effects["by_liquidity_variable"]["depth_before"]
    assert single["at_plus_one_sd"]["value"] == pytest.approx(
        effects["at_plus_one_sd_liquidity"]["value"]
    )
    assert single["at_minus_one_sd"]["value"] == pytest.approx(
        effects["at_minus_one_sd_liquidity"]["value"]
    )

    # Intervals are on the same supplied scale as the point estimate: the
    # standardized coefficient is not inside the raw-unit interval by accident.
    assert cell["shock_slope_ci"]["lower"] < cell["coefficients"]["shock_slope"]
    assert cell["coefficients"]["shock_slope"] < cell["shock_slope_ci"]["upper"]
    assert cell["standardized_shock_slope"] == pytest.approx(
        cell["standardized_coefficients"]["shock_slope"]
    )
