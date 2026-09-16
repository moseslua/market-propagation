"""Nested forecast models and event-study local projections.

The model ladder is configured by ``model_id`` in ``configs/study_v1.yaml``: a
no-change forecast, an own-market autoregression, a release-surprise model, and
a regularized sparse network model. The contemporaneous target price is a fitted
regressor of the own, news and network kinds rather than only the anchor of the
bounded link, so the ladder can express level-dependent responses. News and
network forecasts share one declared own-market, release-surprise and
observation-delay covariate tuple and one validation-selected penalty grid, so
the network model can only earn its place by adding admissible lagged neighbour
information. Exact logical complements are collapsed rather than fitted twice,
while genuinely distinct controls are kept. A network model that does not beat
the shared-news model is not promoted, and no coefficient here carries a causal
label.

Targets are bounded future probability changes, never terminal payouts. A
prediction is produced by applying a documented bounded mapping to an
unconstrained linear predictor,

    level = clip(current_price + SCALE * tanh(delta / SCALE), CLIP, 1 - CLIP)
    predicted_change = level - current_price

so the reported quantity is always inside the admissible probability range and
the clipping is reported with every evaluation. Terminal binary scoring lives
in :mod:`market_propagation.evaluation` and is scored separately.

Missingness is explicit. Every fit records its exact training feature names,
scaling, parameters, cutoff, and event clusters; every model in a nested
comparison is fit and evaluated on one identical held-out row set, so a missing
feature row cannot silently change the comparison sample.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from .evaluation import (
    ForecastEvaluationError,
    chronological_splits,
    clustered_bootstrap,
    curve_uncertainty,
    forecast_scores,
    weighted_event_slope,
)
from .simulation import NEIGHBOR_COLUMNS

__all__ = [
    "BOUNDED_LINK",
    "DEFAULT_ALPHAS",
    "FEATURE_SPECS",
    "FIXED_PENALTY_KINDS",
    "FORECAST_ESTIMATOR",
    "FROZEN_PARAMETERS",
    "INTERPRETATION_LABEL",
    "MODEL_FAMILY_LABEL",
    "MODEL_KINDS",
    "PROJECTION_COEFFICIENT_UNITS",
    "PROJECTION_STANDARDIZED_COEFFICIENT_UNITS",
    "PROMOTION_GATE",
    "REDUNDANT_FEATURE_COLUMNS",
    "FittedModel",
    "FoldSplit",
    "ForecastDataError",
    "LocalProjectionError",
    "ModelFitError",
    "NestedComparisonResult",
    "evaluate",
    "fit",
    "local_projections",
    "nested_comparison",
]

#: Model kinds of the ladder, in nested order.
MODEL_KINDS: tuple[str, ...] = ("no_change", "own", "news", "network")

#: Feature columns of each kind. Each kind's set contains the previous kind's
#: set, so the ladder is genuinely nested. ``current_price`` is a real regressor:
#: the target is a future level change, so the current level is the forecast's
#: reference state and the model may express a level-dependent response rather
#: than being forced through one fixed anchor.
FEATURE_SPECS: Mapping[str, tuple[str, ...]] = {
    "no_change": (),
    "own": ("own_lag", "current_price"),
    "news": ("own_lag", "current_price", "shock", "delayed_shock"),
    "network": (
        "own_lag",
        "current_price",
        "shock",
        "delayed_shock",
        "neighbor_lag",
        "neighbor_lag_control",
    ),
}

#: Neighbour columns that are an exact logical restatement of another column in
#: the same admissible set, so fitting both would only split one coefficient
#: pair. They stay in the forecast table for audit and are recorded here rather
#: than entering an unconstrained design matrix twice. A column is listed here
#: only on an exact identity, never because two series correlate.
REDUNDANT_FEATURE_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "network": ("neighbor_lag_complement",),
}

#: Ridge penalty grid. It is used for every tuned kind, and the selected value
#: is recorded on the frozen model. News and network are selected on the same
#: grid over the same validation rows, so the comparison is about the admissible
#: neighbour columns rather than about one kind being handed a fixed penalty the
#: other is not.
DEFAULT_ALPHAS: tuple[float, ...] = (0.001, 0.01, 0.1, 1.0, 10.0)

#: Kinds whose penalty is fixed rather than data-selected, because they have no
#: tuning at all. Every other kind selects its ``alpha`` on validation rows.
FIXED_PENALTY_KINDS: Mapping[str, float] = {
    "no_change": 0.0,
    "own": 0.0,
}

#: Recorded starting point of the frozen parameters per kind. Only the fixed-penalty
#: kinds keep a concrete ``alpha`` here; the tuned kinds have already written
#: their ``alpha`` onto the model and start from ``None``, which the fit replaces
#: with the validation-selected value.
FROZEN_PARAMETERS: Mapping[str, Mapping[str, Any]] = {
    "no_change": {"parameters": {}},
    "own": {"parameters": {"alpha": 0.0, "alpha_selection": "fixed"}},
    "news": {"parameters": {"alpha": None, "alpha_selection": "validation"}},
    "network": {"parameters": {"alpha": None, "alpha_selection": "validation"}},
}

#: Documented bounded link. ``SCALE`` bounds the modelled probability shift in
#: probability points; ``CLIP`` bounds the reported level away from the
#: absorbing states 0 and 1. Both are recorded on every evaluation.
BOUNDED_LINK: Mapping[str, float | str] = {
    "form": "clip(current_price + scale * tanh(delta / scale), clip, 1 - clip)",
    "scale": 100.0,
    "clip": 1e-06,
}

MODEL_FAMILY_LABEL = "predictive conditional propagation"
INTERPRETATION_LABEL = (
    "prespecified predictive comparison: a conditional association built only from "
    "admissible lagged information, not an intervention effect"
)

#: Identity of the production forecast estimator, its split authority and its
#: metric. The promotion gate accepts a null assessment only when that
#: assessment was produced by this same estimator on the same metric, threshold,
#: time settings and event count, so a rate from an unrelated pipeline -- for
#: example a between-release response-slope power report -- cannot unlock a
#: network forecast gate.
FORECAST_ESTIMATOR: Mapping[str, Any] = {
    "estimator": "nested_holdout_news_vs_network",
    "engine": "validation_selected_ridge_with_bounded_link",
    "split": "evaluation.chronological_splits",
    "metric": "probability_point_mae",
    "target": "future_probability_change",
    "unit": "economic_release",
}

#: Promotion rule: the network model must beat the shared-news baseline by the
#: smallest effect registered for this study, on the same held-out rows, and the
#: supplied null assessment must come from this same estimator. The registered
#: default matches ``configs/study_v1.yaml``
#: ``thresholds.smallest_relevant_mae_gain_probability_points``; callers pass the
#: value explicitly through ``nested_comparison(minimum_mae_gain=...)``.
PROMOTION_GATE: Mapping[str, Any] = {
    "metric": "probability_point_mae",
    "direction": "lower_is_better",
    "baseline_kind": "news",
    "candidate_kind": "network",
    "default_minimum_mae_gain": 0.005,
    "requires": (
        "the candidate must beat the baseline on identical held-out rows by at least "
        "the caller's minimum MAE gain, and the supplied common-news-plus-delay null "
        "assessment must have been produced by this same estimator, metric, threshold, "
        "time settings and event count"
    ),
}

_REQUIRED_TABLE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "contract_id",
    "event_time",
    "horizon_seconds",
    "target",
    "current_price",
    "own_lag",
)


class ForecastDataError(ValueError):
    """The supplied table is not a forecast table or violates the row contract."""


class ModelFitError(RuntimeError):
    """The model could not be fit on the supplied rows."""


#: Pre-event state columns whose observation time the event panel already
#: records: each is a summary of the baseline quote, so it inherits the baseline
#: time and must precede the release. Any other covariate has to declare the
#: column holding its own observation time, because a column whose name does not
#: look post-event is not evidence that it is a baseline control.
CONVENTIONAL_PRE_EVENT_COLUMNS: tuple[str, ...] = (
    "baseline",
    "spread_before",
    "depth_before",
)

#: The one unit convention every reported projection coefficient, standard error
#: and contrast uses: the caller's supplied regressor units. Centering and scaling
#: are an internal conditioning step only and never change it, so a coefficient is
#: the same estimand at every horizon and its curve is horizon-comparable.
PROJECTION_COEFFICIENT_UNITS = (
    "probability points per unit of each regressor exactly as supplied by the caller; every "
    "coefficient, standard error and contrast is on that one input scale at every horizon"
)

#: The explicitly named diagnostic metadata, never the primary series: each
#: regressor is centered and scaled on its own cell's rows, so these values are not
#: the caller's units and are not comparable across horizons.
PROJECTION_STANDARDIZED_COEFFICIENT_UNITS = (
    "diagnostic metadata only: probability points per standard deviation of each regressor as "
    "conditioned on that cell's own rows, so unlike the supplied-unit coefficients these values "
    "are not comparable across horizons and are not a curve"
)


class LocalProjectionError(ValueError):
    """Local-projection inputs violate the stated panel contract."""


@dataclass(frozen=True, slots=True)
class FittedModel:
    """A frozen model: spec, transforms, coefficients, provenance and cutoff."""

    kind: str
    spec: Mapping[str, Any]
    feature_names: tuple[str, ...]
    feature_means: Mapping[str, float]
    feature_scales: Mapping[str, float]
    coefficients: Mapping[str, float]
    intercept: float
    parameters: Mapping[str, Any]
    validation_scores: Mapping[str, float]
    train_cutoff: datetime
    train_event_ids: tuple[str, ...]
    train_clusters: tuple[str, ...]
    n_train_rows: int
    n_train_events: int
    target_definition: str
    link: Mapping[str, Any]
    interpretation: str
    fallback_reason: str | None = None
    selected_on: str = "train"
    notes: tuple[str, ...] = ()

    @property
    def is_fitted(self) -> bool:
        """Whether any coefficient differs from the no-change forecast."""
        return any(value != 0.0 for value in self.coefficients.values())

    def predict_delta(self, features: Mapping[str, np.ndarray]) -> np.ndarray:
        """Unconstrained linear predictor, centred and scaled as fitted."""
        size = _feature_length(features)
        delta = np.full(size, self.intercept, dtype=np.float64)
        for name in self.feature_names:
            values = np.asarray(features[name], dtype=np.float64)
            delta = delta + self.coefficients[name] * (
                (values - self.feature_means[name]) / self.feature_scales[name]
            )
        return delta

    def as_record(self) -> dict[str, Any]:
        """Machine-readable provenance for the report and the model registry."""
        return {
            "kind": self.kind,
            "spec": dict(self.spec),
            "feature_names": list(self.feature_names),
            "feature_means": dict(self.feature_means),
            "feature_scales": dict(self.feature_scales),
            "coefficients": dict(self.coefficients),
            "intercept": self.intercept,
            "parameters": dict(self.parameters),
            "validation_scores": dict(self.validation_scores),
            "train_cutoff": self.train_cutoff.isoformat(),
            "train_event_ids": list(self.train_event_ids),
            "train_clusters": list(self.train_clusters),
            "n_train_rows": self.n_train_rows,
            "n_train_events": self.n_train_events,
            "target_definition": self.target_definition,
            "bounded_link": dict(self.link),
            "interpretation": self.interpretation,
            "selected_on": self.selected_on,
            "fallback_reason": self.fallback_reason,
            "notes": list(self.notes),
        }


@dataclass(frozen=True, slots=True)
class FoldSplit:
    """Chronological split assignment in the order the folds were cut."""

    train_events: tuple[str, ...]
    validation_events: tuple[str, ...]
    test_events: tuple[str, ...]
    train_cutoff: datetime
    validation_cutoff: datetime
    embargo_seconds: float
    purged_rows: int
    policy: Mapping[str, Any] = field(default_factory=dict)

    def as_record(self) -> dict[str, Any]:
        return {
            "train_events": list(self.train_events),
            "validation_events": list(self.validation_events),
            "test_events": list(self.test_events),
            "n_train_events": len(self.train_events),
            "n_validation_events": len(self.validation_events),
            "n_test_events": len(self.test_events),
            "train_cutoff": self.train_cutoff.isoformat(),
            "validation_cutoff": self.validation_cutoff.isoformat(),
            "embargo_seconds": self.embargo_seconds,
            "purged_rows": self.purged_rows,
            "policy": dict(self.policy),
        }


@dataclass(frozen=True, slots=True)
class NestedComparisonResult:
    """Held-out comparison of the nested model ladder on one identical sample."""

    folds: FoldSplit
    seed: int
    split_policy: Mapping[str, Any]
    sample: Mapping[str, Any]
    common_sample_violations: tuple[str, ...]
    models: Mapping[str, Mapping[str, Any]]
    evaluations: Mapping[str, Mapping[str, Any]]
    scores: pd.DataFrame
    promotion: Mapping[str, Any]
    metric: str
    interpretation: str
    schema_issues: tuple[str, ...]
    notes: tuple[str, ...]

    def as_record(self) -> dict[str, Any]:
        """Report-ready provenance, metrics and gate status."""
        record: dict[str, Any] = {
            "folds": self.folds.as_record(),
            "seed": self.seed,
            "split_policy": dict(self.split_policy),
            "sample": dict(self.sample),
            "common_sample_violations": list(self.common_sample_violations),
            "models": {kind: dict(payload) for kind, payload in self.models.items()},
            "evaluations": {kind: dict(payload) for kind, payload in self.evaluations.items()},
            "metric": self.metric,
            "promotion": dict(self.promotion),
            "interpretation": self.interpretation,
            "schema_issues": list(self.schema_issues),
            "notes": list(self.notes),
        }
        record["scores"] = self.scores.to_dict(orient="records")
        return record


def _feature_length(features: Mapping[str, Any]) -> int:
    try:
        column = next(iter(features.values()))
    except StopIteration:  # pragma: no cover - callers pass fitted feature sets
        return 0
    return int(np.asarray(column).shape[0])


def _require_columns(
    table: pd.DataFrame, columns: Sequence[str], *, context: str = "table"
) -> None:
    missing = [column for column in columns if column not in table.columns]
    if missing:
        raise ForecastDataError(
            f"{context} requires column(s) {missing}; present columns are {sorted(table.columns)}"
        )


def _to_float(table: pd.DataFrame, column: str) -> pd.Series:
    """Coerce a column to float64; non-numeric entries become NaN, never errors."""
    values = pd.to_numeric(table[column], errors="coerce")
    return values.astype(np.float64)


def _event_time_series(table: pd.DataFrame, *, context: str = "table") -> pd.Series:
    values = pd.to_datetime(table["event_time"], utc=True, errors="coerce")
    if values.isna().any():
        raise ForecastDataError(
            f"{context} has {int(values.isna().sum())} row(s) with an unparsable event_time"
        )
    return values


def _prepare(
    table: pd.DataFrame,
    spec: Mapping[str, Any],
    *,
    context: str = "table",
) -> tuple[pd.DataFrame, tuple[str, ...], str | None, list[str]]:
    """Validate, restrict to admissible rows and select the feature columns."""
    if not isinstance(table, pd.DataFrame):
        raise ForecastDataError(f"{context} expects a pandas DataFrame, got {type(table).__name__}")
    if table.empty:
        raise ForecastDataError(f"{context} received an empty table")
    kind = str(spec.get("kind", "network"))
    if kind not in FEATURE_SPECS:
        raise ModelFitError(f"unknown model kind {kind!r}; expected one of {list(MODEL_KINDS)}")
    _require_columns(table, _REQUIRED_TABLE_COLUMNS, context=context)
    notes: list[str] = []
    # A column may be declared when it is part of the kind's design set, and also
    # when it is a raw column this kind collapses: a caller auditing every raw
    # neighbour column must be able to say so and still get one coefficient per
    # pair rather than an outright rejection.
    admissible = set(FEATURE_SPECS[kind]) | set(REDUNDANT_FEATURE_COLUMNS.get(kind, ()))
    declared = spec.get("columns")
    if declared is None:
        columns = tuple(FEATURE_SPECS[kind])
    else:
        columns = tuple(str(name) for name in declared)
        unknown = [name for name in columns if name not in admissible]
        if unknown:
            raise ModelFitError(
                f"spec declares column(s) {unknown} outside the admissible set "
                f"{sorted(admissible)} for kind {kind!r}"
            )
        if kind == "network" and set(NEIGHBOR_COLUMNS) - set(columns):
            notes.append(
                "network spec omits neighbour column(s) "
                f"{sorted(set(NEIGHBOR_COLUMNS) - set(columns))}; recorded as declared"
            )
    redundant = [name for name in REDUNDANT_FEATURE_COLUMNS.get(kind, ()) if name in columns]
    if redundant:
        # An exact logical complement carries no separate information, so fitting
        # it beside its source column would only split one coefficient pair. It
        # stays in the forecast table for audit and is dropped from the design.
        columns = tuple(name for name in columns if name not in redundant)
        notes.append(
            f"collapsed exact logical complement column(s) {redundant} out of the "
            f"{kind!r} design; the raw columns remain in the forecast table"
        )
    for named in ("shock", "delayed_shock"):
        if named in columns:
            _require_columns(table, [named], context=context)

    frame = table.copy()
    frame["_event_time_dt"] = _event_time_series(frame, context=context)
    frame["_target"] = _to_float(frame, "target")
    frame["_current_price"] = _to_float(frame, "current_price")
    for name in columns:
        frame[f"_feature_{name}"] = _to_float(frame, name)
    if "max_input_available_time" in frame.columns:
        cutoff = pd.to_datetime(frame["max_input_available_time"], utc=True, errors="coerce")
        frame["_max_input_available_time_dt"] = cutoff
    else:
        frame["_max_input_available_time_dt"] = pd.NaT
        notes.append("no max_input_available_time column; cutoff-admissibility not re-verified")
    cutoff_columns = [column for column in ("training_cutoff", "split") if column in frame.columns]
    if cutoff_columns:
        allowed = spec.get("allowed_splits")
        if allowed is not None and "split" in frame.columns:
            before = len(frame)
            frame = frame.loc[frame["split"].isin(list(allowed))]
            dropped = before - len(frame)
            if dropped:
                notes.append(f"restricted to split(s) {list(allowed)}: dropped {dropped} row(s)")
    feature_column_names = tuple(f"_feature_{name}" for name in columns)
    frame["_complete"] = (
        frame[["_target", "_current_price", *feature_column_names]].notna().all(axis=1)
    )
    if "valid" in frame.columns:
        frame["_complete"] &= frame["valid"].fillna(False).astype(bool)
    else:
        notes.append("no valid column; every parsed row with complete features is used")
    before = len(frame)
    incomplete = int((~frame["_complete"]).sum())
    if incomplete:
        notes.append(f"dropped {incomplete} incomplete or invalid row(s) of {before}")
    frame = frame.loc[frame["_complete"]]
    if frame.empty:
        raise ForecastDataError(
            f"{context} has no complete row after requiring {list(columns)} and target"
        )
    return frame, columns, None, notes


def _scale_features(
    frame: pd.DataFrame, columns: Sequence[str]
) -> tuple[dict[str, float], dict[str, float], tuple[str, ...]]:
    means: dict[str, float] = {}
    scales: dict[str, float] = {}
    notes: list[str] = []
    for name in columns:
        values = frame[f"_feature_{name}"].to_numpy(dtype=np.float64)
        mean = float(values.mean())
        scale = float(values.std(ddof=0))
        if not math.isfinite(scale) or scale <= 1e-12:
            scale = 1.0
            notes.append(f"feature {name!r} has no training variation; scale recorded as 1.0")
        means[name] = mean
        scales[name] = scale
    return means, scales, tuple(notes)


def _fit_ridge(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    alpha: float,
    means: Mapping[str, float],
    scales: Mapping[str, float],
) -> tuple[dict[str, float], float]:
    blocks = [
        (frame[f"_feature_{name}"].to_numpy(dtype=np.float64) - means[name]) / scales[name]
        for name in columns
    ]
    target = frame["_target"].to_numpy(dtype=np.float64)
    if not blocks:
        return {}, float(target.mean())
    design = np.column_stack(blocks)
    design = np.column_stack([np.ones(design.shape[0]), design])
    if alpha > 0.0:
        penalty = np.eye(design.shape[1]) * float(alpha)
        penalty[0, 0] = 0.0
        normal = design.T @ design + penalty
    else:
        normal = design.T @ design
    try:
        solution = np.linalg.solve(normal, design.T @ target)
    except np.linalg.LinAlgError:
        solution, *_ = np.linalg.lstsq(normal, design.T @ target, rcond=None)
    coefficients = {name: float(solution[index + 1]) for index, name in enumerate(columns)}
    if not np.isfinite(solution).all():
        raise ModelFitError("ridge solution is not finite; check the training rows for scale")
    return coefficients, float(solution[0])


def fit(train: pd.DataFrame, spec: dict) -> FittedModel:
    """Fit one nested-ladder model, returning a frozen model object.

    ``spec`` keys:

    ``kind``
        One of ``no_change``, ``own``, ``news``, ``network``.
    ``alphas``
        Optional keyword-only extension: penalty grid for every tuned kind. The
        value is chosen on the supplied validation rows only, and news and
        network are selected over the same grid.
    ``validation``
        The validation rows used to select the penalty. When supplied, every
        tuned kind -- news and network alike -- picks its ``alpha`` by held-out
        MAE on exactly these rows, so the comparison is not a tuning advantage.
        When absent, selection falls back to the training rows and the model's
        notes say so, because that fallback cannot support a promotion claim.
    ``columns``
        Optional keyword-only extension: a subset of the kind's admissible
        feature columns, for sensitivity analysis. Exact logical complements are
        collapsed out of the design even when declared, and the collapse is
        recorded in the model's notes.
    ``allowed_splits``
        Optional keyword-only extension: split labels admitted as training rows.

    Transforms are fit on training rows only: every feature is centred and
    scaled by its training mean and standard deviation, and the training cutoff
    is the maximum training event time. The bounded-mapping parameters are
    frozen constants, never data-selected, and are recorded on the model.
    """
    if not isinstance(spec, dict):
        raise ModelFitError(f"spec must be a dict, got {type(spec).__name__}")
    frame, columns, _unused, notes = _prepare(train, spec)
    kind = str(spec.get("kind", "network"))
    means, scales, scale_notes = _scale_features(frame, columns)
    notes.extend(scale_notes)
    train_cutoff = frame["_event_time_dt"].max()
    event_ids = tuple(sorted(str(value) for value in frame["event_id"].unique()))
    clusters = tuple(
        sorted(str(value) for value in frame.get("cluster_id", frame["event_id"]).unique())
    )
    parameters: dict[str, Any] = dict(FROZEN_PARAMETERS[kind]["parameters"])
    validation_scores: dict[str, float] = {}
    fallback_reason: str | None = None
    selected_on = "train"

    if kind == "no_change":
        coefficients: dict[str, float] = {}
        intercept = 0.0
    elif kind in FIXED_PENALTY_KINDS:
        # A kind with no tuning at all: its penalty is a declared constant.
        coefficients, intercept = _fit_ridge(
            frame,
            columns,
            alpha=float(FIXED_PENALTY_KINDS[kind]),
            means=means,
            scales=scales,
        )
        parameters["alpha"] = float(FIXED_PENALTY_KINDS[kind])
    else:
        # News and network select their penalty on the same grid over the same
        # validation rows. Neither kind is handed a fixed penalty the other is
        # not, so a news-versus-network difference is not a tuning difference.
        alphas = tuple(float(value) for value in spec.get("alphas", DEFAULT_ALPHAS))
        if not alphas or any(value < 0.0 for value in alphas):
            raise ModelFitError(f"alphas={alphas!r} must be a non-empty non-negative grid")
        validation = spec.get("validation")
        candidates: dict[float, tuple[dict[str, float], float]] = {}
        for alpha in alphas:
            candidates[alpha] = _fit_ridge(frame, columns, alpha=alpha, means=means, scales=scales)
        if isinstance(validation, pd.DataFrame) and not validation.empty:
            validation_frame, _v_columns, _unused_v, validation_notes = _prepare(
                validation, spec, context="fit:validation"
            )
            notes.extend(validation_notes)
            if validation_frame.empty:  # pragma: no cover - guarded by fit callers
                raise ModelFitError("validation rows became empty during preparation")
            selected_frame = validation_frame
            selected_on = "validation"
        else:
            selected_frame = frame
            selected_on = "train"
            notes.append(
                f"no validation rows were supplied; the {kind} penalty was chosen by training "
                "loss and must be re-selected on real validation rows before any promotion claim"
            )
        best_alpha: float | None = None
        best_score = math.inf
        for alpha, (candidate_coefficients, candidate_intercept) in candidates.items():
            delta = _linear_delta(
                selected_frame,
                columns,
                candidate_coefficients,
                candidate_intercept,
                means,
                scales,
            )
            current = selected_frame["_current_price"].to_numpy(dtype=np.float64)
            predicted = _bounded_level(current, delta) - current
            actual = selected_frame["_target"].to_numpy(dtype=np.float64)
            score = forecast_scores(actual, predicted)["mae"]
            validation_scores[f"alpha={alpha:g}"] = score
            if score < best_score:
                best_score = score
                best_alpha = alpha
        if best_alpha is None:  # pragma: no cover - alphas is non-empty
            raise ModelFitError("no candidate penalty produced a finite validation score")
        coefficients, intercept = candidates[best_alpha]
        parameters["alpha"] = best_alpha
        if kind == "network" and not any(
            float(coefficients.get(name, 0.0)) != 0.0 for name in columns
        ):
            fallback_reason = "regularized network coefficients all fitted to zero"
            notes.append(
                "every network coefficient fitted to zero, so this network is numerically the "
                "no-change forecast and cannot be promoted as evidence of propagation"
            )

    model = FittedModel(
        kind=kind,
        spec=dict(spec),
        feature_names=tuple(columns),
        feature_means=means,
        feature_scales=scales,
        coefficients={name: float(coefficients.get(name, 0.0)) for name in columns},
        intercept=float(intercept),
        parameters=parameters,
        validation_scores=validation_scores,
        train_cutoff=train_cutoff.to_pydatetime(),
        train_event_ids=event_ids,
        train_clusters=clusters,
        n_train_rows=len(frame),
        n_train_events=int(frame["event_id"].nunique()),
        target_definition=(
            "future change in the target venue's midprice between prediction_time and "
            "prediction_time + horizon_seconds (probability points), never terminal payout"
        ),
        link=dict(BOUNDED_LINK),
        interpretation=INTERPRETATION_LABEL,
        fallback_reason=fallback_reason,
        selected_on=selected_on,
        notes=tuple(notes),
    )
    return model


def _linear_delta(
    frame: pd.DataFrame,
    columns: Sequence[str],
    coefficients: Mapping[str, float],
    intercept: float,
    means: Mapping[str, float],
    scales: Mapping[str, float],
) -> np.ndarray:
    if not columns:
        return np.full(len(frame), float(intercept), dtype=np.float64)
    blocks = [
        ((frame[f"_feature_{name}"].to_numpy(dtype=np.float64) - means[name]) / scales[name])
        for name in columns
    ]
    design = np.column_stack(blocks)
    weights = np.array([float(coefficients.get(name, 0.0)) for name in columns])
    return float(intercept) + design @ weights


def _bounded_level(current_price: np.ndarray, delta: np.ndarray) -> np.ndarray:
    """Documented bounded mapping from the linear predictor to a probability level."""
    scale = float(BOUNDED_LINK["scale"])
    clip = float(BOUNDED_LINK["clip"])
    mapped = current_price + scale * np.tanh(delta / scale)
    return np.clip(mapped, clip, 1.0 - clip)


def evaluate(model: FittedModel, test: pd.DataFrame) -> dict[str, Any]:
    """Score a frozen model on held-out rows.

    Returns a dict with:

    ``n_rows`` / ``n_events`` / ``event_ids``
        The evaluated rows and their event clusters, so the caller can prove
        every nested model saw the same sample.
    ``mae`` / ``mse`` / ``rmse``
        Probability-point errors for the future-probability change. MAE is the
        registered primary metric.
    ``directional_accuracy``
        Secondary metric, reported only on rows with both a real and a
        predicted non-zero move, with ``directional_n`` alongside it.
    ``bad_direction_rate``
        Share of sign-resolved rows whose predicted change has the wrong sign.
    ``bounded_mapping`` / ``clipping``
        The mapping applied and how often it engaged.
    ``per_event``
        Event-level mean absolute error, for cluster-aware aggregation.
    ``notes``
        Explicit missingness and limitations.
    """
    if not isinstance(model, FittedModel):
        raise ForecastDataError(f"evaluate expects a FittedModel, got {type(model).__name__}")
    frame, columns, _unused, notes = _prepare(
        test, {"kind": model.kind, "columns": list(model.feature_names)}, context="evaluate"
    )
    if tuple(columns) != tuple(model.feature_names):
        raise ForecastDataError(
            f"test rows expose columns {list(columns)} but the frozen model was fit on "
            f"{list(model.feature_names)}; transforms must never change after fitting"
        )
    missing_scales = [name for name in model.feature_names if name not in model.feature_scales]
    if missing_scales:  # pragma: no cover - guarded by fit
        raise ForecastDataError(f"frozen model has no scale for feature(s) {missing_scales}")
    delta = _linear_delta(
        frame,
        model.feature_names,
        model.coefficients,
        model.intercept,
        model.feature_means,
        model.feature_scales,
    )
    current_price = frame["_current_price"].to_numpy(dtype=np.float64)
    level = _bounded_level(current_price, delta)
    predicted = level - current_price
    actual = frame["_target"].to_numpy(dtype=np.float64)
    scores = forecast_scores(actual, predicted)
    resolved = (actual != 0.0) & (predicted != 0.0)
    directional_accuracy = (
        float(np.mean(np.sign(actual[resolved]) == np.sign(predicted[resolved])))
        if resolved.any()
        else None
    )
    wrong = (
        float(np.mean(np.sign(actual[resolved]) != np.sign(predicted[resolved])))
        if resolved.any()
        else None
    )
    per_event = (
        frame.assign(_abs_error=np.abs(actual - predicted))
        .groupby("event_id", sort=True)["_abs_error"]
        .mean()
    )
    saturated = int(
        np.sum(level <= float(BOUNDED_LINK["clip"]))
        + np.sum(level >= 1.0 - float(BOUNDED_LINK["clip"]))
    )
    if saturated:
        notes.append(
            f"{saturated} prediction(s) sit on the level clip of "
            f"{float(BOUNDED_LINK['clip'])!r}; clipping must be reported alongside the score"
        )
    return {
        "kind": model.kind,
        "n_rows": len(frame),
        "n_events": int(frame["event_id"].nunique()),
        "n_clusters": int(frame.get("cluster_id", frame["event_id"]).nunique()),
        "event_ids": sorted(str(value) for value in frame["event_id"].unique()),
        "mae": scores["mae"],
        "mse": scores["mse"],
        "rmse": scores["rmse"],
        "directional_accuracy": directional_accuracy,
        "directional_n": int(resolved.sum()),
        "bad_direction_rate": wrong,
        "prediction_scope": (
            "future probability change in probability points between prediction_time and "
            "prediction_time + horizon_seconds; terminal binary scoring is separate"
        ),
        "predictions": [
            {
                "event_id": str(event_id),
                "contract_id": str(contract_id),
                "horizon_seconds": int(horizon),
                "actual": float(actual_value),
                "predicted": float(predicted_value),
                "current_price": float(price),
                "predicted_level": float(level_value),
            }
            for event_id, contract_id, horizon, actual_value, predicted_value, price, level_value in zip(
                frame["event_id"].astype(str),
                frame["contract_id"].astype(str),
                frame["horizon_seconds"].astype(int),
                actual,
                predicted,
                current_price,
                level,
                strict=True,
            )
        ],
        "bounded_mapping": dict(model.link),
        "clipping": {
            "level_clip": float(BOUNDED_LINK["clip"]),
            "tanh_scale": float(BOUNDED_LINK["scale"]),
            "n_at_bound": saturated,
            "max_abs_predicted_change": float(np.max(np.abs(predicted))),
            "data_selected": False,
        },
        "per_event_mae": {str(key): float(value) for key, value in per_event.items()},
        "mean_predicted_change": float(predicted.mean()),
        "mean_actual_change": float(actual.mean()),
        "model": model.as_record(),
        "notes": notes,
    }


def _best_on_validation(
    train_rows: pd.DataFrame,
    validation_rows: pd.DataFrame,
    spec: Mapping[str, Any],
) -> FittedModel:
    selection_spec = dict(spec)
    selection_spec["validation"] = validation_rows
    return fit(train_rows, selection_spec)


def nested_comparison(
    data: pd.DataFrame,
    *,
    seed: int = 20260913,
    train_fraction: float = 0.6,
    validation_fraction: float = 0.2,
    kinds: Sequence[str] | None = None,
    embargo_seconds: float | None = None,
    min_events_per_fold: int = 1,
    null_assessment: Mapping[str, Any] | None = None,
    minimum_mae_gain: float = 0.005,
) -> NestedComparisonResult:
    """Compare the nested ladder on identical held-out rows.

    Fold assignment, cutoff and label purging come from
    :func:`market_propagation.evaluation.chronological_splits`, which is the one
    split authority for this project. A release unit is a whole ``cluster_id``
    when the table carries one, so cross-venue equivalents stay together even
    when their ``event_id`` labels differ; the same label-availability and
    horizon purge applies to every model, because the fold frames are cut once.

    ``null_assessment``
        Optional keyword-only extension: the result of
        :func:`market_propagation.falsification.network_falsification` (or the
        equivalent payload from :mod:`market_propagation.evaluation`). The
        promotion gate accepts it only when it was produced by this same
        estimator, metric, MAE-gain threshold, time settings and event count. A
        rate from any other pipeline -- notably a between-release response-slope
        power report -- is rejected as unrelated and cannot unlock the gate.

    ``minimum_mae_gain``
        Keyword-only extension: the smallest MAE reduction between the shared
        news baseline and the network candidate that the gate requires, in
        probability points. The registered value is 0.005, matching
        ``configs/study_v1.yaml``; the caller supplies it explicitly and it is
        propagated into the reported gate criteria and into the null-assessment
        identity check.

    Returns a :class:`NestedComparisonResult`. The train and validation folds
    are disjoint by construction, so the validation-only penalty selection is
    honest, and the test fold is never used for fitting or for selection.
    """
    if not isinstance(data, pd.DataFrame):
        raise ForecastDataError(
            f"nested_comparison expects a pandas DataFrame, got {type(data).__name__}"
        )
    if data.empty:
        raise ForecastDataError("nested_comparison received an empty table")
    _require_columns(
        data,
        (
            "event_id",
            "event_time",
            "horizon_seconds",
            "target",
            "current_price",
            "max_input_available_time",
            *FEATURE_SPECS["network"],
        ),
        context="nested_comparison",
    )
    if not 0.0 < train_fraction < 1.0 or not 0.0 < validation_fraction < 1.0:
        raise ForecastDataError("train_fraction and validation_fraction must lie in (0, 1)")
    if train_fraction + validation_fraction >= 1.0:
        raise ForecastDataError(
            f"train_fraction + validation_fraction = "
            f"{train_fraction + validation_fraction!r} must leave a test fold"
        )
    resolved_kinds = tuple(kinds) if kinds is not None else MODEL_KINDS
    unknown = [kind for kind in resolved_kinds if kind not in FEATURE_SPECS]
    if unknown:
        raise ModelFitError(f"unknown model kind(s) {unknown}; expected {list(MODEL_KINDS)}")
    resolved_gain = float(minimum_mae_gain)
    if not math.isfinite(resolved_gain) or resolved_gain <= 0.0:
        raise ForecastDataError(
            f"minimum_mae_gain={minimum_mae_gain!r} must be finite and positive; the gate is "
            "defined as a smallest relevant improvement in probability-point MAE"
        )

    schema_issues: list[str] = []
    frame, network_columns, _unused, prep_notes = _prepare(
        data, {"kind": "network"}, context="nested_comparison"
    )
    schema_issues.extend(prep_notes)
    if frame.empty:
        raise ForecastDataError(
            "nested_comparison has no row with every ladder feature, the target and "
            "current_price present; missingness cannot change the comparison sample, so the "
            "comparison is not run"
        )
    if tuple(network_columns) != FEATURE_SPECS["network"]:
        raise ForecastDataError(  # pragma: no cover - guarded by the kind spec
            f"nested_comparison expected the full network feature set, got {list(network_columns)}"
        )

    # Time settings are read off the rows, never asserted from metadata, so the
    # null-assessment identity check compares real values.
    horizons = tuple(sorted(int(value) for value in frame["horizon_seconds"].unique()))
    prediction_delay = (
        (frame["prediction_time"] - frame["event_time"]).dt.total_seconds().median()
        if "prediction_time" in frame.columns and "event_time" in frame.columns
        else None
    )
    time_settings: dict[str, Any] = {
        "horizon_seconds": list(horizons),
        "prediction_delay_seconds": (
            None if prediction_delay is None else round(float(prediction_delay), 6)
        ),
    }

    try:
        fold_frames = chronological_splits(
            frame,
            train_fraction=train_fraction,
            validation_fraction=validation_fraction,
            embargo_seconds=embargo_seconds,
            cluster_column="cluster_id",
            event_column="event_id",
            time_column="event_time",
            horizon_column="horizon_seconds",
            label_column="target_available_time",
            mask_column="valid",
            include_masked=False,
            min_events_per_fold=min_events_per_fold,
        )
    except ForecastEvaluationError as error:
        # One authority, one failure mode: the split's own diagnosis is surfaced
        # as a forecast-data error rather than re-derived here.
        raise ForecastDataError(f"nested_comparison split rejected the table: {error}") from error

    split_frame = fold_frames["test"]
    policy = dict(split_frame.attrs.get("policy", {}))
    split_exclusions = list(split_frame.attrs.get("exclusions", []))
    folios = {fold: fold_frames[fold].copy() for fold in ("train", "validation", "test")}
    purged = int(policy.get("purged_rows", 0))
    embargo = float(policy.get("embargo_seconds", 0.0))
    train_events = tuple(sorted(str(value) for value in folios["train"]["event_id"].unique()))
    validation_events = tuple(
        sorted(str(value) for value in folios["validation"]["event_id"].unique())
    )
    test_events = tuple(sorted(str(value) for value in folios["test"]["event_id"].unique()))
    train_boundary = pd.Timestamp(policy["train_cutoff"])
    validation_boundary = pd.Timestamp(policy["validation_cutoff"])
    for label, ids in (
        ("train", train_events),
        ("validation", validation_events),
        ("test", test_events),
    ):
        if len(ids) < min_events_per_fold:
            raise ForecastDataError(
                f"fold {label!r} has {len(ids)} release(s), fewer than "
                f"min_events_per_fold={min_events_per_fold}"
            )

    models: dict[str, FittedModel] = {}
    evaluations: dict[str, dict[str, Any]] = {}
    holdout_rows: list[str] | None = None
    violations: list[str] = []
    notes: list[str] = []
    for kind in resolved_kinds:
        spec: dict[str, Any] = {"kind": kind}
        model = _best_on_validation(folios["train"], folios["validation"], spec)
        models[kind] = model
        leaked = folios["test"].loc[folios["test"]["_event_time_dt"] <= model.train_cutoff]
        if not leaked.empty:
            violations.append(
                f"{kind}: {len(leaked)} held-out row(s) sit at or before the fit cutoff "
                f"{model.train_cutoff.isoformat()}, so the test fold is not strictly after training"
            )
        evaluation = evaluate(model, folios["test"])
        evaluations[kind] = evaluation
        if holdout_rows is None:
            holdout_rows = evaluation["event_ids"]
        elif holdout_rows != evaluation["event_ids"]:
            violations.append(
                f"{kind}: evaluated event set differs from the first model's held-out events"
            )
        notes.extend(f"{kind}: {note}" for note in model.notes)

    predictions = pd.DataFrame(
        {
            "kind": list(evaluations),
            "n_rows": [evaluations[kind]["n_rows"] for kind in evaluations],
            "n_events": [evaluations[kind]["n_events"] for kind in evaluations],
            "mae": [evaluations[kind]["mae"] for kind in evaluations],
            "mse": [evaluations[kind]["mse"] for kind in evaluations],
            "rmse": [evaluations[kind]["rmse"] for kind in evaluations],
            "directional_accuracy": [
                evaluations[kind]["directional_accuracy"] for kind in evaluations
            ],
        }
    )
    baseline = evaluations.get(str(PROMOTION_GATE["baseline_kind"]))
    candidate = evaluations.get(str(PROMOTION_GATE["candidate_kind"]))
    required = resolved_gain
    if baseline is None or candidate is None:
        promotion: dict[str, Any] = {
            "status": "not_evaluated",
            "reason": (
                "promotion status requires both the shared-news baseline and the network "
                f"candidate; evaluated kinds were {list(evaluations)}"
            ),
        }
    else:
        reduction = float(baseline["mae"]) - float(candidate["mae"])
        null_ok, null_rate, null_reasons, null_record = _null_assessment_verdict(
            null_assessment,
            minimum_mae_gain=required,
            time_settings=time_settings,
            n_events=int(frame["event_id"].nunique()),
        )
        promoted = reduction >= required and not violations and null_ok
        promotion = {
            "status": "promoted" if promoted else "gated",
            "metric": PROMOTION_GATE["metric"],
            "baseline_kind": PROMOTION_GATE["baseline_kind"],
            "candidate_kind": PROMOTION_GATE["candidate_kind"],
            "baseline_mae": float(baseline["mae"]),
            "candidate_mae": float(candidate["mae"]),
            "mae_reduction": reduction,
            "minimum_mae_gain": required,
            "time_settings": time_settings,
            "common_sample_violations": list(violations),
            "criteria_met": {
                "beats_baseline": bool(reduction > 0.0),
                "meets_minimum_mae_gain": bool(reduction >= required),
                "common_sample_and_cutoff_clean": not violations,
                "null_assessment_matches_and_ok": null_ok,
            },
            "null_assessment": {
                "status": null_record["status"],
                "false_positive_rate": null_rate,
                "matches_this_estimator": null_ok,
                "mismatch_reasons": null_reasons,
                "required": (
                    "a common-news-plus-delay null false-positive rate produced by this same "
                    "estimator, metric, MAE-gain threshold, time settings and event count, from "
                    "market_propagation.falsification.network_falsification"
                ),
                "note": (
                    "without a matching assessment the candidate is gated even when it beats the "
                    "baseline, because a predictive edge over the shared-news model is not evidence "
                    "of propagation; a rate from an unrelated estimator is not accepted"
                ),
            },
        }
    sample = {
        "n_rows_total": len(frame),
        "n_events_total": int(frame["event_id"].nunique()),
        "n_rows_train": len(folios["train"]),
        "n_rows_validation": len(folios["validation"]),
        "n_rows_test": len(folios["test"]),
        "n_events_train": int(folios["train"]["event_id"].nunique()),
        "n_events_validation": int(folios["validation"]["event_id"].nunique()),
        "n_events_test": int(folios["test"]["event_id"].nunique()),
        "n_clusters_test": int(folios["test"]["cluster_id"].nunique()),
        "common_columns": list(FEATURE_SPECS["network"]),
        "rows_purged": purged,
        "rows_dropped_incomplete": int(len(data) - len(frame)),
    }
    folds = FoldSplit(
        train_events=train_events,
        validation_events=validation_events,
        test_events=test_events,
        train_cutoff=train_boundary.to_pydatetime(),
        validation_cutoff=validation_boundary.to_pydatetime(),
        embargo_seconds=embargo,
        purged_rows=purged,
        policy={
            "split_authority": "market_propagation.evaluation.chronological_splits",
            "assignment": policy.get("assignment"),
            "n_units": policy.get("n_units"),
            "equivalents": (
                "the release unit is cluster_id when the table carries one, so cross-venue "
                "equivalents stay in one fold even when their event_id labels differ"
            ),
            "label_admission": (
                "the split authority purges a row whose label window, including the declared "
                "embargo, reaches past its own fold boundary; the same purge is applied once for "
                "every model, so no model sees a different sample"
            ),
            "embargo_seconds": embargo,
            "embargo_basis": policy.get("embargo_basis"),
            "split_exclusions": split_exclusions,
        },
    )
    models_record = {kind: models[kind].as_record() for kind in models}
    return NestedComparisonResult(
        folds=folds,
        seed=int(seed),
        split_policy=dict(folds.policy),
        sample=sample,
        common_sample_violations=tuple(violations),
        models=models_record,
        evaluations=evaluations,
        scores=predictions,
        promotion=promotion,
        metric=str(PROMOTION_GATE["metric"]),
        interpretation=INTERPRETATION_LABEL,
        schema_issues=tuple(schema_issues),
        notes=tuple(notes),
    )


def _null_assessment_verdict(
    null_assessment: Mapping[str, Any] | None,
    *,
    minimum_mae_gain: float,
    time_settings: Mapping[str, Any],
    n_events: int,
) -> tuple[bool, float | None, list[str], dict[str, Any]]:
    """Whether a supplied null assessment may unlock the network forecast gate.

    The assessment must have been produced by this same estimator on this same
    metric, at this same MAE-gain threshold, with the same horizon and prediction
    delay, and at this event count. Anything else -- including a between-release
    response-slope power report, which is a different question in different units
    -- is rejected as unrelated and reported with the reason, so a foreign
    statistic can never promote the network model.
    """
    if null_assessment is None:
        return (
            False,
            None,
            ["no null assessment was supplied"],
            {"status": "not_supplied"},
        )
    status = str(null_assessment.get("status", "unknown"))
    reasons: list[str] = []
    if status != "ok":
        reasons.append(f"the assessment reports status={status!r}, not 'ok'")

    identity = FORECAST_ESTIMATOR
    if null_assessment.get("estimator") != identity["estimator"]:
        reasons.append(
            f"estimator={null_assessment.get('estimator')!r} is not this comparison's "
            f"estimator {identity['estimator']!r}"
        )
    if null_assessment.get("metric") != identity["metric"]:
        reasons.append(
            f"metric={null_assessment.get('metric')!r} is not this comparison's metric "
            f"{identity['metric']!r}"
        )
    reported_gain = null_assessment.get("minimum_mae_gain")
    if reported_gain is None or abs(float(reported_gain) - float(minimum_mae_gain)) > 1e-12:
        reasons.append(
            f"minimum_mae_gain={reported_gain!r} is not this comparison's threshold "
            f"{minimum_mae_gain!r}"
        )
    reported_horizons = null_assessment.get("horizon_seconds")
    if reported_horizons is not None:
        reported_horizons = [int(value) for value in np.atleast_1d(reported_horizons)]
    if reported_horizons != list(time_settings["horizon_seconds"]):
        reasons.append(
            f"horizon_seconds={reported_horizons!r} is not this comparison's horizon grid "
            f"{list(time_settings['horizon_seconds'])!r}"
        )
    expected_delay = time_settings["prediction_delay_seconds"]
    reported_delay = null_assessment.get("prediction_delay_seconds")
    if expected_delay is not None and (
        reported_delay is None or abs(float(reported_delay) - float(expected_delay)) > 1e-6
    ):
        reasons.append(
            f"prediction_delay_seconds={reported_delay!r} is not this comparison's "
            f"prediction delay {expected_delay!r}"
        )
    reported_events = null_assessment.get("n_events")
    if reported_events is None or int(reported_events) != int(n_events):
        reasons.append(
            f"n_events={reported_events!r} is not this comparison's event count {int(n_events)}"
        )

    if null_assessment.get("false_positive_rate_at_null") is not None:
        rate = null_assessment["false_positive_rate_at_null"]
    else:
        rate = null_assessment.get("false_positive_rate")
    if rate is None:
        reasons.append("the assessment carries no measurable false-positive rate at the null")
    if reasons:
        return False, None if rate is None else float(rate), reasons, {"status": status}
    return True, float(rate), [], {"status": status}


def local_projections(
    panel: pd.DataFrame,
    *,
    shock_column: str | None = None,
    pre_covariates: Sequence[str] = (),
    liquidity_columns: Sequence[str] = (),
    pre_event_time_columns: Mapping[str, str] | None = None,
    post_event_columns: Sequence[str] = (),
    event_time_column: str = "event_time",
    ineligible_column: str | None = None,
    orientation_column: str | None = None,
    orient_response: bool = True,
    mask_column: str | None = "valid",
    seed: int = 20260913,
    bootstrap_samples: int = 200,
    coverage: float = 0.95,
    horizon_column: str = "horizon_seconds",
    response_column: str = "response",
    cluster_column: str = "cluster_id",
    event_column: str = "event_id",
    family_column: str = "family",
    min_events: int = 4,
    min_clusters: int = 2,
    settling_tolerance: float = 0.05,
) -> dict[str, Any]:
    """Event-study local projections with release-clustered uncertainty.

    The independent information unit is the release, so every mean and interval
    is computed over event-level values rather than over rows, and resampling is
    a cluster bootstrap over releases. Simultaneous curve uncertainty comes from
    the maximum absolute studentized deviation across horizons, so the band
    covers the whole response curve rather than one horizon at a time. One
    release resample is drawn per family curve and reused at every horizon, so
    the horizon replicates in a simultaneous band are aligned by the releases
    actually drawn rather than by replicate index over independently drawn
    horizon samples; a release that is missing at one horizon simply stops
    contributing there. Leave-one-event-out sensitivity is reported per cell.

    ``shock_column=None`` yields descriptive mean responses only; there is no
    shock slope without shock data, and the absence is stated in the result
    rather than filled in. When a shock is supplied, its slope is identified
    between events, because a release-constant shock cannot support within-release
    identification; the result records that explicitly. Event fixed effects are
    never added for exactly that reason: they would absorb the event-constant
    shock coefficient.

    ``pre_covariates`` and ``liquidity_columns`` name pre-event state and
    liquidity columns, and the fitted specification is the local-projection
    estimand frozen in ``configs/study_v1.yaml``,

    ``R_km(h) = alpha + beta * S + gamma * X_before + delta * (S x L_before)``

    with the intercept, the shock, each declared pre-state term and each
    shock-by-liquidity interaction. The result reports the named coefficients,
    the release-clustered standard errors, the shock marginals at mean and at
    one standard deviation of liquidity, and a Wald test of the interactions;
    the exact estimand is the reported model and nothing causal is claimed.
    ``liquidity_columns`` require ``shock_column``, because there is no
    ``S x L_before`` term without a shock.

    A conventional ``baseline``, ``spread_before`` or ``depth_before`` column
    inherits ``baseline_time`` and must be observed strictly before the release.
    Any other covariate must declare its own observation time in
    ``pre_event_time_columns``, so a pre-event input is an asserted fact rather
    than a column name. A requested column that is declared post-event, or that
    is named as one by default, is refused: a variable observed at or after the
    release is a mediator of the response, and adjusting for it would redefine
    the estimand while the coefficient still carried the shock's name. Rows whose
    covariate is missing are excluded with a recorded reason; a requested
    regressor is never dropped silently and a missing value is never filled with
    zero.

    Only rows with a compatible payoff orientation are pooled. A contract with
    no single signed orientation (``orientation_sign == 0``, for example a range
    or two-sided bucket) is excluded from sign-sensitive pooling and counted, not
    silently averaged in. With ``orient_response`` (the default) each response is
    multiplied by its declared orientation sign, so an ``above`` contract stays
    positive and a ``below`` complement is flipped into the equivalent
    increasing-threshold direction; set it to ``False`` only when every pooled
    contract already shares one orientation, such as a prespecified
    single-direction threshold cohort. ``ineligible_column`` marks top-level
    ineligibility: it
    is kept separate from ``mask_column``, which marks rows the panel already
    decided are unusable. Null, rank-deficient, insufficient-release and
    too-few-cluster cases return an explicit ``inconclusive`` cell with a reason
    instead of a number.

    Returns a dict with the estimand, metric, engine, exclusions, per-cell
    results and overall ``status``.
    """
    if not isinstance(panel, pd.DataFrame):
        raise LocalProjectionError(
            f"local_projections expects a pandas DataFrame, got {type(panel).__name__}"
        )
    if panel.empty:
        raise LocalProjectionError("local_projections received an empty panel")
    required = (response_column, event_column, cluster_column, horizon_column)
    missing = [column for column in required if column not in panel.columns]
    if missing:
        raise LocalProjectionError(
            f"panel is missing required column(s) {missing}; present columns are "
            f"{sorted(panel.columns)}"
        )
    if shock_column is not None and shock_column not in panel.columns:
        raise LocalProjectionError(f"shock_column {shock_column!r} is not in the panel")
    if ineligible_column is not None and ineligible_column not in panel.columns:
        raise LocalProjectionError(f"ineligible_column {ineligible_column!r} is not in the panel")
    if mask_column is not None and mask_column not in panel.columns:
        raise LocalProjectionError(f"mask_column {mask_column!r} is not in the panel")
    if orientation_column is None:
        orientation_column = "orientation_sign" if "orientation_sign" in panel.columns else None
    elif orientation_column not in panel.columns:
        raise LocalProjectionError(f"orientation_column {orientation_column!r} is not in the panel")
    if min_events < 1 or min_clusters < 1:
        raise LocalProjectionError("min_events and min_clusters must be at least 1")
    if bootstrap_samples < 1:
        raise LocalProjectionError("bootstrap_samples must be at least 1")
    if (liquidity_columns or pre_covariates) and shock_column is None:
        raise LocalProjectionError(
            "pre_covariates and liquidity_columns require a shock_column: the Level-1 "
            "specification is R = alpha + beta*S + gamma*X_before + delta*(S x L_before), so "
            "without a shock there is no coefficient to estimate and a requested regressor would "
            "be silently dropped"
        )
    if (pre_covariates or liquidity_columns) and event_time_column not in panel.columns:
        raise LocalProjectionError(
            f"event_time_column {event_time_column!r} is not in the panel; a pre-event covariate "
            "is ordered against the release by that column, which carries the event clock whereas "
            f"event_column {event_column!r} identifies the release. Present columns are "
            f"{sorted(panel.columns)}"
        )
    covariates, liquidity, covariate_time_columns = _pre_event_columns(
        panel,
        pre_covariates=pre_covariates,
        liquidity_columns=liquidity_columns,
        pre_event_time_columns=pre_event_time_columns,
        post_event_columns=post_event_columns,
        event_column=event_column,
        event_time_column=event_time_column,
    )
    frame = panel.copy()
    frame["_response"] = _to_float(frame, response_column)
    frame["_horizon"] = pd.to_numeric(frame[horizon_column], errors="coerce")
    exclusions = {
        f"missing_{response_column}": int(frame["_response"].isna().sum()),
        f"missing_{horizon_column}": int(frame["_horizon"].isna().sum()),
        "ineligible": 0,
        "masked": 0,
        "orientation_excluded": 0,
        "orientation_not_supplied": 0,
    }
    if ineligible_column is not None:
        ineligible = frame[ineligible_column].fillna(False).astype(bool)
        exclusions["ineligible"] = int(ineligible.sum())
        frame = frame.loc[~ineligible]
    if mask_column is not None:
        masked = ~frame[mask_column].fillna(False).astype(bool)
        exclusions["masked"] = int(masked.sum())
        frame = frame.loc[~masked]
    frame = frame.loc[frame["_response"].notna() & frame["_horizon"].notna()]
    if frame.empty:
        raise LocalProjectionError(
            "no eligible row remains after response, horizon, ineligibility and mask filters"
        )
    if orientation_column is None:
        exclusions["orientation_not_supplied"] = len(frame)
        orientation_note = (
            "no orientation column was supplied and the panel carries none, so no orientation "
            "alignment was applied and no orientation was excluded; opposite payoff orientations "
            "are pooled on the caller's assertion"
        )
    else:
        orientation = pd.to_numeric(frame[orientation_column], errors="coerce").fillna(0.0)
        zero = orientation == 0.0
        exclusions["orientation_excluded"] = int(zero.sum())
        frame = frame.loc[~zero].assign(_orientation=orientation.loc[~zero])
        if frame.empty:
            raise LocalProjectionError(
                "no row survives orientation filtering; sign-sensitive pooling cannot proceed"
            )
        if orient_response:
            frame["_response"] = frame["_response"] * frame["_orientation"]
            orientation_note = (
                "each response is multiplied by its contract's declared orientation sign, so "
                "pooled rows share one payoff orientation: an `above` move stays positive and a "
                "`below` move is flipped into the equivalent increasing-threshold direction "
                "before pooling; contracts with no single signed orientation are excluded and "
                "counted"
            )
        else:
            orientation_note = (
                "responses were pooled without orientation adjustment because orient_response is "
                "False; that is only defensible when every pooled contract already shares one "
                "orientation, such as a prespecified single-direction threshold cohort. Contracts "
                "with no single signed orientation are still excluded and counted."
            )
        frame["_orientation_applied"] = frame["_orientation"] if orient_response else 1.0
    cluster_series = frame[cluster_column].astype("object")
    event_series = frame[event_column].astype("object")
    multiplicity = cluster_series.groupby(cluster_series).transform("size")
    singleton_clusters = int(cluster_series.loc[multiplicity < 2].nunique(dropna=True))

    use_shock = shock_column is not None
    if use_shock:
        frame["_shock"] = _to_float(frame, shock_column)
        if frame["_shock"].isna().any():
            exclusions[f"missing_{shock_column}"] = int(frame["_shock"].isna().sum())
            frame = frame.loc[frame["_shock"].notna()]
            if frame.empty:
                raise LocalProjectionError(f"every eligible row lacks {shock_column!r}")

    covariate_audit: dict[str, Any] = {}
    if covariates or liquidity:
        for column in covariates + liquidity:
            values = _to_float(frame, column)
            if values.isna().any():
                exclusions[f"missing_{column}"] = int(values.isna().sum())
                frame = frame.loc[values.notna()]
                values = values.loc[frame.index]
                if frame.empty:
                    raise LocalProjectionError(
                        f"every eligible row lacks the requested covariate {column!r}; a "
                        "requested regressor is not dropped silently and its missing values are "
                        "not filled with zero"
                    )
            frame[f"_cov_{column}"] = values
        frame, availability_exclusions, covariate_audit = _covariate_availability(
            frame,
            time_columns=covariate_time_columns,
            covariates=covariates,
            liquidity_columns=liquidity,
            event_time_column=event_time_column,
        )
        exclusions |= availability_exclusions
    use_covariates = bool(covariates or liquidity) and use_shock

    cells: dict[str, Any] = {}
    curves: dict[str, Any] = {}
    families = (
        sorted(str(value) for value in frame[family_column].unique())
        if family_column in frame.columns
        else ["all"]
    )
    horizons = sorted(int(value) for value in frame["_horizon"].unique())
    for family in families:
        family_rows = (
            frame.loc[frame[family_column].astype(str) == family]
            if family_column in frame.columns
            else frame
        )
        # One release resample per family curve, reused at every horizon. The
        # draw depends only on the sorted cluster labels, so every horizon of
        # the curve selects the same releases at the same replicate index; a
        # release absent from a horizon is simply skipped there, which is how a
        # missing-horizon mask survives resampling instead of silently shifting
        # the alignment.
        release_cluster = (
            family_rows.assign(_release=family_rows[event_column].astype(str))
            .groupby("_release", sort=True)[cluster_column]
            .first()
            .astype(str)
        )
        family_cells: dict[int, Any] = {}
        for horizon in horizons:
            cell_rows = family_rows.loc[family_rows["_horizon"] == horizon]
            if cell_rows.empty:
                continue
            if use_covariates:
                cell = _covariate_projection_cell(
                    cell_rows,
                    family=family,
                    horizon=horizon,
                    shock_column=str(shock_column),
                    covariates=covariates,
                    liquidity_columns=liquidity,
                    event_column=event_column,
                    cluster_column=cluster_column,
                    seed=seed,
                    bootstrap_samples=bootstrap_samples,
                    coverage=coverage,
                    min_events=min_events,
                    min_clusters=min_clusters,
                    curve_clusters=release_cluster,
                    covariate_audit=covariate_audit,
                )
            else:
                cell = _projection_cell(
                    cell_rows,
                    family=family,
                    horizon=horizon,
                    use_shock=use_shock,
                    shock_column=shock_column,
                    response_column=response_column,
                    event_column=event_column,
                    cluster_column=cluster_column,
                    seed=seed,
                    bootstrap_samples=bootstrap_samples,
                    coverage=coverage,
                    min_events=min_events,
                    min_clusters=min_clusters,
                    curve_clusters=release_cluster,
                )
            cells[f"{family}|h={horizon}"] = cell
            family_cells[horizon] = cell
        if not family_cells:
            continue
        ordered = sorted(family_cells)
        curve_metric = "shock_slope" if use_shock else "mean_response"
        curve: dict[str, Any] = {
            "horizons_seconds": ordered,
            "curve_metric": curve_metric,
            "mean_response": [family_cells[h]["mean_response"] for h in ordered],
            "shock_slope": [family_cells[h]["shock_slope"] for h in ordered],
            "statuses": {str(h): family_cells[h]["status"] for h in ordered},
            "simultaneous_band": curve_uncertainty(
                [family_cells[h] for h in ordered], kind=curve_metric, coverage=coverage
            ),
            "settling": _settling_flags(family_cells, ordered, settling_tolerance),
            "shared_release_draws": {
                "applied": True,
                "n_releases_in_curve": len(release_cluster),
                "n_clusters_in_curve": int(release_cluster.nunique()),
                "draw_key": _draw_key(
                    release_cluster.to_numpy().tolist(),
                    seed=seed,
                    samples=bootstrap_samples,
                ),
                "note": (
                    "one cluster resample is drawn once for this curve and reused at every "
                    "horizon, so replicates in the simultaneous band are aligned by the releases "
                    "actually drawn; a release missing at one horizon drops out there rather than "
                    "shifting the alignment"
                ),
            },
        }
        if use_covariates:
            curve["coefficients_by_horizon"] = {
                str(h): family_cells[h].get("coefficients") for h in ordered
            }
            curve["coefficient_units"] = PROJECTION_COEFFICIENT_UNITS
            curve["standardized_coefficients_by_horizon"] = {
                str(h): family_cells[h].get("standardized_coefficients") for h in ordered
            }
            curve["standardized_coefficient_units"] = PROJECTION_STANDARDIZED_COEFFICIENT_UNITS
            curve["coefficient_standard_errors_by_horizon"] = {
                str(h): family_cells[h].get("coefficient_standard_errors") for h in ordered
            }
            curve["interaction_test_by_horizon"] = {
                str(h): family_cells[h].get("interaction_test") for h in ordered
            }
            curve["shock_marginal_effects_by_horizon"] = {
                str(h): family_cells[h].get("shock_marginal_effects") for h in ordered
            }
        curves[family] = curve
    if not cells:
        raise LocalProjectionError("no (family, horizon) cell had any eligible row")

    inconclusive = [key for key, cell in cells.items() if cell["status"] != "ok"]
    status = "ok" if not inconclusive else "partially_inconclusive"
    reasons = sorted(
        {cell["reason"] for cell in cells.values() if cell["status"] != "ok" and cell.get("reason")}
    )
    return {
        "estimand": "finite-horizon response to the release shock, in probability points",
        "primary_metric": "shock_slope" if use_shock else "mean_response",
        "metric_note": (
            "the shock slope is the registered primary metric when a shock is supplied; "
            "otherwise the descriptive mean response is the only estimand available"
        ),
        "units": (
            f"probability points per unit of the supplied {shock_column} column"
            if use_shock
            else "probability points"
        ),
        "identification": (
            "between-event: the shock is constant within a release, so only between-release "
            "variation identifies the shock slope and its liquidity interactions. Release fixed "
            "effects would absorb the shock entirely and are therefore not used. Covariate "
            "coefficients are conditional associations within the reported specification, not "
            "causal effects."
            if use_covariates
            else (
                "between-event: the shock is constant within a release, so only between-release "
                "variation identifies the slope. Release fixed effects would absorb it entirely and "
                "are therefore not used."
                if use_shock
                else "descriptive only; no shock column was supplied"
            )
        ),
        "orientation_note": orientation_note,
        "engine": {
            "uncertainty": "cluster bootstrap over releases, percentile intervals",
            "curve_uncertainty": (
                "simultaneous band from the maximum absolute studentized deviation across horizons"
            ),
            "settling": (
                "finite-horizon diagnostic relative to the last horizon in the cell; it is not "
                "evidence of a permanent effect, and a flag means unresolved within the window"
            ),
        },
        "seed": int(seed),
        "bootstrap_samples": int(bootstrap_samples),
        "coverage": float(coverage),
        "shock_column": shock_column,
        "pre_covariates": list(covariates),
        "liquidity_columns": list(liquidity),
        "pre_event_time_columns": dict(covariate_time_columns),
        "event_time_column": event_time_column,
        "covariate_availability": dict(covariate_audit),
        "specification": (
            "row-level parsimonious projection R = alpha + beta*S + gamma*X_before "
            "+ delta*(S x L_before) with the intercept, the shock, each declared pre-state term "
            "and each shock-by-liquidity product. Terms are centered and scaled internally on the "
            "cell's own rows only to condition the solve; every reported coefficient, standard "
            "error and contrast is mapped back to the caller's supplied regressor units, so the "
            "shock coefficient is in probability points per unit of the supplied shock column "
            "with its intercept shifted to the raw scale, and it is comparable across horizons. "
            "The separately named standardized coefficients are diagnostic metadata on the cell's "
            "own scale and are not a horizon-comparable curve. No event fixed effect is fitted, "
            "because the shock is constant within a release and event effects would absorb the "
            "coefficient. This is an association estimate and no causal interpretation is claimed."
            if use_covariates
            else (
                "the scatter of a shock across releases relative to their mean responses"
                if use_shock
                else "descriptive mean response; no shock column was supplied"
            )
        ),
        "orientation_column": orientation_column,
        "orient_response": bool(orient_response),
        "mask_column": mask_column,
        "ineligible_column": ineligible_column,
        "horizon_column": horizon_column,
        "response_column": response_column,
        "event_column": event_column,
        "cluster_column": cluster_column,
        "excluded": exclusions,
        "n_rows_used": len(frame),
        "n_events_used": int(event_series.loc[frame.index].nunique()),
        "n_clusters_used": int(cluster_series.loc[frame.index].nunique()),
        "singleton_clusters": singleton_clusters,
        "singleton_cluster_note": (
            "singleton releases still contribute between-release variation but cannot "
            "contribute to a cluster-robust variance estimate"
        ),
        "min_events": int(min_events),
        "min_clusters": int(min_clusters),
        "cells": cells,
        "curves": curves,
        "status": status,
        "inconclusive_cells": inconclusive,
        "reasons": reasons,
        "promotion_gate": dict(PROMOTION_GATE),
        "model_family_label": MODEL_FAMILY_LABEL,
        "interpretation": INTERPRETATION_LABEL,
    }


def _projection_cell(
    cell_rows: pd.DataFrame,
    *,
    family: str,
    horizon: int,
    use_shock: bool,
    shock_column: str | None,
    response_column: str,
    event_column: str,
    cluster_column: str,
    seed: int,
    bootstrap_samples: int,
    coverage: float,
    min_events: int,
    min_clusters: int,
    curve_clusters: pd.Series | None = None,
    covariates: Sequence[str] = (),
    liquidity_columns: Sequence[str] = (),
    covariate_audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    event_values = cell_rows.groupby(event_column, sort=True)["_response"].mean()
    event_counts = cell_rows.groupby(event_column, sort=True)["_response"].size()
    cluster_of_event = (
        cell_rows.groupby(event_column, sort=True)[cluster_column].first().astype(str)
    )
    n_events = len(event_values)
    n_clusters = int(cluster_of_event.nunique())
    cluster_sizes = cluster_of_event.value_counts()
    base: dict[str, Any] = {
        "family": family,
        "horizon_seconds": int(horizon),
        "n_rows": len(cell_rows),
        "n_events": n_events,
        "n_clusters": n_clusters,
        "cluster_event_counts": {str(k): int(v) for k, v in cluster_sizes.items()},
        "mean_response": float(event_values.mean()),
        "median_response": float(event_values.median()),
        "event_means": {str(key): float(value) for key, value in event_values.items()},
        "mean_across_rows": float(cell_rows["_response"].mean()),
        "shock_column": shock_column,
        "shock_slope": None,
        "shock_slope_ci": None,
        "mean_response_ci": None,
    }
    reasons: list[str] = []
    if n_events < min_events:
        reasons.append(
            f"only {n_events} independent release(s) at h={horizon}s for family {family!r}; "
            f"fewer than min_events={min_events}, so no interval is reported"
        )
    if n_clusters < min_clusters:
        reasons.append(
            f"only {n_clusters} release cluster(s) at h={horizon}s for family {family!r}; "
            f"fewer than min_clusters={min_clusters}, so cluster-robust uncertainty is not "
            "identified"
        )
    leave_one_out = (
        {
            str(event_id): float(event_values.drop(index=event_id).mean())
            for event_id in event_values.index
        }
        if n_events > 1
        else {}
    )
    base["leave_one_event_out_mean_response"] = leave_one_out
    base["leave_one_event_out_max_abs_change"] = (
        max(abs(value - base["mean_response"]) for value in leave_one_out.values())
        if leave_one_out
        else None
    )
    if reasons:
        base["status"] = "inconclusive"
        base["reason"] = "; ".join(reasons)
        base["bootstrap"] = {
            "requested": int(bootstrap_samples),
            "effective_for_mean_response": 0,
            "engine": "cluster bootstrap over releases, percentile intervals",
        }
        return base

    clusters_series = pd.Series(cluster_of_event.to_numpy(), index=event_values.index.astype(str))
    values_series = pd.Series(
        event_values.to_numpy(dtype=np.float64), index=event_values.index.astype(str)
    )
    weights = event_counts.to_numpy(dtype=np.float64)
    event_shocks: pd.Series | None = None
    base["shock_scale"] = None
    base["rank"] = None
    base["standardized_event_shocks"] = None
    base["standardized_shock_slope"] = None

    if use_shock:
        event_shocks = (
            cell_rows.groupby(event_column, sort=True)["_shock"].mean().loc[event_values.index]
        )
        estimate = weighted_event_slope(
            values_series.to_numpy(dtype=np.float64),
            event_shocks.to_numpy(dtype=np.float64),
            weights=weights,
        )
        base["shock_scale"] = estimate.get("shock_scale")
        base["rank"] = estimate.get("rank")
        if estimate["status"] == "ok":
            statistics: dict[str, float] = {
                "mean_response": float(event_values.mean()),
                "shock_slope": float(estimate["raw_slope"]),
            }
            base["shock_slope"] = float(estimate["raw_slope"])
            base["shock_slope_intercept"] = float(estimate["raw_intercept"])
            base["standardized_event_shocks"] = estimate["standardized_shocks"]
            base["standardized_shock_slope"] = float(estimate["slope"])
            base["shock_slope_status"] = "ok"
            base["shock_slope_reason"] = None
        else:
            statistics = {"mean_response": float(event_values.mean())}
            base["shock_slope_status"] = "inconclusive"
            base["shock_slope_reason"] = estimate["reason"]
            reasons.append(estimate["reason"])
    else:
        statistics = {"mean_response": float(event_values.mean())}
        base["shock_slope_status"] = "not_requested"
        base["shock_slope_reason"] = (
            "no shock column was supplied, so no slope is estimated or reported"
        )

    def statistic(sample_events: np.ndarray) -> dict[str, float]:
        labels = [label for label in np.asarray(sample_events) if label in values_series.index]
        if not labels:
            return {}
        subset = values_series.loc[labels]
        payload = {"mean_response": float(subset.mean())}
        if use_shock and event_shocks is not None:
            subset_estimate = weighted_event_slope(
                subset.to_numpy(dtype=np.float64),
                event_shocks.loc[labels].to_numpy(dtype=np.float64),
                weights=event_counts.loc[labels].to_numpy(dtype=np.float64),
            )
            if subset_estimate["status"] == "ok":
                payload["shock_slope"] = float(subset_estimate["raw_slope"])
        return payload

    bootstrap = clustered_bootstrap(
        statistics,
        statistic,
        clusters_series if curve_clusters is None else curve_clusters,
        seed=seed,
        samples=bootstrap_samples,
        coverage=coverage,
    )
    bootstrap["shared_curve_draws"] = curve_clusters is not None
    bootstrap["draw_key"] = _draw_key(
        (clusters_series if curve_clusters is None else curve_clusters).to_numpy().tolist(),
        seed=seed,
        samples=bootstrap_samples,
    )
    bootstrap["curve_releases_resampled"] = None if curve_clusters is None else len(curve_clusters)
    bootstrap["cell_releases_used"] = len(clusters_series)
    if reasons:
        base["status"] = "inconclusive"
        base["reason"] = "; ".join(reasons)
        base["bootstrap"] = bootstrap
        return base

    samples = bootstrap.pop("samples")
    base["bootstrap"] = bootstrap
    for metric in ("mean_response", "shock_slope"):
        interval = samples.get(metric)
        if interval is None:
            continue
        base[f"{metric}_ci"] = {
            "lower": interval["lower"],
            "upper": interval["upper"],
            "percentiles": interval["percentiles"],
            "status": interval["status"],
            "reason": interval["reason"],
        }
    base["shock_slope_ci_status"] = (
        samples["shock_slope"]["status"] if use_shock else "not_requested"
    )
    base["shock_slope_degenerate"] = bool(
        use_shock and "shock_slope" in samples and samples["shock_slope"]["degenerate"]
    )
    base["leave_one_event_out_slope"] = _leave_one_out_slope(
        values_series, event_counts, event_shocks
    )
    if use_shock and base.get("shock_slope") is None:
        base["status"] = "inconclusive"
        base["reason"] = "; ".join(reasons) or base.get("shock_slope_reason")
        return base
    base["status"] = "ok"
    base["reason"] = None
    return base


def _covariate_projection_cell(
    cell_rows: pd.DataFrame,
    *,
    family: str,
    horizon: int,
    shock_column: str,
    covariates: Sequence[str],
    liquidity_columns: Sequence[str],
    event_column: str,
    cluster_column: str,
    seed: int,
    bootstrap_samples: int,
    coverage: float,
    min_events: int,
    min_clusters: int,
    curve_clusters: pd.Series | None,
    covariate_audit: Mapping[str, Any],
) -> dict[str, Any]:
    """One family/horizon cell of the Level-1 row-level projection.

    The specification is the intercept, the shock, each declared pre-state
    covariate, each liquidity main effect and each shock-by-liquidity product. A
    rank-deficient or too-few-release cell is reported as inconclusive with the
    reason, and never as a coefficient no release-level variation supports.
    """
    model = _build_projection_model(
        cell_rows,
        covariates=covariates,
        liquidity_columns=liquidity_columns,
        use_shock=True,
        event_column=event_column,
        cluster_column=cluster_column,
    )
    releases = [str(value) for value in cell_rows[event_column].unique()]
    n_rows = len(cell_rows)
    n_clusters = len({str(value) for value in cell_rows[cluster_column]})
    response_values = cell_rows["_response"].to_numpy(dtype=np.float64)
    event_values = cell_rows.groupby(event_column, sort=True)["_response"].mean()
    if len(event_values) > 1:
        leave_one_out = {
            str(release): float(event_values.drop(index=release).mean())
            for release in event_values.index
        }
    else:
        leave_one_out = {}
    base: dict[str, Any] = {
        "family": family,
        "horizon_seconds": int(horizon),
        "n_rows": n_rows,
        "n_events": len(releases),
        "n_clusters": n_clusters,
        "cluster_event_counts": {
            str(cluster): count
            for cluster, count in cell_rows.groupby(cluster_column)[event_column].nunique().items()
        },
        "mean_response": float(response_values.mean()),
        "median_response": float(np.median(response_values)),
        "event_means": {str(key): float(value) for key, value in event_values.items()},
        "mean_across_rows": float(response_values.mean()),
        "leave_one_event_out_mean_response": leave_one_out,
        "leave_one_event_out_max_abs_change": (
            max(abs(value - float(event_values.mean())) for value in leave_one_out.values())
            if leave_one_out
            else None
        ),
        "shock_column": shock_column,
        "specification": {
            "terms_in_order": ("intercept", *model.coefficient_names),
            "interaction_terms": [term for term, _, _ in model.interaction_pairs],
            "covariate_columns": list(covariates),
            "liquidity_columns": list(liquidity_columns),
            "event_fixed_effects": False,
            "event_fixed_effects_note": (
                "the shock is constant within a release, so event fixed effects would absorb the "
                "coefficient the specification exists to identify; none are fitted"
            ),
            "covariate_availability": dict(covariate_audit),
        },
        "coefficients": None,
        "standardized_coefficients": None,
        "coefficient_units": PROJECTION_COEFFICIENT_UNITS,
        "standardized_coefficient_units": PROJECTION_STANDARDIZED_COEFFICIENT_UNITS,
        "coefficient_standard_errors": None,
        "interaction_test": None,
        "shock_marginal_effects": None,
        "shock_slope": None,
        "shock_slope_ci": None,
        "shock_slope_ci_status": "inconclusive",
        "shock_slope_degenerate": False,
        "standardized_shock_slope": None,
        "regressor_means": None,
        "mean_response_ci": None,
        "leave_one_event_out_slope": None,
        "pre_covariate_columns": list(covariates),
        "liquidity_columns": list(liquidity_columns),
    }

    def inconclusive(*reasons: str, estimate: Mapping[str, Any] | None) -> dict[str, Any]:
        base["status"] = "inconclusive"
        base["reason"] = "; ".join(reason for reason in reasons if reason)
        if estimate is not None:
            base["coefficients"] = dict(estimate["coefficients"])
            base["standardized_coefficients"] = dict(estimate["standardized_coefficients"])
            base["regressor_means"] = dict(estimate["means"])
        base["bootstrap"] = {
            "requested": int(bootstrap_samples),
            "effective_for_shock_slope": 0,
            "engine": "cluster bootstrap over releases, percentile intervals",
            "shared_curve_draws": curve_clusters is not None,
        }
        return base

    if len(releases) < min_events:
        return inconclusive(
            f"only {len(releases)} independent release(s) at h={horizon}s for family {family!r}; "
            f"fewer than min_events={min_events}, so the row-level projection is not estimated",
            estimate=None,
        )
    if n_clusters < max(int(min_clusters), 2):
        return inconclusive(
            f"only {n_clusters} release cluster(s) at h={horizon}s for family {family!r}; fewer "
            f"than min_clusters={max(int(min_clusters), 2)}, so cluster-robust uncertainty is not "
            "identified",
            estimate=None,
        )
    estimate = model.estimate(np.arange(n_rows, dtype=np.int64))
    if estimate is None:
        return inconclusive(
            "the requested design is rank deficient on this cell once every term is standardized "
            f"on its own scale; with {len(releases)} release(s) and "
            f"{len(model.coefficient_names) + 1} requested parameter(s) the coefficients are not "
            "separately identified, so no slope or interaction is reported",
            estimate=None,
        )
    coefficients = estimate["coefficients"]
    base["coefficients"] = dict(coefficients)
    base["standardized_coefficients"] = dict(estimate["standardized_coefficients"])
    base["coefficient_units"] = PROJECTION_COEFFICIENT_UNITS
    base["standardized_coefficient_units"] = PROJECTION_STANDARDIZED_COEFFICIENT_UNITS
    base["shock_slope"] = coefficients.get("shock_slope")
    base["rank"] = int(estimate["rank"])
    base["n_parameters"] = int(estimate["n_parameters"])
    base["shock_scale"] = estimate["scales"].get("shock_slope")
    base["regressor_means"] = dict(estimate["means"])
    base["shock_slope_intercept"] = coefficients.get("intercept")
    base["standardized_shock_slope"] = estimate["standardized_coefficients"].get("shock_slope")
    base["standardized_event_shocks"] = None
    base["pre_covariate_columns"] = list(covariates)
    base["liquidity_columns"] = list(liquidity_columns)
    inference = _model_inference(
        model,
        estimate,
        cluster_codes=model.cluster_codes,
        min_clusters=min_clusters,
    )
    base["coefficient_standard_errors"] = inference.get("standard_errors")
    base["t_statistics"] = inference.get("t_statistics")
    base["interaction_test"] = inference.get("interaction_test")
    base["shock_marginal_effects"] = inference.get("shock_marginal_effects")
    base["inference"] = {
        key: inference.get(key)
        for key in (
            "estimator",
            "covariance",
            "n_parameters",
            "n_clusters",
            "df_residual",
            "status",
            "reason",
        )
    }
    if inference.get("status") != "ok" or coefficients.get("shock_slope") is None:
        return inconclusive(
            inference.get("reason")
            or "the release-clustered variance of the shock slope is not identified",
            estimate=estimate,
        )

    metric = "shock_slope"
    releases_order = sorted(model.row_positions)
    first_row = {release: int(rows[0]) for release, rows in model.row_positions.items()}
    cluster_values = [
        str(cell_rows[cluster_column].to_numpy()[first_row[release]]) for release in releases_order
    ]
    clusters_series = pd.Series(cluster_values, index=releases_order)
    shared = clusters_series if curve_clusters is None else curve_clusters

    def statistic(sample: np.ndarray) -> dict[str, float]:
        sample_rows = model.select([str(label) for label in np.asarray(sample)])
        if sample_rows is None:
            return {}
        drawn = model.coefficients(sample_rows)
        if drawn is None or drawn.get("shock_slope") is None:
            return {}
        return {"shock_slope": float(drawn["shock_slope"])}

    point = {"shock_slope": float(coefficients["shock_slope"])}
    bootstrap = clustered_bootstrap(
        point,
        statistic,
        shared,
        seed=seed,
        samples=bootstrap_samples,
        coverage=coverage,
    )
    samples = bootstrap.pop("samples")
    base["bootstrap"] = bootstrap
    base["bootstrap"]["shared_curve_draws"] = curve_clusters is not None
    base["bootstrap"]["curve_releases_resampled"] = (
        None if curve_clusters is None else len(curve_clusters)
    )
    base["bootstrap"]["cell_releases_used"] = len(clusters_series)
    base["bootstrap"]["draw_key"] = _draw_key(
        [str(label) for label in shared.to_numpy()], seed=seed, samples=bootstrap_samples
    )
    interval = samples.get(metric)
    if interval is not None:
        base[f"{metric}_ci"] = {
            "lower": interval["lower"],
            "upper": interval["upper"],
            "percentiles": interval["percentiles"],
            "status": interval["status"],
            "reason": interval["reason"],
        }
    base["shock_slope_ci_status"] = interval["status"] if interval is not None else "inconclusive"
    base["shock_slope_degenerate"] = bool(interval is not None and interval["degenerate"])
    base["leave_one_event_out_slope"] = _covariate_leave_one_out_slope(model, releases)
    base["status"] = "ok"
    base["reason"] = None
    return base


def _covariate_leave_one_out_slope(
    model: _ProjectionModel,
    releases: Sequence[str],
) -> dict[str, float] | None:
    """Shock slope with each release removed in turn, for influence screening."""
    if len(releases) < 3:
        return None
    results: dict[str, float] = {}
    for release in releases:
        keep = [value for value in releases if value != release]
        rows = model.select(keep)
        if rows is None:
            continue
        coefficients = model.coefficients(rows)
        if coefficients is None or coefficients.get("shock_slope") is None:
            continue
        results[str(release)] = float(coefficients["shock_slope"])
    return results or None


def _pre_event_columns(
    panel: pd.DataFrame,
    *,
    pre_covariates: Sequence[str],
    liquidity_columns: Sequence[str],
    pre_event_time_columns: Mapping[str, str] | None,
    post_event_columns: Sequence[str],
    event_column: str,
    event_time_column: str,
) -> tuple[tuple[str, ...], tuple[str, ...], dict[str, str]]:
    """Validate requested pre-state and liquidity columns against the panel.

    A conventional column inherits ``baseline_time``, because the panel already
    records when that baseline summary was observed. Any other column must name
    the column holding its own observation time, so a caller cannot introduce a
    covariate without stating when it was observed. A column declared post-event
    is refused outright: a mediator observed after the release is not a baseline
    control, and absorbing it into ``gamma`` would redefine the estimand while
    it still carried the shock's name.
    """
    covariates = tuple(dict.fromkeys(str(column) for column in pre_covariates))
    liquidity = tuple(dict.fromkeys(str(column) for column in liquidity_columns))
    declared_post = {str(column) for column in post_event_columns}
    rejected = sorted((set(covariates) | set(liquidity)) & declared_post)
    if rejected:
        raise LocalProjectionError(
            f"column(s) {rejected} are declared post-event, so they are not baseline controls; "
            "a covariate observed at or after the release is a mediator of the response and "
            "cannot be adjusted for while the coefficient is reported as the total shock effect"
        )
    requested = covariates + liquidity
    missing = [column for column in requested if column not in panel.columns]
    if missing:
        raise LocalProjectionError(
            f"requested covariate column(s) {missing} are not in the panel; present columns are "
            f"{sorted(panel.columns)}. A requested regressor is never dropped silently and a "
            "missing covariate is never filled with zero"
        )
    mapping = {
        str(column): str(source) for column, source in dict(pre_event_time_columns or {}).items()
    }
    unknown_mapping = sorted(set(mapping) - set(requested))
    if unknown_mapping:
        raise LocalProjectionError(
            f"pre_event_time_columns names column(s) {unknown_mapping} that were not requested as "
            "a covariate or liquidity column"
        )
    time_columns: dict[str, str] = {}
    for column in requested:
        if column in CONVENTIONAL_PRE_EVENT_COLUMNS:
            time_columns[column] = "baseline_time"
        elif column in mapping:
            time_columns[column] = mapping[column]
        else:
            raise LocalProjectionError(
                f"pre-state column {column!r} declares no observation time; pass "
                f"pre_event_time_columns={{{column!r}: '<time column>'}} naming the column that "
                "holds when it was observed, or use a conventional baseline column "
                f"{list(CONVENTIONAL_PRE_EVENT_COLUMNS)}"
            )
    for column, time_column in time_columns.items():
        if time_column not in panel.columns:
            raise LocalProjectionError(
                f"covariate {column!r} declares availability column {time_column!r}, which is not "
                f"in the panel; present columns are {sorted(panel.columns)}"
            )
    if event_column not in panel.columns:
        raise LocalProjectionError(f"event_column {event_column!r} is not in the panel")
    if event_time_column not in panel.columns:
        raise LocalProjectionError(
            f"event_time_column {event_time_column!r} is not in the panel; a pre-event covariate "
            "is ordered against the release by that column"
        )
    return covariates, liquidity, time_columns


def _covariate_availability(
    frame: pd.DataFrame,
    *,
    time_columns: Mapping[str, str],
    covariates: Sequence[str],
    liquidity_columns: Sequence[str],
    event_time_column: str,
) -> tuple[pd.DataFrame, dict[str, int], dict[str, Any]]:
    """Enforce that every requested pre-state column precedes the release.

    A conventional baseline summary inherits ``baseline_time`` and must be
    observed strictly before the event. A custom covariate declares its own
    availability column and must not be observed after the event. Rows whose
    observation time is unknown, or which are observed at or after the release,
    are excluded with a recorded reason; they are never kept with a missing time
    and the covariate is never imputed.
    """
    exclusions: dict[str, int] = {}
    audit: dict[str, Any] = {}
    event_times = frame[event_time_column]
    if not pd.api.types.is_datetime64_any_dtype(event_times):
        event_times = pd.to_datetime(event_times, utc=True, errors="coerce")
        if event_times.isna().any():
            raise LocalProjectionError(
                f"event_time_column {event_time_column!r} carries no usable event time for every "
                "row, so a pre-event covariate cannot be ordered against the release"
            )
    for column in dict.fromkeys(tuple(covariates) + tuple(liquidity_columns)):
        conventional = column in CONVENTIONAL_PRE_EVENT_COLUMNS
        time_column = time_columns[column]
        declared = frame[time_column]
        if not pd.api.types.is_datetime64_any_dtype(declared):
            declared = pd.to_datetime(declared, utc=True, errors="coerce")
        unknown = declared.isna()
        if unknown.any():
            exclusions[f"missing_availability:{column}"] = int(unknown.sum())
            frame = frame.loc[~unknown]
            declared = declared.loc[frame.index]
            event_times = event_times.loc[frame.index]
            if frame.empty:
                raise LocalProjectionError(
                    f"every eligible row has an unknown observation time for {column!r} in "
                    f"{time_column!r}; a covariate with no recorded availability is not used"
                )
        observed = declared
        event_at = event_times.loc[observed.index]
        if conventional:
            not_before = observed >= event_at
            reason = f"baseline_not_before_event:{column}"
            basis = f"{time_column} strictly before {event_time_column}"
        else:
            not_before = observed > event_at
            reason = f"covariate_after_event:{column}"
            basis = f"{time_column} at or before {event_time_column}"
        if not_before.any():
            exclusions[reason] = int(not_before.sum())
            frame = frame.loc[~not_before]
            if frame.empty:
                raise LocalProjectionError(
                    f"no eligible row has {column!r} observed before the release; every retained "
                    f"row would need {basis}, and a mediator observed after the release is not a "
                    "baseline control"
                )
        audit[column] = {
            "availability_column": time_column,
            "required": basis,
            "source": (
                "conventional baseline summary; inherits the panel's baseline_time"
                if conventional
                else "custom covariate with a declared availability column"
            ),
        }
    return frame, exclusions, audit


def _standardized(values: np.ndarray) -> tuple[np.ndarray, float] | None:
    """Center and scale one regressor, or report that it carries no variation.

    This is an internal conditioning device only. Centering on the cell's own
    mean and scaling by its own population standard deviation keeps the normal
    equations well behaved, and the fitted coefficients and their covariance are
    mapped back to the caller's supplied regressor units before anything is
    reported. The returned spread is the raw standard deviation of the same
    regressor, which is the factor that back-transformation needs.
    """
    spread = float(values.std(ddof=0))
    if not math.isfinite(spread) or spread <= 0.0:
        return None
    return (values - values.mean()) / spread, spread


def _draw_key(
    clusters: Sequence[str],
    *,
    seed: int,
    samples: int,
) -> str:
    """Digest identifying the exact cluster draw a curve reuses at every horizon.

    :func:`market_propagation.evaluation.clustered_bootstrap` derives its
    selections from the seed, the replicate count and the sorted cluster labels,
    so two cells with an equal key are guaranteed to resample the identical
    releases at each replicate index. Recording the key is what lets the
    simultaneous band verify that alignment instead of assuming it.
    """
    labels = sorted({str(label) for label in clusters})
    digest = hashlib.blake2b(digest_size=8)
    for part in ("projection-curve-draw", int(seed), int(samples)):
        digest.update(repr(part).encode("utf-8"))
    for label in labels:
        digest.update(b"\x00")
        digest.update(label.encode("utf-8"))
    return digest.hexdigest()


def _cluster_robust_covariance(
    design: np.ndarray,
    residuals: np.ndarray,
    cluster_codes: np.ndarray,
    *,
    n_parameters: int,
    n_clusters: int,
) -> np.ndarray:
    """Cluster-robust sandwich covariance with a finite-sample correction.

    The independent unit is the release, so the meat sums each cluster's
    cross-product of design rows and residuals rather than each row's. The
    correction uses the usual ``G/(G-1) * (n-1)/(n-k)`` factor.
    """
    bread = np.linalg.pinv(design.T @ design)
    meat = np.zeros((n_parameters, n_parameters), dtype=np.float64)
    for code in range(int(n_clusters)):
        member = cluster_codes == code
        if not member.any():
            continue
        score = design[member].T @ residuals[member]
        meat += np.outer(score, score)
    n_rows = int(design.shape[0])
    if n_clusters > 1 and n_rows > n_parameters:
        correction = (n_clusters / (n_clusters - 1.0)) * ((n_rows - 1.0) / (n_rows - n_parameters))
    else:
        correction = 1.0
    return correction * (bread @ meat @ bread)


@dataclass(frozen=True)
class _ProjectionModel:
    """Row-level parsimonious projection design for one family/horizon cell.

    Terms are the shock, each declared pre-state covariate, each declared
    liquidity variable as its own main effect, and each shock-by-liquidity
    product. An interaction is never fitted without both of its main effects, so
    the reported coefficient on the product is not standing in for a level
    effect. Every requested regressor is kept: one carrying no variation makes
    the design rank deficient rather than being dropped, so a requested
    coefficient is never silently missing from the reported model, and a missing
    covariate is never replaced with a zero.

    No event fixed effect is ever added. The shock is constant within a release,
    so event effects would absorb exactly the coefficient the specification
    exists to identify.
    """

    main_names: tuple[str, ...]
    main_raw: Mapping[str, np.ndarray]
    interaction_pairs: tuple[tuple[str, str, str], ...]
    response: np.ndarray
    row_positions: Mapping[str, np.ndarray]
    cluster_codes: np.ndarray
    liquidity_columns: tuple[str, ...]

    @property
    def coefficient_names(self) -> tuple[str, ...]:
        """Names of the non-intercept coefficients, in design order."""
        return self.main_names + tuple(term for term, _, _ in self.interaction_pairs)

    def select(self, releases: Sequence[str]) -> np.ndarray | None:
        """Row positions for a drawn set of releases, duplicates preserved.

        A release drawn more than once contributes every one of its rows once
        per draw, and every contract of that release comes along, so a resample
        keeps whole releases rather than a scattered subset of their rows. A
        drawn release absent from this cell is skipped, which is how a
        missing-horizon mask survives resampling.
        """
        blocks = [
            self.row_positions[release] for release in releases if release in self.row_positions
        ]
        if not blocks:
            return None
        return np.concatenate(blocks)

    def estimate(self, rows: np.ndarray) -> dict[str, Any] | None:
        """Coefficients, design and residuals for the selected rows.

        Returns ``None`` when the requested design is not identified on those
        rows: a regressor with no variation, or a design that stays rank
        deficient once every term stands on its own scale. Returning a number
        then would hand the caller a coefficient no release-level variation
        supports.

        The solve happens on the standardized design for conditioning, then the
        point estimate is mapped back to the caller's supplied regressor units.
        The map is exact because the standardized columns are affine in the raw
        ones: ``raw_design @ back_transform == standardized design``. So the
        reported intercept and main effects absorb the mean shifts of every
        interacted factor, and ``coefficients`` is on the raw input scale while
        ``standardized_coefficients`` stays available as explicitly named
        diagnostic metadata.
        """
        n_rows = int(rows.shape[0])
        standardized: dict[str, np.ndarray] = {}
        scales: dict[str, float] = {}
        means: dict[str, float] = {}
        for name in self.main_names:
            raw = self.main_raw[name][rows]
            scaled = _standardized(raw)
            if scaled is None:
                return None
            standardized[name], scales[name] = scaled
            means[name] = float(raw.mean())
        blocks: list[np.ndarray] = [np.ones((n_rows, 1), dtype=np.float64)]
        raw_blocks: list[np.ndarray] = [np.ones((n_rows, 1), dtype=np.float64)]
        names: list[str] = ["intercept"]
        for name in self.main_names:
            blocks.append(standardized[name][:, None])
            raw_blocks.append(self.main_raw[name][rows][:, None])
            names.append(name)
        for term, left, right in self.interaction_pairs:
            blocks.append((standardized[left] * standardized[right])[:, None])
            raw_blocks.append((self.main_raw[left][rows] * self.main_raw[right][rows])[:, None])
            names.append(term)
        design = np.column_stack(blocks)
        n_parameters = int(design.shape[1])
        rank = int(np.linalg.matrix_rank(design))
        if rank < n_parameters:
            return None
        target = self.response[rows]
        solution, *_ = np.linalg.lstsq(design, target, rcond=None)
        raw_design = np.column_stack(raw_blocks)
        back_transform, *_ = np.linalg.lstsq(raw_design, design, rcond=None)
        raw_solution = back_transform @ solution
        return {
            "terms": tuple(names),
            "coefficients": {name: float(raw_solution[index]) for index, name in enumerate(names)},
            "standardized_coefficients": {
                name: float(solution[index]) for index, name in enumerate(names)
            },
            "design": design,
            "raw_design": raw_design,
            "back_transform": back_transform,
            "residuals": target - design @ solution,
            "n_rows": n_rows,
            "n_parameters": n_parameters,
            "rank": rank,
            "scales": scales,
            "means": means,
        }

    def coefficients(self, rows: np.ndarray) -> dict[str, float] | None:
        estimate = self.estimate(rows)
        return None if estimate is None else estimate["coefficients"]


def _build_projection_model(
    cell_rows: pd.DataFrame,
    *,
    covariates: Sequence[str],
    liquidity_columns: Sequence[str],
    use_shock: bool,
    event_column: str,
    cluster_column: str,
) -> _ProjectionModel:
    """Assemble the row-level design for one family/horizon cell."""
    main_names: list[str] = []
    main_raw: dict[str, np.ndarray] = {}
    if use_shock:
        main_names.append("shock_slope")
        main_raw["shock_slope"] = cell_rows["_shock"].to_numpy(dtype=np.float64)
    for column in dict.fromkeys(tuple(covariates) + tuple(liquidity_columns)):
        main_names.append(column)
        main_raw[column] = cell_rows[f"_cov_{column}"].to_numpy(dtype=np.float64)
    interaction_pairs = tuple(
        (f"shock_slope_x_{column}", "shock_slope", column) for column in liquidity_columns
    )
    releases = [str(value) for value in cell_rows[event_column]]
    clusters = [str(value) for value in cell_rows[cluster_column]]
    positions: dict[str, list[int]] = {}
    for position, release in enumerate(releases):
        positions.setdefault(release, []).append(position)
    cluster_labels = sorted(set(clusters))
    return _ProjectionModel(
        main_names=tuple(main_names),
        main_raw=main_raw,
        interaction_pairs=interaction_pairs,
        response=cell_rows["_response"].to_numpy(dtype=np.float64),
        row_positions={
            release: np.asarray(rows, dtype=np.int64) for release, rows in positions.items()
        },
        cluster_codes=np.asarray(
            [cluster_labels.index(value) for value in clusters], dtype=np.int64
        ),
        liquidity_columns=tuple(str(column) for column in liquidity_columns),
    )


def _model_inference(
    model: _ProjectionModel,
    estimate: Mapping[str, Any],
    *,
    cluster_codes: np.ndarray,
    min_clusters: int,
) -> dict[str, Any]:
    """Cluster-robust standard errors, contrasts and the interaction test.

    Only the release is treated as an independent unit, so the covariance is
    clustered on the release exactly like every other interval in the package. A
    design with no more releases than parameters, or with fewer than two
    releases, cannot support a variance estimate: the inference is then reported
    as inconclusive with the count that blocked it, and the point coefficients
    are still returned.

    Standard errors and contrasts are reported on the caller's supplied
    regressor scale. The sandwich is computed on the standardized design and then
    carried through the same affine map as the coefficients, ``R V R'``, so the
    reported uncertainty belongs to the reported estimand instead of to the
    internal conditioning change of variables. The Wald statistic is invariant
    under that invertible reparameterization, so no cutoff or inference gate
    moves.
    """
    terms = list(estimate["terms"])
    design = estimate["design"]
    residuals = np.asarray(estimate["residuals"], dtype=np.float64)
    n_parameters = int(estimate["n_parameters"])
    n_rows = int(estimate["n_rows"])
    selected_codes = np.asarray(cluster_codes, dtype=np.int64)
    n_clusters = len(set(selected_codes.tolist()))
    payload: dict[str, Any] = {
        "n_parameters": n_parameters,
        "n_clusters": n_clusters,
        "df_residual": max(n_rows - n_parameters, 0),
        "terms": terms,
        "estimator": "row_level_least_squares",
        "covariance": "release-clustered sandwich, transformed to the supplied regressor units",
    }
    if n_clusters < max(int(min_clusters), 2) or n_clusters <= n_parameters:
        payload["status"] = "inconclusive"
        payload["reason"] = (
            f"{n_clusters} release(s) for a {n_parameters}-parameter design; a "
            "release-clustered variance estimate needs more releases than parameters, so no "
            "standard error, contrast or interaction test is reported"
        )
        payload["standard_errors"] = None
        payload["t_statistics"] = None
        payload["shock_marginal_effects"] = None
        payload["interaction_test"] = None
        return payload
    back_transform = np.asarray(estimate["back_transform"], dtype=np.float64)
    covariance_standardized = _cluster_robust_covariance(
        design,
        residuals,
        selected_codes,
        n_parameters=n_parameters,
        n_clusters=n_clusters,
    )
    covariance = back_transform @ covariance_standardized @ back_transform.T
    diagonal = np.diag(covariance)
    standard_errors = {
        name: (math.sqrt(float(diagonal[index])) if diagonal[index] >= 0.0 else float("nan"))
        for index, name in enumerate(terms)
    }
    coefficients = estimate["coefficients"]
    payload["standard_errors"] = standard_errors
    payload["t_statistics"] = {
        name: (
            float(coefficients[name]) / standard_errors[name]
            if standard_errors[name]
            and math.isfinite(standard_errors[name])
            and standard_errors[name] > 0.0
            else None
        )
        for name in terms
        if name != "intercept"
    }
    payload["variance"] = {
        name: {
            "diagonal": float(diagonal[index]),
            "standard_error": standard_errors[name],
        }
        for index, name in enumerate(terms)
    }
    position_of = {name: index for index, name in enumerate(terms)}
    interaction_terms = [term for term, _, _ in model.interaction_pairs]
    interaction_positions = [position_of[name] for name in interaction_terms]
    shock_position = position_of.get("shock_slope")
    if shock_position is None:
        payload["shock_marginal_effects"] = None
        payload["interaction_test"] = None
        payload["status"] = "ok"
        payload["reason"] = None
        return payload
    coefficient_vector = np.array([coefficients[name] for name in terms], dtype=np.float64)

    def contrast(weights: np.ndarray) -> dict[str, Any]:
        variance = float(weights @ covariance @ weights)
        return {
            "value": float(weights @ coefficient_vector),
            "standard_error": math.sqrt(variance) if variance >= 0.0 else float("nan"),
        }

    liquidity_means = estimate["means"]
    liquidity_scales = estimate["scales"]

    def effect_at_liquidity(
        *,
        moved_column: str | None = None,
        offset_sd: float = 0.0,
    ) -> dict[str, Any]:
        """Shock slope with interacted liquidity held at supplied input levels.

        Liquidity enters at its own raw level, so the contrast is a marginal
        effect in the caller's units rather than one per standard deviation of a
        standardized regressor. With ``moved_column`` unset, every interacted
        liquidity variable moves together; with it set, only that variable moves
        and the rest stay at their cell mean, which is the per-variable marginal
        effect. The mean level is the incremental effect of liquidity at its
        mean, which differs from the raw shock coefficient whenever either mean
        is nonzero.
        """
        weights = np.zeros(n_parameters, dtype=np.float64)
        weights[shock_position] = 1.0
        for column in model.liquidity_columns:
            term = f"shock_slope_x_{column}"
            if term not in position_of:
                continue
            level = float(liquidity_means[column])
            if moved_column is None or moved_column == column:
                level += offset_sd * float(liquidity_scales[column])
            weights[position_of[term]] = level
        return contrast(weights)

    if interaction_positions:
        interaction_values = np.array(
            [coefficients[terms[position]] for position in interaction_positions],
            dtype=np.float64,
        )
        block = covariance[np.ix_(interaction_positions, interaction_positions)]
        try:
            statistic = float(interaction_values @ np.linalg.solve(block, interaction_values))
        except np.linalg.LinAlgError:
            statistic = float("nan")
        degrees = len(interaction_positions)
        p_value = (
            float(stats.chi2.sf(statistic, degrees))
            if math.isfinite(statistic) and degrees > 0
            else None
        )
        payload["interaction_test"] = {
            "null": "every shock-by-liquidity coefficient is zero",
            "statistic": statistic,
            "degrees_of_freedom": degrees,
            "p_value": p_value,
            "terms": interaction_terms,
            "status": "ok" if p_value is not None else "inconclusive",
            "note": (
                "chi-square Wald statistic from the release-clustered covariance with "
                f"{n_clusters} release(s); it is a finite-sample approximation and is not a "
                "correction for a few clusters, and it is invariant to the change of units "
                "between the raw and standardized parameterizations"
            ),
        }
        payload["shock_marginal_effects"] = {
            "definition": (
                "shock slope evaluated at interacted pre-event liquidity, in probability points "
                "per unit of the shock column the caller supplied; coefficients and contrasts use "
                "that one unit at every horizon"
            ),
            "at_mean_liquidity": effect_at_liquidity(),
            "at_minus_one_sd_liquidity": effect_at_liquidity(offset_sd=-1.0),
            "at_plus_one_sd_liquidity": effect_at_liquidity(offset_sd=1.0),
            "all_liquidity_moved_together": True,
            "note": (
                "the mean-liquidity contrast is the shock's marginal effect at the interacted "
                "liquidity means and differs from the reported raw shock coefficient whenever a "
                "regressor mean is nonzero; each one-standard-deviation contrast moves every "
                "interacted liquidity variable by one of its own input units together, and the "
                "per-variable contrasts below move one at a time while the others stay at their "
                "cell means"
            ),
            "by_liquidity_variable": {
                column: {
                    "at_minus_one_sd": effect_at_liquidity(moved_column=column, offset_sd=-1.0),
                    "at_plus_one_sd": effect_at_liquidity(moved_column=column, offset_sd=1.0),
                }
                for column in model.liquidity_columns
                if f"shock_slope_x_{column}" in position_of
            },
        }
    else:
        payload["interaction_test"] = {
            "null": "no shock-by-liquidity interaction was requested",
            "statistic": None,
            "degrees_of_freedom": 0,
            "p_value": None,
            "terms": [],
            "status": "not_requested",
            "note": (
                "no liquidity column was requested, so the shock slope is a single marginal "
                "effect and there is no interaction to test"
            ),
        }
        payload["shock_marginal_effects"] = {
            "definition": (
                "shock slope in probability points per unit of the shock column the caller "
                "supplied, with no interaction term in the specification"
            ),
            "at_mean_liquidity": effect_at_liquidity(),
            "at_minus_one_sd_liquidity": None,
            "at_plus_one_sd_liquidity": None,
            "all_liquidity_moved_together": None,
            "by_liquidity_variable": {},
            "note": "no liquidity column was requested, so only the mean-level marginal effect exists",
        }
    payload["status"] = "ok"
    payload["reason"] = None
    return payload


def _leave_one_out_slope(
    values: pd.Series,
    weights: pd.Series,
    shocks: pd.Series | None,
) -> dict[str, float] | None:
    """Slope with each release removed in turn, for influence screening."""
    if shocks is None or len(values) < 3:
        return None
    results: dict[str, float] = {}
    for event_id in values.index:
        keep = [label for label in values.index if label != event_id]
        estimate = weighted_event_slope(
            values.loc[keep].to_numpy(dtype=np.float64),
            shocks.loc[keep].to_numpy(dtype=np.float64),
            weights=weights.loc[keep].to_numpy(dtype=np.float64),
        )
        if estimate["status"] == "ok":
            results[str(event_id)] = float(estimate["raw_slope"])
    return results or None


def _settling_flags(
    cells: Mapping[int, Mapping[str, Any]],
    horizons: Sequence[int],
    tolerance: float,
) -> dict[str, Any]:
    """Finite-horizon overshoot and settling diagnostics for one family curve."""
    usable = [horizon for horizon in horizons if cells[horizon]["status"] == "ok"]
    payload: dict[str, Any] = {
        "horizons_seconds": list(horizons),
        "usable_horizons_seconds": usable,
        "tolerance": float(tolerance),
        "note": (
            "settling is measured against the last usable horizon in the cell, so it is a "
            "within-window convergence diagnostic and not evidence of a permanent effect"
        ),
    }
    if not usable:
        return payload | {"status": "inconclusive", "reason": "no usable horizon in this cell"}
    last = usable[-1]
    slope_curve = [cells[horizon]["shock_slope"] for horizon in usable]
    mean_curve = [cells[horizon]["mean_response"] for horizon in usable]
    payload["endpoint_horizon_seconds"] = last
    if any(value is None for value in slope_curve):
        payload["signed_overshoot"] = None
        payload["overshoot"] = None
        payload["slope_status"] = "inconclusive"
        payload["slope_reason"] = "at least one horizon has no identified shock slope"
    else:
        payload["signed_overshoot"] = float(max(slope_curve, key=abs) - slope_curve[-1])
        payload["overshoot"] = float(
            max(abs(value) for value in slope_curve) - abs(slope_curve[-1])
        )
        payload["slope_status"] = "ok"
    if any(value is None for value in mean_curve):
        payload["mean_settling_time_seconds"] = None
        payload["mean_status"] = "inconclusive"
    else:
        payload["mean_status"] = "ok"
        settled = None
        for position, horizon in enumerate(usable):
            endpoint = mean_curve[-1]
            if all(
                abs(mean_curve[later] - endpoint) <= tolerance
                for later in range(position, len(usable))
            ):
                settled = horizon
                break
        payload["mean_settling_time_seconds"] = settled
        payload["unresolved_within_window"] = settled is None
    return payload
