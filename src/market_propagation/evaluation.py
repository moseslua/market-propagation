"""Chronological evaluation, uncertainty, falsifiers and power assessment.

The independent information unit is the economic release, so every mean,
interval and resampling step here operates over releases and clusters rather
than over rows. Three things follow from that and are enforced by the code:

* Chronological splits assign whole releases, including cross-venue
  equivalents that share a cluster, to one fold, and purge rows whose forecast
  or label window crosses a fold boundary.
* Interval estimates use a cluster bootstrap over releases. Replicates are
  indexed by a seeded, label-derived draw, so cells computed from the same
  release set share their draws and their replicates can be combined into a
  simultaneous band over the whole response curve.
* With few clusters, a degenerate replicate distribution is reported as
  inconclusive rather than as a zero-width interval. Asymptotic precision is
  not treated as settled when the cluster count cannot support it.

Terminal binary scoring lives in :func:`resolution_scores` and is kept distinct
from probability-point forecast error: millions of snapshots of the same
eventual outcome are not millions of independent binary trials, so scores are
aggregated to the release first and the effective unit count is reported.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

__all__ = [
    "DEFAULT_ALPHA",
    "DEFAULT_PERMUTATIONS",
    "DEFAULT_REPETITIONS",
    "ExclusionLedger",
    "ForecastEvaluationError",
    "ScoreConventionError",
    "chronological_splits",
    "classify_outcome",
    "cluster_bootstrap",
    "clustered_bootstrap",
    "curve_uncertainty",
    "evaluate_quotes",
    "event_level_slope",
    "feasibility_distribution",
    "forecast_scores",
    "label_permutation_placebo",
    "null_false_positive_rate",
    "observed_spread_summary",
    "placebo_tests",
    "power_assessment",
    "resolution_scores",
    "sensitivity_analysis",
    "simulation_calibration",
    "weighted_event_slope",
]

DEFAULT_ALPHA = 0.05
DEFAULT_REPETITIONS = 400
DEFAULT_PERMUTATIONS = 200

#: Log-loss and Brier boundary convention. Probabilities are clipped into
#: ``[epsilon, 1 - epsilon]`` before scoring, and the number of clipped rows is
#: always reported, because an unclipped log loss is infinite on a wrong
#: certain prediction.
PROBABILITY_EPSILON = 1e-12


class ForecastEvaluationError(ValueError):
    """The supplied table or option set does not satisfy the evaluation contract."""


class ScoreConventionError(ValueError):
    """A scoring input violates the declared boundary convention."""


#: Rows excluded by one pass, with the reason they were dropped.
class ExclusionLedger:
    """Ordered reason -> count ledger for the inclusion/exclusion process."""

    __slots__ = ("_counts",)

    def __init__(self, counts: Mapping[str, int] | None = None) -> None:
        self._counts: dict[str, int] = dict(counts or {})

    def add(self, reason: str, count: int) -> None:
        if count:
            self._counts[reason] = self._counts.get(reason, 0) + int(count)

    def as_dict(self) -> dict[str, int]:
        return dict(self._counts)

    def total(self) -> int:
        return int(sum(self._counts.values()))

    def as_records(self) -> list[dict[str, Any]]:
        return [
            {"reason": reason, "count": count} for reason, count in sorted(self._counts.items())
        ]


def _stable_seed(*parts: Any) -> int:
    """Process-independent seed derived from labels, never from ``hash``."""
    digest = hashlib.blake2b(digest_size=8)
    for part in parts:
        digest.update(repr(part).encode("utf-8"))
        digest.update(b"\x1f")
    return int.from_bytes(digest.digest(), "big") % (2**32)


def _as_finite(array: Any, *, name: str, context: str) -> np.ndarray:
    values = np.asarray(array, dtype=np.float64)
    if values.ndim != 1:
        raise ForecastEvaluationError(
            f"{context}: {name} must be one-dimensional, got shape {values.shape}"
        )
    if not np.isfinite(values).all():
        bad = np.argwhere(~np.isfinite(values)).ravel()
        raise ForecastEvaluationError(
            f"{context}: {name} has non-finite entries at indices {bad.tolist()[:5]}"
        )
    return values


def _require_columns(table: pd.DataFrame, columns: Sequence[str], *, context: str) -> None:
    missing = [column for column in columns if column not in table.columns]
    if missing:
        raise ForecastEvaluationError(
            f"{context} requires column(s) {missing}; present columns are {sorted(table.columns)}"
        )


def _unit_series(
    frame: pd.DataFrame,
    *,
    cluster_column: str,
    event_column: str,
    time_column: str,
) -> pd.DataFrame:
    """One row per release unit, ordered by its earliest event time."""
    grouping = cluster_column if cluster_column in frame.columns else event_column
    units = (
        frame.groupby(grouping, sort=False)
        .agg(
            first_event_time=(time_column, "min"),
            n_events=(event_column, "nunique"),
            n_rows=(event_column, "size"),
        )
        .reset_index()
        .rename(columns={grouping: "unit_id"})
    )
    units["unit_id"] = units["unit_id"].astype(str)
    return units.sort_values(["first_event_time", "unit_id"], kind="stable").reset_index(drop=True)


def _resolve_span_seconds(
    frame: pd.DataFrame,
    *,
    time_column: str,
    horizon_column: str,
    label_column: str,
) -> pd.Series:
    """How far past the event time a row's window reaches, from real columns only."""
    if label_column in frame.columns:
        labels = pd.to_datetime(frame[label_column], utc=True, errors="coerce")
        if labels.notna().all():
            times = pd.to_datetime(frame[time_column], utc=True, errors="coerce")
            return (labels - times).dt.total_seconds()
    if horizon_column in frame.columns:
        horizons = pd.to_numeric(frame[horizon_column], errors="coerce")
        return horizons.fillna(0.0)
    raise ForecastEvaluationError(
        f"cannot determine the label window: neither {label_column!r} nor "
        f"{horizon_column!r} is usable"
    )


def chronological_splits(
    data: pd.DataFrame,
    *,
    train_fraction: float = 0.6,
    validation_fraction: float = 0.2,
    embargo_seconds: float | None = None,
    event_column: str = "event_id",
    cluster_column: str = "cluster_id",
    time_column: str = "event_time",
    horizon_column: str = "horizon_seconds",
    label_column: str = "target_available_time",
    mask_column: str = "valid",
    include_masked: bool = False,
    min_events_per_fold: int = 1,
) -> dict[str, pd.DataFrame]:
    """Chronological development, validation and locked-test folds.

    Releases are ordered by their earliest event time and cut into three
    consecutive folds. ``cluster_column`` defines the release unit, so
    cross-venue equivalents that share a cluster stay together even when they
    carry different ``event_id`` values. Rows whose label window reaches past a
    fold boundary are purged from that fold, which is the horizon- and
    label-availability-based purge ``configs/study_v1.yaml`` requires rather than
    a ceremonial fixed embargo.

    Each returned frame carries ``split`` and ``training_cutoff`` set to real
    values: the fit cutoff that applies to that fold. Masked rows are excluded
    unless ``include_masked=True``, and every exclusion is counted in
    ``frame.attrs['exclusions']``.
    """
    if not isinstance(data, pd.DataFrame):
        raise ForecastEvaluationError(
            f"chronological_splits expects a pandas DataFrame, got {type(data).__name__}"
        )
    if data.empty:
        raise ForecastEvaluationError("chronological_splits received an empty table")
    _require_columns(
        data, (event_column, time_column, horizon_column), context="chronological_splits"
    )
    if not 0.0 < train_fraction < 1.0 or not 0.0 < validation_fraction < 1.0:
        raise ForecastEvaluationError("train_fraction and validation_fraction must lie in (0, 1)")
    if train_fraction + validation_fraction >= 1.0:
        raise ForecastEvaluationError(
            "train_fraction + validation_fraction must leave a locked-test fold"
        )

    ledger = ExclusionLedger()
    frame = data.copy()
    frame[time_column] = pd.to_datetime(frame[time_column], utc=True, errors="coerce")
    unparsable = int(frame[time_column].isna().sum())
    ledger.add("unparsable_event_time", unparsable)
    frame = frame.loc[frame[time_column].notna()]
    if frame.empty:
        raise ForecastEvaluationError("no row has a parsable event time")

    if mask_column in frame.columns and not include_masked:
        masked = ~frame[mask_column].fillna(False).astype(bool)
        ledger.add(f"masked_{mask_column}", int(masked.sum()))
        frame = frame.loc[~masked]
    if frame.empty:
        raise ForecastEvaluationError(
            f"every row is masked by {mask_column!r}; pass include_masked=True to keep them"
        )

    span = _resolve_span_seconds(
        frame,
        time_column=time_column,
        horizon_column=horizon_column,
        label_column=label_column,
    )
    frame["_span_seconds"] = np.clip(pd.to_numeric(span, errors="coerce").fillna(0.0), 0.0, None)

    units = _unit_series(
        frame, cluster_column=cluster_column, event_column=event_column, time_column=time_column
    )
    n_units = len(units)
    if n_units < 3:
        raise ForecastEvaluationError(
            f"a three-way chronological split needs at least 3 releases, got {n_units}"
        )
    n_train = max(1, min(math.floor(n_units * train_fraction), n_units - 2))
    n_validation = max(1, min(math.floor(n_units * validation_fraction), n_units - n_train - 1))
    if n_units - n_train - n_validation < 1:  # pragma: no cover - clamped above
        raise ForecastEvaluationError(
            f"{n_units} releases do not divide into train, validation and test folds with "
            f"train_fraction={train_fraction} and validation_fraction={validation_fraction}"
        )
    train_units = units.iloc[:n_train]
    validation_units = units.iloc[n_train : n_train + n_validation]
    test_units = units.iloc[n_train + n_validation :]
    if min(len(train_units), len(validation_units), len(test_units)) < min_events_per_fold:
        raise ForecastEvaluationError(
            f"at least one fold has fewer than min_events_per_fold={min_events_per_fold} releases"
        )
    train_boundary = pd.Timestamp(train_units["first_event_time"].max())
    validation_boundary = pd.Timestamp(validation_units["first_event_time"].max())

    grouping = cluster_column if cluster_column in frame.columns else event_column
    frame["_unit_id"] = frame[grouping].astype(str)
    assignment = {
        **dict.fromkeys(train_units["unit_id"], "train"),
        **dict.fromkeys(validation_units["unit_id"], "validation"),
        **dict.fromkeys(test_units["unit_id"], "test"),
    }
    frame["split"] = frame["_unit_id"].map(assignment)
    if embargo_seconds is None:
        embargo = float(max(0.0, frame["_span_seconds"].quantile(0.95) - 0.0))
    else:
        embargo = float(embargo_seconds)
        if not math.isfinite(embargo) or embargo < 0.0:
            raise ForecastEvaluationError(
                f"embargo_seconds={embargo_seconds!r} must be finite and non-negative"
            )
    # The purge boundary is the start of the *following* fold, not each fold's own
    # last event time: a label window that reaches into the next fold's territory
    # is what leaks, while a fold's own final release is legitimate data. Using
    # the fold's own boundary would discard that release on every run.
    boundary_of = {
        "train": pd.Timestamp(validation_units["first_event_time"].min()),
        "validation": pd.Timestamp(test_units["first_event_time"].min()),
    }
    used_cutoff: dict[str, pd.Timestamp] = {
        "train": train_boundary,
        "validation": train_boundary,
        "test": validation_boundary,
    }
    purged = 0
    for fold, boundary in boundary_of.items():
        selector = frame["split"] == fold
        straddling = selector & (
            frame[time_column] + pd.to_timedelta(frame["_span_seconds"] + embargo, unit="s")
            > boundary
        )
        purged += int(straddling.sum())
        frame.loc[straddling, "split"] = None
    frame = frame.loc[frame["split"].notna()].copy()
    ledger.add("purged_fold_boundary_overlap", purged)
    frame["training_cutoff"] = frame["split"].map(used_cutoff)
    for fold in ("train", "validation", "test"):
        if not (frame["split"] == fold).any():
            raise ForecastEvaluationError(
                f"purge and embargo rules left fold {fold!r} empty; widen the fold definitions "
                "or relax the embargo"
            )

    frame = frame.drop(columns=["_span_seconds", "_unit_id"])
    folds: dict[str, pd.DataFrame] = {}
    for fold in ("train", "validation", "test"):
        part = frame.loc[frame["split"] == fold].reset_index(drop=True)
        part.attrs["fold"] = fold
        part.attrs["exclusions"] = ledger.as_records()
        part.attrs["policy"] = {
            "assignment": (
                "whole release unit (cluster_id when present, otherwise event_id), ordered by "
                "earliest event time"
            ),
            "n_units": int(n_units),
            "units_train": len(train_units),
            "units_validation": len(validation_units),
            "units_test": len(test_units),
            "train_cutoff": train_boundary.isoformat(),
            "validation_cutoff": validation_boundary.isoformat(),
            "training_cutoff_applied": used_cutoff[fold].isoformat(),
            "embargo_seconds": embargo,
            "embargo_basis": (
                "95th percentile of the observed label span plus the declared embargo; the purge "
                "uses the actual horizon and label availability, not a fixed number of days"
            ),
            "purged_rows": int(purged),
            "include_masked": bool(include_masked),
            "cross_venue_equivalents": (
                "units sharing cluster_id stay in one fold even when their event_id differs"
            ),
        }
        part.attrs["exclusions"] = ledger.as_records()
        folds[fold] = part
    return folds


def _cluster_draws(cluster_labels: Sequence[str], *, seed: int, samples: int) -> list[np.ndarray]:
    """One cluster draw per replicate, identical for identical cluster sets."""
    labels = sorted({str(label) for label in cluster_labels})
    rng = np.random.default_rng(_stable_seed("cluster-bootstrap", seed, labels, samples))
    positions = np.arange(len(labels))
    return [rng.choice(positions, size=len(labels), replace=True) for _ in range(samples)]


def clustered_bootstrap(
    statistics: Mapping[str, float],
    statistic: Callable[[np.ndarray], Mapping[str, float]],
    clusters: pd.Series,
    *,
    seed: int = 20260913,
    samples: int = 200,
    coverage: float = 0.95,
) -> dict[str, Any]:
    """Percentile cluster bootstrap over release clusters.

    ``clusters`` is indexed by unit label (one unit per release) and holds the
    cluster each unit belongs to. Each replicate draws clusters with
    replacement, keeps every unit of a drawn cluster, and evaluates
    ``statistic`` on the selected unit labels.

    The draw depends only on the seed, the replicate count and the sorted
    cluster labels, so two statistics over the same release set share their
    draws and their replicate vectors can be combined into a simultaneous band.
    A degenerate replicate distribution (zero width, or fewer than two
    clusters) is reported as such; it never masquerades as a precise interval.
    """
    if not isinstance(clusters, pd.Series):
        raise ForecastEvaluationError(
            f"clusters must be a pandas Series indexed by unit label, got {type(clusters).__name__}"
        )
    if clusters.empty:
        raise ForecastEvaluationError("clusters is empty; there is nothing to resample")
    if int(samples) < 2:
        raise ForecastEvaluationError(f"samples={samples!r} must be at least 2")
    if not 0.0 < float(coverage) < 1.0:
        raise ForecastEvaluationError(f"coverage={coverage!r} must lie strictly inside (0, 1)")

    labels = np.asarray([str(label) for label in clusters.index], dtype=object)
    cluster_values = np.asarray([str(value) for value in clusters.to_numpy()], dtype=object)
    unique_clusters = sorted(set(cluster_values.tolist()))
    n_clusters = len(unique_clusters)
    metric_names = list(statistics)
    replicates: dict[str, list[float | None]] = {name: [] for name in metric_names}
    if n_clusters < 2:
        return {
            "method": "cluster bootstrap over releases, percentile intervals",
            "n_units": len(labels),
            "n_clusters": int(n_clusters),
            "samples_requested": int(samples),
            "samples_effective": 0,
            "coverage": float(coverage),
            "seed": int(seed),
            "degenerate": True,
            "replicates": {name: [] for name in metric_names},
            "samples": {
                name: {
                    "point": float(statistics[name]),
                    "lower": None,
                    "upper": None,
                    "percentiles": None,
                    "n_effective": 0,
                    "degenerate": True,
                    "status": "inconclusive",
                    "reason": (
                        f"only {n_clusters} release cluster(s); a cluster-robust interval is not "
                        "identified and no numeric interval is reported"
                    ),
                }
                for name in metric_names
            },
        }

    draws = _cluster_draws(unique_clusters, seed=int(seed), samples=int(samples))
    positions_of_cluster = {
        cluster: np.flatnonzero(cluster_values == cluster) for cluster in unique_clusters
    }
    for draw in draws:
        selected = np.concatenate(
            [positions_of_cluster[unique_clusters[position]] for position in draw]
        )
        payload = statistic(labels[selected])
        for name in metric_names:
            value = payload.get(name)
            replicates[name].append(
                None if value is None or not math.isfinite(float(value)) else float(value)
            )

    percentile = 100.0 * (1.0 - float(coverage)) / 2.0
    interval_percentiles = [percentile, 100.0 - percentile]
    results: dict[str, Any] = {}
    degenerate_count = 0
    for name in metric_names:
        values = np.asarray(
            [value for value in replicates[name] if value is not None], dtype=np.float64
        )
        if values.size < 2:
            results[name] = {
                "point": float(statistics[name]),
                "lower": None,
                "upper": None,
                "percentiles": None,
                "n_effective": int(values.size),
                "degenerate": True,
                "status": "inconclusive",
                "reason": (
                    f"only {values.size} usable replicate(s) for {name!r}; no interval is reported"
                ),
            }
            degenerate_count += 1
            continue
        lower = float(np.percentile(values, interval_percentiles[0]))
        upper = float(np.percentile(values, interval_percentiles[1]))
        width = upper - lower
        is_degenerate = width <= 0.0 or float(values.std(ddof=0)) <= 0.0
        if is_degenerate:
            degenerate_count += 1
        results[name] = {
            "point": float(statistics[name]),
            "lower": lower,
            "upper": upper,
            "percentiles": interval_percentiles,
            "n_effective": int(values.size),
            "degenerate": bool(is_degenerate),
            "status": "degenerate" if is_degenerate else "ok",
            "reason": (
                "the replicate distribution has zero width; with this many clusters the interval "
                "is not informative"
                if is_degenerate
                else None
            ),
        }
    first = metric_names[0] if metric_names else None
    effective = int(results[first]["n_effective"]) if first is not None and first in results else 0
    return {
        "method": "cluster bootstrap over releases, percentile intervals",
        "n_units": len(labels),
        "n_clusters": int(n_clusters),
        "samples_requested": int(samples),
        "samples_effective": effective,
        "coverage": float(coverage),
        "seed": int(seed),
        "degenerate": bool(degenerate_count == len(metric_names) and metric_names),
        "percentiles": interval_percentiles,
        "replicates": replicates,
        "samples": results,
    }


def cluster_bootstrap(
    values: Any,
    clusters: Any,
    *,
    seed: int = 20260913,
    samples: int = 200,
    coverage: float = 0.95,
    statistic: Callable[[np.ndarray], float] | None = None,
    name: str = "statistic",
) -> dict[str, Any]:
    """Cluster bootstrap of a scalar statistic over per-unit values.

    ``values`` and ``clusters`` are equal-length arrays of unit-level values
    and their cluster labels; a pandas Series may be passed for either. The
    default statistic is the mean. Returns the
    :func:`clustered_bootstrap` payload with the scalar result under ``samples``.
    """
    values_series = (
        values if isinstance(values, pd.Series) else pd.Series(np.asarray(values, dtype=np.float64))
    )
    cluster_series = (
        clusters
        if isinstance(clusters, pd.Series)
        else pd.Series(list(clusters), index=values_series.index)
    )
    if len(cluster_series) != len(values_series):
        raise ForecastEvaluationError(
            f"values has {len(values_series)} unit(s) but clusters has {len(cluster_series)}"
        )
    # Work on one stable string index so both series and the statistic agree.
    labels = [str(label) for label in values_series.index]
    ordered = pd.Series(values_series.to_numpy(dtype=np.float64), index=labels)
    cluster_series = pd.Series(cluster_series.to_numpy(), index=labels)
    if statistic is None:
        runner = lambda sample: float(np.mean(ordered.loc[list(sample)]))  # noqa: E731
    else:
        runner = lambda sample: float(statistic(ordered.loc[list(sample)]))  # noqa: E731
    point = {name: float(runner(np.asarray(labels, dtype=object)))}
    result = clustered_bootstrap(
        point,
        lambda sample: {name: runner(sample)},
        cluster_series,
        seed=seed,
        samples=samples,
        coverage=coverage,
    )
    return result


def curve_uncertainty(
    cells: Sequence[Mapping[str, Any]],
    *,
    kind: str = "shock_slope",
    coverage: float = 0.95,
) -> dict[str, Any]:
    """Simultaneous band for a whole response curve from aligned replicates.

    Each cell supplies its point estimate and its bootstrap replicates for
    ``kind``. Replicates are aligned by replicate index, which is valid because
    every cell in a curve is computed over the same release set with the same
    seeded cluster draws. The critical value is the ``coverage`` quantile of the
    maximum absolute studentized deviation across cells, so the resulting band
    covers the entire curve rather than one horizon at a time.

    Cells without an identified statistic, or with a zero-width replicate
    distribution, are excluded from the maximum and named in ``excluded``.
    If fewer than two cells remain usable the band is reported as inconclusive.
    """
    points: list[float | None] = []
    horizons: list[Any] = []
    usable_positions: list[int] = []
    excluded: list[dict[str, Any]] = []
    replicate_columns: list[list[float | None]] = []
    per_cell_status: list[str] = []
    for position, cell in enumerate(cells):
        horizons.append(cell.get("horizon_seconds"))
        point = cell.get(kind)
        bootstrap = cell.get("bootstrap") or {}
        replicate_values = (bootstrap.get("replicates") or {}).get(kind) or []
        points.append(None if point is None else float(point))
        status = "ok"
        if point is None:
            status = "no_statistic"
        elif len(replicate_values) < 2:
            status = "no_replicates"
        per_cell_status.append(status)
        if status == "ok":
            usable_positions.append(position)
        else:
            excluded.append({"horizon_seconds": cell.get("horizon_seconds"), "reason": status})
        replicate_columns.append(list(replicate_values))
    if len(usable_positions) < 2:
        return {
            "kind": kind,
            "coverage": float(coverage),
            "n_cells": len(cells),
            "horizons_seconds": horizons,
            "point": points,
            "simultaneous_lower": None,
            "simultaneous_upper": None,
            "critical_value": None,
            "status": "inconclusive",
            "reason": (
                f"only {len(usable_positions)} cell(s) have an identified {kind!r} with usable "
                "replicates; a simultaneous band needs at least two"
            ),
            "excluded": excluded,
            "per_cell_status": per_cell_status,
            "usable_cells": len(usable_positions),
        }
    lengths = {len(replicate_columns[position]) for position in usable_positions}
    width = min(lengths)
    matrix = np.array(
        [
            [
                math.nan if value is None else float(value)
                for value in replicate_columns[position][:width]
            ]
            for position in usable_positions
        ],
        dtype=np.float64,
    )
    if np.isnan(matrix).any():
        keep = ~np.isnan(matrix).any(axis=0)
        matrix = matrix[:, keep]
    if matrix.shape[1] < 2:
        return {
            "kind": kind,
            "coverage": float(coverage),
            "n_cells": len(cells),
            "horizons_seconds": horizons,
            "point": points,
            "simultaneous_lower": None,
            "simultaneous_upper": None,
            "critical_value": None,
            "status": "inconclusive",
            "reason": "fewer than two complete replicate vectors remain after alignment",
            "excluded": excluded,
            "per_cell_status": per_cell_status,
        }
    points_usable = np.array([points[position] for position in usable_positions], dtype=np.float64)
    standard_deviation = matrix.std(axis=1, ddof=0)
    zero_width = standard_deviation <= 0.0
    safe = np.where(zero_width, 1.0, standard_deviation)
    studentized = np.abs(matrix - points_usable[:, None]) / safe[:, None]
    studentized[zero_width] = 0.0
    maxima = studentized.max(axis=0)
    critical_value = float(np.percentile(maxima, 100.0 * float(coverage)))
    lower = points_usable - critical_value * standard_deviation
    upper = points_usable + critical_value * standard_deviation
    lower_full: list[float | None] = [None] * len(cells)
    upper_full: list[float | None] = [None] * len(cells)
    for offset, position in enumerate(usable_positions):
        lower_full[position] = float(lower[offset])
        upper_full[position] = float(upper[offset])
    degenerate = bool(zero_width.all())
    return {
        "kind": kind,
        "coverage": float(coverage),
        "n_cells": len(cells),
        "n_usable_cells": len(usable_positions),
        "horizons_seconds": horizons,
        "point": points,
        "simultaneous_lower": lower_full,
        "simultaneous_upper": upper_full,
        "critical_value": critical_value,
        "pointwise_intervals": [
            {
                "horizon_seconds": cells[position].get("horizon_seconds"),
                "lower": (cells[position].get(f"{kind}_ci") or {}).get("lower"),
                "upper": (cells[position].get(f"{kind}_ci") or {}).get("upper"),
                "status": per_cell_status[position],
            }
            for position in range(len(cells))
        ],
        "comparison": (
            "pointwise intervals are per-horizon percentile intervals; the simultaneous band is "
            "wider wherever the maximum studentized deviation exceeds the per-horizon percentile, "
            "which is the cost of covering the whole curve at once"
        ),
        "method": (
            "maximum absolute studentized deviation across horizons, seeded cluster-bootstrap "
            "replicates aligned by replicate index"
        ),
        "status": "degenerate" if degenerate else "ok",
        "reason": (
            "every usable cell has a zero-width replicate distribution, so the band collapses to "
            "the point estimates and carries no precision information"
            if degenerate
            else None
        ),
        "excluded": excluded,
        "per_cell_status": per_cell_status,
    }


def forecast_scores(
    actual: Any,
    predicted: Any,
    *,
    event: Any = None,
    tolerance: float = 1e-12,
) -> dict[str, Any]:
    """Probability-point forecast errors for a future-probability target.

    Primary metrics are MAE and MSE in probability points. Directional accuracy
    is secondary and is reported only over rows with both a real and a predicted
    non-zero change, with its own sample size. When ``event`` supplies release
    labels, event-level errors are returned so a caller can aggregate over the
    independent unit instead of over rows.
    """
    observed = _as_finite(actual, name="actual", context="forecast_scores")
    forecasted = _as_finite(predicted, name="predicted", context="forecast_scores")
    if observed.shape != forecasted.shape:
        raise ForecastEvaluationError(
            f"actual has shape {observed.shape} but predicted has shape {forecasted.shape}"
        )
    if observed.size == 0:
        raise ForecastEvaluationError("forecast_scores received no rows")
    error = forecasted - observed
    absolute = np.abs(error)
    resolved = (np.abs(observed) > tolerance) & (np.abs(forecasted) > tolerance)
    payload: dict[str, Any] = {
        "n": int(observed.size),
        "mae": float(absolute.mean()),
        "mse": float(np.mean(error**2)),
        "rmse": float(math.sqrt(float(np.mean(error**2)))),
        "bias": float(error.mean()),
        "median_absolute_error": float(np.median(absolute)),
        "max_absolute_error": float(absolute.max()),
        "directional_accuracy": (
            float(np.mean(np.sign(observed[resolved]) == np.sign(forecasted[resolved])))
            if resolved.any()
            else None
        ),
        "directional_n": int(resolved.sum()),
        "bad_direction_rate": (
            float(np.mean(np.sign(observed[resolved]) != np.sign(forecasted[resolved])))
            if resolved.any()
            else None
        ),
        "metric_convention": (
            "probability-point errors on a future probability change; MAE is the registered "
            "primary metric, directional accuracy is secondary"
        ),
    }
    if event is not None:
        labels = pd.Series(np.asarray(event, dtype=object))
        if len(labels) != observed.size:
            raise ForecastEvaluationError(
                f"event has {len(labels)} label(s) but there are {observed.size} row(s)"
            )
        frame = pd.DataFrame(
            {"event": labels.astype(str), "absolute_error": absolute, "error": error}
        )
        per_event = frame.groupby("event", sort=True)["absolute_error"].mean()
        payload["n_events"] = int(per_event.size)
        payload["event_mae"] = {str(key): float(value) for key, value in per_event.items()}
        payload["event_mean_mae"] = float(per_event.mean())
        payload["event_clustered_mae_std"] = (
            float(per_event.std(ddof=0)) if per_event.size > 1 else None
        )
    return payload


def resolution_scores(
    actual: Any,
    predicted: Any,
    *,
    known: Any = None,
    exceptional: Any = None,
    horizon: Any = None,
    regime: Any = None,
    cluster: Any = None,
    epsilon: float = PROBABILITY_EPSILON,
    n_bins: int = 5,
    coverage: float = 0.95,
) -> dict[str, Any]:
    """Brier score and log loss for terminal binary outcomes.

    This is a different task from a future-probability forecast and shares no
    accuracy number with :func:`forecast_scores`. ``known`` marks which labels
    were resolved at the training cutoff: unknown labels are excluded before
    scoring, because a label that settles after the cutoff cannot contribute its
    terminal outcome. ``exceptional`` marks non-binary resolutions, which binary
    scoring rejects rather than clipping into ``[0, 1]``.

    The boundary convention is explicit: probabilities are clipped into
    ``[epsilon, 1 - epsilon]`` for log loss and the clipped count is reported.
    Scores are aggregated to the cluster level first and then averaged over
    clusters, so repeated snapshots of one outcome do not create independent
    trials; ``n_effective_units`` names the real unit count.
    """
    observed = _as_finite(actual, name="actual", context="resolution_scores")
    forecasted = _as_finite(predicted, name="predicted", context="resolution_scores")
    if observed.shape != forecasted.shape:
        raise ForecastEvaluationError(
            f"actual has shape {observed.shape} but predicted has shape {forecasted.shape}"
        )
    if observed.size == 0:
        raise ForecastEvaluationError("resolution_scores received no rows")
    if not math.isfinite(float(epsilon)) or not 0.0 < float(epsilon) < 0.5:
        raise ForecastEvaluationError(f"epsilon={epsilon!r} must lie strictly inside (0, 0.5)")

    frame = pd.DataFrame(
        {
            "_actual": observed,
            "_predicted": forecasted,
            "_known": np.ones(observed.shape, dtype=bool)
            if known is None
            else np.asarray(known, dtype=bool),
            "_exceptional": (
                np.zeros(observed.shape, dtype=bool)
                if exceptional is None
                else np.asarray(exceptional, dtype=bool)
            ),
        }
    )
    if frame["_known"].shape[0] != observed.size or frame["_exceptional"].shape[0] != observed.size:
        raise ForecastEvaluationError("known and exceptional must match the input length")
    for name, values in (
        ("horizon", horizon),
        ("regime", regime),
        ("cluster", cluster),
    ):
        if values is not None:
            if len(values) != observed.size:
                raise ForecastEvaluationError(
                    f"{name} has {len(values)} entries but there are {observed.size} row(s)"
                )
            frame[f"_{name}"] = np.asarray(values, dtype=object)
    out_of_range = (frame["_actual"] < 0.0) | (frame["_actual"] > 1.0)
    if out_of_range.any():
        raise ScoreConventionError(
            f"{int(out_of_range.sum())} actual payout(s) lie outside [0, 1]; exceptional "
            "resolutions must be flagged and excluded rather than scored as binary"
        )
    exceptional_excluded = int((~frame["_known"] | frame["_exceptional"]).sum())
    frame = frame.loc[frame["_known"] & ~frame["_exceptional"]]
    non_binary = (frame["_actual"] > 0.0) & (frame["_actual"] < 1.0)
    if non_binary.any():
        offending = frame.loc[non_binary, "_actual"].unique()[:5].tolist()
        raise ScoreConventionError(
            f"{int(non_binary.sum())} payout(s) are neither 0 nor 1 (for example {offending}) "
            "and are not flagged exceptional; terminal binary scoring needs binary payouts, so "
            "flag them as exceptional or score them with a different rule"
        )
    if frame.empty:
        return {
            "brier": None,
            "log_loss": None,
            "n": 0,
            "n_effective_units": 0,
            "status": "inconclusive",
            "reason": (
                "no row has a known, non-exceptional binary label at this cutoff; no resolution "
                "score is reported"
            ),
            "boundary_convention": {
                "epsilon": float(epsilon),
                "log_loss_clipping": "[epsilon, 1 - epsilon]",
                "n_clipped": 0,
            },
            "excluded": {"unknown_or_exceptional": exceptional_excluded},
        }
    probabilities = frame["_predicted"].to_numpy(dtype=np.float64)
    outcomes = frame["_actual"].to_numpy(dtype=np.float64)
    below = int((probabilities < float(epsilon)).sum())
    above = int((probabilities > 1.0 - float(epsilon)).sum())
    clipped = np.clip(probabilities, float(epsilon), 1.0 - float(epsilon))
    frame = frame.assign(
        _brier=(clipped - outcomes) ** 2,
        _log_loss=-(outcomes * np.log(clipped) + (1.0 - outcomes) * np.log(1.0 - clipped)),
        _clipped_probability=clipped,
    )
    unit_column = (
        "_cluster"
        if "_cluster" in frame.columns
        else ("_regime" if "_regime" in frame.columns else None)
    )
    if unit_column is None:
        unit_brier = frame["_brier"]
        unit_log_loss = frame["_log_loss"]
        n_units = len(frame)
        unit_label = "row"
        unit_note = (
            "no cluster or regime labels were supplied, so each row is treated as one unit; this "
            "overstates precision when rows repeat one outcome and must not be reported as "
            "independent trials"
        )
    else:
        grouped = frame.groupby(unit_column, sort=True)
        unit_brier = grouped["_brier"].mean()
        unit_log_loss = grouped["_log_loss"].mean()
        n_units = len(unit_brier)
        unit_label = unit_column.lstrip("_")
        unit_note = (
            f"scores are averaged within each {unit_label} and then across {unit_label}s, because "
            "repeated snapshots of one outcome are not independent binary trials"
        )
    reliability: dict[str, Any] = {}
    if horizon is not None or regime is not None:
        key = "_horizon" if "_horizon" in frame.columns else "_regime"
        for group, part in frame.groupby(key, sort=True):
            reliability[str(group)] = {
                "n": len(part),
                "brier": float(np.mean(part["_brier"])),
                "log_loss": float(np.mean(part["_log_loss"])),
                "mean_predicted": float(part["_clipped_probability"].mean()),
                "mean_actual": float(part["_actual"].mean()),
            }
    bin_edges = np.linspace(0.0, 1.0, int(n_bins) + 1)
    binned = pd.cut(frame["_clipped_probability"], bins=bin_edges, include_lowest=True)
    bin_records: list[dict[str, Any]] = []
    for interval, part in frame.groupby(binned, observed=True, sort=True):
        bin_records.append(
            {
                "bin": str(interval),
                "n": len(part),
                "mean_predicted": float(part["_clipped_probability"].mean()),
                "mean_actual": float(part["_actual"].mean()),
                "brier": float(np.mean(part["_brier"])),
            }
        )
    return {
        "brier": float(unit_brier.mean()),
        "log_loss": float(unit_log_loss.mean()),
        "brier_across_rows": float(frame["_brier"].mean()),
        "log_loss_across_rows": float(frame["_log_loss"].mean()),
        "n": len(frame),
        "n_effective_units": n_units,
        "n_clusters": n_units,
        "unit_label": unit_label,
        "unit_note": unit_note,
        "reliability_by_bin": bin_records,
        "reliability_by_horizon_or_regime": reliability,
        "boundary_convention": {
            "epsilon": float(epsilon),
            "log_loss_clipping": f"[{float(epsilon)!r}, {1.0 - float(epsilon)!r}]",
            "n_clipped": int(below + above),
            "n_clipped_low": below,
            "n_clipped_high": above,
            "brier_uses_clipped_probability": True,
        },
        "excluded": {"unknown_or_exceptional": exceptional_excluded},
        "status": "ok" if n_units >= 2 else "inconclusive",
        "reason": (
            None
            if n_units >= 2
            else f"only {n_units} independent {unit_label} unit(s); the score is reported without "
            "interval or precision claims"
        ),
    }


# Between-event slope estimation, shared by models and falsifiers.


def weighted_event_slope(
    values: Any,
    shocks: Any,
    *,
    weights: Any = None,
    min_events: int = 2,
) -> dict[str, Any]:
    """Weighted between-release slope of event-level values on event shocks.

    This is the one construction behind every slope in the package: the local
    projections, the placebo suite, the sensitivity analysis, the power
    simulation and the residual calibration all call it, so an estimate and its
    bootstrap cannot drift apart. Inputs are already aggregated to one value per
    release, which is what makes the estimate event-level rather than row-level.

    The shock is standardized by its own cross-release standard deviation for
    conditioning, so ``slope`` is expressed in response units per standard
    deviation of the shock and its scaling contract is unchanged. ``raw_slope``
    and ``raw_intercept`` are the same fit back on the caller's shock scale, so a
    caller that must not change its units by horizon can read them directly
    instead of rescaling a standardized number itself. A constant,
    rank-deficient or non-finite design returns ``status`` ``'inconclusive'``
    with a reason and null slopes; it never returns a number that looks like
    certainty.
    """
    values_array = _as_finite(values, name="values", context="weighted_event_slope")
    shocks_array = _as_finite(shocks, name="shocks", context="weighted_event_slope")
    if values_array.shape != shocks_array.shape:
        raise ForecastEvaluationError(
            f"values has shape {values_array.shape} but shocks has shape {shocks_array.shape}"
        )
    n_events = int(values_array.size)

    def _conclusive(
        status: str,
        reason: str | None,
        *,
        scale: float | None,
        standardized: dict[str, float] | None,
        rank: int | None,
        solution: np.ndarray | None = None,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "reason": reason,
            "slope": None if solution is None else float(solution[1]),
            "raw_slope": (
                None
                if solution is None or scale is None or not math.isfinite(scale) or scale <= 0.0
                else float(solution[1]) / float(scale)
            ),
            "raw_intercept": (
                None
                if solution is None
                else float(solution[0]) - (float(solution[1]) / float(scale)) * shock_mean
            ),
            "intercept": None if solution is None else float(solution[0]),
            "shock_scale": scale,
            "standardized_shocks": standardized,
            "rank": rank,
            "n_events": n_events,
        }

    if n_events < int(min_events):
        return _conclusive(
            "inconclusive",
            f"only {n_events} release(s); a slope needs at least {int(min_events)}",
            scale=None,
            standardized=None,
            rank=None,
        )
    scale = float(shocks_array.std(ddof=0))
    shock_mean = float(shocks_array.mean())
    if not math.isfinite(scale) or scale <= 1e-12:
        return _conclusive(
            "inconclusive",
            "the shock is constant across releases, so its slope is not identified",
            scale=scale,
            standardized=None,
            rank=None,
        )
    standardized = (shocks_array - shocks_array.mean()) / scale
    named = {str(index): float(value) for index, value in enumerate(standardized)}
    if weights is None:
        weights_vector = np.ones(n_events, dtype=np.float64)
    else:
        weights_vector = _as_finite(weights, name="weights", context="weighted_event_slope")
        if weights_vector.shape != values_array.shape:
            raise ForecastEvaluationError(
                f"weights has shape {weights_vector.shape} but values has shape "
                f"{values_array.shape}"
            )
        if (weights_vector <= 0.0).any():
            return _conclusive(
                "inconclusive",
                "event weights are not all positive, so the weighted design is not usable",
                scale=scale,
                standardized=named,
                rank=None,
            )
    design = np.column_stack([np.ones(n_events), standardized])
    normal = design.T @ (design * weights_vector[:, None])
    rank = int(np.linalg.matrix_rank(normal))
    if rank < 2:
        return _conclusive(
            "inconclusive",
            "the between-release shock design is rank deficient",
            scale=scale,
            standardized=named,
            rank=rank,
        )
    solution = np.linalg.solve(normal, design.T @ (values_array * weights_vector))
    if not np.isfinite(solution).all():
        return _conclusive(
            "inconclusive",
            "the weighted normal equations produced a non-finite solution",
            scale=scale,
            standardized=named,
            rank=rank,
        )
    return _conclusive("ok", None, scale=scale, standardized=named, rank=rank, solution=solution)


def event_level_slope(
    frame: pd.DataFrame,
    *,
    response_column: str = "response",
    shock_column: str = "shock",
    event_column: str = "event_id",
    cluster_column: str = "cluster_id",
    weight_column: str | None = None,
) -> dict[str, Any]:
    """Between-release slope of a response on a release-constant shock.

    Rows are aggregated to one value per release first, so the estimate is an
    event-level one and a release contributes exactly one observation. The shock
    is constant within a release, so only between-release variation can identify
    its slope; a release fixed effect would absorb it and is therefore not used.
    A constant or rank-deficient shock design returns ``status='inconclusive'``
    with a stated reason instead of a number.

    Returns the point estimate, rank information, and the event-level vectors the
    cluster bootstrap reuses, so the estimator and its uncertainty share one
    construction. ``slope`` is per standard deviation of the shock while
    ``raw_slope`` and ``raw_intercept`` are the same fit on the caller's shock
    scale, so a caller needing one fixed unit can read those directly instead of
    rescaling a standardized number itself.
    """
    _require_columns(
        frame, (response_column, shock_column, event_column), context="event_level_slope"
    )
    if frame.empty:
        raise ForecastEvaluationError("event_level_slope received an empty frame")
    responses = pd.to_numeric(frame[response_column], errors="coerce")
    shocks = pd.to_numeric(frame[shock_column], errors="coerce")
    usable = responses.notna() & shocks.notna()
    responses = responses.loc[usable]
    shocks = shocks.loc[usable]
    events = frame.loc[usable, event_column].astype(str)
    clusters = (
        frame.loc[usable, cluster_column].astype(str) if cluster_column in frame.columns else events
    )
    if weight_column is not None:
        _require_columns(frame, (weight_column,), context="event_level_slope")
        weights = pd.to_numeric(frame.loc[usable, weight_column], errors="coerce").fillna(1.0)
    else:
        weights = pd.Series(1.0, index=responses.index)
    grouped = pd.DataFrame(
        {
            "event": events.to_numpy(),
            "cluster": clusters.to_numpy(),
            "response": responses.to_numpy(),
            "shock": shocks.to_numpy(),
            "weight": weights.to_numpy(dtype=np.float64),
        }
    )
    event_frame = grouped.groupby("event", sort=True).agg(
        response=("response", "mean"),
        shock=("shock", "mean"),
        weight=("weight", "sum"),
        cluster=("cluster", "first"),
        n_rows=("response", "size"),
    )
    n_events = len(event_frame)
    n_clusters = int(event_frame["cluster"].nunique())
    event_values = event_frame["response"]
    event_shocks = event_frame["shock"]
    payload: dict[str, Any] = {
        "n_events": n_events,
        "n_clusters": n_clusters,
        "event_values": {str(key): float(value) for key, value in event_values.items()},
        "event_shocks": {str(key): float(value) for key, value in event_shocks.items()},
        "event_clusters": {str(key): str(value) for key, value in event_frame["cluster"].items()},
        "event_row_counts": {str(key): int(value) for key, value in event_frame["n_rows"].items()},
        "event_weights": {str(key): float(value) for key, value in event_frame["weight"].items()},
        "response_column": response_column,
        "shock_column": shock_column,
        "slope": None,
        "raw_slope": None,
        "raw_intercept": None,
        "intercept": None,
        "shock_scale": None,
        "standardized_shocks": None,
        "rank": None,
        "identification": (
            "between-release variation only: the shock is release-constant, so release fixed "
            "effects would absorb the parameter of interest"
        ),
    }
    weights_vector = event_frame["weight"].to_numpy(dtype=np.float64)
    estimate = weighted_event_slope(
        event_values.to_numpy(dtype=np.float64),
        event_shocks.to_numpy(dtype=np.float64),
        weights=weights_vector,
    )
    scale = estimate.get("shock_scale")
    if estimate["status"] == "ok":
        order = [str(label) for label in event_values.index]
        payload["slope"] = estimate["slope"]
        payload["raw_slope"] = estimate["raw_slope"]
        payload["raw_intercept"] = estimate["raw_intercept"]
        payload["intercept"] = estimate["intercept"]
        payload["shock_scale"] = scale
        payload["standardized_shocks"] = dict(
            zip(order, (estimate["standardized_shocks"] or {}).values(), strict=True)
        )
        payload["rank"] = estimate["rank"]
        payload["status"] = "ok"
        payload["reason"] = None
    else:
        payload["shock_scale"] = scale
        payload["rank"] = estimate.get("rank")
        payload["status"] = "inconclusive"
        payload["reason"] = estimate["reason"]
    return payload


def _slope_test(
    frame: pd.DataFrame,
    *,
    response_column: str,
    shock_column: str,
    event_column: str,
    cluster_column: str,
    seed: int,
    samples: int,
    coverage: float,
) -> dict[str, Any]:
    """Point slope plus cluster-bootstrap interval for one placebo variant."""
    estimate = event_level_slope(
        frame,
        response_column=response_column,
        shock_column=shock_column,
        event_column=event_column,
        cluster_column=cluster_column,
    )
    if estimate["status"] != "ok":
        return {
            "status": "inconclusive",
            "reason": estimate["reason"],
            "slope": None,
            "interval": None,
            "n_events": estimate["n_events"],
        }
    values = pd.Series(estimate["event_values"], dtype=np.float64)
    shocks = pd.Series(estimate["event_shocks"], dtype=np.float64).loc[values.index]
    clusters = pd.Series(estimate["event_clusters"], dtype=object).loc[values.index]
    index = [str(label) for label in values.index]
    values.index = index
    shocks.index = index
    clusters.index = index

    def statistic(sample: np.ndarray) -> dict[str, float]:
        subset_values = values.loc[sample]
        subset_shocks = shocks.loc[sample]
        estimate = weighted_event_slope(
            subset_values.to_numpy(dtype=np.float64),
            subset_shocks.to_numpy(dtype=np.float64),
        )
        return {"slope": (float(estimate["slope"]) if estimate["status"] == "ok" else math.nan)}

    bootstrap = clustered_bootstrap(
        {"slope": float(estimate["slope"])},
        statistic,
        clusters,
        seed=seed,
        samples=samples,
        coverage=coverage,
    )
    interval = bootstrap["samples"]["slope"]
    return {
        "status": "ok",
        "reason": None,
        "slope": float(estimate["slope"]),
        "n_events": estimate["n_events"],
        "n_clusters": estimate["n_clusters"],
        "interval": {"lower": interval["lower"], "upper": interval["upper"]},
        "interval_status": interval["status"],
        "event_values": estimate["event_values"],
        "event_shocks": estimate["event_shocks"],
    }


def _time_of_day_regime(times: pd.Series, *, bucket_minutes: int = 60) -> pd.Series:
    minutes = times.dt.hour * 60 + times.dt.minute
    return (minutes // int(bucket_minutes)).astype("int64")


def label_permutation_placebo(
    frame: pd.DataFrame,
    *,
    response_column: str = "response",
    shock_column: str = "shock",
    event_column: str = "event_id",
    cluster_column: str = "cluster_id",
    regime_column: str | None = None,
    n_permutations: int = DEFAULT_PERMUTATIONS,
    seed: int = 20260913,
    time_column: str = "event_time",
) -> dict[str, Any]:
    """Permutation null that respects the release regime.

    Responses are permuted across releases *within* the same regime (family or
    an explicit regime column), never across the whole series, because a blind
    global shuffle of a nonstationary series is not an exchangeable null. The
    permutation distribution of the slope gives the null quantiles and a
    two-sided p-value for the observed slope.

    The remaining assumption is stated in the result: within-regime
    exchangeability of release responses. It is defensible when the regimes are
    the release families and the horizon is fixed, and it is not valid for a
    trending series or when regimes change over time.
    """
    if n_permutations < 5:
        raise ForecastEvaluationError(
            f"n_permutations={n_permutations!r} is too few to characterise a null distribution"
        )
    if regime_column is None:
        regime_column = "family" if "family" in frame.columns else None
    if regime_column is None:
        return {
            "status": "unavailable",
            "reason": (
                "no regime column was supplied and the frame carries no 'family' column; a "
                "global permutation of releases would not be an exchangeable null"
            ),
            "n_permutations": int(n_permutations),
        }
    if regime_column not in frame.columns:
        return {
            "status": "unavailable",
            "reason": f"regime_column {regime_column!r} is not in the frame",
            "n_permutations": int(n_permutations),
        }
    observed = event_level_slope(
        frame,
        response_column=response_column,
        shock_column=shock_column,
        event_column=event_column,
        cluster_column=cluster_column,
    )
    if observed["status"] != "ok":
        return {
            "status": "inconclusive",
            "reason": observed["reason"],
            "n_permutations": int(n_permutations),
        }
    event_values = pd.Series(observed["event_values"], dtype=np.float64)
    event_regimes = (
        frame.assign(_event=frame[event_column].astype(str))
        .groupby("_event", sort=True)[regime_column]
        .first()
        .astype(str)
        .loc[event_values.index]
    )
    rng = np.random.default_rng(_stable_seed("label-permutation", seed, sorted(event_values.index)))
    null_slopes: list[float] = []
    for _ in range(int(n_permutations)):
        permuted = event_values.copy()
        for regime in sorted(event_regimes.unique()):
            positions = np.flatnonzero(event_regimes.to_numpy() == regime)
            if positions.size < 2:
                continue
            permuted.iloc[positions] = rng.permutation(event_values.to_numpy()[positions])
        variant = frame.copy()
        mapping = permuted.to_dict()
        variant["_permuted_response"] = (
            variant[event_column]
            .astype(str)
            .map(mapping)
            .fillna(pd.to_numeric(variant[response_column], errors="coerce"))
        )
        estimate = event_level_slope(
            variant,
            response_column="_permuted_response",
            shock_column=shock_column,
            event_column=event_column,
            cluster_column=cluster_column,
        )
        if estimate["status"] == "ok":
            null_slopes.append(float(estimate["slope"]))
    if not null_slopes:
        return {
            "status": "inconclusive",
            "reason": "no permutation produced an identifiable slope",
            "n_permutations": int(n_permutations),
        }
    null = np.asarray(null_slopes, dtype=np.float64)
    observed_slope = float(observed["slope"])
    exceed = int(np.sum(np.abs(null) >= abs(observed_slope)))
    p_value = (exceed + 1) / (null.size + 1)
    return {
        "status": "ok",
        "observed_slope": observed_slope,
        "n_permutations": int(null.size),
        "n_permutations_requested": int(n_permutations),
        "null_mean": float(null.mean()),
        "null_std": float(null.std(ddof=0)),
        "null_quantiles": {
            "p2.5": float(np.percentile(null, 2.5)),
            "p50": float(np.percentile(null, 50.0)),
            "p97.5": float(np.percentile(null, 97.5)),
        },
        "p_value_two_sided": float(p_value),
        "regime_column": regime_column,
        "exchangeability": (
            "responses are permuted across releases within one regime only; within-regime "
            "exchangeability is assumed and must be defended, since a global shuffle of a "
            "nonstationary series is not an exchangeable null"
        ),
    }


def placebo_tests(
    frame: pd.DataFrame,
    *,
    response_column: str = "response",
    shock_column: str = "shock",
    event_column: str = "event_id",
    cluster_column: str = "cluster_id",
    time_column: str = "event_time",
    horizon_column: str = "horizon_seconds",
    age_column: str | None = None,
    cohort_column: str | None = "cohort",
    pre_event_column: str | None = None,
    control_cohort: str = "control",
    seed: int = 20260913,
    samples: int = DEFAULT_PERMUTATIONS,
    coverage: float = 0.95,
    shift_days: int = 0,
    regime_column: str | None = None,
    n_permutations: int = DEFAULT_PERMUTATIONS,
) -> dict[str, Any]:
    """Falsification suite for a release-response panel.

    Every placebo reports ``status='unavailable'`` with the exact missing input
    when its prerequisite is absent, rather than inventing a substitute:

    ``pre_release_lead``
        Needs a pre-event change column. A pre-release association with the
        shock is evidence against news timing, not for transmission.
    ``shifted_release_time``
        Pastes the previous release's shock in the same regime onto the current
        release's response, which preserves calendar structure and time of day.
    ``reversed_direction``
        Negates the shock and checks that the estimate is its exact negative,
        which falsifies a sign or orientation bug rather than the mechanism.
    ``quote_age_stratification``
        Estimates the slope within quote-age strata.
    ``endpoint_sensitivity``
        Re-estimates the slope at each horizon and flags a sign reversal.
    ``negative_control_contract``
        Estimates the same slope on the control cohort, where no release
        response is expected.
    ``label_permutation``
        Within-regime permutation null, never a global shuffle.
    """
    if not isinstance(frame, pd.DataFrame):
        raise ForecastEvaluationError(
            f"placebo_tests expects a pandas DataFrame, got {type(frame).__name__}"
        )
    if frame.empty:
        raise ForecastEvaluationError("placebo_tests received an empty frame")
    _require_columns(frame, (response_column, shock_column, event_column), context="placebo_tests")
    results: dict[str, Any] = {}
    unavailable: list[dict[str, str]] = []

    results["pre_release_lead"] = (
        _slope_test(
            frame,
            response_column=pre_event_column,
            shock_column=shock_column,
            event_column=event_column,
            cluster_column=cluster_column,
            seed=seed,
            samples=samples,
            coverage=coverage,
        )
        | {"interpretation": "pre-event change regressed on the release shock"}
        if pre_event_column is not None and pre_event_column in frame.columns
        else {
            "status": "unavailable",
            "reason": (
                "no pre-event change column was supplied; a pre-release lead cannot be computed "
                "from post-event responses alone"
            ),
        }
    )
    if results["pre_release_lead"]["status"] == "unavailable":
        unavailable.append(
            {"placebo": "pre_release_lead", "reason": results["pre_release_lead"]["reason"]}
        )

    times = pd.to_datetime(frame[time_column], utc=True, errors="coerce")
    regime = (
        frame[regime_column].astype(str)
        if regime_column is not None and regime_column in frame.columns
        else frame["family"].astype(str)
        if "family" in frame.columns
        else pd.Series("all", index=frame.index)
    )
    shifted = frame.copy()
    order = pd.DataFrame(
        {
            "event": shifted[event_column].astype(str),
            "regime": regime.to_numpy(),
            "time": times.to_numpy(),
            "shock": pd.to_numeric(shifted[shock_column], errors="coerce").to_numpy(),
        }
    ).sort_values(["regime", "time"], kind="stable")
    shift = int(shift_days) if shift_days else 1
    order["shifted_shock"] = order.groupby("regime", sort=False)["shock"].shift(shift)
    mapping = order.dropna(subset=["shifted_shock"]).set_index("event")["shifted_shock"].to_dict()
    shifted["_shifted_shock"] = shifted[event_column].astype(str).map(mapping)
    results["shifted_release_time"] = _slope_test(
        shifted,
        response_column=response_column,
        shock_column="_shifted_shock",
        event_column=event_column,
        cluster_column=cluster_column,
        seed=seed,
        samples=samples,
        coverage=coverage,
    ) | {
        "interpretation": (
            "previous release's shock in the same regime paired with the current release's "
            "response, preserving calendar structure and time of day"
        ),
        "shift_days": int(shift_days),
    }

    base = _slope_test(
        frame,
        response_column=response_column,
        shock_column=shock_column,
        event_column=event_column,
        cluster_column=cluster_column,
        seed=seed,
        samples=samples,
        coverage=coverage,
    )
    reversed_frame = frame.assign(
        _negated_shock=-pd.to_numeric(frame[shock_column], errors="coerce")
    )
    reversed_result = _slope_test(
        reversed_frame,
        response_column=response_column,
        shock_column="_negated_shock",
        event_column=event_column,
        cluster_column=cluster_column,
        seed=seed,
        samples=samples,
        coverage=coverage,
    )
    if base["status"] == "ok" and reversed_result["status"] == "ok":
        results["reversed_direction"] = {
            "status": "ok",
            "slope": base["slope"],
            "slope_reversed": reversed_result["slope"],
            "matches_negation": bool(
                math.isclose(base["slope"], -reversed_result["slope"], rel_tol=1e-9, abs_tol=1e-12)
            ),
            "interpretation": (
                "an exact sign flip confirms orientation handling; a mismatch points at a sign or "
                "pooling bug rather than at the mechanism"
            ),
        }
    else:
        results["reversed_direction"] = {
            "status": "inconclusive",
            "reason": base.get("reason") or reversed_result.get("reason"),
        }

    if age_column is not None and age_column in frame.columns:
        ages = pd.to_numeric(frame[age_column], errors="coerce")
        strata = pd.qcut(ages, q=min(4, max(2, ages.nunique())), duplicates="drop")
        by_stratum: dict[str, Any] = {}
        for label, part in frame.groupby(strata, observed=True, sort=True):
            estimate = _slope_test(
                part,
                response_column=response_column,
                shock_column=shock_column,
                event_column=event_column,
                cluster_column=cluster_column,
                seed=seed,
                samples=samples,
                coverage=coverage,
            )
            by_stratum[str(label)] = {
                "status": estimate["status"],
                "slope": estimate.get("slope"),
                "interval": estimate.get("interval"),
                "n_events": estimate.get("n_events"),
                "reason": estimate.get("reason"),
            }
        slopes = [entry["slope"] for entry in by_stratum.values() if entry["slope"] is not None]
        results["quote_age_stratification"] = {
            "status": "ok" if slopes else "inconclusive",
            "age_column": age_column,
            "strata": by_stratum,
            "slope_range": [min(slopes), max(slopes)] if slopes else None,
            "slope_sign_flips": bool(slopes and min(slopes) < 0.0 < max(slopes)),
            "reason": None if slopes else "no strata produced an identifiable slope",
        }
    else:
        results["quote_age_stratification"] = {
            "status": "unavailable",
            "reason": (
                f"no quote-age column was supplied or the frame lacks {age_column!r}; lead-lag "
                "results must be reported with quote age rather than as a single number"
            ),
        }
        unavailable.append(
            {
                "placebo": "quote_age_stratification",
                "reason": results["quote_age_stratification"]["reason"],
            }
        )

    if horizon_column in frame.columns and frame[horizon_column].notna().any():
        by_horizon: dict[str, Any] = {}
        for horizon, part in frame.groupby(horizon_column, sort=True):
            estimate = _slope_test(
                part,
                response_column=response_column,
                shock_column=shock_column,
                event_column=event_column,
                cluster_column=cluster_column,
                seed=seed,
                samples=samples,
                coverage=coverage,
            )
            by_horizon[str(horizon)] = {
                "status": estimate["status"],
                "slope": estimate.get("slope"),
                "interval": estimate.get("interval"),
                "n_events": estimate.get("n_events"),
                "reason": estimate.get("reason"),
            }
        slopes = [entry["slope"] for entry in by_horizon.values() if entry["slope"] is not None]
        results["endpoint_sensitivity"] = {
            "status": "ok" if slopes else "inconclusive",
            "horizon_column": horizon_column,
            "by_horizon": by_horizon,
            "sign_reversal_across_endpoints": bool(slopes and min(slopes) < 0.0 < max(slopes)),
            "reason": None if slopes else "no horizon produced an identifiable slope",
        }
        unavailable_all = results["endpoint_sensitivity"]["status"] != "ok"
        if unavailable_all:
            unavailable.append(
                {
                    "placebo": "endpoint_sensitivity",
                    "reason": results["endpoint_sensitivity"]["reason"],
                }
            )
    else:
        results["endpoint_sensitivity"] = {
            "status": "unavailable",
            "reason": f"the frame has no usable {horizon_column!r} column",
        }
        unavailable.append(
            {"placebo": "endpoint_sensitivity", "reason": results["endpoint_sensitivity"]["reason"]}
        )

    if cohort_column is not None and cohort_column in frame.columns:
        control = frame.loc[frame[cohort_column].astype(str) == str(control_cohort)]
        if control.empty:
            results["negative_control_contract"] = {
                "status": "unavailable",
                "reason": f"the frame has no row in cohort {control_cohort!r}",
            }
            unavailable.append(
                {
                    "placebo": "negative_control_contract",
                    "reason": results["negative_control_contract"]["reason"],
                }
            )
        else:
            results["negative_control_contract"] = _slope_test(
                control,
                response_column=response_column,
                shock_column=shock_column,
                event_column=event_column,
                cluster_column=cluster_column,
                seed=seed,
                samples=samples,
                coverage=coverage,
            ) | {
                "cohort": str(control_cohort),
                "interpretation": (
                    "a control cohort with no release linkage should show no shock response"
                ),
            }
    else:
        results["negative_control_contract"] = {
            "status": "unavailable",
            "reason": "the frame carries no cohort column",
        }
        unavailable.append(
            {
                "placebo": "negative_control_contract",
                "reason": results["negative_control_contract"]["reason"],
            }
        )

    results["label_permutation"] = label_permutation_placebo(
        frame,
        response_column=response_column,
        shock_column=shock_column,
        event_column=event_column,
        cluster_column=cluster_column,
        regime_column=regime_column,
        n_permutations=n_permutations,
        seed=seed,
        time_column=time_column,
    )
    if results["label_permutation"]["status"] == "unavailable":
        unavailable.append(
            {"placebo": "label_permutation", "reason": results["label_permutation"]["reason"]}
        )
    return {
        "seed": int(seed),
        "samples": int(samples),
        "coverage": float(coverage),
        "baseline_slope": base,
        "placebos": results,
        "unavailable": unavailable,
        "status": "ok" if base["status"] == "ok" else "inconclusive",
        "exchangeability_note": (
            "placebo exchangeability must be defended for each falsifier; a global shuffle of a "
            "nonstationary time series is not a valid null and is not used here"
        ),
    }


def sensitivity_analysis(
    frame: pd.DataFrame,
    *,
    shock_column: str = "shock",
    response_column: str = "response",
    event_column: str = "event_id",
    cluster_column: str = "cluster_id",
    horizon_column: str = "horizon_seconds",
    time_column: str = "event_time",
    age_column: str | None = None,
    seed: int = 20260913,
    samples: int = DEFAULT_PERMUTATIONS,
    coverage: float = 0.95,
) -> dict[str, Any]:
    """Pre-specified sensitivity set: endpoints, quote age, time of day, and LOO.

    Leave-one-release-out values are reported for the primary slope, so a single
    influential release is visible instead of averaged away. Each axis is a
    comparison of the same estimand under a different cut, with the number of
    releases named so small-cut precision is not overstated.
    """
    _require_columns(
        frame,
        (response_column, shock_column, event_column, horizon_column),
        context="sensitivity_analysis",
    )
    base = _slope_test(
        frame,
        response_column=response_column,
        shock_column=shock_column,
        event_column=event_column,
        cluster_column=cluster_column,
        seed=seed,
        samples=samples,
        coverage=coverage,
    )
    payload: dict[str, Any] = {
        "primary": base,
        "endpoints": {},
        "quote_age": {},
        "time_of_day": {},
        "leave_one_event_out": {},
        "status": "ok" if base["status"] == "ok" else "inconclusive",
    }
    if base["status"] != "ok":
        payload["reason"] = base.get("reason")
        return payload
    event_values = pd.Series(base["event_values"], dtype=np.float64)
    shocks = pd.Series(base["event_shocks"], dtype=np.float64).loc[event_values.index]
    loo: dict[str, Any] = {}
    for event in event_values.index:
        remaining = [label for label in event_values.index if label != event]
        if len(remaining) < 2:
            continue
        estimate = weighted_event_slope(
            event_values.loc[remaining].to_numpy(dtype=np.float64),
            shocks.loc[remaining].to_numpy(dtype=np.float64),
        )
        if estimate["status"] == "ok":
            loo[str(event)] = float(estimate["slope"])
    payload["leave_one_event_out"] = {
        "slope_without_event": loo,
        "max_abs_deviation": (
            max(abs(value - float(base["slope"])) for value in loo.values()) if loo else None
        ),
        "sign_flips": bool(
            loo and any(np.sign(value) != np.sign(float(base["slope"])) for value in loo.values())
        ),
        "n_events": len(event_values),
    }
    for horizon, part in frame.groupby(horizon_column, sort=True):
        estimate = _slope_test(
            part,
            response_column=response_column,
            shock_column=shock_column,
            event_column=event_column,
            cluster_column=cluster_column,
            seed=seed,
            samples=samples,
            coverage=coverage,
        )
        payload["endpoints"][str(horizon)] = {
            "status": estimate["status"],
            "slope": estimate.get("slope"),
            "interval": estimate.get("interval"),
            "n_events": estimate.get("n_events"),
            "reason": estimate.get("reason"),
        }
    if age_column is not None and age_column in frame.columns:
        ages = pd.to_numeric(frame[age_column], errors="coerce")
        strata = pd.qcut(ages, q=min(4, max(2, ages.nunique())), duplicates="drop")
        for label, part in frame.groupby(strata, observed=True, sort=True):
            estimate = _slope_test(
                part,
                response_column=response_column,
                shock_column=shock_column,
                event_column=event_column,
                cluster_column=cluster_column,
                seed=seed,
                samples=samples,
                coverage=coverage,
            )
            payload["quote_age"][str(label)] = {
                "status": estimate["status"],
                "slope": estimate.get("slope"),
                "interval": estimate.get("interval"),
                "n_events": estimate.get("n_events"),
                "reason": estimate.get("reason"),
            }
    else:
        payload["quote_age"] = {
            "status": "unavailable",
            "reason": (
                f"no quote-age column was supplied or the frame lacks {age_column!r}; lead-lag "
                "sensitivity to quote refresh cannot be assessed"
            ),
        }
    if time_column in frame.columns:
        times = pd.to_datetime(frame[time_column], utc=True, errors="coerce")
    else:
        times = pd.Series(pd.NaT, index=frame.index)
    if times.notna().any():
        regime = _time_of_day_regime(times)
        for label, part in frame.groupby(regime, sort=True):
            estimate = _slope_test(
                part,
                response_column=response_column,
                shock_column=shock_column,
                event_column=event_column,
                cluster_column=cluster_column,
                seed=seed,
                samples=samples,
                coverage=coverage,
            )
            payload["time_of_day"][str(label)] = {
                "status": estimate["status"],
                "slope": estimate.get("slope"),
                "interval": estimate.get("interval"),
                "n_events": estimate.get("n_events"),
                "reason": estimate.get("reason"),
            }
    else:
        payload["time_of_day"] = {
            "status": "unavailable",
            "reason": f"the frame has no usable {time_column!r} column",
        }
    return payload


def _simulate_events(
    rng: np.random.Generator,
    *,
    n_events: int,
    true_slope: float,
    residual_sigma: float,
    shock_sigma: float = 1.0,
    cluster_size: int = 2,
) -> pd.DataFrame:
    shocks = rng.standard_normal(n_events) * float(shock_sigma)
    responses = float(true_slope) * shocks + rng.standard_normal(n_events) * float(residual_sigma)
    return pd.DataFrame(
        {
            "event_id": [f"sim-{index:05d}" for index in range(n_events)],
            "cluster_id": [
                f"cluster-{index // max(1, int(cluster_size)):05d}" for index in range(n_events)
            ],
            "response": responses,
            "shock": shocks,
        }
    )


def power_assessment(
    *,
    n_events: int = 24,
    true_slope: float = 0.0,
    relevant_slope: float = 0.02,
    residual_sigma: float = 0.03,
    repetitions: int = DEFAULT_REPETITIONS,
    null_repetitions: int | None = None,
    shock_sigma: float = 1.0,
    cluster_size: int = 2,
    alpha: float = DEFAULT_ALPHA,
    coverage: float = 0.95,
    seed: int = 20260913,
    samples: int = 100,
    sample_grid: Sequence[int] | None = None,
    target_power: float = 0.8,
    scenario: str | None = None,
    calibration_n_events: int = 120,
) -> dict[str, Any]:
    """Cluster-aware power and false-positive rate at an available event count.

    The independent unit is the release, so the simulation draws event-level
    shocks and responses, groups them into clusters of ``cluster_size`` releases,
    and applies the same estimator and cluster bootstrap used on real data. Two
    rates are always reported: ``false_positive_rate`` from repetitions with a
    zero true slope, and ``power`` from repetitions at the smallest
    scientifically relevant slope. A derived sample requirement comes from
    repeating the same exercise across ``sample_grid``.

    ``scenario`` calibrates ``residual_sigma`` from a simulated adversarial
    scenario instead of the supplied constant, which keeps the nuisance-range
    claim tied to a stated process. A quantile-based interval that cannot exclude
    zero under either hypothesis is reported as inconclusive, not as a
    nonsignificant result.
    """
    if int(repetitions) < 20:
        raise ForecastEvaluationError(
            f"repetitions={repetitions!r} is too few for a stable rate estimate"
        )
    if int(n_events) < 3:
        raise ForecastEvaluationError(f"n_events={n_events!r} is too small for a slope estimate")
    if not 0.0 < float(alpha) < 1.0:
        raise ForecastEvaluationError(f"alpha={alpha!r} must lie strictly inside (0, 1)")
    calibration: dict[str, Any] | None = None
    sigma = float(residual_sigma)
    if scenario is not None:
        calibration = simulation_calibration(
            scenario, seed=seed, n_events=calibration_n_events, horizon_seconds=None
        )
        if calibration["status"] == "ok":
            sigma = float(calibration["event_residual_sigma"])
        else:
            raise ForecastEvaluationError(
                f"scenario {scenario!r} could not calibrate a residual scale: "
                f"{calibration['reason']}"
            )
    if sigma <= 0.0:
        raise ForecastEvaluationError(
            f"residual_sigma={sigma!r} must be positive; a zero residual makes every interval "
            "degenerate and every rate an artifact"
        )

    def rate(true_value: float, count: int, *, events: int, seed_offset: int) -> dict[str, Any]:
        rng = np.random.default_rng(_stable_seed("power", seed, seed_offset, true_value, count))
        excluded_zero = 0
        excluded_relevant = 0
        usable = 0
        widths: list[float] = []
        for _ in range(int(count)):
            frame = _simulate_events(
                rng,
                n_events=int(events),
                true_slope=true_value,
                residual_sigma=sigma,
                shock_sigma=shock_sigma,
                cluster_size=cluster_size,
            )
            result = _slope_test(
                frame,
                response_column="response",
                shock_column="shock",
                event_column="event_id",
                cluster_column="cluster_id",
                seed=int(rng.integers(0, 2**31 - 1)),
                samples=samples,
                coverage=coverage,
            )
            if (
                result["status"] != "ok"
                or not result["interval"]
                or result["interval"]["lower"] is None
            ):
                continue
            usable += 1
            lower = float(result["interval"]["lower"])
            upper = float(result["interval"]["upper"])
            widths.append(upper - lower)
            if lower > 0.0 or upper < 0.0:
                excluded_zero += 1
            if lower > float(relevant_slope) or upper < -abs(float(relevant_slope)):
                excluded_relevant += 1
        if usable == 0:
            return {
                "status": "inconclusive",
                "reason": "no repetition produced an identified interval",
                "repetitions": int(count),
                "usable": 0,
                "true_slope": float(true_value),
            }
        return {
            "status": "ok",
            "true_slope": float(true_value),
            "repetitions": int(count),
            "usable": int(usable),
            "excludes_zero_rate": excluded_zero / usable,
            "excludes_relevant_rate": excluded_relevant / usable,
            "median_interval_width": float(np.median(widths)),
            "mean_interval_width": float(np.mean(widths)),
        }

    null_rate = rate(0.0, int(null_repetitions or repetitions), events=n_events, seed_offset=1)
    true_rate = rate(float(true_slope), int(repetitions), events=n_events, seed_offset=2)
    power_rate = rate(float(relevant_slope), int(repetitions), events=n_events, seed_offset=3)

    grid = list(sample_grid) if sample_grid is not None else [12, 24, 36, 60, 120]
    requirement: dict[str, Any] = {
        "target_power": float(target_power),
        "grid": grid,
        "by_event_count": {},
    }
    for count in grid:
        if int(count) < 3:
            continue
        grid_rate = rate(
            float(relevant_slope),
            max(40, int(repetitions) // 4),
            events=int(count),
            seed_offset=4 + int(count),
        )
        requirement["by_event_count"][str(int(count))] = grid_rate
    sufficient = [
        int(count)
        for count, payload in requirement["by_event_count"].items()
        if payload.get("status") == "ok"
        and payload.get("excludes_zero_rate", 0.0) >= float(target_power)
    ]
    requirement["events_for_target_power"] = min(sufficient) if sufficient else None
    requirement["note"] = (
        "the requirement is derived from this simulation at the stated residual scale, not from a "
        "universal event-count rule"
    )
    payload = {
        # This is a between-release response-slope power report, not the
        # news-versus-network forecast comparison. It states its own identity so
        # a forecast promotion gate can reject it as an unrelated statistic
        # instead of reading its rate as a null audit of the forecast pipeline.
        "estimator": "between_release_response_slope",
        "metric": "response_slope",
        "scope": "slope_estimation_not_forecast_comparison",
        "n_events": int(n_events),
        "repetitions": int(repetitions),
        "alpha": float(alpha),
        "coverage": float(coverage),
        "seed": int(seed),
        "samples": int(samples),
        "cluster_size": int(cluster_size),
        "true_slope": float(true_slope),
        "relevant_slope": float(relevant_slope),
        "residual_sigma": sigma,
        "residual_sigma_source": "scenario calibration" if scenario else "supplied constant",
        "scenario": scenario,
        "calibration": calibration,
        "false_positive_rate": null_rate.get("excludes_zero_rate"),
        "null": null_rate,
        "power": power_rate.get("excludes_zero_rate"),
        "power_at_relevant_slope": power_rate.get("excludes_zero_rate"),
        "relevant": power_rate,
        "power_at_true_slope": true_rate.get("excludes_zero_rate"),
        "true_effect": true_rate,
        "sample_requirement": requirement,
        "interpretation": (
            "rates come from repeated event-level simulations that reuse this module's "
            "between-release slope estimator and cluster bootstrap; they bound the study's "
            "resolution at the available event count and do not establish an economic effect"
        ),
        "unrelated_to_forecast_gate": (
            "thresholds and rates here are in slope units for a different estimand; the network "
            "forecast promotion gate requires a null assessment from "
            "market_propagation.falsification.network_falsification, which uses the nested "
            "news-versus-network MAE comparison"
        ),
    }
    if payload["power"] is None or payload["false_positive_rate"] is None:
        payload["status"] = "inconclusive"
        payload["reason"] = (
            "at least one of the null or relevant-effect rates could not be estimated"
        )
    else:
        payload["status"] = "ok"
        payload["reason"] = None
    return payload


def null_false_positive_rate(
    *,
    scenario: str = "shared_news_delay",
    n_events: int = 24,
    repetitions: int = DEFAULT_REPETITIONS,
    seed: int = 20260913,
    **kwargs: Any,
) -> dict[str, Any]:
    """Empirical false-positive rate for the between-release response-slope test.

    Runs the slope estimator on repeated draws from a simulated null scenario:
    the shock is present but there is no communication, so any rejection is a
    false discovery for that slope estimand.

    This is deliberately NOT a null audit of the news-versus-network forecast
    comparison, and it cannot unlock that promotion gate: it reports a slope-unit
    rate on a different estimand. Use
    :func:`market_propagation.falsification.network_falsification` for the
    forecast gate. The payload states this scope explicitly so a caller cannot
    mistake one for the other.
    """
    options = {
        key: value
        for key, value in kwargs.items()
        if key not in {"true_slope", "relevant_slope", "n_events", "repetitions", "seed"}
    }
    payload = power_assessment(
        n_events=n_events,
        true_slope=0.0,
        relevant_slope=0.0,
        residual_sigma=0.03,
        repetitions=repetitions,
        seed=seed,
        scenario=scenario,
        **options,
    )
    payload["null_scenario"] = scenario
    payload["false_positive_rate_at_null"] = payload.get("false_positive_rate")
    payload["forecast_gate_use"] = (
        "prohibited: this is a response-slope null rate, not the nested "
        "news-versus-network forecast comparison; the network promotion gate rejects it"
    )
    return payload


def simulation_calibration(
    scenario: str,
    *,
    seed: int = 20260913,
    n_events: int = 120,
    horizon_seconds: int | None = None,
    model_kinds: Sequence[str] = ("news",),
) -> dict[str, Any]:
    """Calibrate event-level residual scale from a named simulated scenario.

    Fits the declared model on the scenario's primary target contract at one
    horizon, aggregates residuals to the release, and reports the event-level
    residual standard deviation and the conditional R-squared. This is what
    keeps ``power_assessment`` tied to a stated nuisance range rather than a
    made-up constant.
    """
    from .simulation import primary_target, simulate_scenario

    if not model_kinds:
        raise ForecastEvaluationError("model_kinds must not be empty")
    frame = simulate_scenario(scenario, seed=seed, n_events=n_events)
    frame = primary_target(frame)
    frame = frame.loc[frame["valid"].fillna(False).astype(bool)]
    if frame.empty:
        return {
            "status": "inconclusive",
            "reason": (
                f"scenario {scenario!r} has no valid primary-target row; residual scale is not "
                "identified"
            ),
            "scenario": scenario,
        }
    horizons = (
        [int(horizon_seconds)]
        if horizon_seconds is not None
        else sorted(int(value) for value in frame["horizon_seconds"].unique())
    )
    per_horizon: dict[str, Any] = {}
    best: dict[str, Any] | None = None
    for horizon in horizons:
        part = frame.loc[frame["horizon_seconds"] == horizon]
        if part.empty:
            continue
        features = [
            "own_lag",
            "shock",
            "delayed_shock",
        ]
        for kind in model_kinds:
            columns = [name for name in features if name in part.columns]
            if not columns and kind != "no_change":
                columns = ["own_lag"] if "own_lag" in part.columns else []
            design = (
                part[columns].to_numpy(dtype=np.float64) if columns else np.zeros((len(part), 0))
            )
            design = np.column_stack([np.ones(len(part)), design])
            target = part["target"].to_numpy(dtype=np.float64)
            try:
                solution, *_ = np.linalg.lstsq(design, target, rcond=None)
            except np.linalg.LinAlgError:  # pragma: no cover - defensive
                continue
            residual = target - design @ solution
            events = part["event_id"].astype(str).to_numpy()
            residual_frame = pd.DataFrame({"event": events, "residual": residual})
            event_residual = residual_frame.groupby("event", sort=True)["residual"].mean()
            event_target = part.groupby("event_id", sort=True)["target"].mean()
            sigma = float(event_residual.std(ddof=1)) if len(event_residual) > 1 else None
            variance = float(np.mean((event_target - event_target.mean()) ** 2))
            entry = {
                "kind": kind,
                "horizon_seconds": int(horizon),
                "features": columns,
                "n_events": len(event_residual),
                "event_residual_sigma": sigma,
                "response_std": float(np.sqrt(variance)),
                "r_squared": (
                    float(1.0 - (sigma**2) / variance)
                    if sigma is not None and variance > 0
                    else None
                ),
                "event_residuals": {
                    str(key): float(value) for key, value in event_residual.items()
                },
                "status": "ok" if sigma is not None and sigma > 0 else "inconclusive",
                "reason": (
                    None
                    if sigma is not None and sigma > 0
                    else "fewer than two releases, or a zero residual scale; the scenario cannot "
                    "calibrate a nuisance range at this horizon"
                ),
            }
            per_horizon[f"h={horizon}|{kind}"] = entry
            if entry["status"] == "ok" and (
                best is None or entry["horizon_seconds"] < best["horizon_seconds"]
            ):
                best = entry
    if best is None:
        return {
            "status": "inconclusive",
            "reason": f"no horizon of scenario {scenario!r} produced a positive residual scale",
            "scenario": scenario,
            "by_horizon": per_horizon,
        }
    return {
        "status": "ok",
        "scenario": scenario,
        "seed": int(seed),
        "n_events": int(n_events),
        "horizon_seconds": best["horizon_seconds"],
        "kind": best["kind"],
        "features": best["features"],
        "event_residual_sigma": best["event_residual_sigma"],
        "response_std": best["response_std"],
        "r_squared": best["r_squared"],
        "n_events_used": best["n_events"],
        "by_horizon": per_horizon,
        "note": (
            "the residual scale is a property of this simulated process at the stated nuisance "
            "parameters; observational nuisance ranges must still be calibrated from development "
            "data, and values outside that range must be tested"
        ),
    }


def classify_outcome(
    lower: float,
    upper: float,
    *,
    relevant_effect: float,
    null_value: float = 0.0,
) -> dict[str, Any]:
    """Classify an interval into evidence, an informative bound, or inconclusive.

    ``evidence_for``
        The interval excludes the null value and lies entirely beyond the
        smallest relevant effect in magnitude.
    ``informative_bound``
        The interval excludes effects above the relevant threshold, which is a
        bound rather than only a nonsignificant result.
    ``inconclusive``
        The interval spans both the null and the relevant threshold, so the data
        cannot distinguish zero from the effect that matters.
    """
    for value, name in ((lower, "lower"), (upper, "upper"), (relevant_effect, "relevant_effect")):
        if not math.isfinite(float(value)):
            raise ForecastEvaluationError(f"{name}={value!r} must be finite")
    if float(lower) > float(upper):
        raise ForecastEvaluationError(
            f"lower={lower!r} exceeds upper={upper!r}; the interval is not ordered"
        )
    threshold = abs(float(relevant_effect))
    excludes_null = float(lower) > float(null_value) or float(upper) < float(null_value)
    if excludes_null and (float(lower) >= threshold or float(upper) <= -threshold):
        classification = "evidence_for"
        reason = "the interval excludes the null value and lies beyond the smallest relevant effect"
    elif float(upper) <= threshold and float(lower) >= -threshold:
        classification = "informative_bound"
        reason = (
            f"the interval excludes effects above the relevant threshold "
            f"{threshold!r}: an informative bound rather than a nonsignificant result"
        )
    else:
        classification = "inconclusive"
        reason = (
            "the interval spans both the null value and the smallest relevant effect, so zero and "
            "the relevant effect cannot be distinguished"
        )
    return {
        "classification": classification,
        "interval": {"lower": float(lower), "upper": float(upper)},
        "null_value": float(null_value),
        "relevant_effect": float(relevant_effect),
        "excludes_null": bool(excludes_null),
        "excludes_relevant": bool(float(upper) <= threshold or float(lower) >= threshold),
        "interval_width": float(upper) - float(lower),
        "reason": reason,
        "interpretation": (
            "classification is conditional on the stated coverage of the interval and on the "
            "prespecified relevant effect"
        ),
    }


def evaluate_quotes(
    quotes: pd.DataFrame,
    *,
    instrument_filter: str | None = None,
    window_seconds: float = 1.0,
    at: Any = None,
    horizon_seconds: float | None = None,
    prices: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Quote-level assessment: observed spread plus family feasibility.

    Composes :func:`observed_spread_summary` and
    :func:`feasibility_distribution` so a caller gets one provenance-carrying
    record. Both are statements about observed quotations under stated
    assumptions; neither is a probability edge, and ``edge`` stays ``None`` for
    the reason recorded in the payload.

    ``horizon_seconds`` and ``prices`` are recorded for provenance only. A
    horizon would matter for an expected-value calculation, and no such
    calculation is supported here.
    """
    if not isinstance(quotes, pd.DataFrame):
        raise ForecastEvaluationError(
            f"evaluate_quotes expects a pandas DataFrame, got {type(quotes).__name__}"
        )
    spread = observed_spread_summary(quotes, at=at, window_seconds=window_seconds)
    feasibility = feasibility_distribution(quotes, instrument_filter=instrument_filter)
    return {
        "observed_spread": spread,
        "feasibility": feasibility,
        "edge": None,
        "edge_reason": (
            "no probability edge is reported: an edge needs an assumed distribution over atomic "
            "outcomes plus an execution model, and reporting one from quotes alone would claim "
            "certainty the inputs do not support"
        ),
        "recorded_not_used": {
            "horizon_seconds": None if horizon_seconds is None else float(horizon_seconds),
            "prices_supplied": prices is not None,
        },
        "status": (
            "ok" if spread["status"] == "ok" and feasibility["status"] == "ok" else "inconclusive"
        ),
        "interpretation": (
            "quotation-level diagnostics only; the key comparison is between genuine joint-price "
            "inconsistency and a charting artifact, and a projection enforces coherence so it "
            "cannot itself demonstrate market coherence"
        ),
    }


def observed_spread_summary(
    quotes: pd.DataFrame,
    *,
    at: Any = None,
    window_seconds: float = 1.0,
    validity_column: str = "valid",
    time_column: str = "observation_time",
    spread_column: str = "spread",
) -> dict[str, Any]:
    """Observed spread range in a stated window, for a feasibility summary.

    Anchoring on the latest admissible observation time by default means the
    summary describes what was actually observed rather than an assumed
    distribution. Rows failing the validity gate are excluded and counted, and
    the filter that was applied is recorded, because a spread summary is only
    meaningful alongside its instrument set.
    """
    if not isinstance(quotes, pd.DataFrame):
        raise ForecastEvaluationError(
            f"observed_spread_summary expects a pandas DataFrame, got {type(quotes).__name__}"
        )
    missing = [column for column in (time_column, spread_column) if column not in quotes.columns]
    if missing:
        raise ForecastEvaluationError(
            f"quotes is missing required column(s) {missing}; present columns are "
            f"{sorted(quotes.columns)}"
        )
    if quotes.empty:
        return {"status": "inconclusive", "reason": "quotes is empty"}
    times = pd.to_datetime(quotes[time_column], utc=True, errors="coerce")
    excluded_invalid = 0
    usable = quotes.loc[times.notna()]
    if validity_column in usable.columns:
        valid = usable[validity_column].fillna(False).astype(bool)
        excluded_invalid = int((~valid).sum())
        usable = usable.loc[valid]
        times = pd.to_datetime(usable[time_column], utc=True, errors="coerce")
    if usable.empty:
        return {
            "status": "inconclusive",
            "reason": "no row passes the validity gate",
            "excluded_invalid": excluded_invalid,
        }
    anchor = pd.Timestamp(times.max()) if at is None else pd.Timestamp(at)
    window = usable.loc[
        (times >= anchor - pd.Timedelta(seconds=float(window_seconds))) & (times <= anchor)
    ]
    if window.empty:
        return {
            "status": "inconclusive",
            "reason": f"no valid quote within {window_seconds!r}s before {anchor.isoformat()}",
            "excluded_invalid": excluded_invalid,
        }
    spread = pd.to_numeric(window[spread_column], errors="coerce").dropna()
    if spread.empty:
        return {
            "status": "inconclusive",
            "reason": f"no numeric {spread_column!r} value in the stated window",
            "excluded_invalid": excluded_invalid,
        }
    return {
        "status": "ok",
        "as_of": anchor.isoformat(),
        "window_seconds": float(window_seconds),
        "n_quotes": len(window),
        "excluded_invalid": excluded_invalid,
        "spread_min": float(spread.min()),
        "spread_max": float(spread.max()),
        "spread_median": float(spread.median()),
        "spread_mean": float(spread.mean()),
        "venues": (
            sorted(window["venue"].astype(str).unique().tolist())
            if "venue" in window.columns
            else None
        ),
        "contracts": (
            sorted(window["contract_id"].astype(str).unique().tolist())
            if "contract_id" in window.columns
            else None
        ),
        "instrument_filtered": False,
        "note": (
            "spread is the observed quoted width in this window, not an assumed distribution; "
            "any cost model built on it must state that assumption separately"
        ),
    }


def feasibility_distribution(
    quotes: pd.DataFrame,
    *,
    instrument_filter: str | None = None,
    validity_column: str = "valid",
) -> dict[str, Any]:
    """Whether the supplied quotes can admit one coherent state distribution.

    This is a feasibility statement about a quoted box for one instrument set,
    and it is deliberately not a probability edge. An edge would need an assumed
    probability estimate over the atomic outcomes plus a spread and execution
    model; neither is present in quote data alone, so no expected value is
    produced. The result names the instrument set, applies the validity gate, and
    reports the coordinatewise overlap of all quoted intervals, which is exactly
    the condition for a single coherent state vector to lie inside the box.

    Overlap is not an arbitrage claim: fees, inventory, finite size, differing
    cashflows and non-simultaneous execution are all outside the calculation.
    """
    if not isinstance(quotes, pd.DataFrame):
        raise ForecastEvaluationError(
            f"feasibility_distribution expects a pandas DataFrame, got {type(quotes).__name__}"
        )
    missing = [column for column in ("bid", "ask") if column not in quotes.columns]
    if missing:
        raise ForecastEvaluationError(
            f"quotes is missing required column(s) {missing}; present columns are "
            f"{sorted(quotes.columns)}"
        )
    if quotes.empty:
        raise ForecastEvaluationError("quotes is empty; there is nothing to assess")
    if instrument_filter is None:
        return {
            "status": "inconclusive",
            "reason": (
                "no instrument filter was supplied; the supplied quotes may span independent "
                "contracts, so no single logical family is asserted and no feasibility verdict is "
                "reported"
            ),
            "note": (
                "name the contract set that belongs to one exhaustive partition before any "
                "feasibility statement is made"
            ),
            "instrument_filter": None,
            "edge": None,
            "edge_reason": (
                "no probability edge is reported: an edge needs an assumed distribution over "
                "atomic outcomes plus an execution model, which quotes alone do not establish"
            ),
        }
    selected = quotes
    if "contract_id" in quotes.columns:
        selected = quotes.loc[quotes["contract_id"].astype(str) == str(instrument_filter)]
    if selected.empty:
        return {
            "status": "inconclusive",
            "instrument_filter": str(instrument_filter),
            "reason": "no row matches the stated instrument filter",
            "edge": None,
            "edge_reason": (
                "no probability edge is reported: an edge needs an assumed distribution over "
                "atomic outcomes plus an execution model, which quotes alone do not establish"
            ),
        }
    if validity_column in selected.columns:
        valid = selected[validity_column].fillna(False).astype(bool)
        excluded = int((~valid).sum())
        selected = selected.loc[valid]
    else:
        excluded = 0
    bid = pd.to_numeric(selected["bid"], errors="coerce")
    ask = pd.to_numeric(selected["ask"], errors="coerce")
    usable = bid.notna() & ask.notna()
    excluded_missing = int((~usable).sum())
    bid = bid.loc[usable]
    ask = ask.loc[usable]
    if bid.empty:
        return {
            "status": "inconclusive",
            "instrument_filter": str(instrument_filter),
            "reason": "no row has a usable two-sided quote",
            "excluded_invalid": excluded,
            "excluded_missing_side": excluded_missing,
        }
    crossed = bid > ask
    lower = float(bid.max())
    upper = float(ask.min())
    return {
        "status": "ok",
        "instrument_filter": str(instrument_filter),
        "n_quotes": len(bid),
        "excluded_invalid": excluded,
        "excluded_missing_side": excluded_missing,
        "n_crossed": int(crossed.sum()),
        "overlap_lower": lower,
        "overlap_upper": upper,
        "box_intersects_box": bool(lower <= upper),
        "edge": None,
        "edge_reason": (
            "no probability edge is reported: an edge would require an assumed distribution over "
            "the atomic outcomes and an execution model that these quotes do not establish, and "
            "reporting one would claim certainty the inputs do not support"
        ),
        "interpretation": (
            "a non-empty overlap means one coherent state vector can sit inside every quoted "
            "interval; an empty overlap is a quotation inconsistency under the stated "
            "assumptions, not automatically an executable arbitrage"
        ),
    }
