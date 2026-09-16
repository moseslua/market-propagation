"""Null and recovery audit of the network forecast gate.

This module answers one question with the production pipeline and nothing else:
across fixed seeds, does the nested news-versus-network forecast comparison
recover a declared transmission edge when one exists, and how often does it
appear to do so when the process is a common-news-plus-delay null with no
communication at all?

Every repetition therefore runs
:func:`market_propagation.simulation.simulate_scenario` and
:func:`market_propagation.simulation.primary_target` to build the frames, then
:func:`market_propagation.models.nested_comparison` to fit and score the same
ladder, with the same split authority, the same metric and the same MAE-gain
threshold. No alternate estimator is implemented here, no truth column is ever a
predictor, and power is reported only from observed recovery counts -- never
inferred from a scenario's declared mechanism.

A finite repetition count cannot certify a rate better than its own sampling
supports, so the result is explicitly ``inconclusive`` when the one-sided bound
implied by ``repetitions`` is wider than the requested false-positive ceiling or
the requested power target.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from . import models
from .evaluation import clustered_bootstrap
from .simulation import TRUTH_ONLY_COLUMNS, primary_target, simulate_scenario

__all__ = [
    "DEFAULT_FALSE_POSITIVE_CEILING",
    "DEFAULT_REPETITIONS",
    "DEFAULT_SEED",
    "DEFAULT_TARGET_POWER",
    "NETWORK_FALSIFICATION_ESTIMATOR",
    "NULL_SCENARIO",
    "RECOVERY_SCENARIO",
    "falsification_seeds",
    "network_falsification",
]

#: The null process: one common martingale latent, no transmission at all.
NULL_SCENARIO = "shared_news_delay"
#: The process that declares a real transmission edge.
RECOVERY_SCENARIO = "communication"

DEFAULT_SEED = 20260913
DEFAULT_REPETITIONS = 40
DEFAULT_FALSE_POSITIVE_CEILING = 0.1
DEFAULT_TARGET_POWER = 0.8
#: One-sided confidence for the rate bounds implied by a finite repetition count.
RATE_CONFIDENCE = 0.95

#: Identity reported by this routine's ``status='ok'`` payload. It restates
#: :data:`market_propagation.models.FORECAST_ESTIMATOR`, so the promotion gate
#: can confirm that a supplied null assessment came from this same pipeline.
NETWORK_FALSIFICATION_ESTIMATOR: Mapping[str, Any] = {
    "estimator": models.FORECAST_ESTIMATOR["estimator"],
    "engine": models.FORECAST_ESTIMATOR["engine"],
    "split": models.FORECAST_ESTIMATOR["split"],
    "metric": models.FORECAST_ESTIMATOR["metric"],
    "target": models.FORECAST_ESTIMATOR["target"],
    "unit": models.FORECAST_ESTIMATOR["unit"],
}


class FalsificationError(ValueError):
    """The falsification request is not well posed at the stated repetition count."""


def _require_positive(value: Any, *, name: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0.0:
        raise FalsificationError(f"{name}={value!r} must be finite and positive")
    return number


def _require_rate(value: Any, *, name: str) -> float:
    number = float(value)
    if not math.isfinite(number) or not 0.0 < number < 1.0:
        raise FalsificationError(f"{name}={value!r} must lie strictly inside (0, 1)")
    return number


def _repetition_seed(seed: int, index: int) -> int:
    """Process-independent seed for one repetition; never derived from ``hash``.

    A fixed stride keeps the list reproducible across processes and keeps the
    seeds far enough apart that two repetitions do not share a stream by
    accident. The multiplier agrees with the simulator's own default seed scale.
    """
    return (int(seed) + 1_000_003 * int(index)) % (2**31 - 1)


def falsification_seeds(
    *, seed: int = DEFAULT_SEED, repetitions: int = DEFAULT_REPETITIONS
) -> tuple[int, ...]:
    """The exact seed list this audit will use, exposed for the report registry.

    The same seed drives the null and the recovery process of one repetition, so
    the two frames share their nuisance draws and only the declared mechanism
    differs.
    """
    return tuple(_repetition_seed(seed, index) for index in range(int(repetitions)))


def _one_sided_bounds(
    successes: int, trials: int, *, confidence: float | None = None
) -> tuple[float | None, float | None]:
    """Clopper-Pearson one-sided bounds at :data:`RATE_CONFIDENCE`.

    The lower bound is what a finite sample can assert the rate exceeds; the
    upper bound is what it can assert the rate falls below. ``None`` is returned
    for a bound the sample cannot produce at all (no successes for a lower
    bound, all successes for an upper bound), which is what keeps a small
    repetition count from being read as a precise rate.

    ``confidence`` overrides the level, which a caller calibrating several
    scenarios at once needs in order to hold a simultaneous level across them.
    """
    coverage = RATE_CONFIDENCE if confidence is None else float(confidence)
    alpha = 1.0 - coverage
    if not 0.0 < alpha < 1.0:
        raise FalsificationError(
            f"confidence={coverage!r} must lie strictly inside (0, 1) so alpha is defined"
        )
    if trials <= 0:
        return None, None
    successes = int(successes)
    trials = int(trials)
    lower = (
        None if successes == 0 else float(stats.beta.ppf(alpha, successes, trials - successes + 1))
    )
    upper = (
        None
        if successes >= trials
        else float(stats.beta.ppf(1.0 - alpha, successes + 1, trials - successes))
    )
    return lower, upper


def _wilson_interval(
    successes: int, trials: int, *, coverage: float = RATE_CONFIDENCE
) -> dict[str, Any]:
    """Two-sided Wilson score interval for a binomial rate."""
    if trials <= 0:
        return {"lower": None, "upper": None, "coverage": float(coverage), "method": "wilson"}
    z = float(stats.norm.ppf(0.5 + coverage / 2.0))
    n = float(trials)
    phat = successes / n
    denominator = 1.0 + z * z / n
    centre = (phat + z * z / (2.0 * n)) / denominator
    half = (z / denominator) * math.sqrt(phat * (1.0 - phat) / n + z * z / (4.0 * n * n))
    return {
        "lower": float(max(0.0, centre - half)),
        "upper": float(min(1.0, centre + half)),
        "coverage": float(coverage),
        "method": "wilson score interval",
    }


def _release_level_pairs(record: dict[str, Any]) -> list[dict[str, Any]]:
    """Paired release-level news-minus-network losses from one comparison.

    The two kinds are evaluated on identical held-out rows, so their per-release
    mean absolute errors are aligned by release and can be differenced directly.
    """
    evaluations = record["evaluations"]
    if "news" not in evaluations or "network" not in evaluations:
        return []
    news = evaluations["news"]["per_event_mae"]
    network = evaluations["network"]["per_event_mae"]
    events = sorted(set(news) & set(network))
    return [
        {
            "event_id": str(event),
            "news_mae": float(news[event]),
            "network_mae": float(network[event]),
            "gain": float(news[event]) - float(network[event]),
        }
        for event in events
    ]


def _comparison_for(
    scenario: str,
    *,
    seed: int,
    n_events: int,
    horizon_seconds: float,
    prediction_delay_seconds: float,
    minimum_mae_gain: float,
) -> dict[str, Any]:
    """One production nested comparison on one fixed seed.

    The frame is built by the simulator and reduced by the simulator's own
    primary-target selector, then scored by the production nested pipeline. No
    truth-only column enters the design: the comparison reads only
    ``models.FEATURE_SPECS``, which is asserted against the simulator's
    truth-only names below.
    """
    frame = simulate_scenario(
        scenario,
        seed=int(seed),
        n_events=int(n_events),
        horizons=[int(horizon_seconds)],
        forecast_delay=float(prediction_delay_seconds),
    )
    target = primary_target(frame)
    result = models.nested_comparison(
        target,
        seed=int(seed),
        kinds=("news", "network"),
        minimum_mae_gain=float(minimum_mae_gain),
    )
    record = result.as_record()
    release_pairs = _release_level_pairs(record)
    gains = [pair["gain"] for pair in release_pairs]
    return {
        "scenario": scenario,
        "seed": int(seed),
        "news_mae": float(record["promotion"]["baseline_mae"]),
        "network_mae": float(record["promotion"]["candidate_mae"]),
        "gain": float(record["promotion"]["mae_reduction"]),
        "meets_minimum_gain": bool(record["promotion"]["criteria_met"]["meets_minimum_mae_gain"]),
        "n_events_test": int(record["sample"]["n_events_test"]),
        "n_events_total": int(record["sample"]["n_events_total"]),
        "n_clusters_test": int(record["sample"]["n_clusters_test"]),
        "release_level_gains": release_pairs,
        "release_gain_mean": float(np.mean(gains)) if gains else None,
        "release_gain_sd": (float(np.std(gains, ddof=1)) if len(gains) > 1 else None),
        "release_gain_n": len(gains),
        "common_sample_violations": list(record["common_sample_violations"]),
    }


def _clustered_gain_interval(
    runs: Sequence[Mapping[str, Any]], *, seed: int, samples: int = 200
) -> dict[str, Any]:
    """Release-clustered interval for the pooled paired gain of a scenario.

    Every release of every repetition contributes one paired news-minus-network
    loss, labelled by its own release, and the draws come from the production
    cluster bootstrap. Releases from different seeds are distinct sampling units;
    they are pooled to describe the process, not to reuse one seed's draw.
    """
    frames: list[pd.DataFrame] = []
    for run in runs:
        pairs = run.get("release_level_gains") or []
        if not pairs:
            continue
        frames.append(
            pd.DataFrame(
                {
                    "unit": [f"{run['seed']}:{pair['event_id']}" for pair in pairs],
                    "cluster": [f"{run['seed']}:{pair['event_id']}" for pair in pairs],
                    "gain": [float(pair["gain"]) for pair in pairs],
                }
            )
        )
    if not frames:
        return {"status": "inconclusive", "reason": "no comparison produced release-level pairs"}
    pooled = pd.concat(frames, ignore_index=True)
    values = pd.Series(pooled["gain"].to_numpy(dtype=np.float64), index=pooled["unit"])
    clusters = pd.Series(pooled["cluster"].to_numpy(), index=pooled["unit"])
    payload = clustered_bootstrap(
        {"gain": float(values.mean())},
        lambda sample: {"gain": float(values.loc[list(sample)].mean())},
        clusters,
        seed=int(seed),
        samples=int(samples),
    )
    sample = payload["samples"]["gain"]
    return {
        "status": sample["status"],
        "point": sample["point"],
        "lower": sample["lower"],
        "upper": sample["upper"],
        "n_units": payload["n_units"],
        "n_clusters": payload["n_clusters"],
        "samples_effective": payload["samples_effective"],
        "coverage": payload["coverage"],
        "method": payload["method"],
    }


def network_falsification(
    *,
    n_events: int = 120,
    repetitions: int = DEFAULT_REPETITIONS,
    seed: int = DEFAULT_SEED,
    minimum_mae_gain: float = 0.005,
    horizon_seconds: float = 300,
    prediction_delay_seconds: float = 60,
    max_false_positive_rate: float = DEFAULT_FALSE_POSITIVE_CEILING,
    target_power: float = DEFAULT_TARGET_POWER,
) -> dict[str, Any]:
    """Audit the network forecast gate on the production pipeline over fixed seeds.

    For each repetition the same seed drives the common-news-plus-delay null and
    the communication process, so the two frames share their nuisance draws and
    the only difference is the declared transmission edge. Each frame is reduced
    by :func:`market_propagation.simulation.primary_target` and scored by
    :func:`market_propagation.models.nested_comparison` with ``kinds=('news',
    'network')`` and the caller's ``minimum_mae_gain``. Nothing else is fitted:
    there is no alternate estimator, no truth column is used as a predictor, and
    the thresholds, metric, split authority, horizon and prediction delay are the
    production ones.

    Reported quantities:

    ``null``
        False-positive count and rate: repetitions in which the null process
        still cleared ``minimum_mae_gain``, with its binomial interval.
    ``recovery``
        Power: repetitions in which the communication process cleared the
        threshold, with its binomial interval. Power is counted from observed
        recoveries only and is never asserted from scenario metadata.
    ``paired_contrast``
        The per-repetition communication-minus-null gain, plus a release-clustered
        interval for each process's pooled paired release-level losses.

    ``repetitions`` is a finite sample, so ``status`` is ``inconclusive`` unless
    the one-sided Clopper-Pearson lower bound on recovery clears
    ``target_power`` and the upper bound on the null rate stays under
    ``max_false_positive_rate``; the reasons are listed in ``inconclusive_reasons``.
    """
    events = int(n_events)
    if events < 3:
        raise FalsificationError(f"n_events={n_events!r} must be at least 3")
    draws = int(repetitions)
    if draws < 2:
        raise FalsificationError(
            f"repetitions={repetitions!r} must be at least 2; a single draw is not a rate"
        )
    gain = _require_positive(minimum_mae_gain, name="minimum_mae_gain")
    horizon = _require_positive(horizon_seconds, name="horizon_seconds")
    delay = _require_positive(prediction_delay_seconds, name="prediction_delay_seconds")
    ceiling = _require_rate(max_false_positive_rate, name="max_false_positive_rate")
    power_target = _require_rate(target_power, name="target_power")

    truth_only_predictors = sorted(set(TRUTH_ONLY_COLUMNS) & set(models.FEATURE_SPECS["network"]))
    if truth_only_predictors:  # pragma: no cover - guards the feature contract
        raise FalsificationError(
            f"truth-only column(s) {truth_only_predictors} are in the network feature set; "
            "the falsification audit refuses to score a design that reads ground truth"
        )

    seed_list = falsification_seeds(seed=seed, repetitions=draws)
    null_runs: list[dict[str, Any]] = []
    recovery_runs: list[dict[str, Any]] = []
    for repeat_seed in seed_list:
        null_runs.append(
            _comparison_for(
                NULL_SCENARIO,
                seed=repeat_seed,
                n_events=events,
                horizon_seconds=horizon,
                prediction_delay_seconds=delay,
                minimum_mae_gain=gain,
            )
        )
        recovery_runs.append(
            _comparison_for(
                RECOVERY_SCENARIO,
                seed=repeat_seed,
                n_events=events,
                horizon_seconds=horizon,
                prediction_delay_seconds=delay,
                minimum_mae_gain=gain,
            )
        )

    null_successes = int(sum(1 for run in null_runs if run["meets_minimum_gain"]))
    recovery_successes = int(sum(1 for run in recovery_runs if run["meets_minimum_gain"]))
    null_rate = null_successes / draws
    recovery_rate = recovery_successes / draws
    null_lower, null_upper = _one_sided_bounds(null_successes, draws)
    recovery_lower, recovery_upper = _one_sided_bounds(recovery_successes, draws)

    paired = [
        {
            "seed": int(null_run["seed"]),
            "null_gain": float(null_run["gain"]),
            "recovery_gain": float(recovery_run["gain"]),
            "paired_difference": float(recovery_run["gain"]) - float(null_run["gain"]),
        }
        for null_run, recovery_run in zip(null_runs, recovery_runs, strict=True)
    ]
    differences = np.asarray([row["paired_difference"] for row in paired], dtype=np.float64)
    difference_se = (
        float(differences.std(ddof=1) / math.sqrt(draws)) if draws > 1 else None
    )  # pragma: no cover - guarded by the minimum repetition count
    z = float(stats.norm.ppf(0.5 + RATE_CONFIDENCE / 2.0))
    paired_contrast: dict[str, Any] = {
        "n_pairs": len(paired),
        "mean_null_gain": float(np.mean([run["gain"] for run in null_runs])),
        "mean_recovery_gain": float(np.mean([run["gain"] for run in recovery_runs])),
        "mean_paired_difference": float(differences.mean()) if differences.size else None,
        "sd_paired_difference": (float(differences.std(ddof=1)) if differences.size > 1 else None),
        "standard_error": difference_se,
        "interval": (
            {
                "lower": float(differences.mean() - z * difference_se),
                "upper": float(differences.mean() + z * difference_se),
                "coverage": RATE_CONFIDENCE,
                "method": "normal approximation over paired repetition seeds",
            }
            if difference_se is not None
            else {"lower": None, "upper": None, "coverage": RATE_CONFIDENCE, "method": None}
        ),
        "positive_difference_count": int((differences > 0.0).sum()),
        "per_seed": paired,
        "note": (
            "each pair shares one seed and therefore one set of nuisance draws, so the "
            "difference isolates the declared mechanism rather than the noise"
        ),
    }

    release_intervals = {
        "null": _clustered_gain_interval(null_runs, seed=int(seed)),
        "recovery": _clustered_gain_interval(recovery_runs, seed=int(seed) + 1),
    }

    inconclusive_reasons: list[str] = []
    if null_upper is None or null_upper > ceiling:
        inconclusive_reasons.append(
            "the null false-positive rate is not bounded below "
            f"{ceiling!r} at {RATE_CONFIDENCE:.2f} confidence by {draws} repetition(s)"
            + ("" if null_upper is None else f" (one-sided upper bound {null_upper:.4f})")
        )
    if recovery_lower is None or recovery_lower < power_target:
        inconclusive_reasons.append(
            "recovery is not bounded above "
            f"{power_target!r} at {RATE_CONFIDENCE:.2f} confidence by {draws} repetition(s)"
            + ("" if recovery_lower is None else f" (one-sided lower bound {recovery_lower:.4f})")
        )

    event_counts = {
        "n_events_requested": events,
        "per_repetition_test_events": sorted({run["n_events_test"] for run in recovery_runs}),
        "per_repetition_total_events": sorted({run["n_events_total"] for run in recovery_runs}),
        "per_repetition_test_clusters": sorted({run["n_clusters_test"] for run in recovery_runs}),
    }

    return {
        "status": "ok" if not inconclusive_reasons else "inconclusive",
        "inconclusive_reasons": inconclusive_reasons,
        **NETWORK_FALSIFICATION_ESTIMATOR,
        "estimator_identity": dict(NETWORK_FALSIFICATION_ESTIMATOR),
        "metric": models.FORECAST_ESTIMATOR["metric"],
        "direction": "higher_gain_is_better",
        "minimum_mae_gain": gain,
        "horizon_seconds": [int(horizon)],
        "prediction_delay_seconds": float(delay),
        "n_events": events,
        "repetitions": draws,
        "seed": int(seed),
        "seeds": list(seed_list),
        "null_scenario": NULL_SCENARIO,
        "recovery_scenario": RECOVERY_SCENARIO,
        "null": {
            "scenario": NULL_SCENARIO,
            "false_positive_count": null_successes,
            "false_positive_rate": float(null_rate),
            "interval": _wilson_interval(null_successes, draws),
            "one_sided_upper_bound": null_upper,
            "one_sided_lower_bound": null_lower,
            "max_false_positive_rate": ceiling,
            "bounded_below_ceiling": bool(null_upper is not None and null_upper <= ceiling),
            "mean_gain": float(np.mean([run["gain"] for run in null_runs])),
            "runs": null_runs,
        },
        #: Consumed by the promotion gate as ``false_positive_rate_at_null``.
        "false_positive_rate_at_null": float(null_rate),
        "recovery": {
            "scenario": RECOVERY_SCENARIO,
            "recovery_count": recovery_successes,
            "power": float(recovery_rate),
            "interval": _wilson_interval(recovery_successes, draws),
            "one_sided_lower_bound": recovery_lower,
            "one_sided_upper_bound": recovery_upper,
            "target_power": power_target,
            "meets_target_power": bool(
                recovery_lower is not None and recovery_lower >= power_target
            ),
            "mean_gain": float(np.mean([run["gain"] for run in recovery_runs])),
            "runs": recovery_runs,
        },
        "paired_contrast": paired_contrast,
        "release_level_intervals": release_intervals,
        "event_counts": event_counts,
        "rate_confidence": RATE_CONFIDENCE,
        "interpretation": (
            "this audit runs the production nested news-versus-network forecast comparison on "
            "simulated processes only; it bounds the gate's false-positive behaviour and its "
            "recovery rate at the stated event count and is not a real-market causal claim"
        ),
        "power_source": (
            "observed recovery count across fixed seeds; power is never inferred from the "
            "simulator's declared mechanism"
        ),
        "notes": [
            "every repetition shares one seed across the null and recovery frames, so the "
            "paired difference isolates the declared mechanism",
            "the estimator, split authority, metric and threshold are the production ones; no "
            "alternate estimator is implemented in this module",
            "truth-only simulator columns are never predictors",
        ],
    }
