"""Calibrate the study's complete network-promotion decision rule on simulated tapes.

The empirical study cannot calibrate its own gate, because it has no eligible
contracts to run on. This module calibrates the gate on a process whose truth is
known, exercising the production path end to end: transaction tape, exposure graph,
forecast panel, ladder design, nested comparison, paired uncertainty, promotion
decision. A defect anywhere in that chain shows up here as a calibration failure
rather than being hidden by a parallel implementation that agrees with itself.

What the decision rule is
-------------------------
A repetition promotes the network rung when all three hold on identical held-out
rows: the comparison's common-sample and cutoff gates are clean, the point MAE gain
over the shared-news baseline is at least ``minimum_mae_gain``, and the release
clustered one-sided lower bound on the paired gain is above zero. The rate this
module measures is the rate of *that whole conjunction*, not of a bare
``gain >= threshold`` test, which would not certify the procedure that ships.

The one gate this module deliberately does not require
------------------------------------------------------
``nested_comparison`` also exposes ``criteria_met.null_assessment_matches_and_ok``,
which only clears when the caller hands it a null assessment whose estimator,
metric, threshold, horizons, delay and event count all match the comparison in
hand. Requiring it here would be circular: the null assessment *is* the object being
calibrated, so demanding one per repetition would assert the conclusion. This module
therefore reads that flag, records it, and excludes it from the decision, and says so
in the certificate.

What a result means
-------------------
Nothing about any real venue. Every row is generated. A clean calibration says the
declared decision rule holds its size on this process and recovers this mechanism;
it is not an empirical finding about information diffusion.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import platform
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pandas as pd

from . import historical_forecast, neighbors, simulated_tapes, study
from .falsification import (
    DEFAULT_SEED,
    RATE_CONFIDENCE,
    _clustered_gain_interval,
    _one_sided_bounds,
    _release_level_pairs,
    _repetition_seed,
)
from .models import MODEL_KINDS, nested_comparison
from .registry import ExperimentRegistry
from .simulation import SCENARIOS, scenario_names

__all__ = [
    "CALIBRATION_VERSION",
    "CERTIFICATE_NAME",
    "DEFAULT_N_RELEASES",
    "DEFAULT_REPETITIONS",
    "DEFAULT_TARGET_POWER",
    "STATUS_BLOCKED",
    "STATUS_COMPLETE",
    "VERDICT_FAIL",
    "VERDICT_INCONCLUSIVE",
    "VERDICT_PASS",
    "CalibrationCertificate",
    "RepetitionOutcome",
    "ScenarioCalibration",
    "calibrate",
    "write_certificate",
]

#: Identity of the calibrated rule. A changed analysis invalidates the certificate.
CALIBRATION_VERSION = "study_v2_rule_calibration/1"

#: Artifact this module writes.
CERTIFICATE_NAME = "calibration_certificate.json"

#: One registry file serves every calibration run. It is a separate ledger from the
#: empirical study's, because a calibration is a measurement of the rule on a synthetic
#: process and must not be readable out of the store that holds empirical runs.
REGISTRY_DB_NAME = "rule_calibration.sqlite3"

#: Where the shared ledger lives, relative to the checkout root.
SHARED_REGISTRY_DIR = "data/registry"

#: Final calibration repetitions per primary scenario, per the execution plan.
DEFAULT_REPETITIONS = 200

#: Releases per repetition. Set for fold adequacy rather than for power: the declared
#: 60/20/20 split is taken over release clusters, and at 24 releases the held-out fold
#: held only two or three releases, which dominated both measured rates. The same rule
#: at this count leaves twelve or thirteen, and the two arms separate. The rule's own
#: declared parameters are never adjusted to reach a verdict.
DEFAULT_N_RELEASES = 120

#: Declared smallest relevant improvement, in probability points.
DEFAULT_MINIMUM_MAE_GAIN = 0.005

#: The ceiling the null's one-sided upper bound must clear. The falsification module's
#: own default is 0.1, which bounds the null's true rate below ten percent; the study's
#: requirement is that a gate at five percent be certified at five percent, and a run
#: that only bounds the rate below ten does not do that. The tighter value is used here.
DEFAULT_NULL_RATE_CEILING = 0.05

#: Declared power target for a recovery scenario.
DEFAULT_TARGET_POWER = 0.8

#: Release-clustered bootstrap draws for the paired interval.
DEFAULT_BOOTSTRAP_SAMPLES = 200

#: The generative family the tapes are built from. One family at a time, because a
#: declared calendar is a sequence of same-series releases.
TAPE_FAMILY = "cpi"

STATUS_COMPLETE = "complete"
STATUS_BLOCKED = "blocked"

VERDICT_PASS = "pass"
VERDICT_FAIL = "fail"
VERDICT_INCONCLUSIVE = "inconclusive"


#: Measurement-design notes carried in every certificate. The distinction they draw is
#: the one that keeps this a calibration rather than a search for a passing number: the
#: sample the rule is measured on is a design choice, and the rule's declared parameters
#: are not.
DESIGN_NOTES: tuple[str, ...] = (
    "releases per repetition are chosen so the declared 60/20/20 split leaves enough "
    "held-out releases for a release-clustered bound; at 24 releases it left two or "
    "three and the measured rates reflected that rather than the rule",
    "the rule's declared parameters are unchanged from configs/study_v2.yaml: "
    "minimum MAE gain, the null ceiling and the power target are the declared values",
    "every row is generated by market_propagation.simulation and the promotion decision "
    "is read from the production comparison, not from a reimplementation of it",
)


class CalibrationError(RuntimeError):
    """The declared decision rule could not be evaluated as specified."""


@dataclass(frozen=True, slots=True)
class DonorMap:
    """A ``donors_for`` view over donor ids assembled from real graph builds.

    ``build_forecast_rows`` accepts any object exposing ``donors_for``, so a placebo
    donor set assembled from ``build_neighbor_graph`` calls is a supported input
    rather than a second graph implementation.
    """

    donors: Mapping[str, tuple[str, ...]]

    def donors_for(self, receiver_contract_id: str) -> tuple[str, ...]:
        return self.donors.get(receiver_contract_id, ())


@dataclass(frozen=True, slots=True)
class RepetitionOutcome:
    """One repetition: the decision, its inputs, or why it could not be evaluated."""

    scenario: str
    seed: int
    status: str
    promoted: bool
    reason: str | None = None
    gain: float | None = None
    lower: float | None = None
    coverage: float | None = None
    gates_clean: bool | None = None
    meets_minimum_gain: bool | None = None
    internal_null_gate_ok: bool | None = None
    n_test_releases: int = 0
    n_rows: int = 0
    graph_edges: int = 0
    tape_digest: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "seed": self.seed,
            "status": self.status,
            "promoted": self.promoted,
            "reason": self.reason,
            "gain": self.gain,
            "paired_lower_bound": self.lower,
            "interval_coverage": self.coverage,
            "gates_clean": self.gates_clean,
            "meets_minimum_gain": self.meets_minimum_gain,
            "internal_null_gate_ok": self.internal_null_gate_ok,
            "n_test_releases": self.n_test_releases,
            "n_rows": self.n_rows,
            "graph_edges": self.graph_edges,
            "tape_digest": self.tape_digest,
        }


@dataclass(frozen=True, slots=True)
class ScenarioCalibration:
    """One scenario's repetition summary, with its one-sided bound."""

    scenario: str
    kind: str
    repetitions: int
    complete: int
    blocked: int
    promoted: int
    rate: float | None
    one_sided_upper: float | None
    one_sided_lower: float | None
    confidence: float
    meets_requirement: bool
    blocked_reasons: Mapping[str, int] = field(default_factory=dict)
    outcomes: tuple[Mapping[str, Any], ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "kind": self.kind,
            "repetitions": self.repetitions,
            "complete": self.complete,
            "blocked": self.blocked,
            "promoted": self.promoted,
            "rate": self.rate,
            "one_sided_lower_bound": self.one_sided_lower,
            "one_sided_upper_bound": self.one_sided_upper,
            "confidence": self.confidence,
            "meets_requirement": self.meets_requirement,
            "blocked_reasons": dict(self.blocked_reasons),
            "outcomes": [dict(outcome) for outcome in self.outcomes],
        }


@dataclass(frozen=True, slots=True)
class CalibrationCertificate:
    """The calibration result, its declared inputs and its verdict."""

    version: str
    generated_at: str
    seed: int
    repetitions: int
    n_releases: int
    minimum_mae_gain: float
    false_positive_ceiling: float
    target_power: float
    bootstrap_samples: int
    null_scenarios: tuple[str, ...]
    recovery_scenarios: tuple[str, ...]
    scenarios: Mapping[str, ScenarioCalibration]
    verdict: str
    verdict_reasons: tuple[str, ...]
    excluded_gate: str
    tape_digest: str
    flags: tuple[str, ...]
    workers: int = 1
    estimable_nulls: tuple[str, ...] = ()
    registry: Mapping[str, Any] = field(default_factory=dict)
    interpretation_limits: str = (
        "Every input is produced by market_propagation.simulation. This certifies the "
        "behaviour of a decision rule on a declared synthetic process. It is not an "
        "empirical finding about any real venue, release or contract."
    )

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "seed": self.seed,
            "repetitions": self.repetitions,
            "n_releases": self.n_releases,
            "minimum_mae_gain": self.minimum_mae_gain,
            "false_positive_ceiling": self.false_positive_ceiling,
            "target_power": self.target_power,
            "bootstrap_samples": self.bootstrap_samples,
            "workers": self.workers,
            "null_scenarios": list(self.null_scenarios),
            "recovery_scenarios": list(self.recovery_scenarios),
            "scenarios": {name: result.as_dict() for name, result in self.scenarios.items()},
            "verdict": self.verdict,
            "verdict_reasons": list(self.verdict_reasons),
            "decision_rule": {
                "gates": [
                    "common_sample_and_cutoff_clean",
                    f"mae_gain >= {self.minimum_mae_gain}",
                    "release_clustered_one_sided_lower_bound > 0",
                ],
                "excluded_gate": self.excluded_gate,
                "excluded_gate_reason": (
                    "the internal null-assessment gate is the object of this calibration, so "
                    "requiring it per repetition would assert the conclusion being tested"
                ),
            },
            "simultaneous_bounds": {
                "method": "clopper-pearson one-sided, level divided across null scenarios",
                "null_scenarios_declared": len(self.null_scenarios),
                "null_scenarios_estimable": list(self.estimable_nulls),
                "level": (
                    None
                    if not self.estimable_nulls
                    else 1.0 - (1.0 - float(RATE_CONFIDENCE)) / len(self.estimable_nulls)
                ),
            },
            "tape_digest": self.tape_digest,
            "flags": list(self.flags),
            "registry": dict(self.registry),
            "design_notes": list(DESIGN_NOTES),
            "interpretation_limits": self.interpretation_limits,
        }


def _settings() -> historical_forecast.ForecastSettings:
    """The study's declared source-time forecast origin, lag guard and horizon."""
    return historical_forecast.ForecastSettings(
        forecast_origin_seconds=300, lag_guard_seconds=60, future_horizon_seconds=300
    )


def declared_scenarios() -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Split the registered scenarios by their own declared mechanism.

    A scenario that declares a communication edge is an alternative the rule should
    recover; one that declares none is a null the rule should not promote on. The
    split reads the spec rather than a hand-kept list, so a scenario cannot be
    reclassified by editing the calibration.
    """
    names = scenario_names()
    nulls = tuple(name for name in names if not SCENARIOS[name].communication)
    recovery = tuple(name for name in names if SCENARIOS[name].communication)
    return nulls, recovery


def _placebo_donors(tapes: simulated_tapes.SimulatedTapes) -> DonorMap:
    """The declared placebo donor: this meeting's other strike, same venue.

    The exposure graph elects exactly one donor per receiver and offers no second
    choice, so the control cannot be a second election. It is instead chosen by
    construction and handed to the panel as a donor map, which lets the panel compute
    the placebo's lagged return through the same anchor, staleness and tie-group logic
    as the real neighbour rather than through a private shortcut.

    The other strike of the same meeting and venue is the placebo because it shares the
    receiver's release, venue and observation quality while carrying the opposite side
    of the same payout rather than the leading information the fast venue holds. The
    certificate records that this donor is declared rather than elected.
    """
    by_id = {contract.contract_id: contract for contract in tapes.contracts}
    donors: dict[str, tuple[str, ...]] = {}
    for contract in tapes.contracts:
        parts = simulated_tapes._split_tape_id(contract.contract_id)
        if parts is None:
            continue
        venue, family, role, meeting = parts
        other = "C1" if role == "C0" else "C0"
        candidate = f"{venue}-{family}-{other}-{meeting:04d}"
        if candidate in by_id:
            donors[contract.contract_id] = (candidate,)
    return DonorMap(donors)


def _design(
    tapes: simulated_tapes.SimulatedTapes,
    *,
    n_releases: int,
) -> tuple[pd.DataFrame, int, int]:
    """Build the study's ladder design for one tape, through the production chain."""
    settings = _settings()
    origin = tapes.releases[0].event_time + dt.timedelta(seconds=300)
    window_end = origin + dt.timedelta(seconds=settings.future_horizon_seconds)
    receivers = tapes.receivers()

    graph = neighbors.build_neighbor_graph(
        tapes.contracts, at=origin, calendar=tapes.calendar, window_end=window_end
    )
    panel = historical_forecast.build_forecast_rows(
        tapes.trades,
        tapes.releases,
        graph,
        receivers=receivers,
        settings=settings,
        venue=tapes.venue,
    )
    primary = pd.DataFrame([dict(row) for row in panel.rows])

    placebo_panel = historical_forecast.build_forecast_rows(
        tapes.trades,
        tapes.releases,
        _placebo_donors(tapes),
        receivers=receivers,
        settings=settings,
        venue=tapes.venue,
    )
    placebo = {
        (str(row["event_id"]), str(row["receiver_contract_id"])): row["neighbor_lag"]
        for row in (dict(entry) for entry in placebo_panel.rows)
    }
    primary["placebo_neighbor_lag"] = [
        placebo.get((str(row["event_id"]), str(row["receiver_contract_id"])))
        for row in primary.to_dict("records")
    ]

    design = study._ladder_frame(primary, surprise=None, control_column="placebo_neighbor_lag")

    # The news control. This process publishes no release package, so the declared
    # common-news terms are the generator's own per-release shock, recovered from its
    # truth-only columns. The transmission mechanism under test never enters here.
    shocks = simulated_tapes.simulated_release_shocks(
        tapes.scenario, seed=tapes.seed, n_events=n_releases
    )
    ordered = [release.event_id for release in sorted(tapes.releases, key=lambda r: r.event_time)]
    primary_shock = {event_id: shocks.get(event_id) for event_id in ordered}
    delayed: dict[str, float | None] = {}
    previous: float | None = None
    for event_id in ordered:
        delayed[event_id] = previous
        previous = primary_shock.get(event_id)
    design["shock"] = pd.to_numeric(
        pd.Series([primary_shock.get(str(e)) for e in primary["event_id"]]), errors="coerce"
    )
    design["delayed_shock"] = pd.to_numeric(
        pd.Series([delayed.get(str(e)) for e in primary["event_id"]]), errors="coerce"
    )
    return design, len(graph.edges), len(primary)


def _repetition(
    scenario: str,
    *,
    seed: int,
    n_releases: int,
    minimum_mae_gain: float,
    bootstrap_samples: int,
) -> RepetitionOutcome:
    """Evaluate the declared decision rule once, on one seed."""
    try:
        tapes = simulated_tapes.simulate_tapes(
            scenario, seed=seed, n_events=n_releases, family=TAPE_FAMILY
        )
    except Exception as error:  # a blocked repetition is a result, never a silent pass
        return RepetitionOutcome(
            scenario=scenario,
            seed=seed,
            status=STATUS_BLOCKED,
            promoted=False,
            reason=f"tape_not_built: {type(error).__name__}: {error}",
        )
    try:
        design, edges, n_rows = _design(tapes, n_releases=n_releases)
        comparison = nested_comparison(
            design,
            kinds=MODEL_KINDS,
            seed=seed,
            minimum_mae_gain=minimum_mae_gain,
        )
        record = comparison.as_record()
    except Exception as error:  # a blocked repetition is a result, never a silent pass
        return RepetitionOutcome(
            scenario=scenario,
            seed=seed,
            status=STATUS_BLOCKED,
            promoted=False,
            reason=f"comparison_not_run: {type(error).__name__}: {error}",
            tape_digest=tapes.digest(),
        )

    promotion = record["promotion"]
    criteria = promotion["criteria_met"]
    gain = float(promotion["mae_reduction"])
    gates_clean = bool(criteria["common_sample_and_cutoff_clean"])
    meets_gain = bool(criteria["meets_minimum_mae_gain"])

    pairs = _release_level_pairs(record)
    if pairs:
        interval = _clustered_gain_interval(
            [{"seed": seed, "release_level_gains": pairs}], seed=seed, samples=bootstrap_samples
        )
    else:
        interval = {"status": "inconclusive", "lower": None, "coverage": None, "reason": "no pairs"}
    lower = interval.get("lower")
    # The decision rule this certificate calibrates: clean gates, the declared
    # minimum point gain, and a release-clustered one-sided lower bound above zero.
    promoted = bool(gates_clean and meets_gain and lower is not None and lower > 0.0)
    return RepetitionOutcome(
        scenario=scenario,
        seed=seed,
        status=STATUS_COMPLETE,
        promoted=promoted,
        gain=gain,
        lower=None if lower is None else float(lower),
        coverage=interval.get("coverage"),
        gates_clean=gates_clean,
        meets_minimum_gain=meets_gain,
        internal_null_gate_ok=bool(criteria["null_assessment_matches_and_ok"]),
        n_test_releases=int(record["sample"].get("n_clusters_test") or 0),
        n_rows=n_rows,
        graph_edges=edges,
        tape_digest=tapes.digest(),
    )


def _repetition_task(payload: tuple[str, int, int, float, int]) -> RepetitionOutcome:
    """One repetition from a plain payload, so it survives being sent to a worker.

    The arguments are primitives rather than a closure, because a worker process gets a
    fresh interpreter and can only be handed something it can rebuild.
    """
    scenario, seed, n_releases, minimum_mae_gain, bootstrap_samples = payload
    return _repetition(
        scenario,
        seed=seed,
        n_releases=n_releases,
        minimum_mae_gain=minimum_mae_gain,
        bootstrap_samples=bootstrap_samples,
    )


def _summarize(
    scenario: str,
    kind: str,
    outcomes: Sequence[RepetitionOutcome],
    *,
    confidence: float,
    ceiling: float,
    target_power: float,
) -> ScenarioCalibration:
    """Turn one scenario's repetitions into a rate and its one-sided bound.

    A repetition that produced no comparison is counted as blocked and excluded from
    the rate rather than as a non-promotion. Counting a blocked run as a negative
    would report a sampling failure as evidence about the rule.
    """
    complete = [outcome for outcome in outcomes if outcome.status == STATUS_COMPLETE]
    blocked = len(outcomes) - len(complete)
    promoted = sum(1 for outcome in complete if outcome.promoted)
    draws = len(complete)
    rate = None if draws == 0 else promoted / draws
    lower, upper = (
        _one_sided_bounds(promoted, draws, confidence=confidence) if draws else (None, None)
    )
    if kind == "null":
        meets = upper is not None and upper <= ceiling
    else:
        meets = lower is not None and lower >= target_power
    reasons: dict[str, int] = {}
    for outcome in outcomes:
        if outcome.status == STATUS_BLOCKED and outcome.reason:
            head = outcome.reason.split(":", 1)[0]
            reasons[head] = reasons.get(head, 0) + 1
    return ScenarioCalibration(
        scenario=scenario,
        kind=kind,
        repetitions=len(outcomes),
        complete=draws,
        blocked=blocked,
        promoted=promoted,
        rate=rate,
        one_sided_upper=upper,
        one_sided_lower=lower,
        confidence=confidence,
        meets_requirement=bool(meets),
        blocked_reasons=reasons,
        outcomes=tuple(outcome.as_dict() for outcome in outcomes),
    )


def calibrate(
    *,
    seed: int = DEFAULT_SEED,
    repetitions: int = DEFAULT_REPETITIONS,
    n_releases: int = DEFAULT_N_RELEASES,
    minimum_mae_gain: float = DEFAULT_MINIMUM_MAE_GAIN,
    false_positive_ceiling: float = DEFAULT_NULL_RATE_CEILING,
    target_power: float = DEFAULT_TARGET_POWER,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    workers: int = 1,
    record_registry: bool = True,
    registry_path: str | Path | None = None,
    run_id: str | None = None,
    output_dir: str | Path | None = None,
    null_scenarios: Sequence[str] | None = None,
    recovery_scenarios: Sequence[str] | None = None,
) -> CalibrationCertificate:
    """Run the declared decision rule over every registered scenario.

    Null scenarios are held to a simultaneous one-sided bound: the confidence level is
    divided across them, so the whole family is bounded at once rather than each
    scenario being tested at the same level. With several nulls, per-scenario bounds
    at the full level would let the family's false-positive rate exceed the ceiling
    while every individual scenario looked clean.
    """
    declared_nulls, declared_recovery = declared_scenarios()
    nulls = tuple(null_scenarios) if null_scenarios is not None else declared_nulls
    recovery = tuple(recovery_scenarios) if recovery_scenarios is not None else declared_recovery
    if not nulls and not recovery:
        raise CalibrationError("no scenario is declared null or recovery; nothing to calibrate")
    if repetitions < 1:
        raise CalibrationError(f"repetitions={repetitions!r} must be at least 1")
    if workers < 1:
        raise CalibrationError(f"workers={workers!r} must be at least 1")

    # Collect every repetition first, because the simultaneous level depends on how many
    # null scenarios can actually be estimated. Dividing the level across scenarios that
    # yield no comparison would widen every bound for nothing.
    tasks: list[tuple[str, int, int, float, int]] = []
    layout: list[tuple[str, str, int, int]] = []
    for kind, names in (("null", nulls), ("recovery", recovery)):
        for scenario in names:
            start = len(tasks)
            for index in range(repetitions):
                tasks.append(
                    (
                        scenario,
                        _repetition_seed(seed, index),
                        n_releases,
                        minimum_mae_gain,
                        bootstrap_samples,
                    )
                )
            layout.append((scenario, kind, start, repetitions))
    # One repetition per task, so the workers balance against each other rather than
    # against a chunk boundary, and the results stay in submission order.
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            outcomes_flat = list(pool.map(_repetition_task, tasks, chunksize=1))
    else:
        outcomes_flat = [_repetition_task(task) for task in tasks]
    collected: dict[str, tuple[str, list[RepetitionOutcome]]] = {
        scenario: (kind, outcomes_flat[start : start + count])
        for scenario, kind, start, count in layout
    }
    tapes_digest = hashlib.sha256()

    estimable = [
        name
        for name in nulls
        if any(outcome.status == STATUS_COMPLETE for outcome in collected[name][1])
    ]
    null_confidence = 1.0 - (1.0 - RATE_CONFIDENCE) / (len(estimable) or 1)

    results: dict[str, ScenarioCalibration] = {}
    for scenario, (kind, outcomes) in collected.items():
        result = _summarize(
            scenario,
            kind,
            outcomes,
            confidence=null_confidence if kind == "null" else RATE_CONFIDENCE,
            ceiling=false_positive_ceiling,
            target_power=target_power,
        )
        results[scenario] = result
        tapes_digest.update(f"{scenario}:{result.complete}".encode())

    verdict_reasons: list[str] = []
    # A null whose observed rate already exceeds the ceiling has failed. A null whose
    # bound is merely too wide to certify has not: it is under-powered, and the plan
    # requires that be reported as inconclusive rather than as a conclusion.
    failed = [
        name
        for name, result in results.items()
        if result.kind == "null"
        and result.rate is not None
        and result.rate > false_positive_ceiling
    ]
    for name in failed:
        result = results[name]
        verdict_reasons.append(
            f"null scenario {name!r} promoted in {result.promoted} of {result.complete} "
            f"complete repetition(s), a rate of {result.rate!r} above the ceiling "
            f"{false_positive_ceiling!r}"
        )
    uncertified = [
        name
        for name, result in results.items()
        if result.kind == "null"
        and not result.meets_requirement
        and result.rate is not None
        and result.rate <= false_positive_ceiling
    ]
    for name in uncertified:
        result = results[name]
        verdict_reasons.append(
            f"null scenario {name!r} is not bounded below {false_positive_ceiling!r} by "
            f"{result.complete} repetition(s): one-sided upper bound "
            f"{result.one_sided_upper!r}; more repetitions are needed to certify it"
        )
    unestimable = [name for name, result in results.items() if result.complete == 0]
    for name in unestimable:
        result = results[name]
        verdict_reasons.append(
            f"scenario {name!r} produced no complete repetition, so it is not estimable: "
            f"{dict(result.blocked_reasons)}"
        )
    underpowered = [
        name
        for name, result in results.items()
        if result.kind == "recovery" and not result.meets_requirement
    ]
    for name in underpowered:
        result = results[name]
        verdict_reasons.append(
            f"recovery scenario {name!r} is not bounded above {target_power!r}: "
            f"rate={result.rate!r}, one-sided lower bound={result.one_sided_lower!r} "
            f"over {result.complete} complete repetition(s)"
        )
    partly_blocked = [
        name for name, result in results.items() if result.blocked and result.complete > 0
    ]
    for name in partly_blocked:
        verdict_reasons.append(
            f"scenario {name!r} has {results[name].blocked} blocked repetition(s) beside "
            f"{results[name].complete} complete one(s): {dict(results[name].blocked_reasons)}"
        )

    if failed:
        verdict = VERDICT_FAIL
    elif uncertified or unestimable or underpowered or partly_blocked or not results:
        verdict = VERDICT_INCONCLUSIVE
    else:
        verdict = VERDICT_PASS

    flags = {
        "synthetic_process",
        "decision_rule_calibrated_end_to_end",
        "internal_null_gate_excluded_as_circular",
        "null_family_uses_simultaneous_bound",
        "donor_meeting_attribution_is_declared",
        "control_donor_is_declared_not_elected",
    }
    if any(result.blocked for result in results.values()):
        flags.add("some_repetitions_blocked")
    if any(result.complete == 0 for result in results.values()):
        flags.add("some_scenarios_not_estimable")
    if any(
        outcome.get("internal_null_gate_ok")
        for result in results.values()
        for outcome in result.outcomes
    ):
        flags.add("internal_null_gate_cleared_at_least_once")

    certificate = CalibrationCertificate(
        version=CALIBRATION_VERSION,
        generated_at=dt.datetime.now(dt.UTC).isoformat(),
        seed=int(seed),
        repetitions=int(repetitions),
        n_releases=int(n_releases),
        minimum_mae_gain=float(minimum_mae_gain),
        false_positive_ceiling=float(false_positive_ceiling),
        target_power=float(target_power),
        bootstrap_samples=int(bootstrap_samples),
        null_scenarios=nulls,
        recovery_scenarios=recovery,
        scenarios=results,
        verdict=verdict,
        verdict_reasons=tuple(verdict_reasons),
        excluded_gate="null_assessment_matches_and_ok",
        tape_digest=tapes_digest.hexdigest(),
        flags=tuple(sorted(flags)),
        workers=int(workers),
        estimable_nulls=tuple(estimable),
    )
    if not record_registry:
        return certificate
    return replace(
        certificate,
        registry=_registry_section(
            certificate,
            output_dir=output_dir,
            registry_path=registry_path,
            run_id=run_id,
        ),
    )


def _digest_json(payload: Any) -> str:
    """Content hash of a provenance payload, stable under key order."""
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def shared_registry_path() -> Path:
    """The one ledger file every calibration run records into."""
    return _repository_root() / SHARED_REGISTRY_DIR / REGISTRY_DB_NAME


def _registry_section(
    certificate: CalibrationCertificate,
    *,
    output_dir: str | Path | None,
    registry_path: str | Path | None,
    run_id: str | None,
) -> dict[str, Any]:
    """Record the calibration durably, or state why it was not recorded.

    The record is stamped ``synthetic`` because every input is generated. A ledger that
    held this beside an empirical run without that stamp would invite a calibration to be
    read as a measurement of a real market. A registry failure is reported beside the
    certificate rather than replacing it: losing the certificate because the ledger could
    not be written would discard the evidence the ledger exists to point at.
    """
    spec_hash = _digest_json(
        {
            "version": certificate.version,
            "seed": certificate.seed,
            "repetitions": certificate.repetitions,
            "n_releases": certificate.n_releases,
            "minimum_mae_gain": certificate.minimum_mae_gain,
            "false_positive_ceiling": certificate.false_positive_ceiling,
            "target_power": certificate.target_power,
            "bootstrap_samples": certificate.bootstrap_samples,
        }
    )
    data_hash = _digest_json(
        {
            "tape_digest": certificate.tape_digest,
            "complete": {
                name: result.complete for name, result in sorted(certificate.scenarios.items())
            },
        }
    )
    source_hash = _digest_json(
        {
            "version": certificate.version,
            "null_scenarios": list(certificate.null_scenarios),
            "recovery_scenarios": list(certificate.recovery_scenarios),
            "excluded_gate": certificate.excluded_gate,
            "flags": list(certificate.flags),
        }
    )
    environment_hash = _digest_json({"python": platform.python_version(), "pandas": pd.__version__})
    resolved_run_id = run_id or f"calibration-{spec_hash[:12]}-{data_hash[:12]}"

    metrics: dict[str, Any] = {
        "verdict": certificate.verdict,
        "repetitions": certificate.repetitions,
        "n_releases": certificate.n_releases,
        "scenarios": len(certificate.scenarios),
        "estimable_nulls": len(certificate.estimable_nulls),
        "verdict_reason_count": len(certificate.verdict_reasons),
    }
    for name, result in sorted(certificate.scenarios.items()):
        metrics[f"{name}_complete"] = result.complete
        metrics[f"{name}_promoted"] = result.promoted
        if result.rate is not None:
            metrics[f"{name}_rate"] = result.rate
        if result.one_sided_upper is not None:
            metrics[f"{name}_one_sided_upper"] = result.one_sided_upper
        if result.one_sided_lower is not None:
            metrics[f"{name}_one_sided_lower"] = result.one_sided_lower

    path = Path(registry_path) if registry_path is not None else shared_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    section: dict[str, Any] = {
        "run_id": resolved_run_id,
        "spec_hash": spec_hash,
        "data_hash": data_hash,
        "source_hash": source_hash,
        "environment_hash": environment_hash,
    }
    try:
        with ExperimentRegistry(path) as registry:
            registry.record_run(
                {
                    "run_id": resolved_run_id,
                    "spec_hash": spec_hash,
                    "data_hash": data_hash,
                    "source_hash": source_hash,
                    "environment_hash": environment_hash,
                    "event_ids": sorted(certificate.scenarios),
                    "seed": certificate.seed,
                    "metrics": metrics,
                    "synthetic": True,
                }
            )
            if output_dir is not None:
                registry.export_jsonl(Path(output_dir) / f"{resolved_run_id}.jsonl")
        section.update({"recorded": True, "path": str(path), "shared": registry_path is None})
    except Exception as error:  # a missing ledger record is reported, never defaulted
        section["recorded"] = False
        section["reason"] = (
            f"the run was not recorded: {type(error).__name__}: {error}; the certificate is "
            "still written, and the missing record is stated rather than implied"
        )
    return section


def write_certificate(certificate: CalibrationCertificate, path: str | Path) -> Path:
    """Write the certificate by replacing a temporary file, never a partial one."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(
        json.dumps(certificate.as_dict(), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)
    return target
