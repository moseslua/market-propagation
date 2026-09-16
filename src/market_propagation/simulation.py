"""Adversarial simulator with known ground truth.

Every scenario produces the frozen forecast-table contract described in the
implementation contract: one row per ``(event_id, contract_id,
prediction_time, horizon_seconds)`` whose columns are exactly the declared
forecast columns in :mod:`market_propagation.storage`, in declared order.

Those columns carry every predictor the fitted ladder reads, including the
``neighbor_lag`` family, and every column admissibility needs: ``cluster_id``
(the release the row belongs to), ``cohort``, ``orientation_sign`` and
``exclusion_reason``. The truth-only quantities listed in
:data:`TRUTH_ONLY_COLUMNS` are appended after the declared columns and are never
read as features. ``target`` is the future change in the target venue's midprice
between ``prediction_time`` and ``prediction_time + horizon_seconds``; it is
never a terminal payout, and no feature column is a function of it.

The generator encodes explicit observed shocks, source/receipt delays,
observation sparsity, dropped messages and forecast cutoffs. The
common-news-plus-delay null has no communication at all: two venues observe
one common latent with different source delays and update rates, so the fast
venue appears to lead. News and network forecasts see the same delayed
direct-response covariate and the same contemporaneous target price, so the
network model can only earn its place by adding admissible lagged neighbour
information.

Both the null and the communication scenario give the fast venue the same
persistent, release-created private repricing, drawn from one shared random
stream. The null never delivers it to the target venue, so the neighbour's
recent observed change carries no incremental predictive power and any network
edge found there is a false positive. The communication scenario copies that
private level to the target venue after a declared transmission lag that falls
after the forecast cutoff and inside the forecast window, so the fast venue's
observable change before prediction anticipates a movement that reaches the
target afterwards. The drift relaxes on a time scale far longer than the
forecast window, which is what keeps the level available to be delivered
rather than reverting before the copy arrives.

Scenarios are genuinely distinct processes, not aliases. They differ in
communication gain, source delay, update sparsity, drop probability, tick
size, grid step, sensitivity dispersion, reversal, lifecycle pause and spread
behaviour. Scenarios that share nuisance parameters draw them from an
identical random stream, so a controlled comparison isolates the mechanism
rather than the noise. Every scenario is a synthetic software process: the
frames carry an explicit ``synthetic`` marker and scenario results are
falsification and power diagnostics, never empirical evidence about a real
venue.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from .storage import FORECAST_COLUMNS

__all__ = [
    "FORECAST_COLUMNS",
    "NEIGHBOR_COLUMNS",
    "SCENARIOS",
    "TRUTH_ONLY_COLUMNS",
    "ScenarioSpec",
    "primary_target",
    "scenario_names",
    "simulate_quotes",
    "simulate_scenario",
]

#: Admissible neighbour-lag columns, in the order the network model considers.
#: All three are declared forecast columns, so a sealed table carries them.
NEIGHBOR_COLUMNS: tuple[str, ...] = (
    "neighbor_lag",
    "neighbor_lag_complement",
    "neighbor_lag_control",
)

#: Quantity columns the estimator must not read; they exist only as ground truth.
TRUTH_ONLY_COLUMNS: tuple[str, ...] = (
    "latent_target_future_value",
    "latent_move",
    "omitted_common_shock",
    "transmitted_signal",
)

_QUOTE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "cluster_id",
    "family",
    "contract_id",
    "venue",
    "cohort",
    "orientation_sign",
    "event_time",
    "observation_time",
    "source_time",
    "midpoint",
    "bid",
    "ask",
    "spread",
    "valid",
    "exclusion_reason",
    "update_sequence",
    "dropped_previous",
)

_FAST_VENUE = "alpha"
_TARGET_VENUE = "beta"
_CONTROL_VENUE = "gamma"
_HALF_SPREAD = 0.01
_BASE_SENSITIVITY = 0.08
_ABSORB_SECONDS = 45.0
#: Idiosyncratic common-factor innovation per root second, in probability points.
#: Chosen so the common factor wanders by a few tenths of a point over a
#: five-minute window rather than reaching the clip rails.
_INNOVATION_SIGMA = 0.0004
#: Venue-private component of the fast venue: a persistent post-release
#: repricing, drawn from zero at the release. Its relaxation time is far longer
#: than the forecast window, so most of the level the fast venue has already
#: shown is still in place when a declared transmission lag delivers a copy of it
#: to the target. A transient component would be absorbed before the target's
#: next observation, which would leave the neighbour's observed history nothing
#: to say about it. The common factor stays a martingale, so in the null this
#: private level never reaches the target and a neighbour's recent change carries
#: no incremental predictive power even though it appears to lead.
_PRIVATE_TAU_SECONDS = 1500.0
#: Saturation cap on the realised private drift, in probability points.
_PRIVATE_PATH_CLIP = 0.25
#: Declared lag at which a copy of the fast venue's private drift reaches the
#: target venue. It falls after the forecast cutoff and inside the trailing part
#: of the 300s window, so the forecast window contains the arrival rather than
#: starting after it.
_COMMUNICATION_LAG_SECONDS = 300.0
#: Idiosyncratic component of the omitted common factor, which never appears in
#: any feature column.
_OMITTED_INNOVATION_PER_SIGMA = 0.008
_PRIOR_LOW = 0.30
_PRIOR_HIGH = 0.70
#: Independent observation noise on every venue quote, in probability points.
_OBSERVATION_NOISE = 0.002
#: Sentinel for an unavailable truth-only value; never a feature value.
_TRUTH_ONLY_SENTINEL = float("nan")
_HORIZONS: tuple[int, ...] = (60, 300, 900)
_PRE_SECONDS = 150.0
_POST_SECONDS = 1200.0
_FORECAST_DELAY = 30.0
_OWN_LAG_WINDOW = 60.0
_ANCHOR = datetime(2026, 1, 14, 13, 30, tzinfo=UTC)
_EVENT_SPACING = timedelta(days=30)
_CLOCK_UNCERTAINTY_SECONDS = 0.25
#: Floor for the staleness gate. The gate itself scales with the venue's own
#: update spacing and drop rate, so coarse feeds stay usable.
_MIN_STALE_LIMIT_SECONDS = 300.0


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    """Parameters of one simulated process."""

    name: str
    description: str
    communication: bool = False
    gain: float = 0.0
    communication_lag: float = _COMMUNICATION_LAG_SECONDS
    communication_tau: float = 45.0
    private_sigma: float = 0.0
    delay_alpha: float = 0.5
    delay_beta: float = 4.0
    gap_alpha: float = 2.0
    gap_beta: float = 20.0
    drop_alpha: float = 0.0
    drop_beta: float = 0.0
    tick: float = 0.01
    grid_step: float = 1.0
    sensitivity_spread: float = 0.12
    omitted_shock_sigma: float = 0.0
    news_active: bool = True
    complement_orientation: float = 1.0
    reversal_fraction: float = 0.0
    reversal_time: float = 420.0
    reversal_tau: float = 180.0
    spread_multiplier: float = 1.0
    spread_window: tuple[float, float] = (-30.0, 120.0)
    pause: tuple[float, float] | None = None
    pause_jump: float = 0.0
    rule_mismatch: bool = False
    note: str = ""


SCENARIOS: Mapping[str, ScenarioSpec] = {
    spec.name: spec
    for spec in (
        ScenarioSpec(
            name="shared_news_delay",
            description=(
                "Null: one common martingale latent price, two venues observing it with "
                "different source delays and update rates. The fast venue additionally carries "
                "a persistent private repricing that never reaches the target venue, so a "
                "genuine lead exists with no information passing between them."
            ),
            private_sigma=0.15,
            note=(
                "The fast venue refreshes every ~2s with a 0.5s source delay; the target "
                "venue every ~20s with a 4s delay, so it appears to lead even though the "
                "common factor is a martingale. The fast venue's private repricing is drawn "
                "for this scenario too, with the same amplitude and the same per-event draws "
                "as the communication scenario, so the two differ only by the declared "
                "transmission edge; here no copy of it is ever delivered to the target, so "
                "the neighbour's recent change has no incremental predictive power."
            ),
        ),
        ScenarioSpec(
            name="communication",
            description=(
                "The null structure plus a genuine delayed transmission from the fast venue "
                "to the target venue, carried by the fast venue's own persistent private "
                "component and arriving after the forecast cutoff, inside the forecast window."
            ),
            communication=True,
            gain=0.9,
            communication_lag=_COMMUNICATION_LAG_SECONDS,
            communication_tau=45.0,
            private_sigma=0.15,
            note=(
                "Identical per-event draws for the common shock, prior, sensitivity, private "
                "component and update schedule, so the only added structure is the edge that "
                "copies the fast venue's private repricing to the target. The private "
                "component has a 1500s relaxation time, so the level the fast venue has "
                "already shown at the +60s cutoff is still largely in place at the +300s "
                "declared lag: the target venue's +60s to +360s change contains an arrival "
                "that the fast venue's observed +0s to +60s change already anticipates, while "
                "the target venue's own observed history and the shared release cannot."
            ),
        ),
        ScenarioSpec(
            name="heterogeneous_sensitivity",
            description=(
                "No communication; contracts differ widely in news sensitivity and "
                "relaxation speed, so pooling them without payoff and sensitivity controls "
                "mixes opposite dynamics."
            ),
            sensitivity_spread=0.45,
        ),
        ScenarioSpec(
            name="omitted_shock",
            description=(
                "No communication, but a second common factor moves both venues and is "
                "absent from every feature column, so apparent cross-prediction survives any "
                "conditioning set built from the table."
            ),
            omitted_shock_sigma=0.06,
        ),
        ScenarioSpec(
            name="opposing_sign",
            description=(
                "No communication; the event carries an exact complementary pair, so the two "
                "contracts move in opposite directions on the same shock and any pooled "
                "signed response cancels."
            ),
            sensitivity_spread=0.18,
            complement_orientation=-1.0,
        ),
        ScenarioSpec(
            name="spread_only",
            description=(
                "The latent value does not move and spreads widen by 3x around the event, so "
                "midpoint responses must stay near zero."
            ),
            news_active=False,
            spread_multiplier=3.0,
        ),
        ScenarioSpec(
            name="later_reversal",
            description=(
                "An initial news response is later reversed beyond its starting level by "
                "subsequent information, so finite-horizon endpoints flip sign."
            ),
            reversal_fraction=1.6,
            reversal_time=420.0,
        ),
        ScenarioSpec(
            name="rule_mismatch",
            description=(
                "No communication, near-synchronous venues and heavy target-venue message "
                "loss, with candidate pairs whose titles match but whose rule fields differ "
                "alongside one genuine cross-venue equivalent."
            ),
            delay_alpha=0.5,
            delay_beta=1.0,
            gap_beta=8.0,
            drop_beta=0.3,
            rule_mismatch=True,
        ),
        ScenarioSpec(
            name="resolution_pause",
            description=(
                "The target venue halts between +300s and +900s and catches up with a jump on "
                "resume, so rows whose window spans the halt are invalid rather than filled."
            ),
            pause=(300.0, 900.0),
            pause_jump=0.03,
        ),
        ScenarioSpec(
            name="coarse_sampling",
            description=(
                "Thirty-second grid and a coarse tick, so measurement resolution is far below "
                "the one-second scenarios and short-horizon responses are mostly "
                "quantization noise."
            ),
            grid_step=30.0,
            tick=0.05,
            gap_alpha=30.0,
            gap_beta=60.0,
        ),
        ScenarioSpec(
            name="dropped_messages",
            description=(
                "Null dynamics with 60% of the target venue's updates dropped, so it observes "
                "the common latent only intermittently."
            ),
            drop_beta=0.6,
            gap_beta=10.0,
        ),
    )
}


def _check_column_contract() -> None:
    """Guard this module's own metadata against a frozen-column drift at import."""
    missing = sorted(set(NEIGHBOR_COLUMNS) - set(FORECAST_COLUMNS))
    if missing:
        raise RuntimeError(
            "every NEIGHBOR_COLUMNS entry must be a declared forecast column, because the "
            f"network model reads them from the sealed table; missing {missing}"
        )
    if set(FORECAST_COLUMNS) & set(TRUTH_ONLY_COLUMNS):
        raise RuntimeError("a truth-only column is also a frozen forecast column")
    if len(set(FORECAST_COLUMNS)) != len(FORECAST_COLUMNS):
        raise RuntimeError("FORECAST_COLUMNS contains a duplicate")


_check_column_contract()


def scenario_names() -> tuple[str, ...]:
    """Registered scenario names, sorted, for tests and report registries."""
    return tuple(sorted(SCENARIOS))


def _spec(name: str) -> ScenarioSpec:
    try:
        return SCENARIOS[name]
    except KeyError:
        raise ValueError(
            f"unknown scenario {name!r}; registered scenarios are {scenario_names()}"
        ) from None


def _as_seconds(value: Any, *, name: str) -> float:
    seconds = float(value)
    if not math.isfinite(seconds):
        raise ValueError(f"{name}={value!r} must be finite")
    return seconds


def _as_integer(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise ValueError(f"{name}={value!r} must be an integer")
    return int(value)


def _event_time(index: int) -> datetime:
    return _ANCHOR + index * _EVENT_SPACING


def _family(index: int) -> str:
    return "cpi" if index % 2 == 0 else "employment"


def _contract_rules(spec: ScenarioSpec) -> tuple[dict[str, Any], ...]:
    """Payoff rules of the simulated contracts, per spec rather than per scenario name."""
    complement = spec.complement_orientation < 0.0
    return (
        {
            "role": "C0",
            "venue": _TARGET_VENUE,
            "cohort": "downstream",
            "operator": "above",
            "threshold": 0.55,
            "orientation_sign": 1.0,
        },
        {
            "role": "C1",
            "venue": _TARGET_VENUE,
            "cohort": "complement" if complement else "downstream",
            "operator": "below" if complement else "at_least",
            "threshold": 0.30,
            "orientation_sign": spec.complement_orientation,
        },
        {
            "role": "G0",
            "venue": _CONTROL_VENUE,
            "cohort": "control",
            "operator": "above",
            "threshold": 0.50,
            "orientation_sign": 0.0,
        },
    )


def _normalize_kwargs(
    spec: ScenarioSpec,
    *,
    seed: int,
    n_events: int,
    horizons: Sequence[int] | None,
    pre_seconds: float | None,
    post_seconds: float | None,
    forecast_delay: float | None,
    own_lag_window: float | None,
) -> tuple[int, int, tuple[int, ...], float, float, float, float]:
    resolved_events = _as_integer(n_events, name="n_events")
    if resolved_events < 1:
        raise ValueError(f"n_events={n_events!r} must be at least 1")
    resolved_horizons = tuple(
        _as_integer(h, name="horizons") for h in (_HORIZONS if horizons is None else horizons)
    )
    if not resolved_horizons or any(h <= 0 for h in resolved_horizons):
        raise ValueError(f"horizons={resolved_horizons!r} must be positive seconds")
    pre = _PRE_SECONDS if pre_seconds is None else _as_seconds(pre_seconds, name="pre_seconds")
    post = _POST_SECONDS if post_seconds is None else _as_seconds(post_seconds, name="post_seconds")
    delay = (
        _FORECAST_DELAY
        if forecast_delay is None
        else _as_seconds(forecast_delay, name="forecast_delay")
    )
    lag_window = (
        _OWN_LAG_WINDOW
        if own_lag_window is None
        else _as_seconds(own_lag_window, name="own_lag_window")
    )
    if lag_window <= 0.0:
        raise ValueError("own_lag_window must be positive")
    if pre <= lag_window:
        raise ValueError("pre_seconds must exceed own_lag_window")
    if delay + lag_window > pre:
        raise ValueError("pre_seconds must cover the forecast delay plus the own-lag window")
    if post < max(resolved_horizons) + delay:
        raise ValueError(
            f"post_seconds={post!r} must cover the longest horizon plus the forecast delay"
        )
    if spec.grid_step <= 0.0:
        raise ValueError(f"scenario {spec.name!r} has a non-positive grid step")
    if spec.tick <= 0.0:
        raise ValueError(f"scenario {spec.name!r} has a non-positive tick size")
    return (
        _as_integer(seed, name="seed"),
        resolved_events,
        resolved_horizons,
        pre,
        post,
        delay,
        lag_window,
    )


def _update_offsets(
    rng: np.random.Generator,
    *,
    gap: float,
    drop: float,
    pre: float,
    post: float,
    step: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Sparse update schedule on the sampling grid, with dropped messages.

    Exponential intervals are built from uniform draws and an explicit
    ``-gap * log(u)`` transform, so the number of random values consumed does
    not depend on ``gap`` or ``drop``. That keeps the random stream aligned
    across scenarios that share nuisance parameters.
    """
    if gap <= 0.0:
        raise ValueError("update gap must be positive")
    if not 0.0 <= drop < 1.0:
        raise ValueError(f"drop probability {drop!r} must lie in [0, 1)")
    count = int((pre + post) / max(min(gap, step), step * 0.5)) + 32
    uniform = rng.random(count)
    intervals = -gap * np.log(np.clip(uniform, 1e-12, None))
    times = -pre + np.cumsum(intervals)
    kept = rng.random(count) >= drop
    times = times[kept]
    times = times[(times >= -pre) & (times <= post)]
    limit = round((pre + post) / step)
    indices = np.unique(np.rint((times + pre) / step).astype(np.int64))
    indices = indices[(indices >= 0) & (indices <= limit)]
    dropped = np.zeros(indices.shape, dtype=bool)
    if indices.size > 1:
        dropped[1:] = np.diff(indices) > 1
    return indices, dropped


def _observe(
    rng: np.random.Generator,
    *,
    latent: np.ndarray,
    offsets: np.ndarray,
    update_indices: np.ndarray,
    delay: float,
    tick: float,
    noise_sigma: float,
) -> np.ndarray:
    """Sample the latent at each update time minus the venue's source delay."""
    source_time = offsets[update_indices] - delay
    source_index = np.searchsorted(offsets, source_time, side="right") - 1
    source_index = np.clip(source_index, 0, latent.shape[0] - 1)
    values = latent[source_index] + noise_sigma * rng.standard_normal(update_indices.shape)
    values = np.clip(np.rint(values / tick) * tick, 0.0, 1.0)
    return np.round(values, 8)


def _asof(
    *,
    update_indices: np.ndarray,
    update_values: np.ndarray,
    grid_index: int,
) -> tuple[float | None, int | None]:
    """Latest observed value at or before ``grid_index``; never look ahead."""
    position = int(np.searchsorted(update_indices, grid_index, side="right")) - 1
    if position < 0:
        return None, None
    return float(update_values[position]), int(update_indices[position])


def _absorb(offsets: np.ndarray, tau: float) -> np.ndarray:
    return 1.0 - np.exp(-np.clip(offsets, 0.0, None) / tau)


def _private_drift(
    offsets: np.ndarray,
    draws: np.ndarray,
    *,
    phi: float,
    sigma: float,
    clip: float = _PRIVATE_PATH_CLIP,
) -> np.ndarray:
    """Persistent fast-venue-only repricing, created by the release.

    The path is zero before the release and relaxes afterwards with ``phi``, so
    the level it reaches is still largely in place far beyond the forecast
    window. ``draws`` is always supplied by the caller, including for scenarios
    with no private component, which keeps every scenario on one random stream.
    """
    path = np.zeros(offsets.shape, dtype=np.float64)
    if sigma <= 0.0:
        return path
    shock_scale = sigma * math.sqrt(1.0 - phi * phi)
    for position in range(1, offsets.size):
        if offsets[position] < 0.0:
            continue
        path[position] = phi * path[position - 1] + shock_scale * draws[position]
    return np.clip(path, -clip, clip)


def _simulate_event(
    spec: ScenarioSpec,
    *,
    seed: int,
    index: int,
    horizons: Sequence[int],
    pre: float,
    post: float,
    forecast_delay: float,
    own_lag_window: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rng = np.random.default_rng([seed, index])
    step = spec.grid_step
    grid_index = np.arange(round((pre + post) / step) + 1)
    offsets = grid_index * step - pre
    event_id = f"rel-{index:04d}"
    family = _family(index)
    event_time = _event_time(index)
    tau = 40.0 + 20.0 * float(rng.random())
    shock = float(rng.standard_normal())
    rules = _contract_rules(spec)
    shared_noise = _OBSERVATION_NOISE
    omitted_sigma = spec.omitted_shock_sigma * _OMITTED_INNOVATION_PER_SIGMA
    omitted_path = np.zeros(offsets.shape, dtype=np.float64)
    if spec.omitted_shock_sigma:
        omitted_path = (
            np.cumsum(rng.standard_normal(offsets.shape)) * omitted_sigma * math.sqrt(step)
        )
        omitted_path = omitted_path - omitted_path[0]

    prior: dict[str, float] = {}
    sensitivity: dict[str, float] = {}
    latent_move: dict[str, float] = {}
    latents: dict[str, np.ndarray] = {}
    fast_latents: dict[str, np.ndarray] = {}
    private_path: dict[str, np.ndarray] = {}
    response = _absorb(offsets, tau)
    if spec.reversal_fraction:
        late = np.clip(offsets - spec.reversal_time, 0.0, None)
        decay = 1.0 - spec.reversal_fraction * (1.0 - np.exp(-late / spec.reversal_tau))
        response = np.where(offsets < spec.reversal_time, response, response * decay)
    common_innovation = (
        np.cumsum(rng.standard_normal(offsets.shape)) * _INNOVATION_SIGMA * math.sqrt(step)
    )
    common_innovation = common_innovation - common_innovation[0]
    for rule in rules:
        role = rule["role"]
        orientation = rule["orientation_sign"]
        prior[role] = _PRIOR_LOW + float(rng.random()) * (_PRIOR_HIGH - _PRIOR_LOW)
        dispersion = float(rng.standard_normal())
        if orientation == 0.0 or not spec.news_active:
            sensitivity[role] = 0.0
        else:
            sensitivity[role] = _BASE_SENSITIVITY * (1.0 + spec.sensitivity_spread * dispersion)
        news = orientation * sensitivity[role] * shock
        latent_move[role] = news
        # Drawn for every contract in every scenario so that scenarios sharing
        # nuisance parameters stay on one random stream.
        draws = rng.standard_normal(offsets.shape)
        if orientation == 0.0 or not spec.news_active:
            # The control contract carries no private information, which keeps it
            # a clean negative control for the placebo and sensitivity suite.
            private_path[role] = np.zeros(offsets.shape, dtype=np.float64)
        else:
            private_path[role] = _private_drift(
                offsets,
                draws,
                phi=math.exp(-step / _PRIVATE_TAU_SECONDS),
                sigma=spec.private_sigma,
            )

    for rule in rules:
        role = rule["role"]
        news = latent_move[role]
        common = prior[role] + news * response + omitted_path + common_innovation
        # The fast venue quotes the common factor plus its own private component.
        fast_latents[role] = np.clip(common + private_path[role], 0.02, 0.98)
        # The target venue sees only the common factor until transmission below.
        latents[role] = np.clip(common, 0.02, 0.98)

    alpha_indices, alpha_dropped = _update_offsets(
        rng, gap=spec.gap_alpha, drop=spec.drop_alpha, pre=pre, post=post, step=step
    )
    beta_indices, beta_dropped = _update_offsets(
        rng, gap=spec.gap_beta, drop=spec.drop_beta, pre=pre, post=post, step=step
    )

    alpha_values: dict[str, np.ndarray] = {}
    for rule in rules:
        role = rule["role"]
        alpha_values[role] = _observe(
            rng,
            latent=fast_latents[role],
            offsets=offsets,
            update_indices=alpha_indices,
            delay=spec.delay_alpha,
            tick=spec.tick,
            noise_sigma=shared_noise,
        )

    transmitted: dict[str, np.ndarray] = {
        rule["role"]: np.zeros(offsets.shape, dtype=np.float64) for rule in rules
    }
    if spec.communication:
        # Transmission carries the fast venue's private component into the target
        # venue after a delay, which is what makes the fast venue's recent change
        # informative about the target venue's future beyond the shared release.
        lag_steps = max(1, round(spec.communication_lag / step))
        shifted_index = np.clip(grid_index - lag_steps, 0, offsets.size - 1)
        ramp = _absorb(offsets - spec.communication_lag, spec.communication_tau)
        ramp = np.where(offsets >= spec.communication_lag, ramp, 0.0)
        for rule in rules:
            if rule["orientation_sign"] == 0.0:
                # No transmission edge terminates at the control contract.
                continue
            role = rule["role"]
            carried = np.where(grid_index >= lag_steps, private_path[role][shifted_index], 0.0)
            transmitted[role] = spec.gain * ramp * carried
            latents[role] = np.clip(latents[role] + transmitted[role], 0.02, 0.98)

    if spec.pause is not None:
        pause_start, pause_end = spec.pause
        inside = (offsets >= pause_start) & (offsets <= pause_end)
        beta_indices = beta_indices[~inside[beta_indices]]
        resume_index = round((pause_end + pre) / step)
        beta_indices = np.sort(np.unique(np.append(beta_indices, resume_index)))
        if spec.pause_jump:
            latents["C0"] = np.clip(
                latents["C0"] + np.where(offsets >= pause_end, spec.pause_jump, 0.0), 0.02, 0.98
            )

    beta_values: dict[str, np.ndarray] = {}
    for rule in rules:
        beta_values[rule["role"]] = _observe(
            rng,
            latent=latents[rule["role"]],
            offsets=offsets,
            update_indices=beta_indices,
            delay=spec.delay_beta,
            tick=spec.tick,
            noise_sigma=shared_noise,
        )

    control_indices, _control_dropped = _update_offsets(
        rng, gap=spec.gap_alpha, drop=0.0, pre=pre, post=post, step=step
    )
    control_values = _observe(
        rng,
        latent=latents["G0"],
        offsets=offsets,
        update_indices=control_indices,
        delay=spec.delay_beta,
        tick=spec.tick,
        noise_sigma=0.004,
    )

    wide = (offsets >= spec.spread_window[0]) & (offsets <= spec.spread_window[1])
    quote_rows: list[dict[str, Any]] = []
    venue_updates: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]] = {}
    for venue, indices, dropped, values in (
        (_FAST_VENUE, alpha_indices, alpha_dropped, alpha_values),
        (_TARGET_VENUE, beta_indices, beta_dropped, beta_values),
    ):
        for rule in rules:
            if rule["cohort"] == "control":
                continue
            role = rule["role"]
            observed = values[role]
            spread = 2.0 * _HALF_SPREAD * np.where(wide[indices], spec.spread_multiplier, 1.0)
            venue_updates[(venue, role)] = (indices, observed)
            for position, grid_point in enumerate(indices):
                offset = float(offsets[grid_point])
                halted = spec.pause is not None and spec.pause[0] < offset < spec.pause[1]
                quote_rows.append(
                    {
                        "event_id": event_id,
                        "cluster_id": event_id,
                        "family": family,
                        "contract_id": f"{venue}-{family}-{role}-{index:04d}",
                        "venue": venue,
                        "cohort": rule["cohort"],
                        "orientation_sign": float(rule["orientation_sign"]),
                        "event_time": event_time,
                        "observation_time": event_time + timedelta(seconds=offset),
                        "source_time": event_time + timedelta(seconds=offset - spec.delay_beta),
                        "midpoint": float(observed[position]),
                        "bid": round(float(observed[position]) - float(spread[position]) / 2.0, 8),
                        "ask": round(float(observed[position]) + float(spread[position]) / 2.0, 8),
                        "spread": float(spread[position]),
                        "valid": not halted,
                        "exclusion_reason": "halted" if halted else None,
                        "update_sequence": int(position),
                        "dropped_previous": bool(dropped[position]),
                    }
                )
    control_rule = next(rule for rule in rules if rule["cohort"] == "control")
    venue_updates[(_CONTROL_VENUE, "G0")] = (control_indices, control_values)
    for position, grid_point in enumerate(control_indices):
        offset = float(offsets[grid_point])
        quote_rows.append(
            {
                "event_id": event_id,
                "cluster_id": event_id,
                "family": family,
                "contract_id": f"{_CONTROL_VENUE}-{family}-G0-{index:04d}",
                "venue": _CONTROL_VENUE,
                "cohort": "control",
                "orientation_sign": float(control_rule["orientation_sign"]),
                "event_time": event_time,
                "observation_time": event_time + timedelta(seconds=offset),
                "source_time": event_time + timedelta(seconds=offset - spec.delay_beta),
                "midpoint": float(control_values[position]),
                "bid": round(float(control_values[position]) - _HALF_SPREAD, 8),
                "ask": round(float(control_values[position]) + _HALF_SPREAD, 8),
                "spread": 2.0 * _HALF_SPREAD,
                "valid": True,
                "exclusion_reason": None,
                "update_sequence": int(position),
                "dropped_previous": False,
            }
        )

    # The prediction timestamp is a fixed delay after the release, and both lagged
    # features are measured over a trailing window that ends there. The window
    # spans the release, so the lagged change reflects the release response
    # itself; measuring it entirely before the event could not carry any
    # post-release information, which is what the forecast is meant to use.
    prediction_index = round((forecast_delay + pre) / step)
    lag_index = prediction_index - max(1, round(own_lag_window / step))
    prediction_time = event_time + timedelta(seconds=forecast_delay)
    rows: list[dict[str, Any]] = []
    for rule in rules:
        role = rule["role"]
        venue = rule["venue"]
        indices, values = venue_updates[(venue, role)]
        neighbor_values = alpha_values.get(role if role != "G0" else "G0")
        if neighbor_values is None:
            neighbor_values = np.full(alpha_indices.shape, _TRUTH_ONLY_SENTINEL)
        current_price, current_index = _asof(
            update_indices=indices, update_values=values, grid_index=prediction_index
        )
        lag_price, _lag_last = _asof(
            update_indices=indices, update_values=values, grid_index=lag_index
        )
        neighbor_price, _neighbor_last = _asof(
            update_indices=alpha_indices, update_values=neighbor_values, grid_index=prediction_index
        )
        neighbor_lag_price, _neighbor_lag_last = _asof(
            update_indices=alpha_indices, update_values=neighbor_values, grid_index=lag_index
        )
        complement_values = alpha_values.get("C1")
        complement_lag, _ = (
            _asof(
                update_indices=alpha_indices,
                update_values=complement_values,
                grid_index=lag_index,
            )
            if complement_values is not None
            else (None, None)
        )
        complement_now, _ = (
            _asof(
                update_indices=alpha_indices,
                update_values=complement_values,
                grid_index=prediction_index,
            )
            if complement_values is not None
            else (None, None)
        )
        control_now, _ = _asof(
            update_indices=control_indices,
            update_values=control_values,
            grid_index=prediction_index,
        )
        control_lag, _ = _asof(
            update_indices=control_indices, update_values=control_values, grid_index=lag_index
        )
        if current_index is None:
            age_seconds = float("inf")
            max_input_available_time = event_time
        else:
            age_seconds = (prediction_index - current_index) * step
            max_input_available_time = event_time + timedelta(seconds=float(offsets[current_index]))
        delayed_shock = (
            shock * (1.0 - math.exp(-age_seconds / _ABSORB_SECONDS))
            if math.isfinite(age_seconds)
            else 0.0
        )
        reasons: list[str] = []
        if current_price is None:
            reasons.append("no_baseline_quote")
        if lag_price is None:
            reasons.append("no_own_lag_quote")
        if neighbor_price is None or neighbor_lag_price is None:
            reasons.append("no_neighbor_lag_quote")
        # Staleness is judged against the venue's own update spacing, so a
        # genuinely coarse or heavily dropped feed is not discarded merely for
        # being coarser than a fine-grained one.
        stale_limit = max(_MIN_STALE_LIMIT_SECONDS, 4.0 * spec.gap_beta * (1.0 + spec.drop_beta))
        if age_seconds > stale_limit:
            reasons.append("stale_baseline")
        for horizon in horizons:
            endpoint_index = prediction_index + round(horizon / step)
            endpoint_price, _endpoint_last = _asof(
                update_indices=indices, update_values=values, grid_index=endpoint_index
            )
            endpoint_time = prediction_time + timedelta(seconds=float(horizon))
            row_reasons = list(reasons)
            if endpoint_price is None:
                row_reasons.append("no_endpoint_quote")
            if endpoint_time > event_time + timedelta(seconds=post):
                row_reasons.append("beyond_simulation_window")
            if spec.pause is not None:
                pause_start_time = event_time + timedelta(seconds=spec.pause[0])
                pause_end_time = event_time + timedelta(seconds=spec.pause[1])
                if prediction_time < pause_end_time and endpoint_time > pause_start_time:
                    row_reasons.append("halted_during_window")
            target = (
                float(endpoint_price) - float(current_price)
                if endpoint_price is not None and current_price is not None
                else _TRUTH_ONLY_SENTINEL
            )
            rows.append(
                {
                    "event_id": event_id,
                    "family": family,
                    "contract_id": f"{venue}-{family}-{role}-{index:04d}",
                    "event_time": event_time,
                    "prediction_time": prediction_time,
                    "horizon_seconds": int(horizon),
                    "target": round(target, 8),
                    "target_available_time": endpoint_time,
                    "max_input_available_time": max_input_available_time,
                    "current_price": (
                        round(float(current_price), 8)
                        if current_price is not None
                        else _TRUTH_ONLY_SENTINEL
                    ),
                    "own_lag": (
                        round(float(current_price) - float(lag_price), 8)
                        if current_price is not None and lag_price is not None
                        else _TRUTH_ONLY_SENTINEL
                    ),
                    "shock": round(shock, 8),
                    "delayed_shock": round(delayed_shock, 8),
                    "neighbor_lag": (
                        round(float(neighbor_price) - float(neighbor_lag_price), 8)
                        if neighbor_price is not None and neighbor_lag_price is not None
                        else _TRUTH_ONLY_SENTINEL
                    ),
                    "neighbor_lag_complement": (
                        round(float(complement_now) - float(complement_lag), 8)
                        if complement_now is not None and complement_lag is not None
                        else _TRUTH_ONLY_SENTINEL
                    ),
                    "neighbor_lag_control": (
                        round(float(control_now) - float(control_lag), 8)
                        if control_now is not None and control_lag is not None
                        else _TRUTH_ONLY_SENTINEL
                    ),
                    "valid": not row_reasons,
                    "cluster_id": event_id,
                    "cohort": rule["cohort"],
                    "orientation_sign": float(rule["orientation_sign"]),
                    "exclusion_reason": ";".join(row_reasons) if row_reasons else None,
                    "latent_target_future_value": float(latents[role][endpoint_index]),
                    "latent_move": float(latent_move[role]),
                    "omitted_common_shock": float(omitted_path[endpoint_index]),
                    "transmitted_signal": float(transmitted[role][endpoint_index]),
                }
            )

    truth = {
        "event_id": event_id,
        "cluster_id": event_id,
        "family": family,
        "event_time": event_time.isoformat(),
        "shock": shock,
        "omitted_common_shock": {
            "sigma": spec.omitted_shock_sigma,
            "path_amplitude": float(np.max(np.abs(omitted_path)) if omitted_path.size else 0.0),
        },
        "tau_seconds": tau,
        "prior": dict(prior),
        "sensitivity": dict(sensitivity),
        "latent_move": dict(latent_move),
        "private_component": {
            "sigma": spec.private_sigma,
            "tau_seconds": _PRIVATE_TAU_SECONDS,
            "clip": _PRIVATE_PATH_CLIP,
            "interpretation": (
                "fast-venue information visible only in the fast venue's quote; present with "
                "the same amplitude and per-event draws in the communication and null "
                "scenarios, and copied to the target venue only where an edge is declared"
            ),
            "path_amplitude": {
                role: float(np.max(np.abs(path)) if path.size else 0.0)
                for role, path in private_path.items()
            },
        },
        "latent": {role: array.tolist() for role, array in latents.items()},
    }
    return rows, quote_rows, truth


def simulate_scenario(
    name: str,
    *,
    seed: int = 20260913,
    n_events: int = 80,
    horizons: Sequence[int] | None = None,
    pre_seconds: float | None = None,
    post_seconds: float | None = None,
    forecast_delay: float | None = None,
    own_lag_window: float | None = None,
) -> pd.DataFrame:
    """Generate the forecast table for one named scenario.

    Returns the frozen forecast-table columns declared in
    ``market_propagation.storage.TABLE_SCHEMAS['forecast']`` -- including the
    neighbour predictors, ``cluster_id``, ``cohort``, ``orientation_sign`` and
    ``exclusion_reason`` -- plus the :data:`TRUTH_ONLY_COLUMNS`.
    ``DataFrame.attrs`` carries:

    ``scenario`` / ``scenario_description``
        The registered name and its stated mechanism.
    ``ground_truth``
        Observed shocks, per-contract news sensitivity, delays, update gaps,
        drop probabilities, tick and grid resolution, communication edges with
        gain and lag, omitted common shock size, opposite-orientation pairs,
        rule-mismatch pairs, control contracts, reversal and pause settings,
        the prediction delay, the horizon grid and the payoff rules.
    ``truth_only_columns``
        Quantities the ground truth carries but the feature columns never do:
        ``latent_target_future_value``, ``latent_move``,
        ``omitted_common_shock`` and ``transmitted_signal``.
    ``controlled_comparison``
        Which nuisance draws are shared across scenarios.
    ``exclusions``
        Counts of rows masked with each exclusion reason.
    ``cohorts`` / ``contract_rules``
        Which venue and contract plays which cohort role.

    Every row satisfies ``max_input_available_time <= prediction_time``, so no
    feature column carries information from after the forecast cutoff.
    """
    spec = _spec(name)
    seed, n_events, resolved_horizons, pre, post, delay, lag_window = _normalize_kwargs(
        spec,
        seed=seed,
        n_events=n_events,
        horizons=horizons,
        pre_seconds=pre_seconds,
        post_seconds=post_seconds,
        forecast_delay=forecast_delay,
        own_lag_window=own_lag_window,
    )
    rows: list[dict[str, Any]] = []
    truth: list[dict[str, Any]] = []
    for index in range(n_events):
        event_rows, _quote_rows, event_truth = _simulate_event(
            spec,
            seed=seed,
            index=index,
            horizons=resolved_horizons,
            pre=pre,
            post=post,
            forecast_delay=delay,
            own_lag_window=lag_window,
        )
        rows.extend(event_rows)
        truth.append(event_truth)
    if not rows:
        raise RuntimeError(
            f"scenario {name!r} produced no forecast rows; check the window and horizon settings"
        )
    frame = pd.DataFrame(rows)
    # Every predictor, cohort and unit column is part of the declared forecast
    # contract, so only the truth-only quantities are added beyond it. They are
    # kept in the frame for audit and are never read as features.
    extras = [name for name in TRUTH_ONLY_COLUMNS if name not in FORECAST_COLUMNS]
    ordered_columns = [*FORECAST_COLUMNS, *extras]
    frame = frame.loc[:, ordered_columns]
    for column in (
        "event_time",
        "prediction_time",
        "target_available_time",
        "max_input_available_time",
    ):
        frame[column] = pd.to_datetime(frame[column], utc=True)
    frame["horizon_seconds"] = frame["horizon_seconds"].astype("int64")
    frame["valid"] = frame["valid"].astype(bool)
    frame = frame.sort_values(
        ["event_time", "contract_id", "horizon_seconds"], kind="stable"
    ).reset_index(drop=True)

    exclusions = frame.loc[~frame["valid"], "exclusion_reason"].value_counts().to_dict()
    frame.attrs["scenario"] = spec.name
    frame.attrs["scenario_description"] = spec.description
    frame.attrs["forecast_columns"] = list(FORECAST_COLUMNS)
    frame.attrs["neighbor_columns"] = list(NEIGHBOR_COLUMNS)
    frame.attrs["truth_only_columns"] = list(TRUTH_ONLY_COLUMNS)
    frame.attrs["controlled_comparison"] = (
        "per-event seeds are shared across scenarios; scenarios declaring identical nuisance "
        "parameters draw an identical shock, sensitivity, private component and update "
        "schedule, so only the declared mechanism differs"
    )
    frame.attrs["exclusions"] = {str(key): int(value) for key, value in exclusions.items()}
    rules = _contract_rules(spec)
    frame.attrs["cohorts"] = {
        "direct": [_FAST_VENUE],
        "downstream": [_TARGET_VENUE],
        "control": [_CONTROL_VENUE],
    }
    frame.attrs["contract_rules"] = [
        {
            "role": rule["role"],
            "venue": rule["venue"],
            "cohort": rule["cohort"],
            "operator": rule["operator"],
            "threshold": rule["threshold"],
            "orientation_sign": rule["orientation_sign"],
        }
        for rule in rules
    ]
    frame.attrs["ground_truth"] = {
        "scenario": spec.name,
        "description": spec.description,
        "note": spec.note,
        "seed": seed,
        "n_events": n_events,
        "horizons_seconds": list(resolved_horizons),
        "pre_seconds": pre,
        "post_seconds": post,
        "forecast_delay_seconds": delay,
        "own_lag_window_seconds": lag_window,
        "common_news": True,
        "communication": bool(spec.communication),
        "communication_edges": (
            [
                {
                    "from_venue": _FAST_VENUE,
                    "from_role": "C0",
                    "to_venue": _TARGET_VENUE,
                    "to_role": rule["role"],
                    "gain": spec.gain,
                    "lag_seconds": spec.communication_lag,
                    "tau_seconds": spec.communication_tau,
                    "arrival_seconds_after_cutoff": spec.communication_lag - delay,
                    "arrival_inside_forecast_window": (
                        delay < spec.communication_lag < delay + max(resolved_horizons)
                    ),
                    "interpretation": "causal transmission inside the simulator only",
                }
                for rule in rules
            ]
            if spec.communication
            else []
        ),
        "direct_news_edges": [
            {
                "shock": "standardized release surprise",
                "targets": [rule["role"] for rule in rules if rule["orientation_sign"] != 0.0],
                "mechanism": "delayed direct response with venue-specific source delay",
            }
        ],
        "omitted_common_shock": {
            "present": bool(spec.omitted_shock_sigma),
            "sigma": spec.omitted_shock_sigma,
            "in_feature_columns": False,
        },
        "opposing_signs": bool(spec.complement_orientation < 0.0),
        "opposite_pairs": (
            [
                {
                    "event_id": record["event_id"],
                    "above_contract_id": f"{_TARGET_VENUE}-{record['family']}-C0-{record['event_id'][-4:]}",
                    "below_contract_id": f"{_TARGET_VENUE}-{record['family']}-C1-{record['event_id'][-4:]}",
                    "orientation_sign": [1.0, -1.0],
                    "news_sensitivity": [record["sensitivity"]["C0"], record["sensitivity"]["C1"]],
                }
                for record in truth
            ]
            if spec.complement_orientation < 0.0
            else []
        ),
        "rule_pairs": _rule_pairs(spec, truth),
        "spread_only": not spec.news_active,
        "spread_multiplier": spec.spread_multiplier,
        "reversal": {
            "present": bool(spec.reversal_fraction),
            "fraction": spec.reversal_fraction,
            "time_seconds": spec.reversal_time,
            "tau_seconds": spec.reversal_tau,
        },
        "pause": (
            None
            if spec.pause is None
            else {
                "start_seconds": spec.pause[0],
                "end_seconds": spec.pause[1],
                "jump": spec.pause_jump,
                "endpoint_in_window_is_invalid": True,
            }
        ),
        "coarse_sampling": {"grid_step_seconds": spec.grid_step, "tick": spec.tick},
        "dropped_messages": {
            "alpha_drop_probability": spec.drop_alpha,
            "beta_drop_probability": spec.drop_beta,
        },
        "delays": {
            "source_delay_alpha_seconds": spec.delay_alpha,
            "source_delay_beta_seconds": spec.delay_beta,
            "clock_uncertainty_seconds": _CLOCK_UNCERTAINTY_SECONDS,
        },
        "update_gap_seconds": {"alpha": spec.gap_alpha, "beta": spec.gap_beta},
        "sensitivity_spread": spec.sensitivity_spread,
        "base_sensitivity": _BASE_SENSITIVITY,
        "per_event_sensitivity": {
            record["event_id"]: {
                role: float(value) for role, value in record["sensitivity"].items()
            }
            for record in truth
        },
        "per_event_prior": {
            record["event_id"]: {role: float(value) for role, value in record["prior"].items()}
            for record in truth
        },
        "primary_target_contracts": [
            f"{_TARGET_VENUE}-{record['family']}-C0-{record['event_id'][-4:]}" for record in truth
        ],
        "control_contracts": [
            f"{_CONTROL_VENUE}-{record['family']}-G0-{record['event_id'][-4:]}" for record in truth
        ],
        "private_component": {
            "sigma": spec.private_sigma,
            "tau_seconds": _PRIVATE_TAU_SECONDS,
            "clip": _PRIVATE_PATH_CLIP,
            "carried_by": _FAST_VENUE,
            "interpretation": (
                "fast-venue information visible only in the fast venue's quote; the "
                "communication and null scenarios draw it with the same amplitude from the "
                "same random stream, and only a scenario with a declared edge copies it to "
                "the target venue"
            ),
        },
        "process": "synthetic_software_simulation",
        "synthetic": True,
        "interpretation_limits": (
            "Every quantity here is generated by this simulator. It is a falsification and "
            "power diagnostic about a stated synthetic process, not an empirical estimate of "
            "real-market information propagation, and no result computed from it is evidence "
            "about any real venue."
        ),
        "generator": "market_propagation.simulation",
        "generator_version": "2",
    }
    return frame


def _rule_pairs(spec: ScenarioSpec, truth: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Candidate contract pairs for the rule audit, or none when out of scope."""
    if not spec.rule_mismatch:
        return []
    pairs: list[dict[str, Any]] = []
    base_rule = {
        "family": "cpi",
        "reference_period": "2026-02",
        "units": "percent_change",
        "source": "bls-cpi",
        "vintage": "initial",
        "rounding": "nearest",
        "timezone": "America/New_York",
        "settlement": "cash",
        "currency": "USD",
        "exceptional_policy": "reject",
        "operator": "above",
    }
    for record in truth:
        family = str(record["family"])
        rule = dict(base_rule, family=family)
        pairs.append(
            {
                "kind": "mismatch",
                "expected_matches": False,
                "difference_fields": ["threshold", "rule_hash", "contract_id"],
                "left": dict(
                    rule,
                    venue="kalshi",
                    contract_id=f"kalshi-{family}-above-030",
                    threshold="0.3",
                    rule_hash=f"rule-{family}-above-030",
                ),
                "right": dict(
                    rule,
                    venue="polymarket",
                    contract_id=f"polymarket-{family}-above-030",
                    threshold="0.4",
                    rule_hash=f"rule-{family}-above-040",
                ),
            }
        )
        pairs.append(
            {
                "kind": "equivalent",
                "expected_matches": True,
                "difference_fields": [],
                "left": dict(
                    rule,
                    venue="kalshi",
                    contract_id=f"kalshi-{family}-above-030",
                    threshold="0.3",
                    rule_hash=f"rule-{family}-above-030",
                ),
                "right": dict(
                    rule,
                    venue="kalshi",
                    contract_id=f"kalshi-{family}-above-030",
                    threshold="0.3",
                    rule_hash=f"rule-{family}-above-030",
                ),
            }
        )
    return pairs


def simulate_quotes(
    name: str,
    *,
    seed: int = 20260913,
    n_events: int = 8,
    pre_seconds: float | None = None,
    post_seconds: float | None = None,
) -> pd.DataFrame:
    """Observation panel of venue quotes, for replay and spread demonstrations.

    One row per venue update, so target-venue staleness, tick quantization,
    dropped updates and a lifecycle halt are visible as they happen. Rows
    halted by a resolution pause carry ``valid=False`` with
    ``exclusion_reason='halted'`` rather than a silently filled quote. This is a
    plain observation table for demonstrations; it is not a source of
    admissible forecast features, and only :func:`simulate_scenario` produces
    the frozen forecast table.
    """
    spec = _spec(name)
    n_events_value = _as_integer(n_events, name="n_events")
    if n_events_value < 1:
        raise ValueError(f"n_events={n_events!r} must be at least 1")
    seed_value = _as_integer(seed, name="seed")
    pre = _PRE_SECONDS if pre_seconds is None else _as_seconds(pre_seconds, name="pre_seconds")
    post = _POST_SECONDS if post_seconds is None else _as_seconds(post_seconds, name="post_seconds")
    quote_rows: list[dict[str, Any]] = []
    for index in range(n_events_value):
        _rows, event_quotes, _truth = _simulate_event(
            spec,
            seed=seed_value,
            index=index,
            horizons=_HORIZONS,
            pre=pre,
            post=post,
            forecast_delay=_FORECAST_DELAY,
            own_lag_window=_OWN_LAG_WINDOW,
        )
        quote_rows.extend(event_quotes)
    frame = pd.DataFrame(quote_rows).loc[:, list(_QUOTE_COLUMNS)]
    for column in ("event_time", "observation_time", "source_time"):
        frame[column] = pd.to_datetime(frame[column], utc=True)
    frame["update_sequence"] = frame["update_sequence"].astype("int64")
    frame["valid"] = frame["valid"].astype(bool)
    frame["dropped_previous"] = frame["dropped_previous"].astype(bool)
    frame["exclusion_reason"] = frame["exclusion_reason"].astype("object")
    frame = frame.sort_values(
        ["event_time", "contract_id", "observation_time"], kind="stable"
    ).reset_index(drop=True)
    frame.attrs["scenario"] = spec.name
    frame.attrs["scenario_description"] = spec.description
    frame.attrs["ground_truth"] = {
        "scenario": spec.name,
        "seed": seed_value,
        "n_events": n_events_value,
        "spread_multiplier": spec.spread_multiplier,
        "spread_window_seconds": list(spec.spread_window),
        "halted_window_seconds": None if spec.pause is None else list(spec.pause),
        "dropped_messages": {"alpha": spec.drop_alpha, "beta": spec.drop_beta},
        "source_delay_seconds": {"alpha": spec.delay_alpha, "beta": spec.delay_beta},
        "clock_uncertainty_seconds": _CLOCK_UNCERTAINTY_SECONDS,
        "process": "synthetic_software_simulation",
        "synthetic": True,
        "generator": "market_propagation.simulation.simulate_quotes",
    }
    return frame


def primary_target(frame: pd.DataFrame, *, cohort: str = "downstream") -> pd.DataFrame:
    """Rows for the prespecified primary target contract(s) of a scenario frame.

    ``cohort='downstream'`` selects the target venue's C0 contract,
    ``'complement'`` the C1 contract and ``'control'`` the control venue's
    contract. Selecting by declared contract role rather than by hand-typed
    identifiers keeps nested comparisons on one fixed sample.
    """
    if cohort not in {"downstream", "complement", "control"}:
        raise ValueError(f"cohort={cohort!r} must be 'downstream', 'complement' or 'control'")
    if "cohort" not in frame.columns:
        raise ValueError("frame has no 'cohort' column; pass a simulate_scenario frame")
    if cohort == "complement":
        selected = frame["cohort"].isin({"complement"}) | (
            frame["contract_id"].str.contains("-C1-", regex=False)
        )
    elif cohort == "control":
        selected = frame["cohort"] == "control"
    else:
        selected = (frame["cohort"] == "downstream") & frame["contract_id"].str.contains(
            "-C0-", regex=False
        )
    result = frame.loc[selected].copy()
    result.attrs.update(frame.attrs)
    if result.empty:
        raise ValueError(f"scenario {frame.attrs.get('scenario')!r} has no rows for {cohort!r}")
    return result.reset_index(drop=True)
