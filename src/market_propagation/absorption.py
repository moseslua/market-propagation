"""Scalar absorption times for one contract's response to one release.

:mod:`market_propagation.trade_panel` answers "how far had the price moved by
horizon ``h``": it writes one row per declared horizon, and the response stays
indexed by that horizon. The programme also needs the scalar the curve implies —
*when* the move was absorbed — and that quantity does not exist anywhere in this
repository yet. Two estimands are defined here, and nothing else.

For a declared terminal horizon ``H``, the terminal reaction and the cumulative
fraction absorbed at horizon ``h``:

``R = p(release + H) - p(release-)``
``A(h) = (p(release + h) - p(release-)) / R``

``h50`` and ``h90`` are the horizons where ``A`` first reaches 0.5 and 0.9,
linearly interpolated between the two declared horizons that bracket the
crossing.

Five decisions are load-bearing.

**``H`` is an argument with no default, because ``H`` defines ``R``.** ``A`` is a
fraction *of the terminal reaction*, so one path yields a different ``A``, a
different ``h50`` and a different ``h90`` for every choice of ``H``. A default
would let a caller read a scalar absorption time without stating which terminal
reaction it was measured against, and two runs that disagreed about absorption
would then disagree over an unstated argument. ``H`` must also be one of the
path's own declared horizons: a terminal reaction read at a horizon the path
never observed is not the measurement being scaled.

**The baseline is a pre-release print, and that is checked rather than
assumed.** ``ForecastSettings`` in :mod:`market_propagation.historical_forecast`
states the trap plainly: a 300s forecast origin with a 300s forward target
measures the five-to-ten-minute increment, not the first five minutes'
absorption. The two differ by the origin's own reaction, so a path whose baseline
sits *at* the release or after it (offset ``>= 0``) is refused by name instead of
being reported as an absorption curve. The baseline is carried once per pair
rather than once per horizon for the same reason: one release instant has one
pre-release baseline, and a path that anchors every horizon separately is a
forward-increment path wearing the absorption column's name.

**A refusal is a named code — never a zero, never an imputed value.** A zero
terminal reaction makes ``A`` undefined, so the pair is refused with
``terminal_response_is_zero_so_the_absorbed_fraction_is_undefined`` instead of
dividing by it. A horizon whose endpoint was never observed carries a NULL
fraction, because an absent print and a genuine unchanged price are different
facts and ``configs/event_windows_v2.yaml`` forbids filling either one with the
other. A level that is already reached at the first observed horizon is a NULL
with that horizon reported beside it as a bound, because clamping the answer to
the first declared horizon would report an unobserved instant as a measurement.

**A path that reverses is reported, not smoothed.** ``A`` moving back against
``R`` is a feature of the tape rather than noise to be repaired, so the reversals
and any overshoot past the terminal reaction are recorded on the estimate and
counted in the panel summary. Nothing here is extrapolated past the declared
horizon set: every crossing is interpolated between two *observed* horizons, and
when a declared horizon between those brackets was never observed the crossing
says so.

**Aggregation follows the repository's independence unit.** That unit is the
release, and cross-venue equivalents sharing a ``cluster_id`` stay together, so
the panel summary aggregates pairs to their release cluster before taking a
median and takes its interval from
:func:`market_propagation.evaluation.clustered_bootstrap` rather than from a
second bootstrap written here. Refusals are counted by reason and stay in the
denominator: every refusal and every unobserved declared horizon is named, so a
reader sees which pairs the reported median rests on.

Nothing here spends money or reads a private source. These functions consume the
rows a panel already measured and never write a value the tape did not carry.
"""

from __future__ import annotations

import itertools
import math
import numbers
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .evaluation import clustered_bootstrap

__all__ = [
    "ABSORPTION_COLUMN_MAP",
    "ABSORPTION_ESTIMAND",
    "DECLARED_FRACTION_TOLERANCE",
    "DEFAULT_COVERAGE",
    "DEFAULT_SAMPLES",
    "DEFAULT_SEED",
    "H50_FRACTION",
    "H90_FRACTION",
    "PRIMARY_FRACTIONS",
    "REASON_PRECEDENCE",
    "REQUIRED_PANEL_COLUMNS",
    "STATUS_ESTIMATED",
    "STATUS_REFUSED",
    "AbsorptionError",
    "AbsorptionEstimate",
    "AbsorptionPath",
    "estimate_absorption",
    "paths_from_panel",
    "summarise_absorption_panel",
]

#: What ``A(h)`` is. Stated once so a consumer reads the fraction's denominator
#: off the result rather than assuming the last observed horizon was used.
ABSORPTION_ESTIMAND = (
    "cumulative_fraction_of_the_terminal_reaction_absorbed_"
    "where_A(h)_equals_(p(release+h)_minus_p(release-_))_divided_by_R_and_R_is_the_response_at_H"
)

#: The fractions the two reported absorption times solve for, in ascending order.
PRIMARY_FRACTIONS: tuple[float, ...] = (0.5, 0.9)

#: The two fractions, named, so the crossing arithmetic reads against the same
#: declaration the payload's keys do rather than against a copied literal.
H50_FRACTION, H90_FRACTION = PRIMARY_FRACTIONS

#: ``A`` is compared against these fractions with an exact-arithmetic tolerance,
#: so a path that lands on the fraction reports the declared horizon itself
#: rather than an interpolation of a numerically equal pair.
DECLARED_FRACTION_TOLERANCE = 1e-12

STATUS_ESTIMATED = "estimated"
STATUS_REFUSED = "refused"

DEFAULT_SEED = 20260913
DEFAULT_SAMPLES = 200
DEFAULT_COVERAGE = 0.95

REASON_PAIR_BASELINE_UNOBSERVED = (
    "baseline_print_was_not_observed_so_there_is_no_pre_release_reference"
)
REASON_BASELINE_NOT_PRE_RELEASE = (
    "baseline_is_not_strictly_before_the_release_so_the_curve_is_a_forward_increment_"
    "rather_than_absorption"
)
REASON_TERMINAL_HORIZON_UNDECLARED = "terminal_horizon_is_not_one_of_the_paths_declared_horizons"
REASON_TERMINAL_HORIZON_UNOBSERVED = "terminal_horizon_response_was_not_observed"
REASON_TERMINAL_RESPONSE_ZERO = "terminal_response_is_zero_so_the_absorbed_fraction_is_undefined"
#: Every observed horizon is at or below the fraction. This happens when the move
#: is already past the fraction at the path's first observation, so the crossing
#: lies in ``(release, first observed horizon]`` and the declared horizon set
#: cannot locate it. The bound is reported instead of a number: interpolating
#: against the release instant would anchor the curve at an instant the panel
#: never measured.
REASON_REACHED_BEFORE_FIRST_OBSERVED_HORIZON = (
    "the_fraction_is_already_reached_at_the_first_observed_horizon_so_only_a_bound_is_available"
)
REASON_NON_POSITIVE_HORIZON = (
    "a_non_positive_terminal_horizon_is_the_baseline_instant_and_not_a_response_horizon"
)
REASON_NO_CLUSTER_YIELDS_AN_ABSORPTION_TIME = "no_release_cluster_yields_an_absorption_time"
REASON_TOO_FEW_RELEASE_CLUSTERS = (
    "fewer_than_two_release_clusters_so_no_cluster_robust_interval_is_identified"
)
REASON_ZERO_WIDTH_REPLICATE_DISTRIBUTION = "the_replicate_distribution_has_zero_width"

#: Pair-level refusal precedence. The first applicable reason is the pair's
#: ``reason``; the whole list is kept on the estimate so a lower-precedence fault
#: is recorded rather than hidden. Baseline reasons come first because they decide
#: whether the curve is an absorption curve at all, and the terminal horizon next
#: because it defines the reaction every fraction is measured against.
REASON_PRECEDENCE: tuple[str, ...] = (
    REASON_NON_POSITIVE_HORIZON,
    REASON_PAIR_BASELINE_UNOBSERVED,
    REASON_BASELINE_NOT_PRE_RELEASE,
    REASON_TERMINAL_HORIZON_UNDECLARED,
    REASON_TERMINAL_HORIZON_UNOBSERVED,
    REASON_TERMINAL_RESPONSE_ZERO,
)

#: Flags recorded on an estimate. Each one names a feature of the tape that a
#: reader would otherwise have to reconstruct from the fractions.
FLAG_NON_MONOTONE = "response_path_is_non_monotone_against_the_terminal_direction"
FLAG_OVERSHOOT = "response_overshoots_the_terminal_reaction"
FLAG_NEGATIVE_FRACTION = (
    "absorbed_fraction_is_negative_at_some_horizon_because_the_price_moved_through_its_baseline"
)
FLAG_SPANS_UNOBSERVED_HORIZON = (
    "crossing_is_interpolated_across_a_declared_horizon_that_was_not_observed"
)
FLAG_DECLARED_AFTER_LAST_OBSERVATION = (
    "declared_horizons_after_the_last_observation_were_not_observed"
)

#: The sealed trade panel's columns mapped onto this module's declared names. One
#: mapping, in one place: a renamed panel column cannot silently drop the terminal
#: reaction's horizon and leave an absorption time measured against another one.
ABSORPTION_COLUMN_MAP: Mapping[str, str] = {
    "event_id": "event_id",
    "cluster_id": "cluster_id",
    "family": "family",
    "venue": "venue",
    "contract_id": "contract_id",
    "release_time": "event_time",
    "baseline_time": "baseline_source_time",
    "horizon_seconds": "horizon_seconds",
    "response": "response",
}

#: The panel's own column names that must be present for a path to be readable at
#: all, in the panel's vocabulary rather than this module's. ``family`` and
#: ``venue`` are not required: a frame without them still identifies its pairs, and
#: a missing ``family`` is reported as undeclared rather than invented.
REQUIRED_PANEL_COLUMNS: tuple[str, ...] = (
    "event_id",
    "cluster_id",
    "contract_id",
    "event_time",
    "baseline_source_time",
    "horizon_seconds",
    "response",
)

UNSPECIFIED_FAMILY = "family_not_declared_by_the_panel"


class AbsorptionError(ValueError):
    """The supplied path or panel does not satisfy the absorption contract."""


def _text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AbsorptionError(f"{field_name} must be a non-empty str, got {value!r}")
    return value


def _horizon_seconds(value: object, *, field_name: str) -> int:
    """One horizon in whole seconds.

    A numpy integer is accepted because a horizon read out of a sealed panel is
    one, and rejecting it would refuse every real panel while accepting a
    hand-built frame. ``bool`` is refused first: it is an ``Integral``, and a
    True horizon is a mistake rather than a one-second horizon.
    """
    if isinstance(value, bool) or not isinstance(value, numbers.Integral):
        raise AbsorptionError(f"{field_name} must be an int horizon in seconds, got {value!r}")
    return int(value)


def _optional_text(value: object) -> str | None:
    """A declared label, or ``None`` when the panel records no value for it.

    A NaN reaches a string column whenever a frame carrying the column is
    concatenated with one that does not, and ``str(nan)`` is the label ``'nan'``.
    Admitting it would put a family no panel declared into the by-family
    breakdown, so a missing label is reported as undeclared instead.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    return value


def _finite_or_none(value: object) -> float | None:
    """A recorded value, or ``None`` when the tape holds no number for the leg.

    A NaN reaches a float column whenever a panel writes a null, and an infinity
    is not a price difference either. Both stand for "not observed" here, because
    neither can produce a fraction and imputing one would invent an observation.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _require_columns(frame: pd.DataFrame, columns: Sequence[str], *, context: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise AbsorptionError(
            f"{context} needs the declared panel columns {missing}; a path read without "
            "them would be measured against a different baseline or a different horizon set"
        )


def _median_or_none(values: Sequence[float]) -> float | None:
    """The median of the values that exist, or ``None`` when none does."""
    finite = [float(value) for value in values if value is not None and math.isfinite(value)]
    if not finite:
        return None
    return float(np.median(np.asarray(finite, dtype=np.float64)))


@dataclass(frozen=True, slots=True)
class AbsorptionPath:
    """One contract's response curve to one release, on the release's own clock.

    ``responses`` holds the panel's ``response`` value at each declared horizon, in
    ascending horizon order, with ``None`` where the endpoint leg was not observed.
    ``baseline_offset_seconds`` is the baseline print's position relative to the
    release: negative is before the release, which is the only position that makes
    the curve an absorption curve.

    The baseline is one value for the whole path rather than one per horizon,
    because ``A`` is a fraction of a single pre-release reference. A frame that
    anchors each horizon separately is a forward-increment path, and
    :func:`paths_from_panel` refuses it instead of averaging the two readings.
    """

    event_id: str
    cluster_id: str
    contract_id: str
    responses: tuple[tuple[int, float | None], ...]
    baseline_offset_seconds: float | None = None
    family: str | None = None
    venue: str | None = None

    def __post_init__(self) -> None:
        for name in ("event_id", "cluster_id", "contract_id"):
            object.__setattr__(self, name, _text(getattr(self, name), field_name=f"path.{name}"))
        if self.family is not None:
            object.__setattr__(self, "family", _text(self.family, field_name="path.family"))
        if self.venue is not None:
            object.__setattr__(self, "venue", _text(self.venue, field_name="path.venue"))
        offset = _finite_or_none(self.baseline_offset_seconds)
        object.__setattr__(self, "baseline_offset_seconds", offset)

        normalised: list[tuple[int, float | None]] = []
        for item in self.responses:
            if not isinstance(item, Sequence) or len(item) != 2:
                raise AbsorptionError(
                    f"path.responses entries must be (horizon_seconds, response) pairs, got {item!r}"
                )
            horizon = _horizon_seconds(item[0], field_name="path.responses horizon_seconds")
            normalised.append((horizon, _finite_or_none(item[1])))
        normalised.sort(key=lambda pair: pair[0])
        for (earlier, _), (later, _) in itertools.pairwise(normalised):
            if later == earlier:
                raise AbsorptionError(
                    f"path {self.event_id!r}/{self.contract_id!r} declares horizon "
                    f"{later}s more than once; choosing between two rows by order would pick a "
                    "price the record does not rank"
                )
        object.__setattr__(self, "responses", tuple(normalised))

    @property
    def declared_horizons_seconds(self) -> tuple[int, ...]:
        """Every horizon the path carries a row for, whether or not it was observed."""
        return tuple(horizon for horizon, _ in self.responses)

    @property
    def observed_horizons_seconds(self) -> tuple[int, ...]:
        """The horizons whose endpoint leg was actually observed."""
        return tuple(
            horizon for horizon, response in self.responses if response is not None and horizon > 0
        )

    @property
    def unobserved_horizons_seconds(self) -> tuple[int, ...]:
        """Declared horizons that carry no response, in ascending order.

        A non-positive horizon is included here too: at the release instant the
        response is the baseline by construction, so a zero there would be a
        structural zero rather than a measurement and it never enters ``A``.
        """
        return tuple(
            horizon for horizon, response in self.responses if response is None or horizon <= 0
        )

    @property
    def response_by_horizon(self) -> dict[int, float | None]:
        return dict(self.responses)

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "cluster_id": self.cluster_id,
            "contract_id": self.contract_id,
            "family": self.family,
            "venue": self.venue,
            "baseline_offset_seconds": self.baseline_offset_seconds,
            "declared_horizons_seconds": list(self.declared_horizons_seconds),
            "observed_horizons_seconds": list(self.observed_horizons_seconds),
            "unobserved_horizons_seconds": list(self.unobserved_horizons_seconds),
            "responses": {str(horizon): value for horizon, value in self.responses},
        }


@dataclass(frozen=True, slots=True)
class AbsorptionEstimate:
    """One pair's absorption curve, its two absorption times, and any refusal.

    ``status`` is :data:`STATUS_ESTIMATED` only when ``R`` exists and is non-zero,
    so a pair is never reported as an estimate on the strength of a fraction that
    could not be formed. ``h50_seconds``/``h90_seconds`` are independently ``None``
    with their own reason when a single fraction is unreachable, because an
    unreached 90% does not invalidate a measured 50%, and
    ``bound_horizon_seconds`` carries an upper bound when the fraction was already
    reached at the first observed horizon.
    """

    event_id: str
    cluster_id: str
    contract_id: str
    family: str | None
    venue: str | None
    terminal_horizon_seconds: int
    status: str
    baseline_offset_seconds: float | None
    terminal_response: float | None
    reason: str | None
    reasons: tuple[str, ...]
    h50_seconds: float | None = None
    h90_seconds: float | None = None
    h50_reason: str | None = None
    h90_reason: str | None = None
    h50_bound_horizon_seconds: float | None = None
    h90_bound_horizon_seconds: float | None = None
    observed_horizons_seconds: tuple[int, ...] = ()
    unobserved_horizons_seconds: tuple[int, ...] = ()
    horizons_beyond_terminal_seconds: tuple[int, ...] = ()
    cumulative_fractions: tuple[tuple[int, float], ...] = ()
    reversals: tuple[tuple[int, int], ...] = ()
    max_drawdown_fraction: float | None = None
    overshoot_fraction: float | None = None
    flags: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "cluster_id": self.cluster_id,
            "contract_id": self.contract_id,
            "family": self.family,
            "venue": self.venue,
            "terminal_horizon_seconds": self.terminal_horizon_seconds,
            "status": self.status,
            "reason": self.reason,
            "reasons": list(self.reasons),
            "baseline_offset_seconds": self.baseline_offset_seconds,
            "terminal_response": self.terminal_response,
            "h50_seconds": self.h50_seconds,
            "h90_seconds": self.h90_seconds,
            "h50_reason": self.h50_reason,
            "h90_reason": self.h90_reason,
            "h50_bound_horizon_seconds": self.h50_bound_horizon_seconds,
            "h90_bound_horizon_seconds": self.h90_bound_horizon_seconds,
            "observed_horizons_seconds": list(self.observed_horizons_seconds),
            "unobserved_horizons_seconds": list(self.unobserved_horizons_seconds),
            "horizons_beyond_terminal_seconds": list(self.horizons_beyond_terminal_seconds),
            "cumulative_fractions": {
                str(horizon): value for horizon, value in self.cumulative_fractions
            },
            "reversals": [list(pair) for pair in self.reversals],
            "max_drawdown_fraction": self.max_drawdown_fraction,
            "overshoot_fraction": self.overshoot_fraction,
            "flags": list(self.flags),
        }


@dataclass(frozen=True, slots=True)
class _Crossing:
    """Where one fraction is first reached, or why it is not.

    ``seconds`` is the interpolated crossing and is set only when the fraction was
    reached between two observed horizons, so a caller cannot read a crossing off
    a record that reports a refusal. ``bound_horizon_seconds`` is the first
    observed horizon at which the fraction was already exceeded, which is an upper
    bound on the absorption time rather than a measurement of it.
    """

    seconds: float | None
    reason: str | None
    bound_horizon_seconds: float | None
    bracket: tuple[int, int] | None

    def __post_init__(self) -> None:
        if (self.seconds is None) == (self.reason is None):
            raise AbsorptionError(
                "a crossing carries either a located instant or the reason it could not be "
                f"located, never both and never neither: seconds={self.seconds!r}, "
                f"reason={self.reason!r}"
            )


def _first_crossing(
    absorbed: Sequence[tuple[int, float]],
    fraction: float,
    *,
    tolerance: float,
) -> _Crossing:
    """The first horizon at which ``A`` reaches ``fraction``, or why it does not.

    ``absorbed`` holds the observed ``(horizon, A)`` pairs in ascending horizon
    order, and only observed pairs: an unobserved declared horizon is not on this
    axis at all, which is why a crossing interpolated across one carries the flag
    naming the gap.

    Three outcomes exist and no others. The fraction is met exactly at an observed
    horizon, in which case that horizon is the crossing. It is bracketed by two
    observed horizons, in which case the crossing is interpolated linearly inside
    the bracket and never past it. Or the move is already past the fraction at the
    first observed horizon, in which case the declared horizon set cannot place the
    crossing and only the upper bound is reported. Nothing is extrapolated: a
    fraction above every observed ``A`` cannot arise for an estimateable pair,
    because ``A`` is normalised by the terminal response and therefore equals 1 at
    the terminal horizon itself, which is observed by the time this is called.
    """
    first_horizon, first_value = absorbed[0]
    if first_value > fraction + tolerance:
        # The move is already past the fraction at the first observed horizon, so
        # the crossing lies before it and the declared horizon set cannot place it.
        return _Crossing(
            None,
            REASON_REACHED_BEFORE_FIRST_OBSERVED_HORIZON,
            float(first_horizon),
            None,
        )
    if abs(first_value - fraction) <= tolerance:
        # The path is exactly at the fraction at its first observation, so the
        # declared horizon is the crossing rather than an interpolation of it.
        return _Crossing(float(first_horizon), None, None, None)

    for (h_prev, a_prev), (h_cur, a_cur) in itertools.pairwise(absorbed):
        if a_cur <= fraction + tolerance:
            if abs(a_cur - fraction) <= tolerance:
                # Exactly at the fraction at a declared horizon: that horizon is the
                # crossing, and no gap between the bracket can move it.
                return _Crossing(float(h_cur), None, None, None)
            continue
        # ``a_prev < fraction - tolerance < fraction + tolerance < a_cur``, so
        # ``weight`` lies strictly inside ``(0, 1)`` and the interpolated instant
        # strictly inside the bracket. The crossing can therefore never be
        # extrapolated past an observed horizon, by construction rather than by a
        # clamp applied afterwards.
        weight = (fraction - a_prev) / (a_cur - a_prev)
        return _Crossing(
            float(h_prev) + weight * float(h_cur - h_prev), None, None, (h_prev, h_cur)
        )
    # Unreachable for an estimateable pair: ``A`` reaches 1 at the terminal
    # horizon, which is observed by the time this is called, so the loop above
    # always returns. Fail loudly rather than return a quiet null if that ever
    # stops holding.
    raise AbsorptionError(
        f"the fraction {fraction} was not reached by any observed horizon although the path is "
        "normalised by its terminal response; the crossing search and the normalisation disagree"
    )


def _path_refusals(path: AbsorptionPath, *, terminal_horizon_seconds: int) -> tuple[str, ...]:
    """Every reason this pair cannot carry an absorption curve, in precedence order.

    The first reason is the pair's own ``reason``; the rest stay on the estimate so
    a second fault is recorded rather than hidden behind the first. A reason
    decided here is never re-derived at the call site, so there is one place that
    decides what makes ``R`` undefined.
    """
    reasons: list[str] = []
    if terminal_horizon_seconds <= 0:
        # At the release instant the response is the baseline by construction, so a
        # terminal horizon of zero would make every fraction a fraction of a
        # structural zero, and a negative horizon would date the terminal reaction
        # inside the pre-release baseline it is measured from.
        reasons.append(REASON_NON_POSITIVE_HORIZON)
    offset = path.baseline_offset_seconds
    if offset is None:
        reasons.append(REASON_PAIR_BASELINE_UNOBSERVED)
    elif offset >= 0.0:
        # The release instant and anything after it are not a pre-release reference.
        # A path anchored at the forecast origin measures the increment from the
        # origin onward, which is a different estimand from the release's own
        # absorption, and conflating the two is the substitution this refuses.
        reasons.append(REASON_BASELINE_NOT_PRE_RELEASE)

    declared = path.declared_horizons_seconds
    terminal_response = _finite_or_none(path.response_by_horizon.get(terminal_horizon_seconds))
    if terminal_horizon_seconds not in declared:
        reasons.append(REASON_TERMINAL_HORIZON_UNDECLARED)
    elif terminal_response is None:
        reasons.append(REASON_TERMINAL_HORIZON_UNOBSERVED)
    elif terminal_response == 0.0:
        # ``A`` is a fraction of ``R``. A path that ended where it began has no
        # reaction to take a fraction of, and dividing by it would report an
        # undefined quantity as a number, so the pair is refused by name.
        reasons.append(REASON_TERMINAL_RESPONSE_ZERO)

    return tuple(reason for reason in REASON_PRECEDENCE if reason in set(reasons))


def estimate_absorption(
    path: AbsorptionPath,
    *,
    terminal_horizon_seconds: int,
) -> AbsorptionEstimate:
    """One pair's ``h50`` and ``h90`` against a declared terminal horizon.

    ``terminal_horizon_seconds`` has no default on purpose. ``H`` defines ``R``,
    and ``A`` is a fraction of ``R``, so one path yields a different ``h50`` and
    ``h90`` for every choice of ``H``. A default would let a caller read an
    absorption time without stating which terminal reaction it was measured
    against, and two runs that disagreed about absorption would then disagree over
    an argument neither of them wrote down.
    """
    horizon = _horizon_seconds(terminal_horizon_seconds, field_name="terminal_horizon_seconds")
    reasons = _path_refusals(path, terminal_horizon_seconds=horizon)
    observed_responses = path.observed_horizons_seconds
    unobserved = path.unobserved_horizons_seconds
    terminal_response = _finite_or_none(path.response_by_horizon.get(horizon))

    if reasons:
        return AbsorptionEstimate(
            event_id=path.event_id,
            cluster_id=path.cluster_id,
            contract_id=path.contract_id,
            family=path.family,
            venue=path.venue,
            terminal_horizon_seconds=horizon,
            status=STATUS_REFUSED,
            baseline_offset_seconds=path.baseline_offset_seconds,
            terminal_response=terminal_response,
            reason=reasons[0],
            reasons=reasons,
            observed_horizons_seconds=observed_responses,
            unobserved_horizons_seconds=unobserved,
        )
    if terminal_response is None:  # pragma: no cover - _path_refusals covers every case
        raise AbsorptionError(
            f"pair {path.event_id!r}/{path.contract_id!r} reached the fraction step with no "
            "terminal response; the refusal check and the fraction step disagree"
        )

    fractions = path.response_by_horizon
    # ``R`` is the reaction at ``H``, so a horizon past ``H`` is a reaction against
    # a different denominator and is not on this curve. It is recorded rather than
    # silently dropped, so a reader sees which declared rows the curve left out.
    beyond_terminal = tuple(h for h in observed_responses if h > horizon)
    curve_horizons = tuple(h for h in observed_responses if h <= horizon)
    absorbed = tuple(
        (h, float(fractions[h]) / terminal_response)  # type: ignore[arg-type]
        for h in curve_horizons
    )
    values = tuple(value for _, value in absorbed)

    reversals: list[tuple[int, int]] = []
    running_max = float("-inf")
    max_drawdown = 0.0
    for index, (h_cur, a_cur) in enumerate(absorbed):
        running_max = max(running_max, a_cur)
        max_drawdown = max(max_drawdown, running_max - a_cur)
        if index and a_cur < values[index - 1] - DECLARED_FRACTION_TOLERANCE:
            reversals.append((absorbed[index - 1][0], h_cur))

    flags: set[str] = set()
    if reversals or max_drawdown > DECLARED_FRACTION_TOLERANCE:
        flags.add(FLAG_NON_MONOTONE)
    if min(values) < -DECLARED_FRACTION_TOLERANCE:
        flags.add(FLAG_NEGATIVE_FRACTION)
    overshoot = max(values) - 1.0
    overshoot_fraction = overshoot if overshoot > DECLARED_FRACTION_TOLERANCE else None
    if overshoot_fraction is not None:
        flags.add(FLAG_OVERSHOOT)
    if observed_responses and max(path.declared_horizons_seconds) > max(observed_responses):
        flags.add(FLAG_DECLARED_AFTER_LAST_OBSERVATION)

    h50 = _first_crossing(absorbed, H50_FRACTION, tolerance=DECLARED_FRACTION_TOLERANCE)
    h90 = _first_crossing(absorbed, H90_FRACTION, tolerance=DECLARED_FRACTION_TOLERANCE)
    for crossing in (h50, h90):
        if crossing.bracket is not None:
            lower, upper = crossing.bracket
            if any(lower < h < upper for h in unobserved):
                flags.add(FLAG_SPANS_UNOBSERVED_HORIZON)

    return AbsorptionEstimate(
        event_id=path.event_id,
        cluster_id=path.cluster_id,
        contract_id=path.contract_id,
        family=path.family,
        venue=path.venue,
        terminal_horizon_seconds=horizon,
        status=STATUS_ESTIMATED,
        baseline_offset_seconds=path.baseline_offset_seconds,
        terminal_response=terminal_response,
        reason=None,
        reasons=(),
        h50_seconds=h50.seconds,
        h90_seconds=h90.seconds,
        h50_reason=h50.reason,
        h90_reason=h90.reason,
        h50_bound_horizon_seconds=h50.bound_horizon_seconds,
        h90_bound_horizon_seconds=h90.bound_horizon_seconds,
        observed_horizons_seconds=observed_responses,
        unobserved_horizons_seconds=unobserved,
        horizons_beyond_terminal_seconds=beyond_terminal,
        cumulative_fractions=absorbed,
        reversals=tuple(reversals),
        max_drawdown_fraction=(
            float(max_drawdown) if max_drawdown > DECLARED_FRACTION_TOLERANCE else 0.0
        ),
        overshoot_fraction=overshoot_fraction,
        flags=tuple(sorted(flags)),
    )


def _pair_key(record: Mapping[str, Any], *, venue_column: bool) -> tuple[str, str, str]:
    # A contract id without its venue is not a candidate key, so the venue is part
    # of the pair identity when the frame carries the column at all.
    venue = (_optional_text(record.get("venue")) or "") if venue_column else ""
    return (
        _text(record.get("event_id"), field_name="panel event_id"),
        _text(record.get("contract_id"), field_name="panel contract_id"),
        venue,
    )


def paths_from_panel(frame: pd.DataFrame) -> tuple[AbsorptionPath, ...]:
    """Every contract/release pair in a sealed trade panel as an absorption path.

    The panel's own ``response`` column is the estimand already: it is
    ``endpoint - baseline`` on the event-axis price, and its baseline is the last
    print strictly before the release rather than the price carried forward.
    Masked rows arrive here with a null ``response`` and therefore an unobserved
    horizon, which is the panel's verdict preserved rather than a second masking
    written in this module.

    Two frame shapes are refused rather than averaged, because both would produce a
    curve that is not the declared one: a pair whose rows disagree about the
    baseline print, which anchors each horizon separately, and a pair that declares
    the same horizon twice, which would force a choice by row order.
    """
    if not isinstance(frame, pd.DataFrame):
        raise AbsorptionError(
            f"paths_from_panel expects a pandas DataFrame, got {type(frame).__name__}"
        )
    if frame.empty:
        raise AbsorptionError("paths_from_panel received an empty panel")
    _require_columns(frame, REQUIRED_PANEL_COLUMNS, context="paths_from_panel")

    venue_column = "venue" in frame.columns
    family_column = "family" in frame.columns
    columns = dict(ABSORPTION_COLUMN_MAP)

    grouped: dict[tuple[str, str, str], list[tuple[int, float | None]]] = {}
    identity: dict[tuple[str, str, str], tuple[str, str | None]] = {}
    baselines: dict[tuple[str, str, str], float | None] = {}
    for record in frame.to_dict("records"):
        key = _pair_key(record, venue_column=venue_column)
        grouped.setdefault(key, [])
        identity.setdefault(
            key,
            (
                _text(record["cluster_id"], field_name="panel cluster_id"),
                _optional_text(record.get("family")) if family_column else None,
            ),
        )
        horizon = _horizon_seconds(
            record[columns["horizon_seconds"]], field_name=f"panel horizon for pair {key}"
        )
        grouped[key].append((horizon, _finite_or_none(record[columns["response"]])))

        offset = _baseline_offset(record[columns["release_time"]], record[columns["baseline_time"]])
        if key in baselines and baselines[key] != offset:
            raise AbsorptionError(
                f"pair {key} disagrees about its baseline print ({baselines[key]!r} and "
                f"{offset!r} seconds from the release); one release has one pre-release "
                "baseline, and a path that anchors each horizon separately measures an "
                "increment rather than the release's own absorption"
            )
        baselines[key] = offset

    paths: list[AbsorptionPath] = []
    for key in sorted(grouped):
        event_id, contract_id, venue = key
        cluster_id, family = identity[key]
        paths.append(
            AbsorptionPath(
                event_id=event_id,
                cluster_id=cluster_id,
                contract_id=contract_id,
                responses=tuple(grouped[key]),
                baseline_offset_seconds=baselines[key],
                family=family,
                venue=venue or None,
            )
        )
    return tuple(paths)


def _baseline_offset(release_value: Any, baseline_value: Any) -> float | None:
    """Where the baseline print sits relative to the release, or ``None`` if unknown.

    An unparsable or absent time yields ``None`` rather than a guess: the offset
    decides whether the curve is an absorption curve, so an unknown one is refused
    by name at estimation time instead of being assumed to be pre-release.
    """
    release = pd.to_datetime(release_value, utc=True, errors="coerce")
    baseline = pd.to_datetime(baseline_value, utc=True, errors="coerce")
    if pd.isna(release) or pd.isna(baseline):
        return None
    return float((pd.Timestamp(baseline) - pd.Timestamp(release)).total_seconds())


def _release_interval(
    h50_by_cluster: Mapping[str, float],
    h90_by_cluster: Mapping[str, float],
    clusters: Sequence[str],
    *,
    seed: int,
    samples: int,
    coverage: float,
) -> dict[str, Any]:
    """The cluster bootstrap of the two medians, over release clusters.

    One unit per release cluster, carrying that release's own median absorption
    time. Pooling pairs instead would let one release with many contracts dominate
    the median, and cross-venue equivalents sharing a ``cluster_id`` are one
    release rather than several.
    """
    point = {
        name: value
        for name, value in (
            ("median_h50_seconds", _median_or_none(list(h50_by_cluster.values()))),
            ("median_h90_seconds", _median_or_none(list(h90_by_cluster.values()))),
        )
        if value is not None
    }
    if not point:
        return {
            "status": "not_estimated",
            "reason": REASON_NO_CLUSTER_YIELDS_AN_ABSORPTION_TIME,
            "n_clusters": len(clusters),
            "samples_requested": int(samples),
            "seed": int(seed),
            "samples": {},
        }

    labels = [str(cluster) for cluster in clusters]
    units = pd.Series(labels, index=labels, dtype=object)

    def statistic(sample: np.ndarray) -> dict[str, float | None]:
        drawn = [str(label) for label in sample]
        return {
            "median_h50_seconds": _median_or_none(
                [h50_by_cluster[cluster] for cluster in drawn if cluster in h50_by_cluster]
            ),
            "median_h90_seconds": _median_or_none(
                [h90_by_cluster[cluster] for cluster in drawn if cluster in h90_by_cluster]
            ),
        }

    result = clustered_bootstrap(
        point, statistic, units, seed=int(seed), samples=int(samples), coverage=float(coverage)
    )
    n_clusters = int(result["n_clusters"])
    degenerate = bool(result["degenerate"])
    reason = None
    if n_clusters < 2:
        reason = REASON_TOO_FEW_RELEASE_CLUSTERS
    elif degenerate:
        reason = REASON_ZERO_WIDTH_REPLICATE_DISTRIBUTION
    return {
        "status": "degenerate" if degenerate else "ok",
        "reason": reason,
        "method": result["method"],
        "n_units": int(result["n_units"]),
        "n_clusters": n_clusters,
        "samples_requested": int(result["samples_requested"]),
        "samples_effective": int(result["samples_effective"]),
        "coverage": float(result["coverage"]),
        "seed": int(result["seed"]),
        "samples": result["samples"],
    }


def _summary_block(
    estimates: Sequence[AbsorptionEstimate],
    *,
    seed: int,
    samples: int,
    coverage: float,
) -> dict[str, Any]:
    """One group's counts, its release-level medians, and their interval."""
    pair_reasons: Counter[str] = Counter()
    h50_reasons: Counter[str] = Counter()
    h90_reasons: Counter[str] = Counter()
    multiple_reasons = 0
    for estimate in estimates:
        if estimate.status == STATUS_REFUSED and estimate.reason is not None:
            pair_reasons[estimate.reason] += 1
            if len(estimate.reasons) > 1:
                multiple_reasons += 1
        if estimate.h50_reason is not None:
            h50_reasons[estimate.h50_reason] += 1
        if estimate.h90_reason is not None:
            h90_reasons[estimate.h90_reason] += 1

    clusters = sorted({estimate.cluster_id for estimate in estimates})
    h50_by_cluster = {
        cluster: median
        for cluster in clusters
        if (median := _median_or_none(_release_values(estimates, cluster, "h50"))) is not None
    }
    h90_by_cluster = {
        cluster: median
        for cluster in clusters
        if (median := _median_or_none(_release_values(estimates, cluster, "h90"))) is not None
    }

    estimateable = [estimate for estimate in estimates if estimate.status == STATUS_ESTIMATED]
    all_located = [
        estimate
        for estimate in estimateable
        if estimate.h50_seconds is not None and estimate.h90_seconds is not None
    ]
    refusals_by_reason: Counter[str] = Counter()
    for counter in (pair_reasons, h50_reasons, h90_reasons):
        refusals_by_reason.update(counter)

    return {
        "pairs": len(estimates),
        # A pair is estimateable when ``R`` exists and is non-zero, which is the
        # whole of what the fraction needs. A pair whose crossing the declared
        # horizons cannot place is still estimateable and still has a bound, so the
        # two counts partition the panel and ``pairs_with_both_times`` is the
        # stricter count beside them.
        "estimateable_pairs": len(estimateable),
        "refused_pairs": len(estimates) - len(estimateable),
        "pairs_with_both_times": len(all_located),
        "pairs_with_h50": sum(1 for e in estimates if e.h50_seconds is not None),
        "pairs_with_h90": sum(1 for e in estimates if e.h90_seconds is not None),
        "pairs_with_h50_bounded_only": sum(
            1
            for e in estimates
            if e.h50_seconds is None and e.h50_bound_horizon_seconds is not None
        ),
        "pairs_with_h90_bounded_only": sum(
            1
            for e in estimates
            if e.h90_seconds is None and e.h90_bound_horizon_seconds is not None
        ),
        "pairs_with_a_reversal": sum(1 for e in estimates if e.reversals),
        "pairs_with_overshoot": sum(1 for e in estimates if e.overshoot_fraction is not None),
        "pairs_with_a_negative_fraction": sum(
            1 for e in estimates if FLAG_NEGATIVE_FRACTION in e.flags
        ),
        "pairs_whose_crossing_spans_an_unobserved_horizon": sum(
            1 for e in estimates if FLAG_SPANS_UNOBSERVED_HORIZON in e.flags
        ),
        "pairs_with_several_recorded_refusal_reasons": multiple_reasons,
        # Refusals are counted rather than dropped: a pair refused for a named
        # reason is still in ``pairs``, so a median never reads as if it covered
        # the whole panel it was taken over.
        "refusals_by_reason": dict(sorted(refusals_by_reason.items())),
        "refusals_by_stage": {
            "pair": dict(sorted(pair_reasons.items())),
            "h50": dict(sorted(h50_reasons.items())),
            "h90": dict(sorted(h90_reasons.items())),
        },
        "clusters_total": len(clusters),
        "clusters_with_an_h50": len(h50_by_cluster),
        "clusters_with_an_h90": len(h90_by_cluster),
        "median_h50_seconds": _median_or_none(list(h50_by_cluster.values())),
        "median_h90_seconds": _median_or_none(list(h90_by_cluster.values())),
        "uncertainty": _release_interval(
            h50_by_cluster, h90_by_cluster, clusters, seed=seed, samples=samples, coverage=coverage
        ),
    }


def _release_values(
    estimates: Sequence[AbsorptionEstimate], cluster: str, unit: str
) -> list[float]:
    """One release cluster's own absorption times, for the median over releases."""
    return [
        value
        for estimate in estimates
        if estimate.cluster_id == cluster
        and (value := getattr(estimate, f"{unit}_seconds")) is not None
    ]


def summarise_absorption_panel(
    frame: pd.DataFrame,
    *,
    terminal_horizon_seconds: int,
    seed: int = DEFAULT_SEED,
    samples: int = DEFAULT_SAMPLES,
    coverage: float = DEFAULT_COVERAGE,
) -> dict[str, Any]:
    """The whole panel's absorption times, per family and overall.

    ``terminal_horizon_seconds`` is required for the same reason it is required by
    :func:`estimate_absorption`: it defines the reaction every fraction is a
    fraction of, so a summary without it would report medians whose denominator no
    reader could recover.

    Each group reports the pairs it saw, how many were estimateable, every refusal
    counted by name at the stage it happened, and the medians of ``h50`` and
    ``h90`` over release clusters with a cluster bootstrap interval from
    :func:`market_propagation.evaluation.clustered_bootstrap`. A refused pair
    contributes no value and is never imputed; it stays in the counts so the
    coverage of a median is visible beside it.
    """
    frame_paths = paths_from_panel(frame)
    if not frame_paths:
        raise AbsorptionError("the panel holds no contract/release pair")
    estimates = tuple(
        estimate_absorption(path, terminal_horizon_seconds=terminal_horizon_seconds)
        for path in frame_paths
    )
    by_family: dict[str, list[AbsorptionEstimate]] = {}
    for estimate in estimates:
        by_family.setdefault(estimate.family or UNSPECIFIED_FAMILY, []).append(estimate)

    horizons = sorted(
        {horizon for path in frame_paths for horizon in path.declared_horizons_seconds}
    )
    return {
        "estimand": ABSORPTION_ESTIMAND,
        "terminal_horizon_seconds": int(terminal_horizon_seconds),
        "horizons_seconds": horizons,
        "baseline_convention": (
            "the panel's own last print strictly before the release, one baseline per pair; a "
            "path whose baseline is not strictly pre-release is refused rather than read as "
            "absorption"
        ),
        "aggregation_unit": (
            "release cluster: pairs are reduced to their cluster's median absorption time before "
            "the median and the interval are taken"
        ),
        "interpolation": (
            "linear between the two observed horizons bracketing the crossing, inside the "
            "declared horizon set; a fraction not reached by the last observation has no crossing"
        ),
        "pairs": len(estimates),
        "overall": _summary_block(estimates, seed=seed, samples=samples, coverage=coverage),
        "by_family": {
            family: _summary_block(group, seed=seed, samples=samples, coverage=coverage)
            for family, group in sorted(by_family.items())
        },
    }
