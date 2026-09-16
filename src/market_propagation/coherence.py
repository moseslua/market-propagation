"""Payoff-coherence diagnostics and exact rule matching.

For a contract family with common settlement and numeraire assumptions,
enumerate the ``K`` admissible atomic outcomes and let ``A`` be the
``M x K`` payout matrix, with entries in ``[0, 1]``. Binary-only cohorts
have entries in ``{0, 1}``; exceptional payouts require the wider range.
With explicit discount normalization the set of coherent normalized state
prices is ``C = {A pi : pi in Simplex(K-1)}``. Given the quoted box
``B = prod_m [b_m, a_m]`` the finite-dimensional diagnostic is

    d = min_{pi in Simplex(K-1)} max_m dist((A pi)_m, [b_m, a_m]).

``coherence_distance`` solves that min-max program as a linear program with
``scipy.optimize.linprog``. A midpoint can violate an identity while
``C`` still intersects the box, so the raw midpoint diagnostic is reported
separately from the constrained projection: a projection enforces coherence
and therefore cannot itself demonstrate market coherence. Infeasibility is a
quote-coherence finding under stated assumptions, not automatically an
executable arbitrage; fees, inventory constraints, finite size, differing
cashflows and non-simultaneous execution all matter.

Synchronization and valid-quote gates belong to the caller. These functions
require fully specified, aligned input rows and reject anything else instead
of silently repairing it: pass only rows whose quotes are synchronized and
valid, and drop missing/one-sided books before calling.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any

import numpy as np
from scipy.optimize import linprog

if TYPE_CHECKING:  # pragma: no cover - typing only, no runtime coupling
    from .domain import Contract

__all__ = [
    "REQUIRED_SEMANTIC_FIELDS",
    "CoherenceInputError",
    "CoherenceSolverError",
    "coherence_distance",
    "exact_match",
    "threshold_payoffs",
]

#: Fields a rule audit must see before two records can be called the same claim.
REQUIRED_SEMANTIC_FIELDS: tuple[str, ...] = (
    "reference_period",
    "source",
    "units",
    "vintage",
    "rounding",
    "timezone",
    "deadline",
    "settlement",
    "currency",
    "exceptional_policy",
    "operator",
)

#: Compared for equality, but only blocking within one venue: across venues
#: these are per-venue conventions, so a mismatch is recorded as a note.
_SAME_VENUE_FIELDS: tuple[str, ...] = (
    "contract_id",
    "rule_hash",
    "open_time",
    "close_time",
    "resolve_time",
)

#: Recorded when they differ but never blocking on their own. ``venue`` is the
#: axis that defines cross-venue equivalence, so it is reported rather than
#: treated as a rule difference; equal values never certify a match either.
_NON_BLOCKING_FIELDS: frozenset[str] = frozenset({"venue"})

#: All rule fields compared by :func:`exact_match`, in report order.
COHERENCE_FIELDS: tuple[str, ...] = (
    "venue",
    "contract_id",
    "event_id",
    "family",
    "reference_period",
    "source",
    "units",
    "vintage",
    "operator",
    "threshold",
    "lower",
    "upper",
    "rounding",
    "timezone",
    "deadline",
    "settlement",
    "currency",
    "exceptional_policy",
    "rule_hash",
    "open_time",
    "close_time",
    "resolve_time",
)

_THRESHOLD_OPERATORS = frozenset({"above", "at_least", "below", "at_most", "equal"})
_UNROUNDED = frozenset({"", "none"})
_ROUNDING_MODES = {
    "nearest": ROUND_HALF_UP,
    "up": ROUND_CEILING,
    "down": ROUND_FLOOR,
}
#: Operators whose payoff is defined by an external outcome map rather than by
#: a threshold on a scalar statistic.
_ATOM_FREE_OPERATORS = frozenset({"range", "binary"})

_MISSING = object()


class CoherenceInputError(ValueError):
    """Supplied payout matrix or quote box violates the stated input contract."""


class CoherenceSolverError(RuntimeError):
    """The coherence program did not reach a solution, so no distance is reported."""


def _as_decimal(value: Any, *, field: str, index: int) -> Decimal:
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, bool):
        raise CoherenceInputError(f"{field}[{index}] is a bool, not a number")
    elif isinstance(value, (int, np.integer)):
        number = Decimal(int(value))
    elif isinstance(value, (float, np.floating)):
        number = float(value)
        if not math.isfinite(number):
            raise CoherenceInputError(f"{field}[{index}]={value!r} is not finite")
        number = Decimal(str(number))
    else:
        raise CoherenceInputError(
            f"{field}[{index}]={value!r} is {type(value).__name__}, not a number"
        )
    if not number.is_finite():
        raise CoherenceInputError(f"{field}[{index}]={value!r} is not finite")
    return number


def _normalize_operator(operator: Any, *, index: int) -> str:
    text = str(operator).strip().lower()
    if text not in _THRESHOLD_OPERATORS | _ATOM_FREE_OPERATORS:
        raise CoherenceInputError(
            f"operators[{index}]={operator!r} is not one of "
            f"{sorted(_THRESHOLD_OPERATORS | _ATOM_FREE_OPERATORS)}"
        )
    return text


def _normalize_rounding(rounding: Any, *, index: int) -> str:
    text = str(rounding).strip().lower() if rounding is not None else "none"
    if text in _UNROUNDED:
        return "none"
    if text not in _ROUNDING_MODES:
        raise CoherenceInputError(
            f"rounding[{index}]={rounding!r} is not one of {[*sorted(_ROUNDING_MODES), 'none']}"
        )
    return text


def _decimal_places(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int):  # pragma: no cover - guarded by is_finite
        raise CoherenceInputError(f"{value!r} has no finite decimal exponent")
    return max(0, -exponent)


def threshold_payoffs(
    thresholds: Sequence[Any],
    operators: Sequence[Any],
    atoms: Sequence[Any],
    rounding: Any = None,
) -> np.ndarray:
    """Payout matrix for nested-threshold contracts over explicit atomic outcomes.

    ``atoms`` are the ``K`` admissible outcome values of the underlying scalar
    statistic. Row ``m`` collects the payout of contract ``m`` in every atom,
    so an atomic outcome ``k`` yields the state vector ``A[:, k]``.

    Operator semantics follow the rule audit: ``above`` and ``below`` are
    strict, ``at_least`` and ``at_most`` are not, and ``equal`` matches the
    rounded value exactly. When ``rounding`` is not ``none`` the atom is
    quantized to the threshold's own decimal precision first, with
    ``nearest`` (half up), ``up`` (ceiling) or ``down`` (floor); this is the
    only place a rounding rule changes payoffs. The comparison is done in
    :class:`~decimal.Decimal` so strictness and rounding never depend on
    binary-float representation.

    ``rounding`` may be a single value applied to every row or one value per
    row. ``range`` and ``binary`` contracts need a bucket or outcome map
    rather than a scalar threshold; build their payouts explicitly and call
    :func:`coherence_distance` directly.

    Returns a ``float64`` array of shape ``(M, K)`` with entries in ``{0, 1}``.
    """
    threshold_list = list(thresholds)
    operator_list = list(operators)
    atom_list = list(atoms)
    if not threshold_list:
        raise CoherenceInputError("thresholds is empty; nothing to build payouts for")
    if len(operator_list) != len(threshold_list):
        raise CoherenceInputError(
            f"operators has length {len(operator_list)} but thresholds has "
            f"length {len(threshold_list)}"
        )
    if not atom_list:
        raise CoherenceInputError("atoms is empty; no atomic outcomes were supplied")

    operators_norm = [_normalize_operator(op, index=i) for i, op in enumerate(operator_list)]
    atoms_dec = [_as_decimal(value, field="atoms", index=i) for i, value in enumerate(atom_list)]

    if isinstance(rounding, (str, bytes)) or rounding is None:
        rounding_norm = [_normalize_rounding(rounding, index=i) for i in range(len(threshold_list))]
    else:
        rounding_list = list(rounding)
        if len(rounding_list) != len(threshold_list):
            raise CoherenceInputError(
                f"rounding has length {len(rounding_list)} but thresholds has "
                f"length {len(threshold_list)}"
            )
        rounding_norm = [
            _normalize_rounding(value, index=i) for i, value in enumerate(rounding_list)
        ]

    thresholds_dec: list[Decimal] = []
    for index, value in enumerate(threshold_list):
        if operators_norm[index] in _ATOM_FREE_OPERATORS:
            if value is not None:
                raise CoherenceInputError(
                    f"operators[{index}]={operators_norm[index]!r} defines payoffs over buckets "
                    "or an outcome map, not over one threshold; build its payout row explicitly"
                )
            thresholds_dec.append(Decimal(0))
            continue
        if value is None:
            raise CoherenceInputError(
                f"thresholds[{index}] is None for operator {operators_norm[index]!r}, which "
                "needs a threshold"
            )
        thresholds_dec.append(_as_decimal(value, field="thresholds", index=index))

    payouts = np.zeros((len(threshold_list), len(atom_list)), dtype=np.float64)
    for row, (operator, threshold, mode) in enumerate(
        zip(operators_norm, thresholds_dec, rounding_norm, strict=True)
    ):
        if operator in _ATOM_FREE_OPERATORS:
            raise CoherenceInputError(
                f"operators[{row}]={operator!r} is not a scalar threshold comparison"
            )
        places = _decimal_places(threshold)
        quantizer = Decimal(1).scaleb(-places) if places else Decimal(1)
        mode_decimal = _ROUNDING_MODES.get(mode)
        for column, atom in enumerate(atoms_dec):
            value = atom if mode == "none" else atom.quantize(quantizer, rounding=mode_decimal)
            if operator == "above":
                hit = value > threshold
            elif operator == "at_least":
                hit = value >= threshold
            elif operator == "below":
                hit = value < threshold
            elif operator == "at_most":
                hit = value <= threshold
            else:  # equal
                hit = value == threshold
            if hit:
                payouts[row, column] = 1.0
    return payouts


def _as_payout_matrix(payouts: Any) -> np.ndarray:
    matrix = np.asarray(payouts, dtype=np.float64)
    if matrix.ndim != 2:
        raise CoherenceInputError(f"payouts must be a 2-D (M, K) matrix, got shape {matrix.shape}")
    if matrix.shape[0] < 1 or matrix.shape[1] < 1:
        raise CoherenceInputError(f"payouts is empty with shape {matrix.shape}")
    if not np.isfinite(matrix).all():
        bad = np.argwhere(~np.isfinite(matrix))
        raise CoherenceInputError(
            f"payouts contains non-finite entries at row/column {bad.tolist()[:5]}"
        )
    if (matrix < 0.0).any() or (matrix > 1.0).any():
        raise CoherenceInputError(
            "payouts entries must lie in [0, 1]; exceptional payouts widen the range "
            "but never leave it"
        )
    return matrix


def _as_quotes(values: Any, *, field: str, rows: int) -> np.ndarray:
    if values is None:
        raise CoherenceInputError(
            f"{field} is None; apply the valid-quote and synchronization gates before "
            "calling coherence_distance"
        )
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1:
        raise CoherenceInputError(f"{field} must be a 1-D vector, got shape {array.shape}")
    if array.shape[0] != rows:
        raise CoherenceInputError(
            f"{field} has length {array.shape[0]} but payouts has {rows} rows"
        )
    if not np.isfinite(array).all():
        bad = np.argwhere(~np.isfinite(array)).ravel()
        raise CoherenceInputError(
            f"{field} has missing or non-finite quotes at indices {bad.tolist()[:5]}; a "
            "one-sided or stale book must be excluded by the caller"
        )
    return array


def _box_distance(projected: np.ndarray, bids: np.ndarray, asks: np.ndarray) -> np.ndarray:
    return np.maximum.reduce(
        [
            np.maximum(bids - projected, 0.0),
            np.maximum(projected - asks, 0.0),
            np.zeros_like(projected),
        ]
    )


def coherence_distance(
    payouts: Any,
    bids: Any,
    asks: Any,
    *,
    feasible_tolerance: float = 1e-9,
) -> dict[str, Any]:
    """Min-max distance from the coherent price set to the quoted box.

    Solves ``min_{pi in Simplex(K-1)} max_m dist((A pi)_m, [b_m, a_m])`` as a
    linear program in the simplex weights ``pi`` and a scalar bound ``t``:
    minimize ``t`` subject to ``A pi - t <= a``, ``-A pi - t <= -b``,
    ``sum(pi) = 1``, ``pi >= 0`` and ``t >= 0``.

    ``bids`` and ``asks`` must already be synchronized valid quotes for the
    same ``M`` rows as ``payouts``, with ``bid <= ask``. Missing or crossed
    rows are rejected rather than repaired, because repairing them would
    manufacture coherence the caller did not observe.

    Returns a dict with the contract keys plus explicitly named extras:

    ``distance``
        The min-max distance ``d`` in probability points; ``0`` when the box
        meets the coherent set.
    ``feasible``
        ``distance <= feasible_tolerance``. Feasibility is a statement about
        the quotes under the stated payout assumptions, not an arbitrage.
    ``probabilities``
        The minimizing simplex weights ``pi`` of length ``K``.
    ``projected``
        The coherent price vector ``A pi`` of length ``M``.
    ``midpoint_distance`` / ``midpoint_feasible``
        The same diagnostic run on the raw midpoint box (a point interval per
        row), so the constrained projection can be compared with the raw
        midpoint diagnostic.
    ``residual_max``
        Sup norm of the achieved violation, recomputed from ``projected`` as
        an independent check on the solver's objective value.
    ``solver``
        ``{method, status, message, iterations}`` for provenance.
    """
    matrix = _as_payout_matrix(payouts)
    rows, atoms_count = matrix.shape
    lower = _as_quotes(bids, field="bids", rows=rows)
    upper = _as_quotes(asks, field="asks", rows=rows)
    tolerance = float(feasible_tolerance)
    if not math.isfinite(tolerance) or tolerance < 0.0:
        raise CoherenceInputError(
            f"feasible_tolerance={feasible_tolerance!r} must be finite and non-negative"
        )
    crossed = np.argwhere(lower > upper).ravel()
    if crossed.size:
        preview = [(int(index), float(lower[index]), float(upper[index])) for index in crossed[:5]]
        raise CoherenceInputError(
            f"bids exceed asks at (row, bid, ask) {preview}; a crossed book is not a quoted box"
        )

    def solve(
        lower_bound: np.ndarray, upper_bound: np.ndarray
    ) -> tuple[float, np.ndarray, np.ndarray, dict[str, Any]]:
        """Minimize the max coordinate violation of the box ``[lower, upper]``.

        Rows ``0..M-1`` enforce ``A pi - t <= upper`` and rows ``M..2M-1``
        enforce ``-A pi - t <= -lower``, so ``t`` bounds the distance above the
        ask and below the bid in one linear program.
        """
        variables = atoms_count + 1
        constraint = np.zeros((2 * rows, variables), dtype=np.float64)
        constraint[:rows, :atoms_count] = matrix
        constraint[:rows, atoms_count] = -1.0
        constraint[rows:, :atoms_count] = -matrix
        constraint[rows:, atoms_count] = -1.0
        bounds_vector = np.concatenate([upper_bound, -lower_bound])
        objective = np.zeros(variables, dtype=np.float64)
        objective[atoms_count] = 1.0
        equality = np.zeros((1, variables), dtype=np.float64)
        equality[0, :atoms_count] = 1.0
        result = linprog(
            objective,
            A_ub=constraint,
            b_ub=bounds_vector,
            A_eq=equality,
            b_eq=np.array([1.0]),
            bounds=[(0.0, 1.0)] * atoms_count + [(0.0, None)],
            method="highs",
        )
        if not result.success or result.x is None:
            raise CoherenceSolverError(
                f"coherence program failed (status={result.status}, "
                f"message={result.message!r}); no distance is reported"
            )
        weights = np.clip(np.asarray(result.x[:atoms_count], dtype=np.float64), 0.0, 1.0)
        total = float(weights.sum())
        if total <= 0.0:  # pragma: no cover - blocked by the simplex equality
            raise CoherenceSolverError("solver returned non-positive simplex weight mass")
        weights = weights / total
        projected = matrix @ weights
        info = {
            "method": "highs",
            "status": int(result.status),
            "message": str(result.message),
            "iterations": int(getattr(result, "nit", 0) or 0),
        }
        return float(result.fun), weights, projected, info

    distance, probabilities, projected, solver = solve(lower, upper)
    residual = float(np.max(_box_distance(projected, lower, upper))) if rows else 0.0
    if not math.isclose(distance, residual, rel_tol=1e-7, abs_tol=1e-9):
        raise CoherenceSolverError(
            f"solver objective {distance!r} disagrees with the achieved violation "
            f"{residual!r}; refusing to report an unverified distance"
        )
    midpoint = 0.5 * (lower + upper)
    midpoint_distance, midpoint_probabilities, midpoint_projected, _ = solve(midpoint, midpoint)
    projected_list = [float(value) for value in projected]
    return {
        "distance": distance,
        "feasible": bool(distance <= tolerance),
        "probabilities": [float(value) for value in probabilities],
        "projected": projected_list,
        "residual_max": residual,
        "feasible_tolerance": tolerance,
        "midpoint_distance": midpoint_distance,
        "midpoint_feasible": bool(midpoint_distance <= tolerance),
        "midpoint_probabilities": [float(value) for value in midpoint_probabilities],
        "midpoint_projected": [float(value) for value in midpoint_projected],
        "box": {
            "bids": [float(value) for value in lower],
            "asks": [float(value) for value in upper],
            "midpoints": [float(value) for value in midpoint],
        },
        "payouts": matrix.tolist(),
        "n_contracts": int(rows),
        "n_atoms": int(atoms_count),
        "solver": solver,
    }


def _as_finite_number(value: Any, *, name: str) -> Decimal | None:
    """Decimal value of a finite number, or ``None`` when it is not finite."""
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, (float, np.floating)):
        as_float = float(value)
        if not math.isfinite(as_float):
            return None
        number = Decimal(str(as_float))
    else:
        number = Decimal(int(value))
    return number


def _label(value: Any) -> str:
    if isinstance(value, Decimal):
        return format(value.normalize(), "f") if value == value.to_integral_value() else str(value)
    if isinstance(value, np.generic):  # pragma: no cover - defensive
        return str(value.item())
    return str(value)


def _report_value(value: Any) -> Any:
    if value is _MISSING:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _read(record: Any, field: str) -> Any:
    try:
        value = getattr(record, field)
    except AttributeError:
        return _MISSING
    if value is None:
        return _MISSING
    if isinstance(value, str) and not value.strip():
        return _MISSING
    return value


def _same(left: Any, right: Any) -> bool:
    """Total equality that never raises on mixed numeric and non-numeric values."""
    if isinstance(left, np.ndarray) or isinstance(right, np.ndarray):
        return bool(np.array_equal(np.asarray(left), np.asarray(right)))
    numeric_left = isinstance(left, (Decimal, int, float, np.number)) and not isinstance(left, bool)
    numeric_right = isinstance(right, (Decimal, int, float, np.number)) and not isinstance(
        right, bool
    )
    if numeric_left != numeric_right:
        return False
    if not numeric_left:
        return bool(left == right)
    left_number = _as_finite_number(left, name="left")
    right_number = _as_finite_number(right, name="right")
    if left_number is None or right_number is None:
        return format(left, "") == format(right, "")
    return bool(left_number == right_number)


def exact_match(left: Contract, right: Contract) -> dict[str, Any]:
    """Field-by-field rule audit between two candidate-equivalent contracts.

    Compares every rule field listed in :data:`COHERENCE_FIELDS` and reports
    ``matches`` plus the ``differences`` that caused a mismatch. Similar
    titles or identifiers never certify a match, and equal missing metadata is
    not a verified match either. Fields in :data:`REQUIRED_SEMANTIC_FIELDS`
    that are absent on *both* records produce ``matches=False`` with an
    explicit missing-required-semantic-field reason, because two blank rules
    cannot be shown to be the same rule. An absent field always differs from a
    present one.

    A genuinely inapplicable bound is not an unknown rule: ``threshold`` is
    not required for ``range`` and ``binary`` operators, and ``lower`` and
    ``upper`` are not required for scalar-threshold operators. Those absences
    are recorded as notes.

    ``rule_hash``, ``contract_id`` and the lifecycle times are blocking only
    within one venue, since each venue defines its own identifiers and
    schedules; across venues they are recorded as notes. Equal rule hashes are
    therefore strong evidence, but unequal hashes across venues are not by
    themselves a rule difference.

    Returns a dict with the contract keys ``matches`` and ``differences`` plus
    ``matched_fields``, ``missing_fields``, ``missing_reasons``,
    ``blocking_differences``, ``notes``, ``cross_venue`` and
    ``compared_fields``.
    """
    cross_venue = _read(left, "venue") != _read(right, "venue")
    differences: list[dict[str, Any]] = []
    matched_fields: list[str] = []
    notes: list[str] = []
    missing_fields: list[str] = []
    missing_reasons: list[str] = []

    for field in COHERENCE_FIELDS:
        left_value = _read(left, field)
        right_value = _read(right, field)
        absent_left = left_value is _MISSING
        absent_right = right_value is _MISSING
        if absent_left and absent_right:
            if field in REQUIRED_SEMANTIC_FIELDS:
                missing_fields.append(field)
                missing_reasons.append(
                    f"missing-required-semantic-field: {field} is absent on both records, so "
                    "the rule cannot be verified as identical"
                )
            else:
                notes.append(f"both records omit {field}; no claim is made about it")
            continue
        if absent_left or absent_right:
            present, absent = (right_value, "left") if absent_left else (left_value, "right")
            blocking = not (field in _SAME_VENUE_FIELDS and cross_venue)
            differences.append(
                {
                    "field": field,
                    "left": _report_value(left_value),
                    "right": _report_value(right_value),
                    "blocking": blocking,
                    "kind": "presence",
                    "reason": f"{field} is present on the {('right' if absent == 'left' else 'left')} "
                    f"record as {_label(present)!r} but absent on the {absent} record",
                }
            )
            continue
        if _same(left_value, right_value):
            matched_fields.append(field)
            continue
        blocking = field not in _NON_BLOCKING_FIELDS and not (
            field in _SAME_VENUE_FIELDS and cross_venue
        )
        differences.append(
            {
                "field": field,
                "left": _report_value(left_value),
                "right": _report_value(right_value),
                "blocking": blocking,
                "kind": "value",
                "reason": f"{field} differs: {_label(left_value)!r} != {_label(right_value)!r}",
            }
        )

    operator = _read(left, "operator")
    operator_text = str(operator).strip().lower() if operator is not _MISSING else ""
    atom_free = operator_text in _ATOM_FREE_OPERATORS
    scalar = operator_text in _THRESHOLD_OPERATORS
    # Absence of a bound is judged by what the operator needs, not by a fixed
    # field list: a scalar comparison needs its threshold, and a bucket or
    # outcome-map contract needs bounds (or an explicit payout row) instead.
    for field in ("threshold", "lower", "upper"):
        if _read(left, field) is not _MISSING or _read(right, field) is not _MISSING:
            continue
        needs_field = (field == "threshold" and scalar) or (
            field in ("lower", "upper") and atom_free
        )
        if not needs_field:
            notes.append(
                f"{field} is inapplicable for operator "
                f"{operator_text or 'unknown'!r} on both records"
            )
            continue
        missing_fields.append(field)
        missing_reasons.append(
            f"missing-required-semantic-field: {field} is absent on both records and is "
            f"needed to define the payoff under operator {operator_text!r}"
        )

    blocking_differences = [entry["field"] for entry in differences if entry["blocking"]]
    for field in blocking_differences:
        missing_reasons.append(f"blocking-difference: {field} differs between the records")
    matches = not missing_reasons and not blocking_differences
    return {
        "matches": bool(matches),
        "differences": differences,
        "blocking_differences": blocking_differences,
        "matched_fields": matched_fields,
        "missing_fields": missing_fields,
        "missing_reasons": missing_reasons,
        "notes": notes,
        "cross_venue": bool(cross_venue),
        "compared_fields": list(COHERENCE_FIELDS),
    }
