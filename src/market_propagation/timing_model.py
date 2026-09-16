"""Timing-only response baseline: declared calendar and pre-event covariates.

The forecast ladder in :mod:`market_propagation.models` declares covariate
tuples whose kinds above ``own`` read ``shock`` and ``delayed_shock``. No
release expectation is verified for the external-history releases and the
pipeline configuration prohibits filling those columns with an invented number,
so on this cohort the ladder cannot be fit at all without fabricating an input.
This module supplies the baseline the configuration does declare: a nested
ladder assembled only from the release horizon, the release family, and the
pre-event price and activity state of a contract, every one of them measured
strictly before the release.

Three properties here are structural rather than conventional. The rungs are cut
as cumulative prefixes of the configured admissible covariate tuple, so a rung's
covariate set contains the previous rung's by construction and no rung can drift
out of the ladder. Every weight, mean and resampling unit is the economic
release, so contracts inside one release share that release's weight and
duplicating a release's rows cannot manufacture precision. And a prohibited
covariate is refused when it is read rather than when it is present, because the
design projects the declared columns and nothing else, so a full panel row
carrying an unverified ``shock`` column fits identically to the same row without
it.

The penalty grid is ``models.DEFAULT_ALPHAS``, the same declared grid the
forecast ladder tunes on, so a rung-to-rung difference is a covariate difference
and not a tuning difference. The predictor is the identity link on the response
itself: the target is already a probability-point change, and the bounded-price
geometry the forecast ladder would express through ``current_price`` is carried
here by declared covariates instead, so no bounded mapping is applied and none
is reported.

Only timing information is admitted, so nothing here carries a causal label and
no coefficient is an estimate of propagation. The paired comparison resamples
whole releases; a bootstrap that resampled rows inside a release would be pseudo
replication, because rows sharing a release share that release's shock and its
error component.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from .evaluation import (
    ExclusionLedger,
    ForecastEvaluationError,
    chronological_splits,
    classify_outcome,
    cluster_bootstrap,
    forecast_scores,
)
from .models import DEFAULT_ALPHAS, FEATURE_SPECS, FoldSplit, ForecastDataError
from .storage import TRADE_PANEL_COLUMNS

__all__ = [
    "AGGREGATION_UNIT",
    "COVARIATE_BOUNDARY_PROXIMITY",
    "COVARIATE_BOUNDED_PRICE_CURVATURE",
    "COVARIATE_FAMILY_INDICATOR",
    "COVARIATE_HORIZON",
    "COVARIATE_PRE_EVENT_PRICE",
    "COVARIATE_PRE_RELEASE_ACTIVITY",
    "DEFAULT_BOOTSTRAP_SAMPLES",
    "DEFAULT_CONFIG_PATH",
    "DEFAULT_COVERAGE",
    "DEFAULT_TIMING_ALPHAS",
    "DEFAULT_TIMING_FAMILIES",
    "DEFAULT_TIMING_SEED",
    "DERIVED_COVARIATES",
    "FAMILY_INDICATOR_PREFIX",
    "FORECAST_LADDER_COVARIATES",
    "MEANINGFUL_SIZE_STATUS",
    "PANEL_COLUMNS_READ",
    "POST_EVENT_PANEL_COLUMNS",
    "PROHIBITED_COVARIATES",
    "REQUIRED_SPEC_KEYS",
    "RESPONSE_COLUMN",
    "SOURCE_COLUMNS",
    "SUPPORTED_LOSS",
    "TIMING_ADMISSIBLE_COVARIATES",
    "TIMING_CONFIG_BLOCK",
    "TIMING_FEATURE_SPECS",
    "TIMING_LADDER_BASELINE_KIND",
    "TIMING_LADDER_COVARIATES",
    "TIMING_LADDER_KINDS",
    "TIMING_MODEL_KIND",
    "TIMING_PROHIBITED_COVARIATES",
    "ProhibitedCovariateError",
    "TimingComparison",
    "TimingDesign",
    "TimingFit",
    "TimingModelError",
    "TimingSpec",
    "add_derived_covariates",
    "assert_permitted_covariates",
    "build_timing_features",
    "compare_timing_kinds",
    "event_weighted_mae",
    "expanded_covariate_columns",
    "family_level_terms",
    "feature_columns",
    "fit_timing_model",
    "load_timing_spec",
    "release_labels",
    "release_weights",
    "score_timing_fit",
]


class TimingModelError(ForecastDataError):
    """The supplied rows, specification or configuration violate the timing contract."""


class ProhibitedCovariateError(TimingModelError):
    """A covariate this module refuses to read as a predictor was requested."""


#: Kind of the whole baseline, as ``timing_only.model_kind`` declares it. The
#: ladder rungs below are kinds of their own; this labels the specification.
TIMING_MODEL_KIND = "timing_only"

#: Configuration block that defines the baseline. The block is the
#: specification, so its absence is an error rather than a fallback.
TIMING_CONFIG_BLOCK = "timing_only"

#: Pipeline configuration the loader reads by default.
DEFAULT_CONFIG_PATH = "configs/external_history_v1.yaml"

#: Penalty grid, taken from ``timing_only.alphas``. It is the same declared grid
#: as ``models.DEFAULT_ALPHAS``, so a rung fitted here and a forecast-ladder kind
#: are tuned on one grid and an accuracy difference is not a grid difference.
#: Callers import this constant, and ``load_timing_spec`` refuses a block that
#: declares a different grid rather than let the two disagree.
DEFAULT_TIMING_ALPHAS: tuple[float, ...] = DEFAULT_ALPHAS

#: Families whose coefficients are reported separately, from
#: ``timing_only.coefficient_families``. A configuration that adds or renames a
#: family is refused rather than quietly analyzed under this expansion.
DEFAULT_TIMING_FAMILIES: tuple[str, ...] = ("cpi", "employment")

#: Seed declared under ``reproducibility.seeds`` for runs of this pipeline.
DEFAULT_TIMING_SEED = 20260915

#: Bootstrap replicate count, matching the default of
#: :func:`market_propagation.evaluation.clustered_bootstrap`.
DEFAULT_BOOTSTRAP_SAMPLES = 200

#: Interval coverage for every reported uncertainty.
DEFAULT_COVERAGE = 0.95

#: The loss the configuration declares, and the only one this module computes.
SUPPORTED_LOSS = "event_weighted_mean_absolute_error"

#: The independent unit every weight, mean and resample uses.
AGGREGATION_UNIT = "economic_release"

#: Status the configuration declares for the two meaningful sizes: scientific
#: choices awaiting a power assessment at this study's event count, never
#: measured capabilities. This module refuses to report them as anything else.
MEANINGFUL_SIZE_STATUS = "scientific_choices_requiring_power_assessment"

COVARIATE_HORIZON = "horizon_seconds"
COVARIATE_FAMILY_INDICATOR = "family_indicator"
COVARIATE_PRE_EVENT_PRICE = "pre_event_price"
COVARIATE_BOUNDED_PRICE_CURVATURE = "bounded_price_curvature"
COVARIATE_BOUNDARY_PROXIMITY = "boundary_proximity"
COVARIATE_PRE_RELEASE_ACTIVITY = "pre_release_activity"

#: Admissible covariate tuple, in the order ``timing_only.admissible_covariates``
#: lists. The order is load-bearing: the ladder is cut as prefixes of this tuple,
#: so reordering it reorders the rungs.
TIMING_ADMISSIBLE_COVARIATES: tuple[str, ...] = (
    COVARIATE_HORIZON,
    COVARIATE_FAMILY_INDICATOR,
    COVARIATE_PRE_EVENT_PRICE,
    COVARIATE_BOUNDED_PRICE_CURVATURE,
    COVARIATE_BOUNDARY_PROXIMITY,
    COVARIATE_PRE_RELEASE_ACTIVITY,
)

#: Covariates this module constructs rather than reads. ``pre_event_price`` is a
#: declared pass-through of the panel's baseline price and is not listed here.
DERIVED_COVARIATES: tuple[str, ...] = (
    COVARIATE_FAMILY_INDICATOR,
    COVARIATE_BOUNDED_PRICE_CURVATURE,
    COVARIATE_BOUNDARY_PROXIMITY,
)

#: Prefix of the per-family indicator columns.
FAMILY_INDICATOR_PREFIX = "family_indicator"

#: Panel column each covariate is read from. ``pre_event_price`` is the panel's
#: baseline transaction price on the declared payout axis, which the frozen
#: contract fixes as a probability on the 0-1 scale; the two geometry terms are
#: functions of that same price and therefore name the same source column.
#: ``pre_release_activity`` is the panel's own count of the transactions behind
#: the baseline, which is the only pre-event activity count the frozen panel
#: schema declares.
SOURCE_COLUMNS: Mapping[str, str] = {
    COVARIATE_HORIZON: COVARIATE_HORIZON,
    COVARIATE_FAMILY_INDICATOR: "family",
    COVARIATE_PRE_EVENT_PRICE: "baseline",
    COVARIATE_BOUNDED_PRICE_CURVATURE: "baseline",
    COVARIATE_BOUNDARY_PROXIMITY: "baseline",
    COVARIATE_PRE_RELEASE_ACTIVITY: "baseline_trade_count",
}

#: Target column of the panel: the absolute probability change from the row's
#: own baseline to the endpoint its declared horizon reaches.
RESPONSE_COLUMN = "response"

#: Panel columns the design reads, and the only ones it may read.
PANEL_COLUMNS_READ: tuple[str, ...] = (
    "event_id",
    "cluster_id",
    "contract_id",
    "family",
    "event_time",
    "horizon_seconds",
    "baseline",
    "baseline_trade_count",
    "response",
    "valid",
    "exclusion_reason",
)

#: Covariates the configuration prohibits and this module refuses to read. The
#: last entry is the configuration's own catch-all name for any post-event
#: quantity or price; the concrete panel columns that realize it are derived
#: below rather than spelled out here, so the prohibition does not depend on
#: someone remembering to extend a hand-written list.
PROHIBITED_COVARIATES: tuple[str, ...] = (
    "shock",
    "delayed_shock",
    "surprise",
    "expectation_gap",
    "winning_outcome_label",
    "resolution_status",
    "any_post_event_quantity_or_price",
)

#: Panel columns whose value is measured at or after the release. They are
#: derived from the frozen panel schema rather than listed by hand, so a
#: post-event column added to the panel later is prohibited here automatically
#: instead of admitted by omission.
POST_EVENT_PANEL_COLUMNS: tuple[str, ...] = tuple(
    name
    for name in TRADE_PANEL_COLUMNS
    if name.startswith(("endpoint", "tie_group_response", "post_release"))
)

#: Everything the design refuses to read, checked by name on the way in. The
#: response column is named as well: it is the target, and a target read as a
#: predictor would make the fit a restatement of the label.
TIMING_PROHIBITED_COVARIATES: tuple[str, ...] = (
    *PROHIBITED_COVARIATES,
    RESPONSE_COLUMN,
    *POST_EVENT_PANEL_COLUMNS,
)

#: Covariate names the forecast ladder reads. The timing tuple is disjoint from
#: it by construction, which is what makes this a second specification rather
#: than a renaming of the ladder; the tests assert the disjointness.
FORECAST_LADDER_COVARIATES: tuple[str, ...] = tuple(
    sorted({name for names in FEATURE_SPECS.values() for name in names})
)

#: Ladder kinds, one per admissible covariate, in the order the configuration
#: lists them: the rung named for a covariate is that covariate together with
#: every covariate before it. Names are derived from the covariates, so a rung
#: cannot drift out of step with its covariate set.
TIMING_LADDER_KINDS: tuple[str, ...] = tuple(
    f"timing_{covariate}" for covariate in TIMING_ADMISSIBLE_COVARIATES
)

#: Covariate set of each rung, as cumulative prefixes of the admissible tuple.
TIMING_LADDER_COVARIATES: Mapping[str, tuple[str, ...]] = {
    f"timing_{covariate}": TIMING_ADMISSIBLE_COVARIATES[: position + 1]
    for position, covariate in enumerate(TIMING_ADMISSIBLE_COVARIATES)
}

#: The rung every richer rung is compared against: release structure alone.
TIMING_LADDER_BASELINE_KIND = TIMING_LADDER_KINDS[0]

#: Covariates whose meaning the configuration does not restate, because each is
#: the panel's own column read directly: the horizon is the row's declared
#: horizon, and the family indicator is a one-hot of the row's declared family.
#: Every other admissible covariate needs prose in ``timing_only.definitions`` so
#: it can be checked against the meaning the configuration gave it.
STRUCTURAL_COVARIATES: tuple[str, ...] = (COVARIATE_HORIZON, COVARIATE_FAMILY_INDICATOR)

#: Covariates the configuration must define in prose.
DEFINED_COVARIATES: tuple[str, ...] = tuple(
    name for name in TIMING_ADMISSIBLE_COVARIATES if name not in STRUCTURAL_COVARIATES
)

#: Fragment each declared definition must contain, whitespace-normalized. The
#: loader compares the configuration's own prose against these, so a
#: configuration that restates a covariate's formula without the code changing
#: with it is refused rather than analyzed under the older formula.
EXPECTED_DEFINITION_FRAGMENTS: Mapping[str, str] = {
    COVARIATE_PRE_EVENT_PRICE: "probability on the 0-1 scale",
    COVARIATE_BOUNDED_PRICE_CURVATURE: "p * (1 - p)",
    COVARIATE_BOUNDARY_PROXIMITY: "min(p, 1 - p)",
    COVARIATE_PRE_RELEASE_ACTIVITY: "count of pre-window transactions",
}

#: Keys the ``timing_only`` block must declare. A missing key is an error rather
#: than a default, because a specification completed by the loader is not the
#: specification the configuration declared.
REQUIRED_SPEC_KEYS: tuple[str, ...] = (
    "model_kind",
    "purpose",
    "admissible_covariates",
    "definitions",
    "prohibited_covariates",
    "prohibited_note",
    "aggregation_unit",
    "equal_weight_economic_events",
    "contracts_inside_an_event_share_its_weight",
    "separate_by_family",
    "coefficient_families",
    "family_coefficients_reported_separately",
    "alphas",
    "loss",
    "meaningful_response_size",
    "meaningful_gain_size",
    "meaningful_size_status",
    "surprise_slopes_enabled",
    "missing_surprise_effect",
    "alternative_weights_note",
)


def expanded_covariate_columns(
    covariates: Sequence[str], *, families: Sequence[str] = DEFAULT_TIMING_FAMILIES
) -> tuple[str, ...]:
    """Design column names for a covariate tuple.

    ``family_indicator`` expands to one indicator per declared family, because
    the configuration reports family coefficients separately: one indicator per
    family makes each family's coefficient a level of its own instead of a
    contrast a reader has to reconstruct against an arbitrary reference family.
    """
    names: list[str] = []
    for covariate in covariates:
        if covariate == COVARIATE_FAMILY_INDICATOR:
            names.extend(f"{FAMILY_INDICATOR_PREFIX}_{family}" for family in families)
        else:
            names.append(covariate)
    return tuple(names)


def family_level_terms(
    covariates: Sequence[str], *, families: Sequence[str] = DEFAULT_TIMING_FAMILIES
) -> tuple[str, ...]:
    """Level terms a covariate tuple carries, which take the intercept's place.

    The family indicators span the constant, so when they are present they take
    the intercept's place rather than sitting beside it. That keeps the design
    full rank while each family coefficient stays the fitted response level for
    that family, and it is what makes the rungs nested model spaces: the previous
    rung's level term lies in the span of the family block.
    """
    if COVARIATE_FAMILY_INDICATOR in covariates:
        return tuple(f"{FAMILY_INDICATOR_PREFIX}_{family}" for family in families)
    return ("intercept",)


def feature_columns(
    covariates: Sequence[str], *, families: Sequence[str] = DEFAULT_TIMING_FAMILIES
) -> tuple[str, ...]:
    """Full design column list for a covariate tuple, level terms first."""
    level = family_level_terms(covariates, families=families)
    expanded = expanded_covariate_columns(covariates, families=families)
    return (*level, *(name for name in expanded if name not in level))


#: Feature columns of each rung, mirroring ``models.FEATURE_SPECS``. Each rung's
#: covariate set contains the previous rung's, and its design matrix has a
#: strictly larger rank on rows where the added covariate varies.
TIMING_FEATURE_SPECS: Mapping[str, tuple[str, ...]] = {
    kind: feature_columns(covariates) for kind, covariates in TIMING_LADDER_COVARIATES.items()
}


def assert_permitted_covariates(covariates: Sequence[str], *, context: str) -> tuple[str, ...]:
    """Return the covariates unchanged, or refuse to read a prohibited one.

    Presence is not the test. A row may carry an unverified ``shock`` column and
    still be admissible, because the design never reads a column it did not
    declare; reading one is the error, and it is raised here so a caller cannot
    fit a specification the configuration prohibits by asking for it.
    """
    names = tuple(str(covariate) for covariate in covariates)
    prohibited = sorted({name for name in names if name in TIMING_PROHIBITED_COVARIATES})
    if prohibited:
        raise ProhibitedCovariateError(
            f"{context} requests prohibited covariate(s) {prohibited}. The configuration "
            f"prohibits reading {list(PROHIBITED_COVARIATES)} and every panel column measured at "
            "or after the release, because no release expectation is verified for this cohort and "
            "no post-event quantity is a pre-event predictor. An absent expectation disables only "
            "a surprise slope, and a timing-only specification has none."
        )
    unknown = sorted({name for name in names if name not in TIMING_ADMISSIBLE_COVARIATES})
    if unknown:
        raise TimingModelError(
            f"{context} requests covariate(s) {unknown} that this module does not implement. The "
            f"implemented set is {list(TIMING_ADMISSIBLE_COVARIATES)}; an unimplemented covariate "
            "is refused rather than approximated by a column of the same name."
        )
    return names


@dataclass(frozen=True, slots=True)
class TimingSpec:
    """The declared timing-only specification, read from the configuration."""

    model_kind: str
    purpose: str
    admissible_covariates: tuple[str, ...]
    definitions: Mapping[str, str]
    prohibited_covariates: tuple[str, ...]
    prohibited_note: str
    aggregation_unit: str
    equal_weight_economic_events: bool
    contracts_inside_an_event_share_its_weight: bool
    separate_by_family: bool
    coefficient_families: tuple[str, ...]
    family_coefficients_reported_separately: bool
    alphas: tuple[float, ...]
    loss: str
    meaningful_response_size: float
    meaningful_gain_size: float
    meaningful_size_status: str
    surprise_slopes_enabled: bool
    missing_surprise_effect: str
    alternative_weights_note: str
    reproducibility_seeds: tuple[int, ...]
    source_path: str

    def meaningful_sizes(self) -> dict[str, Any]:
        """The two meaningful sizes as declared choices awaiting a power assessment.

        They are reported this way on purpose. A size that reads like an achieved
        precision invites a reader to compare a measured gain against a number
        that was never estimated from this cohort's event count.
        """
        return {
            "meaningful_response_size": self.meaningful_response_size,
            "meaningful_gain_size": self.meaningful_gain_size,
            "status": self.meaningful_size_status,
            "measured": False,
            "power_assessment": None,
            "note": (
                "declared scientific choices in probability points, carried into the comparison as "
                "the relevance threshold an interval is classified against; they are not measured "
                "capabilities of this cohort and no power assessment has been run for them here"
            ),
        }

    def covariates_for(self, kind: str) -> tuple[str, ...]:
        """Covariate tuple a ladder rung declares, checked against this specification."""
        if kind not in TIMING_LADDER_COVARIATES:
            raise TimingModelError(
                f"unknown timing rung {kind!r}; the ladder is {list(TIMING_LADDER_KINDS)}"
            )
        declared = TIMING_LADDER_COVARIATES[kind]
        unadmitted = [name for name in declared if name not in self.admissible_covariates]
        if unadmitted:  # pragma: no cover - guarded by load_timing_spec
            raise TimingModelError(
                f"rung {kind!r} needs covariate(s) {unadmitted} that this configuration does not "
                f"admit; it admits {list(self.admissible_covariates)}"
            )
        return declared

    def as_record(self) -> dict[str, Any]:
        return {
            "model_kind": self.model_kind,
            "purpose": self.purpose,
            "admissible_covariates": list(self.admissible_covariates),
            "definitions": dict(self.definitions),
            "prohibited_covariates": list(self.prohibited_covariates),
            "prohibited_note": self.prohibited_note,
            "aggregation_unit": self.aggregation_unit,
            "equal_weight_economic_events": self.equal_weight_economic_events,
            "contracts_inside_an_event_share_its_weight": (
                self.contracts_inside_an_event_share_its_weight
            ),
            "separate_by_family": self.separate_by_family,
            "coefficient_families": list(self.coefficient_families),
            "family_coefficients_reported_separately": (
                self.family_coefficients_reported_separately
            ),
            "alphas": list(self.alphas),
            "loss": self.loss,
            "meaningful_sizes": self.meaningful_sizes(),
            "surprise_slopes_enabled": self.surprise_slopes_enabled,
            "missing_surprise_effect": self.missing_surprise_effect,
            "alternative_weights_note": self.alternative_weights_note,
            "reproducibility_seeds": list(self.reproducibility_seeds),
            "source_path": self.source_path,
        }


def _require_mapping(value: Any, *, key: str, path: Path) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.{key} in {path} must be a mapping, got {type(value).__name__}"
        )
    return value


def _text(block: Mapping[str, Any], key: str, *, path: Path) -> str:
    value = block[key]
    if not isinstance(value, str) or not value.strip():
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.{key} in {path} must be a non-empty string, got {value!r}"
        )
    return value.strip()


def _text_tuple(block: Mapping[str, Any], key: str, *, path: Path) -> tuple[str, ...]:
    value = block[key]
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or not value:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.{key} in {path} must be a non-empty list of names, "
            f"got {value!r}"
        )
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise TimingModelError(
                f"{TIMING_CONFIG_BLOCK!r}.{key} in {path} must hold names, got {item!r}"
            )
        items.append(item.strip())
    return tuple(items)


def _flag(block: Mapping[str, Any], key: str, *, path: Path) -> bool:
    value = block[key]
    if not isinstance(value, bool):
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.{key} in {path} must be a boolean, got {value!r}"
        )
    return value


def _number(block: Mapping[str, Any], key: str, *, path: Path) -> float:
    value = block[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.{key} in {path} must be a number, got {value!r}"
        )
    if not math.isfinite(float(value)):
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.{key} in {path} must be finite, got {value!r}"
        )
    return float(value)


def _alpha_grid(block: Mapping[str, Any], *, path: Path) -> tuple[float, ...]:
    raw = block["alphas"]
    if isinstance(raw, (str, bytes)) or not isinstance(raw, Sequence) or not raw:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.alphas in {path} must be a non-empty list of penalties, "
            f"got {raw!r}"
        )
    values: list[float] = []
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(item):
            raise TimingModelError(
                f"{TIMING_CONFIG_BLOCK!r}.alphas in {path} must hold finite numbers, got {item!r}"
            )
        if float(item) < 0.0:
            raise TimingModelError(
                f"{TIMING_CONFIG_BLOCK!r}.alphas in {path} must be non-negative, got {item!r}"
            )
        values.append(float(item))
    return tuple(values)


def load_timing_spec(config_path: str | Path = DEFAULT_CONFIG_PATH) -> TimingSpec:
    """Read the ``timing_only`` block, failing when it is absent.

    The block is the specification, so this loader never supplies one of its own.
    A configuration with no block, or with a block that omits a declared key, is
    an error: the alternative is a specification nobody declared silently
    standing in for the one the caller asked for.

    A block that disagrees with a module constant derived from it is an error
    too. The covariate tuple, the family list and the penalty grid are imported
    by callers rather than re-derived, so a configuration that changes one of
    them while the constant keeps the old value must be refused rather than
    analyzed under whichever of the two the reader did not see.
    """
    path = Path(config_path)
    if not path.is_file():
        raise TimingModelError(f"pipeline configuration {path} does not exist")
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise TimingModelError(f"pipeline configuration {path} is not valid YAML: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise TimingModelError(f"pipeline configuration {path} is not a mapping of blocks")
    block = payload.get(TIMING_CONFIG_BLOCK)
    if block is None:
        raise TimingModelError(
            f"pipeline configuration {path} declares no {TIMING_CONFIG_BLOCK!r} block. The "
            "timing-only baseline is defined by that block alone: with no block there is no "
            "declared covariate tuple, loss, penalty grid or aggregation unit, and this loader "
            "will not substitute a specification nobody declared."
        )
    block = _require_mapping(block, key=TIMING_CONFIG_BLOCK, path=path)
    missing = [key for key in REQUIRED_SPEC_KEYS if key not in block]
    if missing:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r} in {path} declares no {missing}, so the specification is "
            "incomplete; every declared key is required and none is defaulted"
        )

    model_kind = _text(block, "model_kind", path=path)
    if model_kind != TIMING_MODEL_KIND:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.model_kind in {path} is {model_kind!r}, but this module "
            f"implements {TIMING_MODEL_KIND!r} only"
        )
    admissible = _text_tuple(block, "admissible_covariates", path=path)
    if admissible != TIMING_ADMISSIBLE_COVARIATES:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.admissible_covariates in {path} is {list(admissible)}, but "
            f"the module declares {list(TIMING_ADMISSIBLE_COVARIATES)}. The ladder is cut as "
            "prefixes of that tuple, so the tuple is part of the implementation and a change "
            "belongs in both places."
        )
    prohibited = _text_tuple(block, "prohibited_covariates", path=path)
    overlap = sorted(set(admissible) & set(prohibited))
    if overlap:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r} in {path} both admits and prohibits {overlap}; a covariate "
            "cannot be a predictor and a prohibited column at once"
        )
    families = _text_tuple(block, "coefficient_families", path=path)
    if families != DEFAULT_TIMING_FAMILIES:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.coefficient_families in {path} is {list(families)}, but the "
            f"module declares {list(DEFAULT_TIMING_FAMILIES)}; the family indicator columns are "
            "built from the module constant and must be rebuilt with the configuration"
        )
    alphas = _alpha_grid(block, path=path)
    if alphas != DEFAULT_TIMING_ALPHAS:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.alphas in {path} is {list(alphas)}, but "
            f"DEFAULT_TIMING_ALPHAS is {list(DEFAULT_TIMING_ALPHAS)}; callers import that constant "
            "as the declared grid, so the two must not disagree"
        )
    loss = _text(block, "loss", path=path)
    if loss != SUPPORTED_LOSS:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.loss in {path} is {loss!r}, but this module computes "
            f"{SUPPORTED_LOSS!r} only; reporting another loss under this name would misstate the "
            "estimand"
        )
    aggregation_unit = _text(block, "aggregation_unit", path=path)
    if aggregation_unit != AGGREGATION_UNIT:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.aggregation_unit in {path} is {aggregation_unit!r}, but "
            f"every weight and resample here is at the {AGGREGATION_UNIT!r} level"
        )
    if not _flag(block, "equal_weight_economic_events", path=path):
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r} in {path} does not declare equal-weight economic events. "
            "This module implements equal weights only; volume weighting changes the estimand and "
            "must be declared and implemented separately."
        )
    if not _flag(block, "contracts_inside_an_event_share_its_weight", path=path):
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r} in {path} does not declare that contracts inside an event "
            "share its weight, which is the weighting this module applies"
        )
    if _flag(block, "surprise_slopes_enabled", path=path):
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r} in {path} enables surprise slopes, which this module does "
            "not implement; a surprise slope needs a verified pre-release expectation"
        )
    size_status = _text(block, "meaningful_size_status", path=path)
    if size_status != MEANINGFUL_SIZE_STATUS:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.meaningful_size_status in {path} is {size_status!r}, but "
            f"this module reports the two meaningful sizes as {MEANINGFUL_SIZE_STATUS!r} only; "
            "neither size is a measured capability"
        )
    response_size = _number(block, "meaningful_response_size", path=path)
    gain_size = _number(block, "meaningful_gain_size", path=path)
    for value, name in (
        (response_size, "meaningful_response_size"),
        (gain_size, "meaningful_gain_size"),
    ):
        if value <= 0.0:
            raise TimingModelError(
                f"{TIMING_CONFIG_BLOCK!r}.{name} in {path} must be positive, got {value!r}"
            )
    definitions = _require_mapping(block["definitions"], key="definitions", path=path)
    if not definitions:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.definitions in {path} must define the declared covariates"
        )
    for key, value in definitions.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TimingModelError(
                f"{TIMING_CONFIG_BLOCK!r}.definitions in {path} must map covariate names to the "
                f"prose that defines them, got {key!r}: {value!r}"
            )
    undeclared_definitions = sorted(set(DEFINED_COVARIATES) - set(definitions))
    if undeclared_definitions:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.definitions in {path} does not define "
            f"{undeclared_definitions}, which it admits as covariates; a covariate whose declared "
            "meaning is not written down cannot be checked against the column it is read from"
        )
    unexplained = sorted(set(definitions) - set(admissible))
    if unexplained:
        raise TimingModelError(
            f"{TIMING_CONFIG_BLOCK!r}.definitions in {path} defines {unexplained}, which it does "
            f"not admit as covariates {list(admissible)}"
        )
    for covariate, fragment in EXPECTED_DEFINITION_FRAGMENTS.items():
        declared_text = " ".join(definitions[covariate].split())
        if " ".join(fragment.split()) not in declared_text:
            raise TimingModelError(
                f"{TIMING_CONFIG_BLOCK!r}.definitions.{covariate} in {path} does not state "
                f"{fragment!r}, which is the construction this module implements; the "
                "configuration's declared meaning and the code must not disagree"
            )

    seeds: tuple[int, ...] = ()
    reproducibility = payload.get("reproducibility")
    if isinstance(reproducibility, Mapping):
        declared_seeds = reproducibility.get("seeds")
        if isinstance(declared_seeds, Sequence) and not isinstance(declared_seeds, (str, bytes)):
            seeds = tuple(int(value) for value in declared_seeds if isinstance(value, int))

    return TimingSpec(
        model_kind=model_kind,
        purpose=_text(block, "purpose", path=path),
        admissible_covariates=admissible,
        definitions={str(key): str(value) for key, value in definitions.items()},
        prohibited_covariates=prohibited,
        prohibited_note=_text(block, "prohibited_note", path=path),
        aggregation_unit=aggregation_unit,
        equal_weight_economic_events=True,
        contracts_inside_an_event_share_its_weight=True,
        separate_by_family=_flag(block, "separate_by_family", path=path),
        coefficient_families=families,
        family_coefficients_reported_separately=_flag(
            block, "family_coefficients_reported_separately", path=path
        ),
        alphas=alphas,
        loss=loss,
        meaningful_response_size=response_size,
        meaningful_gain_size=gain_size,
        meaningful_size_status=size_status,
        surprise_slopes_enabled=False,
        missing_surprise_effect=_text(block, "missing_surprise_effect", path=path),
        alternative_weights_note=_text(block, "alternative_weights_note", path=path),
        reproducibility_seeds=seeds,
        source_path=str(path),
    )


def _as_frame(rows: Any, *, context: str) -> pd.DataFrame:
    """Materialize panel rows into a frame, leaving panel column names untouched."""
    if isinstance(rows, pd.DataFrame):
        return rows.copy()
    materialised = list(rows)
    if not materialised:
        raise TimingModelError(f"{context} received no rows")
    if not all(isinstance(row, Mapping) for row in materialised):
        offenders = sorted({type(row).__name__ for row in materialised})
        raise TimingModelError(
            f"{context} expects panel rows as mappings keyed by {list(TRADE_PANEL_COLUMNS)}, "
            f"got {offenders}"
        )
    return pd.DataFrame(materialised)


def _require_columns(frame: pd.DataFrame, columns: Sequence[str], *, context: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise TimingModelError(
            f"{context} requires column(s) {missing}; present columns are {sorted(frame.columns)}"
        )


def _require_panel_columns(columns: Sequence[str], *, context: str) -> None:
    undeclared = sorted({name for name in columns if name not in TRADE_PANEL_COLUMNS})
    if undeclared:
        raise TimingModelError(
            f"{context} reads column(s) {undeclared} that the frozen trade-panel schema does not "
            f"declare; the design is built from {list(TRADE_PANEL_COLUMNS)} only"
        )


def _numeric(frame: pd.DataFrame, column: str, *, context: str) -> np.ndarray:
    if column not in frame.columns:
        raise TimingModelError(
            f"{context} needs column {column!r}, which this frame does not carry"
        )
    return pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=np.float64)


def _row_labels(frame: pd.DataFrame, mask: np.ndarray) -> str:
    """A bounded list of row identifiers, for an error a reader can act on."""
    labels: list[str] = []
    for position in np.flatnonzero(mask)[:3]:
        parts = [
            f"{column}={frame[column].iloc[position]!r}"
            for column in ("event_id", "contract_id", "horizon_seconds")
            if column in frame.columns
        ]
        labels.append("(" + ", ".join(parts) + ")")
    return ", ".join(labels)


def release_labels(frame: pd.DataFrame, *, release_column: str | None = None) -> pd.Series:
    """The economic release each row belongs to, as a string label.

    ``cluster_id`` groups cross-venue equivalents that carry different
    ``event_id`` values, so it is the release when the panel declares it. The
    event label is the fallback for a panel without the cluster column, and it is
    the unit this module weights and resamples over.
    """
    if release_column is None:
        release_column = "cluster_id" if "cluster_id" in frame.columns else "event_id"
    if release_column not in frame.columns:
        raise TimingModelError(
            "no release column: the frame carries neither 'cluster_id' nor 'event_id'"
        )
    return frame[release_column].astype(str)


def release_weights(frame: pd.DataFrame, *, release_column: str | None = None) -> pd.Series:
    """Row weights that give every economic release a total weight of one.

    Contracts inside one release share its weight, so a release with four
    observed rows contributes one unit of information rather than four. That is
    what the configuration's equal-weight aggregation means in a row-level fit,
    and it is also what makes duplicating a release's rows leave the fit
    unchanged instead of doubling that release's influence.
    """
    labels = release_labels(frame, release_column=release_column)
    counts = labels.map(labels.value_counts()).astype(np.float64)
    return (1.0 / counts).astype(np.float64)


def add_derived_covariates(
    frame: pd.DataFrame,
    *,
    families: Sequence[str] = DEFAULT_TIMING_FAMILIES,
    context: str = "add_derived_covariates",
) -> pd.DataFrame:
    """Add the declared covariates this module constructs to a panel frame.

    The baseline price is passed through under its declared name
    ``pre_event_price``. The two bounded-price geometry terms are computed from
    it: ``bounded_price_curvature`` is ``p * (1 - p)``, the width of the bounded
    interval, and ``boundary_proximity`` is ``min(p, 1 - p)``, the distance to
    the nearer absorbing state. ``family_indicator`` becomes one indicator per
    declared family, and the horizon column is carried numerically.

    The geometry terms are price covariates, not distance to a rate or inflation
    strike: an underlying-moneyness term needs an expectation and a strike in the
    same economic units, and neither is verified for these releases. A baseline
    of exactly 0 or 1 is admissible and is not a missing value; both geometry
    terms are zero there by construction, which is a real bounded-price
    observation rather than an imputation.
    """
    assert_permitted_covariates(TIMING_ADMISSIBLE_COVARIATES, context=context)
    _require_panel_columns((*PANEL_COLUMNS_READ, *SOURCE_COLUMNS.values()), context=context)
    _require_columns(frame, ("baseline", "horizon_seconds", "family"), context=context)
    out = frame.copy()
    prices = _numeric(out, "baseline", context=context)
    is_null = np.isnan(prices)
    outside = (~is_null) & ((prices < 0.0) | (prices > 1.0))
    if outside.any():
        raise TimingModelError(
            f"{context} received {int(outside.sum())} row(s) whose baseline price is outside the "
            f"admissible 0-1 probability scale at {_row_labels(out, outside)}. A baseline outside "
            "the declared payout axis is a schema or unit error, so the analysis stops instead of "
            "clipping or dropping it silently."
        )
    out[COVARIATE_PRE_EVENT_PRICE] = prices
    out[COVARIATE_BOUNDED_PRICE_CURVATURE] = np.where(is_null, np.nan, prices * (1.0 - prices))
    out[COVARIATE_BOUNDARY_PROXIMITY] = np.where(is_null, np.nan, np.minimum(prices, 1.0 - prices))
    family_values = out["family"].astype(str)
    for family in families:
        out[f"{FAMILY_INDICATOR_PREFIX}_{family}"] = (family_values == str(family)).astype(
            np.float64
        )
    out[COVARIATE_HORIZON] = pd.to_numeric(out["horizon_seconds"], errors="coerce").astype(
        np.float64
    )
    if "baseline_trade_count" in out.columns:
        out[COVARIATE_PRE_RELEASE_ACTIVITY] = pd.to_numeric(
            out["baseline_trade_count"], errors="coerce"
        ).astype(np.float64)
    return out


@dataclass(frozen=True, slots=True)
class TimingDesign:
    """One ladder rung's admissible design over one restricted set of rows."""

    kind: str
    model_kind: str
    covariates: tuple[str, ...]
    derived_covariates: tuple[str, ...]
    columns: tuple[str, ...]
    level_terms: tuple[str, ...]
    penalized_columns: tuple[str, ...]
    source_columns: Mapping[str, str]
    target_column: str
    release_column: str
    frame: pd.DataFrame
    exclusions: Mapping[str, int]
    n_rows: int
    n_events: int
    n_releases: int
    flags: tuple[str, ...]
    notes: tuple[str, ...]

    def matrix(self) -> np.ndarray:
        """The design matrix this rung's declared columns produce.

        The intercept lives here rather than in the stored frame, because the
        frame travels through the split authority and the storage schema, both of
        which carry their own column lists. Reading the matrix through this
        method keeps that detail out of every caller, and the column order is
        exactly ``columns``.
        """
        frame = _with_intercept(self.frame, self.columns)
        return np.column_stack(
            [
                np.ones(len(frame), dtype=np.float64)
                if name == "intercept"
                else _numeric(frame, name, context=f"design[{self.kind}]")
                for name in self.columns
            ]
        )

    def as_record(self) -> dict[str, Any]:
        """Provenance without the rows themselves, which the caller already holds."""
        return {
            "kind": self.kind,
            "model_kind": self.model_kind,
            "covariates": list(self.covariates),
            "derived_covariates": list(self.derived_covariates),
            "columns": list(self.columns),
            "level_terms": list(self.level_terms),
            "penalized_columns": list(self.penalized_columns),
            "source_columns": dict(self.source_columns),
            "target_column": self.target_column,
            "release_column": self.release_column,
            "n_rows": self.n_rows,
            "n_events": self.n_events,
            "n_releases": self.n_releases,
            "exclusions": dict(self.exclusions),
            "flags": list(self.flags),
            "notes": list(self.notes),
        }


def build_timing_features(
    rows: Any,
    *,
    spec: TimingSpec | None = None,
    kind: str | None = None,
    covariates: Sequence[str] | None = None,
) -> TimingDesign:
    """Build a ladder rung's feature matrix from trade-panel rows.

    Rows enter on the panel's own terms. A row the panel marks invalid, a row
    without an observed response, and a row missing any covariate this rung
    requires are excluded and counted under the reason the panel itself
    recorded; nothing is imputed, a null is never a zero, and validity is never
    re-derived here from a post-event field.

    The baseline price is the one exception that stops the analysis rather than
    excluding a row: a missing price is an exclusion the panel already names,
    while a price outside the 0-1 probability scale is a schema or unit error and
    raises.
    """
    if spec is None:
        spec = load_timing_spec()
    if kind is None:
        kind = TIMING_LADDER_KINDS[-1]
    if covariates is None:
        declared = spec.covariates_for(kind)
    else:
        declared = assert_permitted_covariates(covariates, context="build_timing_features")
        unadmitted = [name for name in declared if name not in spec.admissible_covariates]
        if unadmitted:
            raise TimingModelError(
                f"requested covariate(s) {unadmitted} are not admitted by the loaded "
                f"specification, which admits {list(spec.admissible_covariates)}"
            )
    _require_panel_columns(PANEL_COLUMNS_READ, context="build_timing_features")
    frame = _as_frame(rows, context="build_timing_features")
    required = ("event_id", "family", "event_time", "horizon_seconds", "baseline", RESPONSE_COLUMN)
    _require_columns(frame, required, context="build_timing_features")
    sources = tuple(SOURCE_COLUMNS[name] for name in declared)
    _require_columns(
        frame,
        tuple(name for name in sources if name not in required),
        context=f"build_timing_features[{kind}]",
    )

    ledger = ExclusionLedger()
    prepared = add_derived_covariates(frame, families=spec.coefficient_families)
    keep = pd.Series(True, index=prepared.index)

    def drop(reason: str, mask: pd.Series) -> None:
        """Record and exclude rows, counting each row under its first reason only.

        The mask is intersected with the rows still kept, so a row that left the
        design at an earlier step is not counted twice in the ledger. A row's
        exclusion reason is the first one that applies, which keeps the ledger
        readable as an ordered account of what left the sample.
        """
        nonlocal keep
        remaining = mask & keep
        count = int(remaining.sum())
        if count:
            ledger.add(reason, count)
            keep &= ~remaining

    identifiers = prepared["event_id"].astype(str)
    drop("missing_event_id", identifiers.isin({"", "nan", "None", "NaN"}))
    times = pd.to_datetime(prepared["event_time"], utc=True, errors="coerce")
    drop("unparsable_event_time", times.isna())
    if "valid" in prepared.columns:
        invalid = ~prepared["valid"].fillna(False).astype(bool)
    else:
        invalid = pd.Series(False, index=prepared.index)
        ledger.add("no_valid_column", 0)
    recorded = prepared.get("exclusion_reason")
    recorded_labels = (
        recorded.fillna("").astype(str)
        if recorded is not None
        else pd.Series("", index=prepared.index)
    )
    named = recorded_labels.where(recorded_labels.str.strip() != "", "invalid_row")
    if invalid.any():
        for reason in sorted(set(named.loc[invalid].tolist())):
            drop(str(reason), invalid & (named == reason))
    drop("missing_response", pd.isna(_numeric(prepared, RESPONSE_COLUMN, context="design")))
    family_values = prepared["family"].astype(str)
    drop("family_outside_declared_families", ~family_values.isin(set(spec.coefficient_families)))
    for name in declared:
        if name in DERIVED_COVARIATES:
            continue
        missing_values = pd.Series(
            ~np.isfinite(_numeric(prepared, name, context="design")), index=prepared.index
        )
        if not missing_values.any():
            continue
        fallback = f"missing_{name}"
        labels = named.where(named != "invalid_row", fallback)
        for reason in sorted(set(labels.loc[missing_values].tolist())):
            drop(str(reason), missing_values & (labels == reason))

    design_frame = prepared.loc[keep].copy()
    if design_frame.empty:
        raise TimingModelError(
            "no row carries the identifiers, a response and the declared covariate(s) "
            f"{list(declared)}; missingness cannot be imputed away, so there is nothing to fit"
        )
    design_frame["valid"] = True
    columns = feature_columns(declared, families=spec.coefficient_families)
    level = family_level_terms(declared, families=spec.coefficient_families)
    penalized = tuple(name for name in columns if name not in level)

    notes: list[str] = []
    flags: list[str] = []
    if ledger.total():
        notes.append(
            "excluded rows, by the reason the panel recorded: "
            f"{ledger.as_dict()}; nothing was imputed and no null became a zero"
        )
    for name in level:
        if name != "intercept" and not bool(design_frame[name].any()):
            flags.append(f"level_term_absent:{name}")
    prices = design_frame[COVARIATE_PRE_EVENT_PRICE].to_numpy(dtype=np.float64)
    if bool(np.all(prices <= 0.5)) or bool(np.all(prices >= 0.5)):
        flags.append("baseline_prices_on_one_side_of_one_half")
        notes.append(
            "every retained baseline price sits on one side of one half, where boundary_proximity "
            "and pre_event_price carry the same information up to sign and shift; the rung that "
            "adds boundary_proximity therefore adds little or no design dimension on these rows"
        )
    if bool(np.any((prices <= 0.0) | (prices >= 1.0))):
        flags.append("baseline_at_probability_boundary")
    return TimingDesign(
        kind=kind,
        model_kind=spec.model_kind,
        covariates=declared,
        derived_covariates=tuple(name for name in declared if name in DERIVED_COVARIATES),
        columns=columns,
        level_terms=level,
        penalized_columns=penalized,
        source_columns={name: SOURCE_COLUMNS[name] for name in declared},
        target_column=RESPONSE_COLUMN,
        release_column="cluster_id" if "cluster_id" in design_frame.columns else "event_id",
        frame=design_frame,
        exclusions=ledger.as_dict(),
        n_rows=len(design_frame),
        n_events=int(design_frame["event_id"].nunique()),
        n_releases=int(release_labels(design_frame).nunique()),
        flags=tuple(flags),
        notes=tuple(notes),
    )


@dataclass(frozen=True, slots=True)
class TimingFit:
    """A frozen timing-only rung: level terms, conditioned covariates, provenance."""

    kind: str
    model_kind: str
    covariates: tuple[str, ...]
    feature_names: tuple[str, ...]
    level_terms: tuple[str, ...]
    penalized_columns: tuple[str, ...]
    coefficients: Mapping[str, float]
    feature_means: Mapping[str, float]
    feature_scales: Mapping[str, float]
    alpha: float
    alpha_grid: tuple[float, ...]
    penalty_selected_on: str
    validation_scores: Mapping[str, float]
    family_coefficients: Mapping[str, Mapping[str, Any]]
    design_rank: int
    loss: str
    event_weighted_mae: float
    per_release_mae: Mapping[str, float]
    row_mae: float
    n_rows: int
    n_events: int
    n_releases: int
    release_column: str
    weight_unit: str
    target_column: str
    target_definition: str
    train_event_ids: tuple[str, ...]
    train_releases: tuple[str, ...]
    train_cutoff: datetime
    weighting: Mapping[str, Any]
    notes: tuple[str, ...]

    def predict(self, rows: Any) -> np.ndarray:
        """Fitted response for rows carrying every column the fit was made from."""
        frame = _as_frame(rows, context=f"predict[{self.kind}]")
        _require_columns(
            frame,
            tuple(name for name in self.feature_names if name != "intercept"),
            context=f"predict[{self.kind}]",
        )
        return self.matrix(frame) @ np.array(
            [self.coefficients[name] for name in self.feature_names], dtype=np.float64
        )

    def matrix(self, frame: pd.DataFrame) -> np.ndarray:
        """Design matrix with the fitted conditioning re-applied, never refit."""
        blocks = []
        for name in self.feature_names:
            if name == "intercept":
                blocks.append(np.ones(len(frame), dtype=np.float64))
                continue
            values = _numeric(frame, name, context=f"predict[{self.kind}]")
            if not np.isfinite(values).all():
                raise TimingModelError(
                    f"predict[{self.kind}] received {int((~np.isfinite(values)).sum())} row(s) "
                    f"with no usable {name!r}; a missing covariate is not imputed at prediction "
                    "time either"
                )
            blocks.append((values - self.feature_means[name]) / self.feature_scales[name])
        return np.column_stack(blocks)

    def as_record(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "model_kind": self.model_kind,
            "covariates": list(self.covariates),
            "feature_names": list(self.feature_names),
            "level_terms": list(self.level_terms),
            "penalized_columns": list(self.penalized_columns),
            "coefficients": dict(self.coefficients),
            "feature_means": dict(self.feature_means),
            "feature_scales": dict(self.feature_scales),
            "parameters": {
                "alpha": self.alpha,
                "alpha_grid": list(self.alpha_grid),
                "alpha_selection": self.penalty_selected_on,
                "alpha_grid_matches_forecast_ladder": tuple(self.alpha_grid) == DEFAULT_ALPHAS,
                "level_terms_unpenalized": True,
                "design_rank": self.design_rank,
                "bounded_link": None,
                "link": (
                    "identity on the probability-point response: the target is already a bounded "
                    "absolute change, and the bounded-price geometry is carried by declared "
                    "covariates rather than by a mapping"
                ),
            },
            "validation_scores": dict(self.validation_scores),
            "family_coefficients": {
                family: dict(payload) for family, payload in self.family_coefficients.items()
            },
            "loss": self.loss,
            "event_weighted_mae": self.event_weighted_mae,
            "per_release_mae": dict(self.per_release_mae),
            "row_mae": self.row_mae,
            "n_rows": self.n_rows,
            "n_events": self.n_events,
            "n_releases": self.n_releases,
            "release_column": self.release_column,
            "weight_unit": self.weight_unit,
            "target_column": self.target_column,
            "target_definition": self.target_definition,
            "train_event_ids": list(self.train_event_ids),
            "train_releases": list(self.train_releases),
            "train_cutoff": self.train_cutoff.isoformat(),
            "weighting": dict(self.weighting),
            "notes": list(self.notes),
        }


def event_weighted_mae(actual: Any, predicted: Any, *, releases: Any) -> dict[str, Any]:
    """Equal-weight release mean of the per-release mean absolute error.

    The independent unit is the release, so a release with many observed
    contracts counts once. The row-level mean is reported beside it, and the two
    differ exactly when releases carry unequal numbers of rows, which is what
    makes an unweighted row mean a statement about the observation grid rather
    than about releases.
    """
    scores = forecast_scores(actual, predicted, event=releases)
    return {
        "loss": SUPPORTED_LOSS,
        "value": float(scores["event_mean_mae"]),
        "n_releases": int(scores["n_events"]),
        "per_release": {str(key): float(value) for key, value in scores["event_mae"].items()},
        "row_mae": float(scores["mae"]),
        "n_rows": int(scores["n"]),
        "weighting": {
            "unit": AGGREGATION_UNIT,
            "scheme": "equal weight per economic release, contracts inside a release sharing it",
        },
    }


def _with_intercept(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Attach the intercept column when a design needs one.

    It is attached at use rather than stored, because ``chronological_splits``
    and ``storage`` both carry their own column lists and an added column would
    have to survive every one of them. The rung that needs it is known from its
    own covariate tuple, so the column is created exactly where it is read.
    """
    if "intercept" in columns and "intercept" not in frame.columns:
        return frame.assign(intercept=1.0)
    return frame


def _condition(
    frame: pd.DataFrame,
    columns: Sequence[str],
    weights: np.ndarray,
    *,
    context: str,
) -> tuple[dict[str, float], dict[str, float], list[str]]:
    """Center and scale the penalized block on the fitting rows only.

    The weights are the same release weights the normal equations use, so a
    release contributes its one unit of information to the conditioning as well
    as to the fit. Conditioning on unweighted row statistics would let a release
    with many observed contracts move the means and scales, which would make the
    fitted coefficients depend on the observation grid rather than on the
    releases, and would make duplicating a release's rows change the fit.

    Level terms keep mean zero and scale one, so a family coefficient stays the
    fitted response level for that family in probability points and is readable
    without undoing a transform. The conditioned covariates are reported with
    their means and scales, so their supplied-unit reading stays recoverable.
    """
    means: dict[str, float] = {}
    scales: dict[str, float] = {}
    notes: list[str] = []
    total = float(weights.sum())
    for name in columns:
        values = _numeric(frame, name, context=context)
        mean = float(np.sum(weights * values) / total)
        variance = float(np.sum(weights * (values - mean) ** 2) / total)
        scale = math.sqrt(max(variance, 0.0))
        if not math.isfinite(scale) or scale <= 1e-12:
            scale = 1.0
            notes.append(
                f"covariate {name!r} has no variation on the fitting rows; its scale is recorded "
                "as 1.0 and its coefficient is not interpretable as a slope"
            )
        means[name] = mean
        scales[name] = scale
    return means, scales, notes


def _matrix(
    frame: pd.DataFrame,
    columns: Sequence[str],
    means: Mapping[str, float],
    scales: Mapping[str, float],
    *,
    context: str,
) -> np.ndarray:
    return np.column_stack(
        [(_numeric(frame, name, context=context) - means[name]) / scales[name] for name in columns]
    )


def _weighted_ridge(
    matrix: np.ndarray, target: np.ndarray, weights: np.ndarray, *, penalty: np.ndarray
) -> np.ndarray:
    """Weighted ridge normal equations, with a least-squares fallback.

    The penalty enters the diagonal, so a design with an exactly collinear pair
    still has one determinate solution. That matters here: below one half,
    ``boundary_proximity`` is an exact linear restatement of ``pre_event_price``,
    and the penalty is what decides the split between the two coefficients
    instead of leaving it to the solver.
    """
    normal = matrix.T @ (matrix * weights[:, None]) + penalty
    rhs = matrix.T @ (weights * target)
    try:
        solution = np.linalg.solve(normal, rhs)
    except np.linalg.LinAlgError:  # pragma: no cover - the penalty keeps this regular
        solution, *_ = np.linalg.lstsq(normal, rhs, rcond=None)
    if not np.isfinite(solution).all():
        raise TimingModelError("the ridge solution is not finite; check the fitting rows for scale")
    return solution


def _fit_frame(
    train: pd.DataFrame,
    *,
    kind: str,
    spec: TimingSpec,
    alphas: Sequence[float],
    validation: pd.DataFrame | None,
) -> TimingFit:
    """Fit one rung, selecting its penalty on held-out rows when they are supplied."""
    declared = spec.covariates_for(kind)
    columns = feature_columns(declared, families=spec.coefficient_families)
    level = family_level_terms(declared, families=spec.coefficient_families)
    needed = (*columns, RESPONSE_COLUMN)
    missing = [name for name in needed if name != "intercept" and name not in train.columns]
    if missing:
        raise TimingModelError(
            f"the design for rung {kind!r} is missing column(s) {missing}; build it with "
            "build_timing_features against this specification"
        )
    train = _with_intercept(train.copy(), columns)
    notes: list[str] = []
    # A level term with no row at all is dropped rather than left in at zero: an
    # all-zero column would make the unpenalized level block rank deficient and
    # split the level between an absent family and the others.
    dropped_level = [
        name
        for name in level
        if name != "intercept" and not bool(_numeric(train, name, context="fit").any())
    ]
    if dropped_level:
        level = tuple(name for name in level if name not in dropped_level)
        columns = tuple(name for name in columns if name not in dropped_level)
        notes.append(
            f"dropped level term(s) {dropped_level} because no fitting row belongs to that family; "
            "the family indicator is reported as absent rather than fitted to zero"
        )
    penalized = tuple(name for name in columns if name not in level)
    weights = release_weights(train).to_numpy(dtype=np.float64)
    means, scales, scale_notes = _condition(train, penalized, weights, context=f"fit[{kind}]")
    for name in level:
        means[name] = 0.0
        scales[name] = 1.0
    notes.extend(scale_notes)
    matrix = _matrix(train, columns, means, scales, context=f"fit[{kind}]")
    target = _numeric(train, RESPONSE_COLUMN, context=f"fit[{kind}]")
    if not np.isfinite(target).all():
        raise TimingModelError(
            f"the fitting rows for rung {kind!r} carry {int((~np.isfinite(target)).sum())} "
            "non-finite response value(s); the design excludes those rows before fitting"
        )
    design_rank = int(np.linalg.matrix_rank(matrix))
    if design_rank < len(columns):
        notes.append(
            f"the design has rank {design_rank} over {len(columns)} column(s), so at least one "
            "covariate is an exact linear combination of the others on these rows; the penalty "
            "identifies the split, and the individual coefficients of the collinear group are not "
            "separately interpretable"
        )
    index_of = {name: position for position, name in enumerate(columns)}

    validation_matrix: np.ndarray | None = None
    validation_target: np.ndarray | None = None
    validation_releases: np.ndarray | None = None
    if validation is not None and len(validation):
        candidate = _with_intercept(validation, columns)
        validation_matrix = _matrix(candidate, columns, means, scales, context=f"fit[{kind}]")
        validation_target = _numeric(candidate, RESPONSE_COLUMN, context=f"fit[{kind}]")
        validation_releases = release_labels(candidate).to_numpy()
        penalty_selected_on = "validation"
    else:
        penalty_selected_on = "train"

    candidates: list[tuple[float, np.ndarray, float]] = []
    validation_scores: dict[str, float] = {}
    for alpha in alphas:
        penalty = np.zeros((len(columns), len(columns)), dtype=np.float64)
        for name in penalized:
            penalty[index_of[name], index_of[name]] = float(alpha)
        solution = _weighted_ridge(matrix, target, weights, penalty=penalty)
        if validation_matrix is not None:
            score = float(
                event_weighted_mae(
                    validation_target, validation_matrix @ solution, releases=validation_releases
                )["value"]
            )
        else:
            score = float(
                event_weighted_mae(target, matrix @ solution, releases=release_labels(train))[
                    "value"
                ]
            )
        validation_scores[f"alpha={float(alpha):g}"] = score
        candidates.append((float(alpha), solution, score))
    # The same tie rule applies to every rung: the best loss wins, and an exact
    # tie goes to the smaller penalty. A rung is never handed a rule of its own.
    best_alpha, solution, _best_score = min(candidates, key=lambda item: (item[2], item[0]))
    if penalty_selected_on == "train":
        notes.append(
            "no validation rows were supplied, so the penalty was chosen by fitting loss and must "
            "be re-selected on held-out releases before any comparative claim"
        )
    coefficients = {name: float(solution[index_of[name]]) for name in columns}
    fitted = matrix @ solution
    metrics = event_weighted_mae(target, fitted, releases=release_labels(train))
    if not math.isfinite(float(metrics["value"])):
        raise TimingModelError("the fitted loss is not finite; the fit is not reportable")

    family_coefficients: dict[str, dict[str, Any]] = {}
    if spec.family_coefficients_reported_separately:
        for family in spec.coefficient_families:
            name = f"{FAMILY_INDICATOR_PREFIX}_{family}"
            if name not in coefficients:
                continue
            family_rows = (_numeric(train, name, context="fit") > 0.0).astype(bool)
            family_coefficients[family] = {
                "indicator": name,
                "coefficient": float(coefficients[name]),
                "n_rows": int(family_rows.sum()),
                "n_releases": int(release_labels(train.loc[family_rows]).nunique()),
                "event_weighted_mae": (
                    float(
                        event_weighted_mae(
                            target[family_rows],
                            fitted[family_rows],
                            releases=release_labels(train.loc[family_rows]).to_numpy(),
                        )["value"]
                    )
                    if bool(family_rows.any())
                    else None
                ),
                "units": (
                    "probability points: the fitted response level for this family on the "
                    "conditioned covariates, because the family indicators carry the intercept"
                ),
            }
    else:
        notes.append(
            "the specification does not report family coefficients separately, so only the pooled "
            "level block is reported"
        )
    train_releases = release_labels(train)
    return TimingFit(
        kind=kind,
        model_kind=spec.model_kind,
        covariates=declared,
        feature_names=columns,
        level_terms=level,
        penalized_columns=penalized,
        coefficients=coefficients,
        feature_means=means,
        feature_scales=scales,
        alpha=best_alpha,
        alpha_grid=tuple(float(alpha) for alpha in alphas),
        penalty_selected_on=penalty_selected_on,
        validation_scores=validation_scores,
        family_coefficients=family_coefficients,
        design_rank=design_rank,
        loss=SUPPORTED_LOSS,
        event_weighted_mae=float(metrics["value"]),
        per_release_mae=metrics["per_release"],
        row_mae=float(metrics["row_mae"]),
        n_rows=len(train),
        n_events=int(train["event_id"].nunique()),
        n_releases=int(train_releases.nunique()),
        release_column="cluster_id" if "cluster_id" in train.columns else "event_id",
        weight_unit=AGGREGATION_UNIT,
        target_column=RESPONSE_COLUMN,
        target_definition=(
            "transaction-price change in absolute probability units from the row's own baseline to "
            "the endpoint at its declared horizon, exactly as the panel recorded it"
        ),
        train_event_ids=tuple(sorted(str(value) for value in train["event_id"].unique())),
        train_releases=tuple(sorted(str(value) for value in train_releases.unique())),
        train_cutoff=pd.Timestamp(
            pd.to_datetime(train["event_time"], utc=True, errors="coerce").max()
        ).to_pydatetime(),
        weighting={
            "unit": AGGREGATION_UNIT,
            "release_column": "cluster_id" if "cluster_id" in train.columns else "event_id",
            "row_weight": "one divided by the number of observed rows in the row's release",
            "release_weight_total": 1.0,
            "equal_weight_economic_events": True,
            "alternative_weights": "disabled; quantity is unverified for the cleaned layer",
        },
        notes=tuple(notes),
    )


def fit_timing_model(
    design: TimingDesign,
    *,
    spec: TimingSpec | None = None,
    kind: str | None = None,
    alphas: Sequence[float] | None = None,
    validation: TimingDesign | None = None,
) -> TimingFit:
    """Fit one timing-only rung, selecting its penalty on held-out rows.

    The penalty comes from ``DEFAULT_TIMING_ALPHAS`` and is chosen by the
    release-weighted mean absolute error on ``validation``, never on the rows the
    coefficients were fit from, whenever validation rows are supplied. When they
    are not, the fit records ``penalty_selected_on='train'`` and says so in its
    notes, because a penalty chosen in sample cannot support a comparative
    claim.
    """
    spec = spec or load_timing_spec()
    kind = kind or design.kind
    grid = tuple(float(alpha) for alpha in (spec.alphas if alphas is None else alphas))
    if not grid or any(not math.isfinite(alpha) or alpha < 0.0 for alpha in grid):
        raise TimingModelError(
            f"alphas={list(grid)!r} must be a non-empty finite non-negative grid"
        )
    if validation is not None:
        needed = (
            *feature_columns(spec.covariates_for(kind), families=spec.coefficient_families),
            RESPONSE_COLUMN,
        )
        missing = sorted(
            name for name in needed if name != "intercept" and name not in validation.frame.columns
        )
        if missing:
            raise TimingModelError(
                f"validation rows are missing column(s) {missing}; every rung is selected on rows "
                "carrying the same covariate block it is fit on"
            )
    return _fit_frame(
        design.frame,
        kind=kind,
        spec=spec,
        alphas=grid,
        validation=None if validation is None else validation.frame,
    )


def score_timing_fit(fit: TimingFit, rows: Any) -> dict[str, Any]:
    """Score a frozen rung on held-out rows, at the release and at the event.

    The reported loss is the release-weighted mean absolute error. Per-event
    errors are reported beside it because a paired comparison reads more easily
    event by event, but the loss and any interval over it are computed at the
    release, which is the independent unit.
    """
    frame = _as_frame(rows, context=f"score_timing_fit[{fit.kind}]")
    _require_columns(
        frame,
        (
            *(name for name in fit.feature_names if name != "intercept"),
            RESPONSE_COLUMN,
            "event_id",
        ),
        context=f"score[{fit.kind}]",
    )
    actual = _numeric(frame, RESPONSE_COLUMN, context=f"score[{fit.kind}]")
    if not np.isfinite(actual).all():
        raise TimingModelError(
            f"score_timing_fit[{fit.kind}] received {int((~np.isfinite(actual)).sum())} row(s) "
            "without an observed response; those rows belong in the design's exclusion ledger"
        )
    predicted = fit.predict(frame)
    releases = release_labels(frame, release_column=fit.release_column).to_numpy()
    events = frame["event_id"].astype(str).to_numpy()
    loss = event_weighted_mae(actual, predicted, releases=releases)
    by_event = forecast_scores(actual, predicted, event=events)
    contracts = frame["contract_id"].astype(str) if "contract_id" in frame.columns else None
    horizons = pd.to_numeric(frame["horizon_seconds"], errors="coerce").fillna(0).astype(int)
    return {
        "kind": fit.kind,
        "n_rows": len(frame),
        "n_events": int(by_event["n_events"]),
        "n_releases": int(loss["n_releases"]),
        "loss": SUPPORTED_LOSS,
        "event_weighted_mae": float(loss["value"]),
        "row_mae": float(loss["row_mae"]),
        "per_release_mae": dict(loss["per_release"]),
        "per_event_mae": {str(key): float(value) for key, value in by_event["event_mae"].items()},
        "directional_accuracy": by_event["directional_accuracy"],
        "alpha": fit.alpha,
        "penalty_selected_on": fit.penalty_selected_on,
        "family_coefficients": {
            family: dict(payload) for family, payload in fit.family_coefficients.items()
        },
        "event_ids": sorted(str(value) for value in frame["event_id"].unique()),
        "predictions": [
            {
                "event_id": str(event_id),
                "contract_id": str(contract_id),
                "horizon_seconds": int(horizon),
                "actual": float(actual_value),
                "predicted": float(predicted_value),
            }
            for event_id, contract_id, horizon, actual_value, predicted_value in zip(
                frame["event_id"].astype(str),
                contracts if contracts is not None else frame["event_id"].astype(str),
                horizons,
                actual,
                predicted,
                strict=True,
            )
        ],
        "weighting": dict(loss["weighting"]),
    }


@dataclass(frozen=True, slots=True)
class TimingComparison:
    """Held-out paired comparison of the timing ladder on one identical sample."""

    kinds: tuple[str, ...]
    baseline_kind: str
    folds: FoldSplit
    sample: Mapping[str, Any]
    fits: Mapping[str, Mapping[str, Any]]
    evaluations: Mapping[str, Mapping[str, Any]]
    gains: Mapping[str, Mapping[str, Any]]
    metric: str
    loss: str
    bootstrap: Mapping[str, Any]
    meaningful_sizes: Mapping[str, Any]
    seed: int
    spec: Mapping[str, Any]
    notes: tuple[str, ...]
    flags: tuple[str, ...]

    def as_record(self) -> dict[str, Any]:
        return {
            "kinds": list(self.kinds),
            "baseline_kind": self.baseline_kind,
            "folds": self.folds.as_record(),
            "sample": dict(self.sample),
            "fits": {kind: dict(payload) for kind, payload in self.fits.items()},
            "evaluations": {kind: dict(payload) for kind, payload in self.evaluations.items()},
            "gains": {kind: dict(payload) for kind, payload in self.gains.items()},
            "metric": self.metric,
            "loss": self.loss,
            "bootstrap": dict(self.bootstrap),
            "meaningful_sizes": dict(self.meaningful_sizes),
            "seed": self.seed,
            "spec": dict(self.spec),
            "notes": list(self.notes),
            "flags": list(self.flags),
        }


def compare_timing_kinds(
    rows: Any,
    *,
    spec: TimingSpec | None = None,
    kinds: Sequence[str] | None = None,
    baseline_kind: str | None = None,
    design: TimingDesign | None = None,
    train_fraction: float = 0.6,
    validation_fraction: float = 0.2,
    embargo_seconds: float | None = None,
    min_events_per_fold: int = 1,
    samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    coverage: float = DEFAULT_COVERAGE,
    seed: int = DEFAULT_TIMING_SEED,
) -> TimingComparison:
    """Fit the nested rungs on identical rows and report paired release gains.

    Every rung is fit on the same training rows and scored on the same held-out
    rows: the design is built once from the union of the requested rungs'
    covariates, so no rung can win by being handed a different sample, and
    because the rungs are covariate prefixes the comparison is between nested
    specifications rather than between differently sized models. Rows are split
    by :func:`market_propagation.evaluation.chronological_splits`, the one split
    authority in this project, which assigns whole release clusters and purges
    rows whose label window reaches the next fold.

    A gain is the baseline's mean absolute error minus the rung's, so a positive
    gain means the richer rung is closer. Gains are computed release by release
    and then resampled by whole release: the bootstrap draws releases with
    replacement and keeps every row of a drawn release, so the interval is the
    precision of the study's own independent unit. Resampling rows inside a
    release is pseudo replication and is neither used nor offered as an option,
    because rows sharing a release share its price path and error component, so a
    row-level interval would shrink toward zero as contracts are added without
    any new release evidence.

    The interval is classified against the configuration's declared meaningful
    gain size, which is a scientific choice awaiting a power assessment and is
    reported as exactly that, never as a measured capability.
    """
    spec = spec or load_timing_spec()
    if spec.reproducibility_seeds and int(seed) not in spec.reproducibility_seeds:
        raise TimingModelError(
            f"seed {seed!r} is not among the seeds the configuration declares for runs "
            f"{list(spec.reproducibility_seeds)}"
        )
    requested = tuple(kinds) if kinds is not None else TIMING_LADDER_KINDS
    for kind in requested:
        spec.covariates_for(kind)
    if len(set(requested)) != len(requested):
        raise TimingModelError(f"kinds={list(requested)} repeats a rung")
    baseline_kind = baseline_kind or requested[0]
    if baseline_kind not in requested:
        raise TimingModelError(
            f"baseline_kind={baseline_kind!r} is not among the compared kinds {list(requested)}"
        )
    union: list[str] = []
    for kind in requested:
        for covariate in spec.covariates_for(kind):
            if covariate not in union:
                union.append(covariate)
    ordered_union = tuple(name for name in spec.admissible_covariates if name in set(union))
    if design is None:
        design = build_timing_features(rows, spec=spec, covariates=ordered_union)
    frame = design.frame
    try:
        folds = chronological_splits(
            frame,
            train_fraction=train_fraction,
            validation_fraction=validation_fraction,
            embargo_seconds=embargo_seconds,
            event_column="event_id",
            cluster_column=design.release_column,
            time_column="event_time",
            horizon_column="horizon_seconds",
            label_column="target_available_time",
            mask_column="valid",
            include_masked=False,
            min_events_per_fold=min_events_per_fold,
        )
    except ForecastEvaluationError as error:
        raise TimingModelError(f"the chronological split rejected the design: {error}") from error
    policy = dict(folds["test"].attrs.get("policy", {}))
    fits: dict[str, TimingFit] = {}
    records: dict[str, dict[str, Any]] = {}
    evaluations: dict[str, dict[str, Any]] = {}
    notes = list(design.notes)
    notes.append(
        "rows sharing a release share its price path and its error component, so uncertainty is "
        "resampled by whole release; row-level resampling inside a release is pseudo replication "
        "and is not computed here"
    )
    for kind in requested:
        subsets = {
            name: folds[name].reset_index(drop=True) for name in ("train", "validation", "test")
        }
        fit = _fit_frame(
            subsets["train"],
            kind=kind,
            spec=spec,
            alphas=spec.alphas,
            validation=subsets["validation"],
        )
        fits[kind] = fit
        records[kind] = fit.as_record()
        evaluations[kind] = score_timing_fit(fit, subsets["test"])
        notes.extend(f"{kind}: {note}" for note in fit.notes)
    held_out = set(evaluations[baseline_kind]["event_ids"])
    for kind in requested:
        if set(evaluations[kind]["event_ids"]) != held_out:  # pragma: no cover - one fold frame
            raise TimingModelError(
                f"rung {kind!r} was scored on a different event set than the baseline, so the "
                "comparison is not paired"
            )
    gains: dict[str, dict[str, Any]] = {}
    baseline_release = evaluations[baseline_kind]["per_release_mae"]
    baseline_event = evaluations[baseline_kind]["per_event_mae"]
    held_out_releases = set(baseline_release)
    for kind in requested:
        rung_release = evaluations[kind]["per_release_mae"]
        per_release = {
            release: float(baseline_release[release]) - float(rung_release[release])
            for release in sorted(baseline_release)
        }
        rung_event = evaluations[kind]["per_event_mae"]
        per_event = {
            event: float(baseline_event[event]) - float(rung_event[event])
            for event in sorted(baseline_event)
        }
        releases = sorted(per_release)
        values = pd.Series(
            np.array([per_release[release] for release in releases], dtype=np.float64),
            index=pd.Index(releases, dtype=object),
        )
        bootstrap = cluster_bootstrap(
            values,
            pd.Series(np.array(releases, dtype=object), index=values.index),
            seed=int(seed),
            samples=int(samples),
            coverage=float(coverage),
            name="paired_gain",
        )
        interval = bootstrap["samples"]["paired_gain"]
        if interval["lower"] is None or interval["upper"] is None:
            classification: dict[str, Any] = {
                "classification": "inconclusive",
                "interval": {"lower": None, "upper": None},
                "relevant_effect": spec.meaningful_gain_size,
                "reason": interval.get("reason"),
                "classification_basis": "no release-level interval is identified",
            }
        else:
            classification = classify_outcome(
                float(interval["lower"]),
                float(interval["upper"]),
                relevant_effect=spec.meaningful_gain_size,
            )
        gains[kind] = {
            "kind": kind,
            "baseline_kind": baseline_kind,
            "metric": SUPPORTED_LOSS,
            "direction": "positive_gain_means_lower_error_for_this_rung",
            "point": float(interval["point"]),
            "per_event_gain": per_event,
            "per_release_gain": per_release,
            "n_events": len(per_event),
            "n_releases": len(per_release),
            "interval": {
                "lower": interval["lower"],
                "upper": interval["upper"],
                "coverage": float(coverage),
                "status": interval["status"],
            },
            "bootstrap": {
                "method": bootstrap["method"],
                "unit": AGGREGATION_UNIT,
                "resampling": "whole releases; every row of a drawn release is kept",
                "samples_requested": bootstrap["samples_requested"],
                "samples_effective": bootstrap["samples_effective"],
                "n_clusters": bootstrap["n_clusters"],
                "seed": bootstrap["seed"],
                "row_level_resampling": "refused: pseudo replication inside a release",
            },
            "classification": classification,
            "meaningful_gain_size": spec.meaningful_gain_size,
            "meaningful_gain_size_status": spec.meaningful_size_status,
        }
    sample = {
        "n_rows": int(sum(len(folds[name]) for name in ("train", "validation", "test"))),
        "n_train_rows": len(folds["train"]),
        "n_validation_rows": len(folds["validation"]),
        "n_test_rows": len(folds["test"]),
        "n_events": int(frame["event_id"].nunique()),
        "n_releases": int(release_labels(frame).nunique()),
        "design_rows": len(frame),
        "rows_dropped_by_split": int(
            len(frame) - sum(len(folds[name]) for name in ("train", "validation", "test"))
        ),
        "held_out_events": sorted(held_out),
        "held_out_releases": sorted(held_out_releases),
        "release_column": design.release_column,
        "covariates": list(ordered_union),
        "identical_rows_for_every_rung": True,
        "nesting": "each rung's covariate tuple is a prefix of the next rung's",
    }
    return TimingComparison(
        kinds=requested,
        baseline_kind=baseline_kind,
        folds=FoldSplit(
            train_events=tuple(sorted(str(value) for value in folds["train"]["event_id"].unique())),
            validation_events=tuple(
                sorted(str(value) for value in folds["validation"]["event_id"].unique())
            ),
            test_events=tuple(sorted(str(value) for value in folds["test"]["event_id"].unique())),
            train_cutoff=pd.Timestamp(policy["train_cutoff"]).to_pydatetime(),
            validation_cutoff=pd.Timestamp(policy["validation_cutoff"]).to_pydatetime(),
            embargo_seconds=float(policy.get("embargo_seconds", 0.0)),
            purged_rows=int(policy.get("purged_rows", 0)),
            policy=policy,
        ),
        sample=sample,
        fits=records,
        evaluations=evaluations,
        gains=gains,
        metric=SUPPORTED_LOSS,
        loss=SUPPORTED_LOSS,
        bootstrap={
            "method": "cluster bootstrap over releases, percentile intervals",
            "unit": AGGREGATION_UNIT,
            "samples": int(samples),
            "coverage": float(coverage),
            "seed": int(seed),
            "row_level_resampling": "refused: pseudo replication inside a release",
        },
        meaningful_sizes=spec.meaningful_sizes(),
        seed=int(seed),
        spec=spec.as_record(),
        notes=tuple(notes),
        flags=tuple(design.flags),
    )
