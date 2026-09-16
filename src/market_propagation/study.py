"""Fit the study's declared model ladder on a sealed transaction panel.

The reporting path in :mod:`market_propagation.external_report` describes a
panel: it counts coverage, draws response curves and states, in prose, which
model kind the configuration declares. It never fits one. A report that names a
configured model kind while no fit has run leaves the study's central claim
unevidenced, so this module exists to close that gap: it reads the same sealed
panel, builds the declared design, fits it, scores it on held-out releases and
records the fitted rows, the design columns, the predictions and the input
identities alongside the result.

Three properties are load-bearing.

*Absorption and propagation are separate claims with separate evidence.* The
timing-only ladder is estimable from a transaction panel alone, because its
covariates are the contract's own state and the calendar. The conditional
predictive propagation rung needs a neighbour column that a same-contract panel
cannot carry, so this module reports that rung as blocked with the missing
columns named rather than fitting a substituted model and calling it a
propagation result. A blocked rung is a result.

*Nothing is fitted on rows the design refused.* The design's own exclusion
ledger is recorded beside the fit, so the difference between the rows the panel
carries and the rows the fit saw is visible in the artifact instead of being
recoverable only by re-running the pipeline.

*The artifact names its inputs.* A result carries the content hash of the sealed
panel it read, the resolved specification, the source-time clock basis and the
row identities the fit used, because the repository holds untracked code and a
Git hash alone would not identify these bytes.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from . import storage
from .models import FEATURE_SPECS, MODEL_KINDS
from .registry import ExperimentRegistry
from .timing_model import (
    TIMING_ADMISSIBLE_COVARIATES,
    TimingComparison,
    TimingModelError,
    build_timing_features,
    compare_timing_kinds,
    load_timing_spec,
)

#: The artifact this module writes.
STUDY_RESULT_NAME = "study_result.json"

#: One registry file serves every run directory, so two runs of the same study
#: cannot silently write to two different stores and each look complete.
REGISTRY_DB_NAME = "empirical_study.sqlite3"

#: Where the shared store lives, relative to the checkout root. A per-run directory
#: is writable output, so a registry inside it would be one more artifact of the run
#: rather than a durable cross-run ledger.
SHARED_REGISTRY_DIR = "data/registry"

#: The claim this module can evidence from a transaction panel alone.
CLAIM_ABSORPTION = "absorption_response_in_source_time_transaction_data"

#: The claim the propagation rung would support, and which this module refuses to
#: raise without a neighbour column and a verified news vector.
CLAIM_PROPAGATION = "conditional_predictive_propagation"

#: Status vocabulary. ``blocked`` means a required input is absent and is named;
#: ``inconclusive`` means the fit ran and the interval did not settle it.
STATUS_COMPLETE = "complete"
STATUS_BLOCKED = "blocked"
STATUS_INCONCLUSIVE = "inconclusive"

#: A propagation rung needs a lagged neighbour return and the news terms the
#: neighbour term is compared against. Without the news vector, a shared shock
#: that moved both contracts is indistinguishable from transmission between
#: them, which is exactly the identification failure this study exists to avoid.
REQUIRED_NEIGHBOR_COLUMNS: tuple[str, ...] = ("neighbor_lag",)
REQUIRED_NEWS_COLUMNS: tuple[str, ...] = ("surprise",)

REASON_NO_NEIGHBOR_COLUMN = "no_admissible_neighbor_column_in_panel"
REASON_NO_VERIFIED_NEWS = "no_verified_surprise_vector"
REASON_TOO_FEW_RELEASES = "too_few_independent_releases_for_a_paired_comparison"

#: Evidence class stamped on every result, so a consumer cannot read an
#: absorption measurement as a propagation claim.
EVIDENCE_CLASS = "source_time_transaction_measurement"


@dataclass(frozen=True, slots=True)
class FamilyFit:
    """One family's absorption ladder, fitted and scored on held-out releases."""

    family: str
    status: str
    comparison: TimingComparison | None
    reason: str | None
    n_design_rows: int
    n_events: int
    n_releases: int
    exclusions: Mapping[str, int]
    fitted_row_keys: tuple[tuple[str, str, int], ...]

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "family": self.family,
            "status": self.status,
            "reason": self.reason,
            "n_design_rows": self.n_design_rows,
            "n_events": self.n_events,
            "n_releases": self.n_releases,
            "exclusions": dict(self.exclusions),
            "n_fitted_row_keys": len(self.fitted_row_keys),
            "fitted_row_keys_digest": _digest_json([list(key) for key in self.fitted_row_keys]),
        }
        if self.comparison is not None:
            payload["comparison"] = self.comparison.as_record()
        return payload


def _digest_json(value: Any) -> str:
    """Stable sha256 over canonical JSON, so equal content yields equal identity."""
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_panel(path: str | Path) -> pd.DataFrame:
    """Read a sealed panel, verifying its declared schema and content hash.

    The read is :func:`market_propagation.storage.read_parquet`, which refuses a
    dataset whose bytes no longer match the hash it was sealed under. A panel
    that does not verify is not read as though it had.
    """
    frame = storage.read_parquet(path, table="trade_panel")
    frame.attrs["panel_path"] = str(path)
    frame.attrs["panel_hash"] = storage.hash_file(Path(path))
    return frame


def propagation_support(frame: pd.DataFrame) -> dict[str, Any]:
    """Whether this panel can carry the propagation rung, and what is missing.

    The test is on columns, not on intent. A same-contract response panel carries
    one contract per row and no lagged return from a matched neighbour, so the
    network design has nothing to read. This function says that once, in one
    place, rather than letting each caller decide whether to fit anyway.
    """
    missing_neighbor = [name for name in REQUIRED_NEIGHBOR_COLUMNS if name not in frame.columns]
    missing_news = [name for name in REQUIRED_NEWS_COLUMNS if name not in frame.columns]
    reasons: list[str] = []
    if missing_neighbor:
        reasons.append(REASON_NO_NEIGHBOR_COLUMN)
    if missing_news:
        reasons.append(REASON_NO_VERIFIED_NEWS)
    return {
        "claim": CLAIM_PROPAGATION,
        "supported": not reasons,
        "missing_neighbor_columns": missing_neighbor,
        "missing_news_columns": missing_news,
        "reasons": reasons,
        "detail": (
            "a neighbour return and a verified surprise vector are both required: without the "
            "surprise vector a common shock that moved both contracts is indistinguishable from "
            "transmission between them, and without a neighbour column there is no lagged "
            "information to attribute to transmission at all"
        ),
    }


def fit_family(
    frame: pd.DataFrame,
    *,
    family: str,
    spec: Any | None = None,
    kinds: Sequence[str] | None = None,
    **kwargs: Any,
) -> FamilyFit:
    """Fit the timing ladder for one family, or report why it could not be fitted.

    The ladder is fit on one family's rows because the configuration declares the
    coefficient families separately: pooling CPI and employment would let a
    release family with more observed contracts set the scale of a covariate the
    other family never varies.
    """
    spec = spec or load_timing_spec()
    subset = frame[frame["family"].astype(str) == family].reset_index(drop=True)
    empty = FamilyFit(
        family=family,
        status=STATUS_BLOCKED,
        comparison=None,
        reason=REASON_TOO_FEW_RELEASES,
        n_design_rows=0,
        n_events=0,
        n_releases=0,
        exclusions={},
        fitted_row_keys=(),
    )
    if subset.empty:
        return empty
    releases = subset["cluster_id"].astype(str).nunique() if "cluster_id" in subset else 0
    if releases < 2:
        return FamilyFit(
            family=family,
            status=STATUS_BLOCKED,
            comparison=None,
            reason=REASON_TOO_FEW_RELEASES,
            n_design_rows=0,
            n_events=int(subset["event_id"].nunique()),
            n_releases=releases,
            exclusions={},
            fitted_row_keys=(),
        )
    ordered_union = tuple(
        name for name in TIMING_ADMISSIBLE_COVARIATES if name in set(TIMING_ADMISSIBLE_COVARIATES)
    )
    try:
        design = build_timing_features(subset, spec=spec, covariates=ordered_union)
        comparison = compare_timing_kinds(
            subset,
            spec=spec,
            kinds=kinds,
            design=design,
            **kwargs,
        )
    except TimingModelError as error:
        return FamilyFit(
            family=family,
            status=STATUS_BLOCKED,
            comparison=None,
            reason=f"{type(error).__name__}: {error}",
            n_design_rows=0,
            n_events=int(subset["event_id"].nunique()),
            n_releases=releases,
            exclusions={},
            fitted_row_keys=(),
        )
    keys = tuple(
        (str(row["event_id"]), str(row["contract_id"]), int(row["horizon_seconds"]))
        for _, row in design.frame.iterrows()
    )
    return FamilyFit(
        family=family,
        status=STATUS_COMPLETE,
        comparison=comparison,
        reason=None,
        n_design_rows=design.n_rows,
        n_events=design.n_events,
        n_releases=design.n_releases,
        exclusions=design.exclusions,
        fitted_row_keys=keys,
    )


#: Exclusion reasons that describe the contract's rule evidence rather than the
#: observation. A row masked for one of these still carries both leg prices,
#: because the masking applies to the estimand rather than to what was seen.
RULE_ONLY_EXCLUSIONS: tuple[str, ...] = (
    "rule_evidence_missing",
    "rule_version_unknown",
)

#: Label carried by every exploratory measurement, so a reader cannot mistake an
#: unverified-predicate response for the primary estimand.
EXPLORATORY_LABEL = "exploratory_unverified_rule_semantics"

EXPLORATORY_CAVEAT = (
    "the contract's payoff predicate is not attested for the release date, so these responses "
    "measure the transaction price change of contracts that are very likely the intended "
    "predicate but whose rule vintage is unverified; the primary panel masks these rows and "
    "this measurement is not a substitute for it"
)


def _row_reasons(value: Any) -> tuple[str, ...]:
    """Every exclusion reason a panel row records, not only its first one.

    ``exclusion_reason`` holds the first reason in precedence order, and rule
    reasons precede observation reasons, so a row whose observation also failed
    reports the rule reason as its headline. Reading the full list is what keeps
    an exploratory measurement from treating an unobserved endpoint as if it were
    merely unverified.
    """
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except ValueError:
            return ()
    elif isinstance(value, Mapping):
        payload = value
    else:
        return ()
    reasons = payload.get("exclusion_reasons") if isinstance(payload, Mapping) else None
    if not isinstance(reasons, Sequence) or isinstance(reasons, (str, bytes)):
        return ()
    return tuple(str(reason) for reason in reasons)


def exploratory_absorption_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Rows measurable once the rule-evidence mask is set aside, and how many.

    This is the plan's exploratory candidate panel. A row qualifies only when both
    legs were observed and every recorded reason for excluding it is a rule reason,
    so a row missing its endpoint stays missing here rather than being repaired.
    The response is the difference of the two retained leg prices, which is the
    same estimand the primary panel computes before the mask nulls it.
    """
    out = frame.copy()
    reasons = out.get("flags_json")
    decoded = (
        reasons.map(_row_reasons)
        if reasons is not None
        else pd.Series([() for _ in range(len(out))], index=out.index)
    )
    both_legs = out["baseline"].notna() & out["endpoint"].notna()
    rule_only = decoded.map(
        lambda names: bool(names) and set(names).issubset(set(RULE_ONLY_EXCLUSIONS))
    )
    eligible = both_legs & rule_only
    out["valid"] = out["valid"].fillna(False).astype(bool) | eligible
    computed = pd.to_numeric(out["endpoint"], errors="coerce") - pd.to_numeric(
        out["baseline"], errors="coerce"
    )
    out["response"] = pd.to_numeric(out["response"], errors="coerce")
    out.loc[eligible, "response"] = computed[eligible].astype(float)
    out.loc[eligible, "exclusion_reason"] = None
    counts = {
        "rows": len(out),
        "rows_with_both_legs": int(both_legs.sum()),
        "rows_rule_only": int(rule_only.sum()),
        "rows_measurable": int(eligible.sum()),
        "rows_still_unobserved": int((both_legs & ~rule_only).sum()),
        "rows_without_both_legs": int((~both_legs).sum()),
    }
    return out, counts


#: The sealed forecast table's columns mapped onto the ladder's declared names.
#: One mapping, in one place: a renamed column cannot silently drop a rung's
#: regressor and leave a comparison that reads as if the term were fitted.
FORECAST_COLUMN_MAP: Mapping[str, str] = {
    "event_id": "event_id",
    "cluster_id": "cluster_id",
    "contract_id": "receiver_contract_id",
    "event_time": "release_time",
    "prediction_time": "forecast_origin",
    "max_input_available_time": "max_input_source_time",
    "target": "target",
    "current_price": "recipient_anchor",
    "own_lag": "own_lag",
    "neighbor_lag": "neighbor_lag",
}

#: Ladder columns that no stored forecast column supplies, with the input each one
#: needs. They are named here rather than defaulted, because a zero-filled shock or
#: a copied neighbour term would fit a model that is not the declared one.
UNSUPPLIED_LADDER_COLUMNS: Mapping[str, str] = {
    "shock": (
        "a validated release surprise from market_propagation.ingest.expectations, "
        "read on the release's own statistic and unit"
    ),
    "delayed_shock": "the preceding release's validated surprise for the same family",
    "neighbor_lag_control": "a matched placebo donor's lagged return, built by the exposure graph",
}

#: The release statistic each family's surprise is read from. It is declared once
#: here so a family cannot be fitted against a different number than it reports.
FAMILY_SURPRISE_STATISTIC: Mapping[str, str] = {
    "cpi": "cpi_headline_sa_mom_pct",
    "employment": "payrolls_change_thousands",
}

REASON_LADDER_COLUMNS_ABSENT = "declared_ladder_columns_absent_from_the_forecast_table"
REASON_LADDER_COMMON_SAMPLE_INCOMPLETE = (
    "the_common_comparison_sample_is_incomplete_because_a_rung_column_is_absent"
)
REASON_LADDER_SAMPLE_EMPTY = "no_forecast_row_supplies_every_declared_ladder_column"
REASON_LADDER_FIT_FAILED = "the_ladder_fit_raised"


@dataclass(frozen=True, slots=True)
class RungFit:
    """One ladder rung, fitted on the real forecast rows or blocked with its reason."""

    kind: str
    status: str
    reason: str | None
    missing_columns: tuple[str, ...]
    n_rows: int
    n_clusters: int
    comparison: Mapping[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "status": self.status,
            "reason": self.reason,
            "missing_columns": list(self.missing_columns),
            "n_rows": self.n_rows,
            "n_clusters": self.n_clusters,
            "params_digest": None,
            "predictions_digest": None,
        }
        if self.comparison is not None:
            payload["comparison"] = dict(self.comparison)
        return payload


def ladder_support(
    frame: pd.DataFrame,
    *,
    surprise: Mapping[str, Mapping[str, Any]] | None = None,
    control_column: str | None = None,
) -> dict[str, Any]:
    """Which declared ladder columns the forecast table can supply, per kind.

    The test is on columns and on the real presence of values, not on intent: a
    declared regressor with no column, or a column whose every value is null, is
    named as a missing input rather than fitted as a constant.
    """
    supplied = {
        name
        for name, column in FORECAST_COLUMN_MAP.items()
        if column in frame.columns and frame[column].notna().any()
    }
    if surprise:
        supplied.add("shock")
        supplied.add("delayed_shock")
    if (
        control_column is not None
        and control_column in frame.columns
        and frame[control_column].notna().any()
    ):
        supplied.add("neighbor_lag_control")
    kinds: dict[str, Any] = {}
    missing_by_kind: dict[str, list[str]] = {}
    for kind in MODEL_KINDS:
        needed = FEATURE_SPECS[kind]
        missing = [name for name in needed if name not in supplied]
        missing_by_kind[kind] = missing
        kinds[kind] = {
            "declared_columns": list(needed),
            "missing_columns": missing,
            "supported": not missing,
        }
    # Every rung is scored on one identical sample, so a column a single rung needs
    # removes that row from the sample the other rungs are fitted on too. A rung is
    # therefore only fittable when the whole requested ladder's columns are all
    # supplied, and the common gap is reported apart from the rung's own.
    common = sorted({name for missing in missing_by_kind.values() for name in missing})
    return {
        "supplied_columns": sorted(supplied),
        "unsupplied_columns": {
            name: detail
            for name, detail in sorted(UNSUPPLIED_LADDER_COLUMNS.items())
            if name not in supplied
        },
        "kinds": kinds,
        "comparison_common_missing_columns": common,
        "comparison_supported": not common,
    }


def _ladder_frame(
    frame: pd.DataFrame,
    *,
    surprise: Mapping[str, Mapping[str, Any]] | None,
    control_column: str | None = None,
) -> pd.DataFrame:
    """The sealed forecast rows as the ladder's declared design, or what is missing."""
    design = pd.DataFrame({name: frame[column] for name, column in FORECAST_COLUMN_MAP.items()})
    design["horizon_seconds"] = 0
    if "family" in frame.columns:
        design["family"] = frame["family"]
    if control_column is not None and control_column in frame.columns:
        design["neighbor_lag_control"] = pd.to_numeric(frame[control_column], errors="coerce")
    if surprise is not None:
        primary = frame.apply(
            lambda row: surprise.get(str(row["event_id"]), {}).get(
                FAMILY_SURPRISE_STATISTIC.get(str(row.get("family")), "")
            ),
            axis=1,
        )
        design["shock"] = pd.to_numeric(primary, errors="coerce")
        # The delayed term is the preceding release's surprise for the same family,
        # so it is a lagged value of the same declared statistic rather than a
        # second measurement of the same release.
        ordered = frame.sort_values(["family", "release_time"]) if "family" in frame else frame
        lagged = pd.to_numeric(
            ordered.assign(shock=design.loc[ordered.index, "shock"])["shock"], errors="coerce"
        )
        design["delayed_shock"] = (
            lagged.groupby(frame.loc[ordered.index, "family"]).shift(1)
            if "family" in frame
            else lagged.shift(1)
        ).reindex(design.index)
    return design


def fit_forecast_ladder(
    frame: pd.DataFrame,
    *,
    surprise: Mapping[str, Mapping[str, Any]] | None = None,
    control_column: str | None = None,
    kinds: Sequence[str] | None = None,
    seed: int = 20260913,
    minimum_mae_gain: float = 0.005,
) -> dict[str, Any]:
    """Fit the declared nested ladder on real source-time forecast rows.

    Each rung is fitted only when the forecast table supplies every one of its
    declared feature columns with a value on at least one row, and each rung is
    reported blocked with the missing columns named otherwise. A rung is never
    fitted on a substitute column, because the comparison's whole point is which
    declared information a rung adds.
    """
    from .models import ForecastDataError, nested_comparison

    support = ladder_support(frame, surprise=surprise, control_column=control_column)
    requested = tuple(kinds) if kinds is not None else MODEL_KINDS
    results: dict[str, RungFit] = {}
    for kind in requested:
        block = support["kinds"].get(kind)
        if block is None:
            raise ValueError(f"unknown ladder kind {kind!r}; expected {list(MODEL_KINDS)}")
        n_rows = int(frame["target"].notna().sum()) if "target" in frame.columns else 0
        n_clusters = (
            int(frame["cluster_id"].astype(str).nunique()) if "cluster_id" in frame.columns else 0
        )
        if not block["supported"]:
            results[kind] = RungFit(
                kind=kind,
                status=STATUS_BLOCKED,
                reason=REASON_LADDER_COLUMNS_ABSENT,
                missing_columns=tuple(block["missing_columns"]),
                n_rows=n_rows,
                n_clusters=n_clusters,
                comparison=None,
            )
            continue
        common_missing = tuple(support["comparison_common_missing_columns"])
        if common_missing:
            results[kind] = RungFit(
                kind=kind,
                status=STATUS_BLOCKED,
                reason=REASON_LADDER_COMMON_SAMPLE_INCOMPLETE,
                missing_columns=common_missing,
                n_rows=n_rows,
                n_clusters=n_clusters,
                comparison=None,
            )
            continue
        design = _ladder_frame(frame, surprise=surprise, control_column=control_column)
        try:
            comparison = nested_comparison(
                design,
                kinds=(kind,),
                seed=seed,
                minimum_mae_gain=minimum_mae_gain,
            )
        except (ForecastDataError, ValueError, KeyError) as error:
            results[kind] = RungFit(
                kind=kind,
                status=STATUS_BLOCKED,
                reason=f"{REASON_LADDER_FIT_FAILED}: {type(error).__name__}: {error}",
                missing_columns=(),
                n_rows=n_rows,
                n_clusters=n_clusters,
                comparison=None,
            )
            continue
        record = comparison.as_record()
        results[kind] = RungFit(
            kind=kind,
            status=STATUS_COMPLETE,
            reason=None,
            missing_columns=(),
            n_rows=int(design["target"].notna().sum()),
            n_clusters=int(design["cluster_id"].astype(str).nunique()),
            comparison=record,
        )
    return {
        "claim": CLAIM_PROPAGATION,
        "design": {
            "column_map": dict(FORECAST_COLUMN_MAP),
            "unsupplied_columns": support["unsupplied_columns"],
        },
        "support": support,
        "rungs": {kind: fit.as_dict() for kind, fit in results.items()},
    }


def _repo_root() -> Path:
    """The checkout this module was imported from."""
    return Path(__file__).resolve().parents[2]


def _source_manifest(forecast_panel_path: str | Path | None = None) -> dict[str, str | None]:
    """The identities of the declarations and code that produced a result.

    A Git hash would not identify these bytes: the checkout carries untracked work,
    so every file the result's numbers depend on is hashed by content. The list is
    the declarations (the timing specification and the pipeline configuration) and
    the modules that define the estimands and the ladder, so a reader can tell
    whether the rules changed rather than only whether the panel did.
    """
    from . import historical_forecast, models, neighbors, timing_model
    from .ingest import expectations

    root = _repo_root()
    files = {
        "timing_model": Path(timing_model.__file__).resolve(),
        "models": Path(models.__file__).resolve(),
        "study": Path(__file__).resolve(),
        "neighbors": Path(neighbors.__file__).resolve(),
        "historical_forecast": Path(historical_forecast.__file__).resolve(),
        "expectations": Path(expectations.__file__).resolve(),
        "pipeline_config": root / "configs" / "external_history_v1.yaml",
    }
    if forecast_panel_path is not None:
        files["forecast_panel"] = Path(forecast_panel_path)
    manifest: dict[str, str | None] = {}
    for name, path in files.items():
        manifest[name] = storage.hash_file(path) if path.exists() else None
    return manifest


def _source_hash(spec: Any, frame: pd.DataFrame, forecast_panel_path: str | Path | None) -> str:
    """sha256 over the real source, configuration and release-data identities."""
    manifest = _source_manifest(forecast_panel_path)
    manifest["spec_source"] = str(getattr(spec, "source_path", None))
    manifest["release_records"] = _digest_json(
        sorted({str(value) for value in frame["event_id"].dropna().unique()})
    )
    return _digest_json(manifest)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write JSON by replacing a temporary file, so a reader never sees a partial one."""
    text = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def run_study(
    panel_path: str | Path,
    *,
    output_dir: str | Path,
    run_id: str | None = None,
    families: Sequence[str] = ("cpi", "employment"),
    record_registry: bool = True,
    forecast_panel_path: str | Path | None = None,
    surprise: Mapping[str, Mapping[str, Any]] | None = None,
    control_column: str | None = None,
    registry_path: str | Path | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Fit the declared absorption ladder and audit the propagation rung.

    Returns the result payload and writes ``study_result.json`` under
    ``output_dir``. The propagation rung is audited rather than fitted, and its
    blocked reason is written into the artifact, because a panel that cannot
    carry a neighbour column cannot support the claim however the model is
    specified.

    ``forecast_panel_path`` points at the sealed source-time forecast table. When it
    is given, the declared nested ladder is fitted on those real rows and each rung
    is reported with its own status: the own rung is estimable from the recipient's
    own history, and the news and network rungs are blocked with the missing columns
    named until a validated surprise vector and a control donor exist.
    """
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    frame = load_panel(panel_path)
    spec = load_timing_spec()
    resolved_run_id = run_id or f"study-{_digest_json(str(panel_path))[:12]}"
    fits = {family: fit_family(frame, family=family, spec=spec, **kwargs) for family in families}
    support = propagation_support(frame)
    exploratory, exploratory_counts = exploratory_absorption_frame(frame)
    exploratory_fits: dict[str, FamilyFit] = {}
    if exploratory_counts["rows_measurable"]:
        exploratory_fits = {
            family: fit_family(exploratory, family=family, spec=spec, **kwargs)
            for family in families
        }
    panel_hash = frame.attrs.get("panel_hash")
    spec_payload = {
        "model_kind": spec.model_kind,
        "admissible_covariates": list(spec.admissible_covariates),
        "coefficient_families": list(spec.coefficient_families),
        "meaningful_gain_size": spec.meaningful_gain_size,
        "meaningful_response_size": spec.meaningful_response_size,
        "loss": spec.loss,
    }
    result: dict[str, Any] = {
        "run_id": resolved_run_id,
        "generated_by": "market_propagation.study.run_study",
        "evidence_class": EVIDENCE_CLASS,
        "claim": CLAIM_ABSORPTION,
        "claim_not_made": CLAIM_PROPAGATION,
        "clock_basis": "source",
        "panel": {
            "path": str(panel_path),
            "content_hash": panel_hash,
            "rows": len(frame),
            "columns": list(frame.columns),
            "distinct_events": int(frame["event_id"].nunique()),
            "distinct_contracts": int(frame["contract_id"].nunique()),
            "valid_rows": int(frame["valid"].fillna(False).astype(bool).sum()),
        },
        "specification": spec_payload,
        "specification_hash": _digest_json(spec_payload),
        "propagation": support,
        "families": {name: fit.as_dict() for name, fit in fits.items()},
        "exploratory": {
            "label": EXPLORATORY_LABEL,
            "caveat": EXPLORATORY_CAVEAT,
            "counts": exploratory_counts,
            "families": {name: fit.as_dict() for name, fit in exploratory_fits.items()},
        },
    }
    result["flags"] = _flags(fits, support, frame)
    if forecast_panel_path is not None:
        forecast_frame = storage.read_parquet(forecast_panel_path, table="historical_forecast")
        ladder = fit_forecast_ladder(
            forecast_frame, surprise=surprise, control_column=control_column
        )
        ladder["panel"] = {
            "path": str(forecast_panel_path),
            "content_hash": storage.hash_file(Path(forecast_panel_path)),
            "rows": len(forecast_frame),
            "columns": list(forecast_frame.columns),
        }
        result["forecast_ladder"] = ladder
        if any(rung.get("status") == STATUS_COMPLETE for rung in ladder["rungs"].values()):
            result["flags"].append("forecast_ladder_fitted_on_real_rows")
        blocked_rungs = sorted(
            name for name, rung in ladder["rungs"].items() if rung.get("status") != STATUS_COMPLETE
        )
        if blocked_rungs:
            result["flags"].append("forecast_ladder_rungs_blocked")
            result["forecast_ladder"]["blocked_rungs"] = blocked_rungs
    if record_registry:
        result["registry"] = _registry_section(
            output=output,
            registry_path=registry_path,
            run_id=resolved_run_id,
            panel_hash=panel_hash,
            spec_hash=result["specification_hash"],
            source_hash=_source_hash(spec, frame, forecast_panel_path),
            environment_hash=_environment_digest(),
            families=families,
            frame=frame,
            fits=fits,
            result=result,
        )
    path = output / STUDY_RESULT_NAME
    _atomic_json(path, result)
    result["outputs"] = {"study_result": str(path)}
    return result


def _flags(
    fits: Mapping[str, FamilyFit], support: Mapping[str, Any], frame: pd.DataFrame
) -> list[str]:
    flags: list[str] = ["source_clock_alignment_is_retrospective"]
    if any(fit.status == STATUS_COMPLETE for fit in fits.values()):
        flags.append("absorption_ladder_fitted_on_real_rows")
    if not support.get("supported"):
        flags.append("propagation_rung_blocked_inputs_absent")
    if int(frame["valid"].fillna(False).astype(bool).sum()) == 0:
        flags.append("no_valid_panel_rows_so_absorption_is_measured_on_masked_rows_only")
    return flags


def _environment_digest() -> str:
    """Identity of the interpreter and library versions a fit ran under.

    A numeric result is only reproducible against the environment that produced
    it, and the repository holds untracked code, so the environment is recorded
    as its own digest rather than inferred from a commit.
    """
    import platform
    import sys

    import numpy

    return _digest_json(
        {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "numpy": numpy.__version__,
            "pandas": pd.__version__,
        }
    )


def shared_registry_path() -> Path:
    """The one registry file every run of this study records into."""
    return _repo_root() / SHARED_REGISTRY_DIR / REGISTRY_DB_NAME


def _registry_section(
    *,
    output: Path,
    registry_path: str | Path | None = None,
    run_id: str,
    panel_hash: str | None,
    spec_hash: str,
    source_hash: str,
    environment_hash: str,
    families: Sequence[str],
    frame: pd.DataFrame,
    fits: Mapping[str, FamilyFit],
    result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Record the run durably, or state why it was not recorded.

    The registry is one file for every run unless the caller names another, so two
    runs of the same study cannot each write a store of their own and each look
    complete. It is written only when the run names an input identity and at least
    one release, because a record that identifies neither would make the store look
    populated while identifying nothing. A registry failure is reported beside the
    result rather than replacing it: losing the fit because the ledger could not be
    written would discard the evidence the ledger exists to point at.

    ``metrics`` carries the fitted row identities, the frozen model parameters, the
    predictions and the paired event losses, so the stored record points at the
    evidence rather than only at the run.
    """
    section: dict[str, Any] = {"recorded": False, "reason": None, "path": None}
    event_ids = sorted({str(value) for value in frame["event_id"].dropna().unique()})
    if not panel_hash or not event_ids:
        section["reason"] = (
            "the run names no panel content hash or no release, and a record that identifies "
            "neither input would make the registry look populated while identifying nothing"
        )
        return section
    metrics: dict[str, Any] = {f"{name}_status": fit.status for name, fit in fits.items()}
    metrics["panel_rows"] = len(frame)
    metrics["valid_rows"] = int(frame["valid"].fillna(False).astype(bool).sum())
    metrics["fitted_row_ids"] = {
        name: [[str(part) for part in key] for key in fit.fitted_row_keys]
        for name, fit in sorted(fits.items())
    }
    metrics["fitted_row_ids_digest"] = _digest_json(
        {name: [list(key) for key in fit.fitted_row_keys] for name, fit in sorted(fits.items())}
    )
    # The frozen parameters, the held-out predictions and the paired release losses
    # travel with the record: a ledger that identifies the run but not the numbers
    # cannot be re-read into the result it points at.
    metrics["model_comparisons"] = {
        name: dict(fit.comparison.as_record())
        for name, fit in sorted(fits.items())
        if fit.comparison is not None
    }
    if result is not None and "forecast_ladder" in result:
        ladder = result["forecast_ladder"]
        metrics["forecast_ladder_status"] = {
            name: rung.get("status") for name, rung in sorted((ladder.get("rungs") or {}).items())
        }
        metrics["forecast_ladder_blocked_rungs"] = ladder.get("blocked_rungs", [])
        metrics["forecast_ladder_missing_columns"] = {
            name: list(rung.get("missing_columns") or ())
            for name, rung in sorted((ladder.get("rungs") or {}).items())
        }
    path = Path(registry_path) if registry_path is not None else shared_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with ExperimentRegistry(path) as registry:
            registry.record_run(
                {
                    "run_id": run_id,
                    "spec_hash": spec_hash,
                    "data_hash": panel_hash,
                    "source_hash": source_hash,
                    "environment_hash": environment_hash,
                    "event_ids": event_ids,
                    "seed": None,
                    "synthetic": False,
                    "metrics": metrics,
                }
            )
            registry.export_jsonl(output / f"{run_id}.jsonl")
        section.update({"recorded": True, "path": str(path), "shared": registry_path is None})
    except Exception as error:
        section["reason"] = (
            f"the run was not recorded: {type(error).__name__}: {error}; the fitted result is "
            "still written, and the missing record is stated rather than implied"
        )
    return section
