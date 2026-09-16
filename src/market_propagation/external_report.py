"""External-history report: coverage, event cards, response figures and honest gates.

This module owns the orchestration behind the ``report-external`` command. It
turns one verified transaction panel plus the pipeline configuration into a
measurement report a reviewer can trace back to the bytes and the specification
it came from. It owns no estimator and no second implementation of anything the
package already provides:

:func:`market_propagation.storage.read_parquet`
    reads the sealed panel with ``table="trade_panel"``, so the declared schema
    version and the manifest content hash are verified before any row is
    reported on. A panel that does not verify is reported as blocked rather than
    read as if it had.
:func:`market_propagation.evaluation.cluster_bootstrap`
    the event-level mean response at the primary horizon, with a
    release-clustered percentile interval. The independent unit is the economic
    release, so an event contributes one value however many contracts it has.
:class:`market_propagation.registry.ExperimentRegistry`
    durable run provenance, written only where the panel declares the
    provenance the registry requires.
:mod:`market_propagation.reporting`
    the plotting backend discipline, the atomic writers, the markdown table
    formatter, the canonical digest, the stage runner and the recorded runtime,
    dependency and source-tree digests. Reusing them keeps one convention for
    each; a private helper is imported rather than copied, as
    :mod:`market_propagation.sample` already does for
    ``storage._atomic_write_bytes``. The one thing not reused is
    ``reporting._save``, whose artifact record asserts synthetic provenance that
    an external archive panel does not have; the local saver records the panel's
    own declared provenance instead.

Four properties are load-bearing.

*Missing stays missing.* A masked row keeps a null response, a horizon with no
observation has no marker in a figure rather than a zero, and an unavailable
statistic is reported as null with its reason. Null is never read as zero.

*Declared is not observed.* A capability declared in the configuration is
reported beside what this run actually observed. The declared value is overridden
only by an observation, never by the presence of a file, and quantity-weighted
flow stays disabled even where verified contract counts exist.

*Claims and gates are separate.* ``gate`` answers whether this report delivered
what it claims from inputs that verified; ``evidence_gates`` records what the
underlying evidence can support, so a satisfied report over a blocked cohort gate
is the expected development outcome rather than a contradiction.

*No substitute analysis.* A request outside the run's declared specification is
refused with a structured reason. Rows it covers stay in the counts, visibly
outside the governed scope, and are never folded into a declared horizon or
re-labelled into a declared family.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import yaml

from .evaluation import classify_outcome, cluster_bootstrap
from .registry import ExperimentRegistry
from .reporting import (
    BASELINE_REPORT_NAME,
    FIGURES_DIRECTORY,
    JSONL_NAME,
    REGISTRY_DB_NAME,
    _dependency_record,
    _dig,
    _digest,
    _display_path,
    _fmt,
    _locked_environment_digest,
    _md_table,
    _Run,
    _runtime_record,
    _source_tree_digest,
    _write_json,
    _write_text,
    json_ready,
)
from .storage import TRADE_PANEL_COLUMNS, hash_file, read_parquet

__all__ = [
    "CAPABILITY_NAMES",
    "CAPABILITY_TABLE_NAME",
    "CLOCK_CAVEAT",
    "COVERAGE_REPORT_NAME",
    "ESTIMATE_KIND",
    "EVENT_CARDS_NAME",
    "EVIDENCE_CLASS",
    "EXTERNAL_REPORT_NAME",
    "FIGURE_NAMES",
    "GATE_BLOCKED",
    "GATE_SATISFIED",
    "LINEAGE_NAME",
    "STATUS_BLOCKED",
    "STATUS_OK",
    "run_external_report",
]

#: The report's own measurement gate. It answers one question: did this report
#: deliver what it claims, from inputs that verified? It is deliberately not the
#: study's empirical gate, which ``evidence_gates`` carries; a satisfied report
#: over a blocked cohort gate is the expected development outcome.
GATE_SATISFIED = "satisfied"
GATE_BLOCKED = "blocked"

STATUS_OK = "ok"
STATUS_BLOCKED = "blocked"

#: Artifact names this report owns. The baseline summary, the figures directory,
#: the registry database and its JSONL export reuse the names the reproduction
#: aggregator already owns, so one output name means one artifact package-wide.
EXTERNAL_REPORT_NAME = "external_report.json"
COVERAGE_REPORT_NAME = "coverage_report.json"
EVENT_CARDS_NAME = "event_cards.json"
LINEAGE_NAME = "lineage.json"
CAPABILITY_TABLE_NAME = "capability_table.md"
FIGURE_NAMES: tuple[str, ...] = (
    "response_by_horizon.png",
    "coverage_by_horizon.png",
    "response_by_event.png",
)

#: The evidence class every response number in this report belongs to. It names
#: the pipeline path the rows came from, not a claim that a venue's coverage is
#: complete or that a rule vintage is known.
EVIDENCE_CLASS = "external_history_source_time_transaction_panel"

#: The estimate this report is willing to describe, named once so the payload and
#: the artifacts cannot disagree about what was computed.
ESTIMATE_KIND = "descriptive_event_weighted_mean_response_at_the_primary_horizon"

#: The clock caveat, repeated in every exported artifact so a figure, a table or
#: the payload read alone states the same limit.
CLOCK_CAVEAT = (
    "source-clock alignment is retrospective event alignment over recorded venue or block "
    "times; it is not evidence of what a live participant knew"
)

#: Capability names, in the order the configuration declares them. A run reports
#: every one of these whether or not it observed it, so a capality that disappears
#: from the configuration is caught instead of silently dropped.
CAPABILITY_NAMES: tuple[str, ...] = (
    "historical_trades",
    "historical_quotes",
    "receipt_clock",
    "rule_vintage_verified",
    "initial_release_verified",
    "expectation_verified",
    "economic_size_verified",
)

#: Substrings that would mark a panel column as quote-bearing. The trade-panel
#: table declares no bid, ask, spread, depth or quote column, so a report that
#: found one is reading a table this pipeline does not own.
_QUOTE_COLUMN_HINTS: tuple[str, ...] = ("bid", "ask", "spread", "depth", "quote")

#: Availability status the panel records when no receipt evidence exists. A row
#: carrying it establishes no usable interval.
_SOURCE_AVAILABILITY_STATUS = "source_time_only"
_UNIDENTIFIABLE_AVAILABILITY_STATUS = "unidentifiable"

#: The read-modifying request surfaces a panel can carry. Both are refused with a
#: structured reason rather than approximated: a window the configuration does not
#: declare is never rounded to a declared one, and a family it does not govern is
#: never re-labelled into one it does.
_HORIZON_NOT_DECLARED = "horizon_not_declared"
_FAMILY_NOT_DECLARED = "family_not_declared"

#: Blocker codes that prevent the report from delivering what it claims, so the gate
#: fails when any of them is present. A code absent from this set is a recorded limit
#: of the run: it stays visible in ``blockers`` and in the capability table, but it does
#: not block a report whose measurement completed. The line is drawn there because an
#: unverified rule vintage or an unjoined pair count is the expected development state,
#: while a panel that yields no response at all is a blocked measurement.
_GATE_BLOCKING_BLOCKERS: frozenset[str] = frozenset(
    {
        "panel_unverified",
        "panel_incomplete",
        "no_governed_valid_row",
        "no_observed_response",
        "panel_reports_no_response",
        _HORIZON_NOT_DECLARED,
        _FAMILY_NOT_DECLARED,
    }
)


@dataclass(frozen=True, slots=True)
class _ReportSettings:
    """The measurement settings this report applied, read from the configuration.

    Every field is a value the configuration states rather than a default this
    module invents, so the digest records what a run was governed by.
    """

    config_version: str
    scope: str
    horizons_seconds: tuple[int, ...]
    primary_horizon_seconds: int
    permitted_clock_modes: tuple[str, ...]
    calendar_timezone: str | None
    families: tuple[str, ...]
    report_missing_cells: bool
    confirmatory_estimation_permitted: bool
    seeds: tuple[int, ...]
    timing_only_model_kind: str | None
    timing_only_meaningful_response_size: float | None
    timing_only_aggregation_unit: str | None
    timing_only_admissible_covariates: tuple[str, ...]
    timing_only_prohibited_covariates: tuple[str, ...]

    def as_record(self) -> dict[str, Any]:
        return {
            "config_version": self.config_version,
            "scope": self.scope,
            "horizons_seconds": list(self.horizons_seconds),
            "primary_horizon_seconds": self.primary_horizon_seconds,
            "permitted_clock_modes": list(self.permitted_clock_modes),
            "calendar_timezone": self.calendar_timezone,
            "families": list(self.families),
            "report_missing_cells": self.report_missing_cells,
            "confirmatory_estimation_permitted": self.confirmatory_estimation_permitted,
            "seeds": list(self.seeds),
            "timing_only_model_kind": self.timing_only_model_kind,
            "timing_only_meaningful_response_size": self.timing_only_meaningful_response_size,
            "timing_only_aggregation_unit": self.timing_only_aggregation_unit,
            "timing_only_admissible_covariates": list(self.timing_only_admissible_covariates),
            "timing_only_prohibited_covariates": list(self.timing_only_prohibited_covariates),
        }


def _read_settings(config: Mapping[str, Any], *, context: str) -> _ReportSettings:
    """Read the frozen measurement settings, naming the key that is absent."""
    horizons = tuple(
        int(value) for value in _dig(config, "response.horizons_seconds", context=context)
    )
    if not horizons:
        raise ValueError(f"{context}: response.horizons_seconds declares no horizon")
    primary = int(_dig(config, "response.primary_horizon_seconds", context=context))
    if primary not in horizons:
        raise ValueError(
            f"{context}: response.primary_horizon_seconds={primary} is not one of the declared "
            f"horizons {list(horizons)}; a primary horizon outside the curve cannot be reported"
        )
    modes = tuple(str(value) for value in _dig(config, "clock.permitted_modes", context=context))
    if not modes:
        raise ValueError(f"{context}: clock.permitted_modes declares no clock mode")
    coverage = config.get("coverage") or {}
    coverage = coverage if isinstance(coverage, Mapping) else {}
    families = tuple(str(value) for value in coverage.get("families") or ())
    reproducibility = config.get("reproducibility") or {}
    reproducibility = reproducibility if isinstance(reproducibility, Mapping) else {}
    seeds = tuple(int(value) for value in reproducibility.get("seeds") or ())
    timing = config.get("timing_only") or {}
    timing = timing if isinstance(timing, Mapping) else {}
    return _ReportSettings(
        config_version=str(_dig(config, "config_version", context=context)),
        scope=str(_dig(config, "scope", context=context)).strip(),
        horizons_seconds=horizons,
        primary_horizon_seconds=primary,
        permitted_clock_modes=modes,
        calendar_timezone=(
            str(coverage["calendar_timezone"]) if coverage.get("calendar_timezone") else None
        ),
        families=families,
        report_missing_cells=bool(coverage.get("report_missing_cells", False)),
        confirmatory_estimation_permitted=bool(
            (config.get("registered_estimation") or {}).get("confirmatory_estimation_permitted")
        ),
        seeds=seeds,
        timing_only_model_kind=(str(timing["model_kind"]) if timing.get("model_kind") else None),
        timing_only_meaningful_response_size=(
            float(timing["meaningful_response_size"])
            if timing.get("meaningful_response_size") is not None
            else None
        ),
        timing_only_aggregation_unit=(
            str(timing["aggregation_unit"]) if timing.get("aggregation_unit") else None
        ),
        timing_only_admissible_covariates=tuple(
            str(value) for value in timing.get("admissible_covariates") or ()
        ),
        timing_only_prohibited_covariates=tuple(
            str(value) for value in timing.get("prohibited_covariates") or ()
        ),
    )


def _resolve_relative(candidate: str, *, root: Path) -> Path:
    """A configuration path, resolved against the checkout when it is not already rooted."""
    path = Path(candidate)
    if path.is_absolute() or path.exists():
        return path
    return root / path


def _analysis_spec_record(raw: Mapping[str, Any], *, root: Path) -> dict[str, Any]:
    """The registered-estimation specification, hashed when it actually resolves.

    The configuration names this file as the structure a confirmatory fit would be
    frozen against. A path that resolves is hashed so a reported number traces to
    both the measurement rules and the analysis specification; a path that does
    not resolve is recorded as unresolved with its reason and contributes no hash,
    because an absent specification is a fact about this run.
    """
    declared = (raw.get("registered_estimation") or {}).get("analysis_spec")
    record: dict[str, Any] = {
        "declared": declared if isinstance(declared, str) else None,
        "resolved": False,
        "path": None,
        "sha256": None,
        "spec_status": (raw.get("registered_estimation") or {}).get("spec_status"),
        "confirmatory_estimation_permitted": bool(
            (raw.get("registered_estimation") or {}).get("confirmatory_estimation_permitted")
        ),
        "reason": None,
        "used_as_a_fitted_input": False,
    }
    if not isinstance(declared, str) or not declared.strip():
        record["reason"] = (
            "registered_estimation.analysis_spec is absent or not a path, so no analysis "
            "specification hash is recorded"
        )
        return record
    path = _resolve_relative(declared, root=root)
    record["path"] = str(path)
    if not path.is_file():
        record["reason"] = (
            f"registered_estimation.analysis_spec names {declared!r}, which does not resolve to a "
            f"file at {path}; no analysis specification hash is recorded"
        )
        return record
    record["resolved"] = True
    record["sha256"] = hash_file(path)
    record["reason"] = (
        "the analysis specification was hashed so a reported number traces to both the "
        "measurement rules and the specification structure it was produced under"
    )
    return record


def _report_environment_record() -> dict[str, Any]:
    """The recorded runtime, dependency and locked-environment digests.

    The configuration asks a run to record its tool versions. These are the
    reproduction aggregator's own records, so one environment hash means the same
    thing in both entry points.
    """
    runtime = _runtime_record()
    dependencies = _dependency_record()
    return {
        "runtime": runtime,
        "dependencies": dependencies,
        "environment_lock_hash": _locked_environment_digest(dependencies),
    }


def _sealed_metadata(path: Path) -> tuple[dict[str, str], str | None]:
    """Caller-declared metadata a sealed dataset carries, or why it could not be read.

    ``storage.write_parquet(metadata=...)`` stores declarations under
    ``market_propagation.meta.*``. They are the only place a sealer can state a
    provenance fact such as whether the rows are synthetic, and this report reads
    them rather than assuming either answer.
    """
    try:
        schema = pq.read_schema(path)
    except Exception as error:
        return {}, f"{type(error).__name__}: {error}"
    declared: dict[str, str] = {}
    for key, value in (schema.metadata or {}).items():
        name = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
        if not name.startswith("market_propagation.meta."):
            continue
        text = value.decode() if isinstance(value, (bytes, bytearray)) else str(value)
        declared[name[len("market_propagation.meta.") :]] = text
    return declared, None


def _declared_bool(metadata: Mapping[str, str], name: str) -> bool | None:
    """A declared boolean, or ``None`` when the sealer did not declare one."""
    text = metadata.get(name)
    if text is None:
        return None
    lowered = text.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return None


def _panel_manifest(path: Path) -> tuple[dict[str, Any], str | None]:
    """The manifest written beside a sealed dataset, or why it is unreadable."""
    manifest_path = path.with_name(path.name + ".manifest.json")
    if not manifest_path.is_file():
        return {}, f"no manifest at {manifest_path}"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as error:
        return {}, f"{type(error).__name__}: {error}"
    if not isinstance(payload, Mapping):
        return {}, f"{manifest_path} does not carry a JSON object"
    return dict(payload), None


def _as_bool(value: Any) -> bool | None:
    """A declared boolean, or ``None`` where the panel records no value."""
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    return None


def _as_float(value: Any) -> float | None:
    """A finite number, or ``None``. A bool is never a measurement, and a null is not a zero."""
    if isinstance(value, (bool, np.bool_)):
        return None
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        number = float(value)
        return number if math.isfinite(number) else None
    return None


def _as_int(value: Any) -> int | None:
    """An integer count, or ``None``. A bool is never a count."""
    if isinstance(value, (bool, np.bool_)):
        return None
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        number = float(value)
        return int(number) if math.isfinite(number) and number.is_integer() else None
    return None


def _text(value: Any) -> str | None:
    """A non-empty text value, or ``None`` for a missing or empty one."""
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    text = str(value).strip()
    return text or None


def _instant_text(value: Any) -> str | None:
    """A timestamp as ISO-8601 UTC text, or ``None`` where the panel records no time."""
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        return value.tz_convert("UTC").isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, dt.datetime):
        return value.isoformat() if value.tzinfo else value.replace(tzinfo=dt.UTC).isoformat()
    return None


def _flag_list(record: Mapping[str, Any]) -> list[str]:
    """The per-row flags a sealed panel decoded back into a list."""
    value = record.get("flags_json")
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return []


def _rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """The panel as plain mappings, so every later decision reads a Python value.

    A panel is bounded by the configuration (one row per event, contract and
    horizon), so materializing the rows costs nothing next to the archive scan
    that produced them, and it keeps a null a null instead of a pandas sentinel.
    """
    return frame.to_dict(orient="records")


def _counts_of(records: Sequence[Mapping[str, Any]], key: str) -> list[dict[str, Any]]:
    """Rows per distinct value of one column, sorted by descending count.

    A null value is reported as ``null`` rather than dropped, because a column the
    panel leaves empty is a fact about the panel.
    """
    counter: Counter[str | None] = Counter(_text(record.get(key)) for record in records)
    return [
        {key: value, "rows": count}
        for value, count in sorted(counter.items(), key=lambda item: (-item[1], str(item[0])))
    ]


def _exclusion_reasons(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[str | None] = Counter(
        _text(record.get("exclusion_reason"))
        for record in records
        if _as_bool(record.get("valid")) is not True
    )
    return [
        {"exclusion_reason": reason, "rows": count}
        for reason, count in sorted(counter.items(), key=lambda item: (-item[1], str(item[0])))
    ]


def _row_flags(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter(flag for record in records for flag in _flag_list(record))
    return [
        {"flag": flag, "rows": count}
        for flag, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _provenance_text(declared_synthetic: bool | None) -> str:
    """What the sealed dataset declares about its own provenance, in one line."""
    if declared_synthetic is True:
        return (
            "Panel provenance: the sealed dataset declares its rows synthetic, so no number "
            "here is a market observation."
        )
    if declared_synthetic is False:
        return "Panel provenance: the sealed dataset declares its rows non-synthetic."
    return (
        "Panel provenance: the sealed dataset declares no synthetic or non-synthetic "
        "provenance, so neither claim is made here."
    )


def _figure_title(headline: str, provenance: str) -> str:
    """A figure title that states its evidence, so a PNG read alone is not misread."""
    return f"{headline}\n{provenance}\n{CLOCK_CAVEAT}"


def _save_figure(
    fig: Any, path: Path, *, title: str, declared_synthetic: bool | None
) -> dict[str, Any]:
    """Save one figure and record its bytes, title and declared panel provenance.

    The reproduction aggregator's own saver is not used here: it asserts synthetic
    provenance in every record it returns, and this report draws real external
    archive rows whose provenance the sealed dataset has to state for itself.
    """
    try:
        fig.savefig(path, dpi=120, metadata={"Software": "market-propagation"})
    finally:
        plt.close(fig)
    return {
        "name": path.name,
        "sha256": hash_file(path),
        "bytes": path.stat().st_size,
        "title": title,
        "evidence_class": EVIDENCE_CLASS,
        "panel_declared_synthetic": declared_synthetic,
    }


def _figure_response_by_horizon(
    section: Mapping[str, Any], path: Path, *, declared_synthetic: bool | None
) -> dict[str, Any]:
    """Event-weighted mean response by declared horizon, with gaps left as gaps.

    A horizon with no observed response is not plotted. Drawing it at zero would
    report the absence of a print as an unchanged price, which is the substitution
    the panel contract exists to prevent.

    Every cohort is reported over the same declared horizon list, so the axis comes
    from the first cohort and a cohort missing a horizon simply has no point there.
    """
    cohorts = section["cohorts"]
    horizons = (
        [int(entry["horizon_seconds"]) for entry in cohorts[0]["by_horizon"]] if cohorts else []
    )
    positions = np.arange(len(horizons), dtype=np.float64)
    fig, axis = plt.subplots(figsize=(8.4, 4.8))
    plotted = 0
    for cohort in cohorts:
        lookup = {int(entry["horizon_seconds"]): entry for entry in cohort["by_horizon"]}
        observed = [
            (position, _as_float((lookup.get(horizon) or {}).get("mean_response")), horizon)
            for position, horizon in zip(positions, horizons, strict=True)
        ]
        usable = [(position, value) for position, value, _horizon in observed if value is not None]
        if not usable:
            continue
        axis.plot(
            [item[0] for item in usable],
            [item[1] for item in usable],
            marker="o",
            label=f"{cohort['cohort']} ({cohort['n_events_observed']} event(s) with a response)",
        )
        plotted += len(usable)
    axis.axhline(0.0, color="grey", linewidth=0.8, linestyle=":", label="zero reference line")
    axis.set_xticks(positions, [str(horizon) for horizon in horizons])
    axis.set_xlabel("declared horizon (seconds)")
    axis.set_ylabel("mean response (absolute probability units)")
    if plotted:
        axis.legend(fontsize=7)
    else:
        axis.text(
            0.5,
            0.5,
            "no observed response at any declared horizon\n(a missing response is not a zero)",
            ha="center",
            va="center",
            transform=axis.transAxes,
            fontsize=9,
        )
    title = _figure_title(
        "Mean transaction response by horizon, equal weight per economic release",
        _provenance_text(declared_synthetic),
    )
    axis.set_title(title, fontsize=8)
    fig.tight_layout()
    return _save_figure(fig, path, title=title, declared_synthetic=declared_synthetic)


def _figure_coverage_by_horizon(
    coverage: Mapping[str, Any], path: Path, *, declared_synthetic: bool | None
) -> dict[str, Any]:
    """Masked and valid panel rows per declared horizon, counted from the sealed bytes."""
    entries = [entry for entry in coverage["by_horizon"] if entry["declared_in_specification"]]
    horizons = [int(entry["horizon_seconds"]) for entry in entries]
    positions = np.arange(len(entries), dtype=np.float64)
    valid = np.array([float(entry["valid_rows"]) for entry in entries], dtype=np.float64)
    masked = np.array([float(entry["masked_rows"]) for entry in entries], dtype=np.float64)
    fig, axis = plt.subplots(figsize=(8.4, 4.4))
    axis.bar(positions, valid, 0.55, label="valid rows")
    axis.bar(positions, masked, 0.55, bottom=valid, label="masked rows")
    axis.set_xticks(positions, [str(horizon) for horizon in horizons])
    axis.set_xlabel("declared horizon (seconds)")
    axis.set_ylabel("sealed panel rows")
    title = _figure_title(
        "Panel coverage by horizon: every row is valid or masked with a reason",
        _provenance_text(declared_synthetic),
    )
    axis.set_title(title, fontsize=8)
    axis.legend(fontsize=8)
    fig.tight_layout()
    return _save_figure(fig, path, title=title, declared_synthetic=declared_synthetic)


def _figure_response_by_event(
    section: Mapping[str, Any], path: Path, *, declared_synthetic: bool | None
) -> dict[str, Any]:
    """Per-event mean response at the primary horizon, without inventing a bar for a gap.

    An event with no observed primary-horizon response gets no bar, and the note
    counts it against every event the panel reports anywhere in the curve rather
    than only against those that reached the primary horizon. An event whose only
    rows are masked must appear in the missing tally, not vanish from it.
    """
    fig, axis = plt.subplots(figsize=(9.6, 5.2))
    primary = int(section["primary_horizon_seconds"])
    values: list[float] = []
    labels: list[str] = []
    absent: list[str] = []
    total_events: set[str] = set()
    for cohort in section["cohorts"]:
        # The denominator is the cohort's own release universe, so a release whose only
        # rows are masked is counted as missing rather than vanishing from the tally.
        known = {str(event_id) for event_id in cohort["event_ids"]}
        total_events |= known
        entry = next(
            (item for item in cohort["by_horizon"] if int(item["horizon_seconds"]) == primary),
            None,
        )
        if entry is None:
            absent.extend(sorted(known))
            continue
        observed = {
            str(event["event_id"])
            for event in entry["events"]
            if _as_float(event["mean_response"]) is not None
        }
        absent.extend(sorted(known - observed))
        for event in entry["events"]:
            value = _as_float(event["mean_response"])
            if value is None:
                continue
            labels.append(str(event["event_id"]))
            values.append(value)
    absent = sorted(set(absent))
    if values:
        axis.bar(np.arange(len(values), dtype=np.float64), np.array(values, dtype=np.float64), 0.6)
        axis.set_xticks(np.arange(len(labels), dtype=np.float64), labels, rotation=30, fontsize=7)
    note_lines = [
        f"unobserved at {primary}s: {len(absent)} of {len(total_events)} release(s), left empty",
        "an endpoint with no post-release trade is not a zero",
    ]
    if absent:
        note_lines.append("empty: " + ", ".join(absent[:4]) + (" ..." if len(absent) > 4 else ""))
    axis.text(
        0.995,
        0.97,
        "\n".join(note_lines),
        ha="right",
        va="top",
        transform=axis.transAxes,
        fontsize=7,
        color="dimgrey",
    )
    axis.set_xlabel("economic release")
    axis.set_ylabel("mean response (absolute probability units)")
    axis.axhline(0.0, color="grey", linewidth=0.8, linestyle=":")
    axis.margins(x=0.08, y=0.16)
    title = _figure_title(
        f"Mean transaction response per economic release at {primary}s, equal weight per contract",
        _provenance_text(declared_synthetic),
    )
    axis.set_title(title, fontsize=8)
    fig.tight_layout()
    return _save_figure(fig, path, title=title, declared_synthetic=declared_synthetic)


def _scope_declared(settings: _ReportSettings, record: Mapping[str, Any]) -> bool:
    """Whether a row falls inside the event window and family this run declares."""
    family = _text(record.get("family"))
    horizon = _as_int(record.get("horizon_seconds"))
    return (
        family is not None
        and family in settings.families
        and horizon is not None
        and horizon in settings.horizons_seconds
    )


def _unsupported_requests(
    records: Sequence[Mapping[str, Any]], settings: _ReportSettings
) -> list[dict[str, Any]]:
    """Requests the sealed panel expresses that this run's specification does not govern.

    A horizon outside ``response.horizons_seconds`` is refused rather than folded
    into the nearest declared horizon, and a family outside ``coverage.families``
    is refused rather than pooled with a governed family. The rows stay counted so
    the refusal is visible against the panel it came from.
    """
    out: list[dict[str, Any]] = []
    undeclared_horizons: Counter[int] = Counter()
    undeclared_families: Counter[str] = Counter()
    for record in records:
        horizon = _as_int(record.get("horizon_seconds"))
        if horizon is not None and horizon not in settings.horizons_seconds:
            undeclared_horizons[horizon] += 1
        family = _text(record.get("family"))
        if family is not None and family not in settings.families:
            undeclared_families[family] += 1
    if undeclared_horizons:
        out.append(
            {
                "code": _HORIZON_NOT_DECLARED,
                "request": f"a response at horizons {sorted(undeclared_horizons)}",
                "supported": False,
                "rows": int(sum(undeclared_horizons.values())),
                "horizons_seconds": sorted(undeclared_horizons),
                "declared_horizons_seconds": list(settings.horizons_seconds),
                "reason": (
                    "the configuration declares horizons "
                    f"{list(settings.horizons_seconds)}; a response at another horizon is outside "
                    "this run's specification and is reported rather than folded into a declared "
                    "horizon, because widening or interpolating a window changes the estimand"
                ),
                "substitute_analysis_used": False,
            }
        )
    if undeclared_families:
        out.append(
            {
                "code": _FAMILY_NOT_DECLARED,
                "request": f"coverage and response for release families {sorted(undeclared_families)}",
                "supported": False,
                "rows": int(sum(undeclared_families.values())),
                "families": sorted(undeclared_families),
                "declared_families": list(settings.families),
                "reason": (
                    "the configuration governs families "
                    f"{list(settings.families)}; another family is outside this run's "
                    "specification and is counted without being pooled into a governed family, "
                    "because a pooled family would change what the number describes"
                ),
                "substitute_analysis_used": False,
            }
        )
    return sorted(out, key=lambda record: record["code"])


def _coverage_section(
    records: Sequence[Mapping[str, Any]],
    settings: _ReportSettings,
    *,
    spec_digest: str,
    panel: Mapping[str, Any],
) -> dict[str, Any]:
    """The coverage grid: what the panel holds per cell, with missing cells kept."""
    by_horizon: list[dict[str, Any]] = []
    seen_horizons = sorted(
        {
            horizon
            for horizon in (_as_int(record.get("horizon_seconds")) for record in records)
            if horizon is not None
        }
    )
    for horizon in seen_horizons:
        group = [record for record in records if _as_int(record.get("horizon_seconds")) == horizon]
        valid = [record for record in group if _as_bool(record.get("valid")) is True]
        observed = [record for record in valid if _as_float(record.get("response")) is not None]
        by_horizon.append(
            {
                "horizon_seconds": horizon,
                "declared_in_specification": horizon in settings.horizons_seconds,
                "rows": len(group),
                "valid_rows": len(valid),
                "masked_rows": len(group) - len(valid),
                "response_observed_rows": len(observed),
                "response_missing_rows": len(group) - len(observed),
                "events": len({str(record.get("event_id")) for record in group}),
                "contracts": len({str(record.get("contract_id")) for record in group}),
            }
        )
    by_family: list[dict[str, Any]] = []
    seen_families = sorted(
        {value for value in (_text(record.get("family")) for record in records) if value}
    )
    for family in seen_families:
        group = [record for record in records if _text(record.get("family")) == family]
        valid = [record for record in group if _as_bool(record.get("valid")) is True]
        by_family.append(
            {
                "family": family,
                "declared_in_specification": family in settings.families,
                "rows": len(group),
                "valid_rows": len(valid),
                "masked_rows": len(group) - len(valid),
                "events": len({str(record.get("event_id")) for record in group}),
                "contracts": len({str(record.get("contract_id")) for record in group}),
            }
        )
    events = sorted({str(record.get("event_id")) for record in records})
    missing_cells: list[dict[str, Any]] = []
    if settings.report_missing_cells:
        for event_id in events:
            event_rows = [record for record in records if str(record.get("event_id")) == event_id]
            family = _text(event_rows[0].get("family")) if event_rows else None
            if family not in settings.families:
                continue
            covered = {_as_int(record.get("horizon_seconds")) for record in event_rows}
            for horizon in settings.horizons_seconds:
                if horizon not in covered:
                    missing_cells.append(
                        {
                            "event_id": event_id,
                            "family": family,
                            "horizon_seconds": horizon,
                            "reason": "no_row_in_panel",
                            "note": (
                                "the cell is kept in the grid as empty rather than dropped, so the "
                                "missing fraction is read against the preselected denominator"
                            ),
                        }
                    )
    non_null = [record for record in records if _as_float(record.get("response")) is not None]
    envelopes = [
        (
            _as_float(record.get("endpoint_envelope_low")),
            _as_float(record.get("endpoint_envelope_high")),
        )
        for record in records
    ]
    usable_envelopes = [
        (low, high) for low, high in envelopes if low is not None and high is not None
    ]
    widths = [high - low for low, high in usable_envelopes]
    return {
        "spec_digest": spec_digest,
        "panel": dict(panel),
        "engine": (
            "pandas over the rows returned by storage.read_parquet, whose schema version and "
            "manifest content hash were verified before any row was reported on"
        ),
        "rows": len(records),
        "by_horizon": by_horizon,
        "by_family": by_family,
        "by_clock_mode": _counts_of(records, "clock_mode"),
        "by_availability_status": _counts_of(records, "availability_status"),
        "by_cohort": _counts_of(records, "cohort"),
        "by_venue": _counts_of(records, "venue"),
        "by_size_quality": _counts_of(records, "size_quality"),
        "by_event_axis": _counts_of(records, "event_axis"),
        "by_price_convention": _counts_of(records, "price_convention"),
        "by_exclusion_reason": _exclusion_reasons(records),
        "row_flags": _row_flags(records),
        "missing_cells": missing_cells,
        "missing_cell_count": len(missing_cells),
        "missing_cells_reported": settings.report_missing_cells,
        "missing_cells_note": (
            "a missing cell is a cell the panel does not carry; it is reported empty and never "
            "filled by interpolation, widening or a carried-forward price"
        ),
        "post_release_trade_observed": {
            "rows_with": sum(
                1
                for record in records
                if _as_bool(record.get("post_release_trade_observed")) is True
            ),
            "rows_without": sum(
                1
                for record in records
                if _as_bool(record.get("post_release_trade_observed")) is not True
            ),
            "note": (
                "an activity outcome, reported beside the price estimand because the price "
                "response is conditional on observable trading"
            ),
        },
        "endpoint_envelope": {
            "rows_with_envelope": len(usable_envelopes),
            "min_width": min(widths) if widths else None,
            "max_width": max(widths) if widths else None,
            "note": (
                "a tie group is one observation at the finest supported timestamp; the envelope "
                "reports how much the aggregation choice moves the endpoint"
            ),
        },
        "response_null_rows": len(records) - len(non_null),
        "response_null_note": "a null response is an unobserved endpoint, never a zero response",
    }


def _event_cards(
    records: Sequence[Mapping[str, Any]],
    settings: _ReportSettings,
    *,
    spec_digest: str,
    evidence_class: str,
) -> list[dict[str, Any]]:
    """One card per release, carrying its row accounting and every exclusion reason."""
    events = sorted({str(record.get("event_id")) for record in records})
    cards: list[dict[str, Any]] = []
    for event_id in events:
        group = [record for record in records if str(record.get("event_id")) == event_id]
        head = group[0]
        valid = [record for record in group if _as_bool(record.get("valid")) is True]
        observed = [record for record in valid if _as_float(record.get("response")) is not None]
        by_horizon: list[dict[str, Any]] = []
        for horizon in sorted(
            {
                horizon
                for horizon in (_as_int(record.get("horizon_seconds")) for record in group)
                if horizon is not None
            }
        ):
            cells = [
                record for record in group if _as_int(record.get("horizon_seconds")) == horizon
            ]
            cells_valid = [record for record in cells if _as_bool(record.get("valid")) is True]
            responses = [
                value
                for value in (_as_float(record.get("response")) for record in cells_valid)
                if value is not None
            ]
            by_horizon.append(
                {
                    "horizon_seconds": horizon,
                    "declared_in_specification": horizon in settings.horizons_seconds,
                    "rows": len(cells),
                    "valid_rows": len(cells_valid),
                    "contracts_with_a_response": len(responses),
                    "mean_response": (sum(responses) / len(responses)) if responses else None,
                    "min_response": min(responses) if responses else None,
                    "max_response": max(responses) if responses else None,
                }
            )
        cards.append(
            {
                "spec_digest": spec_digest,
                "evidence_class": evidence_class,
                "event_id": event_id,
                "cluster_id": _text(head.get("cluster_id")),
                "family": _text(head.get("family")),
                "family_declared_in_specification": _text(head.get("family")) in settings.families,
                "venue": _text(head.get("venue")),
                "cohort": _text(head.get("cohort")),
                "event_time": _instant_text(head.get("event_time")),
                "clock_mode": _text(head.get("clock_mode")),
                "availability_status": _text(head.get("availability_status")),
                "rule_version": _text(head.get("rule_version")),
                "rule_evidence_quality": _text(head.get("rule_evidence_quality")),
                "rows": len(group),
                "valid_rows": len(valid),
                "masked_rows": len(group) - len(valid),
                "observed_response_rows": len(observed),
                "contracts": len({str(record.get("contract_id")) for record in group}),
                "post_release_trade_observed": any(
                    _as_bool(record.get("post_release_trade_observed")) is True for record in group
                ),
                "by_horizon": by_horizon,
                "exclusion_reasons": _exclusion_reasons(group),
                "row_flags": _row_flags(group),
                "responses": [
                    {
                        "horizon_seconds": _as_int(record.get("horizon_seconds")),
                        "contract_id": _text(record.get("contract_id")),
                        "valid": _as_bool(record.get("valid")),
                        "exclusion_reason": _text(record.get("exclusion_reason")),
                        "baseline": _as_float(record.get("baseline")),
                        "endpoint": _as_float(record.get("endpoint")),
                        "response": _as_float(record.get("response")),
                        "baseline_source_time": _instant_text(record.get("baseline_source_time")),
                        "endpoint_source_time": _instant_text(record.get("endpoint_source_time")),
                        "baseline_age_seconds": _as_float(record.get("baseline_age_seconds")),
                        "endpoint_age_seconds": _as_float(record.get("endpoint_age_seconds")),
                        "tie_group_size": _as_int(record.get("tie_group_size")),
                        "endpoint_envelope_low": _as_float(record.get("endpoint_envelope_low")),
                        "endpoint_envelope_high": _as_float(record.get("endpoint_envelope_high")),
                        "size_quality": _text(record.get("size_quality")),
                        "size_verified": _as_bool(record.get("size_verified")),
                        "flags": _flag_list(record),
                        "locators": record.get("provenance_locators_json"),
                    }
                    for record in group
                ],
                "limits": [
                    CLOCK_CAVEAT,
                    "this card establishes no causal attribution of a price movement to the release",
                    "no quote, spread or depth statement is made: the panel declares no such column",
                ],
            }
        )
    return cards


def _response_section(
    records: Sequence[Mapping[str, Any]],
    settings: _ReportSettings,
    *,
    spec_digest: str,
    seed: int | None,
) -> dict[str, Any]:
    """The event-weighted response per cohort, with the primary horizon estimated once.

    The unit is the economic release, so an event's contracts are averaged first
    and each event then contributes one value. That ordering matters: averaging
    contracts into a row-weighted mean instead would let a release with many
    listed contracts outweigh one with few.
    """
    cohorts = sorted(
        {value for value in (_text(record.get("cohort")) for record in records) if value}
    )
    sections: list[dict[str, Any]] = []
    for cohort in cohorts:
        group = [
            record
            for record in records
            if _text(record.get("cohort")) == cohort and _scope_declared(settings, record)
        ]
        by_horizon: list[dict[str, Any]] = []
        for horizon in settings.horizons_seconds:
            cells = [
                record
                for record in group
                if _as_int(record.get("horizon_seconds")) == horizon
                and _as_bool(record.get("valid")) is True
            ]
            per_event: dict[str, list[float]] = {}
            for record in cells:
                value = _as_float(record.get("response"))
                if value is None:
                    continue
                per_event.setdefault(str(record.get("event_id")), []).append(value)
            event_means = {
                event_id: sum(values) / len(values)
                for event_id, values in sorted(per_event.items())
            }
            values = list(event_means.values())
            by_horizon.append(
                {
                    "horizon_seconds": horizon,
                    "n_events_observed": len(event_means),
                    "n_contract_rows_observed": len(cells)
                    - sum(1 for record in cells if _as_float(record.get("response")) is None),
                    "mean_response": (sum(values) / len(values)) if values else None,
                    "min_response": min(values) if values else None,
                    "max_response": max(values) if values else None,
                    "events": [
                        {
                            "event_id": event_id,
                            "cluster_id": next(
                                (
                                    _text(record.get("cluster_id"))
                                    for record in cells
                                    if str(record.get("event_id")) == event_id
                                ),
                                None,
                            ),
                            "rows_with_a_response": len(per_event[event_id]),
                            "mean_response": value,
                        }
                        for event_id, value in event_means.items()
                    ],
                }
            )
        primary = next(
            (
                entry
                for entry in by_horizon
                if int(entry["horizon_seconds"]) == settings.primary_horizon_seconds
            ),
            None,
        )
        estimate: dict[str, Any]
        if primary is None or not primary["events"]:
            estimate = {
                "status": "unavailable",
                "reason": (
                    f"no governed row carries a response at the primary horizon "
                    f"{settings.primary_horizon_seconds}s, so no interval is computed and none is "
                    "substituted from another horizon"
                ),
                "point": None,
                "coverage": None,
                "interval": None,
                "reading": None,
                "bootstrap": None,
            }
        else:
            cluster_of = {
                str(event["event_id"]): str(event["cluster_id"] or event["event_id"])
                for event in primary["events"]
            }
            labels = [str(event["event_id"]) for event in primary["events"]]
            values = pd.Series(
                [float(event["mean_response"]) for event in primary["events"]], index=labels
            )
            clusters = pd.Series([cluster_of[label] for label in labels], index=labels)
            kwargs: dict[str, Any] = {"name": "event_mean_response"}
            if seed is not None:
                kwargs["seed"] = int(seed)
            bootstrap = cluster_bootstrap(values, clusters, **kwargs)
            sample = (bootstrap.get("samples") or {}).get("event_mean_response") or {}
            interval = (
                None
                if sample.get("lower") is None or sample.get("upper") is None
                else {"lower": sample.get("lower"), "upper": sample.get("upper")}
            )
            # The declared meaningful size turns a bare interval into a reading. It is
            # reported as a classification of the interval, never as a significance
            # claim: the estimate stays descriptive either way.
            reading: dict[str, Any] | None = None
            if interval is not None and settings.timing_only_meaningful_response_size is not None:
                classified = classify_outcome(
                    float(interval["lower"]),
                    float(interval["upper"]),
                    relevant_effect=settings.timing_only_meaningful_response_size,
                )
                reading = {
                    "classification": classified["classification"],
                    "relevant_effect": classified["relevant_effect"],
                    "excludes_null": classified["excludes_null"],
                    "excludes_relevant": classified["excludes_relevant"],
                    "reason": classified["reason"],
                    "meaningful_size_status": (
                        "the configuration declares this as a scientific choice requiring "
                        "power assessment, so the classification is a reading of the interval "
                        "rather than a power claim"
                    ),
                }
            estimate = {
                "status": sample.get("status") or "unavailable",
                "reason": sample.get("reason"),
                "point": sample.get("point"),
                "interval": interval,
                "reading": reading,
                "coverage": bootstrap.get("coverage"),
                "bootstrap": {
                    "method": bootstrap.get("method"),
                    "n_units": bootstrap.get("n_units"),
                    "n_clusters": bootstrap.get("n_clusters"),
                    "samples_requested": bootstrap.get("samples_requested"),
                    "samples_effective": bootstrap.get("samples_effective"),
                    "seed": bootstrap.get("seed"),
                    "seed_source": (
                        "configuration reproducibility.seeds[0]"
                        if seed is not None
                        else "cluster_bootstrap default, the configuration names no seed"
                    ),
                    "degenerate": bootstrap.get("degenerate"),
                },
            }
        sections.append(
            {
                "cohort": cohort,
                "rows": len([r for r in records if _text(r.get("cohort")) == cohort]),
                "governed_rows": len(group),
                # The cohort's full release universe, including releases whose every row is
                # masked. A reader that counted only the observed events would understate
                # the missing fraction against its own denominator.
                "event_ids": sorted(
                    {
                        str(record.get("event_id"))
                        for record in records
                        if _text(record.get("cohort")) == cohort
                    }
                ),
                "n_events_observed": max(
                    [int(entry["n_events_observed"]) for entry in by_horizon] or [0]
                ),
                "by_horizon": by_horizon,
                "primary_horizon_seconds": settings.primary_horizon_seconds,
                "primary_estimate": estimate,
            }
        )
    return {
        "spec_digest": spec_digest,
        "kind": ESTIMATE_KIND,
        "aggregation_unit": (
            "one value per economic release, equal weight; a release's contracts are averaged "
            "inside that release before it enters any cross-release mean"
        ),
        "primary_horizon_seconds": settings.primary_horizon_seconds,
        "cohorts": sections,
        "declared_aggregation_unit": settings.timing_only_aggregation_unit,
        "declared_aggregation_unit_applied": (
            settings.timing_only_aggregation_unit is None
            or settings.timing_only_aggregation_unit == "economic_release"
        ),
        "declared_model_kind": settings.timing_only_model_kind,
        "admissible_covariates": list(settings.timing_only_admissible_covariates),
        "prohibited_covariates": list(settings.timing_only_prohibited_covariates),
        "prohibited_covariates_read_by_this_report": [],
        "prohibited_covariates_note": (
            "the configuration prohibits these because an expectation, a strike in the same "
            "economic units, and any post-event quantity are all unverified here. This report "
            "reads none of them: it uses the response the panel already carries together with its "
            "horizon, its family and its own row counts, and it constructs no covariate model"
        ),
        "covariate_model_fitted": False,
        "covariate_model_fitted_reason": (
            "this report describes observed responses and fits no covariate model, so the "
            "admissible list is recorded as the specification a later fit would follow rather "
            "than as something applied here"
        ),
        "pooled_across_cohorts": len(sections) <= 1,
        "pooling_refused_reason": (
            None
            if len(sections) <= 1
            else (
                "the panel carries more than one cohort; the configuration never pools a "
                "neg-risk cohort with a standard-binary one, so each is reported separately"
            )
        ),
        "confirmatory": False,
        "confirmatory_estimation_permitted": settings.confirmatory_estimation_permitted,
        "tests_reported": False,
    }


def _capabilities(
    raw: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    *,
    panel_verified: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Declared capabilities beside what this run observed, and the effective value.

    The declared value is overridden only by an observation. A file existing, a
    column being present or a trade history joining does not turn a capability on,
    and a capability the panel cannot evidence stays ``unknown`` rather than
    defaulting either way.
    """
    declared_config = raw.get("capabilities") or {}
    declared_config = declared_config if isinstance(declared_config, Mapping) else {}
    columns = [str(name) for name in manifest.get("columns") or TRADE_PANEL_COLUMNS]
    quote_like = sorted(
        name for name in columns if any(hint in name.lower() for hint in _QUOTE_COLUMN_HINTS)
    )
    availability = {_text(record.get("availability_status")) for record in records}
    size_verified_rows = sum(
        1 for record in records if _as_bool(record.get("size_verified")) is True
    )
    rule_versions = {_text(record.get("rule_version")) for record in records}
    has_rule_version = any(value for value in rule_versions)
    declared: dict[str, Any] = {}
    observed: dict[str, Any] = {}
    effective: dict[str, Any] = {}
    reasons: dict[str, str] = {}
    for name in CAPABILITY_NAMES:
        declared[name] = declared_config.get(name, "unknown")
    if not panel_verified:
        for name in CAPABILITY_NAMES:
            observed[name] = None
            effective[name] = declared[name]
            reasons[name] = (
                "the sealed panel did not verify, so no capability was observed and the declared "
                "value is reported unchanged"
            )
        return effective, {
            "declared": declared,
            "observed": observed,
            "reason": reasons,
            "size_verified_rows": size_verified_rows,
            "declared_note": declared_config.get("notes") or {},
            "override_rule": (
                "an effective value is the observation when this run made one, otherwise the "
                "declared value; a run never raises a capability on the strength of a file existing"
            ),
        }
    observed["historical_trades"] = bool(records)
    reasons["historical_trades"] = (
        f"the sealed panel verified with {len(records)} row(s), so trade evidence is present"
        if records
        else "the sealed panel verified and carries no row, so no trade history was observed"
    )
    effective["historical_trades"] = observed["historical_trades"]
    observed["historical_quotes"] = False
    effective["historical_quotes"] = False
    reasons["historical_quotes"] = (
        "no panel column is a quote column (checked "
        f"{len(columns)} declared column(s): "
        f"{', '.join(quote_like) if quote_like else 'none matches a bid, ask, spread, depth or quote name'}"
        "), so spread, depth and coherence recovery stay disabled"
    )
    observed["receipt_clock"] = False
    effective["receipt_clock"] = False
    reasons["receipt_clock"] = (
        "every row records availability as "
        f"{sorted(value for value in availability if value) or 'unset'}, which is not receipt "
        "evidence, so no usable interval is established and none is invented"
    )
    if not has_rule_version:
        observed["rule_vintage_verified"] = False
        effective["rule_vintage_verified"] = False
        reasons["rule_vintage_verified"] = (
            "no row carries a rule version, so the rule vintage is not verified"
        )
    else:
        observed["rule_vintage_verified"] = None
        effective["rule_vintage_verified"] = "unknown"
        reasons["rule_vintage_verified"] = (
            "rows name a rule version, but a version string is not the bound rule evidence "
            "(contract id, raw rule hash, source, observation time, in-force interval and "
            "settlement semantics), and a trade history joining a release cannot establish it"
        )
    observed["initial_release_verified"] = None
    effective["initial_release_verified"] = declared["initial_release_verified"]
    reasons["initial_release_verified"] = (
        "the panel carries market rows only, so this run observed no initial-release payload and "
        "reports the declared value unchanged"
    )
    observed["expectation_verified"] = False
    effective["expectation_verified"] = False
    reasons["expectation_verified"] = (
        "no expectation column exists in the panel, so no surprise-relative statement is "
        "supported and a missing expectation is never a zero-valued surprise"
    )
    observed["economic_size_verified"] = False
    effective["economic_size_verified"] = False
    reasons["economic_size_verified"] = (
        f"{size_verified_rows} row(s) carry a verified source quantity, which is a contract count "
        "on its own axis, and the cleaned Polymarket layers omit quantity entirely; quantity "
        "weighted flow therefore stays disabled"
    )
    if quote_like and declared.get("historical_quotes") is not False:
        reasons["historical_quotes"] += (
            "; a quote-like column is present in the sealed schema, which the trade-panel table "
            "does not declare"
        )
    return effective, {
        "declared": declared,
        "observed": observed,
        "reason": reasons,
        "size_verified_rows": size_verified_rows,
        "declared_note": declared_config.get("notes") or {},
        "override_rule": (
            "an effective value is the observation when this run made one, otherwise the declared "
            "value; a run never raises a capability on the strength of a file existing"
        ),
    }


def _blockers(
    *,
    settings: _ReportSettings,
    panel_check: Mapping[str, Any],
    counts: Mapping[str, Any],
    capabilities: Mapping[str, Any],
    unsupported: Sequence[Mapping[str, Any]],
    masked_reasons: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Every reason this report cannot state something, as a structured record.

    These are the report's own findings about its inputs, so they are surfaced at the
    top level rather than only inside the per-cohort estimates. A reader of the report
    artifact alone has to be able to learn what blocked it without parsing a nested
    reason or reading a caller's stderr.
    """
    out: list[dict[str, Any]] = []
    for record in unsupported:
        out.append(
            {
                "code": record["code"],
                "scope": "unsupported_request",
                "reason": record["reason"],
                "blocks": ["response_at_that_request"],
                "rows": record.get("rows"),
            }
        )
    if panel_check.get("status") == "blocked":
        out.append(
            {
                "code": panel_check["code"],
                "scope": "input",
                "reason": panel_check["reason"],
                "blocks": ["every row of this report"],
                "rows": None,
            }
        )
    if counts.get("governed_valid_rows") == 0:
        out.append(
            {
                "code": "no_governed_valid_row",
                "scope": "measurement",
                "reason": (
                    "no row inside the declared families and horizons is valid, so no response "
                    "exists to report and none is substituted from a coarser window"
                ),
                "blocks": ["response_curve", "baseline_response_summary"],
                "rows": counts.get("valid_rows"),
            }
        )
    elif counts.get("governed_observed_response_rows") == 0:
        out.append(
            {
                "code": "no_observed_response",
                "scope": "measurement",
                "reason": (
                    "valid governed rows exist but every response is null, so no mean is computed "
                    "and a missing endpoint is not read as an unchanged price"
                ),
                "blocks": ["response_curve", "baseline_response_summary"],
                "rows": counts.get("governed_valid_rows"),
            }
        )
    # The masks are the input's own stated reasons, so they are repeated here with their
    # counts. Without this a reader would have to open the coverage artifact to learn that
    # every row was masked, and by which reason.
    named = [
        record
        for record in masked_reasons
        if record.get("exclusion_reason") and int(record.get("rows") or 0) > 0
    ]
    if named:
        out.append(
            {
                "code": "rows_masked_by_reason",
                "scope": "measurement",
                "reason": (
                    "panel rows are masked and carry the reason they cannot be used: "
                    + "; ".join(
                        f"{record['exclusion_reason']}={record['rows']}" for record in named
                    )
                ),
                "blocks": (
                    ["every reported response"]
                    if counts.get("governed_observed_response_rows") == 0
                    else ["the masked rows named in the coverage grid"]
                ),
                "rows": int(sum(int(record.get("rows") or 0) for record in named)),
                "by_reason": [
                    {
                        "exclusion_reason": record["exclusion_reason"],
                        "rows": record["rows"],
                    }
                    for record in named
                ],
            }
        )
    all_masked = counts.get("governed_rows") and counts.get("governed_valid_rows") == 0
    if all_masked:
        out.append(
            {
                "code": "panel_reports_no_response",
                "scope": "measurement",
                "reason": (
                    "a panel was read but it establishes no response: every governed row is "
                    "masked, so the report records the blocked measurement rather than an "
                    "estimate, and no coarser window, carried-forward price or zero is "
                    "substituted for it"
                ),
                "blocks": ["response_curve", "baseline_response_summary", "primary_estimate"],
                "rows": counts.get("governed_rows"),
            }
        )
    if (capabilities.get("observed") or {}).get("rule_vintage_verified") is not True:
        out.append(
            {
                "code": "rule_evidence_missing",
                "scope": "evidence",
                "reason": (
                    "no row carries a verified rule vintage, so eligibility rests on no rule "
                    "evidence: a trade-history join cannot establish which rule was in force, and "
                    "the per-pair rule gate therefore stays unmet"
                ),
                "blocks": ["primary_cohort_eligibility", "G0"],
                "rows": counts.get("rows"),
            }
        )
    out.append(
        {
            "code": "pair_window_counts_not_joined",
            "scope": "evidence",
            "reason": (
                "this report reads the sealed panel and its own row counts; the pre-window, "
                "baseline-window, release-window and endpoint-window pair counts that require the "
                "joined contract coverage grid are not joined here and are not reported as if "
                "they were"
            ),
            "blocks": ["pair_level_window_counts", "selection_bias_comparison"],
            "rows": None,
            "would_be_derived_from": (
                "the contract coverage artifact's per-pair window counts, joined to this panel by "
                "event and contract"
            ),
        }
    )
    if not settings.confirmatory_estimation_permitted:
        out.append(
            {
                "code": "confirmatory_estimation_not_permitted",
                "scope": "analysis",
                "reason": (
                    "registered_estimation.confirmatory_estimation_permitted is false, so this "
                    "report describes observed rows and reports no confirmatory estimate"
                ),
                "blocks": ["confirmatory_estimate", "hypothesis_test"],
                "rows": None,
            }
        )
    if (capabilities.get("observed") or {}).get("receipt_clock") is False:
        out.append(
            {
                "code": "receipt_clock_unavailable",
                "scope": "capability",
                "reason": capabilities["reason"]["receipt_clock"],
                "blocks": ["usable_time_analysis", "latency_attribution"],
                "rows": None,
            }
        )
    if (capabilities.get("observed") or {}).get("economic_size_verified") is False:
        out.append(
            {
                "code": "quantity_weighting_unavailable",
                "scope": "capability",
                "reason": capabilities["reason"]["economic_size_verified"],
                "blocks": ["volume_weighted_response"],
                "rows": capabilities.get("size_verified_rows"),
            }
        )
    # The gate role is stamped in one place rather than repeated on every record, so a
    # code cannot be gate-blocking in one branch and a mere limit in another.
    for record in out:
        record["gate_blocking"] = record["code"] in _GATE_BLOCKING_BLOCKERS
    return out


def _evidence_gates(
    *,
    counts: Mapping[str, Any],
    capabilities: Mapping[str, Any],
    coverage_artifact: Mapping[str, Any],
    estimate: Mapping[str, Any],
    spec_digest: str,
) -> dict[str, Any]:
    """The study's own gates, each recorded from what this run can actually see.

    These are not this report's gate. A blocked G0 is the expected outcome while
    no rule vintage verifies, and reporting it beside a satisfied report gate is
    the honest description of a development run.
    """
    rules_verified = capabilities.get("observed", {}).get("rule_vintage_verified") is True
    g0_detail = (
        "contract rule evidence verified for every reported row"
        if rules_verified
        else (
            "the rule vintage in force at each release is not verified: "
            + str(capabilities.get("reason", {}).get("rule_vintage_verified"))
        )
    )
    coverage_ok = coverage_artifact.get("status") in {"readable", "verified"}
    return {
        "G0": {
            "name": "cohort_and_rule_evidence",
            "status": "ok" if rules_verified else "blocked",
            "evidence_class": EVIDENCE_CLASS,
            "detail": g0_detail,
            "audit_status": coverage_artifact.get("audit_status"),
            "audit_complete": coverage_artifact.get("audit_complete"),
        },
        "G1": {
            "name": "clock_and_availability",
            "status": "blocked",
            "evidence_class": EVIDENCE_CLASS,
            "detail": (
                "external trade rows carry no receipt evidence, so availability is unidentifiable "
                "and the source clock is retrospective alignment only"
            ),
        },
        "G2": {
            "name": "measurement",
            "status": "ok" if counts.get("governed_observed_response_rows") else "blocked",
            "evidence_class": EVIDENCE_CLASS,
            "detail": (
                f"{counts.get('governed_observed_response_rows')} governed row(s) carry an observed "
                f"response out of {counts.get('governed_rows')} governed row(s); masked rows carry "
                "a reason and a null measurement"
            ),
        },
        "G3": {
            "name": "falsification_and_power",
            "status": "blocked",
            "evidence_class": "none",
            "detail": (
                "no falsification or power calculation is run over this panel, so the gate is "
                "blocked rather than assumed to pass"
            ),
        },
        "G4": {
            "name": "held_out_gain",
            "status": "blocked",
            "evidence_class": "none",
            "basis": "declared_default_no_predictive_stage_in_this_report",
            "would_be_derived_from": (
                "a source-time predictive specification with its own clock and label schema"
            ),
            "detail": (
                "this report computes no forecast and no model comparison, so no held-out gain "
                f"exists to report (primary estimate status {estimate.get('status')!r})"
            ),
        },
        "G5": {
            "name": "robustness_and_replication",
            "status": "blocked",
            "evidence_class": "none",
            "basis": "declared_default_no_replication_cohort_in_this_report",
            "would_be_derived_from": (
                "a verified independent replication cohort or a verified cross-venue equivalent "
                "contract pair supplied as an explicit input"
            ),
            "detail": "no independent replication cohort is an input to this report",
        },
        "G6": {
            "name": "claims_and_package",
            "status": "ok" if coverage_ok else "blocked",
            "evidence_class": EVIDENCE_CLASS,
            "detail": (
                f"every artifact carries the specification digest {spec_digest} and the evidence "
                "class it belongs to, and every unsupported request is refused in writing"
            ),
        },
    }


def _baseline_summary_text(payload: Mapping[str, Any]) -> str:
    """The baseline summary: what the panel measured, and what it deliberately does not."""
    counts = payload["counts"]
    coverage = payload["coverage"]
    response = payload["response"]
    lines = [
        "# External-history baseline summary",
        "",
        f"Specification digest: `{payload['spec_digest']}`",
        f"Run id: `{payload['run_id']}`",
        f"Report gate: **{payload['gate']}** ({payload['gate_detail']['reason']})",
        f"Evidence class: `{payload['evidence_class']}`",
        f"Panel: `{payload['inputs']['panel']['path']}` "
        f"(table `{payload['inputs']['panel']['table']}`, "
        f"content hash `{payload['inputs']['panel']['content_hash']}`, "
        f"verified `{payload['inputs']['panel']['verified']}`)",
        "",
        CLOCK_CAVEAT,
        "",
        "## Panel accounting",
        "",
        _md_table(
            ["measure", "rows"],
            [
                ["rows in the sealed panel", counts["rows"]],
                ["rows inside the declared families and horizons", counts["governed_rows"]],
                ["valid rows", counts["valid_rows"]],
                ["masked rows", counts["masked_rows"]],
                ["governed valid rows", counts["governed_valid_rows"]],
                ["rows with an observed response", counts["observed_response_rows"]],
                [
                    "governed rows with an observed response",
                    counts["governed_observed_response_rows"],
                ],
                ["economic releases", counts["events"]],
                ["release clusters", counts["clusters"]],
                ["contracts", counts["contracts"]],
                ["declared families present", counts["families"]],
                ["cohorts", counts["cohorts"]],
                ["rows outside the declared scope", counts["out_of_scope_rows"]],
            ],
        ),
        "",
        "## Coverage by horizon",
        "",
        _md_table(
            ["horizon (s)", "declared", "rows", "valid", "masked", "response observed"],
            [
                [
                    entry["horizon_seconds"],
                    str(entry["declared_in_specification"]).lower(),
                    entry["rows"],
                    entry["valid_rows"],
                    entry["masked_rows"],
                    entry["response_observed_rows"],
                ]
                for entry in coverage["by_horizon"]
            ],
        ),
        "",
        "## Coverage by family",
        "",
        _md_table(
            ["family", "declared", "rows", "valid", "masked", "events", "contracts"],
            [
                [
                    entry["family"],
                    str(entry["declared_in_specification"]).lower(),
                    entry["rows"],
                    entry["valid_rows"],
                    entry["masked_rows"],
                    entry["events"],
                    entry["contracts"],
                ]
                for entry in coverage["by_family"]
            ],
        ),
        "",
        "## Masked rows by reason",
        "",
        _md_table(
            ["exclusion reason", "rows"],
            [
                [entry["exclusion_reason"] or "(none recorded)", entry["rows"]]
                for entry in coverage["by_exclusion_reason"]
            ]
            or [["(none recorded)", 0]],
        ),
        "",
        f"Missing cells kept in the grid: {coverage['missing_cell_count']}. "
        f"{coverage['missing_cells_note']}",
        "",
        "## Response at the primary horizon (descriptive only)",
        "",
    ]
    for cohort in response["cohorts"]:
        estimate = cohort["primary_estimate"]
        interval = estimate.get("interval") or {}
        bootstrap = estimate.get("bootstrap") or {}
        lines.extend(
            [
                f"Cohort `{cohort['cohort']}`: {cohort['governed_rows']} governed row(s), "
                f"{cohort['n_events_observed']} event(s) with an observed response at any declared "
                "horizon.",
                "",
                _md_table(
                    ["measure", "value"],
                    [
                        ["primary horizon (s)", response["primary_horizon_seconds"]],
                        ["estimate kind", response["kind"]],
                        ["status", estimate.get("status")],
                        [
                            "events with a governed response",
                            len(
                                next(
                                    (
                                        entry["events"]
                                        for entry in cohort["by_horizon"]
                                        if int(entry["horizon_seconds"])
                                        == int(response["primary_horizon_seconds"])
                                    ),
                                    [],
                                )
                            ),
                        ],
                        ["mean response", _fmt(estimate.get("point"))],
                        ["interval lower", _fmt(interval.get("lower"))],
                        ["interval upper", _fmt(interval.get("upper"))],
                        ["coverage", _fmt(estimate.get("coverage"))],
                        ["release clusters resampled", bootstrap.get("n_clusters")],
                        ["bootstrap samples effective", bootstrap.get("samples_effective")],
                        ["seed source", bootstrap.get("seed_source")],
                        ["reason", estimate.get("reason")],
                    ],
                ),
                "",
            ]
        )
    lines.extend(
        [
            "Null means the statistic is unavailable, unidentified or unobserved at this input. "
            "It is never a zero, and no interval is reported where the cluster count cannot "
            "support one.",
            "",
            "## What this summary does not establish",
            "",
        ]
    )
    for blocker in payload["blockers"]:
        lines.append(f"- `{blocker['code']}` ({blocker['scope']}): {blocker['reason']}")
    lines.extend(
        [
            "",
            "No confirmatory estimate, hypothesis test, forecast or model comparison is reported "
            "here, and no quantity-weighted or quote-derived number is produced.",
            "",
            f"Specification digest `{payload['spec_digest']}`; run id `{payload['run_id']}`.",
            "",
        ]
    )
    return "\n".join(lines)


def _capability_table_text(payload: Mapping[str, Any]) -> str:
    """The capability and blocker table: declared, observed, effective, and what blocks what."""
    caps = payload["capabilities"]
    lines = [
        "# Capability and blocker table",
        "",
        f"Specification digest: `{payload['spec_digest']}`",
        f"Run id: `{payload['run_id']}`",
        f"Evidence class: `{payload['evidence_class']}`",
        "",
        CLOCK_CAVEAT,
        "",
        "## Capabilities",
        "",
        _md_table(
            ["capability", "declared", "observed", "effective", "reason"],
            [
                [
                    name,
                    _fmt(caps["declared"].get(name)),
                    _fmt(caps["observed"].get(name)),
                    _fmt(payload["capability_flags"].get(name)),
                    caps["reason"].get(name),
                ]
                for name in CAPABILITY_NAMES
            ],
        ),
        "",
        caps["override_rule"],
        "",
        "## Blockers",
        "",
        _md_table(
            ["code", "scope", "gate blocking", "reason", "blocks", "rows"],
            [
                [
                    blocker["code"],
                    blocker["scope"],
                    _fmt(bool(blocker.get("gate_blocking"))),
                    blocker["reason"],
                    ", ".join(blocker["blocks"]),
                    _fmt(blocker["rows"]),
                ]
                for blocker in payload["blockers"]
            ]
            or [["(none)", "", "", "", "", ""]],
        ),
        "",
        "## Unsupported requests",
        "",
        _md_table(
            ["code", "request", "rows", "reason"],
            [
                [record["code"], record["request"], record["rows"], record["reason"]]
                for record in (payload["unsupported"] or {}).get("records", [])
            ]
            or [["(none)", "", "", ""]],
        ),
        "",
        "## Evidence gates",
        "",
        _md_table(
            ["gate", "name", "status", "evidence class", "detail"],
            [
                [
                    key,
                    value.get("name"),
                    value.get("status"),
                    value.get("evidence_class"),
                    value.get("detail"),
                ]
                for key, value in sorted(payload["evidence_gates"].items())
            ],
        ),
        "",
    ]
    return "\n".join(lines)


def _registry_section(
    *,
    output: Path,
    run_id: str,
    spec_digest: str,
    event_ids: Sequence[str],
    panel: Mapping[str, Any],
    config_hash: str,
    environment_hash: str,
    declared_synthetic: bool | None,
    counts: Mapping[str, Any],
) -> dict[str, Any]:
    """The recorded durable provenance, or an explicit record of why nothing was stored.

    The registry demands a boolean synthetic flag, and it never assumes one. This
    report can only state what the sealed dataset declared, so a panel that
    declares nothing is not recorded rather than recorded under a guessed flag.
    """
    section: dict[str, Any] = {
        "path": REGISTRY_DB_NAME,
        "export": JSONL_NAME,
        "recorded": False,
        "reason": None,
        "run_id": None,
        "n_runs": None,
        "n_reservations": None,
        "n_event_claims": None,
        "spec_digest": spec_digest,
        "data_hash": panel.get("content_hash"),
        "source_hash": config_hash,
        "environment_hash": environment_hash,
    }
    if not panel.get("verified"):
        section["reason"] = "the sealed panel did not verify, so no run was recorded for it"
        return section
    if declared_synthetic is None:
        section["reason"] = (
            "the sealed panel declares no synthetic or non-synthetic provenance in its metadata, "
            "and the registry never assumes which one a run is, so no run was recorded"
        )
        return section
    if not event_ids:
        section["reason"] = "the panel names no economic release, so there is no cohort to record"
        return section
    try:
        path = output / REGISTRY_DB_NAME
        with ExperimentRegistry(path) as registry:
            registry.record_run(
                {
                    "run_id": run_id,
                    "spec_hash": spec_digest,
                    "data_hash": str(panel.get("content_hash")),
                    "source_hash": config_hash,
                    "environment_hash": environment_hash,
                    "event_ids": sorted({str(event_id) for event_id in event_ids}),
                    "metrics": {
                        "kind": ESTIMATE_KIND,
                        "rows": int(counts.get("rows") or 0),
                        "valid_rows": int(counts.get("valid_rows") or 0),
                        "governed_observed_response_rows": int(
                            counts.get("governed_observed_response_rows") or 0
                        ),
                        "evidence_class": EVIDENCE_CLASS,
                        "confirmatory": False,
                    },
                    "synthetic": declared_synthetic,
                }
            )
            snapshot = registry.snapshot()
            registry.export_jsonl(output / JSONL_NAME)
    except Exception as error:
        section["reason"] = (
            f"the run was not recorded: {type(error).__name__}: {error}; the report still "
            "describes every row it read"
        )
        return section
    section.update(
        {
            "recorded": True,
            "reason": (
                "the sealed panel declared its provenance, so the run is recorded with the "
                "specification digest, the panel content hash and the configuration hash"
            ),
            "run_id": run_id,
            "n_runs": len(snapshot["runs"]),
            "n_reservations": len(snapshot["reservations"]),
            "n_event_claims": len(snapshot["event_claims"]),
            "event_ids_recorded": len({str(event_id) for event_id in event_ids}),
            "synthetic_as_declared_by_the_panel": declared_synthetic,
        }
    )
    return section


def run_external_report(
    config_path: str | Path,
    panel_path: str | Path,
    output_dir: str | Path,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Build the external-history report from a verified panel and one configuration.

    ``config_path`` names the pipeline configuration that governs the run: the
    declared horizons, families, permitted clock modes and capability
    declarations are read from it rather than assumed here. ``panel_path`` names a
    panel already sealed by :func:`market_propagation.storage.write_parquet` with
    ``table="trade_panel"``; it is read through
    :func:`market_propagation.storage.read_parquet`, so its declared schema version
    and manifest content hash are verified before any row is reported on.
    ``output_dir`` receives the report, the coverage report, the event cards, the
    baseline summary, the lineage record, the capability and blocker table and the
    response figures. ``run_id`` defaults to the specification digest, so a rerun
    over unchanged inputs lands the same run id and the same artifact bytes.

    A missing configuration or a missing panel raises: neither is discovered and
    neither is substituted. A panel that exists but fails verification is a
    blocked result with a usable artifact rather than an exception, because the
    refusal is the finding.

    The returned mapping carries ``outputs``, ``gate``, ``blocked``,
    ``capability_flags``, ``spec_digest``, ``run_id``, ``counts`` and ``flags``.
    ``blocked`` is true when the report gate is blocked, when a request outside
    the run's specification was refused, or when a stage could not produce its
    artifact; the CLI maps it to exit code 2.
    """
    config = Path(config_path)
    panel = Path(panel_path)
    if not config.is_file():
        raise FileNotFoundError(f"pipeline configuration not found: {config}")
    if not panel.is_file():
        raise FileNotFoundError(
            f"sealed trade panel not found: {panel}; an external report is produced from a "
            "verified panel it is given, never from one selected on the caller's behalf"
        )
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"{config}: the pipeline configuration must be a mapping")
    settings = _read_settings(raw, context=str(config))
    if run_id is not None:
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError(f"run_id={run_id!r} must be a non-empty str or None")
        run_id = run_id.strip()

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parent.parent.parent
    stages = _Run()

    config_hash = hash_file(config)
    analysis_spec = _analysis_spec_record(raw, root=root)
    environment = _report_environment_record()
    environment_hash = _digest(environment)
    metadata, metadata_error = _sealed_metadata(panel)
    manifest, manifest_error = _panel_manifest(panel)
    declared_synthetic = _declared_bool(metadata, "synthetic")
    panel_sha256 = hash_file(panel)
    spec_digest = _digest(
        {
            "config_sha256": config_hash,
            "analysis_spec_sha256": analysis_spec.get("sha256"),
            "panel_file_sha256": panel_sha256,
            "settings": settings.as_record(),
        }
    )
    if run_id is None:
        run_id = spec_digest

    frame: pd.DataFrame | None = None
    panel_check: dict[str, Any] = {
        "code": "panel_verified",
        "status": STATUS_OK,
        "reason": (
            "the panel verified through storage.read_parquet: its declared table and schema "
            "version agree with this build and its manifest content hash matches its bytes"
        ),
    }
    try:
        frame = read_parquet(panel, table="trade_panel")
    except FileNotFoundError as error:
        raise FileNotFoundError(f"sealed trade panel not found: {panel}") from error
    except Exception as error:
        panel_check = {
            "code": "panel_unverified",
            "status": STATUS_BLOCKED,
            "reason": (
                f"the panel did not verify through storage.read_parquet: "
                f"{type(error).__name__}: {error}; the report describes the failure instead of "
                "reporting on bytes it cannot verify"
            ),
        }
    if frame is not None:
        missing = [name for name in TRADE_PANEL_COLUMNS if name not in frame.columns]
        if missing:
            panel_check = {
                "code": "panel_incomplete",
                "status": STATUS_BLOCKED,
                "reason": (
                    f"the verified panel is missing declared trade-panel column(s) {missing}; a "
                    "partial panel is not reported on"
                ),
            }
            frame = None

    records = _rows(frame) if frame is not None else []
    panel_record = {
        "path": _display_path(panel, root),
        "manifest_path": _display_path(panel.with_name(panel.name + ".manifest.json"), root),
        "table": _text(manifest.get("table")),
        "schema_version": _text(manifest.get("schema_version")),
        "coverage_epoch": _text(manifest.get("coverage_epoch")),
        "content_hash": _text(manifest.get("content_hash")),
        "manifest_row_count": _as_int(manifest.get("row_count")),
        "file_sha256": panel_sha256,
        "row_count": len(records),
        "verified": panel_check["status"] == STATUS_OK,
        "error": panel_check.get("reason") if panel_check["status"] == STATUS_BLOCKED else None,
        "manifest_error": manifest_error,
        "declared_metadata": dict(metadata),
        "declared_metadata_error": metadata_error,
        "declared_synthetic": declared_synthetic,
    }

    coverage_artifact = _coverage_artifact(raw, root=root)
    governed = [record for record in records if _scope_declared(settings, record)]
    governed_valid = [record for record in governed if _as_bool(record.get("valid")) is True]
    governed_observed = [
        record for record in governed_valid if _as_float(record.get("response")) is not None
    ]
    valid = [record for record in records if _as_bool(record.get("valid")) is True]
    observed = [record for record in valid if _as_float(record.get("response")) is not None]
    unsupported = _unsupported_requests(records, settings)
    counts: dict[str, Any] = {
        "rows": len(records),
        "governed_rows": len(governed),
        "out_of_scope_rows": len(records) - len(governed),
        "valid_rows": len(valid),
        "masked_rows": len(records) - len(valid),
        "governed_valid_rows": len(governed_valid),
        "observed_response_rows": len(observed),
        "governed_observed_response_rows": len(governed_observed),
        "response_null_rows": len(records) - len(observed),
        "governed_response_null_rows": len(governed) - len(governed_observed),
        "events": len({str(record.get("event_id")) for record in records}),
        "clusters": len(
            {value for value in (_text(record.get("cluster_id")) for record in records) if value}
        ),
        "contracts": len({str(record.get("contract_id")) for record in records}),
        "families": len(
            {
                value
                for value in (_text(record.get("family")) for record in records)
                if value in settings.families
            }
        ),
        "cohorts": len(
            {value for value in (_text(record.get("cohort")) for record in records) if value}
        ),
        "horizons_seconds": sorted(
            {
                horizon
                for horizon in (_as_int(record.get("horizon_seconds")) for record in records)
                if horizon is not None
            }
        ),
        "rows_by_horizon": {
            str(entry["horizon_seconds"]): entry["rows"]
            for entry in _counts_of(records, "horizon_seconds")
        },
        "rows_by_family": {
            str(entry["family"]): entry["rows"] for entry in _counts_of(records, "family")
        },
    }
    capability_flags, capabilities = _capabilities(
        raw, records, manifest, panel_verified=panel_check["status"] == STATUS_OK
    )
    response = _response_section(
        records, settings, spec_digest=spec_digest, seed=_first_seed(settings)
    )
    coverage = _coverage_section(records, settings, spec_digest=spec_digest, panel=panel_record)
    cards = _event_cards(records, settings, spec_digest=spec_digest, evidence_class=EVIDENCE_CLASS)
    primary_estimate = _primary_estimate(response)
    evidence_gates = _evidence_gates(
        counts=counts,
        capabilities=capabilities,
        coverage_artifact=coverage_artifact,
        estimate=primary_estimate,
        spec_digest=spec_digest,
    )
    blockers = _blockers(
        settings=settings,
        panel_check=panel_check,
        counts=counts,
        capabilities=capabilities,
        unsupported=unsupported,
        masked_reasons=coverage["by_exclusion_reason"],
    )

    # A row may record the usable clock only with availability evidence behind it. Source
    # alignment is retrospective, so a usable claim over source-only availability is the
    # one overclaim this report can catch from the panel alone.
    overclaimed = sorted(
        {
            str(record.get("contract_id"))
            for record in records
            if _text(record.get("clock_mode")) == "usable"
            and _text(record.get("availability_status")) == _SOURCE_AVAILABILITY_STATUS
        }
    )
    checks = [
        {
            "id": panel_check["code"],
            "scope": "input",
            "status": panel_check["status"],
            "reason": panel_check["reason"],
        },
        {
            "id": "panel_rows",
            "scope": "input",
            "status": STATUS_OK if counts["rows"] else STATUS_BLOCKED,
            "reason": f"the sealed panel carries {counts['rows']} row(s)",
        },
        {
            "id": "panel_events",
            "scope": "input",
            "status": STATUS_OK if counts["events"] else STATUS_BLOCKED,
            "reason": f"the sealed panel names {counts['events']} economic release(s)",
        },
        {
            "id": "clock_mode_permitted",
            "scope": "specification",
            "status": (
                STATUS_OK
                if {_text(record.get("clock_mode")) for record in records}
                <= set(settings.permitted_clock_modes)
                else STATUS_BLOCKED
            ),
            "reason": (
                "every recorded clock mode is one the configuration permits: "
                f"{sorted({value for value in (_text(record.get('clock_mode')) for record in records) if value})}"
            ),
            "permitted_clock_modes": list(settings.permitted_clock_modes),
        },
        {
            "id": "clock_mode_does_not_overclaim_availability",
            "scope": "specification",
            "status": STATUS_BLOCKED if overclaimed else STATUS_OK,
            "reason": (
                f"row(s) for contract(s) {overclaimed} claim the usable clock while recording "
                f"{_SOURCE_AVAILABILITY_STATUS} availability: source alignment is retrospective and "
                "never becomes usable time without receipt evidence"
                if overclaimed
                else (
                    f"no row records usable time over {_SOURCE_AVAILABILITY_STATUS} availability; "
                    f"rows recording {_UNIDENTIFIABLE_AVAILABILITY_STATUS} correctly establish no "
                    "usable interval"
                )
            ),
            "contracts": overclaimed,
        },
        {
            "id": "governed_valid_row",
            "scope": "measurement",
            "status": STATUS_OK if counts["governed_valid_rows"] else STATUS_BLOCKED,
            "reason": (
                f"{counts['governed_valid_rows']} row(s) inside the declared families and horizons "
                "are valid"
            ),
        },
        {
            "id": "governed_observed_response",
            "scope": "measurement",
            "status": STATUS_OK if counts["governed_observed_response_rows"] else STATUS_BLOCKED,
            "reason": (
                f"{counts['governed_observed_response_rows']} governed row(s) carry an observed "
                "response"
            ),
        },
        {
            "id": "coverage_artifact",
            "scope": "input",
            "status": (
                STATUS_OK
                if coverage_artifact["status"] in {"readable", "verified"}
                else STATUS_BLOCKED
            ),
            "reason": coverage_artifact["reason"],
        },
    ]
    # The gate reads both the measurement checks and the blockers that prevent this report
    # from delivering its claim, so ``gate`` and ``blocked`` can never disagree. A blocker
    # that only records a limit of the run is reported without failing the gate.
    gating_blockers = [record for record in blockers if record.get("gate_blocking")]
    blocked_checks = [check for check in checks if check["status"] == STATUS_BLOCKED]
    gate = GATE_BLOCKED if blocked_checks or gating_blockers else GATE_SATISFIED
    gate_reasons = [f"{check['id']}: {check['reason']}" for check in blocked_checks] + [
        f"{record['code']}: {record['reason']}" for record in gating_blockers
    ]
    gate_reason = (
        "; ".join(gate_reasons)
        if gate_reasons
        else (
            "the panel verified, every recorded clock mode is permitted, governed valid rows with "
            "observed responses exist and the named coverage artifact was read"
        )
    )

    figures: list[dict[str, Any]] = []
    figures_dir = output / FIGURES_DIRECTORY
    figures_dir.mkdir(parents=True, exist_ok=True)
    for name, action in (
        (
            FIGURE_NAMES[0],
            lambda path: _figure_response_by_horizon(
                response, path, declared_synthetic=declared_synthetic
            ),
        ),
        (
            FIGURE_NAMES[1],
            lambda path: _figure_coverage_by_horizon(
                coverage, path, declared_synthetic=declared_synthetic
            ),
        ),
        (
            FIGURE_NAMES[2],
            lambda path: _figure_response_by_event(
                response, path, declared_synthetic=declared_synthetic
            ),
        ),
    ):
        record = stages.stage(f"figure:{name}", lambda p=figures_dir / name, a=action: a(p))
        if record:
            figures.append(record)

    registry_section = _registry_section(
        output=output,
        run_id=run_id,
        spec_digest=spec_digest,
        event_ids=sorted({str(record.get("event_id")) for record in records}),
        panel=panel_record,
        config_hash=config_hash,
        environment_hash=environment_hash,
        declared_synthetic=declared_synthetic,
        counts=counts,
    )
    source_tree = stages.stage("source_tree_digest", lambda: _source_tree_digest(root))
    lineage = {
        "spec_digest": spec_digest,
        "run_id": run_id,
        "generated_by": "market_propagation.external_report.run_external_report",
        "evidence_class": EVIDENCE_CLASS,
        "config": {
            "path": _display_path(config, root),
            "sha256": config_hash,
            "config_version": settings.config_version,
            "scope": settings.scope,
        },
        "analysis_specification": dict(analysis_spec),
        "panel": dict(panel_record),
        "coverage_artifact": dict(coverage_artifact),
        "external_root": _external_root_record(raw, root=root),
        "release_dataset": _release_dataset_record(raw, root=root),
        "environment": {
            "hash": environment_hash,
            "runtime": environment["runtime"],
            "dependencies": environment["dependencies"],
            "environment_lock_hash": environment["environment_lock_hash"],
        },
        "source_tree": source_tree,
        "source_tree_error": (
            None
            if source_tree
            else next(
                (entry for entry in stages.blocked if entry.startswith("source_tree_digest:")),
                None,
            )
        ),
        "timing": {
            "elapsed_seconds": None,
            "reason": (
                "elapsed wall-clock time is deliberately absent: every value in this report is a "
                "function of the declared inputs and the bytes on disk, so a rerun over unchanged "
                "inputs lands identical bytes"
            ),
        },
        "record_scope": [
            "the configuration hash and the analysis specification hash, when it resolves",
            "the panel table, schema version, coverage epoch and content hash it verified against",
            "the coverage artifact the configuration names, at the status it recorded",
            "the runtime, dependency and source-tree digests behind the reported numbers",
        ],
        "reproducibility": _reproducibility_record(raw, panel=panel_record, root=root),
    }
    capability_table = _capability_table_text(
        {
            "spec_digest": spec_digest,
            "run_id": run_id,
            "evidence_class": EVIDENCE_CLASS,
            "capabilities": capabilities,
            "capability_flags": capability_flags,
            "blockers": blockers,
            "unsupported": _unsupported_record(unsupported),
            "evidence_gates": evidence_gates,
        }
    )
    baseline_text = _baseline_summary_text(
        {
            "spec_digest": spec_digest,
            "run_id": run_id,
            "gate": gate,
            "gate_detail": {"reason": gate_reason},
            "evidence_class": EVIDENCE_CLASS,
            "inputs": {"panel": panel_record},
            "counts": counts,
            "coverage": coverage,
            "response": response,
            "blockers": blockers,
        }
    )

    created: list[dict[str, Any]] = []

    def emit(record: dict[str, Any] | None, *, label: str) -> dict[str, Any] | None:
        if record is None:
            stages.blocked.append(f"{label}: the artifact was not written")
            stages.blocked_ids.append(label)
            return None
        record = {**record, "label": label}
        created.append(record)
        return record

    emit(
        stages.stage(
            "coverage_report", lambda: _write_json(output / COVERAGE_REPORT_NAME, coverage)
        ),
        label=COVERAGE_REPORT_NAME,
    )
    emit(
        stages.stage(
            "event_cards",
            lambda: _write_json(
                output / EVENT_CARDS_NAME,
                {
                    "spec_digest": spec_digest,
                    "run_id": run_id,
                    "evidence_class": EVIDENCE_CLASS,
                    "card_count": len(cards),
                    "cards": cards,
                },
            ),
        ),
        label=EVENT_CARDS_NAME,
    )
    emit(
        stages.stage(
            "baseline_summary",
            lambda: _write_text(output / BASELINE_REPORT_NAME, baseline_text),
        ),
        label=BASELINE_REPORT_NAME,
    )
    emit(
        stages.stage("lineage", lambda: _write_json(output / LINEAGE_NAME, lineage)),
        label=LINEAGE_NAME,
    )
    emit(
        stages.stage(
            "capability_table",
            lambda: _write_text(output / CAPABILITY_TABLE_NAME, capability_table),
        ),
        label=CAPABILITY_TABLE_NAME,
    )
    for record in figures:
        created.append(
            {
                **record,
                "label": f"{FIGURES_DIRECTORY}/{record['name']}",
                "synthetic": declared_synthetic,
            }
        )

    outputs = _output_records(created, root=root)
    flags: list[str] = []
    if counts["masked_rows"]:
        flags.append("panel_masks_present")
    if counts["out_of_scope_rows"]:
        flags.append("panel_carries_rows_outside_the_declared_specification")
    if coverage["row_flags"]:
        flags.append("panel_rows_carry_quality_flags")
    if coverage["endpoint_envelope"]["rows_with_envelope"]:
        flags.append("tie_group_envelopes_present")
    if coverage["missing_cell_count"]:
        flags.append("missing_coverage_cells_kept_in_the_grid")
    if unsupported:
        flags.append("unsupported_request_refused_without_substitution")
    if declared_synthetic is None:
        flags.append("panel_provenance_not_declared")
    if registry_section["recorded"]:
        flags.append("run_recorded_in_the_durable_registry")
    else:
        flags.append("run_not_recorded_in_the_durable_registry")
    flags.append("source_clock_alignment_is_retrospective")
    if analysis_spec.get("resolved"):
        flags.append("analysis_specification_hashed")
    else:
        flags.append("analysis_specification_unresolved")

    # ``blocked`` is derived from the gate and from whether every stage produced its
    # artifact, so the two can never disagree. A limit that does not fail the gate is
    # recorded in ``blockers`` without blocking the report.
    blocked = gate == GATE_BLOCKED or bool(stages.blocked_ids)
    status = STATUS_BLOCKED if blocked else STATUS_OK
    blocked_reason = (
        "; ".join(f"{record['code']}: {record['reason']}" for record in gating_blockers)
        if gating_blockers
        else None
    )
    unsupported_record = _unsupported_record(unsupported)
    self_record = {
        "name": EXTERNAL_REPORT_NAME,
        "path": _display_path(output / EXTERNAL_REPORT_NAME, root),
        "sha256": None,
        "bytes": None,
        "reason": (
            "a document cannot record the hash of its own final bytes; hash the file at "
            f"{_display_path(output / EXTERNAL_REPORT_NAME, root)} to verify it"
        ),
        "label": EXTERNAL_REPORT_NAME,
    }
    payload: dict[str, Any] = {
        "operation": "external_report",
        "classification": "external_history_measurement_report",
        "status": status,
        "blocked": blocked,
        "gate": gate,
        "gate_detail": {
            "gate": gate,
            "status": status,
            "reason": gate_reason,
            "checks": checks,
            "blocked_by": [record["code"] for record in gating_blockers],
            "blocked_reasons": [
                {
                    "code": record["code"],
                    "scope": record["scope"],
                    "reason": record["reason"],
                    "blocks": record["blocks"],
                    "rows": record.get("rows"),
                }
                for record in gating_blockers
            ],
            "policy": (
                "this gate reports whether the report delivered what it claims from inputs that "
                "verified; the study's own gates are in evidence_gates, so a satisfied report over "
                "a blocked cohort gate is the expected development outcome"
            ),
        },
        "spec_digest": spec_digest,
        "run_id": run_id,
        "config_hash": config_hash,
        "analysis_spec_hash": analysis_spec.get("sha256"),
        "panel_file_sha256": panel_sha256,
        "output_dir": str(output),
        "evidence_class": EVIDENCE_CLASS,
        "clock_caveat": CLOCK_CAVEAT,
        "settings": settings.as_record(),
        "inputs": {
            "config_path": _display_path(config, root),
            "panel": panel_record,
            "coverage_artifact": coverage_artifact,
            "analysis_specification": dict(analysis_spec),
            "verified": panel_check["status"] == STATUS_OK
            and coverage_artifact["status"] in {"readable", "verified"},
            "checks": checks,
        },
        "lineage": lineage,
        "counts": counts,
        "flags": flags,
        "capability_flags": capability_flags,
        "capabilities": capabilities,
        "blockers": blockers,
        "blocked_reason": blocked_reason,
        "limits": [
            {
                "code": record["code"],
                "scope": record["scope"],
                "reason": record["reason"],
                "blocks": record["blocks"],
            }
            for record in blockers
            if not record.get("gate_blocking")
        ],
        "limits_note": (
            "these are limits of the run rather than causes of its gate: they stay visible so a "
            "reader can see what the panel cannot support, and they are listed here so the gate "
            "reasons are not confused with them"
        ),
        "unsupported": unsupported_record,
        "evidence_gates": evidence_gates,
        "estimates": {
            "reported": bool(counts["governed_observed_response_rows"]),
            "kind": ESTIMATE_KIND,
            "confirmatory": False,
            "confirmatory_estimation_permitted": settings.confirmatory_estimation_permitted,
            "tests_reported": False,
            "primary_horizon_seconds": settings.primary_horizon_seconds,
            "by_cohort": [
                {"cohort": cohort["cohort"], **(cohort["primary_estimate"])}
                for cohort in response["cohorts"]
            ],
            "reason": (
                "a descriptive event-weighted mean over observed rows is reported with a "
                "release-clustered interval; no confirmatory estimate, significance claim or "
                "substitute analysis is produced"
                if counts["governed_observed_response_rows"]
                else "no governed row carries an observed response, so no estimate is reported"
            ),
        },
        "coverage": coverage,
        "event_cards": cards,
        "response": response,
        "figures": figures,
        "registry": registry_section,
        "outputs": outputs,
        "files": [outputs[record["name"]] for record in created if record["name"] in outputs],
        "self": self_record,
        "warnings": list(stages.warnings),
        "blocked_stages": list(stages.blocked),
        "notes": [
            "every artifact carries the specification digest, so a reported number traces to the "
            "configuration, the analysis specification and the panel it came from",
            "a null denotes an unobserved, unavailable or unidentified statistic; it is never a zero",
            "no synthetic substitution, implicit widening, interpolation or post-close filling is "
            "performed at any point in this report",
            CLOCK_CAVEAT,
        ],
    }
    payload["files"] = sorted(payload["files"], key=lambda record: str(record.get("name")))
    # The gate is decided before the document is written, so the file on disk and the
    # returned mapping cannot disagree about it. Only the digest of the document's own
    # bytes is added afterwards, because a document cannot contain its own final hash.
    if stages.blocked and gate == GATE_SATISFIED:
        gate = GATE_BLOCKED
        payload["gate"] = GATE_BLOCKED
        payload["gate_detail"]["gate"] = GATE_BLOCKED
        payload["gate_detail"]["checks"] = [
            *payload["gate_detail"]["checks"],
            {
                "id": "artifact_written",
                "scope": "output",
                "status": STATUS_BLOCKED,
                "reason": "; ".join(stages.blocked),
            },
        ]
        payload["gate_detail"]["blocked_by"] = [
            *payload["gate_detail"]["blocked_by"],
            "artifact_written",
        ]
        payload["gate_detail"]["reason"] = (
            "the measurement checks passed but a stage did not produce its artifact: "
            + "; ".join(stages.blocked)
        )
        payload["blocked_reason"] = "; ".join(stages.blocked)
    payload["blocked"] = gate == GATE_BLOCKED or bool(stages.blocked_ids)
    payload["status"] = STATUS_BLOCKED if payload["blocked"] else STATUS_OK
    payload["blocked_stages"] = list(stages.blocked)
    payload["warnings"] = list(stages.warnings)
    ready = json_ready(payload)
    stages.stage(
        "external_report",
        lambda: _write_json(output / EXTERNAL_REPORT_NAME, ready),
    )
    written = output / EXTERNAL_REPORT_NAME
    final_self = {
        "name": EXTERNAL_REPORT_NAME,
        "path": _display_path(written, root),
        "sha256": hash_file(written) if written.is_file() else None,
        "bytes": written.stat().st_size if written.is_file() else None,
        "label": EXTERNAL_REPORT_NAME,
        "reason": (
            "recorded in the returned mapping because the artifact's own copy of this entry "
            "cannot hold the hash of its own final bytes"
        ),
    }
    ready["self"] = final_self
    ready["outputs"] = {**ready["outputs"], EXTERNAL_REPORT_NAME: final_self}
    ready["files"] = sorted(
        [*ready["files"], final_self],
        key=lambda record: str(record.get("name")),
    )
    return ready


def _first_seed(settings: _ReportSettings) -> int | None:
    """The resampling seed the configuration names, or ``None`` when it names none."""
    return settings.seeds[0] if settings.seeds else None


def _primary_estimate(response: Mapping[str, Any]) -> dict[str, Any]:
    """The primary-horizon estimate of a single-cohort panel, for the gate records."""
    cohorts = response.get("cohorts") or []
    if not cohorts:
        return {"status": "unavailable", "reason": "the panel names no cohort"}
    return dict(cohorts[0]["primary_estimate"])


def _coverage_artifact(raw: Mapping[str, Any], *, root: Path) -> dict[str, Any]:
    """The coverage artifact the configuration names, read at the status it records.

    The artifact belongs to the coverage stage, not to this report, so its own
    completeness is repeated rather than asserted. A missing or unreadable
    artifact is a blocked input: without it this report cannot show the coverage
    grid the panel's rows should be read against.
    """
    declared = (raw.get("inputs") or {}).get("audit_coverage")
    record: dict[str, Any] = {
        "declared": declared if isinstance(declared, str) else None,
        "path": None,
        "sha256": None,
        "bytes": None,
        "status": "unreadable",
        "reason": None,
        "audit_status": None,
        "audit_complete": None,
        "audit_gate": None,
        "event_count": None,
        "unsatisfied_gates": [],
        "explicitly_selected": True,
    }
    if not isinstance(declared, str) or not declared.strip():
        record["reason"] = (
            "inputs.audit_coverage is absent from the configuration, so this report has no "
            "coverage artifact to read the panel against"
        )
        return record
    path = _resolve_relative(declared, root=root)
    record["path"] = _display_path(path, root)
    if not path.is_file():
        record["reason"] = f"the coverage artifact named by the configuration is not at {path}"
        return record
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:
        record["reason"] = (
            f"the coverage artifact at {path} could not be read: {type(error).__name__}: {error}"
        )
        return record
    payload = payload if isinstance(payload, Mapping) else {}
    record.update(
        {
            "status": "readable",
            "sha256": hash_file(path),
            "bytes": path.stat().st_size,
            "reason": (
                "the named coverage artifact was read and is cited at the status it recorded; it "
                "is never re-run or upgraded here"
            ),
            "audit_status": payload.get("status"),
            "audit_complete": payload.get("complete"),
            "audit_gate": payload.get("gate"),
            "event_count": payload.get("event_count"),
            "unsatisfied_gates": list(payload.get("unsatisfied_gates") or ()),
        }
    )
    return record


def _external_root_record(raw: Mapping[str, Any], *, root: Path) -> dict[str, Any]:
    """The archive root the configuration names, recorded without reading it.

    The panel is already sealed, so the archives themselves are upstream of this
    report. Whether the root is present is a fact about the checkout, not a claim
    about the rows.
    """
    declared = (raw.get("inputs") or {}).get("root")
    if not isinstance(declared, str) or not declared.strip():
        return {
            "declared": None,
            "path": None,
            "exists": None,
            "reason": "inputs.root is absent from the configuration",
        }
    path = _resolve_relative(declared, root=root)
    return {
        "declared": declared,
        "path": _display_path(path, root),
        "exists": path.exists(),
        "reason": (
            "recorded for lineage only: this report reads the sealed panel, not the archive layers, "
            "and never rescans them"
        ),
    }


def _release_dataset_record(raw: Mapping[str, Any], *, root: Path) -> dict[str, Any]:
    """The archived-release dataset the configuration names, recorded without re-hashing it.

    Hashing an archive-sized input is the inventory's job. This record states where
    the dataset is and whether it is present, and says plainly that the digest is
    owned elsewhere so a consumer does not read a null here as an absent file.
    """
    declared = (raw.get("inputs") or {}).get("release_dataset")
    if not isinstance(declared, str) or not declared.strip():
        return {
            "declared": None,
            "path": None,
            "exists": None,
            "sha256": None,
            "reason": "inputs.release_dataset is absent from the configuration",
        }
    path = _resolve_relative(declared, root=root)
    return {
        "declared": declared,
        "path": _display_path(path, root),
        "exists": path.is_file(),
        "size_bytes": path.stat().st_size if path.is_file() else None,
        "sha256": None,
        "reason": (
            "the dataset digest is owned by the coverage and inventory stages, which re-read each "
            "record against its own raw bytes; this report cites the path and does not re-hash it"
        ),
    }


def _unsupported_record(unsupported: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    """The refused requests, or ``None`` when the panel asked for nothing outside the run."""
    if not unsupported:
        return None
    first = dict(unsupported[0])
    return {
        **first,
        "count": len(unsupported),
        "records": [dict(record) for record in unsupported],
    }


def _reproducibility_record(
    raw: Mapping[str, Any], *, panel: Mapping[str, Any], root: Path
) -> dict[str, Any]:
    """What the configuration asks a run to record, and where each declaration is answered.

    The configuration's reproducibility block names the facts a run should preserve:
    the source tree, the selected shards, the input hashes, the tool versions, peak
    memory and elapsed time. A panel is sealed upstream of this report, so the shard
    selection is answered by the manifest's coverage epoch and by the inventory that
    produced it, not re-derived here. Peak memory and elapsed time are deliberately
    not reported for this entry point, because this module performs no memory-heavy
    scan and a recorded duration would put wall-clock variance into artifacts whose
    whole point is to be reproducible byte for byte. Each declaration is therefore
    reported as satisfied, delegated or deliberately omitted, with the reason, so an
    omission is visible instead of merely absent.
    """
    declared = raw.get("reproducibility") or {}
    declared = declared if isinstance(declared, Mapping) else {}
    return {
        "declared": {str(key): value for key, value in declared.items()},
        "source_tree": (
            "recorded in the lineage section as a digest over the measured source files"
            if declared.get("record_source_tree_hash")
            else "not requested by the configuration"
        ),
        "selected_shards": (
            "not re-derived here: the panel is sealed upstream, and the manifest's coverage "
            f"epoch {panel.get('coverage_epoch')!r} names the vintage its rows came from. Shard "
            "selection is the inventory and extraction stages' record, not this report's"
            if declared.get("record_selected_shards")
            else "not requested by the configuration"
        ),
        "input_hashes": (
            "recorded: the configuration, the analysis specification and the sealed panel are all "
            "hashed before any row is reported on"
            if declared.get("record_input_hashes")
            else "not requested by the configuration"
        ),
        "tool_versions": (
            "recorded in the lineage section as the runtime record and the dependency versions"
            if declared.get("record_tool_versions")
            else "not requested by the configuration"
        ),
        "peak_memory": (
            "deliberately not reported by this entry point: a single panel read is bounded by the "
            "configuration, and a process-wide high-water mark would vary between two runs over "
            "identical bytes"
            if declared.get("record_peak_memory")
            else "not requested by the configuration"
        ),
        "elapsed_seconds": (
            "deliberately not reported by this entry point: see lineage.timing; a duration would "
            "put wall-clock variance into artifacts that must reproduce byte for byte"
            if declared.get("record_elapsed_seconds")
            else "not requested by the configuration"
        ),
        "seeds": list(declared.get("seeds") or ()),
        "root": _display_path(root, root),
    }


def _output_records(
    created: Sequence[Mapping[str, Any]], *, root: Path
) -> dict[str, dict[str, Any]]:
    """One record per written artifact, keyed by the name a consumer reads it under.

    A figure sits inside the figures directory while its writer records only its
    own file name, so the key is prefixed with that directory here. Every other
    writer already records its bare name, so its key is that name unchanged.
    """
    del root  # each writer records the path it actually wrote
    out: dict[str, dict[str, Any]] = {}
    for record in created:
        bare = str(record["name"])
        labelled = str(record.get("label") or bare)
        is_figure = labelled.startswith(FIGURES_DIRECTORY + "/")
        key = f"{FIGURES_DIRECTORY}/{bare}" if is_figure else bare
        out[key] = {
            "name": key,
            "path": key,
            "label": labelled,
            "sha256": record.get("sha256"),
            "bytes": record.get("bytes"),
            "kind": "figure" if is_figure else "artifact",
            "reason": None,
        }
        if "title" in record:
            out[key]["title"] = record["title"]
            out[key]["evidence_class"] = record.get("evidence_class")
            out[key]["panel_declared_synthetic"] = record.get("panel_declared_synthetic")
    return out
