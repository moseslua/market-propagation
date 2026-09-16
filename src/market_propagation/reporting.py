"""Reproduction aggregator: one call builds the real offline artifacts and reports honestly.

This module owns no estimator, no data reducer and no second implementation of
anything upstream already does. It wires the settled public APIs together for the
``reproduce`` command:

:func:`market_propagation.sample.build_sample`
    builds the packaged synthetic raw/replay example and seals both replay folds.
:func:`market_propagation.storage.query_sealed`
    counts coverage over the sealed Parquet, so the counts come from the sealed
    bytes rather than from the in-memory frames that produced them.
:func:`market_propagation.models.local_projections`
    descriptive mean-response curves from that sample with release-clustered
    uncertainty. No shock column exists in the fixture, so no slope is estimated.
:func:`market_propagation.simulation.simulate_scenario` with
:func:`market_propagation.simulation.primary_target` and
:func:`market_propagation.models.nested_comparison`
    the production held-out comparison of the whole implemented ladder
    (``no_change``, ``own``, ``news``, ``network``) on the null and the recovery
    process, with the promotion gate still reading the news-versus-network
    contrast.
:func:`market_propagation.falsification.network_falsification`
    the production null/recovery audit of that gate, whose payload is fed back in
    as the comparison's null assessment.
:func:`market_propagation.evaluation.power_assessment`
    between-release response-slope power at explicitly synthetic residual scale.
:func:`market_propagation.coherence.coherence_distance`
    constructed feasible, midpoint-inconsistent and infeasible boxes, each with
    its own payout matrix and assumptions, reporting the min-max distance from
    the quoted box to the coherent set beside the raw midpoint diagnostic.
:class:`market_propagation.registry.ExperimentRegistry`
    the run record, with no locked-test reservation.

Three properties are load-bearing.

*Honest gates.* A stage that cannot run records why and blocks the run's
``complete`` flag instead of producing a substitute. Missing empirical controls
(the fixture carries no expectation, so no surprise slope and no shock placebo
exists) are reported unevaluated, never filled with zero.

*Convergent output.* Every value written is a function of the declared inputs,
seeds and the actual bytes on disk. Nothing wall-clock enters ``metrics.json`` or
``manifest.json``; the one timestamp lives in the registry's own run record,
where it is stamped once and reused on replay.

*Immutability.* Sealed datasets go through
:func:`market_propagation.storage.write_parquet`, which refuses to replace
differing bytes. Reports, metrics and figures use an atomic replace, so a rerun
with different wording updates the file while a rerun with identical content
lands identical bytes.

Every result here is a synthetic software/methods reproduction. Nothing in it is
empirical evidence about a real venue.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import importlib.metadata as importlib_metadata
import json
import math
import os
import platform
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

from .coherence import coherence_distance, threshold_payoffs
from .evaluation import power_assessment
from .falsification import network_falsification
from .models import MODEL_KINDS, nested_comparison
from .registry import ExperimentRegistry
from .sample import SOURCE_PANEL_NAME, USABLE_PANEL_NAME, build_sample
from .simulation import primary_target, simulate_scenario
from .storage import (
    FORECAST_COLUMNS,
    FORECAST_SCHEMA_VERSION,
    DatasetRef,
    RawStore,
    duckdb_connection,
    hash_bytes,
    hash_file,
    query_sealed,
    read_parquet,
    write_parquet,
)

__all__ = [
    "FIGURE_NAMES",
    "FORECAST_PANEL_NAME",
    "JSONL_NAME",
    "MANIFEST_NAME",
    "METRICS_NAME",
    "REGISTRY_DB_NAME",
    "REPORT_NAMES",
    "json_ready",
    "reproduce",
]

METRICS_NAME = "metrics.json"
MANIFEST_NAME = "manifest.json"
JSONL_NAME = "experiment_registry.jsonl"
REGISTRY_DB_NAME = "experiment_registry.sqlite3"
FORECAST_PANEL_NAME = "forecast_panel.parquet"
FIGURES_DIRECTORY = "figures"
BASELINE_REPORT_NAME = "baseline_report.md"
CONDITIONAL_REPORT_NAME = "conditional_propagation_report.md"
DATA_CARD_NAME = "data_card.md"
PAPER_NAME = "paper.md"

FIGURE_NAMES: tuple[str, ...] = (
    "coverage.png",
    "response_uncertainty.png",
    "model_comparison.png",
    "null_vs_communication.png",
    "coherence.png",
)

REPORT_NAMES: tuple[str, ...] = (
    BASELINE_REPORT_NAME,
    CONDITIONAL_REPORT_NAME,
    DATA_CARD_NAME,
    PAPER_NAME,
)

#: The measurement bound the sample is rebuilt with; the panel contract's default.
MAX_AGE_SECONDS = 120.0
#: This entry point's own floors, enforced below; the estimators' own minima are
#: lower (``evaluation.weighted_event_slope`` accepts two events, ``falsification``
#: two draws, ``evaluation.clustered_bootstrap`` two samples).
MIN_N_EVENTS = 4
MIN_REPETITIONS = 20
MIN_BOOTSTRAP_SAMPLES = 2

#: Runtime distributions whose versions the manifest records.
DEPENDENCIES: tuple[str, ...] = (
    "numpy",
    "scipy",
    "pandas",
    "pyarrow",
    "duckdb",
    "httpx",
    "PyYAML",
    "matplotlib",
    "websockets",
)

#: Files whose hashes define the dirty source-tree digest.
_SOURCE_PATTERNS: tuple[str, ...] = (
    "src/market_propagation/**/*.py",
    "src/market_propagation/**/*.json",
    "configs/*.yaml",
)


def _json_convert(value: Any, path: str, notes: list[str]) -> Any:
    """Convert one value into strict JSON, recording every lossy decision.

    An unhandled type raises instead of falling back to ``str()``: a whole
    estimator or record object stringified into the report would read as
    provenance nobody can use.
    """
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        return _json_convert(value.item(), path, notes)
    if isinstance(value, np.ndarray):
        return [
            _json_convert(item, f"{path}[{index}]", notes)
            for index, item in enumerate(value.tolist())
        ]
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        notes.append(f"{path}: non-finite float {value!r} serialized as null")
        return None
    if isinstance(value, Decimal):
        if value.is_finite():
            return str(value)
        notes.append(f"{path}: non-finite Decimal {value!r} serialized as null")
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).hex()
    if isinstance(value, pd.Timestamp):
        if value is pd.NaT or pd.isna(value):
            notes.append(f"{path}: NaT serialized as null")
            return None
        return value.tz_convert("UTC").isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()
    if isinstance(value, pd.Series):
        return [
            _json_convert(item, f"{path}[{index}]", notes)
            for index, item in enumerate(value.tolist())
        ]
    if isinstance(value, pd.DataFrame):
        return [
            _json_convert(row, f"{path}[{index}]", notes)
            for index, row in enumerate(value.to_dict(orient="records"))
        ]
    if isinstance(value, dt.datetime):
        return value.isoformat() if value.tzinfo else value.replace(tzinfo=dt.UTC).isoformat()
    if isinstance(value, dt.timedelta):
        return value.total_seconds()
    if isinstance(value, dt.date | dt.time):
        return value.isoformat()
    if isinstance(value, Enum):
        return _json_convert(value.value, path, notes)
    if isinstance(value, Path):
        return str(value)
    for accessor in ("as_record", "as_dict"):
        method = getattr(value, accessor, None)
        if callable(method):
            return _json_convert(method(), path, notes)
    if is_dataclass(value) and not isinstance(value, type):
        return _json_convert(asdict(value), path, notes)
    if isinstance(value, Mapping):
        return {
            (key if isinstance(key, str) else str(key)): _json_convert(item, f"{path}.{key}", notes)
            for key, item in value.items()
        }
    if isinstance(value, set | frozenset):
        return [
            _json_convert(item, f"{path}[{index}]", notes)
            for index, item in enumerate(sorted(value, key=repr))
        ]
    if isinstance(value, Sequence):
        return [_json_convert(item, f"{path}[{index}]", notes) for index, item in enumerate(value)]
    raise TypeError(
        f"{path}: {type(value).__name__} has no declared JSON representation; convert it "
        "intentionally at the boundary rather than stringifying it"
    )


def json_ready(value: Any) -> Any:
    """A JSON-compatible object: finite numbers, exact decimals as strings, missing as null.

    Decimals stay exact strings because a binary float would lose the quoted
    precision. Non-finite floats and NaT become ``null``; use
    :func:`_json_ready_with_notes` when the reason for each null is needed.
    """
    return _json_convert(value, "$", [])


def _json_ready_with_notes(value: Any) -> tuple[Any, list[str]]:
    notes: list[str] = []
    return _json_convert(value, "$", notes), notes


def _canonical(value: Any) -> str:
    return json.dumps(
        json_ready(value), sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    )


def _digest(value: Any) -> str:
    return hash_bytes(_canonical(value).encode("utf-8"))


def _text_digest(text: str) -> str:
    return hash_bytes(text.encode("utf-8"))


def _write_text(path: Path, text: str) -> dict[str, Any]:
    """Atomically replace ``path`` with ``text``, never leaving a partial file."""
    payload = text.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise
    return {"name": path.name, "sha256": hash_bytes(payload), "bytes": len(payload)}


def _write_json(path: Path, payload: Any) -> dict[str, Any]:
    text = json.dumps(
        json_ready(payload), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
    )
    return _write_text(path, text + "\n")


class _Run:
    """Stage runner: a failed stage is recorded and blocks, never substituted."""

    def __init__(self) -> None:
        self.blocked_ids: list[str] = []
        self.blocked: list[str] = []
        self.warnings: list[str] = []

    def stage(self, stage_id: str, action: Any) -> Any:
        try:
            return action()
        except Exception as error:  # a blocked stage is a result, not a crash
            self.blocked_ids.append(stage_id)
            self.blocked.append(f"{stage_id}: {type(error).__name__}: {error}")
            return None


def _dig(mapping: Any, dotted: str, *, context: str) -> Any:
    current = mapping
    for key in dotted.split("."):
        if not isinstance(current, Mapping) or key not in current:
            raise ValueError(f"{context}: required configuration key {dotted!r} is absent")
        current = current[key]
    return current


@dataclass(frozen=True, slots=True)
class _Settings:
    """The scientific settings this run actually passed to the estimators."""

    master_seed: int
    simulation_seed: int
    resampling_seed: int
    config_bootstrap_samples: int
    bootstrap_samples: int
    horizon_seconds: int
    train_fraction: float
    validation_fraction: float
    minimum_response: float
    minimum_mae_gain: float
    prediction_delay_seconds: float

    def as_record(self) -> dict[str, Any]:
        return {
            "master_seed": self.master_seed,
            "simulation_seed": self.simulation_seed,
            "resampling_seed": self.resampling_seed,
            "config_bootstrap_samples_default": self.config_bootstrap_samples,
            "bootstrap_samples_used": self.bootstrap_samples,
            "horizon_seconds": self.horizon_seconds,
            "train_fraction": self.train_fraction,
            "validation_fraction": self.validation_fraction,
            "smallest_relevant_response_probability_points": self.minimum_response,
            "smallest_relevant_mae_gain_probability_points": self.minimum_mae_gain,
            "prediction_time_offset_seconds": self.prediction_delay_seconds,
        }

    def sources(self) -> dict[str, str]:
        return {
            "master_seed": "randomness.master_seed",
            "simulation_seed": "randomness.simulation_seed",
            "resampling_seed": "randomness.resampling_seed",
            "config_bootstrap_samples_default": "randomness.bootstrap_samples_default",
            "bootstrap_samples_used": "reproduce(bootstrap_samples=)",
            "horizon_seconds": "forecast.horizon_seconds",
            "train_fraction": "splits.train_fraction",
            "validation_fraction": "splits.validation_fraction",
            "smallest_relevant_response_probability_points": (
                "thresholds.smallest_relevant_response_probability_points"
            ),
            "smallest_relevant_mae_gain_probability_points": (
                "thresholds.smallest_relevant_mae_gain_probability_points"
            ),
            "prediction_time_offset_seconds": (
                "event_windows.forecast_windows.prediction_time_offset_seconds"
            ),
        }


def _resolve_relative(candidate: str, *, root: Path) -> Path:
    path = Path(candidate)
    if path.is_absolute() or path.exists():
        return path
    return root / path


def _load_settings(
    spec_path: Path, *, bootstrap_samples: int
) -> tuple[_Settings, dict[str, Any], dict[str, Path], dict[str, Any]]:
    study = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    root = spec_path.parent.parent if spec_path.parent.name == "configs" else spec_path.parent
    windows_path = _resolve_relative(
        _dig(study, "study.event_windows_path", context=str(spec_path)), root=root
    )
    cohort_path = _resolve_relative(
        _dig(study, "study.cohort_path", context=str(spec_path)), root=root
    )
    windows = yaml.safe_load(windows_path.read_text(encoding="utf-8"))
    forecast_windows = windows.get(
        "forecast_windows", windows.get("event_windows", {}).get("forecast_windows")
    )
    if not isinstance(forecast_windows, Mapping):
        raise ValueError(
            f"{windows_path}: no forecast_windows mapping; the prediction-delay setting cannot be read"
        )
    offset = forecast_windows.get("prediction_time_offset_seconds")
    if offset is None:
        raise ValueError(
            f"{windows_path}: forecast_windows.prediction_time_offset_seconds is absent"
        )
    settings = _Settings(
        master_seed=int(_dig(study, "randomness.master_seed", context=str(spec_path))),
        simulation_seed=int(_dig(study, "randomness.simulation_seed", context=str(spec_path))),
        resampling_seed=int(_dig(study, "randomness.resampling_seed", context=str(spec_path))),
        config_bootstrap_samples=int(
            _dig(study, "randomness.bootstrap_samples_default", context=str(spec_path))
        ),
        bootstrap_samples=int(bootstrap_samples),
        horizon_seconds=int(_dig(study, "forecast.horizon_seconds", context=str(spec_path))),
        train_fraction=float(_dig(study, "splits.train_fraction", context=str(spec_path))),
        validation_fraction=float(
            _dig(study, "splits.validation_fraction", context=str(spec_path))
        ),
        minimum_response=float(
            _dig(
                study,
                "thresholds.smallest_relevant_response_probability_points",
                context=str(spec_path),
            )
        ),
        minimum_mae_gain=float(
            _dig(
                study,
                "thresholds.smallest_relevant_mae_gain_probability_points",
                context=str(spec_path),
            )
        ),
        prediction_delay_seconds=float(offset),
    )
    meta = {
        "study_id": _dig(study, "study.study_id", context=str(spec_path)),
        "spec_version": _dig(study, "study.spec_version", context=str(spec_path)),
        "spec_status": _dig(study, "study.spec_status", context=str(spec_path)),
        "frozen_on": _dig(study, "study.frozen_on", context=str(spec_path)),
        "frozen_basis": _dig(study, "study.frozen_basis", context=str(spec_path)),
        "human_owner_approval_claimed": bool(
            _dig(study, "study.human_owner_approval_claimed", context=str(spec_path))
        ),
        "external_registration": _dig(study, "study.external_registration", context=str(spec_path)),
        "empirical_status": _dig(study, "study.empirical_status", context=str(spec_path)),
        "empirical_status_anchor": _dig(
            study, "study.empirical_status_anchor", context=str(spec_path)
        ),
        "synthetic_experiments_are_empirical": bool(
            _dig(
                study,
                "study.synthetic_software_experiments_count_as_empirical_results",
                context=str(spec_path),
            )
        ),
        "preregistration_path": _dig(study, "study.preregistration_path", context=str(spec_path)),
    }
    paths = {
        "spec": spec_path,
        "event_windows": windows_path,
        "cohort": cohort_path,
        "preregistration": _resolve_relative(meta["preregistration_path"], root=root),
    }
    return settings, meta, paths, study


def _source_tree_digest(root: Path) -> dict[str, Any]:
    files: list[Path] = []
    for pattern in _SOURCE_PATTERNS:
        files.extend(path for path in root.glob(pattern) if path.is_file())
    files.append(root / "pyproject.toml")
    unique = sorted({path for path in files if path.is_file()}, key=lambda path: str(path))
    if not unique:
        raise FileNotFoundError(f"no source file matched {_SOURCE_PATTERNS} under {root}")
    ledger = "".join(f"{path.relative_to(root).as_posix()}\0{hash_file(path)}\n" for path in unique)
    return {
        "root": str(root),
        "file_count": len(unique),
        "sha256": _text_digest(ledger),
        "patterns": list(_SOURCE_PATTERNS),
    }


def _git_record(root: Path) -> dict[str, Any]:
    record: dict[str, Any] = {
        "repository_root": str(root),
        "head": None,
        "dirty": None,
        "note": None,
    }
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as error:
        record["note"] = f"git metadata unavailable: {type(error).__name__}: {error}"
        return record
    if head.returncode != 0:
        record["note"] = f"git rev-parse failed: {head.stderr.strip() or 'no output'}"
        return record
    record["head"] = head.stdout.strip()
    if status.returncode == 0:
        record["dirty"] = bool(status.stdout.strip())
        record["dirty_entry_count"] = len(
            [line for line in status.stdout.splitlines() if line.strip()]
        )
    else:
        record["note"] = "git status unavailable; dirty state unknown"
    return record


def _runtime_record() -> dict[str, Any]:
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "executable": Path(sys.executable).name,
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "matplotlib_backend": matplotlib.get_backend(),
    }


def _dependency_record() -> dict[str, Any]:
    versions: dict[str, Any] = {}
    for name in DEPENDENCIES:
        try:
            versions[name] = importlib_metadata.version(name)
        except importlib_metadata.PackageNotFoundError:
            versions[name] = None
    return versions


def _locked_environment_digest(versions: Mapping[str, Any]) -> str:
    """Digest the installed distribution set, which is what a lock file would pin."""
    pinned = [f"{name}=={versions[name] or 'absent'}" for name in sorted(versions)]
    return _text_digest("\n".join(pinned))


def _lock_file_record(root: Path) -> dict[str, Any]:
    candidates = ("uv.lock", "poetry.lock", "requirements.txt", "requirements.lock", "pylock.toml")
    for name in candidates:
        path = root / name
        if path.is_file():
            return {"path": str(path), "sha256": hash_file(path), "note": None}
    return {
        "path": None,
        "sha256": None,
        "note": (
            "no dependency lock file exists in the repository at this revision; the environment is "
            "pinned by the installed distribution set recorded under environment_lock_hash"
        ),
    }


def _referenced_raw_hashes(*panels: pd.DataFrame) -> list[str]:
    referenced: set[str] = set()
    for panel in panels:
        if "raw_hashes" not in panel.columns:
            continue
        for value in panel["raw_hashes"].dropna():
            referenced.update(part.strip() for part in str(value).split(",") if part.strip())
    return sorted(referenced)


def _raw_provenance(artifacts: Any, raw_store: RawStore) -> dict[str, Any]:
    """Verify every raw hash a panel row cites, straight through the store.

    ``RawStore.get`` re-hashes the stored payload, so a recorded hash that no
    longer matches its bytes fails here rather than reaching the report.
    """
    referenced = _referenced_raw_hashes(artifacts.source_panel, artifacts.usable_panel)
    verified: list[str] = []
    failures: list[dict[str, str]] = []
    total_bytes = 0
    for raw_hash in referenced:
        try:
            payload = raw_store.get(raw_hash)
        except Exception as error:
            failures.append({"raw_hash": raw_hash, "error": f"{type(error).__name__}: {error}"})
            continue
        verified.append(raw_hash)
        total_bytes += len(payload)
    fixture_error = None
    try:
        fixture_payload = raw_store.get(artifacts.fixture_hash)
        fixture_bytes = len(fixture_payload)
    except Exception as error:
        fixture_error = f"{type(error).__name__}: {error}"
        fixture_bytes = 0
    if failures:
        raise ValueError(
            f"{len(failures)} raw hash(es) cited by a panel row do not resolve to stored bytes, so "
            f"the raw-to-report provenance chain is broken: {failures[:3]}"
        )
    if fixture_error is not None:
        raise ValueError(
            f"the sample's own fixture hash does not resolve to its bytes: {fixture_error}"
        )
    archived = raw_store.stored_hashes()
    return {
        "raw_root": str(artifacts.raw_root),
        "fixture_hash": artifacts.fixture_hash,
        "fixture_sha256_matches_bytes": fixture_error is None,
        "fixture_bytes": fixture_bytes,
        "fixture_error": fixture_error,
        "panel_referenced_hashes": len(referenced),
        "panel_hashes_verified": len(verified),
        "panel_hashes_failed": failures,
        "bytes_read_back": total_bytes,
        "archived_payload_count": len(archived),
        "archived_payloads_hashed": _text_digest("\n".join(archived)),
        "note": (
            "each cited hash was re-hashed from its stored payload; the archive is the packaged "
            "synthetic replay fixture, not acquired market data"
        ),
    }


def _dataset_ref(parquet_path: Path) -> DatasetRef:
    manifest_path = parquet_path.with_name(parquet_path.name + ".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return DatasetRef(
        path=str(parquet_path),
        table=manifest["table"],
        schema_version=manifest["schema_version"],
        coverage_epoch=manifest["coverage_epoch"],
        content_hash=manifest["content_hash"],
        row_count=int(manifest["row_count"]),
    )


_TOTAL_SQL = (
    "SELECT COUNT(*) AS rows, COUNT(DISTINCT event_id) AS events, "
    "COUNT(DISTINCT contract_id) AS contracts, COUNT(DISTINCT family) AS families, "
    "COUNT(DISTINCT replay_order) AS replay_orders, "
    "SUM(CASE WHEN valid THEN 1 ELSE 0 END) AS valid_rows "
    "FROM event_panel"
)
_HORIZON_SQL = (
    "SELECT horizon_seconds, COUNT(*) AS rows, "
    "SUM(CASE WHEN valid THEN 1 ELSE 0 END) AS valid_rows, "
    "SUM(CASE WHEN valid THEN 0 ELSE 1 END) AS masked_rows "
    "FROM event_panel GROUP BY horizon_seconds ORDER BY horizon_seconds"
)
_REASON_SQL = (
    "SELECT COALESCE(exclusion_reason, '(none)') AS exclusion_reason, COUNT(*) AS rows "
    "FROM event_panel WHERE NOT valid GROUP BY 1 ORDER BY rows DESC, 1"
)


def _panel_coverage(name: str, path: Path) -> dict[str, Any]:
    reference = _dataset_ref(path)
    # One connection, three queries. Each still goes through query_sealed, which
    # re-verifies the sealed content hash before executing.
    connection = duckdb_connection()
    try:
        totals = query_sealed(reference, _TOTAL_SQL, connection=connection).to_dict(
            orient="records"
        )[0]
        horizons = query_sealed(reference, _HORIZON_SQL, connection=connection).to_dict(
            orient="records"
        )
        reasons = query_sealed(reference, _REASON_SQL, connection=connection).to_dict(
            orient="records"
        )
    finally:
        connection.close()
    frame = read_parquet(path)
    return {
        "name": name,
        "path": path.name,
        "table": reference.table,
        "content_hash": reference.content_hash,
        "coverage_epoch": reference.coverage_epoch,
        "row_count": reference.row_count,
        "replay_order": sorted(str(value) for value in frame["replay_order"].unique()),
        "totals": totals,
        "by_horizon": horizons,
        "masked_by_reason": reasons,
        "families": sorted(str(value) for value in frame["family"].unique()),
        "cohorts": sorted(str(value) for value in frame["cohort"].unique()),
    }


def _trim_comparison(record: Mapping[str, Any]) -> dict[str, Any]:
    trimmed = dict(record)
    evaluations: dict[str, Any] = {}
    omitted = 0
    for kind, payload in record.get("evaluations", {}).items():
        entry = dict(payload)
        predictions = entry.pop("predictions", None)
        if predictions is not None:
            omitted += len(predictions)
            entry["predictions_omitted"] = len(predictions)
        evaluations[kind] = entry
    trimmed["evaluations"] = evaluations
    trimmed["omitted_for_size"] = {
        "per_row_predictions": omitted,
        "reason": (
            "per-row forecasts are not embedded; the held-out event set and its folds are "
            "recorded under folds and sample, and the aggregate scores are above"
        ),
    }
    return trimmed


def _trim_model_record(record: Mapping[str, Any]) -> dict[str, Any]:
    trimmed = {
        key: value
        for key, value in record.items()
        if key not in {"train_event_ids", "train_clusters"}
    }
    trimmed["train_event_ids_count"] = len(record.get("train_event_ids") or ())
    trimmed["train_clusters_count"] = len(record.get("train_clusters") or ())
    return trimmed


def _trim_falsification(payload: Mapping[str, Any]) -> dict[str, Any]:
    trimmed: dict[str, Any] = {}
    for key, value in payload.items():
        if key in {"null", "recovery"} and isinstance(value, Mapping):
            inner = {name: item for name, item in value.items() if name != "runs"}
            inner["runs_omitted"] = len(value.get("runs") or ())
            trimmed[key] = inner
        else:
            trimmed[key] = value
    trimmed["omitted_for_size"] = {
        "null.runs": len(payload.get("null", {}).get("runs") or ()),
        "recovery.runs": len(payload.get("recovery", {}).get("runs") or ()),
        "reason": (
            "per-repetition release-level rows are not embedded; the per-seed gains, the "
            "aggregate counters and the clustered intervals are recorded instead"
        ),
    }
    return trimmed


def _comparison_frames(settings: _Settings, n_events: int) -> dict[str, Any]:
    frames: dict[str, Any] = {}
    for scenario in ("shared_news_delay", "communication"):
        frame = simulate_scenario(
            scenario,
            seed=settings.simulation_seed,
            n_events=n_events,
            horizons=[settings.horizon_seconds],
            forecast_delay=settings.prediction_delay_seconds,
        )
        frames[scenario] = {
            "forecast": frame,
            "target": primary_target(frame),
            "scenario": scenario,
            "synthetic": True,
            "ground_truth_process": str(frame.attrs.get("scenario_description", "")),
        }
    return frames


def _comparison(
    settings: _Settings, target: pd.DataFrame, assessment: Mapping[str, Any] | None
) -> dict[str, Any]:
    """The ordinary held-out comparison over the whole implemented ladder.

    Every implemented kind is fit and scored on the same held-out rows, so the
    report shows the four baselines and models side by side rather than only the
    pair the promotion gate reads. The gate itself still compares exactly the
    registered ``news`` baseline against the ``network`` candidate.
    """
    result = nested_comparison(
        target,
        seed=settings.simulation_seed,
        train_fraction=settings.train_fraction,
        validation_fraction=settings.validation_fraction,
        kinds=MODEL_KINDS,
        minimum_mae_gain=settings.minimum_mae_gain,
        null_assessment=assessment,
    )
    return _trim_comparison(result.as_record())


_ATOMS = ("0.2", "0.4", "0.6")
_THRESHOLDS = ("0.3", "0.5")
_OPERATORS = ("above", "above")


def _coherence_example(
    label: str,
    bids: Sequence[float],
    asks: Sequence[float],
    payouts: Any,
    *,
    family: str,
    assumptions: str,
) -> dict[str, Any]:
    result = coherence_distance(payouts, bids, asks)
    return {
        "label": label,
        "family": family,
        "assumptions": assumptions,
        "input_class": "constructed_illustrative",
        "bids": list(bids),
        "asks": list(asks),
        "payouts": result.get("payouts"),
        "n_contracts": result.get("n_contracts"),
        "n_atoms": result.get("n_atoms"),
        "distance": result.get("distance"),
        "feasible": result.get("feasible"),
        "midpoint_distance": result.get("midpoint_distance"),
        "midpoint_feasible": result.get("midpoint_feasible"),
        "midpoints": (result.get("box") or {}).get("midpoints"),
        "projected": result.get("projected"),
        "probabilities": result.get("probabilities"),
        "residual_max": result.get("residual_max"),
        "solver": result.get("solver"),
    }


def _coherence_section() -> dict[str, Any]:
    payouts = threshold_payoffs(
        [Decimal(value) for value in _THRESHOLDS],
        list(_OPERATORS),
        [Decimal(value) for value in _ATOMS],
    )
    # A partition family over two atomic outcomes: two contracts pay on atom 0
    # and one pays on atom 1. Coherence forces the pair paying on atom 0 to be
    # equal and the family to sum to one, so a box can be feasible while its
    # midpoints violate the identity. The genuinely infeasible case is built
    # beside it.
    partition_payouts = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 0.0]], dtype=np.float64)
    inconsistent = _coherence_example(
        "feasible_inconsistent_midpoints",
        (0.48, 0.52, 0.44),
        (0.52, 0.56, 0.48),
        partition_payouts,
        family="complement_and_partition_over_two_atomic_outcomes",
        assumptions=(
            "constructed example: two atoms, unit payout in the atom a contract pays on and zero "
            "otherwise, no fees, no discounting, synchronized quotes. The quoted box intersects the "
            "coherent price set, so the distance is zero, while the raw midpoints break the family "
            "identity by attributing different values to the two contracts that pay on the same atom"
        ),
    )
    feasible = _coherence_example(
        "feasible_box",
        (0.40, 0.20),
        (0.60, 0.40),
        payouts,
        family="nested_thresholds_over_three_atomic_outcomes",
        assumptions=(
            "constructed example: two nested threshold contracts over atoms 0.2/0.4/0.6 built by "
            "threshold_payoffs, unit payouts, synchronized quotes; the quotes admit a coherent "
            "state-price distribution inside the box"
        ),
    )
    infeasible = _coherence_example(
        "infeasible_box",
        (0.20, 0.70),
        (0.30, 0.80),
        payouts,
        family="nested_thresholds_over_three_atomic_outcomes",
        assumptions=(
            "constructed example: the same nested-threshold payout matrix as the feasible box, with "
            "a quote box disjoint from the coherent price set, so the distance is positive"
        ),
    )
    rejected: dict[str, Any]
    try:
        coherence_distance(payouts, (0.60, 0.20), (0.50, 0.80))
    except Exception as error:
        rejected = {
            "label": "crossed_book",
            "status": "rejected",
            "error": type(error).__name__,
            "message": str(error),
            "note": "a crossed book is refused rather than repaired; it is not a quoted box",
        }
    else:
        rejected = {
            "label": "crossed_book",
            "status": "unexpectedly_accepted",
            "note": "coherence_distance accepted a crossed box; the input gate needs review",
        }
    return {
        "estimand": "min-max distance from the coherent simplex image to the quoted box",
        "engine": "scipy.optimize.linprog over simplex weights",
        "contracts": [
            {
                "index": index,
                "operator": operator,
                "threshold": threshold,
                "atom_payouts": payouts[index].tolist(),
            }
            for index, (operator, threshold) in enumerate(zip(_OPERATORS, _THRESHOLDS, strict=True))
        ],
        "atoms": list(_ATOMS),
        "examples": [inconsistent, feasible, infeasible],
        "rejected": rejected,
        "note": (
            "every example here is constructed, not an observed quote: each carries its own payout "
            "matrix and stated assumptions, and the examples use different families rather than one "
            "assumption set. The constrained distance is reported beside the raw midpoint distance, "
            "because a projection enforces coherence and so cannot itself demonstrate market "
            "coherence"
        ),
    }


def _placebo_section(panel: pd.DataFrame, payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Endpoint and leave-one-release-out sensitivity, plus every placebo that is unavailable.

    Only sensitivities the actual panel can support are reported. Each missing
    placebo names the input it lacks instead of returning a zero that would read
    as a passed test.
    """
    columns = set(panel.columns)
    has_shock = "shock" in columns and bool(panel["shock"].notna().any())
    has_pre_event = "pre_event_change" in columns
    control = (
        panel["cohort"].eq("control")
        if "cohort" in columns
        else pd.Series(False, index=panel.index, dtype=bool)
    )
    cells = dict((payload or {}).get("cells") or {})
    available: dict[str, Any] = {}
    unavailable: list[dict[str, str]] = []
    if not has_shock:
        unavailable.append(
            {
                "placebo": "shock_based_placebos",
                "reason": (
                    "the packaged sample carries no release-expectation or shock column, so no "
                    "surprise slope, shifted-shock, reversed-direction or within-regime permutation "
                    "placebo is identified; the value is left unevaluated rather than set to zero"
                ),
            }
        )
    if not has_pre_event:
        unavailable.append(
            {
                "placebo": "pre_release_lead",
                "reason": "the packaged sample carries no pre-event change column",
            }
        )
    if not bool(control.any()):
        unavailable.append(
            {
                "placebo": "negative_control_contract",
                "reason": (
                    "the packaged sample assigns every row to the direct cohort and contains no "
                    "control contract"
                ),
            }
        )
    if not cells:
        unavailable.append(
            {
                "placebo": "endpoint_sensitivity",
                "reason": "the descriptive curve stage produced no cell to compare across horizons",
            }
        )
        unavailable.append(
            {
                "placebo": "leave_one_event_out",
                "reason": "the descriptive curve stage produced no cell to re-estimate",
            }
        )
    else:
        endpoints: dict[str, dict[str, Any]] = {}
        leave_one_out: dict[str, dict[str, Any]] = {}
        for cell in sorted(
            cells.values(),
            key=lambda item: (str(item.get("family")), int(item.get("horizon_seconds") or 0)),
        ):
            family = str(cell.get("family"))
            horizon = str(int(cell.get("horizon_seconds")))
            endpoints.setdefault(family, {})[horizon] = {
                "mean_response": cell.get("mean_response"),
                "status": cell.get("status"),
                "reason": cell.get("reason"),
            }
            leave_one_out.setdefault(family, {})[horizon] = {
                "max_abs_change": cell.get("leave_one_event_out_max_abs_change"),
                "values": cell.get("leave_one_event_out_mean_response"),
            }
        available["endpoint_sensitivity"] = {
            "status": "ok",
            "estimand": "descriptive mean response at each registered horizon",
            "by_family": endpoints,
            "note": (
                "a sign change across horizons is visible in the per-horizon values; with a "
                "descriptive estimand it is a shape reading, not a falsification test"
            ),
        }
        available["leave_one_event_out"] = {
            "status": "ok",
            "independent_unit": "economic_release",
            "by_family": leave_one_out,
            "note": "each value is that cell's mean with one release removed",
        }
    return {
        "available": available,
        "unavailable": unavailable,
        "shock_column_present": has_shock,
        "panel_columns": sorted(columns),
        "note": (
            "every placebo whose prerequisite is absent is reported unevaluated with the missing "
            "input named; none is filled with a zero or a substitute estimate"
        ),
    }


def _power_section(settings: _Settings, n_events: int, repetitions: int) -> dict[str, Any]:
    payload = power_assessment(
        n_events=n_events,
        relevant_slope=settings.minimum_response,
        repetitions=repetitions,
        seed=settings.resampling_seed,
        samples=settings.bootstrap_samples,
        scenario="shared_news_delay",
        calibration_n_events=n_events,
    )
    payload["synthetic_assumption_note"] = (
        "the residual scale is calibrated from the named synthetic common-news-plus-delay process, "
        "not from observed data; the rate bounds the study's resolution at this event count and is "
        "not an empirical power estimate"
    )
    payload["universal_event_count_rule"] = (
        "prohibited by the specification: the requirement is derived from this simulation at the "
        "stated residual scale"
    )
    return payload


def _save(fig: Any, path: Path, *, title: str) -> dict[str, Any]:
    try:
        fig.savefig(path, dpi=120, metadata={"Software": "market-propagation"})
    finally:
        plt.close(fig)
    return {
        "name": path.name,
        "sha256": hash_file(path),
        "bytes": path.stat().st_size,
        "title": title,
        "synthetic": True,
    }


#: Every figure title carries this prefix. Each figure here is drawn from a
#: seeded simulator or a packaged synthetic fixture, so a PNG read on its own
#: must not be mistakable for an observed market or macroeconomic result.
SYNTHETIC_TITLE_PREFIX = (
    "Synthetic: simulated process or packaged fixture, not empirical CPI or employment evidence"
)


def _synthetic_title(axis: Any, title: str) -> str:
    """Title a figure so the standalone image states its synthetic provenance.

    Returns the full title, which the figure's artifact record also carries, so
    the manifest states the same provenance the pixels do.
    """
    full = f"{SYNTHETIC_TITLE_PREFIX}\n{title}"
    axis.set_title(full, fontsize=10)
    return full


def _figure_coverage(coverage: Mapping[str, Any], path: Path) -> dict[str, Any]:
    panels = [coverage[name] for name in ("source", "usable") if name in coverage]
    horizons = sorted(
        {int(row["horizon_seconds"]) for panel in panels for row in panel["by_horizon"]}
    )
    positions = np.arange(len(horizons), dtype=np.float64)
    width = 0.38
    fig, axis = plt.subplots(figsize=(8.0, 4.4))
    for offset, panel in enumerate(panels):
        lookup = {int(row["horizon_seconds"]): row for row in panel["by_horizon"]}
        valid = [int(lookup[h]["valid_rows"]) if h in lookup else 0 for h in horizons]
        masked = [int(lookup[h]["masked_rows"]) if h in lookup else 0 for h in horizons]
        shift = positions + (offset - (len(panels) - 1) / 2.0) * width
        axis.bar(shift, valid, width, label=f"{panel['replay_order'][0]} valid")
        axis.bar(shift, masked, width, bottom=valid, label=f"{panel['replay_order'][0]} masked")
    axis.set_xticks(positions, [str(value) for value in horizons])
    axis.set_xlabel("horizon (seconds)")
    axis.set_ylabel("sealed panel rows")
    title = _synthetic_title(axis, "sealed coverage by replay fold and horizon")
    axis.legend(fontsize=8)
    fig.tight_layout()
    return _save(fig, path, title=title)


def _figure_response(response: Mapping[str, Any], path: Path) -> dict[str, Any]:
    curves = response.get("usable", {}).get("curves") or {}
    fig, axis = plt.subplots(figsize=(8.0, 4.4))
    plotted = 0
    for family, curve in curves.items():
        horizons = [int(value) for value in curve.get("horizons_seconds") or []]
        points = curve.get("mean_response") or []
        if not horizons or len(horizons) != len(points):
            continue
        plotted += 1
        axis.plot(horizons, points, marker="o", label=f"{family} mean response")
        band = curve.get("simultaneous_band") or {}
        lower, upper = band.get("simultaneous_lower"), band.get("simultaneous_upper")
        if lower and upper and all(value is not None for value in lower + upper):
            axis.fill_between(
                horizons, lower, upper, alpha=0.2, label=f"{family} simultaneous band"
            )
    if plotted:
        minimum = response.get("minimum_response")
        if isinstance(minimum, int | float):
            axis.axhline(
                float(minimum), linestyle="--", linewidth=1.0, label="smallest relevant response"
            )
        axis.axhline(0.0, color="black", linewidth=0.8)
    axis.set_xlabel("horizon (seconds)")
    axis.set_ylabel("mean response (probability units)")
    title = _synthetic_title(
        axis,
        "descriptive mean-response curve, usable fold"
        if plotted
        else "descriptive mean-response curve unavailable",
    )
    if plotted:
        axis.legend(fontsize=8)
    fig.tight_layout()
    return _save(fig, path, title=title)


def _figure_model_comparison(comparisons: Mapping[str, Any], path: Path) -> dict[str, Any]:
    processes = [name for name in ("shared_news_delay", "communication") if name in comparisons]
    kinds = sorted(
        {kind for name in processes for kind in (comparisons[name].get("evaluations") or {})}
    )
    positions = np.arange(len(kinds), dtype=np.float64)
    width = 0.38
    fig, axis = plt.subplots(figsize=(8.0, 4.4))
    for offset, name in enumerate(processes):
        evaluations = comparisons[name].get("evaluations") or {}
        values = [
            float(evaluations[kind].get("mae"))
            if kind in evaluations and evaluations[kind].get("mae") is not None
            else 0.0
            for kind in kinds
        ]
        axis.bar(
            positions + (offset - (len(processes) - 1) / 2.0) * width, values, width, label=name
        )
    axis.set_xticks(positions, kinds)
    axis.set_ylabel("held-out MAE (probability units)")
    title = _synthetic_title(axis, "nested ladder on the identical held-out sample")
    axis.legend(fontsize=8)
    fig.tight_layout()
    return _save(fig, path, title=title)


def _figure_null_vs_communication(falsification: Mapping[str, Any], path: Path) -> dict[str, Any]:
    per_seed = (falsification.get("paired_contrast") or {}).get("per_seed") or []
    fig, axis = plt.subplots(figsize=(8.0, 4.4))
    if per_seed:
        index = np.arange(len(per_seed), dtype=np.float64)
        axis.plot(
            index, [row.get("null_gain") for row in per_seed], marker="o", label="null process gain"
        )
        axis.plot(
            index,
            [row.get("recovery_gain") for row in per_seed],
            marker="s",
            label="communication process gain",
        )
        threshold = falsification.get("minimum_mae_gain")
        if isinstance(threshold, int | float):
            axis.axhline(float(threshold), linestyle="--", linewidth=1.0, label="minimum MAE gain")
        axis.axhline(0.0, color="black", linewidth=0.8)
        axis.set_xlabel("repetition (shared seed)")
        axis.set_ylabel("news-minus-network MAE gain")
    else:
        axis.text(0.5, 0.5, "null audit unavailable", ha="center", va="center")
        axis.set_axis_off()
    title = _synthetic_title(axis, "null versus communication: gate recovery per repetition")
    if per_seed:
        axis.legend(fontsize=8)
    fig.tight_layout()
    return _save(fig, path, title=title)


def _figure_coherence(coherence: Mapping[str, Any], path: Path) -> dict[str, Any]:
    examples = coherence.get("examples") or []
    labels = [str(example.get("label")) for example in examples]
    positions = np.arange(len(examples), dtype=np.float64)
    fig, axis = plt.subplots(figsize=(9.6, 4.6))
    if examples:
        # Both bars are distances from a quoted box to the coherent price set:
        # the constrained one over the simplex, and the raw one using the box
        # midpoints. Neither is a projected price, so neither is labelled as one.
        axis.bar(
            positions - 0.18,
            [example.get("distance") or 0.0 for example in examples],
            0.36,
            label="constrained min-max distance (quoted box to coherent set)",
        )
        axis.bar(
            positions + 0.18,
            [example.get("midpoint_distance") or 0.0 for example in examples],
            0.36,
            label="raw midpoint-box distance (midpoints held fixed)",
        )
        axis.set_xticks(positions, labels, fontsize=8)
        axis.set_ylabel("min-max distance (probability units)")
        axis.legend(fontsize=8)
    else:
        axis.text(0.5, 0.5, "coherence examples unavailable", ha="center", va="center")
        axis.set_axis_off()
    title = _synthetic_title(
        axis, "coherence distance: constructed feasible, inconsistent and infeasible boxes"
    )
    fig.tight_layout()
    return _save(fig, path, title=title)


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return "null" if not math.isfinite(value) else f"{value:.{digits}f}"
    return str(value)


def _md_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join("" if cell is None else str(cell) for cell in row) + " |")
    return "\n".join(lines)


def _gate_table(gates: Mapping[str, Any]) -> str:
    return _md_table(
        ["gate", "name", "status", "evidence class", "detail"],
        [
            [
                key,
                value.get("name"),
                value.get("status"),
                value.get("evidence_class"),
                value.get("detail"),
            ]
            for key, value in sorted(gates.items())
        ],
    )


def _coverage_table(coverage: Mapping[str, Any]) -> str:
    rows = []
    for name in ("source", "usable"):
        panel = coverage.get(name) or {}
        totals = panel.get("totals") or {}
        rows.append(
            [
                name,
                panel.get("replay_order"),
                totals.get("rows"),
                totals.get("valid_rows"),
                totals.get("events"),
                totals.get("contracts"),
                (panel.get("content_hash") or "")[:12],
            ]
        )
    return _md_table(
        ["fold", "replay order", "rows", "valid", "releases", "contracts", "content hash"], rows
    )


def _curve_table(response: Mapping[str, Any]) -> str:
    rows = []
    for fold in ("usable", "source"):
        section = response.get(fold) or {}
        for family, curve in (section.get("curves") or {}).items():
            band = curve.get("simultaneous_band") or {}
            rows.append(
                [
                    fold,
                    family,
                    ", ".join(str(value) for value in curve.get("horizons_seconds") or []),
                    ", ".join(_fmt(value, 5) for value in curve.get("mean_response") or []),
                    band.get("status"),
                    band.get("reason") or "",
                ]
            )
    return _md_table(["fold", "family", "horizons (s)", "mean response", "band", "band note"], rows)


def _baseline_report(payload: Mapping[str, Any]) -> str:
    sample = payload.get("sample") or {}
    coverage = payload.get("coverage") or {}
    response = payload.get("response_curve") or {}
    lines = [
        "# Baseline report",
        "",
        "Descriptive baseline for the offline reproduction. Every number below was computed in this "
        "run from the packaged synthetic sample, and nothing here is an empirical estimate about a "
        "real venue.",
        "",
        "## What this run measured",
        "",
        f"- Sample fixture: `{sample.get('fixture_hash')}` with {sample.get('release_count')} releases, "
        f"{sample.get('quote_count')} quotes and {sample.get('raw_hash_count')} archived payloads.",
        f"- Sealed panels: {sample.get('source_rows')} source rows and {sample.get('usable_rows')} "
        "usable rows.",
        f"- Run status: {payload.get('status')}; complete: {_fmt(payload.get('complete'))}.",
        "",
        "## Sealed coverage",
        "",
        _coverage_table(coverage),
        "",
        "Masked rows carry an exclusion reason and a null measurement:",
        "",
        _md_table(
            ["fold", "exclusion reason", "rows"],
            [
                [name, row.get("exclusion_reason"), row.get("rows")]
                for name in ("source", "usable")
                for row in ((coverage.get(name) or {}).get("masked_by_reason") or [])
            ],
        ),
        "",
        "## Descriptive response curves",
        "",
        "The estimator is `local_projections` with no shock column, so the estimand is the "
        "descriptive mean response in probability units with release-clustered uncertainty.",
        "",
        _curve_table(response),
        "",
        "## Reading the band",
        "",
        f"- Usable-fold band status: {_fmt((response.get('usable') or {}).get('curve_status'))}.",
        f"- releases used: {_fmt((response.get('usable') or {}).get('n_events_used'))}; "
        f"clusters used: {_fmt((response.get('usable') or {}).get('n_clusters_used'))}.",
        "- A band that is degenerate or unavailable at this release count is reported as such. It is "
        "not widened into an interval the release cluster structure cannot support.",
        "",
        "## Blocked",
        "",
        "\n".join(f"- {entry}" for entry in (payload.get("blocked_stages") or ["none"]))
        if payload.get("blocked_stages")
        else "- No stage blocked.",
        "",
    ]
    return "\n".join(lines)


def _conditional_report(payload: Mapping[str, Any]) -> str:
    comparisons = payload.get("model_comparison") or {}
    falsification = payload.get("null_vs_communication") or {}
    coherence = payload.get("coherence") or {}
    power = payload.get("power") or {}
    rows = []
    for name in ("shared_news_delay", "communication"):
        record = comparisons.get(name) or {}
        promotion = record.get("promotion") or {}
        rows.append(
            [
                name,
                promotion.get("status"),
                _fmt(promotion.get("baseline_mae"), 6),
                _fmt(promotion.get("candidate_mae"), 6),
                _fmt(promotion.get("mae_reduction"), 6),
                _fmt(promotion.get("minimum_mae_gain"), 6),
            ]
        )
    null_section = falsification.get("null") or {}
    recovery_section = falsification.get("recovery") or {}
    ladder_rows = [
        [process, kind, _fmt((comparisons[process]["evaluations"].get(kind) or {}).get("mae"), 6)]
        for process in ("shared_news_delay", "communication")
        for kind in (comparisons.get("kinds") or [])
        if kind in (comparisons[process].get("evaluations") or {})
    ]
    lines = [
        "# Conditional propagation report",
        "",
        "Held-out comparison of the nested ladder on the production pipeline, "
        "run on simulated processes with known mechanisms. A predictive edge over the shared-news "
        "baseline is not evidence of propagation, and no result here is an empirical claim.",
        "",
        "## Nested comparison",
        "",
        _md_table(["process", "gate", "news MAE", "network MAE", "gain", "threshold"], rows),
        "",
        f"Estimator: `{falsification.get('estimator')}`; metric `{falsification.get('metric')}`; "
        f"horizon {falsification.get('horizon_seconds')}s; prediction delay "
        f"{falsification.get('prediction_delay_seconds')}s; threshold "
        f"{_fmt(falsification.get('minimum_mae_gain'), 6)}.",
        "",
        "Every implemented kind is fit and scored on that one held-out sample. The promotion gate "
        "above reads the news-versus-network contrast only; the remaining kinds are reported so the "
        "ladder is visible in full.",
        "",
        _md_table(["process", "kind", "held-out MAE"], ladder_rows),
        "",
        "## Null versus communication audit",
        "",
        f"- Status: {falsification.get('status')}.",
        f"- Null false positives: {null_section.get('false_positive_count')} of "
        f"{falsification.get('repetitions')} repetitions "
        f"(rate {_fmt(null_section.get('false_positive_rate'), 4)}, one-sided upper bound "
        f"{_fmt(null_section.get('one_sided_upper_bound'), 5)}).",
        f"- Recovery: {_fmt(recovery_section.get('power'), 4)} "
        f"(one-sided lower bound {_fmt(recovery_section.get('one_sided_lower_bound'), 5)}, "
        f"target {_fmt(recovery_section.get('target_power'), 2)}).",
        f"- Power source: {falsification.get('power_source')}",
        "",
        "Inconclusive reasons:",
        "",
        "\n".join(
            f"- {reason}" for reason in (falsification.get("inconclusive_reasons") or ["none"])
        ),
        "",
        "## Coherence",
        "",
        _md_table(
            ["example", "family", "distance", "feasible", "midpoint distance", "midpoint feasible"],
            [
                [
                    example.get("label"),
                    example.get("family"),
                    _fmt(example.get("distance"), 6),
                    _fmt(example.get("feasible")),
                    _fmt(example.get("midpoint_distance"), 6),
                    _fmt(example.get("midpoint_feasible")),
                ]
                for example in (coherence.get("examples") or [])
            ],
        ),
        "",
        "Assumptions and payout matrix per constructed example:",
        "",
        "\n".join(
            f"- `{example.get('label')}`: {example.get('assumptions')} "
            f"payouts={json.dumps(example.get('payouts'))}"
            for example in (coherence.get("examples") or [])
        )
        or "- none",
        "",
        f"Crossed box handling: {(coherence.get('rejected') or {}).get('status')}.",
        "",
        "## Response-slope power",
        "",
        f"- Power at the smallest relevant response "
        f"{_fmt(power.get('relevant_slope'), 4)}: {_fmt(power.get('power'), 4)}.",
        f"- False-positive rate: {_fmt(power.get('false_positive_rate'), 4)}.",
        f"- Residual scale source: {power.get('residual_sigma_source')}.",
        "",
        "## Placebos",
        "",
        "\n".join(
            f"- {entry.get('placebo')}: {entry.get('reason')}"
            for entry in ((payload.get("placebos") or {}).get("unavailable") or [])
        )
        or "- none unavailable",
        "",
        "## Gates",
        "",
        _gate_table(payload.get("gates") or {}),
        "",
    ]
    return "\n".join(lines)


def _real_inputs_block(payload: Mapping[str, Any]) -> list[str]:
    """State plainly which real inputs were named, verified, and what they do not establish.

    Shared by the data card and the paper so the two cannot drift on the one thing
    that matters most here: whether a real input was cited at all, and whether
    anything empirical follows from it.
    """
    external = payload.get("external_evidence") or {}
    audit = external.get("audit") or {}
    releases = external.get("releases") or {}
    lines: list[str] = []
    if not audit and not releases:
        return [
            "No real input was named for this run. It cites no acquisition audit and no archived "
            "release dataset, so it is a synthetic software and methods reproduction only. Nothing "
            "was selected on the caller's behalf.",
        ]
    if audit:
        lines.extend(
            [
                "The caller named this real acquisition audit explicitly. It is cited at the status "
                "it recorded and is neither re-run nor upgraded here.",
                "",
                _md_table(
                    ["input", "path", "sha256", "recorded status", "complete"],
                    [
                        [
                            "real acquisition audit",
                            audit.get("path"),
                            audit.get("sha256"),
                            audit.get("status"),
                            _fmt(audit.get("complete")),
                        ]
                    ],
                ),
                "",
                "Its own recorded fields, under the names it used:",
                "",
                _md_table(
                    ["field", "value"],
                    [
                        [name, json.dumps(value) if isinstance(value, list | dict) else value]
                        for name, value in sorted((audit.get("recorded_fields") or {}).items())
                    ],
                ),
                "",
            ]
        )
    if releases:
        status = (
            "verified against its own original bytes"
            if releases.get("verified")
            else (f"NOT verified ({_fmt(releases.get('reason'))})")
        )
        lines.extend(
            [
                "The caller named this normalized original-release dataset explicitly. It is "
                f"{status} and is never fitted: it does not enter the synthetic sample or any model.",
                "",
                _md_table(
                    ["input", "path", "sha256", "records verified", "records blocked"],
                    [
                        [
                            "archived release dataset",
                            releases.get("path"),
                            (releases.get("dataset") or {}).get("content_hash"),
                            releases.get("records_verified"),
                            releases.get("records_blocked"),
                        ]
                    ],
                ),
                "",
            ]
        )
        if releases.get("records"):
            lines.extend(
                [
                    _md_table(
                        [
                            "event",
                            "reference period",
                            "scheduled at",
                            "values",
                            "revisions",
                            "raw hash",
                        ],
                        [
                            [
                                record.get("event_id"),
                                record.get("reference_period"),
                                record.get("scheduled_at"),
                                record.get("values_key_count"),
                                record.get("revisions_key_count"),
                                record.get("archived_raw_hash"),
                            ]
                            for record in releases["records"]
                        ],
                    ),
                    "",
                ]
            )
        if releases.get("blocked_records"):
            lines.extend(
                [
                    "Records that did not verify, with their own reasons:",
                    "",
                    _md_table(
                        ["reason", "detail"],
                        [
                            [record.get("reason"), json.dumps(record.get("detail"))]
                            for record in releases["blocked_records"]
                        ],
                    ),
                    "",
                ]
            )
        lines.extend(
            [
                f"- Usable time: {_fmt(releases.get('usable_time'))}. {releases.get('usable_time_note')}",
                f"- {releases.get('interpretation')}",
                f"- {releases.get('limits_note')}",
                "",
            ]
        )
    lines.extend(
        [
            "Citing a real input does not make any result here empirical. The fitted panels remain "
            "the packaged synthetic fixture and the simulated processes, and the empirical status "
            f"below stays `{(payload.get('empirical') or {}).get('status')}`.",
        ]
    )
    return lines


def _data_card(payload: Mapping[str, Any]) -> str:
    manifest = payload.get("manifest") or {}
    hashes = manifest.get("hashes") or {}
    git = manifest.get("git") or {}
    dependencies = manifest.get("dependencies") or {}
    lines = [
        "# Data card",
        "",
        f"Classification: `{payload.get('classification')}`. This run consumes no acquired market "
        "data. Every input is either a packaged synthetic fixture or a seeded simulation.",
        "",
        "## Inputs",
        "",
        _md_table(
            ["input", "path", "sha256", "role"],
            [
                [
                    "study specification",
                    manifest.get("inputs", {}).get("spec_path"),
                    hashes.get("spec"),
                    "settings",
                ],
                [
                    "event windows",
                    manifest.get("inputs", {}).get("event_windows_path"),
                    hashes.get("event_windows"),
                    "prediction delay",
                ],
                [
                    "cohort specification",
                    manifest.get("inputs", {}).get("cohort_path"),
                    hashes.get("universe"),
                    "prespecified universe",
                ],
                [
                    "packaged replay fixture",
                    "src/market_propagation/fixtures/replay.json",
                    hashes.get("source"),
                    "synthetic raw/replay example",
                ],
            ],
        ),
        "",
        "## Real inputs cited",
        "",
        *_real_inputs_block(payload),
        "",
        "## Provenance",
        "",
        f"- Git head: `{git.get('head')}`; working tree dirty: {_fmt(git.get('dirty'))}.",
        f"- Dirty source-tree digest: `{git.get('source_tree_sha256')}`.",
        f"- Environment digest: `{hashes.get('environment')}`.",
        f"- Dependency lock file: `{_fmt((manifest.get('lock') or {}).get('path'))}`.",
        "",
        _md_table(
            ["distribution", "version"],
            [[name, version] for name, version in sorted(dependencies.items())],
        ),
        "",
        "## Raw provenance chain",
        "",
        f"- Panel-referenced raw hashes verified against stored payloads: "
        f"{((payload.get('sample') or {}).get('raw_provenance') or {}).get('panel_hashes_verified')} of "
        f"{((payload.get('sample') or {}).get('raw_provenance') or {}).get('panel_referenced_hashes')}.",
        f"- Fixture hash re-read from the archive: "
        f"{_fmt(((payload.get('sample') or {}).get('raw_provenance') or {}).get('fixture_sha256_matches_bytes'))}.",
        "",
        "## Synthetic content and limits",
        "",
        "- The packaged sample is an illustrative synthetic replay fixture opened in 2031. It is not "
        "`configs/cohort.yaml` and not the ten scientific releases.",
        "- Forecast panels are generated by the seeded simulator; they are software processes, not "
        "observations.",
        "- No release expectation is packaged, so no surprise slope and no shock placebo is identified.",
        "- No outcome is packaged, so `label_available_time` and `training_cutoff` are null on every "
        "panel row rather than filled with a settlement time the study never observed.",
        "",
        "## Locked cohorts",
        "",
        "- The registry holds no locked-test reservation and no event claim from this run. A "
        "synthetic reproduction does not consume a real locked cohort.",
        "",
    ]
    return "\n".join(lines)


def _paper(payload: Mapping[str, Any]) -> str:
    gates = payload.get("gates") or {}
    external = payload.get("external_evidence") or {}
    rows = []
    for name in ("shared_news_delay", "communication"):
        promotion = ((payload.get("model_comparison") or {}).get(name) or {}).get("promotion") or {}
        rows.append(
            [
                name,
                promotion.get("status"),
                _fmt(promotion.get("baseline_mae"), 6),
                _fmt(promotion.get("candidate_mae"), 6),
                _fmt(promotion.get("mae_reduction"), 6),
            ]
        )
    links = "\n".join(f"- `{entry}`" for entry in external.get("paths") or []) or "- none present"
    lines = [
        "# Reproduction paper: synthetic software and methods result",
        "",
        "## Classification",
        "",
        "This paper reports a **synthetic software and methods reproduction**. Every estimate in it "
        "is produced by a seeded generator or by a fixture packaged with the code. No result here is "
        "empirical evidence about a real venue, a real release, or a real contract, and no synthetic "
        "effect estimate is treated as an empirical finding.",
        "",
        f"The local specification was frozen as "
        f"`{(payload.get('manifest') or {}).get('study', {}).get('spec_status')}` on "
        f"`{(payload.get('manifest') or {}).get('study', {}).get('frozen_on')}`. Human owner approval "
        "is not claimed and no external registration exists. The specification's freeze-time "
        "statement that no model was fit on market data and no power calculation had been run stays "
        "anchored to that date; this later synthetic verification does not revise it.",
        "",
        "## What was run",
        "",
        "- The packaged synthetic replay sample was rebuilt into both replay folds, sealed, and its "
        "coverage counted from the sealed Parquet through the storage query layer.",
        "- Descriptive mean-response curves were fit on that sample with release-clustered "
        "uncertainty. No shock is packaged, so no slope was estimated.",
        "- The production nested news-versus-network comparison was fit on the null and the "
        "communication processes at the registered horizon and threshold.",
        "- The production null/recovery audit was run, and nothing about its outcome was inferred "
        "from scenario metadata.",
        "- Between-release response-slope power was computed at a residual scale calibrated from the "
        "named synthetic process.",
        "",
        "## Held-out comparison on simulated processes",
        "",
        _md_table(["process", "gate", "news MAE", "network MAE", "gain"], rows),
        "",
        "Both processes are synthetic. A gain below the registered threshold keeps the network model "
        "at the baseline level; it does not measure a real transmission effect.",
        "",
        "## Gates",
        "",
        _gate_table(gates),
        "",
        f"Empirical status: {(payload.get('empirical') or {}).get('status')} "
        f"({(payload.get('empirical') or {}).get('reason')}).",
        "",
        "## Real inputs cited",
        "",
        "A real acquisition audit or archived release dataset is cited only when the caller names one "
        "explicitly. Nothing is selected from the checkout on the caller's behalf, and a real input is "
        "never substituted for the synthetic reproduction:",
        "",
        links,
        "",
        *_real_inputs_block(payload),
        "",
        external.get("note", ""),
        "",
        "## Limits",
        "",
        "- Release-count resolution: the packaged sample holds ten releases, which bounds the "
        "precision of every curve reported here.",
        "- Synthetic mechanisms are declared, matched across the null and recovery processes by "
        "shared seed, and never read from ground-truth columns as predictors.",
        "- Unavailable real cohorts stay blocked rather than approximated.",
        "",
    ]
    return "\n".join(lines)


def reproduce(
    output_dir: str | Path,
    *,
    spec_path: str | Path = "configs/study_v1.yaml",
    n_events: int = 120,
    repetitions: int = 40,
    bootstrap_samples: int = 200,
    real_audit_dir: str | Path | None = None,
    release_dataset: str | Path | None = None,
) -> dict:
    """Build every offline artifact for one reproduction and report honest gate statuses.

    ``output_dir`` receives the metrics, the manifest, both sealed sample panels,
    the sealed forecast panel, five figures, four reports and the registry export.
    ``spec_path`` is the local study specification whose seeds, splits, horizon and
    thresholds are passed through to the estimators. ``n_events`` and
    ``repetitions`` are caller settings, recorded as such; ``bootstrap_samples``
    overrides ``randomness.bootstrap_samples_default`` and both values are
    recorded.

    ``real_audit_dir`` and ``release_dataset`` name real evidence this run cites.
    Neither is discovered for you: with neither one supplied, this reproduces the
    synthetic fixture alone and cites no real input, exactly as it did before these
    arguments existed. An explicitly named input must exist and be verified, and it
    is then listed with its own paths, hashes and counts in the manifest, the data
    card and the paper. Citing one never fits it: the fitted panels stay the
    packaged synthetic fixture and the simulated processes, and a real input that
    establishes no eligible cohort leaves the empirical gates blocked. A real
    input that is named but does not verify is reported as unverified and counts
    for nothing.

    The returned dict carries ``complete``: true only when every required stage
    produced its real artifact, and ``blocked_stages`` naming what did not. A
    blocked stage is reported, never substituted.
    """
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    spec = Path(spec_path)
    if not spec.exists():
        raise FileNotFoundError(f"study specification not found: {spec}")
    if int(n_events) < MIN_N_EVENTS:
        raise ValueError(f"n_events={n_events!r} must be at least {MIN_N_EVENTS}")
    if int(repetitions) < MIN_REPETITIONS:
        raise ValueError(
            f"repetitions={repetitions!r} must be at least {MIN_REPETITIONS}; a stable rate needs more"
        )
    if int(bootstrap_samples) < MIN_BOOTSTRAP_SAMPLES:
        raise ValueError(
            f"bootstrap_samples={bootstrap_samples!r} must be at least {MIN_BOOTSTRAP_SAMPLES}"
        )

    run = _Run()
    root = Path(__file__).resolve().parent.parent.parent
    figures_dir = output / FIGURES_DIRECTORY

    settings, study_meta, paths, _study = _load_settings(spec, bootstrap_samples=bootstrap_samples)
    if settings.bootstrap_samples != settings.config_bootstrap_samples:
        run.warnings.append(
            f"bootstrap_samples={settings.bootstrap_samples} differs from "
            f"randomness.bootstrap_samples_default={settings.config_bootstrap_samples}; "
            "the caller value was used and both are recorded"
        )
    if int(repetitions) < 30:
        run.warnings.append(
            f"repetitions={int(repetitions)} is below the registered default of 40; the rate "
            "bounds are correspondingly wider"
        )

    git = _git_record(root)
    dependencies = _dependency_record()
    runtime = _runtime_record()
    lock = _lock_file_record(root)
    try:
        source_tree = _source_tree_digest(root)
        git["source_tree_sha256"] = source_tree["sha256"]
        git["source_tree_file_count"] = source_tree["file_count"]
    except Exception as error:
        source_tree = None
        git["source_tree_sha256"] = None
        git["source_tree_digest_error"] = f"{type(error).__name__}: {error}"

    spec_hash = hash_file(paths["spec"])
    event_windows_hash = hash_file(paths["event_windows"])
    universe_hash = hash_file(paths["cohort"]) if paths["cohort"].is_file() else None
    spec_digest = _digest(
        {
            "spec_sha256": spec_hash,
            "event_windows_sha256": event_windows_hash,
            "cohort_sha256": universe_hash,
            "settings": settings.as_record(),
        }
    )
    environment_hash = _digest(
        {
            "runtime": runtime,
            "dependencies": dependencies,
            "environment_lock": _locked_environment_digest(dependencies),
            "git_head": git.get("head"),
            "source_tree": (source_tree or {}).get("sha256"),
        }
    )
    if paths["cohort"].is_file():
        pass
    else:
        run.warnings.append(
            f"cohort specification {paths['cohort']} is absent; the universe hash is not recorded"
        )

    artifacts = run.stage("sample", lambda: build_sample(output, max_age_seconds=MAX_AGE_SECONDS))
    sample_section: dict[str, Any] | None = None
    raw_section: dict[str, Any] | None = None
    if artifacts is not None:
        raw_store = RawStore(artifacts.raw_root)
        raw_section = run.stage("raw_provenance", lambda: _raw_provenance(artifacts, raw_store))
        sample_section = {
            "fixture_hash": artifacts.fixture_hash,
            "fixture_classification": "packaged_synthetic_illustrative_fixture",
            "fixture_is_scientific_cohort": False,
            "quote_count": artifacts.quote_count,
            "release_count": artifacts.release_count,
            "raw_hash_count": len(artifacts.raw_hashes),
            "raw_root": str(artifacts.raw_root),
            "source_rows": len(artifacts.source_panel),
            "usable_rows": len(artifacts.usable_panel),
            "source_events": int(artifacts.source_panel["event_id"].nunique()),
            "usable_events": int(artifacts.usable_panel["event_id"].nunique()),
            "event_ids": sorted(
                str(value) for value in artifacts.source_panel["event_id"].unique()
            ),
            "families": sorted(str(value) for value in artifacts.source_panel["family"].unique()),
            "horizons_seconds": sorted(
                int(value) for value in artifacts.source_panel["horizon_seconds"].unique()
            ),
            "replay_disagreements": artifacts.disagreements,
            "raw_provenance": raw_section,
            "note": (
                "the packaged fixture is illustrative synthetic content opened in 2031; it is not "
                "configs/cohort.yaml and not the scientific release cohort"
            ),
        }

    coverage_section: dict[str, Any] | None = None
    if artifacts is not None:
        coverage_section = run.stage(
            "coverage",
            lambda: {
                "source": _panel_coverage("source", output / SOURCE_PANEL_NAME),
                "usable": _panel_coverage("usable", output / USABLE_PANEL_NAME),
                "engine": "duckdb over the sealed Parquet, hash-verified before each query",
            },
        )

    frames = run.stage("forecast_frames", lambda: _comparison_frames(settings, int(n_events)))
    falsification_summary: dict[str, Any] | None = None
    falsification_payload: dict[str, Any] | None = None
    if frames is not None:
        falsification_payload = run.stage(
            "null_vs_communication",
            lambda: network_falsification(
                n_events=int(n_events),
                repetitions=int(repetitions),
                seed=settings.master_seed,
                minimum_mae_gain=settings.minimum_mae_gain,
                horizon_seconds=float(settings.horizon_seconds),
                prediction_delay_seconds=settings.prediction_delay_seconds,
            ),
        )
    if falsification_payload is not None:
        falsification_summary = _trim_falsification(falsification_payload)

    forecast_section: dict[str, Any] | None = None
    if frames is not None:
        forecast_section = run.stage(
            "forecast_panel", _forecast_panel_writer(frames, settings, output, spec_digest)
        )

    model_section: dict[str, Any] | None = None
    if frames is not None:

        def _model_comparison_stage() -> dict[str, Any]:
            return {
                "shared_news_delay": _comparison(
                    settings, frames["shared_news_delay"]["target"], None
                ),
                "communication": _comparison(
                    settings, frames["communication"]["target"], falsification_payload
                ),
                "kinds": list(MODEL_KINDS),
                "seed": settings.simulation_seed,
                "train_fraction": settings.train_fraction,
                "validation_fraction": settings.validation_fraction,
                "minimum_mae_gain": settings.minimum_mae_gain,
                "horizon_seconds": settings.horizon_seconds,
                "prediction_delay_seconds": settings.prediction_delay_seconds,
                "null_assessment_attached_to": (
                    "communication" if falsification_payload is not None else None
                ),
            }

        model_section = run.stage("model_comparison", _model_comparison_stage)

    response_section: dict[str, Any] | None = None
    if artifacts is not None:
        response_section = run.stage("response_curve", _response_stage(artifacts, settings))

    coherence_section = run.stage("coherence", _coherence_section)
    power_section = run.stage(
        "power", lambda: _power_section(settings, int(n_events), int(repetitions))
    )

    figure_records: list[dict[str, Any]] = []
    figures_dir.mkdir(parents=True, exist_ok=True)
    if coverage_section is not None:
        record = run.stage(
            "figure_coverage",
            lambda: _figure_coverage(coverage_section, figures_dir / FIGURE_NAMES[0]),
        )
        if record:
            figure_records.append(record)
    if response_section is not None:
        record = run.stage(
            "figure_response_uncertainty",
            lambda: _figure_response(response_section, figures_dir / FIGURE_NAMES[1]),
        )
        if record:
            figure_records.append(record)
    if model_section is not None:
        record = run.stage(
            "figure_model_comparison",
            lambda: _figure_model_comparison(model_section, figures_dir / FIGURE_NAMES[2]),
        )
        if record:
            figure_records.append(record)
    if falsification_summary is not None:
        record = run.stage(
            "figure_null_vs_communication",
            lambda: _figure_null_vs_communication(
                falsification_summary, figures_dir / FIGURE_NAMES[3]
            ),
        )
        if record:
            figure_records.append(record)
    if coherence_section is not None:
        record = run.stage(
            "figure_coherence",
            lambda: _figure_coherence(coherence_section, figures_dir / FIGURE_NAMES[4]),
        )
        if record:
            figure_records.append(record)

    external_evidence = _external_evidence(
        root, real_audit_dir=real_audit_dir, release_dataset=release_dataset
    )
    gates = _gates(
        sample_section=sample_section,
        coverage_section=coverage_section,
        response_section=response_section,
        model_section=model_section,
        falsification=falsification_summary,
        coherence_section=coherence_section,
        power_section=power_section,
        external_evidence=external_evidence,
    )
    empirical = {
        "status": "blocked",
        "evidence_class": "none",
        "reason": (
            "no real eligible cohort, panel or outcome is consumed by this reproduction; the sample "
            "is a packaged synthetic fixture and the forecast processes are simulated"
            + (
                "; a real acquisition audit was named and is referenced, but it establishes no "
                "eligible cohort here"
                if external_evidence.get("audit")
                else ""
            )
            + (
                "; normalized original BLS release values were named and verified, but a published "
                "release value certifies neither the market rule version in force at the release nor "
                "the quote coverage of any contract, so it establishes no eligible cohort either"
                if external_evidence.get("releases")
                else ""
            )
        ),
        "synthetic_results_are_empirical_results": False,
        "referenced_real_audit": (external_evidence.get("audit") or {}).get("path"),
        "referenced_release_dataset": (external_evidence.get("releases") or {})
        .get("dataset", {})
        .get("path")
        if (external_evidence.get("releases") or {}).get("verified")
        else None,
        "release_values_are_not_market_evidence": True,
    }

    sample_event_ids = (sample_section or {}).get("event_ids") or []
    simulated_event_ids = sorted(
        {
            str(value)
            for name in ("shared_news_delay", "communication")
            for value in ((frames or {}).get(name) or {})
            .get("target", pd.DataFrame())["event_id"]
            .unique()
        }
        if frames
        else []
    )
    seed_record = {
        "master_seed": settings.master_seed,
        "simulation_seed": settings.simulation_seed,
        "resampling_seed": settings.resampling_seed,
        "bootstrap_samples": settings.bootstrap_samples,
        "network_falsification_seeds": (falsification_summary or {}).get("seeds") or [],
        "power_assessment_seed": (power_section or {}).get("seed"),
    }
    registry_section = run.stage(
        "registry",
        lambda: _registry_stage(
            output_dir=output,
            run_id=_digest(
                {
                    "spec_digest": spec_digest,
                    "environment_hash": environment_hash,
                    "sample_fixture_hash": (sample_section or {}).get("fixture_hash"),
                    "n_events": int(n_events),
                    "repetitions": int(repetitions),
                    "bootstrap_samples": settings.bootstrap_samples,
                    "classification": "synthetic_software_methods_reproduction",
                }
            ),
            spec_digest=spec_digest,
            environment_hash=environment_hash,
            sync_hashes={
                "source": (raw_section or {}).get("archived_payloads_hashed")
                or (sample_section or {}).get("fixture_hash")
                or _digest({"sample_stage": "unavailable"}),
                "data": _digest(
                    {
                        name: (coverage_section or {}).get(name, {}).get("content_hash")
                        for name in ("source", "usable")
                    }
                    | {"forecast_panel": (forecast_section or {}).get("content_hash")}
                ),
            },
            event_ids=[*sample_event_ids, *simulated_event_ids],
            seed=settings.master_seed,
            metrics={
                "n_events_requested": int(n_events),
                "n_repetitions": int(repetitions),
                "bootstrap_samples": settings.bootstrap_samples,
                "sample_source_rows": (sample_section or {}).get("source_rows"),
                "sample_usable_rows": (sample_section or {}).get("usable_rows"),
                "sample_valid_rows": _valid_total(coverage_section),
                "network_null_false_positive_rate": (falsification_summary or {})
                .get("null", {})
                .get("false_positive_rate"),
                "network_recovery_power": (falsification_summary or {})
                .get("recovery", {})
                .get("power"),
                "null_process_mae_reduction": _mae_reduction(model_section, "shared_news_delay"),
                "communication_process_mae_reduction": _mae_reduction(
                    model_section, "communication"
                ),
                "response_slope_power": (power_section or {}).get("power"),
                "response_slope_false_positive_rate": (power_section or {}).get(
                    "false_positive_rate"
                ),
                "coherence_feasible_distance": _coherence_distance_for(
                    coherence_section, "feasible_box"
                ),
                "coherence_infeasible_distance": _coherence_distance_for(
                    coherence_section, "infeasible_box"
                ),
                "synthetic": True,
            },
            classification="synthetic_software_methods_reproduction",
        ),
    )
    registry_record = _registry_record(
        registration=registry_section,
        seed_record=seed_record,
        spec_digest=spec_digest,
        environment_hash=environment_hash,
    )

    artifacts_written = [*figure_records]
    manifest = {
        "classification": "synthetic_software_methods_reproduction",
        "synthetic": True,
        "generated_by": "market_propagation.reporting.reproduce",
        "study": study_meta,
        "inputs": {
            "spec_path": str(spec),
            "event_windows_path": str(paths["event_windows"]),
            "cohort_path": str(paths["cohort"]),
            "cohort_present": paths["cohort"].is_file(),
            "preregistration_path": str(paths["preregistration"]),
            "n_events": int(n_events),
            "n_events_source": "reproduce(n_events=); not a configuration key",
            "repetitions": int(repetitions),
            "repetitions_source": "reproduce(repetitions=); not a configuration key",
            "bootstrap_samples": settings.bootstrap_samples,
            "max_age_seconds": MAX_AGE_SECONDS,
            "real_audit_dir": (str(real_audit_dir) if real_audit_dir is not None else None),
            "real_audit_dir_source": (
                "reproduce(real_audit_dir=); no directory is selected for the caller"
                if real_audit_dir is not None
                else "not supplied, so no real acquisition audit is cited by this run"
            ),
            "release_dataset": (str(release_dataset) if release_dataset is not None else None),
            "release_dataset_source": (
                "reproduce(release_dataset=); the dataset is verified and cited, never fitted"
                if release_dataset is not None
                else "not supplied, so no archived release dataset is cited by this run"
            ),
            "real_inputs_used_as_fitted_inputs": False,
        },
        "settings": settings.as_record(),
        "settings_sources": settings.sources(),
        "seeds": seed_record,
        "hashes": {
            "spec": spec_hash,
            "event_windows": event_windows_hash,
            "universe": universe_hash,
            "source": (raw_section or {}).get("archived_payloads_hashed")
            or (sample_section or {}).get("fixture_hash"),
            "source_fixture": (sample_section or {}).get("fixture_hash"),
            "data": (registry_section or {}).get("data_hash"),
            "environment": environment_hash,
            "spec_digest": spec_digest,
            "real_audit_coverage": (external_evidence.get("audit") or {}).get("sha256"),
            "release_dataset": ((external_evidence.get("releases") or {}).get("dataset") or {}).get(
                "content_hash"
            ),
        },
        "git": git,
        "runtime": runtime,
        "dependencies": dependencies,
        "environment_lock_hash": _locked_environment_digest(dependencies),
        "lock": lock,
        "fitted_models": _fitted_models(model_section),
        "cutoffs_and_event_counts": _cutoffs(model_section, coverage_section),
        "registry": registry_record,
        "figures": [record["name"] for record in figure_records],
        "blocked_stages": list(run.blocked),
        "notes": [
            "every recorded value is a function of the declared inputs, seeds and the bytes on disk",
            "the registry run record is the only artifact carrying a wall-clock stamp, and it is "
            "stamped once and reused on replay",
        ],
    }

    placebos = run.stage(
        "placebos",
        lambda: _placebo_section(
            artifacts.usable_panel
            if artifacts is not None
            else pd.DataFrame(columns=["cohort", "shock", "pre_event_change"]),
            (response_section or {}).get("usable") or {},
        ),
    )

    metrics: dict[str, Any] = {
        "status": None,
        "complete": None,
        "classification": "synthetic_software_methods_reproduction",
        "synthetic": True,
        "spec_path": str(spec),
        "output_dir": str(output),
        "settings": settings.as_record(),
        "seeds": seed_record,
        "sample": sample_section,
        "panels": {
            name: {
                key: (coverage_section or {}).get(name, {}).get(key)
                for key in (
                    "path",
                    "table",
                    "content_hash",
                    "coverage_epoch",
                    "row_count",
                    "replay_order",
                    "totals",
                )
            }
            for name in ("source", "usable")
        },
        "coverage": coverage_section,
        "response_curve": response_section,
        "forecast": forecast_section,
        "model_comparison": model_section,
        "null_vs_communication": falsification_summary,
        "coherence": coherence_section,
        "power": power_section,
        "placebos": placebos,
        "gates": gates,
        "empirical": empirical,
        "external_evidence": external_evidence,
        "methods_used": _methods_used(
            response_section,
            model_section,
            falsification_summary,
            power_section,
            coherence_section,
            placebos,
        ),
        "registry": registry_record,
        "figures": manifest["figures"],
        "files": [],
        "warnings": run.warnings,
        "manifest_file": MANIFEST_NAME,
        "manifest": manifest,
    }
    figures_only = {
        "name": FIGURES_DIRECTORY + "/",
        "sha256": None,
        "entries": [record["name"] for record in figure_records],
    }

    complete = not run.blocked_ids
    metrics["complete"] = bool(complete)
    metrics["status"] = "ok" if complete else "blocked"
    metrics["blocked_stages"] = list(run.blocked)

    ledger = [*artifacts_written, figures_only]
    for name, text in (
        (BASELINE_REPORT_NAME, _baseline_report(metrics)),
        (CONDITIONAL_REPORT_NAME, _conditional_report(metrics)),
        (DATA_CARD_NAME, _data_card(metrics)),
        (PAPER_NAME, _paper(metrics)),
    ):
        record = run.stage(
            f"report:{name}", lambda name=name, text=text: _write_text(output / name, text)
        )
        if record:
            ledger.append(record)
    if run.blocked_ids:
        complete = False

    metrics["complete"] = bool(complete)
    metrics["status"] = "ok" if complete else "blocked"
    metrics["blocked_stages"] = list(run.blocked)
    metrics, conversion_notes = _json_ready_with_notes(metrics)
    metrics["interpretation_notes"] = [
        *sorted(set(conversion_notes)),
        "null denotes a statistic that is unavailable, non-finite or not identified at this input; "
        "it is never a zero",
    ]

    metrics_record = run.stage("metrics", lambda: _write_json(output / METRICS_NAME, metrics))
    if metrics_record:
        ledger.append(metrics_record)
        metrics["files"] = sorted(ledger, key=lambda item: str(item.get("name")))
        run.stage("metrics", lambda: _write_json(output / METRICS_NAME, metrics))

    manifest["files"] = sorted(ledger, key=lambda item: str(item.get("name")))
    manifest["complete"] = bool(complete)
    manifest["status"] = "ok" if complete else "blocked"
    manifest_record = run.stage("manifest", lambda: _write_json(output / MANIFEST_NAME, manifest))

    return {
        "status": "ok" if complete else "blocked",
        "complete": bool(complete),
        "blocked_stages": list(run.blocked),
        "warnings": run.warnings,
        "classification": "synthetic_software_methods_reproduction",
        "output_dir": str(output),
        "spec_path": str(spec),
        "config": study_meta,
        "seed": settings.master_seed,
        "settings": settings.as_record(),
        "sample": sample_section,
        "panels": metrics["panels"],
        "coverage": coverage_section,
        "forecast": forecast_section,
        "gates": gates,
        "empirical": empirical,
        "response_curve": response_section,
        "model_comparison": model_section,
        "null_vs_communication": falsification_summary,
        "coherence": coherence_section,
        "power": power_section,
        "placebos": placebos,
        "registry": registry_record,
        "manifest": manifest,
        "manifest_file": manifest_record,
        "metrics": {"path": METRICS_NAME, "json_ready": True},
        "files": sorted(ledger, key=lambda item: str(item.get("name"))),
        "methods_used": metrics["methods_used"],
    }


def _registry_record(
    *,
    registration: Mapping[str, Any] | None,
    seed_record: Mapping[str, Any],
    spec_digest: str,
    environment_hash: str,
) -> dict[str, Any]:
    """The recorded registry state, or an explicit record of why nothing was stored."""
    if not registration:
        return {
            "path": REGISTRY_DB_NAME,
            "export": JSONL_NAME,
            "run_id": None,
            "n_runs": None,
            "n_reservations": None,
            "n_event_claims": None,
            "locked_test_reserved": False,
            "recorded": False,
            "reason": "the registry stage did not complete, so no run was recorded",
        }
    return {
        "path": REGISTRY_DB_NAME,
        "export": JSONL_NAME,
        "run_id": registration.get("run_id"),
        "n_runs": registration.get("n_runs"),
        "n_reservations": registration.get("n_reservations"),
        "n_event_claims": registration.get("n_event_claims"),
        "event_ids_recorded": registration.get("event_ids_recorded"),
        "locked_test_reserved": False,
        "recorded": True,
        "spec_hash": spec_digest,
        "data_hash": registration.get("data_hash"),
        "source_hash": registration.get("source_hash"),
        "environment_hash": environment_hash,
        "seeds": dict(seed_record),
        "note": (
            "a synthetic run records its provenance and reserves nothing: reserve_locked_test is "
            "never called, so no real locked-test cohort is consumed and the real cohort remains "
            "available for the one-shot locked evaluation"
        ),
    }


def _valid_total(coverage_section: Mapping[str, Any] | None) -> Any:
    if not coverage_section:
        return None
    return {
        name: (coverage_section.get(name) or {}).get("totals", {}).get("valid_rows")
        for name in ("source", "usable")
    }


def _mae_reduction(model_section: Mapping[str, Any] | None, name: str) -> Any:
    if not model_section:
        return None
    return ((model_section.get(name) or {}).get("promotion") or {}).get("mae_reduction")


def _coherence_distance_for(coherence_section: Mapping[str, Any] | None, label: str) -> Any:
    for example in (coherence_section or {}).get("examples") or []:
        if example.get("label") == label:
            return example.get("distance")
    return None


def _forecast_panel_writer(
    frames: Mapping[str, Any], settings: _Settings, output: Path, spec_digest: str
) -> Any:
    def write() -> dict[str, Any]:
        declared = list(FORECAST_COLUMNS)
        source = frames["communication"]["target"]
        missing = [column for column in declared if column not in source.columns]
        if missing:
            raise ValueError(
                f"forecast rows are missing declared column(s) {missing}; the sealed forecast table "
                "requires every declared column"
            )
        subset = source.loc[:, declared].copy()
        # The declared schema now carries every predictor the fitted ladder reads
        # and the release, cohort and orientation columns admissibility needs, so
        # what remains outside it is the truth-only quantities alone.
        unsealed = sorted(set(source.columns) - set(declared))
        path = output / FORECAST_PANEL_NAME
        reference = write_parquet(
            subset,
            path,
            table="forecast",
            coverage_epoch=f"synthetic-{settings.simulation_seed}-{settings.horizon_seconds}",
            metadata={
                "scenario": frames["communication"]["scenario"],
                "generator": "market_propagation.simulation.simulate_scenario",
                "seed": settings.simulation_seed,
                "n_events": int(subset["event_id"].nunique()),
                "horizon_seconds": settings.horizon_seconds,
                "prediction_delay_seconds": settings.prediction_delay_seconds,
                "spec_digest": spec_digest,
                "synthetic": True,
                "rows_scope": "primary_target_rows_of_the_communication_process",
                "paired_null_scenario": "shared_news_delay",
                "raw_provenance": (
                    "generated by the seeded simulator; a simulated row has no raw payload, so its "
                    "provenance is the generator, its seed and the recorded settings"
                ),
            },
        )
        return {
            "path": path.name,
            "table": reference.table,
            "schema_version": reference.schema_version,
            "required_schema_version": FORECAST_SCHEMA_VERSION,
            "content_hash": reference.content_hash,
            "coverage_epoch": reference.coverage_epoch,
            "row_count": reference.row_count,
            "scenario": frames["communication"]["scenario"],
            "columns": declared,
            "rows_written": len(subset),
            "truth_only_columns_excluded": unsealed,
            "cluster_column": "cluster_id",
            "n_clusters": int(subset["cluster_id"].nunique()),
            "manifest": reference.manifest,
            "note": (
                "the sealed panel holds the primary-target rows the production comparison scores, "
                "carrying every predictor column the fitted ladder reads -- including the "
                "neighbour-lag family -- together with cluster_id, cohort, orientation_sign and "
                "exclusion_reason, so the model comparison and its release splits can be rebuilt "
                "from these bytes; the truth-only quantities named above are excluded, and no field "
                "a model reads is among them"
            ),
        }

    return write


def _response_stage(artifacts: Any, settings: _Settings) -> Any:
    def run_stage() -> dict[str, Any]:
        from .models import local_projections

        section: dict[str, Any] = {
            "estimator": "local_projections",
            "primary_metric": "mean_response",
            "shock_column": None,
            "identification": "descriptive only; the packaged sample carries no release expectation",
            "mask_column": "valid",
            "cluster_column": "cluster_id",
            "orientation_column": "orientation_sign",
            "seed": settings.resampling_seed,
            "bootstrap_samples": settings.bootstrap_samples,
            "minimum_response": settings.minimum_response,
            "independent_unit": "economic_release",
            "results": {},
        }
        for fold, panel in (("usable", artifacts.usable_panel), ("source", artifacts.source_panel)):
            payload = local_projections(
                panel,
                shock_column=None,
                seed=settings.resampling_seed,
                bootstrap_samples=settings.bootstrap_samples,
                cluster_column="cluster_id",
                event_column="event_id",
                family_column="family",
                horizon_column="horizon_seconds",
                response_column="response",
            )
            curves = payload.get("curves") or {}
            for curve in curves.values():
                band = curve.get("simultaneous_band") or {}
                curve["band_status"] = band.get("status")
                curve["band_reason"] = band.get("reason")
            section[fold] = {
                "replay_order": sorted(str(value) for value in panel["replay_order"].unique()),
                "curve_status": payload.get("status"),
                "n_rows_used": payload.get("n_rows_used"),
                "n_events_used": payload.get("n_events_used"),
                "n_clusters_used": payload.get("n_clusters_used"),
                "singleton_clusters": payload.get("singleton_clusters"),
                "singleton_cluster_note": payload.get("singleton_cluster_note"),
                "excluded": payload.get("excluded"),
                "orientation_note": payload.get("orientation_note"),
                "engine": payload.get("engine"),
                "curves": curves,
                "cells": payload.get("cells"),
                "inconclusive_cells": payload.get("inconclusive_cells"),
                "reasons": payload.get("reasons"),
            }
        return section

    return run_stage


def _registry_stage(
    *,
    output_dir: Path,
    run_id: str,
    spec_digest: str,
    environment_hash: str,
    sync_hashes: Mapping[str, Any],
    event_ids: Sequence[str],
    seed: int,
    metrics: Mapping[str, Any],
    classification: str,
) -> dict[str, Any]:
    path = output_dir / REGISTRY_DB_NAME
    with ExperimentRegistry(path) as registry:
        registry.record_run(
            {
                "run_id": run_id,
                "spec_hash": spec_digest,
                "data_hash": sync_hashes["data"],
                "source_hash": sync_hashes["source"],
                "environment_hash": environment_hash,
                "event_ids": sorted({str(event_id) for event_id in event_ids}),
                "seed": int(seed),
                "metrics": {**dict(metrics), "classification": classification},
                "synthetic": True,
            }
        )
        snapshot = registry.snapshot()
        registry.export_jsonl(output_dir / JSONL_NAME)
    return {
        "path": path.name,
        "export": JSONL_NAME,
        "run_id": run_id,
        "n_runs": len(snapshot["runs"]),
        "n_reservations": len(snapshot["reservations"]),
        "n_event_claims": len(snapshot["event_claims"]),
        "locked_test_reserved": False,
        "event_ids_recorded": len({str(event_id) for event_id in event_ids}),
        "data_hash": sync_hashes["data"],
        "source_hash": sync_hashes["source"],
        "environment_hash": environment_hash,
        "spec_digest": spec_digest,
    }


def _fitted_models(model_section: Mapping[str, Any] | None) -> dict[str, Any]:
    fitted: dict[str, Any] = {}
    for process in ("shared_news_delay", "communication"):
        record = (model_section or {}).get(process) or {}
        fitted[process] = {
            kind: _trim_model_record(payload)
            for kind, payload in (record.get("models") or {}).items()
        }
    return fitted


def _cutoffs(
    model_section: Mapping[str, Any] | None, coverage_section: Mapping[str, Any] | None
) -> dict[str, Any]:
    cutoffs: dict[str, Any] = {}
    for process in ("shared_news_delay", "communication"):
        record = (model_section or {}).get(process) or {}
        folds = record.get("folds") or {}
        sample = record.get("sample") or {}
        cutoffs[process] = {
            "train_cutoff": folds.get("train_cutoff"),
            "validation_cutoff": folds.get("validation_cutoff"),
            "embargo_seconds": folds.get("embargo_seconds"),
            "purged_rows": folds.get("purged_rows"),
            "n_events_train": sample.get("n_events_train"),
            "n_events_validation": sample.get("n_events_validation"),
            "n_events_test": sample.get("n_events_test"),
            "n_clusters_test": sample.get("n_clusters_test"),
            "n_rows_total": sample.get("n_rows_total"),
        }
    cutoffs["sample_panel"] = {
        "source_rows": ((coverage_section or {}).get("source") or {}).get("row_count"),
        "usable_rows": ((coverage_section or {}).get("usable") or {}).get("row_count"),
        "releases": ((coverage_section or {}).get("source") or {}).get("totals", {}).get("events"),
    }
    return cutoffs


def _methods_used(
    response: Mapping[str, Any] | None,
    models: Mapping[str, Any] | None,
    falsification: Mapping[str, Any] | None,
    power: Mapping[str, Any] | None,
    coherence: Mapping[str, Any] | None,
    placebos: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "response_estimation": {
            "method": "local_projections",
            "shock_column": None,
            "status": (response or {}).get("usable", {}).get("curve_status"),
        },
        "network_comparison": {
            "method": "nested_held_out_comparison",
            "kinds": (models or {}).get("kinds"),
            "status": {
                name: ((models or {}).get(name) or {}).get("promotion", {}).get("status")
                for name in ("shared_news_delay", "communication")
            },
        },
        "null_audit": {
            "method": "network_falsification",
            "status": (falsification or {}).get("status"),
            "power_source": (falsification or {}).get("power_source"),
        },
        "power_assessment": {
            "method": "cluster_aware_event_level_simulation",
            "residual_sigma": (power or {}).get("residual_sigma"),
            "residual_sigma_source": (power or {}).get("residual_sigma_source"),
            "status": (power or {}).get("status"),
        },
        "coherence": {
            "method": "min_max_distance_to_quoted_box",
            "status": "ok" if (coherence or {}).get("examples") else None,
        },
        "placebos_available": sorted((placebos or {}).get("available") or {}),
        "not_run": [
            {
                "method": "local_projections_with_liquidity_interaction",
                "reason": "the packaged sample carries no liquidity interaction variable",
            },
            {
                "method": "surprise_slope_estimation",
                "reason": "no release expectation source is packaged; the specification forbids "
                "gating the timing study on an absent expectation",
            },
            {
                "method": "resolution_scoring",
                "reason": "the packaged fixture contains no outcome, so no label is available",
            },
            {
                "method": "cross_venue_replication",
                "reason": "no verified equivalent contract pair is present in this reproduction",
            },
            {
                "method": "iid_resampling_of_individual_quote_snapshots",
                "reason": "prohibited by the specification; the independent unit is the release",
            },
        ],
    }


def _scalar_projection(payload: Any, *, depth: int = 2, prefix: str = "") -> dict[str, Any]:
    """Scalars from a JSON document, keyed by dotted path, bounded by depth.

    The real audit's own schema is not restated here: whatever scalar fields it
    carries are surfaced under their own names, so a renamed or added key shows
    up rather than reading as absent.
    """
    if depth < 0:
        return {}
    out: dict[str, Any] = {}
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, Mapping):
                out.update(_scalar_projection(value, depth=depth - 1, prefix=path))
            elif isinstance(value, list | tuple):
                if all(not isinstance(item, Mapping | list | tuple) for item in value):
                    out[f"{path}[]"] = list(value)
                else:
                    out[f"{path}[]"] = f"<{len(value)} entries omitted>"
            else:
                out[path] = value
    return out


def _display_path(path: Path, root: Path) -> str:
    """A path relative to the repository root when it sits inside it, else absolute.

    An explicitly named input may live outside the checkout, and a relative-path
    convenience must not turn that into a crash or a misleading ``../..`` chain.
    """
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def _external_evidence(
    root: Path,
    *,
    real_audit_dir: str | Path | None = None,
    release_dataset: str | Path | None = None,
) -> dict[str, Any]:
    """Real evidence this run was explicitly pointed at, cited at the status it has.

    Nothing is selected for the caller. An audit or release dataset is cited only
    when it was named by path, so a checkout that happens to contain one cannot
    change what a reproduction claims, and a caller who wants synthetic-only output
    gets exactly that by supplying nothing.

    The audit's own status is repeated rather than restated, and a partial audit
    whose candidate set is incomplete does not supply empirical observations, so
    the eligibility it failed to establish is reported as not established. A named
    release dataset is verified through the same archived-release source the audit
    path uses: every selected payload is re-read out of its own raw store and
    reparsed, and a record that disagrees with the bytes it cites is reported as
    unverified and counts for nothing.
    """
    audit: dict[str, Any] | None = None
    present: list[str] = []
    raw_store_present = False
    if real_audit_dir is not None:
        audit_dir = Path(real_audit_dir).expanduser()
        if not audit_dir.is_dir():
            raise FileNotFoundError(
                f"real audit directory not found: {audit_dir}; an explicitly named input must "
                "exist, and no directory is selected on the caller's behalf"
            )
        candidates = {
            "coverage": audit_dir / "coverage.json",
            "event_card": audit_dir / "event_card.json",
            "series_discovery": audit_dir / "series_discovery.json",
            "raw_hashes": audit_dir / "raw_hashes.json",
        }
        present = [_display_path(path, root) for path in candidates.values() if path.is_file()]
        raw_store_present = (audit_dir / "raw").is_dir()
        coverage_path = candidates["coverage"]
        if not coverage_path.is_file():
            audit = {
                "status": "unreadable",
                "reason": f"no coverage.json under the named audit directory {audit_dir}",
                "path": _display_path(coverage_path, root),
                "explicitly_selected": True,
            }
        else:
            try:
                payload = json.loads(coverage_path.read_text(encoding="utf-8"))
            except Exception as error:
                audit = {
                    "status": "unreadable",
                    "reason": f"{type(error).__name__}: {error}",
                    "path": _display_path(coverage_path, root),
                    "explicitly_selected": True,
                }
            else:
                scalars = _scalar_projection(payload)
                reported_complete = scalars.get("complete")
                audit = {
                    "path": _display_path(coverage_path, root),
                    "audit_dir": _display_path(audit_dir, root),
                    "explicitly_selected": True,
                    "sha256": hash_file(coverage_path),
                    "status": scalars.get("status"),
                    "complete": reported_complete,
                    "eligible_cohort_established": bool(reported_complete is True),
                    "recorded_fields": scalars,
                    "recorded_field_count": len(scalars),
                    "payload_sha256": _digest(payload),
                    "interpretation": (
                        "the audit is reported at the status and completeness it recorded, with its "
                        "own field names preserved; a partial audit does not establish an eligible "
                        "cohort, so its candidate counts are not reported here as eligible "
                        "observations"
                        if reported_complete is not True
                        else "the audit recorded itself complete, so its coverage is available as "
                        "evidence for the cohort and access gate"
                    ),
                }

    releases: dict[str, Any] | None = None
    if release_dataset is not None:
        dataset_path = Path(release_dataset).expanduser()
        releases = _verify_release_dataset(dataset_path, root)
        if releases.get("verified"):
            present.extend(releases.get("dataset_paths") or [])

    if audit is not None:
        note = (
            "a real public acquisition audit was named explicitly and is linked at the status it "
            "recorded. It is never produced, re-run or upgraded by this reproduction, and its "
            "incompleteness keeps empirical claims blocked."
        )
    elif releases is not None:
        note = (
            "no real acquisition audit was named, so no audit is cited. A normalized original "
            "release dataset was named and is linked at the verification status it recorded; the "
            "synthetic sample and the simulated processes remain the only fitted inputs."
        )
    else:
        note = (
            "no real public acquisition audit and no archived release dataset were named, so this "
            "reproduction cites no real input at all; real cohorts stay blocked and are not "
            "approximated by the synthetic sample. Nothing is selected for the caller: name "
            "real_audit_dir or release_dataset to cite a real input deliberately."
        )
    return {
        "paths": present,
        "audit": audit,
        "releases": releases,
        "raw_store_present": raw_store_present,
        "note": note,
    }


def _verify_release_dataset(dataset_path: Path, root: Path) -> dict[str, Any]:
    """Verify a named archived-release dataset, reporting rather than raising on doubt.

    A dataset that cannot be opened at all is reported as unverified with its
    reason. That is a fact about the citation, not a fault in the reproduction, so
    it is reported beside the synthetic result instead of aborting a run whose
    fitted inputs never touch this file.
    """
    from .ingest.macro_releases import ArchivedReleaseSource

    manifest_path = dataset_path.with_name(dataset_path.name + ".manifest.json")
    cited: dict[str, Any] = {
        "path": _display_path(dataset_path, root),
        "explicitly_selected": True,
        "verified": False,
        "reason": None,
        # Present on every outcome so a consumer never has to branch on the shape of
        # a failed citation: an unverified dataset verified nothing.
        "records_verified": 0,
        "records_blocked": 0,
        "all_records_verified": False,
        "records": [],
        "blocked_records": [],
        "dataset": None,
        "usable_time": None,
        "usable_time_note": None,
        "interpretation": None,
        "dataset_paths": [
            _display_path(path, root)
            for path in (dataset_path, manifest_path, dataset_path.parent / "raw")
            if path.exists()
        ],
        "used_as_a_fitted_input": False,
        "entered_synthetic_fit": False,
        "certifies_historical_market_rule_versions": False,
        "certifies_quote_coverage": False,
        "limits_note": (
            "original BLS values alone do not certify which market rule version was in force at a "
            "release, nor the quote coverage of any contract, so they cannot promote a blocked "
            "cohort gate to an empirical result"
        ),
    }
    if not dataset_path.exists():
        cited["reason"] = f"no archived release dataset at {dataset_path}"
        return cited
    try:
        detail = ArchivedReleaseSource(dataset_path).verify_all()
    except Exception as error:
        cited["reason"] = f"{type(error).__name__}: {error}"
        return cited
    cited.update(
        {
            "verified": bool(detail["records_verified"]) and not detail["records_blocked"],
            "reason": None,
            "dataset": detail["dataset"],
            "records_verified": detail["records_verified"],
            "records_blocked": detail["records_blocked"],
            "all_records_verified": detail["all_records_verified"],
            "records": detail["records"],
            "blocked_records": detail["blocked_records"],
            "usable_time": detail["usable_time"],
            "usable_time_note": detail["usable_time_note"],
            "interpretation": detail["interpretation"],
        }
    )
    return cited


def _gates(
    *,
    sample_section: Mapping[str, Any] | None,
    coverage_section: Mapping[str, Any] | None,
    response_section: Mapping[str, Any] | None,
    model_section: Mapping[str, Any] | None,
    falsification: Mapping[str, Any] | None,
    coherence_section: Mapping[str, Any] | None,
    power_section: Mapping[str, Any] | None,
    external_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    synthetic = "synthetic_software_experiment"
    audit = (external_evidence or {}).get("audit") or {}
    if audit.get("eligible_cohort_established") is True:
        g0_status = "partial"
        g0_detail = (
            f"the separate real acquisition audit at {audit.get('path')} recorded itself complete; "
            "it is referenced here, not reproduced, and no empirical claim follows from it in this run"
        )
    elif audit:
        g0_status = "blocked"
        g0_detail = (
            f"the separate real acquisition audit at {audit.get('path')} recorded status "
            f"{audit.get('status')!r} with complete={audit.get('complete')!r}, so it establishes no "
            "eligible empirical cohort; this reproduction contributes only the synthetic sample"
        )
    else:
        g0_status = "blocked"
        g0_detail = (
            "no real eligible cohort is present: the sample is the packaged synthetic fixture "
            f"{(sample_section or {}).get('fixture_hash')} and no acquisition audit is part of "
            "this reproduction"
        )
    gates: dict[str, Any] = {
        "G0": {
            "name": "cohort_and_access",
            "status": g0_status,
            "evidence_class": "real_public_acquisition_audit_referenced" if audit else "none",
            "detail": g0_detail,
        },
        "G1": {
            "name": "replay_and_availability",
            "status": "ok" if (sample_section or {}).get("replay_disagreements") else "blocked",
            "evidence_class": synthetic,
            "detail": (
                "two real replay folds over the same archived bytes disagree as reported: "
                f"{_digest((sample_section or {}).get('replay_disagreements') or {})[:12]}"
            ),
        },
        "G2": {
            "name": "measurement",
            "status": "ok" if coverage_section else "blocked",
            "evidence_class": synthetic,
            "detail": (
                "sealed panels counted through the storage query layer; masked rows carry a reason "
                "and a null measurement"
            ),
        },
        "G3": {
            "name": "falsification_and_power",
            "status": "ok" if (falsification or {}).get("status") == "ok" else "blocked",
            "evidence_class": synthetic,
            "detail": (
                f"null audit status {(falsification or {}).get('status')}; "
                f"null rate {(falsification or {}).get('null', {}).get('false_positive_rate')}; "
                f"recovery power {(falsification or {}).get('recovery', {}).get('power')}"
            ),
        },
        "G4": {
            "name": "held_out_gain",
            "status": "ok"
            if ((model_section or {}).get("communication") or {}).get("promotion", {}).get("status")
            == "promoted"
            else "blocked",
            "evidence_class": synthetic,
            "detail": (
                "the network candidate is "
                f"{((model_section or {}).get('communication') or {}).get('promotion', {}).get('status')} "
                "on the communication process; a predictive edge would still not be a propagation claim"
            ),
        },
        "G5": {
            "name": "robustness_and_replication",
            # This status is a declared conservative default, not a computed result:
            # no replication cohort is an input to this reproduction, so there is
            # nothing here that could report success. ``basis`` says so in the
            # artifact, and it names the input that would have to exist before this
            # gate can be derived rather than asserted.
            "status": "blocked",
            "evidence_class": "none",
            "basis": "declared_default_no_replication_input_in_this_reproduction",
            "would_be_derived_from": (
                "a verified independent replication cohort, or a verified cross-venue "
                "equivalent contract pair, supplied as an explicit input"
            ),
            "detail": (
                "endpoint and leave-one-release-out sensitivities are reported, but no independent "
                "replication cohort exists and no verified cross-venue equivalent pair is present"
            ),
        },
        "G6": {
            "name": "claims_and_package",
            "status": "ok"
            if coherence_section is not None and power_section is not None
            else "blocked",
            "evidence_class": synthetic,
            "detail": (
                "reports are generated from the observed values in this output directory and every "
                "claim is labelled synthetic"
            ),
        },
    }
    if not (response_section or {}).get("usable"):
        gates["G2"]["detail"] += "; the descriptive curve stage did not complete"
    return gates
