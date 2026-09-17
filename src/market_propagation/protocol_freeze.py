"""The protocol freeze: one sealed, dated identity over the declarations.

The preregistration declares an estimand, a cohort, a ladder, horizons, seeds, a
threshold and a stopping rule. Those declarations live in configurations and in the
modules that define the estimands, and a result records the identities it depended on
(:func:`market_propagation.study._source_manifest`). What a per-result record does not
give a reader is a single *instant* to check a later run against: which bytes were the
protocol when a claim was registered, and what stops the analysis.

This module supplies exactly that and nothing else. It hashes the declarative inputs
and the estimand-defining modules, records the instant the freeze was taken (T0) and
the declared stopping rule, and re-verifies a held freeze against the checkout.

It decides no eligibility, reads no market data, and asserts no result. A freeze is
not evidence about any contract or release; it is evidence about which rules were in
force when a claim was registered.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .storage import hash_file

#: Version of the freeze *format*, so a later freeze can be told from this one.
PROTOCOL_VERSION = "protocol_v1"

#: The file a freeze is written to and read from.
FREEZE_NAME = "protocol_freeze.json"

#: Declarations: every configuration and document whose text states a rule the
#: estimand, the cohort, the ladder, the thresholds or the stopping rule depends on.
#: Hashing the document as well as the configuration matters because the
#: preregistration is where several of the rules exist only as prose.
DECLARATION_FILES: tuple[str, ...] = (
    "configs/cohort_v2.yaml",
    "configs/cohort.yaml",
    "configs/cohort_forward.yaml",
    "configs/event_windows_v2.yaml",
    "configs/external_history_v1.yaml",
    "configs/neighbor_graph_v2.yaml",
    "configs/study_v2.yaml",
    "configs/matching_v1.yaml",
    "configs/rule_attestation_v1.yaml",
    "configs/endpoints.yaml",
    "configs/studies/study_a_absorption.yaml",
    "configs/studies/study_b_crossvenue.yaml",
    "configs/studies/study_c_crossmeeting.yaml",
    "configs/studies/study_d_perp.yaml",
    "reports/preregistration_v2.md",
    "reports/preregistration_studies.md",
    "reports/population_change_d2.md",
    "reports/contract_rule_registry.json",
)

#: The modules that define the estimands, the ladder, the split authority, the
#: promotion gate, the null family and the universe the candidates are read from.
#: A configuration that declares an estimand the code computes differently is not a
#: frozen protocol, so these bytes are part of the freeze rather than context for it.
ESTIMAND_MODULES: tuple[str, ...] = (
    "src/market_propagation/models.py",
    "src/market_propagation/neighbors.py",
    "src/market_propagation/study.py",
    "src/market_propagation/timing_model.py",
    "src/market_propagation/historical_forecast.py",
    "src/market_propagation/simulation.py",
    "src/market_propagation/simulated_tapes.py",
    "src/market_propagation/calibration.py",
    "src/market_propagation/falsification.py",
    "src/market_propagation/cross_venue.py",
    "src/market_propagation/ingest/audit.py",
    "src/market_propagation/ingest/expectations.py",
    "src/market_propagation/ingest/kalshi_universe.py",
)

#: The declared stopping rule. Analysis is reported at each arm's declared release
#: count; no release is ever added or removed once the arm has been run against it;
#: and neither arm is ever pooled with the other.
STOPPING_RULE: Mapping[str, Any] = {
    "retrospective_arm": {
        "cohort_id": "core_2025h1",
        "stops_at": "its declared ten releases",
        "extension": "none",
        "a_release_is_never_added_or_removed": True,
    },
    "forward_arm": {
        "cohort_id": "forward_2026h2",
        "stops_at": "the last release of its declared 2026-10 through 2026-12 window",
        "extension": "next_scheduled_release_per_family, read from the declared BLS calendars",
        "membership_fixed_before_each_release_instant": True,
        "a_release_is_never_added_or_removed": True,
        "a_release_that_contributes_no_row_stays_in_the_denominator": True,
    },
    "pooling": "prohibited",
    "each_arm_is_its_own_denominator": True,
    "confirmatory_estimation_requires": (
        "an arm whose declared prerequisites are met; registered_estimation."
        "confirmatory_estimation_permitted is false on this checkout"
    ),
}

#: Sources a freeze is read from, so a reader can see what the rule does not depend on.
NOT_HASHED: tuple[str, ...] = (
    "data/**",
    "reports/*.json and -e reports/*.md other than the preregistration and the "
    "population change page",
)


class ProtocolDriftError(ValueError):
    """A held freeze no longer matches the bytes it was taken over."""


def _repo_root() -> Path:
    """The checkout this module was imported from."""
    return Path(__file__).resolve().parents[2]


def _normalize(frozen_at: str | dt.datetime) -> str:
    """T0 as a single UTC instant, so two spellings of one moment compare equal."""
    if isinstance(frozen_at, dt.datetime):
        moment = frozen_at
    else:
        text = str(frozen_at).strip()
        if not text:
            raise ValueError("frozen_at must be a non-empty instant")
        moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(
            f"frozen_at={frozen_at!r} carries no offset, and a freeze instant without a "
            "zone is not a single instant"
        )
    return moment.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def protocol_inputs(root: str | Path | None = None) -> dict[str, Path]:
    """Every file a freeze covers, by the relative path it is named under."""
    base = Path(root) if root is not None else _repo_root()
    return {name: base / name for name in (*DECLARATION_FILES, *ESTIMAND_MODULES)}


def freeze_hash(files: Mapping[str, str | None]) -> str:
    """One identity over the covered files, stable under iteration order.

    A file that was absent at the freeze hashes as ``null`` and stays in the digest, so
    a file that appears later is a change rather than an addition the freeze never saw.
    """
    payload = json.dumps(dict(sorted(files.items())), separators=(",", ":"), default=str)
    import hashlib

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_protocol_freeze(
    *,
    frozen_at: str | dt.datetime,
    root: str | Path | None = None,
    stopping_rule: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Seal the declarations and the estimand modules at one instant.

    ``frozen_at`` is T0 and is required: a freeze taken without a stated instant cannot
    be told from a freeze taken after a result was seen, which is the only thing the
    instant is for. A covered file that is absent is recorded as ``null`` rather than
    omitted, so the freeze still says it was expected.
    """
    files = {
        name: (hash_file(path) if path.is_file() else None)
        for name, path in protocol_inputs(root).items()
    }
    return {
        "protocol_version": PROTOCOL_VERSION,
        "frozen_at": _normalize(frozen_at),
        "t0_is_the_freeze_instant": True,
        "freeze_is_not_evidence_about_any_contract_or_release": True,
        "stopping_rule": dict(stopping_rule if stopping_rule is not None else STOPPING_RULE),
        "declaration_files": list(DECLARATION_FILES),
        "estimand_modules": list(ESTIMAND_MODULES),
        "not_hashed": list(NOT_HASHED),
        "files": files,
        "files_covered": len(files),
        "files_absent_at_freeze": sorted(name for name, digest in files.items() if digest is None),
        "freeze_hash": freeze_hash(files),
    }


def verify_protocol_freeze(
    manifest: Mapping[str, Any], *, root: str | Path | None = None
) -> dict[str, Any]:
    """Re-hash every covered file and report whether the protocol still matches.

    Reports rather than raises, because a reader asking "has the protocol moved since
    the freeze" wants the list of files that moved, not the first one. Use
    :func:`assert_protocol_freeze` where a drifted freeze must stop a run.
    """
    declared = manifest.get("files")
    if not isinstance(declared, Mapping):
        raise ValueError("the freeze carries no `files` mapping, so it declares no protocol")
    inputs = protocol_inputs(root)
    drifted: list[dict[str, str | None]] = []
    missing: list[str] = []
    unexpected: list[str] = []
    for name, expected in declared.items():
        path = inputs.get(str(name))
        if path is None:
            path = (Path(root) if root is not None else _repo_root()) / str(name)
        if not path.is_file():
            missing.append(str(name))
            continue
        actual = hash_file(path)
        if actual != expected:
            drifted.append({"name": str(name), "frozen": expected, "now": actual})
    for name in inputs:
        if name not in declared:
            unexpected.append(name)
    return {
        "protocol_version": manifest.get("protocol_version"),
        "frozen_at": manifest.get("frozen_at"),
        "verified": not drifted and not missing and not unexpected,
        "files_checked": len(declared),
        "drifted": drifted,
        "missing": sorted(missing),
        "expected_but_not_covered": sorted(unexpected),
        "freeze_hash": manifest.get("freeze_hash"),
        "matches_freeze_hash": (
            freeze_hash({str(k): v for k, v in declared.items()}) == manifest.get("freeze_hash")
        ),
        "what_a_drift_means": (
            "a covered declaration or estimand module changed after T0, so a result "
            "computed from the checkout no longer belongs to the frozen protocol unless "
            "the change is recorded and re-frozen"
        ),
    }


def assert_protocol_freeze(
    manifest: Mapping[str, Any], *, root: str | Path | None = None
) -> dict[str, Any]:
    """The verification report, or :class:`ProtocolDriftError` naming what moved."""
    report = verify_protocol_freeze(manifest, root=root)
    if not report["verified"]:
        moved = [entry["name"] for entry in report["drifted"]]
        raise ProtocolDriftError(
            "the checkout no longer matches the protocol freeze at "
            f"{report['frozen_at']}: drifted={moved}, missing={report['missing']}, "
            f"expected_but_not_covered={report['expected_but_not_covered']}"
        )
    return report


def write_protocol_freeze(manifest: Mapping[str, Any], path: str | Path) -> Path:
    """Write a freeze by replacing a temporary file, so no reader sees a partial one."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(dict(manifest), indent=2, sort_keys=True, default=str) + "\n"
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(target)
    return target


def read_protocol_freeze(path: str | Path) -> dict[str, Any]:
    """Read a held freeze, refusing a file that is not one."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "files" not in payload:
        raise ProtocolDriftError(f"{path} is not a protocol freeze: it carries no `files` mapping")
    if not payload.get("frozen_at"):
        raise ProtocolDriftError(f"{path} is not a protocol freeze: it states no frozen_at (T0)")
    return payload
