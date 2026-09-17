"""The confirmatory progress ledger: how far the study is from a confirmatory sample.

Every prerequisite the preregistration names for a confirmatory claim is reported per
declared release, computed from the artifacts on disk rather than asserted in prose, so
the ledger cannot drift from what is actually held. A prerequisite that no artifact can
answer is reported ``unobservable`` with its reason — never as met, and never as unmet,
because those are different facts.

The one prerequisite this ledger derives end to end is the **observation ahead of the
release**: for each declared release, how many held rule captures state an instant at or
before it. A capture certifies a window that opens at or after the instant its own
serving system states, so a capture taken after a release cannot certify that release
however good the capture is. That is the D2 gate, and it is the quantity the capture
cadence changes.

This module answers how far the study is from a confirmatory sample and nothing else.
It fits no model, selects no sample, admits no contract, and decides no eligibility.
A progress ledger is not evidence about any contract or release.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .ingest import rule_attestation

#: Version of the ledger *format*, so a later ledger can be told from this one.
LEDGER_VERSION = "confirmatory_progress_v1"

#: The file a ledger is written to.
LEDGER_NAME = "confirmatory_progress.json"

#: The declared arms are named by the v2 cohort file rather than by a list here, so a
#: second copy of the arm membership cannot drift from the one the study reads.
COHORT_CONFIG = "configs/cohort_v2.yaml"

#: Reported state of one prerequisite for one release.
STATE_MET = "met"
STATE_UNMET = "unmet"
STATE_UNOBSERVABLE = "unobservable"


@dataclass(frozen=True, slots=True)
class DeclaredRelease:
    """One release a declared arm states, with the instant it is measured against."""

    arm: str
    cohort_id: str
    event_id: str
    family: str
    scheduled_at_utc: dt.datetime
    eligibility_status: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "arm": self.arm,
            "cohort_id": self.cohort_id,
            "event_id": self.event_id,
            "family": self.family,
            "scheduled_at_utc": self.scheduled_at_utc.isoformat(),
            "eligibility_status": self.eligibility_status,
        }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _instant(value: Any, *, where: str) -> dt.datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{where} states no instant, and a release is measured against one")
    moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(f"{where}={text!r} carries no offset, so it is not a single instant")
    return moment.astimezone(dt.UTC)


def declared_releases(root: str | Path | None = None) -> tuple[DeclaredRelease, ...]:
    """Every release both declared arms state, in arm order then release order.

    The arms come from the cohort file's ``arms:`` block, and each arm's releases come
    from the file that arm names. Neither list is written here, so a release added to an
    arm is tracked without this module being edited.
    """
    base = Path(root) if root is not None else _repo_root()
    payload = yaml.safe_load((base / COHORT_CONFIG).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"{COHORT_CONFIG} is not a mapping")
    members = ((payload.get("arms") or {}) or {}).get("members")
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes)) or not members:
        raise ValueError(
            f"{COHORT_CONFIG} declares no arms.members; the ledger tracks declared arms and "
            "keeps no fallback list of its own"
        )
    out: list[DeclaredRelease] = []
    for member in members:
        arm = str(member.get("arm") or "")
        cohort_id = str(member.get("cohort_id") or "")
        arm_path = base / str(member.get("file") or "")
        if not arm or not cohort_id:
            raise ValueError(f"an arm in {COHORT_CONFIG} names no arm or cohort_id")
        arm_payload = yaml.safe_load(arm_path.read_text(encoding="utf-8"))
        events = (arm_payload or {}).get("events")
        if not isinstance(events, Sequence) or isinstance(events, (str, bytes)):
            raise ValueError(f"{arm_path} declares no events list")
        for event in events:
            out.append(
                DeclaredRelease(
                    arm=arm,
                    cohort_id=cohort_id,
                    event_id=str(event.get("event_id") or ""),
                    family=str(event.get("family") or ""),
                    scheduled_at_utc=_instant(
                        event.get("scheduled_at_utc"),
                        where=f"{arm_path}:{event.get('event_id')}.scheduled_at_utc",
                    ),
                    eligibility_status=str(event.get("eligibility_status") or ""),
                )
            )
    return tuple(out)


def _evidence_captures(
    store: rule_attestation.RuleCaptureStore,
) -> tuple[tuple[rule_attestation.RuleCapture, ...], int]:
    """Every held capture, and how many of them state an instant at all."""
    captures = store.captures()
    with_an_instant = sum(1 for capture in captures if capture.bounding_instant is not None)
    return captures, with_an_instant


def _observation_prerequisite(
    release: DeclaredRelease,
    captures: Sequence[rule_attestation.RuleCapture],
) -> dict[str, Any]:
    """Whether any held capture states an instant at or before the release.

    The test is the attestation module's own :attr:`RuleCapture.bounding_instant`, so a
    capture whose response states no instant counts for nothing here rather than being
    dated by this run's clock.
    """
    preceding = [
        capture.bounding_instant
        for capture in captures
        if capture.bounding_instant is not None
        and capture.bounding_instant <= release.scheduled_at_utc
    ]
    if preceding:
        earliest = min(preceding)
        return {
            "state": STATE_MET,
            "evidence": (
                f"{len(preceding)} of {len(captures)} held capture(s) state an instant at or "
                f"before {release.scheduled_at_utc.isoformat()}; earliest "
                f"{earliest.isoformat()}"
            ),
            "blocker": None,
            "what_it_withholds": None,
        }
    return {
        "state": STATE_UNMET,
        "evidence": (
            f"none of the {len(captures)} held capture(s) states an instant at or before "
            f"{release.scheduled_at_utc.isoformat()}"
        ),
        "blocker": "no_dated_observation_precedes_the_release_instant",
        "what_it_withholds": (
            "no rule interval can be in force across this release's window, so no edge "
            "over it is admissible and the panel stays masked"
        ),
    }


#: Prerequisites the preregistration names that no artifact on this checkout can answer.
#: Each is reported unobservable with its reason rather than as met or unmet, because
#: "we cannot read it" and "it is not there" are different facts.
UNREADABLE_PREREQUISITES: tuple[tuple[str, str], ...] = (
    (
        "point_in_time_expectation_source",
        "no expectation source is configured on this checkout, so a surprise slope is not "
        "estimable for any release in either arm; the absence is reported by "
        "ingest.expectations rather than filled with a zero",
    ),
    (
        "cross_venue_matched_instrument",
        "a matched pair requires a declared parser for the second venue's payout text, "
        "which this repository does not have; the venue's records are present, so this is "
        "a missing parser rather than a missing venue",
    ),
    (
        "observed_post_release_endpoints",
        "an endpoint count needs a built response panel for the release, which needs the "
        "rule evidence above before any row is admissible",
    ),
)

#: What a progress ledger is not, carried in the artifact so a reader of it alone knows.
LEDGER_CAVEAT = (
    "this ledger records which prerequisites are observable and what their state is; it "
    "is not evidence about any contract or release, it admits no contract, and it "
    "decides no eligibility"
)


def confirmatory_progress(
    *,
    root: str | Path | None = None,
    capture_root: str | Path | None = None,
    attest: bool = True,
) -> dict[str, Any]:
    """Report each declared release's prerequisites, read from the artifacts held.

    ``attest`` reads the held captures through the attestation module to report how many
    contracts reach an attested rule interval. It is the D1 figure and is reported as the
    module computes it rather than recounted here.
    """
    releases = declared_releases(root)
    store = rule_attestation.RuleCaptureStore(capture_root)
    captures, with_an_instant = _evidence_captures(store)

    attestation: dict[str, Any] = {
        "read": False,
        "reason": "not asked for",
    }
    if attest:
        report = rule_attestation.RuleAttestor(store).report()
        attestation = {"read": True, "reason": None, **report.totals()}

    arms: dict[str, dict[str, Any]] = {}
    for release in releases:
        arm = arms.setdefault(
            release.arm,
            {
                "cohort_id": release.cohort_id,
                "declared_releases": 0,
                "releases_whose_window_an_observation_precedes": 0,
                "releases": [],
            },
        )
        prerequisite = _observation_prerequisite(release, captures)
        if prerequisite["state"] == STATE_MET:
            arm["releases_whose_window_an_observation_precedes"] += 1
        arm["releases"].append(
            {
                **release.as_dict(),
                "prerequisites": {"observation_precedes_the_release": prerequisite},
            }
        )
        arm["declared_releases"] += 1

    out: dict[str, Any] = {
        "ledger_version": LEDGER_VERSION,
        "produced_by": "market_propagation.confirmatory",
        "caveat": LEDGER_CAVEAT,
        "cohort_config": COHORT_CONFIG,
        "capture_root": str(store.root),
        "captures_held": len(captures),
        "captures_stating_an_instant": with_an_instant,
        "captures_stating_none": len(captures) - with_an_instant,
        "rule_attestation": attestation,
        "arms": {name: dict(arm) for name, arm in sorted(arms.items())},
        "prerequisites_not_readable_from_any_artifact": [
            {"prerequisite": name, "reason": reason} for name, reason in UNREADABLE_PREREQUISITES
        ],
    }
    out["releases_total"] = sum(arm["declared_releases"] for arm in out["arms"].values())
    out["releases_whose_window_an_observation_precedes"] = sum(
        arm["releases_whose_window_an_observation_precedes"] for arm in out["arms"].values()
    )
    return out


def write_progress_ledger(payload: Mapping[str, Any], path: str | Path) -> Path:
    """Write a ledger by replacing a temporary file, so no reader sees a partial one."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n"
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(target)
    return target
