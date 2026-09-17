"""The confirmatory progress ledger: how far the study is from a confirmatory sample.

Every prerequisite the preregistration names for a confirmatory claim is reported per
declared release, computed from the artifacts on disk rather than asserted in prose, so
the ledger cannot drift from what is actually held. A prerequisite that no artifact can
answer is reported ``unobservable`` with its reason — never as met, and never as unmet,
because those are different facts.

The one prerequisite this ledger derives end to end is the **observation ahead of the
release**: whether a held capture covers the contracts *this release is measured on*, and
whether its interval reaches back past the opening of the release's declared window. A
capture certifies a window that opens at or after the instant its own serving system
states, so a capture taken after a release cannot certify that release however good the
capture is. That is the D2 gate, and it is the quantity the capture cadence changes.

Both halves are load-bearing, and each one alone is a defect. A capture for an unrelated
contract says nothing about this release's candidates, and a capture taken inside the
window says nothing about its baseline half: the declared absorption window opens 1,800
seconds *before* the release, so the requirement is stated against the window's own
opening instant rather than against the release instant.

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

#: The declared event windows. The window a rule interval has to span is read from here
#: rather than written as a constant, because a window whose pre-event period moved would
#: otherwise silently change which captures count as covering it.
WINDOWS_CONFIG = "configs/event_windows_v2.yaml"

#: The window admissible rows are measured over. It is the absorption window, which is
#: the primary response window, and it opens *before* the release: a rule interval that
#: starts between the window's opening and the release leaves the baseline half
#: uncertified.
PRIMARY_WINDOW_ID = "absorption"

#: The columns candidacy is read from, projected identically from every declared layer.
#: It is the panel's own selection set, so a candidate here and a candidate there cannot
#: be decided from different fields.
CANDIDATE_COLUMNS: tuple[str, ...] = ("ticker", "open_time", "close_time")

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


def _instant_or_none(value: Any) -> dt.datetime | None:
    """One recorded instant, or ``None`` when the record states none or states nonsense.

    A listing interval with no readable end is not an interval, and a candidacy test that
    read a missing instant as the epoch would admit every contract in the universe.
    """
    text = str(value or "").strip()
    if not text:
        return None
    try:
        moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        return None
    return moment.astimezone(dt.UTC)


def declared_window(
    release: DeclaredRelease, *, root: str | Path | None = None
) -> tuple[dt.datetime, dt.datetime]:
    """The declared analysis window for one release, as ``(opens, closes)``."""
    base = Path(root) if root is not None else _repo_root()
    payload = yaml.safe_load((base / WINDOWS_CONFIG).read_text(encoding="utf-8"))
    windows = (payload or {}).get("windows") or {}
    window = windows.get(PRIMARY_WINDOW_ID)
    if not isinstance(window, Mapping):
        raise ValueError(
            f"{WINDOWS_CONFIG} declares no windows.{PRIMARY_WINDOW_ID}; the ledger measures "
            "rule coverage against a declared window and keeps no fallback of its own"
        )
    pre = window.get("pre_event_seconds")
    post = window.get("post_event_seconds")
    if (
        isinstance(pre, bool)
        or isinstance(post, bool)
        or not isinstance(pre, int)
        or not isinstance(post, int)
    ):
        raise ValueError(
            f"{WINDOWS_CONFIG} windows.{PRIMARY_WINDOW_ID} declares no integer "
            "pre_event_seconds/post_event_seconds"
        )
    return (
        release.scheduled_at_utc - dt.timedelta(seconds=pre),
        release.scheduled_at_utc + dt.timedelta(seconds=post),
    )


def declared_candidate_markets(
    root: str | Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Every declared-series contract with the listing interval candidacy is read from.

    Read through the declared union of observation paths, exactly as the study panel
    reads it, so this ledger's candidate rule and the panel's cannot drift apart: a
    contract the venue listed and the archive omits is still a candidate here.
    """
    from .ingest.kalshi_universe import union_market_rows

    base = Path(root) if root is not None else _repo_root()
    payload = yaml.safe_load((base / COHORT_CONFIG).read_text(encoding="utf-8"))
    series = payload.get("policy_series") if isinstance(payload, Mapping) else None
    if not isinstance(series, Sequence) or isinstance(series, (str, bytes)) or not series:
        raise ValueError(
            f"{COHORT_CONFIG} declares no policy_series; the candidate population is "
            "undefined without it"
        )
    return union_market_rows(sorted({str(name) for name in series}), columns=CANDIDATE_COLUMNS)


def candidate_contracts_for_release(
    release: DeclaredRelease, markets: Sequence[Mapping[str, Any]]
) -> tuple[str, ...]:
    """The contract ids this release is measured on, from listing intervals alone.

    The rule is the study panel's own: a contract is a candidate when its recorded
    listing interval covers the release instant. Activity after the release never enters
    it, because a universe chosen by what traded afterwards is the selection the plan
    prohibits.
    """
    out: list[str] = []
    for market in markets:
        opened = _instant_or_none(market.get("open_time"))
        closed = _instant_or_none(market.get("close_time"))
        if opened is None or closed is None:
            continue
        if opened <= release.scheduled_at_utc < closed:
            out.append(str(market["ticker"]))
    return tuple(sorted(set(out)))


def _observation_prerequisite(
    release: DeclaredRelease,
    captures: Sequence[rule_attestation.RuleCapture],
    *,
    candidate_contracts: Sequence[str],
    window_opens: dt.datetime,
    window_closes: dt.datetime,
) -> dict[str, Any]:
    """Whether a held capture covers this release's own candidates across its window.

    Two things have to hold together, and checking either one alone reports a readiness
    the evidence does not support:

    * The capture has to be for a contract this release is measured on. A capture for an
      unrelated contract is no evidence about this release's window at all, however early
      its instant is.
    * The capture's interval has to reach back to the window's opening, and not merely to
      the release. The declared window opens before the release, so a capture taken inside
      that gap covers the response half and leaves the baseline half uncertified.

    The test is the attestation module's own :attr:`RuleCapture.bounding_instant`, so a
    capture whose response states no instant counts for nothing here rather than being
    dated by this run's clock.
    """
    candidates = {str(name) for name in candidate_contracts}
    if not candidates:
        return {
            "state": STATE_UNOBSERVABLE,
            "evidence": (
                f"no declared-series contract's listing interval covers "
                f"{release.scheduled_at_utc.isoformat()}, so this release has no candidates "
                "and coverage over them cannot be read"
            ),
            "blocker": "release_has_no_candidate_contracts",
            "what_it_withholds": (
                "with no candidates there is no pair to admit or refuse, so the release "
                "contributes a denominator of zero rather than a measured row"
            ),
            "candidate_contracts": 0,
            "candidates_with_a_covering_capture": 0,
        }

    for_candidates = [
        capture
        for capture in captures
        if capture.bounding_instant is not None and str(capture.contract_id) in candidates
    ]
    covering: dict[str, dt.datetime] = {}
    for capture in for_candidates:
        instant = capture.bounding_instant
        assert instant is not None
        closes = capture.stated_in_force_to
        # An interval the venue stated as ended before this window closes cannot cover
        # it: a rule no longer in force is not a rule in force across the window.
        if closes is not None and closes < window_closes:
            continue
        if instant <= window_opens:
            contract_id = str(capture.contract_id)
            earliest = covering.get(contract_id)
            covering[contract_id] = instant if earliest is None else min(earliest, instant)

    if covering:
        earliest = min(covering.values())
        return {
            "state": STATE_MET,
            "evidence": (
                f"{len(covering)} of {len(candidates)} candidate contract(s) carry a capture "
                f"opening at or before the window opening {window_opens.isoformat()} "
                f"(release {release.scheduled_at_utc.isoformat()}, window closes "
                f"{window_closes.isoformat()}); earliest {earliest.isoformat()}"
            ),
            "blocker": None,
            "what_it_withholds": None,
            "candidate_contracts": len(candidates),
            "candidates_with_a_covering_capture": len(covering),
        }
    return {
        "state": STATE_UNMET,
        "evidence": (
            f"none of {len(candidates)} candidate contract(s) for "
            f"{release.scheduled_at_utc.isoformat()} carries a capture opening at or before "
            f"the window opening {window_opens.isoformat()}; "
            f"{len(for_candidates)} capture(s) are held for a candidate contract"
        ),
        "blocker": (
            "no_capture_is_held_for_any_candidate_contract"
            if not for_candidates
            else "no_candidate_capture_reaches_back_to_the_window_opening"
        ),
        "what_it_withholds": (
            "no rule interval is in force across this release's whole window, so no edge "
            "over it is admissible and the panel stays masked"
        ),
        "candidate_contracts": len(candidates),
        "candidates_with_a_covering_capture": 0,
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
        "the second venue's payout grammar is declared and reads 63 of its 229 candidate "
        "records with every component observed, but no pair grades EXACT: all 23,751 pairs "
        "of two readable contracts are refused because the first venue's own market record "
        "publishes no reference period and no settlement criterion, so the matched "
        "instrument waits on the same rule evidence D1 needs rather than on the second "
        "venue",
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
    candidate_markets: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Report each declared release's prerequisites, read from the artifacts held.

    ``attest`` reads the held captures through the attestation module to report how many
    contracts reach an attested rule interval. It is the D1 figure and is reported as the
    module computes it rather than recounted here.

    ``candidate_markets`` is the contract population candidacy is read from. It defaults
    to the declared union of observation paths, and a caller may supply one instead, which
    is how the candidacy rule is exercised without standing up a markets layer.
    """
    releases = declared_releases(root)
    store = rule_attestation.RuleCaptureStore(capture_root)
    captures, with_an_instant = _evidence_captures(store)
    # The candidate population is resolved once and sliced per release, so every release
    # is measured against the same universe read rather than one re-derived per release.
    if candidate_markets is None:
        markets, population = declared_candidate_markets(root)
    else:
        markets = list(candidate_markets)
        population = {
            "read_from": "supplied_by_the_caller",
            "contracts_supplied": len(markets),
        }

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
                "releases_with_no_candidate_contract": 0,
                "candidate_contracts_declared": 0,
                "candidates_with_a_covering_capture": 0,
                "releases": [],
            },
        )
        candidates = candidate_contracts_for_release(release, markets)
        window_opens, window_closes = declared_window(release, root=root)
        prerequisite = _observation_prerequisite(
            release,
            captures,
            candidate_contracts=candidates,
            window_opens=window_opens,
            window_closes=window_closes,
        )
        if prerequisite["state"] == STATE_MET:
            arm["releases_whose_window_an_observation_precedes"] += 1
        if prerequisite["state"] == STATE_UNOBSERVABLE:
            arm["releases_with_no_candidate_contract"] += 1
        arm["candidates_with_a_covering_capture"] += prerequisite[
            "candidates_with_a_covering_capture"
        ]
        arm["candidate_contracts_declared"] += prerequisite["candidate_contracts"]
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
        "candidate_universe": population,
        "candidate_contracts_declared": len(markets),
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
