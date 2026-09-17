"""The confirmatory progress ledger: prerequisites computed, not asserted."""

from __future__ import annotations

import datetime as dt
import json

import pytest
import yaml

from market_propagation import confirmatory
from market_propagation.ingest import rule_attestation

SOURCE_KIND = "dated_observation_of_the_live_rule_text"
BODY = b"<html><body>KXFED-25JAN-T4.25 pays 1 when the target rate is above 4.25 percent.</body></html>"
RULE_TEXT = "KXFED-25JAN-T4.25 pays 1 when the target rate is above 4.25 percent."

RETROSPECTIVE_INSTANT = dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC)
FORWARD_INSTANT = dt.datetime(2026, 10, 14, 12, 30, tzinfo=dt.UTC)
OBSERVED_AT = dt.datetime(2026, 9, 17, tzinfo=dt.UTC)


def declared_arms(tmp_path):
    """Two declared arms, one release each, in a root the module is pointed at."""
    configs = tmp_path / "configs"
    configs.mkdir(parents=True, exist_ok=True)
    (configs / "cohort_v2.yaml").write_text(
        yaml.safe_dump(
            {
                "arms": {
                    "members": [
                        {
                            "arm": "retrospective",
                            "file": "configs/cohort_a.yaml",
                            "cohort_id": "core_test",
                        },
                        {
                            "arm": "forward",
                            "file": "configs/cohort_b.yaml",
                            "cohort_id": "forward_test",
                        },
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    for name, event_id, instant in (
        ("cohort_a.yaml", "cpi_2025_01", RETROSPECTIVE_INSTANT),
        ("cohort_b.yaml", "cpi_2026_10", FORWARD_INSTANT),
    ):
        (configs / name).write_text(
            yaml.safe_dump(
                {
                    "events": [
                        {
                            "event_id": event_id,
                            "family": "cpi",
                            "scheduled_at_utc": instant.isoformat(),
                            "eligibility_status": "declared",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
    return tmp_path


def write_capture(root, *, observed_at):
    """One capture through the store's own public write path."""
    store = rule_attestation.RuleCaptureStore(root)
    return store.put(
        BODY,
        contract_id="KXFED-25JAN-T4.25",
        source_url="https://example.invalid/trade-api/v2/markets",
        source_kind=SOURCE_KIND,
        captured_at=OBSERVED_AT,
        content_type="text/html",
        rule_text=RULE_TEXT,
        source_observed_at=observed_at,
        source_names_contract=True,
    )


def test_every_release_of_every_declared_arm_is_tracked(tmp_path):
    releases = confirmatory.declared_releases(root=declared_arms(tmp_path))
    assert [(r.arm, r.cohort_id, r.event_id) for r in releases] == [
        ("retrospective", "core_test", "cpi_2025_01"),
        ("forward", "forward_test", "cpi_2026_10"),
    ]
    assert all(release.scheduled_at_utc.tzinfo is not None for release in releases)


def test_a_capture_that_precedes_a_release_opens_it_and_one_that_follows_does_not(tmp_path):
    """The instant test itself: the same capture is ahead of one release and behind the other."""
    declared_arms(tmp_path)
    write_capture(tmp_path / "rules", observed_at=OBSERVED_AT)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path, capture_root=tmp_path / "rules", attest=False
    )
    states = {
        release["event_id"]: release["prerequisites"]["observation_precedes_the_release"]["state"]
        for arm in ledger["arms"].values()
        for release in arm["releases"]
    }
    # Observed 2026-09-17: after the 2025 instant, before the 2026-10 instant.
    assert states["cpi_2025_01"] == confirmatory.STATE_UNMET
    assert states["cpi_2026_10"] == confirmatory.STATE_MET
    assert ledger["releases_whose_window_an_observation_precedes"] == 1


def test_a_capture_that_states_no_instant_never_opens_a_release(tmp_path):
    """A response that states no instant is dated by nothing, this run's clock included."""
    declared_arms(tmp_path)
    write_capture(tmp_path / "rules", observed_at=None)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path, capture_root=tmp_path / "rules", attest=False
    )
    assert ledger["captures_held"] == 1
    assert ledger["captures_stating_an_instant"] == 0
    assert ledger["captures_stating_none"] == 1
    # The forward release is in the future relative to every capture, yet the capture
    # states no instant, so it opens nothing.
    assert ledger["releases_whose_window_an_observation_precedes"] == 0


def test_no_capture_at_all_leaves_every_release_unmet_and_names_the_blocker(tmp_path):
    declared_arms(tmp_path)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path, capture_root=tmp_path / "rules", attest=False
    )
    assert ledger["captures_held"] == 0
    assert ledger["releases_whose_window_an_observation_precedes"] == 0
    for arm in ledger["arms"].values():
        for release in arm["releases"]:
            prerequisite = release["prerequisites"]["observation_precedes_the_release"]
            assert prerequisite["state"] == confirmatory.STATE_UNMET
            assert prerequisite["blocker"] == "no_dated_observation_precedes_the_release_instant"
            assert prerequisite["what_it_withholds"]


def test_the_unreadable_prerequisites_are_reported_as_unobservable_with_reasons(tmp_path):
    declared_arms(tmp_path)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path, capture_root=tmp_path / "rules", attest=False
    )
    named = {
        item["prerequisite"] for item in ledger["prerequisites_not_readable_from_any_artifact"]
    }
    assert named == {
        "point_in_time_expectation_source",
        "cross_venue_matched_instrument",
        "observed_post_release_endpoints",
    }
    assert all(item["reason"] for item in ledger["prerequisites_not_readable_from_any_artifact"])


def test_an_arm_that_declares_no_releases_is_refused_rather_than_silently_empty(tmp_path):
    declared_arms(tmp_path)
    (tmp_path / "configs" / "cohort_b.yaml").write_text(
        yaml.safe_dump({"events": None}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="declares no events"):
        confirmatory.declared_releases(root=tmp_path)


def test_a_cohort_that_declares_no_arms_is_refused(tmp_path):
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "cohort_v2.yaml").write_text(
        yaml.safe_dump({"arms": {}}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match=r"arms\.members"):
        confirmatory.declared_releases(root=tmp_path)


def test_a_written_ledger_reads_back_as_written(tmp_path):
    declared_arms(tmp_path)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path, capture_root=tmp_path / "rules", attest=False
    )
    path = confirmatory.write_progress_ledger(ledger, tmp_path / "out" / confirmatory.LEDGER_NAME)
    assert json.loads(path.read_text(encoding="utf-8")) == json.loads(
        json.dumps(ledger, default=str)
    )


def test_the_ledger_carries_its_own_caveat(tmp_path):
    declared_arms(tmp_path)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path, capture_root=tmp_path / "rules", attest=False
    )
    assert ledger["caveat"] == confirmatory.LEDGER_CAVEAT
    assert "not evidence" in ledger["caveat"]
