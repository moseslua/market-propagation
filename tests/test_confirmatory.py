"""The confirmatory progress ledger: prerequisites computed, not asserted."""

from __future__ import annotations

import datetime as dt
import json

import pytest
import yaml

from market_propagation import confirmatory
from market_propagation.ingest import rule_attestation

SOURCE_KIND = rule_attestation.SOURCE_KIND_LIVE_RULE_TEXT
CANDIDATE_CONTRACT = "KXFED-25JAN-T4.25"
UNRELATED_CONTRACT = "KXFED-26JAN-T4.25"
#: The listing interval the fixture gives its candidate contract. It has to cover both
#: declared release instants for the one capture the fixture writes to be a candidate
#: for both of them, which is what makes the ahead-of-one/behind-the-other test a test
#: of the capture's instant rather than of which release happened to be measured.
LISTING_OPENS = "2024-12-01T00:00:00+00:00"
LISTING_CLOSES = "2027-01-01T00:00:00+00:00"


def contract_body(ticker):
    """One contract's listing bytes, carrying its own ticker beside its payout text."""
    return (
        f"<html><body>{ticker} pays 1 when the target rate is above 4.25 percent.</body></html>"
    ).encode()


def contract_rule_text(ticker):
    return f"{ticker} pays 1 when the target rate is above 4.25 percent."


BODY = contract_body(CANDIDATE_CONTRACT)
RULE_TEXT = contract_rule_text(CANDIDATE_CONTRACT)

RETROSPECTIVE_INSTANT = dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC)
FORWARD_INSTANT = dt.datetime(2026, 10, 14, 12, 30, tzinfo=dt.UTC)
OBSERVED_AT = dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
#: The declared absorption window opens 1,800 s before the release and closes 3,600 s
#: after it. The instants below are derived rather than written, so a test that says
#: "inside the window" is inside the window the module reads.
WINDOW_PRE_EVENT_SECONDS = 1800
WINDOW_POST_EVENT_SECONDS = 3600
FORWARD_WINDOW_OPENS = FORWARD_INSTANT - dt.timedelta(seconds=WINDOW_PRE_EVENT_SECONDS)
FORWARD_WINDOW_CLOSES = FORWARD_INSTANT + dt.timedelta(seconds=WINDOW_POST_EVENT_SECONDS)
#: An instant before every declared release and before every declared window opening, so
#: a capture taken then is as early as a capture can be.
BEFORE_EVERY_WINDOW = dt.datetime(2024, 12, 1, tzinfo=dt.UTC)


def declared_markets(*contracts, opens=LISTING_OPENS, closes=LISTING_CLOSES):
    """A candidate population in the three columns candidacy is read from.

    One row per supplied contract, each with its own listing interval. The fixture's
    candidate contract is given an interval covering both declared release instants so
    the same held capture is a candidate for both releases; a contract supplied with a
    narrower interval is a candidate for neither.
    """
    return [{"ticker": ticker, "open_time": opens, "close_time": closes} for ticker in contracts]


FIXTURE_MARKETS = declared_markets(CANDIDATE_CONTRACT)


def declared_arms(tmp_path):
    """Two declared arms, one release each, in a root the module is pointed at."""
    configs = tmp_path / "configs"
    configs.mkdir(parents=True, exist_ok=True)
    (configs / "event_windows_v2.yaml").write_text(
        yaml.safe_dump(
            {
                "windows": {
                    confirmatory.PRIMARY_WINDOW_ID: {
                        "pre_event_seconds": WINDOW_PRE_EVENT_SECONDS,
                        "post_event_seconds": WINDOW_POST_EVENT_SECONDS,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
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


def write_capture(root, *, observed_at, contract_id=CANDIDATE_CONTRACT, stated_in_force_to=None):
    """One capture through the store's own public write path.

    ``stated_in_force_to`` is the end the *source* states for its own interval. The
    capture still bound by its observation instant, so ``bounding_instant`` is
    ``observed_at``; the stated end is a separate field the coverage test reads.
    """
    store = rule_attestation.RuleCaptureStore(root)
    return store.put(
        contract_body(contract_id),
        contract_id=contract_id,
        source_url="https://example.invalid/trade-api/v2/markets",
        source_kind=SOURCE_KIND,
        captured_at=OBSERVED_AT,
        content_type="text/html",
        rule_text=contract_rule_text(contract_id),
        source_observed_at=observed_at,
        stated_in_force_to=stated_in_force_to,
        source_names_contract=True,
    )


def release_states(ledger):
    """The observation prerequisite's state for each release in a ledger, by event id."""
    return {
        release["event_id"]: release["prerequisites"]["observation_precedes_the_release"]["state"]
        for arm in ledger["arms"].values()
        for release in arm["releases"]
    }


def prerequisite_for(ledger, event_id):
    """One release's observation prerequisite, by event id."""
    for arm in ledger["arms"].values():
        for release in arm["releases"]:
            if release["event_id"] == event_id:
                return release["prerequisites"]["observation_precedes_the_release"]
    raise AssertionError(f"no declared release named {event_id}")


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
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
    )
    states = release_states(ledger)
    # Observed 2026-09-17: after the 2025 instant, before the 2026-10 instant. The
    # contract's listing interval covers both releases, so the capture is a candidate
    # for both and the two outcomes are decided by the capture's own instant.
    assert states["cpi_2025_01"] == confirmatory.STATE_UNMET
    assert states["cpi_2026_10"] == confirmatory.STATE_MET
    assert ledger["releases_whose_window_an_observation_precedes"] == 1


def test_a_capture_that_states_no_instant_never_opens_a_release(tmp_path):
    """A response that states no instant is dated by nothing, this run's clock included."""
    declared_arms(tmp_path)
    write_capture(tmp_path / "rules", observed_at=None)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
    )
    assert ledger["captures_held"] == 1
    assert ledger["captures_stating_an_instant"] == 0
    assert ledger["captures_stating_none"] == 1
    # The forward release is in the future relative to every capture, yet the capture
    # states no instant, so it opens nothing.
    assert ledger["releases_whose_window_an_observation_precedes"] == 0
    assert release_states(ledger)["cpi_2026_10"] == confirmatory.STATE_UNMET


def test_no_capture_at_all_leaves_every_release_unmet_and_names_the_blocker(tmp_path):
    declared_arms(tmp_path)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
    )
    assert ledger["captures_held"] == 0
    assert ledger["releases_whose_window_an_observation_precedes"] == 0
    for arm in ledger["arms"].values():
        for release in arm["releases"]:
            prerequisite = release["prerequisites"]["observation_precedes_the_release"]
            assert prerequisite["state"] == confirmatory.STATE_UNMET
            assert prerequisite["blocker"] == "no_capture_is_held_for_any_candidate_contract"
            # The release is measured on real candidates, so its zero is a count of
            # candidates with no covering capture rather than a denominator of zero.
            assert prerequisite["candidate_contracts"] > 0
            assert prerequisite["candidates_with_a_covering_capture"] == 0
            assert prerequisite["what_it_withholds"]


def test_the_unreadable_prerequisites_are_reported_as_unobservable_with_reasons(tmp_path):
    declared_arms(tmp_path)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
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
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
    )
    path = confirmatory.write_progress_ledger(ledger, tmp_path / "out" / confirmatory.LEDGER_NAME)
    assert json.loads(path.read_text(encoding="utf-8")) == json.loads(
        json.dumps(ledger, default=str)
    )


def test_the_ledger_carries_its_own_caveat(tmp_path):
    declared_arms(tmp_path)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
    )
    assert ledger["caveat"] == confirmatory.LEDGER_CAVEAT
    assert "not evidence" in ledger["caveat"]


def test_a_capture_for_an_unrelated_contract_does_not_open_a_release(tmp_path):
    """Pins that a capture is evidence about this release only if it is for a candidate.

    The capture's instant precedes every declared window, so the old rule — which asked
    only whether *any* held capture states an instant at or before the release — reported
    this release ``met``. A capture for a contract the release is not measured on says
    nothing about it, so the release stays unmet and names that no capture is held for
    any candidate.
    """
    declared_arms(tmp_path)
    write_capture(
        tmp_path / "rules", observed_at=BEFORE_EVERY_WINDOW, contract_id=UNRELATED_CONTRACT
    )
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
    )
    # The capture is held and dated, so neither count zero is the reason it opens nothing.
    assert ledger["captures_held"] == 1
    assert ledger["captures_stating_an_instant"] == 1
    assert ledger["candidate_contracts_declared"] == 1
    assert ledger["releases_whose_window_an_observation_precedes"] == 0
    for event_id in ("cpi_2025_01", "cpi_2026_10"):
        prerequisite = prerequisite_for(ledger, event_id)
        assert prerequisite["state"] == confirmatory.STATE_UNMET
        assert prerequisite["blocker"] == "no_capture_is_held_for_any_candidate_contract"
        assert prerequisite["candidate_contracts"] > 0
        assert prerequisite["candidates_with_a_covering_capture"] == 0
        assert prerequisite["what_it_withholds"]


def test_a_capture_inside_the_window_does_not_open_the_release(tmp_path):
    """Pins that coverage is measured against the window opening, not the release.

    The declared absorption window opens 1,800 s before the release, so a capture taken
    between the window opening and the release certifies the response half and leaves the
    baseline half uncertified. It is a capture for a candidate, so the blocker says so
    rather than saying no capture is held.
    """
    declared_arms(tmp_path)
    inside = FORWARD_WINDOW_OPENS + dt.timedelta(seconds=60)
    assert inside < FORWARD_INSTANT
    write_capture(tmp_path / "rules", observed_at=inside)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
    )
    prerequisite = prerequisite_for(ledger, "cpi_2026_10")
    assert prerequisite["state"] == confirmatory.STATE_UNMET
    assert prerequisite["blocker"] == "no_candidate_capture_reaches_back_to_the_window_opening"
    assert prerequisite["candidate_contracts"] > 0
    assert prerequisite["candidates_with_a_covering_capture"] == 0
    assert prerequisite["what_it_withholds"]


def test_a_release_with_no_candidate_contract_is_unobservable(tmp_path):
    """Pins that an empty candidate population is unobservable, not unmet.

    "no contract in the declared series covers this release instant" and "a candidate
    exists and no capture covers it" are different facts, and only the second is what an
    unmet prerequisite reports.
    """
    declared_arms(tmp_path)
    write_capture(tmp_path / "rules", observed_at=BEFORE_EVERY_WINDOW)
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=[],
    )
    assert ledger["candidate_contracts_declared"] == 0
    assert ledger["candidate_universe"]["read_from"] == "supplied_by_the_caller"
    assert ledger["releases_whose_window_an_observation_precedes"] == 0
    for event_id in ("cpi_2025_01", "cpi_2026_10"):
        prerequisite = prerequisite_for(ledger, event_id)
        assert prerequisite["state"] == confirmatory.STATE_UNOBSERVABLE
        assert prerequisite["blocker"] == "release_has_no_candidate_contracts"
        assert prerequisite["candidate_contracts"] == 0
        assert prerequisite["candidates_with_a_covering_capture"] == 0
    assert ledger["arms"]["forward"]["releases_with_no_candidate_contract"] == 1


def test_a_candidate_capture_whose_stated_end_falls_inside_the_window_does_not_cover(tmp_path):
    """Pins that a rule the source states as ended before the window closes does not cover.

    The capture's own instant precedes the window opening, so the instant test alone
    would report the release covered. Its source states the rule text was in force only
    until an instant inside the window, and a rule no longer in force is not a rule in
    force across the window.
    """
    declared_arms(tmp_path)
    states_end = FORWARD_WINDOW_CLOSES - dt.timedelta(seconds=60)
    write_capture(
        tmp_path / "rules",
        observed_at=BEFORE_EVERY_WINDOW,
        stated_in_force_to=states_end,
    )
    ledger = confirmatory.confirmatory_progress(
        root=tmp_path,
        capture_root=tmp_path / "rules",
        attest=False,
        candidate_markets=FIXTURE_MARKETS,
    )
    assert ledger["releases_whose_window_an_observation_precedes"] == 1
    # The stated end falls after the retrospective window closes, so that release is
    # covered; only the release whose window the end falls inside stays unmet.
    assert release_states(ledger)["cpi_2025_01"] == confirmatory.STATE_MET
    prerequisite = prerequisite_for(ledger, "cpi_2026_10")
    assert prerequisite["state"] == confirmatory.STATE_UNMET
    assert prerequisite["blocker"] == "no_candidate_capture_reaches_back_to_the_window_opening"
    assert prerequisite["candidate_contracts"] > 0
    assert prerequisite["candidates_with_a_covering_capture"] == 0
    assert prerequisite["what_it_withholds"]
