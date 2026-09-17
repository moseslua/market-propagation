"""The protocol freeze: sealing the declarations, and catching a checkout that moved."""

from __future__ import annotations

import pytest

from market_propagation.protocol_freeze import (
    DECLARATION_FILES,
    ESTIMAND_MODULES,
    FREEZE_NAME,
    ProtocolDriftError,
    assert_protocol_freeze,
    build_protocol_freeze,
    protocol_inputs,
    read_protocol_freeze,
    verify_protocol_freeze,
    write_protocol_freeze,
)

T0 = "2026-09-17T00:00:00Z"
#: A file the freeze covers and this test never writes to.
COVERED = "configs/study_v2.yaml"
ZERO = "0" * 64


def test_a_freeze_covers_every_declared_input_and_finds_none_absent():
    manifest = build_protocol_freeze(frozen_at=T0)
    assert set(manifest["files"]) == set(protocol_inputs())
    assert manifest["files_covered"] == len(DECLARATION_FILES) + len(ESTIMAND_MODULES)
    assert manifest["files_absent_at_freeze"] == []


def test_the_checkout_verifies_against_its_own_freeze():
    report = verify_protocol_freeze(build_protocol_freeze(frozen_at=T0))
    assert report["verified"] is True
    assert report["drifted"] == []
    assert report["missing"] == []
    assert report["expected_but_not_covered"] == []
    assert report["matches_freeze_hash"] is True


def test_changed_bytes_are_reported_by_name_and_not_merely_counted():
    manifest = build_protocol_freeze(frozen_at=T0)
    manifest["files"] = {**manifest["files"], COVERED: ZERO}
    report = verify_protocol_freeze(manifest)
    assert report["verified"] is False
    assert [entry["name"] for entry in report["drifted"]] == [COVERED]
    assert report["drifted"][0]["frozen"] == ZERO


def test_a_drifted_freeze_stops_a_run_by_name():
    manifest = build_protocol_freeze(frozen_at=T0)
    manifest["files"] = {**manifest["files"], COVERED: ZERO}
    with pytest.raises(ProtocolDriftError, match=r"study_v2\.yaml"):
        assert_protocol_freeze(manifest)


def test_a_file_that_vanished_since_the_freeze_is_reported_missing(tmp_path):
    manifest = build_protocol_freeze(frozen_at=T0)
    manifest["files"] = {**manifest["files"], "configs/not-here.yaml": ZERO}
    report = verify_protocol_freeze(manifest)
    assert report["verified"] is False
    assert report["missing"] == ["configs/not-here.yaml"]


def test_an_absolutely_empty_checkout_still_records_what_was_expected(tmp_path):
    """A freeze over a checkout holding none of the inputs is a freeze, not an empty one."""
    manifest = build_protocol_freeze(frozen_at=T0, root=tmp_path)
    assert manifest["files_absent_at_freeze"] == sorted(manifest["files"])
    assert all(digest is None for digest in manifest["files"].values())
    report = verify_protocol_freeze(manifest, root=tmp_path)
    assert report["verified"] is False
    assert report["missing"] == sorted(manifest["files"])


def test_a_freeze_instant_without_a_zone_is_refused():
    """A freeze with no stated instant cannot be told from one taken after a result."""
    with pytest.raises(ValueError, match="no offset"):
        build_protocol_freeze(frozen_at="2026-09-17T00:00:00")


def test_two_spellings_of_one_instant_normalize_together():
    as_utc = build_protocol_freeze(frozen_at="2026-09-17T13:30:00+00:00")
    as_eastern = build_protocol_freeze(frozen_at="2026-09-17T09:30:00-04:00")
    assert as_utc["frozen_at"] == as_eastern["frozen_at"] == "2026-09-17T13:30:00Z"
    # The content identity is over the covered bytes, so the spelling of T0 cannot
    # change it; only the bytes can.
    assert as_utc["freeze_hash"] == as_eastern["freeze_hash"]


def test_the_stopping_rule_is_carried_and_names_both_arms():
    rule = build_protocol_freeze(frozen_at=T0)["stopping_rule"]
    assert rule["pooling"] == "prohibited"
    assert rule["each_arm_is_its_own_denominator"] is True
    assert rule["retrospective_arm"]["extension"] == "none"
    assert "next_scheduled_release_per_family" in rule["forward_arm"]["extension"]
    assert rule["forward_arm"]["a_release_is_never_added_or_removed"] is True


def test_a_written_freeze_reads_back_and_still_verifies(tmp_path):
    manifest = build_protocol_freeze(frozen_at=T0)
    path = write_protocol_freeze(manifest, tmp_path / FREEZE_NAME)
    held = read_protocol_freeze(path)
    assert held["freeze_hash"] == manifest["freeze_hash"]
    assert verify_protocol_freeze(held)["verified"] is True


def test_a_file_that_is_not_a_freeze_is_refused(tmp_path):
    plain = tmp_path / "plain.json"
    plain.write_text('{"hello": "world"}', encoding="utf-8")
    with pytest.raises(ProtocolDriftError, match="files"):
        read_protocol_freeze(plain)


def test_a_freeze_that_states_no_instant_is_refused(tmp_path):
    timeless = tmp_path / "timeless.json"
    timeless.write_text('{"files": {}}', encoding="utf-8")
    with pytest.raises(ProtocolDriftError, match="frozen_at"):
        read_protocol_freeze(timeless)


def test_the_cli_default_freeze_path_matches_the_package_constant():
    """The CLI keeps no second copy of the name without this pinning it."""
    from market_propagation.cli import DEFAULT_FREEZE_PATH

    assert DEFAULT_FREEZE_PATH == FREEZE_NAME
