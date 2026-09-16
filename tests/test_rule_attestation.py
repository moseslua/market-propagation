"""Rule capture and attestation, against the record the graph actually consumes.

The gate this pipeline exists to open is blocked on one input, and the failure mode
these tests defend against is the one that would silently close it wrongly: deriving
an in-force interval from a field that dates something else. Every refused field is
therefore asserted to produce **its own** named reason, not merely to be refused — a
single ``inadmissible_bound`` shared by six different mistakes would report that
something was wrong without saying what, and those six mistakes are exactly the ones
that look right when you are trying to unblock a study.

The distinction the fixtures keep visible is the crux of the whole module. Every
capture carries two instants: ``fetched_at``, when *this run* archived it, and
``source_observed_at``, when the *source* records the text as live. Only the second
can bound an interval. Most tests set them far apart on purpose, because a run that
confused them would certify an interval from its own wall clock.

Attestation is checked through the consumer rather than around it. A record that
attests is exercised with
:meth:`market_propagation.ingest.audit.RuleVersionEvidence.applies_to`, the in-force
test the graph asks, at an instant inside the interval and outside it on each side. A
record that satisfied this module's reading of its own interval but failed the
consumer's would be an interval nobody can use.

Fixtures are synthetic and no capture reaches the network: bytes are archived through
the real :class:`~market_propagation.storage.RawStore`, and the one live-fetch path
runs through ``httpx.MockTransport``, the same offline boundary the ingest package's
own tests use.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import pathlib
import sys
from typing import Any

import httpx
import pytest
import yaml

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from market_propagation.ingest.audit import (
    REQUIRED_RULE_VERSION_EVIDENCE_FIELDS,
    RuleVersionEvidence,
)
from market_propagation.ingest.rule_attestation import (
    CONFIG_PATH,
    REFUSAL_AMBIGUOUS_STATED_INTERVALS,
    REFUSAL_CAPTURE_DIGEST_MISMATCH,
    REFUSAL_CAPTURE_EVIDENCE_MISSING,
    REFUSAL_EMPTY_INTERVAL,
    REFUSAL_INADMISSIBLE_SOURCE_KIND,
    REFUSAL_NO_BOUNDING_SOURCE,
    REFUSAL_NO_CAPTURES_HELD,
    REFUSAL_RULE_TEXT_ABSENT,
    REFUSAL_SETTLEMENT_SEMANTICS_UNSTATED,
    REFUSAL_SOURCE_NAMES_NO_CONTRACT,
    REFUSAL_UNDECLARED_BOUND_FIELD,
    REFUSALS,
    REFUSED_BOUND_REASONS,
    RuleAttestationReport,
    RuleAttestor,
    RuleCaptureStore,
    load_attestation_settings,
    visible_text,
)
from market_propagation.ingest.rule_attestation import (
    RuleVersionEvidence as ProducedRecord,
)
from market_propagation.ingest.transport import HttpTransport, RetryPolicy

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

SNAPSHOT = "archived_snapshot_of_the_rule_text"
FILING = "regulatory_self_certification_filing"
NOTICE = "venue_rule_change_notice"
CURRENT_TEXT = "current_venue_rule_text"

#: The meeting whose settled records the venue was measured rewriting. The
#: ``FED-25MAY`` markets closed 2025-05-07 and carry ``updated_time`` 2026-02-19,
#: roughly nine and a half months later, which is why ``updated_time`` cannot bound
#: a vintage.
SETTLEMENT = dt.datetime(2025, 5, 7, 17, 55, tzinfo=dt.UTC)
REWRITE = dt.datetime(2026, 2, 19, 8, 48, 16, tzinfo=dt.UTC)

#: When a run doing the work today would have fetched.
TODAY = dt.datetime(2026, 9, 14, 10, 0, tzinfo=dt.UTC)

RULE_TEXT = (
    "If the upper bound of the target federal funds rate published on the Federal "
    "Reserve's official website is greater than 4.25% following the Federal Reserve's "
    "May 7, 2025 meeting, then the market resolves to Yes."
)
SEMANTICS = "upper_bound_federal_funds_target_rate greater than 4.25 percent"
FED_25MAY = "FED-25MAY-T4.25"
FILING_URL = "https://www.cftc.gov/filings/ptc/25/03/ptc03172529868.pdf"


def at(year: int, month: int, day: int, hour: int = 0) -> dt.datetime:
    return dt.datetime(year, month, day, hour, tzinfo=dt.UTC)


def body_for(rule_text: str) -> bytes:
    return json.dumps({"rules_primary": rule_text}).encode("utf-8")


def archive(
    store: RuleCaptureStore,
    *,
    contract_id: str = FED_25MAY,
    payload: bytes | None = None,
    fetched_at: dt.datetime = TODAY,
    source_observed_at: Any = None,
    source_kind: str = SNAPSHOT,
    source_url: str = "https://web.archive.org/web/20250115085107/https://example.test/rules",
    rule_text: str | None = None,
    settlement_semantics: str | None = SEMANTICS,
    stated_in_force_from: Any = None,
    stated_in_force_to: Any = None,
    source_names_contract: bool = True,
    offered_bound_fields: dict[str, Any] | None = None,
) -> Any:
    """Archive one synthetic capture through the real store.

    ``fetched_at`` is when this run took the capture and ``source_observed_at`` is the
    instant the source itself records; they are separate arguments so no test can
    accidentally let the first stand in for the second.
    """
    body = body_for(RULE_TEXT) if payload is None else payload
    return store.put(
        body,
        contract_id=contract_id,
        source_url=source_url,
        source_kind=source_kind,
        captured_at=fetched_at,
        content_type="application/json",
        rule_text=rule_text if rule_text is not None else body.decode("utf-8"),
        settlement_semantics=settlement_semantics,
        source_observed_at=source_observed_at,
        stated_in_force_from=stated_in_force_from,
        stated_in_force_to=stated_in_force_to,
        source_names_contract=source_names_contract,
        offered_bound_fields=offered_bound_fields,
    )


def filing(
    store: RuleCaptureStore,
    *,
    contract_id: str = FED_25MAY,
    opening: dt.datetime,
    closing: Any = None,
    fetched_at: dt.datetime = TODAY,
    source_names_contract: bool = True,
    source_url: str = FILING_URL,
) -> Any:
    """One capture of a document that states its own in-force interval."""
    return archive(
        store,
        contract_id=contract_id,
        fetched_at=fetched_at,
        source_kind=FILING,
        source_url=source_url,
        stated_in_force_from=opening,
        stated_in_force_to=closing,
        source_names_contract=source_names_contract,
    )


@pytest.fixture
def store(tmp_path: pathlib.Path) -> RuleCaptureStore:
    """A store rooted outside the checkout, so no captured byte enters the repo."""
    return RuleCaptureStore(tmp_path / "rules")


@pytest.fixture
def attestor(store: RuleCaptureStore) -> RuleAttestor:
    return RuleAttestor(store)


def unattested(outcome: Any, *reasons: str) -> None:
    """Assert the refusals include ``reasons`` and that no interval was produced.

    A contract refused for a bad capture is also a contract with no dated source
    bounding its rule text, so :data:`REFUSAL_NO_BOUNDING_SOURCE` accompanies every
    other refusal. These tests assert the *specific* reason is present and that the
    attestation is empty, rather than asserting an exact set that would encode that
    coincidence into the expectations.
    """
    assert outcome.records_attested == 0
    assert outcome.evidence == ()
    assert outcome.attested is False
    present = set(outcome.refusals_by_reason)
    missing = [reason for reason in reasons if reason not in present]
    assert not missing, f"{missing} not among {sorted(present)}"
    assert present <= {*reasons, REFUSAL_NO_BOUNDING_SOURCE}


# ---------------------------------------------------------------------------
# The record produced is the record consumed.
# ---------------------------------------------------------------------------


def test_the_module_imports_the_record_rather_than_redefining_it() -> None:
    assert ProducedRecord is RuleVersionEvidence


def test_the_configured_record_requirement_is_the_consumers_own_field_set() -> None:
    """The declared requirement is the consumer's field set, not a subset of it.

    ``in_force_to`` is in the graph's declared requirement and is optional in the
    audit's own minimum, which is why the two are compared as sets rather than by
    identity: an open interval is *stated* as open, so the field is required and its
    null is meaningful.
    """
    settings = load_attestation_settings()
    assert sorted(settings.required_record_fields) == sorted(
        field.name for field in dataclasses.fields(RuleVersionEvidence)
    )
    assert set(REQUIRED_RULE_VERSION_EVIDENCE_FIELDS) <= set(settings.required_record_fields)
    assert "in_force_to" in settings.required_record_fields


def test_the_configuration_states_that_current_rule_text_cannot_attest_its_past() -> None:
    document = yaml.safe_load((REPO_ROOT / CONFIG_PATH).read_text(encoding="utf-8"))
    assert document["dated_sources"]["venue_rule_text_cannot_attest_its_own_past_version"] is True
    assert CURRENT_TEXT in {entry["id"] for entry in document["dated_sources"]["inadmissible"]}
    assert document["capture"]["writes_captured_data_into_the_repository"] is False


def test_a_configuration_that_dropped_the_declaration_is_refused(tmp_path: pathlib.Path) -> None:
    document = yaml.safe_load((REPO_ROOT / CONFIG_PATH).read_text(encoding="utf-8"))
    document["dated_sources"]["venue_rule_text_cannot_attest_its_own_past_version"] = False
    path = tmp_path / "rule_attestation_v1.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="cannot_attest_its_own_past_version"):
        load_attestation_settings(path)


def test_a_configuration_whose_record_fields_drift_from_the_consumer_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    document = yaml.safe_load((REPO_ROOT / CONFIG_PATH).read_text(encoding="utf-8"))
    document["record"]["required_record_fields"] = [
        name for name in document["record"]["required_record_fields"] if name != "observed_at"
    ]
    path = tmp_path / "rule_attestation_v1.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="disagrees with"):
        load_attestation_settings(path)


def test_a_configuration_that_moved_a_refusal_reason_is_refused(tmp_path: pathlib.Path) -> None:
    document = yaml.safe_load((REPO_ROOT / CONFIG_PATH).read_text(encoding="utf-8"))
    for entry in document["refused_fields"]:
        if entry["field"] == "updated_time":
            entry["reason"] = "recorded_open_time_is_not_a_rule_bound"
    path = tmp_path / "rule_attestation_v1.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="refused-field mapping"):
        load_attestation_settings(path)


# ---------------------------------------------------------------------------
# Refused fields: each its own named reason, and none of them ever a bound.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field", sorted(REFUSED_BOUND_REASONS))
def test_each_refused_field_produces_its_own_named_reason(
    attestor: RuleAttestor, store: RuleCaptureStore, field: str
) -> None:
    """Six inadmissible bounds, six distinct reasons, and no interval from any."""
    archive(
        store,
        fetched_at=REWRITE,
        offered_bound_fields={field: "2025-05-07T17:55:00Z"},
    )
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 0
    assert outcome.evidence == ()
    assert REFUSED_BOUND_REASONS[field] in outcome.refusals_by_reason
    assert outcome.primary_reason == REFUSED_BOUND_REASONS[field]


def test_the_refused_field_reasons_are_pairwise_distinct() -> None:
    """One shared code would report that something was wrong without saying what."""
    assert len(set(REFUSED_BOUND_REASONS.values())) == len(REFUSED_BOUND_REASONS)
    assert len(REFUSED_BOUND_REASONS) == 6


def test_every_declared_refusal_reason_is_a_declared_code(attestor: RuleAttestor) -> None:
    for field in REFUSED_BOUND_REASONS:
        assert attestor.refused_bound_field(FED_25MAY, field).reason in REFUSALS
    assert REFUSAL_UNDECLARED_BOUND_FIELD in REFUSALS


def test_a_refusal_names_the_field_and_the_measured_reason_it_is_refused(
    attestor: RuleAttestor,
) -> None:
    refusal = attestor.refused_bound_field(FED_25MAY, "updated_time")
    assert refusal.field == "updated_time"
    assert refusal.reason == "recorded_updated_time_is_not_a_rule_bound"
    # The measured counterexample is what makes the refusal concrete.
    assert "rewrite" in refusal.detail
    assert "2026-02-19" in refusal.detail


def test_an_undeclared_field_is_refused_rather_than_read_as_a_declared_one(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    archive(
        store,
        fetched_at=REWRITE,
        offered_bound_fields={"expiration_time": "2025-05-07T19:05:00Z"},
    )
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 0
    assert REFUSAL_UNDECLARED_BOUND_FIELD in outcome.refusals_by_reason
    assert outcome.refusals[0].field == "expiration_time"


def test_the_venue_rewrite_stamp_cannot_close_an_interval_that_settlement_opened(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """The measured counterexample, as two refusals rather than one interval.

    ``FED-25MAY`` closed 2025-05-07 and carries ``updated_time`` 2026-02-19. Reading
    those as the two bounds would certify a nine-and-a-half-month interval out of a
    settlement instant and a rewrite stamp, which is the inference this pipeline
    exists to refuse.
    """
    archive(
        store,
        fetched_at=REWRITE,
        offered_bound_fields={
            "open_time": SETTLEMENT.isoformat(),
            "updated_time": REWRITE.isoformat(),
        },
    )
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 0
    assert outcome.refusals_by_reason == {
        REFUSED_BOUND_REASONS["open_time"]: 1,
        REFUSED_BOUND_REASONS["updated_time"]: 1,
        # The contract also genuinely has no bounding source, and that is reported
        # alongside the two refused fields rather than instead of them.
        REFUSAL_NO_BOUNDING_SOURCE: 1,
    }


def test_a_refused_field_does_not_disqualify_an_otherwise_admissible_capture(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """Offering a bad bound is reported, not treated as a defect in the capture.

    Refusing the capture outright for an offered ``open_time`` would discard evidence
    that exists, which is the opposite failure to admitting a bad bound.
    """
    filing(store, opening=at(2025, 3, 17), closing=at(2026, 1, 1))
    archive(
        store,
        fetched_at=REWRITE,
        source_kind=FILING,
        source_url=FILING_URL + "?v=2",
        stated_in_force_from=at(2025, 3, 17),
        stated_in_force_to=at(2026, 1, 1),
        offered_bound_fields={"updated_time": REWRITE.isoformat()},
    )
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 1
    assert outcome.evidence[0].in_force_from == at(2025, 3, 17)
    assert outcome.refusals_by_reason == {REFUSED_BOUND_REASONS["updated_time"]: 1}


# ---------------------------------------------------------------------------
# A complete dated capture attests, and the consumer accepts the interval.
# ---------------------------------------------------------------------------


def test_a_stated_interval_attests_and_the_consumer_accepts_it(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    capture = filing(store, opening=at(2025, 3, 17), closing=at(2026, 1, 1))
    outcome = attestor.attest(FED_25MAY)
    assert outcome.attested is True
    assert outcome.records_attested == 1
    assert outcome.refusals == ()

    record = outcome.evidence[0]
    assert isinstance(record, RuleVersionEvidence)
    assert record.rule_hash == capture.raw_hash
    assert record.in_force_from < record.in_force_to
    assert record.applies_to(rule_hash=capture.raw_hash, at=SETTLEMENT) is True
    assert (
        record.applies_to(
            rule_hash=capture.raw_hash, at=record.in_force_from - dt.timedelta(seconds=1)
        )
        is False
    )
    assert record.applies_to(rule_hash=capture.raw_hash, at=record.in_force_to) is False
    assert record.applies_to(rule_hash=capture.raw_hash, at=at(2026, 6, 1)) is False
    assert record.applies_to(rule_hash="0" * 64, at=SETTLEMENT) is False


def test_the_produced_record_satisfies_the_declared_requirement(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    filing(store, opening=at(2025, 3, 17), closing=at(2026, 1, 1))
    record = attestor.attest(FED_25MAY).evidence[0]
    settings = load_attestation_settings()
    stated = record.as_dict()
    assert set(settings.required_record_fields) <= set(stated)
    assert all(stated[name] not in (None, "") for name in settings.required_record_fields)
    assert record.verified_by == settings.verification_method("stated_effective_interval")
    assert record.settlement_semantics == SEMANTICS
    assert record.source_url.startswith("https://")


def test_an_observation_run_attests_over_the_observed_span(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """Two confirmations of one digest, then a third showing a different one.

    Both bounds come from the sources' own recorded instants; the fetch instants are
    set later and must not appear in the record.
    """
    first = archive(store, source_observed_at=at(2025, 3, 1), fetched_at=at(2026, 1, 5))
    archive(store, source_observed_at=at(2025, 5, 7), fetched_at=at(2026, 4, 5))
    later = archive(
        store,
        payload=body_for(RULE_TEXT.replace("4.25", "4.50")),
        source_observed_at=at(2025, 9, 1),
        fetched_at=at(2026, 7, 5),
        source_url="https://web.archive.org/web/20250901000000/https://example.test/rules",
    )
    record = attestor.attest(FED_25MAY).evidence[0]
    assert record.rule_hash == first.raw_hash
    assert record.in_force_from == at(2025, 3, 1)
    assert record.in_force_to == later.source_observed_at
    assert record.applies_to(rule_hash=first.raw_hash, at=SETTLEMENT) is True
    assert record.applies_to(rule_hash=first.raw_hash, at=at(2025, 9, 1)) is False
    assert record.applies_to(rule_hash=later.raw_hash, at=at(2025, 9, 1)) is False


def test_the_fetch_instant_never_becomes_an_interval_bound(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """The crux: two captures of one digest bound by the sources, not by this run."""
    archive(store, source_observed_at=at(2025, 3, 1), fetched_at=TODAY)
    archive(store, source_observed_at=at(2025, 5, 7), fetched_at=TODAY)
    record = attestor.attest(FED_25MAY).evidence[0]
    assert record.in_force_from == at(2025, 3, 1)
    assert record.in_force_to is None
    assert record.as_dict()["in_force_from"] != TODAY.isoformat()
    assert record.observed_at == TODAY


def test_an_uncontradicted_run_leaves_the_interval_open_rather_than_extending_it(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    archive(store, source_observed_at=at(2025, 3, 1))
    record = attestor.attest(FED_25MAY).evidence[0]
    assert record.in_force_to is None
    assert record.as_dict()["in_force_to_is_open"] is True
    assert record.applies_to(rule_hash=record.rule_hash, at=at(2030, 1, 1)) is True


def test_the_requested_version_is_the_one_certified(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    first = archive(store, source_observed_at=at(2025, 3, 1))
    second = archive(
        store,
        payload=body_for(RULE_TEXT.replace("4.25", "4.50")),
        source_observed_at=at(2025, 9, 1),
        source_url="https://web.archive.org/web/20250901000000/https://example.test/rules",
    )
    assert attestor.attest(FED_25MAY).evidence[0].rule_hash == first.raw_hash
    wanted = attestor.attest(FED_25MAY, wanted_rule_hash=second.raw_hash)
    assert wanted.evidence[0].rule_hash == second.raw_hash
    assert wanted.evidence[0].in_force_from == at(2025, 9, 1)


# ---------------------------------------------------------------------------
# A capture that cannot bound an interval is refused, never defaulted.
# ---------------------------------------------------------------------------


def test_rule_text_read_today_with_no_source_date_is_refused_not_defaulted(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """The exact state the archived file is in: a body readable now, no date on it."""
    archive(
        store,
        fetched_at=TODAY,
        source_observed_at=None,
        source_names_contract=False,
        offered_bound_fields={"open_time": SETTLEMENT.isoformat()},
    )
    outcome = attestor.attest(FED_25MAY)
    unattested(
        outcome,
        REFUSED_BOUND_REASONS["open_time"],
        REFUSAL_SOURCE_NAMES_NO_CONTRACT,
    )


def test_the_fetch_instant_is_reported_as_not_a_bound(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """An admissible capture whose source records no instant carries no bound at all.

    The capture is a well-formed archived snapshot naming the contract, and the only
    thing missing is a date the *source* records. The refusal then says so and names
    the run's own fetch instant as not being a substitute for one.
    """
    archive(store, source_observed_at=None, fetched_at=TODAY)
    outcome = attestor.attest(FED_25MAY)
    unattested(outcome, REFUSAL_NO_BOUNDING_SOURCE)
    assert outcome.refusals_by_reason == {REFUSAL_NO_BOUNDING_SOURCE: 1}
    detail = outcome.refusals[0].detail
    assert TODAY.isoformat() in detail
    assert "not bounds" in detail


def test_a_capture_that_states_no_interval_is_refused_with_no_bound_imputed(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    archive(store, source_kind=FILING, source_url=FILING_URL, fetched_at=TODAY)
    outcome = attestor.attest(FED_25MAY)
    unattested(outcome, REFUSAL_NO_BOUNDING_SOURCE)
    assert "No bound is imputed" in outcome.refusals[0].detail


def test_a_stated_interval_that_does_not_follow_is_refused_rather_than_clamped(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    filing(store, opening=at(2025, 3, 17), closing=at(2025, 3, 17))
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 0
    assert {r.reason for r in outcome.refusals} == {REFUSAL_EMPTY_INTERVAL}


def test_two_dated_sources_stating_different_intervals_are_refused(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    filing(store, opening=at(2025, 3, 17), closing=at(2025, 7, 1))
    filing(
        store,
        opening=at(2025, 5, 1),
        closing=at(2026, 1, 1),
        source_url="https://www.cftc.gov/filings/ptc/25/05/ptc05012529868.pdf",
    )
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 0
    assert {r.reason for r in outcome.refusals} == {REFUSAL_AMBIGUOUS_STATED_INTERVALS}


def test_a_venue_rule_change_notice_that_names_the_contract_attests(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """The third declared category, so all three are exercised."""
    archive(
        store,
        source_kind=NOTICE,
        source_url="https://example.test/notices/fed-25may-rule-change",
        source_observed_at=at(2025, 3, 17),
        stated_in_force_from=at(2025, 3, 17),
        stated_in_force_to=at(2025, 12, 1),
    )
    record = attestor.attest(FED_25MAY).evidence[0]
    assert record.in_force_from == at(2025, 3, 17)
    assert record.applies_to(rule_hash=record.rule_hash, at=SETTLEMENT) is True


def test_a_filing_that_names_no_contract_bounds_no_contract(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    filing(
        store,
        opening=at(2025, 3, 17),
        closing=at(2026, 1, 1),
        source_names_contract=False,
    )
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 0
    assert REFUSAL_SOURCE_NAMES_NO_CONTRACT in outcome.refusals_by_reason
    assert "product template" in outcome.refusals[0].detail


def test_a_current_state_capture_is_refused_at_attestation_though_it_may_be_held(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """Holding current rule text is legitimate; deriving an interval from it is not."""
    archive(
        store,
        source_kind=CURRENT_TEXT,
        source_url="https://external-api.kalshi.test/trade-api/v2/historical/markets",
        fetched_at=TODAY,
    )
    outcome = attestor.attest(FED_25MAY)
    assert outcome.captures_held == 1
    unattested(outcome, REFUSAL_INADMISSIBLE_SOURCE_KIND)


def test_an_undeclared_source_kind_cannot_be_captured_at_all(store: RuleCaptureStore) -> None:
    with pytest.raises(ValueError, match="not declared by the evidence standard"):
        archive(store, source_kind="makes_me_feel_good")


def test_a_capture_carrying_no_rule_text_or_no_semantics_is_refused(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    archive(
        store,
        payload=b"<html><body><div id='root'></div></body></html>",
        source_observed_at=at(2025, 3, 1),
        rule_text="",
    )
    archive(
        store,
        source_observed_at=at(2025, 6, 2),
        fetched_at=at(2026, 6, 1),
        settlement_semantics=None,
        source_url="https://web.archive.org/web/20250602000000/https://example.test/rules",
    )
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 0
    assert REFUSAL_RULE_TEXT_ABSENT in outcome.refusals_by_reason
    assert REFUSAL_SETTLEMENT_SEMANTICS_UNSTATED in outcome.refusals_by_reason


def test_a_capture_whose_bytes_were_removed_attests_nothing(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    capture = filing(store, opening=at(2025, 6, 1))
    store.raw_store._blob_path(capture.raw_hash).unlink()
    outcome = attestor.attest(FED_25MAY)
    unattested(outcome, REFUSAL_CAPTURE_EVIDENCE_MISSING)


def test_a_capture_record_edited_to_cite_another_payload_is_refused(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    capture = filing(store, opening=at(2025, 6, 1))
    other = archive(
        store,
        contract_id="FED-25JUL-T4.25",
        payload=b"a different contract's rule text",
        source_observed_at=at(2025, 6, 1),
    )
    path = store.contract_directory(FED_25MAY) / f"{capture.capture_id}.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["raw_hash"] = other.raw_hash
    document["raw_size"] = other.raw_size
    path.write_text(json.dumps(document), encoding="utf-8")
    outcome = attestor.attest(FED_25MAY)
    unattested(outcome, REFUSAL_CAPTURE_DIGEST_MISMATCH)


def test_a_capture_recorded_against_another_contract_cannot_certify_this_one(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """A file moved into the wrong contract's directory is refused, not believed."""
    capture = filing(store, contract_id="FED-25JUL-T4.25", opening=at(2025, 3, 17))
    target = store.contract_directory(FED_25MAY)
    target.mkdir(parents=True, exist_ok=True)
    source = store.contract_directory("FED-25JUL-T4.25") / f"{capture.capture_id}.json"
    (target / source.name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 0
    assert "capture_is_indexed_under_a_different_contract" in outcome.refusals_by_reason


def test_no_capture_at_all_is_a_named_unattested_state_not_a_zero(
    attestor: RuleAttestor,
) -> None:
    outcome = attestor.attest("FED-25SEP-T4.00")
    assert outcome.captures_held == 0
    assert outcome.records_attested == 0
    assert outcome.evidence == ()
    assert outcome.primary_reason == REFUSAL_NO_CAPTURES_HELD
    assert outcome.refusals_by_reason == {REFUSAL_NO_CAPTURES_HELD: 1}


# ---------------------------------------------------------------------------
# The report: refusals counted by reason, per contract and in total.
# ---------------------------------------------------------------------------


def test_the_report_counts_refusals_by_reason(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    archive(
        store,
        contract_id=FED_25MAY,
        fetched_at=REWRITE,
        offered_bound_fields={"open_time": SETTLEMENT.isoformat()},
    )
    archive(
        store,
        contract_id="FED-25JUL-T4.25",
        fetched_at=REWRITE,
        offered_bound_fields={"updated_time": REWRITE.isoformat()},
    )
    filing(store, contract_id="FED-25SEP-T4.25", opening=at(2025, 6, 1), closing=at(2025, 12, 1))

    report = attestor.report()
    assert isinstance(report, RuleAttestationReport)
    assert sorted(c.contract_id for c in report.contracts) == [
        "FED-25JUL-T4.25",
        FED_25MAY,
        "FED-25SEP-T4.25",
    ]

    by_reason = report.refusals_by_reason
    assert by_reason[REFUSED_BOUND_REASONS["open_time"]] == 1
    assert by_reason[REFUSED_BOUND_REASONS["updated_time"]] == 1
    # Each refused contract is also unattested for want of a bounding source.
    assert by_reason[REFUSAL_NO_BOUNDING_SOURCE] == 2

    totals = report.totals()
    assert totals["contracts_examined"] == 3
    assert totals["contracts_attested"] == 1
    assert totals["contracts_unattested"] == 2
    assert totals["records_attested"] == 1
    assert totals["captures_held"] == 3
    assert report.unattested_contract_ids == ("FED-25JUL-T4.25", FED_25MAY)
    assert sum(by_reason.values()) == totals["refusals"]


def test_each_contracts_counts_are_its_own(attestor: RuleAttestor, store: RuleCaptureStore) -> None:
    archive(store, fetched_at=REWRITE, offered_bound_fields={"close_time": SETTLEMENT.isoformat()})
    filing(store, opening=at(2025, 6, 1))
    by_contract = {c.contract_id: c for c in attestor.report().contracts}
    assert by_contract[FED_25MAY].refusals_by_reason == {REFUSED_BOUND_REASONS["close_time"]: 1}
    assert by_contract[FED_25MAY].captures_held == 2
    assert by_contract[FED_25MAY].records_attested == 1


def test_the_report_states_a_contract_with_no_capture_as_an_unattested_refusal(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    filing(store, opening=at(2025, 6, 1))
    report = attestor.report([FED_25MAY, "FED-25DEC-T4.00"])
    assert report.totals()["captures_held"] == 1
    assert report.totals()["records_attested"] == 1
    assert report.refusals_by_reason == {REFUSAL_NO_CAPTURES_HELD: 1}
    assert report.unattested_contract_ids == ("FED-25DEC-T4.00",)


def test_the_report_written_to_disk_reports_the_same_counts(
    attestor: RuleAttestor, store: RuleCaptureStore, tmp_path: pathlib.Path
) -> None:
    archive(store, fetched_at=REWRITE, offered_bound_fields={"close_time": SETTLEMENT.isoformat()})
    report = attestor.report()
    path = report.write(tmp_path / "out")
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["refusals_by_reason"] == report.refusals_by_reason
    assert document["totals"] == report.totals()
    assert REFUSED_BOUND_REASONS["close_time"] in document["refusals_by_reason"]
    assert document["record_type"].endswith("RuleVersionEvidence")
    assert document["contracts"][0]["records_attested"] == 0
    assert document["contracts"][0]["refusals"][0]["field"] == "close_time"


def test_an_empty_report_is_a_measured_state_rather_than_a_failure(
    attestor: RuleAttestor,
) -> None:
    report = attestor.report()
    assert report.contracts == ()
    assert report.refusals_by_reason == {}
    assert report.totals()["contracts_examined"] == 0
    assert report.unattested_contract_ids == ()


# ---------------------------------------------------------------------------
# The capture store: bytes archived, digests re-read, one occurrence per identity.
# ---------------------------------------------------------------------------


def test_the_store_archives_the_exact_bytes_its_digest_addresses(
    store: RuleCaptureStore,
) -> None:
    payload = body_for(RULE_TEXT)
    capture = archive(store, payload=payload, fetched_at=TODAY)
    assert capture.raw_hash == hashlib.sha256(payload).hexdigest()
    assert capture.raw_size == len(payload)
    assert store.raw_store.get(capture.raw_hash) == payload
    store.verify(capture)


def test_a_capture_is_readable_back_from_disk_unchanged(store: RuleCaptureStore) -> None:
    written = archive(store, source_observed_at=at(2025, 6, 1), fetched_at=TODAY)
    loaded = store.captures(FED_25MAY)[0]
    assert loaded == written
    assert loaded.source_observed_at == at(2025, 6, 1)
    assert loaded.captured_at == TODAY
    assert store.contract_ids() == (FED_25MAY,)


def test_two_contracts_are_keyed_apart_even_with_identical_rule_text(
    store: RuleCaptureStore,
) -> None:
    first = archive(store, contract_id=FED_25MAY, source_observed_at=at(2025, 6, 1))
    second = archive(store, contract_id="FED-25MAY-T4.50", source_observed_at=at(2025, 6, 1))
    assert first.raw_hash == second.raw_hash
    assert first.capture_id != second.capture_id
    assert [c.contract_id for c in store.captures()] == [FED_25MAY, "FED-25MAY-T4.50"]


def test_the_same_capture_twice_is_one_record_and_a_changed_one_is_refused(
    store: RuleCaptureStore,
) -> None:
    first = archive(store, source_observed_at=at(2025, 6, 1), fetched_at=TODAY)
    again = archive(store, source_observed_at=at(2025, 6, 1), fetched_at=TODAY)
    assert again.capture_id == first.capture_id
    assert len(store.captures(FED_25MAY)) == 1
    with pytest.raises(FileExistsError, match="differs"):
        archive(
            store,
            payload=body_for(RULE_TEXT.replace("4.25", "4.75")),
            source_observed_at=at(2025, 6, 1),
            fetched_at=TODAY,
        )


def test_a_live_fetch_is_archived_once_and_recorded_through_the_shared_transport(
    store: RuleCaptureStore,
) -> None:
    """The one live path, driven offline through ``httpx.MockTransport``.

    The transport archives every body it receives, so the capture cites the payload
    already in the store rather than archiving the same occurrence a second time.
    """
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(
            200, content=body_for(RULE_TEXT), headers={"content-type": "application/json"}
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        transport = HttpTransport(store.raw_store, client=client, policy=RetryPolicy())
        capture = store.fetch(
            "https://example.test/trade-api/v2/rules",
            contract_id=FED_25MAY,
            source_kind=SNAPSHOT,
            transport=transport,
            settlement_semantics=SEMANTICS,
            source_observed_at=at(2025, 3, 1),
        )

    assert seen == ["https://example.test/trade-api/v2/rules"]
    assert capture.raw_hash == hashlib.sha256(body_for(RULE_TEXT)).hexdigest()
    assert store.raw_store.get(capture.raw_hash) == body_for(RULE_TEXT)
    receipts = store.raw_store.receipts(raw_hash=capture.raw_hash)
    assert len(receipts) == 1
    assert receipts[0]["metadata"]["credentials"] == "none"
    assert receipts[0]["source"].startswith("rule_capture:")


def test_visible_text_reduces_a_page_and_reports_a_shell_as_no_text() -> None:
    page = f"<html><body><script>var x=1;</script><p>{RULE_TEXT}</p></body></html>".encode()
    assert visible_text(page, content_type="text/html; charset=utf-8") == RULE_TEXT
    shell = b"<html><body><div id='root'></div></body></html>"
    assert visible_text(shell, content_type="text/html") is None
    assert visible_text(b"   ", content_type="text/html") is None


def test_a_contract_id_that_is_not_a_plain_token_is_refused_rather_than_sanitized(
    store: RuleCaptureStore,
) -> None:
    with pytest.raises(ValueError, match="plain contract token"):
        store.contract_directory("../FED-25MAY")


def test_the_cli_default_config_is_the_module_default() -> None:
    """The CLI repeats this path; this is the thing that notices if the two drift."""
    from market_propagation import cli

    assert cli.DEFAULT_ATTESTATION_CONFIG == CONFIG_PATH


# ---------------------------------------------------------------------------
# A dated observation of the venue's own live listing.
#
# This route is admitted because the capture is taken *before* the window it is
# used on: the listing carries the contract's own ticker beside its payout text,
# and the response carries the instant the serving system states it answered at.
# A record built from it therefore opens at that instant and cannot reach back over
# a window that closed earlier -- which is the property, not a promise, that keeps
# the 2025 cohort refused.
# ---------------------------------------------------------------------------

LIVE_KIND = "dated_observation_of_the_live_rule_text"

#: The instant the venue's own response stated when this route was measured. It is
#: read off the response, never off the run's clock.
LISTING_DATE = "Wed, 16 Sep 2026 16:15:16 GMT"
LISTED_AT = dt.datetime(2026, 9, 16, 16, 15, 16, tzinfo=dt.UTC)
LISTING_URL = "https://external-api.kalshi.com/trade-api/v2/markets"


def listing_page(contract_id: str = FED_25MAY, rules: str = RULE_TEXT) -> bytes:
    """One venue listing page: the contract's own ticker beside its payout text."""
    return json.dumps(
        {"markets": [{"ticker": contract_id, "rules_primary": rules, "status": "active"}]}
    ).encode("utf-8")


def observe_live_listing(
    store: RuleCaptureStore,
    *,
    contract_id: str = FED_25MAY,
    page: bytes | None = None,
    stated_date: str | None = LISTING_DATE,
) -> Any:
    """Fetch a listing page offline and record one capture per contract on it.

    This mirrors the capture command: the bytes are the transport's, the instant
    recorded is the one the response itself states, and the rule text is the payout
    text read from the same object as the ticker.
    """
    payload = listing_page(contract_id) if page is None else page
    headers = {"content-type": "application/json"}
    if stated_date is not None:
        headers["date"] = stated_date

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload, headers=headers)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        transport = HttpTransport(store.raw_store, client=client, policy=RetryPolicy())
        envelope = transport.get(
            LISTING_URL,
            params={"series_ticker": "KXFED"},
            source=f"rule_capture:{LIVE_KIND}",
            record_id="live-markets-KXFED-00000",
        )
    for market in json.loads(envelope.text)["markets"]:
        store.capture(
            envelope,
            contract_id=str(market["ticker"]),
            source_kind=LIVE_KIND,
            rule_text=str(market["rules_primary"]),
            settlement_semantics=str(market["rules_primary"]),
            source_observed_at=envelope.server_date,
            source_names_contract=True,
        )
    return envelope


def test_a_dated_observation_of_the_live_rule_text_attests_an_open_interval(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    envelope = observe_live_listing(store)
    assert envelope.server_date == LISTED_AT

    outcome = attestor.attest(FED_25MAY)
    assert outcome.records_attested == 1
    record = outcome.evidence[0]
    assert record.in_force_from == LISTED_AT
    assert record.in_force_to is None
    assert record.as_dict()["in_force_to_is_open"] is True
    assert (
        record.applies_to(rule_hash=record.rule_hash, at=LISTED_AT + dt.timedelta(seconds=1))
        is True
    )
    assert (
        record.applies_to(rule_hash=record.rule_hash, at=LISTED_AT - dt.timedelta(seconds=1))
        is False
    )


def test_a_page_that_moved_around_an_unchanged_rule_text_does_not_close_the_interval(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """The listing page carries live fields; the rule text inside it does not.

    Two fetches of one unchanged rule text differ in their bytes because the page
    around it moved. Grouping the run on the payload would close the interval on the
    second fetch and publish a one-observation version of a text that never changed,
    so the run is grouped on the text and the interval stays open.
    """
    first = observe_live_listing(store)
    later = observe_live_listing(
        store,
        stated_date="Wed, 16 Sep 2026 17:05:00 GMT",
        page=json.dumps(
            {
                "markets": [
                    {
                        "ticker": FED_25MAY,
                        "rules_primary": RULE_TEXT,
                        "status": "active",
                        "volume": 41200,
                    }
                ]
            }
        ).encode("utf-8"),
    )

    assert first.provenance.raw_hash != later.provenance.raw_hash
    assert len(store.captures(FED_25MAY)) == 2

    record = attestor.attest(FED_25MAY).evidence[0]
    assert record.rule_hash == first.provenance.raw_hash
    assert record.in_force_from == LISTED_AT
    assert record.in_force_to is None
    assert record.as_dict()["in_force_to_is_open"] is True


def test_a_capture_whose_response_states_no_instant_attests_nothing(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """No ``Date`` on the response means no bound, and this run's clock is not one."""
    envelope = observe_live_listing(store, stated_date=None)
    assert envelope.server_date is None
    unattested(attestor.attest(FED_25MAY), REFUSAL_NO_BOUNDING_SOURCE)


def test_the_new_kind_cannot_certify_a_window_that_closed_before_it(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """The 2025 cohort stays refused by construction rather than by intent."""
    observe_live_listing(store)
    record = attestor.attest(FED_25MAY).evidence[0]
    assert record.in_force_from == LISTED_AT
    assert record.applies_to(rule_hash=record.rule_hash, at=at(2025, 5, 7)) is False
    assert record.applies_to(rule_hash=record.rule_hash, at=SETTLEMENT) is False

    settings = load_attestation_settings()
    assert LIVE_KIND in settings.admissible_sources
    assert CURRENT_TEXT in settings.inadmissible_sources
    assert settings.venue_rule_text_cannot_attest_its_own_past_version is True


def test_the_emitted_document_carries_every_field_the_graph_requires(
    attestor: RuleAttestor, store: RuleCaptureStore
) -> None:
    """The producer's own field names are the ones the graph's requirement names."""
    observe_live_listing(store)
    report = attestor.report()
    document = {
        "document_version": "1",
        "produced_by": "market-propagation.attest-rules",
        "config_version": report.config_version,
        "contract_rules": [
            record.as_dict() for contract in report.contracts for record in contract.evidence
        ],
    }
    assert len(document["contract_rules"]) == 1
    graph = yaml.safe_load(
        (REPO_ROOT / "configs/neighbor_graph_v2.yaml").read_text(encoding="utf-8")
    )
    required = graph["rule_vintage"]["required_record_fields"]
    entry = document["contract_rules"][0]
    assert entry["contract_id"] == FED_25MAY
    assert entry["in_force_to"] is None
    assert {name for name in required if name not in entry} == set()
