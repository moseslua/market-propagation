"""Study eligibility: a lifecycle candidate is not a verified study input.

The contradiction these tests prevent is a cohort that reports study-eligible
contracts while the gates guarding that same claim are printed as unsatisfied.
Full study eligibility rests on a configured rule-version record that names the
contract, matches the rule hash actually fetched and covers the release instant.
Lifecycle eligibility rests only on the contract's own times. The two are
reported apart, and a zero study-eligible count beside unsatisfied gates is the
correct reading of a cohort whose rule versions were never verified -- not an
access failure to work around, and not a gate hardcoded closed either: correctly
dated complete evidence passes with no code change here.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
from decimal import Decimal
from typing import Any

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from market_propagation.domain import Provenance
from market_propagation.ingest.audit import (
    RULE_VERSION_EVIDENCE_KEY,
    CandidateContract,
    CohortAudit,
    CohortAuditor,
    EventAudit,
    VerifiedOrigin,
    _study_eligibility_gates,
    policy_cohort,
    rule_version_evidence,
)
from market_propagation.ingest.kalshi_rest import KalshiClient
from market_propagation.ingest.macro_releases import MacroReleaseClient
from market_propagation.ingest.normalize import (
    normalize_kalshi_contract,
    rule_hash,
)
from market_propagation.ingest.pagination import RecordOrigin
from market_propagation.ingest.transport import HttpTransport, RetryPolicy
from market_propagation.storage import RawStore

RELEASE_AT = dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC)
RULE_READ_URL = "https://kalshi.example/rules/FED-25JAN-T3.5"
POLICY_RULES_PRIMARY = "Resolves YES if the Fed funds target range is at or above 3.5%."
POLICY_RULES_SECONDARY = "Resolves from the FOMC statement."
OTHER_RULE_DIGEST = "b" * 64

CUTOFF_BODY = {
    "market_positions_last_updated_ts": "2026-07-15T00:00:00Z",
    "market_settled_ts": "2026-07-15T00:00:00Z",
    "orders_updated_ts": "2026-07-15T00:00:00Z",
    "trades_created_ts": "2026-07-15T00:00:00Z",
}

CPI_HTML = """
<title>Consumer Price Index News Release - 2024 M12 Results</title>
<pre>
Transmission of material in this release is embargoed until
8:30 a.m. (ET) Wednesday, January 15, 2025
</pre>
"""


def fetched_rule_hash() -> str:
    """The digest the audit itself computes from the fixture's fetched rule text."""
    return rule_hash(POLICY_RULES_PRIMARY, POLICY_RULES_SECONDARY)


def policy_market(ticker: str = "FED-25JAN-T3.5") -> dict[str, Any]:
    """A policy contract open before the release, spanning it, with real rule text."""
    return {
        "ticker": ticker,
        "event_ticker": "FED-25JAN",
        "market_type": "binary",
        "status": "finalized",
        "strike_type": "greater",
        "floor_strike": 3,
        "open_time": "2024-12-02T21:00:00Z",
        "close_time": "2025-06-18T18:00:00Z",
        "created_time": "2024-12-02T20:00:00Z",
        "settlement_ts": "2025-06-18T18:30:00Z",
        "volume_fp": "1200.00",
        "open_interest_fp": "900.00",
        "rules_primary": POLICY_RULES_PRIMARY,
        "rules_secondary": POLICY_RULES_SECONDARY,
    }


def cpi_market() -> dict[str, Any]:
    return {
        "ticker": "KXCPI-25JAN-T0.3",
        "event_ticker": "KXCPI-25JAN",
        "market_type": "binary",
        "status": "finalized",
        "strike_type": "greater",
        "floor_strike": 1,
        "open_time": "2024-12-02T21:00:00Z",
        "close_time": "2025-02-15T13:25:00Z",
        "created_time": "2024-12-02T20:00:00Z",
        "settlement_ts": "2025-02-15T14:30:00Z",
        "volume_fp": "3022.04",
        "open_interest_fp": "15842.78",
        "rules_primary": "If the CPI rises above 0.3%, this market resolves YES.",
        "rules_secondary": "Resolves from the BLS Consumer Price Index release.",
    }


def evidence_record(
    *,
    contract_id: str = "FED-25JAN-T3.5",
    digest: str | None = None,
    in_force_from: str = "2024-12-01T00:00:00+00:00",
    in_force_to: str | None = None,
    observed_at: str = "2026-09-01T00:00:00+00:00",
) -> dict[str, Any]:
    """One complete verified rule-version record, shaped as configuration carries it."""
    record: dict[str, Any] = {
        "contract_id": contract_id,
        "rule_hash": fetched_rule_hash() if digest is None else digest,
        "source_url": RULE_READ_URL,
        "verified_by": "independent_rule_audit_2026_09_01",
        "in_force_from": in_force_from,
        "observed_at": observed_at,
        "settlement_semantics": ("pays on the FOMC target range stated at the settlement instant"),
    }
    if in_force_to is not None:
        record["in_force_to"] = in_force_to
    return record


def market_rule_hash(market: dict[str, Any]) -> str:
    """The digest the audit computes from a fixture market's own rule text."""
    return rule_hash(market["rules_primary"], market["rules_secondary"])


def evidence_for_market(
    market: dict[str, Any],
    *,
    in_force_from: str = "2024-12-01T00:00:00+00:00",
) -> dict[str, Any]:
    """A complete verified record for one fixture market's actual rule text."""
    return evidence_record(
        contract_id=market["ticker"],
        digest=market_rule_hash(market),
        in_force_from=in_force_from,
    )


def policy_config(
    *,
    series: list[str] | None = None,
    evidence: list[dict[str, Any]] | None = None,
    verified_contract_ids: list[str] | None = None,
) -> dict[str, Any]:
    """A cohort configuration naming the policy series, with optional rule evidence."""
    family: dict[str, Any] = {
        "family_key": "policy_rate_decision",
        "relation_type": "economic_exposure",
        "is_primary_cohort": True,
        "observed_venue_series": (
            series if series is not None else ["KXFED", "FED", "KXFEDDECISION", "FEDDECISION"]
        ),
        "candidate_venue_series": [],
        "verified_contract_ids": list(verified_contract_ids or []),
    }
    if evidence is not None:
        family[RULE_VERSION_EVIDENCE_KEY] = evidence
    return {
        "candidate_contract_families": {
            "policy_linked_downstream": [
                family,
                {
                    "family_key": "recession_or_growth_threshold",
                    "relation_type": "economic_exposure",
                    "is_primary_cohort": False,
                    "observed_venue_series": [],
                    "verified_contract_ids": [],
                },
            ]
        },
    }


def cohort_event() -> dict[str, Any]:
    """A canonical ``configs/cohort.yaml`` row: aware instant, family, source URLs."""
    return {
        "event_id": "cpi_2025_01",
        "family": "cpi",
        "reference_period": "2024-12",
        "scheduled_at": "2025-01-15T13:30:00+00:00",
        "calendar_url": "https://www.bls.gov/schedule/2025/01_sched_list.htm",
        "initial_release_url": "https://www.bls.gov/news.release/archives/cpi_01152025.htm",
    }


def routes_for(
    *,
    policy_markets: list[dict[str, Any]],
    direct_markets: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "/historical/cutoff": CUTOFF_BODY,
        "/news.release/archives": CPI_HTML,
        "/series": {"series": [], "cursor": None},
        "/events": {"events": [], "cursor": None},
        "/historical/trades": {"trades": [], "cursor": None},
        "/historical/markets": {
            "markets": [cpi_market()] if direct_markets is None else direct_markets,
            "cursor": None,
        },
        "/markets": {"markets": [], "cursor": None},
        "series_ticker=KXFED": {
            "events": [{"event_ticker": "FED-25JAN", "series_ticker": "KXFED"}],
            "cursor": None,
        },
        "event_ticker=FED-25JAN": {"markets": policy_markets, "cursor": None},
    }


def closed_cpi_market() -> dict[str, Any]:
    """The direct release contract as the venue actually closes it: pre-release.

    Closing five minutes before the release is the venue's own lifecycle fact, so
    the contract belongs to the direct-resolution cohort and not to the downstream
    propagation cohort the policy gates are about.
    """
    return {**cpi_market(), "close_time": "2025-01-15T13:25:00Z"}


class FakeResponse:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        self.headers: dict[str, str] = {}
        self._body = (
            json.dumps(body).encode("utf-8")
            if isinstance(body, (dict, list))
            else str(body).encode("utf-8")
        )

    @property
    def content(self) -> bytes:
        return self._body

    def json(self) -> Any:
        return json.loads(self._body.decode("utf-8"))


class FakeHttpClient:
    """Replays responses keyed by URL fragment, recording every request made."""

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.calls: list[str] = []

    def get(self, url: str, headers: dict[str, str] | None = None) -> FakeResponse:
        self.calls.append(url)
        if url in self.routes:
            body = self.routes[url]
        else:
            matches = [key for key in self.routes if key in url]
            if not matches:
                raise AssertionError(f"unexpected request in test: {url}")
            body = self.routes[max(matches, key=len)]
        return body if isinstance(body, FakeResponse) else FakeResponse(200, body)

    def close(self) -> None:
        pass


def make_transport(store: Any, client: FakeHttpClient) -> HttpTransport:
    class _Client:
        def get(self, url: str, headers: dict[str, str] | None = None) -> FakeResponse:
            return client.get(url, headers)

        def close(self) -> None:
            pass

    return HttpTransport(
        store,
        client=_Client(),  # type: ignore[arg-type]
        policy=RetryPolicy(min_interval_seconds=0.0),
        sleep=lambda _seconds: None,
        now=lambda: dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC),
    )


def auditor(store: Any, client: FakeHttpClient, *, candle_contracts: int = 0) -> CohortAuditor:
    return CohortAuditor(
        store,
        kalshi=KalshiClient(store, transport=make_transport(store, client)),
        bls=MacroReleaseClient(store, transport=make_transport(store, client)),
        max_contracts_per_event=40,
        max_candle_contracts_per_event=candle_contracts,
        max_pages=2,
    )


@pytest.fixture()
def store(tmp_path: pathlib.Path) -> Any:
    return RawStore(tmp_path / "raw")


def verified_origin() -> VerifiedOrigin:
    """A resolved origin for a candidate fixture, with no store read behind it.

    These tests exercise eligibility and gating, not provenance: the real
    resolution path is driven through the transport where the page is archived and
    read back. A ``VerifiedOrigin`` cannot be fabricated by the audit itself, so
    this stands in for the verifier's output.
    """
    return VerifiedOrigin(
        RecordOrigin(
            raw_hash="0" * 64,
            page_index=0,
            record_index=0,
            items_key="markets",
        ),
        record_hash="1" * 64,
    )


def policy_candidate(**overrides: Any) -> CandidateContract:
    """A lifecycle-eligible policy candidate, with no rule-version record by default."""
    base: dict[str, Any] = {
        "ticker": "FED-25JAN-T3.5",
        "event_ticker": "FED-25JAN",
        "series_ticker": "KXFED",
        "open_time": dt.datetime(2024, 12, 2, 21, 0, tzinfo=dt.UTC),
        "close_time": dt.datetime(2025, 6, 18, 18, 0, tzinfo=dt.UTC),
        "resolve_time": dt.datetime(2025, 6, 18, 18, 30, tzinfo=dt.UTC),
        "status": "finalized",
        "active_at_release": True,
        "known_at_release": True,
        "volume_fp": Decimal("1200.00"),
        "open_interest_fp": Decimal("900.00"),
        "strike_type": "greater",
        "floor_strike": Decimal("3"),
        "cap_strike": None,
        "rule_hash": "h",
        "rule_available_at": None,
        "partition": "historical",
        "origin": verified_origin(),
        "policy_series": True,
        "created_time": dt.datetime(2024, 12, 2, 20, 0, tzinfo=dt.UTC),
    }
    base.update(overrides)
    return CandidateContract(**base)


def test_created_time_does_not_become_rule_available_at() -> None:
    """A market record publishes no rule-version instant, so none is asserted."""
    record = {
        "ticker": "KXCPI-26AUG-T1.0",
        "event_ticker": "KXCPI-26AUG",
        "status": "finalized",
        "strike_type": "greater",
        "floor_strike": 1,
        "open_time": "2026-07-23T21:00:00Z",
        "close_time": "2026-09-11T12:25:00Z",
        "created_time": "2026-07-23T20:33:23.485958Z",
        "settlement_ts": "2026-09-11T14:30:00Z",
        "rules_primary": "If the CPI rises above 1.0%, this market resolves YES.",
        "rules_secondary": "Resolves from the BLS Consumer Price Index release.",
    }
    contract = normalize_kalshi_contract(
        record, provenance=Provenance(raw_hash="a" * 64, record_id="r", source="test")
    )
    assert contract.rule_available_at is None, (
        "created_time dates the market, not the rule text a later fetch returned"
    )
    # The lifecycle times the record does state are preserved in their own fields.
    assert contract.open_time == dt.datetime(2026, 7, 23, 21, 0, tzinfo=dt.UTC)
    assert contract.close_time == dt.datetime(2026, 9, 11, 12, 25, tzinfo=dt.UTC)


def test_explicit_rule_observation_instant_is_used_when_supplied() -> None:
    """A caller that read the rule text from a versioned source states the instant."""
    observed = dt.datetime(2026, 7, 24, 9, 15, tzinfo=dt.UTC)
    contract = normalize_kalshi_contract(
        {
            "ticker": "KXCPI-26AUG-T1.0",
            "event_ticker": "KXCPI-26AUG",
            "status": "finalized",
            "strike_type": "greater",
            "floor_strike": 1,
            "open_time": "2026-07-23T21:00:00Z",
            "close_time": "2026-09-11T12:25:00Z",
            "created_time": "2026-07-23T20:33:23.485958Z",
            "rules_primary": "If the CPI rises above 1.0%, this market resolves YES.",
        },
        provenance=Provenance(raw_hash="a" * 64, record_id="r", source="test"),
        rule_observed_at=observed,
    )
    assert contract.rule_available_at == observed


def test_bare_verified_contract_ids_are_not_rule_evidence() -> None:
    """An identifier list states what was believed verified, not what was verified."""
    resolved = rule_version_evidence(policy_config(verified_contract_ids=["FED-25JAN-T3.5"]))
    assert resolved == ()


def test_incomplete_evidence_record_is_refused_at_the_boundary() -> None:
    """A record missing its source or interval has verified nothing."""
    thin = evidence_record()
    del thin["source_url"]
    with pytest.raises(ValueError, match="source_url"):
        rule_version_evidence(policy_config(evidence=[thin]))


def test_evidence_rule_hash_must_be_a_real_digest() -> None:
    """An identifier or truncated digest is not a binding to rule text."""
    with pytest.raises(ValueError, match="sha256"):
        rule_version_evidence(policy_config(evidence=[evidence_record(digest="FED-25JAN-T3.5")]))


def test_evidence_validity_interval_must_not_be_empty() -> None:
    with pytest.raises(ValueError, match="not after"):
        rule_version_evidence(
            policy_config(
                evidence=[
                    evidence_record(
                        in_force_from="2025-01-01T00:00:00+00:00",
                        in_force_to="2024-01-01T00:00:00+00:00",
                    )
                ]
            )
        )


def test_complete_evidence_record_is_accepted() -> None:
    """Valid evidence is readable, so the gate is a statement about evidence held."""
    resolved = rule_version_evidence(policy_config(evidence=[evidence_record()]))
    assert len(resolved) == 1
    assert resolved[0].contract_id == "FED-25JAN-T3.5"
    assert resolved[0].in_force_to is None
    assert resolved[0].as_dict()["in_force_to_is_open"] is True


def _run_policy_audit(store: Any, tmp_path: pathlib.Path, config: dict[str, Any]):
    """One event whose downstream cohort is exactly the policy contract.

    The direct release contract is closed by the venue before the release, so it is
    direct-resolution material rather than a downstream contract, which keeps the
    policy verdict isolated from the release family's own contracts.
    """
    routes = routes_for(policy_markets=[policy_market()], direct_markets=[closed_cpi_market()])
    return auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "eligibility",
        config={**config, "events": [cohort_event()]},
        discover_series=False,
    )


def _policy_candidates(result: Any) -> list[Any]:
    return [c for c in result.events[0].candidates if c.policy_series]


def test_current_config_shape_cannot_report_study_eligible_candidates(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """The lifecycle shape with present rule text and no version evidence is blocked.

    This is the reported contradiction: a cohort of lifecycle-eligible contracts
    whose rule-vintage and settlement-semantics gates both say unsatisfied must not
    also report those contracts as study-eligible.
    """
    result = _run_policy_audit(store, tmp_path, policy_config(series=["KXFED"]))

    eligibility = result.as_dict()["study_eligibility"]
    assert eligibility["lifecycle_eligible_downstream"] >= 1
    assert eligibility["study_eligible_downstream"] == 0, (
        "present rule text is not a verified rule version"
    )
    assert eligibility["verified_rule_version_record_count"] == 0
    assert eligibility["rule_vintage_gate"]["satisfied"] is False
    assert eligibility["source_semantics_gate"]["satisfied"] is False
    assert eligibility["rule_vintage_gate"]["blocks"]

    # The counts and the gates read the same decision, and the top-level G0 verdict
    # is blocked by the scientific gates rather than printing them beside a pass.
    assert result.cohort_size["policy_series_study_eligible"] == 0
    assert result.cohort_size["policy_series_lifecycle_eligible"] >= 1
    assert result.complete is False
    assert set(result.unsatisfied_scientific_gates) == {
        "rule_vintage_gate",
        "source_semantics_gate",
    }

    candidate = _policy_candidates(result)[0]
    record = candidate.as_dict()
    assert record["lifecycle_eligible"] is True
    assert record["study_eligible"] is False
    assert record["rule_version_verified"] is False
    assert record["rule_version_evidence"] is None
    # The market's own creation time is reported, and is not read as rule availability.
    assert record["created_time"] == "2024-12-02T20:00:00+00:00"
    assert record["rule_available_at"] is None
    assert record["rule_available_at_basis"] == "unknown_no_verified_rule_version"
    assert record["created_time_is_not_a_rule_version_timestamp"] is True


def test_wrong_rule_hash_evidence_fails_to_certify(store: Any, tmp_path: pathlib.Path) -> None:
    """Evidence bound to different rule text cannot certify this contract's version."""
    result = _run_policy_audit(
        store,
        tmp_path,
        policy_config(series=["KXFED"], evidence=[evidence_record(digest=OTHER_RULE_DIGEST)]),
    )

    eligibility = result.as_dict()["study_eligibility"]
    assert eligibility["verified_rule_version_record_count"] == 1
    assert eligibility["study_eligible_downstream"] == 0
    assert eligibility["rule_vintage_gate"]["satisfied"] is False
    assert result.complete is False


def test_evidence_that_takes_effect_after_the_release_fails(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A version that postdates the release was not the version in force at it."""
    result = _run_policy_audit(
        store,
        tmp_path,
        policy_config(
            series=["KXFED"],
            evidence=[
                evidence_for_market(policy_market(), in_force_from="2025-06-01T00:00:00+00:00")
            ],
        ),
    )

    eligibility = result.as_dict()["study_eligibility"]
    assert eligibility["study_eligible_downstream"] == 0
    assert eligibility["rule_vintage_gate"]["satisfied"] is False
    assert eligibility["lifecycle_eligible_without_verified_rule_version"] >= 1


def test_evidence_in_force_at_the_release_certifies_without_code_changes(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A complete record covering the release promotes the contract, no edits needed."""
    market = policy_market()
    result = _run_policy_audit(
        store,
        tmp_path,
        policy_config(series=["KXFED"], evidence=[evidence_for_market(market)]),
    )
    assert market_rule_hash(market) == fetched_rule_hash()

    eligibility = result.as_dict()["study_eligibility"]
    assert eligibility["lifecycle_eligible_downstream"] >= 1
    assert (
        eligibility["study_eligible_downstream"] == (eligibility["lifecycle_eligible_downstream"])
    )
    assert eligibility["lifecycle_eligible_without_verified_rule_version"] == 0
    assert eligibility["rule_vintage_gate"]["satisfied"] is True
    assert eligibility["source_semantics_gate"]["satisfied"] is True
    assert eligibility["rule_vintage_gate"]["blocks"] == ()
    assert result.unsatisfied_scientific_gates == ()
    assert (
        result.cohort_size["policy_series_study_eligible"]
        == (result.cohort_size["policy_series_lifecycle_eligible"])
    )

    candidate = _policy_candidates(result)[0]
    assert candidate.study_eligible is True
    record = candidate.as_dict()
    assert record["rule_version_verified"] is True
    assert record["rule_version_evidence"]["source_url"] == RULE_READ_URL
    assert record["rule_available_at"] == "2024-12-01T00:00:00+00:00"
    assert record["rule_available_at_basis"] == "verified_rule_version_in_force_from"


def test_evidence_for_one_contract_does_not_cover_an_unverified_sibling() -> None:
    """A configured record certifies only the contract and digest it names."""
    digest = fetched_rule_hash()
    evidence = rule_version_evidence(
        policy_config(evidence=[evidence_record(contract_id="FED-25JAN-T3.5", digest=digest)])
    )
    verified = policy_candidate(rule_hash=digest, rule_evidence=evidence[0])
    sibling = policy_candidate(ticker="FED-25JAN-T4.0", rule_hash="c" * 64)

    # The record binds to one digest, so the sibling's different rule text is not
    # certified even though the same contract family is configured.
    assert evidence[0].applies_to(rule_hash=digest, at=RELEASE_AT) is True
    assert evidence[0].applies_to(rule_hash=sibling.rule_hash, at=RELEASE_AT) is False

    event = _audited_event(candidates=(verified, sibling))
    gates = _study_eligibility_gates([event], policy_cohort({}), (), evidence)
    assert gates["lifecycle_eligible_downstream"] == 2
    assert gates["study_eligible_downstream"] == 1
    assert gates["lifecycle_eligible_without_verified_rule_version"] == 1
    assert gates["rule_vintage_gate"]["satisfied"] is False
    assert gates["source_semantics_gate"]["satisfied"] is False


def _audited_event(*, candidates: tuple[CandidateContract, ...] = ()) -> EventAudit:
    """One fully acquired event with no coverage gate left unsatisfied."""
    return EventAudit(
        event_id="cpi_2025_01",
        family="cpi",
        reference_period="2024-12",
        scheduled_at=RELEASE_AT,
        window_start=RELEASE_AT - dt.timedelta(seconds=1800),
        window_end=RELEASE_AT + dt.timedelta(seconds=3600),
        status="audited",
        partition=None,
        candidates=candidates,
        candle_audits=(),
        trade_counts={},
        release=None,
        missing_expectations={},
        blocked=(),
        errors=(),
        raw_hashes=(),
        latency_seconds=0.0,
        gates=(),
    )


def _cohort(study_eligibility: dict[str, Any]) -> CohortAudit:
    return CohortAudit(
        venue="kalshi",
        started_at=RELEASE_AT,
        ended_at=RELEASE_AT,
        events=(_audited_event(),),
        cohort_definition_hash="hash",
        discoverable_series={},
        study_eligibility=study_eligibility,
    )


def test_acquisition_success_never_implies_study_completeness() -> None:
    """Every page walked, every gate satisfied, and the cohort still not usable."""
    blocked = _cohort(
        {
            "rule_vintage_gate": {"satisfied": False},
            "source_semantics_gate": {"satisfied": False},
        }
    )
    assert blocked.acquisition_complete is True
    assert blocked.complete is False
    assert blocked.status == "acquisition_complete_study_blocked"
    assert set(blocked.unsatisfied_scientific_gates) == {
        "rule_vintage_gate",
        "source_semantics_gate",
    }
    payload = blocked.as_dict()
    assert payload["acquisition_complete"] is True
    assert payload["complete"] is False
    assert payload["acquisition_is_not_study_eligibility"] is True

    settled = _cohort(
        {
            "rule_vintage_gate": {"satisfied": True},
            "source_semantics_gate": {"satisfied": True},
        }
    )
    assert settled.acquisition_complete is True
    assert settled.complete is True
    assert settled.status == "complete"
    assert settled.unsatisfied_scientific_gates == ()
