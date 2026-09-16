"""Primary downstream policy cohort: discovery, classification and reuse.

The study's primary cohort is the contract whose payoff depends on a later US
policy-rate decision, and the earlier audit reached only the release families'
own contracts plus a keyword sweep that pulled foreign CPI families in
alphabetically. These tests defend the four facts that fix that:

* a foreign central-bank rate-decision series is not a US policy contract, and a
  Fed-personnel or Fed-communication series is not a policy-rate outcome either;
* the retired legacy series prefix stays reachable, so an inactive contract is
  not lost by dropping a prefix the venue has since superseded;
* the primary cohort is ordered ahead of the alphabetic tiebreak, so a cost bound
  truncates the tail rather than the cohort the audit exists to measure;
* the contract universe is fetched once per request identity and reused across
  events, because a per-event refetch returns identical bodies.

Everything runs against a fixed HTTP fixture at the process boundary, and each
assertion is about what a consumer of the audit observes rather than about the
requests that produced it.
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

from market_propagation.ingest.audit import (
    DIRECT_RELEASE_SERIES,
    POLICY_COHORT_SECTION,
    POLICY_FAMILY_KEY,
    CandidateContract,
    CohortAuditor,
    VerifiedOrigin,
    _select_candidates,
    classify_policy_series,
    policy_cohort,
)
from market_propagation.ingest.kalshi_rest import KalshiClient
from market_propagation.ingest.macro_releases import MacroReleaseClient
from market_propagation.ingest.pagination import RecordOrigin
from market_propagation.ingest.transport import HttpTransport, RetryPolicy
from market_propagation.storage import RawStore

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


def series_record(
    ticker: str,
    title: str,
    *,
    category: str = "Economics",
    source_name: str = "Federal Reserve",
    source_url: str = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
) -> dict[str, Any]:
    """One catalog series record, in the listing's own field names."""
    return {
        "ticker": ticker,
        "title": title,
        "category": category,
        "settlement_sources": [{"name": source_name, "url": source_url}],
    }


#: The real catalog's own records, as read from the exchange's series listing.
KXFED_SERIES = series_record(
    "KXFED",
    "Fed funds rate",
    source_name="Federal Reserve Board of Governors",
    source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
)
FED_SERIES = series_record(
    "FED",
    "Fed funds rate",
    source_name="Federal Reserve Board of Governors",
    source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
)
KXFEDDECISION_SERIES = series_record(
    "KXFEDDECISION",
    "Fed meeting",
    source_url="https://www.federalreserve.gov",
)
FEDDECISION_SERIES = series_record(
    "FEDDECISION",
    "Fed meeting",
    source_url="https://www.federalreserve.gov",
)

#: A foreign central bank's rate-decision contract: the title matches and the
#: settlement source is not a US Federal Reserve property.
FOREIGN_CBD_SERIES = series_record(
    "KXCBDECISIONENGLAND",
    "Bank Of ENGLAND policy interest rate decision",
    source_name="central bank",
    source_url="https://kalshi.com/",
)

#: A Fed personnel contract: it carries a Fed source and pays on a confirmation,
#: not on a policy rate.
FED_CHAIR_SERIES = series_record(
    "KXFEDCHAIRCONFIRM",
    "Who will be confirmed as fed chair?",
    category="Politics",
    source_name="U.S. Senate",
    source_url="https://www.rules.senate.gov/rules-of-the-senate",
)

#: A Fed communications contract: a social-media mention is not a policy outcome.
FED_TWEETS_SERIES = series_record(
    "KXFEDTWEETS",
    "Fed tweets",
    source_name="X",
    source_url="https://x.com/federalreserve",
)

#: A foreign inflation series: an exposure to a different country's price level.
FOREIGN_CPI_SERIES = series_record(
    "KXBRAZILINF",
    "Brazil inflation",
    source_name="IGBE",
    source_url="https://www.ibge.gov.br/en/indicators",
)

#: A US direct-release contract: it settles on the announced statistic itself.
DIRECT_CPI_SERIES = series_record(
    "KXCPI",
    "CPI",
    source_name="Bureau of Labor Statistics",
    source_url="https://www.bls.gov/cpi/",
)


def policy_config(*, series: list[str] | None = None) -> dict[str, Any]:
    """A cohort configuration naming the observed policy series."""
    return {
        "candidate_contract_families": {
            "policy_linked_downstream": [
                {
                    "family_key": POLICY_FAMILY_KEY,
                    "relation_type": "economic_exposure",
                    "strike_dependency": "later_policy_decision",
                    "is_primary_cohort": True,
                    "series_selection_bounds": {
                        "max_events_per_series": 60,
                        "max_markets_per_event_listing": 200,
                    },
                    "observed_venue_series": (
                        series
                        if series is not None
                        else ["KXFED", "FED", "KXFEDDECISION", "FEDDECISION"]
                    ),
                    "candidate_venue_series": ["KXFEDFUTURE"],
                    "verified_contract_ids": [],
                },
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


def policy_market(
    ticker: str,
    event_ticker: str,
    *,
    open_time: str = "2024-12-02T21:00:00Z",
    close_time: str = "2025-06-18T18:00:00Z",
) -> dict[str, Any]:
    """A policy contract open before the January 2025 CPI release and spanning it."""
    return {
        "ticker": ticker,
        "event_ticker": event_ticker,
        "market_type": "binary",
        "status": "finalized",
        "strike_type": "greater",
        "floor_strike": 3,
        "open_time": open_time,
        "close_time": close_time,
        "settlement_ts": "2025-06-18T18:30:00Z",
        "volume_fp": "1200.00",
        "open_interest_fp": "900.00",
        "rules_primary": "Resolves YES if the Fed funds target range is at or above 3.5%.",
        "rules_secondary": "Resolves from the FOMC statement.",
    }


def cpi_market(**overrides: Any) -> dict[str, Any]:
    """A direct CPI contract, open before the release and spanning it."""
    base = {
        "ticker": "KXCPI-25JAN-T0.3",
        "event_ticker": "KXCPI-25JAN",
        "market_type": "binary",
        "status": "finalized",
        "strike_type": "greater",
        "floor_strike": 1,
        "open_time": "2024-12-02T21:00:00Z",
        "close_time": "2025-02-15T13:25:00Z",
        "settlement_ts": "2025-02-15T14:30:00Z",
        "volume_fp": "3022.04",
        "open_interest_fp": "15842.78",
        "rules_primary": "If the CPI rises above 0.3%, this market resolves YES.",
        "rules_secondary": "Resolves from the BLS Consumer Price Index release.",
    }
    base.update(overrides)
    return base


class FakeHttpClient:
    """Replays responses keyed by URL fragment, recording every request made.

    The longest matching key wins rather than the first declared one. Substring
    routing otherwise lets a generic listing key swallow a more specific request
    because of declaration order, which is a fixture footgun rather than the
    behaviour under test.
    """

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.calls: list[str] = []

    def _lookup(self, url: str) -> Any:
        if url in self.routes:
            return self.routes[url]
        matches = [key for key in self.routes if key in url]
        if not matches:
            raise AssertionError(f"unexpected request in test: {url}")
        return self.routes[max(matches, key=len)]

    def get(self, url: str, headers: dict[str, str] | None = None) -> Any:
        self.calls.append(url)
        return self._lookup(url)

    def close(self) -> None:
        pass


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


def make_transport(store: Any, client: FakeHttpClient) -> HttpTransport:
    """Transport with pacing removed, archiving into the real store."""
    from market_propagation.ingest.transport import HttpTransport as _T

    def _get(url: str, headers: dict[str, str] | None = None) -> FakeResponse:
        body = client.get(url, headers)
        if isinstance(body, FakeResponse):
            return body
        return FakeResponse(200, body)

    class _Client:
        def get(self, url: str, headers: dict[str, str] | None = None) -> FakeResponse:
            return _get(url, headers)

        def close(self) -> None:
            pass

    return _T(
        store,
        client=_Client(),  # type: ignore[arg-type]
        policy=RetryPolicy(min_interval_seconds=0.0),
        sleep=lambda _seconds: None,
        now=lambda: dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC),
    )


def routes_for(
    *,
    events: dict[str, list[str]] | None = None,
    markets: dict[str, list[dict[str, Any]]] | None = None,
    series_listing: list[dict[str, Any]] | None = None,
    direct_markets: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Fixture routes for a policy-cohort audit."""
    routes: dict[str, Any] = {
        "/historical/cutoff": CUTOFF_BODY,
        "/news.release/archives": CPI_HTML,
        "/series": {"series": series_listing or [], "cursor": None},
        "/events": {"events": [], "cursor": None},
        "/historical/trades": {"trades": [], "cursor": None},
        "/historical/markets": {"markets": direct_markets or [], "cursor": None},
        "/markets": {"markets": [], "cursor": None},
    }
    for event_ticker, tickers in (markets or {}).items():
        routes[f"event_ticker={event_ticker}"] = {
            "markets": tickers,
            "cursor": None,
        }
    return routes


def auditor(store: Any, client: FakeHttpClient, **overrides: Any) -> CohortAuditor:
    return CohortAuditor(
        store,
        kalshi=KalshiClient(store, transport=make_transport(store, client)),
        bls=MacroReleaseClient(store, transport=make_transport(store, client)),
        max_contracts_per_event=overrides.get("max_contracts", 40),
        max_candle_contracts_per_event=0,
        max_pages=overrides.get("max_pages", 2),
    )


@pytest.fixture()
def store(tmp_path: pathlib.Path) -> Any:
    return RawStore(tmp_path / "raw")


def cohort_event(event_id: str = "cpi_2025_01", **overrides: Any) -> dict[str, Any]:
    base = {
        "event_id": event_id,
        "family": "cpi",
        "reference_period": "2024-12",
        "scheduled_at": "2025-01-15T13:30:00+00:00",
        "calendar_url": "https://www.bls.gov/schedule/2025/01_sched_list.htm",
        "initial_release_url": "https://www.bls.gov/news.release/archives/cpi_01152025.htm",
    }
    base.update(overrides)
    return base


def test_us_fed_rate_and_meeting_series_are_admitted() -> None:
    """The current and retired prefixes both name the same two policy families."""
    for record in (KXFED_SERIES, FED_SERIES, KXFEDDECISION_SERIES, FEDDECISION_SERIES):
        verdict = classify_policy_series(record)
        assert verdict.admitted is True, record["ticker"]
        assert "federalreserve.gov" in (verdict.source_host or "")


def test_foreign_central_bank_rate_decision_is_not_a_us_policy_contract() -> None:
    """A matching title on a non-US source is the lookalike this cohort excludes."""
    verdict = classify_policy_series(FOREIGN_CBD_SERIES)
    assert verdict.admitted is False
    assert verdict.exclusion_reason == "not_us_policy_source"
    assert verdict.source_host == "kalshi.com"


def test_fed_personnel_and_communication_series_are_not_policy_outcomes() -> None:
    """A confirmation vote and a Fed tweet do not pay on a policy rate."""
    chair = classify_policy_series(FED_CHAIR_SERIES)
    assert chair.admitted is False
    assert chair.exclusion_reason == "fed_personnel_or_nomination"

    tweets = classify_policy_series(FED_TWEETS_SERIES)
    assert tweets.admitted is False
    assert tweets.exclusion_reason == "fed_communication_or_mention"


def test_foreign_inflation_and_us_direct_release_series_are_not_policy_contracts() -> None:
    """A foreign CPI print and a US direct CPI contract are both other questions."""
    foreign = classify_policy_series(FOREIGN_CPI_SERIES)
    assert foreign.admitted is False
    assert foreign.exclusion_reason == "foreign_or_non_us_inflation"

    direct = classify_policy_series(DIRECT_CPI_SERIES)
    assert direct.admitted is False
    # It is excluded on its source, not on its ticker: the US CPI series settles on
    # the BLS, which is not a Federal Reserve policy source.
    assert direct.exclusion_reason == "not_us_policy_source"
    assert direct.source_host == "www.bls.gov"


def test_direct_release_series_are_disjoint_from_the_policy_cohort() -> None:
    """The direct study keeps its own material; the policy cohort is other series."""
    assert "KXFED" not in DIRECT_RELEASE_SERIES
    assert "KXCPI" in DIRECT_RELEASE_SERIES


def test_news_sourced_fomc_series_is_excluded_despite_a_fed_url_in_its_list() -> None:
    """The primary settlement source decides, not any host somewhere in the list.

    The real catalog carries an FOMC contract whose settlement sources are
    eighteen news outlets with one Federal Reserve URL among them. It settles from
    the news list, so admitting it would let a media-summarised contract pass as a
    policy-rate contract.
    """
    record = {
        "ticker": "KXFOMCDISSENTCOUNT",
        "title": "FOMC dissent count",
        "category": "Economics",
        "settlement_sources": [
            {"name": "ABC", "url": "https://abcnews.go.com/"},
            {"name": "Reuters", "url": "https://www.reuters.com/"},
            {"name": "Federal Reserve", "url": "https://www.federalreserve.gov/"},
        ],
    }
    verdict = classify_policy_series(record)
    assert verdict.admitted is False
    assert verdict.exclusion_reason == "not_us_policy_source"
    assert verdict.source_host == "abcnews.go.com"


def test_fomc_vote_series_is_excluded_by_category() -> None:
    """A Fed source and an FOMC title still do not make a political vote a rate."""
    record = series_record(
        "KXFOMCVOTE",
        "Next FOMC vote unanimous",
        category="Politics",
    )
    verdict = classify_policy_series(record)
    assert verdict.admitted is False
    assert verdict.exclusion_reason == "category_not_economics"


def test_config_declares_the_observed_policy_series_and_a_lead_that_is_not_queried() -> None:
    """Configured observed series are queried; an unobserved lead is only recorded."""
    config = policy_config()
    resolved = policy_cohort(config)
    assert resolved.configured is True
    assert resolved.family_key == POLICY_FAMILY_KEY
    assert resolved.configured_series == (
        "KXFED",
        "FED",
        "KXFEDDECISION",
        "FEDDECISION",
    )
    assert resolved.unobserved_leads == ("KXFEDFUTURE",)
    assert resolved.verified_contract_ids == ()
    assert resolved.max_events_per_series == 60
    assert resolved.max_markets_per_event_listing == 200


def test_absent_policy_configuration_is_reported_rather_than_invented() -> None:
    """A cohort-only run is allowed, but it cannot claim policy coverage."""
    resolved = policy_cohort({})
    assert resolved.configured is False
    assert resolved.configured_series == ()


def verified_origin() -> VerifiedOrigin:
    """A resolved origin for a candidate fixture, with no store read behind it.

    These tests exercise ordering and cohort membership, not provenance: the real
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


def candidate(ticker: str, *, policy: bool, window_overlap: bool = True) -> CandidateContract:
    return CandidateContract(
        ticker=ticker,
        event_ticker=ticker.rsplit("-", 1)[0],
        series_ticker=ticker.split("-", 1)[0],
        open_time=dt.datetime(2024, 12, 2, tzinfo=dt.UTC),
        close_time=dt.datetime(2025, 6, 18, tzinfo=dt.UTC),
        resolve_time=None,
        status="finalized",
        active_at_release=True,
        known_at_release=True,
        volume_fp=Decimal("0.00"),
        open_interest_fp=Decimal("0.00"),
        strike_type="greater",
        floor_strike=Decimal("1"),
        cap_strike=None,
        rule_hash="h",
        rule_available_at=dt.datetime(2024, 12, 2, tzinfo=dt.UTC),
        partition="historical",
        origin=verified_origin(),
        policy_series=policy,
        window_overlap=window_overlap,
    )


def test_policy_series_outranks_an_alphabetically_earlier_lookalike() -> None:
    """The cost bound truncates the tail, never the configured primary cohort."""
    lookalike = candidate("AAA-25JAN-T1", policy=False)
    policy = candidate("KXFED-25JAN-T3", policy=True)
    selected = _select_candidates([lookalike, policy], limit=1)
    assert [c.ticker for c in selected] == ["KXFED-25JAN-T3"]


def test_non_policy_contracts_keep_their_lifecycle_ordering() -> None:
    """Policy priority does not disturb the downstream-versus-direct separation."""
    direct = candidate("KXCPI-25JAN-T0.5", policy=False, window_overlap=False)
    live = candidate("KXCPI-25JAN-LIVE", policy=False)
    selected = _select_candidates([direct, live], limit=2)
    assert [c.ticker for c in selected] == ["KXCPI-25JAN-LIVE", "KXCPI-25JAN-T0.5"]


def test_audit_reaches_the_configured_policy_cohort_through_event_tickers(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A CPI release audits the Fed contracts that were open across it."""
    routes = routes_for(
        series_listing=[KXFED_SERIES, FOREIGN_CBD_SERIES, FED_CHAIR_SERIES],
        direct_markets=[cpi_market()],
    )
    routes["series_ticker=KXFED"] = {
        "events": [
            {"event_ticker": "FED-25JAN", "series_ticker": "KXFED"},
        ],
        "cursor": None,
    }
    routes["event_ticker=FED-25JAN"] = {
        "markets": [policy_market("FED-25JAN-T3.5", "FED-25JAN")],
        "cursor": None,
    }
    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "policy",
        config={**policy_config(), "events": [cohort_event()]},
        discover_series=True,
    )

    event = result.events[0]
    policy_candidates = [c for c in event.candidates if c.policy_series]
    assert [c.ticker for c in policy_candidates] == ["FED-25JAN-T3.5"]
    assert policy_candidates[0].eligible is True
    gate = {g.name: g for g in event.gates}
    assert gate["configured_policy_cohort_reached"].satisfied is True
    assert gate["policy_contracts_are_not_direct_release_contracts"].satisfied is True


def test_audit_excludes_a_foreign_rate_decision_from_the_policy_cohort(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A foreign central bank's contract is a recorded lookalike, not a cohort member."""
    routes = routes_for(
        series_listing=[FOREIGN_CBD_SERIES],
        direct_markets=[cpi_market()],
    )
    auditor_ = auditor(store, FakeHttpClient(routes))
    result = auditor_.audit_cohort(
        tmp_path / "foreign",
        config={**policy_config(series=[]), "events": [cohort_event()]},
        discover_series=True,
    )

    discovery = json.loads((tmp_path / "foreign" / "series_discovery.json").read_text())
    excluded = {row["ticker"]: row for row in discovery["policy_series_excluded"]}
    assert excluded["KXCBDECISIONENGLAND"]["exclusion_reason"] == "not_us_policy_source"
    assert discovery["policy_series_admitted"] == []

    # With nothing configured, the cohort claim is vacuously satisfied but the run
    # still records that no policy contract was reached.
    assert result.events[0].status in {"audited", "audited_with_errors"}


def test_a_shapeless_event_listing_blocks_its_series_instead_of_aborting_the_run(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A 200 body that is not the documented listing shape must not kill the run.

    The paginator raises ``WireShapeError`` for it, and that type is not caught by
    the CLI, so an unhandled raise here would abort before any artifact is written
    and leave a misbehaving venue with nothing recorded.
    """
    routes = routes_for(direct_markets=[cpi_market()])
    routes["series_listing"] = {"series": [KXFED_SERIES], "cursor": None}
    routes["series_ticker=KXFED"] = "<html><body>maintenance</body></html>"
    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "shapeless",
        config={**policy_config(series=["KXFED"]), "events": [cohort_event()]},
        discover_series=False,
    )

    # The run completed and wrote its artifacts, and the series is a blocked record.
    assert result.events[0].status in {"audited", "audited_with_errors"}
    assert result.complete is False
    blocked = [record for record in result.blocked if record.get("policy_series") == "KXFED"]
    assert blocked, "the unreadable series must be recorded as blocked"
    assert blocked[0]["empty_result"] is False
    assert (tmp_path / "shapeless" / "coverage.json").exists()


def test_unreached_policy_cohort_blocks_the_policy_claim(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A read that reached no policy contract is a coverage limit, not a result."""
    routes = routes_for(direct_markets=[cpi_market()])
    routes["events"] = {"events": [], "cursor": None}
    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "unreached",
        config={**policy_config(), "events": [cohort_event()]},
        discover_series=False,
    )
    event = result.events[0]
    gate = {g.name: g for g in event.gates}
    assert gate["configured_policy_cohort_reached"].satisfied is False
    assert "primary downstream policy cohort" in " ".join(
        gate["configured_policy_cohort_reached"].blocks
    )
    assert result.complete is False


def test_retired_legacy_prefix_series_is_reachable_and_retained() -> None:
    """The retired prefix is the same family and must not be dropped as inactive."""
    legacy = classify_policy_series(FED_SERIES)
    current = classify_policy_series(KXFED_SERIES)
    assert legacy.admitted is True and current.admitted is True
    assert legacy.source_host == current.source_host


def test_legacy_event_contract_is_audited_under_the_policy_cohort(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A contract under the retired prefix is cohort material, not a stray."""
    routes = routes_for(direct_markets=[cpi_market()])
    routes["series_ticker=FED"] = {
        "events": [{"event_ticker": "FED-25MAR", "series_ticker": "FED"}],
        "cursor": None,
    }
    routes["event_ticker=FED-25MAR"] = {
        "markets": [policy_market("FED-25MAR-T3.5", "FED-25MAR")],
        "cursor": None,
    }
    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "legacy",
        config={**policy_config(series=["FED"]), "events": [cohort_event()]},
        discover_series=False,
    )
    event = result.events[0]
    assert [c.ticker for c in event.candidates if c.policy_series] == ["FED-25MAR-T3.5"]


def test_lifecycle_eligibility_is_reported_apart_from_study_eligibility(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """Creation time dates the market, not the rule version readable now."""
    routes = routes_for(direct_markets=[cpi_market()])
    routes["series_ticker=KXFED"] = {
        "events": [{"event_ticker": "FED-25JAN", "series_ticker": "KXFED"}],
        "cursor": None,
    }
    routes["event_ticker=FED-25JAN"] = {
        "markets": [policy_market("FED-25JAN-T3.5", "FED-25JAN")],
        "cursor": None,
    }
    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "vintage",
        config={**policy_config(series=["KXFED"]), "events": [cohort_event()]},
        discover_series=False,
    )

    eligibility = result.as_dict()["study_eligibility"]
    assert eligibility["lifecycle_eligible_downstream"] >= 1
    # The gate stays unsatisfied: present rule text is not a verified rule version,
    # and the lifecycle-eligible contracts are therefore not study-eligible.
    assert eligibility["rule_vintage_gate"]["satisfied"] is False
    assert eligibility["rule_vintage_gate"]["blocks"]
    assert eligibility["source_semantics_gate"]["satisfied"] is False
    assert eligibility["study_eligible_downstream"] == 0
    assert eligibility["verified_rule_version_record_count"] == 0

    candidate_record = next(c for c in result.events[0].candidates if c.policy_series).as_dict()
    # No rule-version verification means no instant at which the rule text was
    # certified readable, so availability stays unknown rather than borrowing the
    # market's own creation time.
    assert candidate_record["rule_available_at_basis"] == ("unknown_no_verified_rule_version")
    assert candidate_record["rule_available_at"] is None
    assert candidate_record["lifecycle_eligible"] is True
    assert candidate_record["study_eligible"] is False


def test_universe_is_fetched_once_and_reused_across_events(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """Identical bodies are not re-requested once per event."""
    routes = routes_for(direct_markets=[cpi_market()])
    client = FakeHttpClient(routes)
    events = [
        cohort_event("cpi_2025_01"),
        cohort_event(
            "cpi_2025_02",
            scheduled_at="2025-02-12T13:30:00+00:00",
            reference_period="2025-01",
        ),
    ]
    result = auditor(store, client).audit_cohort(
        tmp_path / "reuse",
        config={**policy_config(), "events": events},
        discover_series=False,
    )

    assert len(result.events) == 2
    reuse = result.as_dict()["universe_reuse"]
    assert reuse["persisted_across_runs"] is False
    assert reuse["lifecycle_eligibility_cached"] is False
    assert reuse["requests_served_from_cache"] >= 1

    # Each event applies its own lifecycle to the shared records: the second
    # release is in February, so a contract opened in December is still eligible
    # there while a contract that closed in January would not be.
    for event in result.events:
        assert all(c.active_at_release for c in event.candidates if c.policy_series)


def test_lifecycle_eligibility_is_per_event_even_on_a_shared_universe(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A shared fetch must not imply a shared eligibility answer."""
    routes = routes_for(direct_markets=[])
    routes["series_ticker=KXFED"] = {
        "events": [{"event_ticker": "FED-25JAN", "series_ticker": "KXFED"}],
        "cursor": None,
    }
    routes["event_ticker=FED-25JAN"] = {
        "markets": [
            # Open only after the January release, so it is ineligible there.
            policy_market("FED-25JAN-T4.0", "FED-25JAN", open_time="2025-02-01T00:00:00Z")
        ],
        "cursor": None,
    }
    events = [
        cohort_event("cpi_2025_01"),
        cohort_event(
            "cpi_2025_03",
            scheduled_at="2025-03-12T12:30:00+00:00",
            reference_period="2025-02",
        ),
    ]
    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "per-event",
        config={**policy_config(series=["KXFED"]), "events": events},
        discover_series=False,
    )

    january, march = result.events
    jan = [c for c in january.candidates if c.policy_series]
    mar = [c for c in march.candidates if c.policy_series]
    assert jan and jan[0].eligible is False
    assert "created_after_release" in jan[0].exclusion_reasons
    assert mar and mar[0].eligible is True


def test_truncated_event_walk_is_reported_as_a_bound(store: Any, tmp_path: pathlib.Path) -> None:
    """A policy series walk that stops early bounds coverage rather than hiding it."""
    routes = routes_for(direct_markets=[cpi_market()])
    routes["series_ticker=KXFED"] = {
        "events": [{"event_ticker": "FED-25JAN", "series_ticker": "KXFED"}],
        "cursor": "MORE",
    }
    result = auditor(store, FakeHttpClient(routes), max_pages=1).audit_cohort(
        tmp_path / "bounded",
        config={**policy_config(series=["KXFED"]), "events": [cohort_event()]},
        discover_series=False,
    )
    reasons = " ".join(str(b.get("reason", "")) for b in result.blocked)
    assert "bounded" in reasons or "stopped early" in reasons
    assert any("listing" in note or "tickers" in note for note in result.limitations)


def test_configured_market_bound_truncates_and_is_reported(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """The configured per-event market bound is enforced, and its truncation is gated."""
    routes = routes_for(direct_markets=[cpi_market()])
    routes["series_ticker=KXFED"] = {
        "events": [{"event_ticker": "FED-25JAN", "series_ticker": "KXFED"}],
        "cursor": None,
    }
    routes["event_ticker=FED-25JAN"] = {
        "markets": [
            policy_market("FED-25JAN-T3.5", "FED-25JAN"),
            policy_market("FED-25JAN-T4.0", "FED-25JAN"),
        ],
        "cursor": "MORE",
    }
    config = policy_config()
    config["candidate_contract_families"]["policy_linked_downstream"][0][
        "series_selection_bounds"
    ] = {"max_events_per_series": 60, "max_markets_per_event_listing": 1}

    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "market-bound",
        config={**config, "events": [cohort_event()]},
        discover_series=False,
    )
    event = result.events[0]
    assert [c.ticker for c in event.candidates if c.policy_series] == ["FED-25JAN-T3.5"]
    gate = {g.name: g for g in event.gates}
    assert gate["listing_pagination_complete"].satisfied is False


def test_series_override_narrows_only_to_configured_series(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """The override cannot introduce a series the configuration does not carry."""
    routes = routes_for(direct_markets=[cpi_market()])
    auditor_ = auditor(store, FakeHttpClient(routes))
    with pytest.raises(ValueError, match="does not carry"):
        auditor_.audit_cohort(
            tmp_path / "override-bad",
            config={**policy_config(), "events": [cohort_event()]},
            discover_series=False,
            series_override=["KXNOTCONFIGURED"],
        )


def test_series_override_requires_a_configured_policy_cohort(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """Without a configured cohort the override would be a second, hidden cohort."""
    routes = routes_for(direct_markets=[cpi_market()])
    auditor_ = auditor(store, FakeHttpClient(routes))
    with pytest.raises(ValueError, match="requires a configured policy cohort"):
        auditor_.audit_cohort(
            tmp_path / "override-unconfigured",
            events=[cohort_event()],
            discover_series=False,
            series_override=["KXFED"],
        )


def test_series_override_accepts_a_configured_subset(store: Any, tmp_path: pathlib.Path) -> None:
    """A bounded rerun may narrow to one configured series."""
    routes = routes_for(direct_markets=[cpi_market()])
    routes["series_ticker=KXFED"] = {
        "events": [{"event_ticker": "FED-25JAN", "series_ticker": "KXFED"}],
        "cursor": None,
    }
    routes["event_ticker=FED-25JAN"] = {
        "markets": [policy_market("FED-25JAN-T3.5", "FED-25JAN")],
        "cursor": None,
    }
    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "override-ok",
        config={**policy_config(), "events": [cohort_event()]},
        discover_series=False,
        series_override=["KXFED"],
    )
    selection = result.as_dict()["series_selection"]
    assert selection["policy_series_queried"] == ["KXFED"]
    assert selection["override_is_narrowing_only"] is True


def test_excluded_families_and_unqueried_leads_are_reported(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """Everything configured but not audited is named rather than silently absent."""
    routes = routes_for(direct_markets=[cpi_market()])
    result = auditor(store, FakeHttpClient(routes)).audit_cohort(
        tmp_path / "reported",
        config={**policy_config(), "events": [cohort_event()]},
        discover_series=False,
    )
    selection = result.as_dict()["series_selection"]
    excluded = {row["family_key"] for row in selection["excluded_families"]}
    assert "recession_or_growth_threshold" in excluded
    assert selection["unobserved_leads_not_queried"] == ["KXFEDFUTURE"]
    assert POLICY_COHORT_SECTION == "policy_linked_downstream"
