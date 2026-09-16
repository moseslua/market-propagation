"""Candidate provenance at the real transport/audit boundary.

A candidate is only evidence if a reader can retrieve the bytes behind it. These
tests hold that end to end: a listing is served through ``httpx.MockTransport``,
the real transport archives every page into a real ``RawStore``, the real
``paginate`` walk supplies each record's origin, and the real auditor resolves
that origin before it reports a candidate.

Every assertion here resolves something against the retrieved bytes. A substring
check on a locator string would pass just as happily against a pointer nothing
resolves, which is the failure this file exists to catch: the locator is resolved
with an RFC 6901 pointer walk, and the record found at the end of that walk is
compared against the record the candidate cites.

Two contracts share one archived page on purpose. Both must resolve, each to its
own record, which the old ``raw_hash=...#page[i].items[j]`` locator could not do:
it named a page hash with no JSON key in it, so nothing could follow it to the
bytes it claimed to address.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import pathlib
import sys
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from market_propagation.ingest.audit import (
    CandidateOriginMismatch,
    CohortAudit,
    CohortAuditor,
    _CandidateProvenanceVerifier,
)
from market_propagation.ingest.kalshi_rest import KalshiClient
from market_propagation.ingest.macro_releases import MacroReleaseClient
from market_propagation.ingest.pagination import (
    PointerResolutionError,
    RecordOrigin,
    paginate,
    resolve_json_pointer,
)
from market_propagation.ingest.transport import HttpTransport, RetryPolicy
from market_propagation.storage import RawStore

BASE = "https://external-api.kalshi.com/trade-api/v2"
CUTOFF_URL = f"{BASE}/historical/cutoff"
HISTORICAL_MARKETS_URL = f"{BASE}/historical/markets"
LIVE_MARKETS_URL = f"{BASE}/markets"
RELEASE_PREFIX = "https://www.bls.gov/news.release/archives/"

FIXED_NOW = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC)

CUTOFF_BODY: dict[str, Any] = {
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

RULES_PRIMARY = "Resolves YES if the CPI rises above the strike."
RULES_SECONDARY = "Resolves from the BLS Consumer Price Index release."


def market(ticker: str, *, floor_strike: str = "0.3") -> dict[str, Any]:
    """One market record, in the listing's own field names."""
    return {
        "ticker": ticker,
        "event_ticker": ticker.rsplit("-", 1)[0],
        "market_type": "binary",
        "status": "finalized",
        "strike_type": "greater",
        "floor_strike": floor_strike,
        "open_time": "2024-12-02T21:00:00Z",
        "close_time": "2025-06-18T18:00:00Z",
        "created_time": "2024-12-02T20:00:00Z",
        "settlement_ts": "2025-06-18T18:30:00Z",
        "volume_fp": "1200.00",
        "open_interest_fp": "900.00",
        "rules_primary": RULES_PRIMARY,
        "rules_secondary": RULES_SECONDARY,
    }


class FixtureClient:
    """Serves one body per URL prefix, from a queue that repeats its last entry.

    The repeat matters because a listing is walked once per configured series, and
    every one of those walks reaches the same endpoint. A queue that ran dry would
    make an unrelated series look like a coverage limit instead of the same page
    being read twice.
    """

    def __init__(self, routes: dict[str, list[bytes] | bytes]) -> None:
        self.routes: dict[str, list[bytes]] = {
            key: ([value] if isinstance(value, bytes) else list(value))
            for key, value in routes.items()
        }
        self.calls: list[str] = []
        self.served: dict[str, int] = {}

    def get(self, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        self.calls.append(url)
        matches = [key for key in self.routes if url.startswith(key)]
        assert matches, f"unexpected request in test: {url}"
        key = max(matches, key=len)
        bodies = self.routes[key]
        assert bodies, f"no fixture body queued for {key}"
        index = min(self.served.get(key, 0), len(bodies) - 1)
        self.served[key] = index + 1
        return httpx.Response(
            200,
            content=bodies[index],
            headers={"content-type": "application/json"},
        )

    def close(self) -> None:
        pass


class UnreadablePage(RawStore):
    """The real store, with one page made unreadable on the way back out.

    Writes are the real ones, so the page is genuinely archived and digested by the
    real transport. Only the read of the chosen hash changes, which is the condition
    a dropped or altered archive leaves behind: bytes the audit is told it can
    retrieve and cannot.
    """

    def __init__(
        self,
        root: str | pathlib.Path,
        raw_hash: str,
        *,
        missing: bool,
        replacement: bytes = b"",
    ) -> None:
        super().__init__(root)
        self._unreadable = raw_hash
        self._missing = missing
        self._replacement = replacement

    def get(self, raw_hash: str) -> bytes:
        if raw_hash == self._unreadable:
            if self._missing:
                raise FileNotFoundError(f"no payload stored for {raw_hash!r} under {self._root}")
            return self._replacement
        return super().get(raw_hash)


def body(payload: Any) -> bytes:
    return json.dumps(payload).encode("utf-8")


def page(items: list[dict[str, Any]], cursor: str | None = None) -> bytes:
    return body({"markets": items, "cursor": cursor})


#: Two contracts on one page, because a per-page hash cannot tell them apart.
TWO_ON_ONE_PAGE = (market("KXCPI-25JAN-T0.3"), market("KXCPI-25JAN-T0.5", floor_strike="0.5"))
#: One ticker twice with different terms. The market listings configure no identity
#: key, so nothing is collapsed: these are two occurrences of one ticker.
SAME_TICKER_TWICE = (market("KXCPI-25JAN-T0.3"), market("KXCPI-25JAN-T0.3", floor_strike="0.9"))

ONE_PAGE = page(list(TWO_ON_ONE_PAGE))
#: The archived hash of a served body, known before the run that serves it.
ONE_PAGE_HASH = hashlib.sha256(ONE_PAGE).hexdigest()

TRADES_URL = f"{BASE}/historical/trades"


def trade(trade_id: str) -> dict[str, Any]:
    """One historical trade, which the client does give an identity key."""
    return {
        "trade_id": trade_id,
        "ticker": "KXCPI-25JAN-T0.3",
        "count_fp": "10.00",
        "yes_price_dollars": "0.9200",
        "taker_side": "no",
        "created_time": "2025-01-15T13:18:04.434321Z",
    }


def trade_page(items: list[dict[str, Any]], cursor: str | None) -> bytes:
    return body({"trades": items, "cursor": cursor})


def routes(pages: list[bytes] | bytes) -> dict[str, Any]:
    """Fixture routes: the cutoff, the listing, the live listing, the release."""
    return {
        CUTOFF_URL: body(CUTOFF_BODY),
        HISTORICAL_MARKETS_URL: pages,
        LIVE_MARKETS_URL: body({"markets": [], "cursor": None}),
        RELEASE_PREFIX: body(CPI_HTML),
    }


def transport_for(store: RawStore, client: FixtureClient) -> HttpTransport:
    return HttpTransport(
        store,
        client=client,  # type: ignore[arg-type]
        policy=RetryPolicy(min_interval_seconds=0.0),
        sleep=lambda _seconds: None,
        now=lambda: FIXED_NOW,
    )


def auditor(store: RawStore, client: FixtureClient, **overrides: Any) -> CohortAuditor:
    transport = transport_for(store, client)
    return CohortAuditor(
        store,
        kalshi=KalshiClient(store, transport=transport),
        bls=MacroReleaseClient(store, transport=transport),
        max_contracts_per_event=overrides.get("max_contracts", 40),
        max_candle_contracts_per_event=0,
        max_pages=overrides.get("max_pages", 2),
    )


def cohort_event(event_id: str = "cpi_2025_01") -> dict[str, Any]:
    return {
        "event_id": event_id,
        "family": "cpi",
        "reference_period": "2024-12",
        "scheduled_at": "2025-01-15T13:30:00+00:00",
        "calendar_url": "https://www.bls.gov/schedule/2025/01_sched_list.htm",
        "initial_release_url": f"{RELEASE_PREFIX}cpi_01152025.htm",
    }


def run_audit(
    store: RawStore,
    client: FixtureClient,
    out: pathlib.Path,
    *,
    events: list[dict[str, Any]] | None = None,
    **overrides: Any,
) -> CohortAudit:
    return auditor(store, client, **overrides).audit_cohort(
        out,
        events=events or [cohort_event()],
        discover_series=False,
    )


def candidates_of(result: CohortAudit, index: int = 0) -> list[dict[str, Any]]:
    return [c.as_dict() for c in result.events[index].candidates]


def gate(result: CohortAudit, name: str, index: int = 0) -> Any:
    gates = {g.name: g for g in result.events[index].gates}
    assert name in gates, sorted(gates)
    return gates[name]


def resolve_cited(store: RawStore, candidate: dict[str, Any]) -> Any:
    """Follow a candidate's own reported provenance back to its record."""
    payload = json.loads(store.get(candidate["raw_hash"]).decode("utf-8"))
    return resolve_json_pointer(payload, candidate["raw_record_pointer"])


def test_two_records_on_one_page_each_resolve_to_their_own_record(
    tmp_path: pathlib.Path,
) -> None:
    """One page backs two candidates, and each locator reaches its own record."""
    store = RawStore(tmp_path / "raw")
    result = run_audit(store, FixtureClient(routes(ONE_PAGE)), tmp_path / "out")

    found = candidates_of(result)
    assert [c["ticker"] for c in found] == ["KXCPI-25JAN-T0.3", "KXCPI-25JAN-T0.5"]
    assert {c["raw_hash"] for c in found} == {ONE_PAGE_HASH}, "both records came from one page"
    assert {c["raw_record_pointer"] for c in found} == {"/markets/0", "/markets/1"}, (
        "the two records sit at different indexes of the page's own list"
    )

    for candidate in found:
        record = resolve_cited(store, candidate)
        assert isinstance(record, dict), candidate["ticker"]
        assert record["ticker"] == candidate["ticker"], (
            "the locator must address this candidate's own record, not its neighbour"
        )
        assert candidate["raw_items_key"] == "markets"
        assert candidate["raw_page_index"] == 0
        assert candidate["origin_resolved_against_archived_bytes"] is True


def test_keyless_repeated_occurrences_keep_separate_origins(
    tmp_path: pathlib.Path,
) -> None:
    """With no identity key, two deliveries are two occurrences with two origins.

    Both records carry the same ticker, so the audit's one-record-per-ticker step
    keeps a single candidate. The occurrences stay distinct, which is what the walk
    is asserted to preserve: a content digest substituted for an identity key would
    collapse them, and the second origin would have nothing of its own to name.
    """
    store = RawStore(tmp_path / "raw")
    client = FixtureClient(routes(page(list(SAME_TICKER_TWICE))))
    result = KalshiClient(store, transport=transport_for(store, client)).list_historical_markets(
        limit=1000,
        max_pages=2,
    )

    assert result.distinct_ids_across_pages == 0, "the listing configures no identity key"
    assert len(result.items) == 2, "neither delivery was collapsed by content"
    assert [origin.pointer for origin in result.origins] == ["/markets/0", "/markets/1"]

    for item, origin in zip(result.items, result.origins, strict=True):
        archived = json.loads(store.get(origin.raw_hash).decode("utf-8"))
        assert origin.resolve(archived) == item, "each origin resolves to its own record"
    first_page = json.loads(store.get(result.origins[0].raw_hash).decode("utf-8"))
    assert result.origins[0].resolve(first_page)["floor_strike"] == "0.3"
    assert result.origins[1].resolve(first_page)["floor_strike"] == "0.9"
    assert result.origins[1].record_index == 1

    audited = run_audit(
        store, FixtureClient(routes(page(list(SAME_TICKER_TWICE)))), tmp_path / "out"
    )
    found = candidates_of(audited)
    assert len(found) == 1, "one ticker keeps one candidate"
    assert resolve_cited(store, found[0])["ticker"] == found[0]["ticker"]


def test_the_same_records_delivered_twice_keep_one_candidate_with_a_real_source(
    tmp_path: pathlib.Path,
) -> None:
    """A page delivered twice is retained twice, then collapsed to one candidate.

    The market listings configure no identity key, so a repeat is not an identity
    repeat: every delivery is a separate occurrence and the walk keeps all of them.
    The audit's one-record-per-ticker step is what collapses them, and the candidate
    left standing still cites a page and a position that resolve.
    """
    store = RawStore(tmp_path / "raw")
    listed = list(TWO_ON_ONE_PAGE)
    repeated = [page(listed, "MORE"), page(listed, None)]
    client = FixtureClient(routes(repeated))
    walk = KalshiClient(store, transport=transport_for(store, client)).list_historical_markets(
        limit=1000,
        max_pages=2,
    )

    assert walk.complete is True
    assert len(walk.pages) == 2
    assert walk.distinct_ids_across_pages == 0, "no identity key is configured for markets"
    assert walk.repeated_ids_across_pages == 0, "with no identity there is nothing to repeat"
    assert len(walk.items) == 4, "both deliveries are retained as occurrences"
    assert len({origin.raw_hash for origin in walk.origins}) == 2, (
        "each occurrence names the page it actually arrived on"
    )
    for item, origin in zip(walk.items, walk.origins, strict=True):
        archived = json.loads(store.get(origin.raw_hash).decode("utf-8"))
        assert origin.resolve(archived) == item

    audited = run_audit(store, FixtureClient(routes(repeated)), tmp_path / "out")
    found = candidates_of(audited)
    assert [c["ticker"] for c in found] == ["KXCPI-25JAN-T0.3", "KXCPI-25JAN-T0.5"], (
        "two occurrences of each ticker collapse to one candidate per ticker"
    )
    for candidate in found:
        record = resolve_cited(store, candidate)
        assert record["ticker"] == candidate["ticker"]
        assert record in listed, "the record read back is the venue's own record"


def test_a_repeated_trade_identity_is_dropped_and_the_kept_origin_resolves(
    tmp_path: pathlib.Path,
) -> None:
    """Where an identity key exists, a cross-page repeat is dropped and traced."""
    store = RawStore(tmp_path / "raw")
    one = trade("t-1")
    two = trade("t-2")
    traded = [trade_page([one, two], "MORE"), trade_page([one], None)]
    client = FixtureClient({TRADES_URL: traded})
    walk = KalshiClient(store, transport=transport_for(store, client)).get_historical_trades(
        ticker="KXCPI-25JAN-T0.3",
        limit=100,
        max_pages=2,
    )

    assert walk.complete is True
    assert walk.repeated_ids_across_pages == 1, "t-1 arrived again on the second page"
    assert [item["trade_id"] for item in walk.items] == ["t-1", "t-2"]
    assert len(walk.items) + walk.dups_total == sum(walk.page_counts)

    for item, origin in zip(walk.items, walk.origins, strict=True):
        archived = json.loads(store.get(origin.raw_hash).decode("utf-8"))
        assert origin.resolve(archived) == item, "the kept record resolves to its own bytes"
    assert (
        walk.origins[0].resolve(json.loads(store.get(walk.origins[0].raw_hash).decode("utf-8")))[
            "trade_id"
        ]
        == "t-1"
    )


def test_a_missing_page_is_refused_and_blocks_the_candidate_count(
    tmp_path: pathlib.Path,
) -> None:
    """A page the store cannot produce backs no candidate and blocks the count."""
    store = UnreadablePage(tmp_path / "raw", ONE_PAGE_HASH, missing=True)
    result = run_audit(store, FixtureClient(routes(ONE_PAGE)), tmp_path / "out")

    assert result.events[0].candidates == (), (
        "a record with no retrievable source is not reported as a candidate"
    )
    assert result.events[0].status == "no_candidates"
    provenance = gate(result, "candidate_origins_verified")
    assert provenance.satisfied is False
    assert ONE_PAGE_HASH in provenance.detail
    assert "no payload is stored" in provenance.detail
    assert "reported candidate set" in " ".join(provenance.blocks)
    assert result.complete is False
    assert result.as_dict()["candidate_provenance"]["records_refused"] >= 2


def test_a_corrupt_page_never_yields_an_accepted_candidate(
    tmp_path: pathlib.Path,
) -> None:
    """Bytes that no longer hold the cited record are refused, not reported."""
    impostor = b'{"markets": [{"ticker": "KXCPI-25JAN-IMPOSTOR"}]}'
    store = UnreadablePage(tmp_path / "raw", ONE_PAGE_HASH, missing=False, replacement=impostor)
    result = run_audit(store, FixtureClient(routes(ONE_PAGE)), tmp_path / "out")

    assert result.events[0].candidates == ()
    assert "KXCPI-25JAN-IMPOSTOR" not in {c["ticker"] for c in candidates_of(result)}, (
        "bytes that changed under a stored hash are not a source for any candidate"
    )
    provenance = gate(result, "candidate_origins_verified")
    assert provenance.satisfied is False
    assert ONE_PAGE_HASH in provenance.detail
    assert "not the record digesting to" in provenance.detail
    assert result.complete is False


def test_a_page_that_is_not_json_is_refused_rather_than_guessed(
    tmp_path: pathlib.Path,
) -> None:
    """A stored page that will not parse cannot back a candidate."""
    store = UnreadablePage(
        tmp_path / "raw", ONE_PAGE_HASH, missing=False, replacement=b"{ not json"
    )
    result = run_audit(store, FixtureClient(routes(ONE_PAGE)), tmp_path / "out")

    assert result.events[0].candidates == ()
    provenance = gate(result, "candidate_origins_verified")
    assert provenance.satisfied is False
    assert "could not be read as a JSON page" in provenance.detail


def test_a_locator_pointing_at_another_record_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """The audit's own verification refuses a pointer that names a neighbour."""
    store = RawStore(tmp_path / "raw")
    result = run_audit(store, FixtureClient(routes(ONE_PAGE)), tmp_path / "out")
    found = candidates_of(result)
    assert [c["raw_record_pointer"] for c in found] == ["/markets/0", "/markets/1"]

    verifier = _CandidateProvenanceVerifier(store)
    neighbour = RecordOrigin(
        raw_hash=ONE_PAGE_HASH,
        page_index=0,
        record_index=1,
        items_key="markets",
    )
    resolved, error = verifier.verify(neighbour, TWO_ON_ONE_PAGE[0])

    assert resolved is None, "a pointer to the other record is not a source for this one"
    assert isinstance(error, CandidateOriginMismatch)
    assert "not the record digesting to" in str(error)
    assert verifier.blocked and ONE_PAGE_HASH in verifier.blocked[0]

    honest, honest_error = verifier.verify(
        RecordOrigin(
            raw_hash=ONE_PAGE_HASH,
            page_index=0,
            record_index=0,
            items_key="markets",
        ),
        TWO_ON_ONE_PAGE[0],
    )
    assert honest_error is None
    assert honest is not None and honest.record_hash == found[0]["record_hash"]
    assert honest.pointer == "/markets/0"


def test_every_reported_candidate_resolves_through_the_store(
    tmp_path: pathlib.Path,
) -> None:
    """The acceptance condition: retrieval plus pointer resolution, for all."""
    store = RawStore(tmp_path / "raw")
    result = run_audit(store, FixtureClient(routes(ONE_PAGE)), tmp_path / "out")

    verified = 0
    for candidate in candidates_of(result):
        assert candidate["raw_hash"], "a candidate may not cite an empty source"
        assert candidate["raw_record_pointer"].startswith("/")
        record = resolve_cited(store, candidate)
        assert record["ticker"] == candidate["ticker"]
        recomputed = hashlib.sha256(
            json.dumps(record, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        assert recomputed == candidate["record_hash"], (
            "the reported digest is recomputable from the retrieved record"
        )
        verified += 1

    assert verified == len(result.events[0].candidates) == 2
    provenance = result.as_dict()["candidate_provenance"]
    assert provenance["candidates_verified"] >= verified
    assert provenance["records_refused"] == 0
    assert provenance["persisted_across_runs"] is False
    assert provenance["scope"] == "single_run_in_memory"
    assert gate(result, "candidate_origins_verified").satisfied is True


def test_page_bytes_are_read_once_per_page_not_once_per_event(
    tmp_path: pathlib.Path,
) -> None:
    """A shared page is resolved once, so a second event does not re-parse it."""
    store = RawStore(tmp_path / "raw")
    events = [
        cohort_event("cpi_2025_01"),
        {
            **cohort_event("cpi_2025_02"),
            "reference_period": "2025-01",
            "scheduled_at": "2025-02-12T13:30:00+00:00",
        },
    ]
    audit = run_audit(
        store,
        FixtureClient(routes(ONE_PAGE)),
        tmp_path / "out",
        events=events,
    )

    assert len(audit.events) == 2
    per_event = [len(candidates_of(audit, index)) for index in (0, 1)]
    assert per_event == [2, 2]

    provenance = audit.as_dict()["candidate_provenance"]
    assert provenance["page_bodies_parsed"] == 1, "the page was parsed once for the run"
    assert provenance["verdicts_memoized"] == 2, "one verdict per record, not per event"
    assert provenance["candidates_verified"] >= 4, "two records were verified per event"

    # Both events cite the same records, and both citations resolve.
    for index in (0, 1):
        for candidate in candidates_of(audit, index):
            assert resolve_cited(store, candidate)["ticker"] == candidate["ticker"]


def test_pointer_resolution_is_rfc6901() -> None:
    """The locator is a real pointer: escaping, nesting and refusal included."""
    payload = {"markets": [{"ticker": "a"}, {"ticker": "b"}], "a/b": {"~x": 7}, "n": None}
    assert resolve_json_pointer(payload, "/markets/1/ticker") == "b"
    assert resolve_json_pointer(payload, "") is payload
    assert resolve_json_pointer(payload, "/a~1b/~0x") == 7, "~1 is '/', ~0 is '~'"

    with pytest.raises(PointerResolutionError):
        resolve_json_pointer(payload, "/markets/2")
    with pytest.raises(PointerResolutionError):
        resolve_json_pointer(payload, "/markets/-")
    with pytest.raises(PointerResolutionError):
        resolve_json_pointer(payload, "/absent")
    with pytest.raises(PointerResolutionError):
        resolve_json_pointer(payload, "/n/deeper")
    with pytest.raises(PointerResolutionError):
        resolve_json_pointer(payload, "markets/0")


def test_an_items_key_that_is_not_a_pointer_token_is_escaped(
    tmp_path: pathlib.Path,
) -> None:
    """A member name carrying '/' or '~' still yields a resolvable pointer."""
    store = RawStore(tmp_path / "raw")
    payload = json.dumps({"a/b": [{"ticker": "KXCPI-25JAN-T0.3"}], "cursor": None}).encode("utf-8")
    client = FixtureClient({HISTORICAL_MARKETS_URL: payload})
    result = paginate(
        transport_for(store, client),
        HISTORICAL_MARKETS_URL,
        items_key="a/b",
        source="kalshi.historical.markets",
        record_prefix="escaped",
    )

    (origin,) = result.origins
    assert origin.items_key == "a/b"
    assert origin.pointer == "/a~1b/0", "a '/' inside a member name is escaped, not a separator"
    assert (
        origin.resolve(json.loads(store.get(origin.raw_hash).decode("utf-8"))) == (result.items[0])
    )
    assert origin.resolve(json.loads(payload))["ticker"] == "KXCPI-25JAN-T0.3"
    assert store.get(origin.raw_hash) == payload
    assert resolve_json_pointer(json.loads(payload), "/a~1b/0") == result.items[0]
