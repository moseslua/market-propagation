"""Ingest integration tests: real wire formats, archival, and cutoff reconciliation.

Everything here runs against a fixed HTTP fixture at the process boundary. The
real schemas, field names, absent-versus-null encodings and payload shapes are
copied from live responses observed during implementation, so the fixtures test
the actual wire formats rather than an idealised version of them.

The transport is stubbed only at ``httpx.Client.get``, which is the network
boundary. No test reaches the network, asserts on mock call plumbing, or pins
source text. Each asserts what a consumer of the ingest API observes.

Three real upstream behaviours are pinned deliberately, because each one is a
trap that would otherwise pass silently:

* A malformed pagination cursor returns page one again with HTTP 200, so a naive
  loop repeats forever.
* Two distinct trades can share a timestamp and price, so deduplication must key
  off ``trade_id`` rather than content.
* The historical and live candle endpoints share key names with different leaf
  names, and express "no trade" as explicit nulls versus absent keys.
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

from market_propagation.domain import (
    BookEvent,
    Clock,
    Contract,
    Operator,
    Provenance,
)
from market_propagation.ingest.audit import (
    COHORT_DIRECT_CLOSED_PRE_RELEASE,
    DEFAULT_AFTER_SECONDS,
    DEFAULT_BEFORE_SECONDS,
    THIN_VOLUME_FP,
    CohortAuditor,
    VerifiedOrigin,
    _dedupe_candidates,
    _select_candidates,
    event_cohort,
)
from market_propagation.ingest.kalshi_rest import (
    CANDLE_INTERVALS_MINUTES,
    KalshiClient,
    inspect_candle_spacing,
    parse_fixed_point_dollars,
)
from market_propagation.ingest.macro_releases import (
    MacroReleaseClient,
    archive_url,
    parse_archive_index,
    parse_calendar,
    parse_release_payload,
)
from market_propagation.ingest.normalize import (
    CANDLE_SCHEMA_HISTORICAL,
    CANDLE_SCHEMA_LIVE,
    UNMAPPED,
    candles_to_quotes,
    compare_contract_versions,
    contract_is_closed,
    normalize_kalshi_candle,
    normalize_kalshi_contract,
    normalize_kalshi_orderbook_snapshot,
    normalize_kalshi_trade,
    statistic_for_series,
)
from market_propagation.ingest.pagination import (
    RecordOrigin,
    cursor_sets_consistent,
)
from market_propagation.ingest.polymarket_public import (
    PolymarketPublicClient,
    PolymarketUnreachable,
    capture_market_channel,
    classify_channel_message,
    inspect_history_spacing,
    normalize_book_snapshot,
    normalize_price_history,
)
from market_propagation.ingest.transport import (
    HttpTransport,
    RetryPolicy,
    TransportError,
    WireShapeError,
    blocked_record,
    build_url,
    retry_after_seconds,
)
from market_propagation.storage import RawStore

CUTOFF_BODY = {
    "market_positions_last_updated_ts": "2026-07-15T00:00:00Z",
    "market_settled_ts": "2026-07-15T00:00:00Z",
    "orders_updated_ts": "2026-07-15T00:00:00Z",
    "trades_created_ts": "2026-07-15T00:00:00Z",
}

MARKET_BODY = {
    "ticker": "KXCPI-26AUG-T1.0",
    "event_ticker": "KXCPI-26AUG",
    "market_type": "binary",
    "status": "finalized",
    "strike_type": "greater",
    "floor_strike": 1,
    "can_close_early": True,
    "close_time": "2026-09-11T12:25:00Z",
    "created_time": "2026-07-23T20:33:23.485958Z",
    "open_time": "2026-07-23T21:00:00Z",
    "expected_expiration_time": "2026-09-11T13:56:00Z",
    "settlement_ts": "2026-09-11T14:30:00Z",
    "latest_expiration_time": "2026-12-11T13:56:00Z",
    "expiration_value": "0.4",
    "yes_bid_dollars": "0.0100",
    "yes_ask_dollars": "0.0200",
    "no_bid_dollars": "0.9800",
    "no_ask_dollars": "0.9900",
    "last_price_dollars": "0.0100",
    "previous_price_dollars": "0.0100",
    "yes_bid_size_fp": "120.00",
    "yes_ask_size_fp": "45.00",
    "volume_fp": "3022.04",
    "volume_24h_fp": "0.00",
    "open_interest_fp": "15842.78",
    "settlement_value_dollars": "0.0000",
    "result": "no",
    "rules_primary": "If the CPI rises above 1.0%, this market resolves YES.",
    "rules_secondary": "Resolves from the BLS Consumer Price Index release.",
    "price_level_structure": "linear_cent",
    "price_ranges": [{"start": "0.00", "end": "1.00", "step": "0.01"}],
    "exchange_index": 0,
}

# Two records that agree on time, price and size and differ only by trade_id.
TRADE_BODY = {
    "trade_id": "f56482f8-f9be-48ad-4322-18c23b5c65e7",
    "ticker": "KXCPI-26JUN-T-0.3",
    "count_fp": "10.00",
    "yes_price_dollars": "0.9200",
    "no_price_dollars": "0.0800",
    "taker_side": "no",
    "taker_outcome_side": "no",
    "taker_book_side": "ask",
    "created_time": "2026-07-14T12:18:04.434321Z",
    "is_block_trade": False,
}
TRADE_BODY_TWIN = {**TRADE_BODY, "trade_id": "bad689c8-b16b-4fd1-7b18-bbb046f68ac8"}

# Historical candle: legacy plain names, explicit nulls when no trade occurred.
HISTORICAL_CANDLES = {
    "ticker": "KXCPI-26JUN-T-0.3",
    "candlesticks": [
        {
            "end_period_ts": 1781337600,
            "open_interest": "576.00",
            "price": {
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "mean": None,
                "previous": "0.9900",
            },
            "volume": "0.00",
            "yes_ask": {"open": "0.9900", "high": "0.9900", "low": "0.9900", "close": "0.9900"},
            "yes_bid": {"open": "0.9300", "high": "0.9300", "low": "0.9000", "close": "0.9000"},
        },
        {
            "end_period_ts": 1781366400,
            "open_interest": "576.00",
            "price": {
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "mean": None,
                "previous": "0.9900",
            },
            "volume": "0.00",
            "yes_ask": {"open": "0.9900", "high": "0.9900", "low": "0.9900", "close": "0.9900"},
            "yes_bid": {"open": "0.9000", "high": "0.9000", "low": "0.9000", "close": "0.9000"},
        },
        {
            "end_period_ts": 1781388000,
            "open_interest": "576.00",
            "price": {
                "open": "0.9800",
                "high": "0.9900",
                "low": "0.9800",
                "close": "0.9900",
                "mean": "0.9900",
                "previous": "0.9900",
            },
            "volume": "23.00",
            "yes_ask": {"open": "0.9900", "high": "0.9900", "low": "0.9900", "close": "0.9900"},
            "yes_bid": {"open": "0.9000", "high": "0.9000", "low": "0.9000", "close": "0.9000"},
        },
    ],
}

# Live candle: modern nested names, absent keys when no trade occurred.
LIVE_CANDLES = {
    "candlesticks": [
        {
            "end_period_ts": 1789002000,
            "open_interest_fp": "1717.53",
            "price": {"previous_dollars": "0.0100"},
            "volume_fp": "303.55",
            "yes_ask": {
                "open_dollars": "0.0100",
                "high_dollars": "0.0100",
                "low_dollars": "0.0100",
                "close_dollars": "0.0100",
            },
            "yes_bid": {
                "open_dollars": "0.0000",
                "high_dollars": "0.0000",
                "low_dollars": "0.0000",
                "close_dollars": "0.0000",
            },
        }
    ]
}

ORDERBOOK_BODY = {
    "orderbook_fp": {
        "yes_dollars": [["0.4100", "120.00"], ["0.4000", "300.00"]],
        "no_dollars": [["0.5700", "50.00"]],
    }
}

# A real calendar page: unzoned 12-hour times plus the note that fixes the zone.
CALENDAR_HTML = """
<table>
<tr><td>Friday, September 4, 2026</td><td>08:30 AM</td>
<td><b>Employment Situation</b> for August 2026</td></tr>
<tr><td>Friday, September 11, 2026</td><td>08:30 AM</td>
<td><b>Consumer Price Index</b> for August 2026</td></tr>
<tr><td>Thursday, September 24, 2026</td><td>10:00 AM</td>
<td><b>Employee Tenure</b> for Biennial 2026</td></tr>
</table>
<p>NOTE: All times on calendar are Eastern Time.</p>
"""

# The same page without the zone declaration: unusable, must be refused.
CALENDAR_HTML_NO_ZONE = CALENDAR_HTML.replace(
    "NOTE: All times on calendar are Eastern Time.", "NOTE: Times are local."
)

CPI_HTML = """
<title>Consumer Price Index News Release - 2026 M07 Results</title>
<pre>
Transmission of material in this release is embargoed until
8:30 a.m. (ET) Wednesday, August 12, 2026      USDL-26-1378
CONSUMER PRICE INDEX - JULY 2026
The Consumer Price Index for All Urban Consumers (CPI-U) increased 0.1 percent on a seasonally adjusted basis in July
after falling 0.4 percent in June.
The index for all items less food and energy rose 0.2 percent after being unchanged in June.
The all items index rose 3.4 percent for the 12 months ending July after rising 3.5 percent.
The all items less food and energy index rose 2.5 percent over the year, following a 2.6-percent increase.
The Consumer Price Index for All Urban Consumers (CPI-U) increased 3.4 percent over the last 12 months to an index level
of 333.918 (1982-84=100).
</pre>
"""

#: The payload the audit-path routes serve for the January 2025 CPI cohort.
#: ``CPI_HTML`` above is a parser fixture for a later period; serving it to a
#: cohort scheduled in January 2025 put the payload's own embargo line in
#: direct contradiction with the cohort instant. This page states the cohort's
#: own schedule so the audit's comparison is a real one. It is a hand-written
#: fixture page, not a transcription of the published release.
CPI_2025_01_HTML = """
<title>Consumer Price Index News Release - 2024 M12 Results</title>
<pre>
Transmission of material in this release is embargoed until
8:30 a.m. (ET) Wednesday, January 15, 2025      USDL-25-0021
CONSUMER PRICE INDEX - DECEMBER 2024
The Consumer Price Index for All Urban Consumers (CPI-U) increased 0.1 percent on a seasonally adjusted basis in December
after rising 0.3 percent in November.
The index for all items less food and energy rose 0.2 percent after increasing 0.3 percent in November.
The all items index rose 2.9 percent for the 12 months ending December.
</pre>
"""

EMPSIT_HTML = """
<title>Employment Situation News Release - 2026 M08 Results</title>
<pre>
Transmission of material in this news release is embargoed until                       USDL-26-1435
8:30 a.m. (ET) Friday, September 4, 2026
THE EMPLOYMENT SITUATION - AUGUST 2026
Total nonfarm payroll employment increased by 162,000 in August, and the unemployment rate was
unchanged at 4.1 percent.
In August, average hourly earnings for all employees on private nonfarm payrolls rose by 10
cents, or 0.3 percent, to $37.75. Over the year, average hourly earnings have increased by 3.1
percent.
The average workweek for all employees on private nonfarm payrolls edged up by 0.1 hour to
34.4 hours in August.
The change in total nonfarm payroll employment for June was revised up by 11,000, from
+20,000 to +31,000, and the change for July was revised up by 44,000, from -23,000 to +21,000.
With these revisions, employment in June and July combined is 55,000 higher than previously
reported.
</pre>
"""

#: The payload the audit-path routes serve for the January 2025 employment cohort.
#: Same reason as ``CPI_2025_01_HTML``: the scheduler fixture above states a
#: schedule in a different year, which the payload's own embargo line would
#: contradict.
EMPSIT_2025_01_HTML = """
<title>Employment Situation News Release - 2024 M12 Results</title>
<pre>
Transmission of material in this news release is embargoed until
8:30 a.m. (ET) Friday, January 10, 2025      USDL-25-0002
THE EMPLOYMENT SITUATION -- DECEMBER 2024
Total nonfarm payroll employment increased by 256,000 in December, and the unemployment rate
was unchanged at 4.1 percent.
</pre>
"""

ARCHIVE_INDEX_HTML = """
<a href="/news.release/archives/cpi_08122026.htm">July 2026 Consumer Price Index</a>
<a href="/news.release/archives/cpi_07142026.htm">June 2026 Consumer Price Index</a>
October 2025 Consumer Price Index - Not published because of 2025 lapse in federal government appropriations
"""

PRICE_HISTORY_BODY = {
    "history": [
        {"t": 1789000000, "p": 0.42},
        {"t": 1789003600, "p": 0.43},
        {"t": 1789007200, "p": 0.41},
    ],
    "fidelity": 60,
}

POLYMARKET_BOOK_BODY = {
    "market": "0xabc",
    "asset_id": "71321045679252212594626385532706912750332728571942532289631379312455583992563",
    "bids": [{"price": "0.41", "size": "120"}, {"price": "0.40", "size": "300"}],
    "asks": [{"price": "0.43", "size": "75"}],
    "hash": "0xbookhash",
    "timestamp": "1789000000000",
}


class FakeResponse:
    """Enough of the httpx response surface for the transport."""

    def __init__(
        self,
        status_code: int,
        content: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = {k.lower(): v for k, v in (headers or {}).items()}


# A fixture entry is (status, body) or (status, body, headers). A body may be a
# dict/list (serialised as JSON), a str, or raw bytes.
Entry = tuple


def ok(body: Any, headers: dict[str, str] | None = None) -> Entry:
    return (200, body, headers or {})


class FakeHttpClient:
    """Replays queued responses keyed by URL fragment, in order.

    A value may be a single entry or a list of entries, so "429 then 200" is
    expressed directly. An unregistered URL is a hard error, which keeps a test
    from silently exercising an endpoint it never declared.
    """

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.calls: list[str] = []
        self.headers_sent: list[dict[str, str] | None] = []
        self._indices: dict[str, int] = {}

    def _lookup(self, url: str) -> tuple[str, Any]:
        if url in self.routes:
            return url, self.routes[url]
        for key, value in self.routes.items():
            if key in url:
                return key, value
        raise AssertionError(f"unexpected request in test: {url}")

    def _pop(self, url: str) -> Entry:
        key, value = self._lookup(url)
        if isinstance(value, list):
            index = self._indices.get(key, 0)
            if index >= len(value):
                raise AssertionError(f"exhausted fixture responses for {url}")
            self._indices[key] = index + 1
            return value[index]
        return value

    def get(self, url: str, headers: dict[str, str] | None = None) -> FakeResponse:
        self.calls.append(url)
        self.headers_sent.append(headers)
        status, body, extra = (*self._pop(url), {})[:3]
        if isinstance(body, (dict, list)):
            content = json.dumps(body).encode("utf-8")
        elif isinstance(body, str):
            content = body.encode("utf-8")
        else:
            content = body
        return FakeResponse(status, content, extra)

    def close(self) -> None:
        pass


@pytest.fixture()
def store(tmp_path: pathlib.Path) -> Any:
    return RawStore(tmp_path / "raw")


def make_transport(store: Any, client: FakeHttpClient, **policy: Any) -> HttpTransport:
    """Transport with pacing removed and sleeps recorded rather than performed."""

    slept: list[float] = []
    kwargs = {"min_interval_seconds": 0.0}
    kwargs.update(policy)
    transport = HttpTransport(
        store,
        client=client,  # type: ignore[arg-type]
        policy=RetryPolicy(**kwargs),
        sleep=slept.append,
        now=lambda: dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC),
    )
    transport.slept = slept  # type: ignore[attr-defined]
    return transport


def make_clock() -> Clock:
    received = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC)
    return Clock.captured(
        dt.datetime(2026, 9, 13, 11, 59, tzinfo=dt.UTC), received, monotonic_ns=1234
    )


def make_provenance() -> Provenance:
    return Provenance(raw_hash="a" * 64, record_id="rec-1", source="test")


def test_transport_archives_body_before_returning_it(store: Any) -> None:
    client = FakeHttpClient({"/historical/cutoff": ok(CUTOFF_BODY)})
    transport = make_transport(store, client)
    envelope = transport.get(
        "https://example.test/historical/cutoff",
        source="kalshi.historical.cutoff",
        record_id="cutoff",
    )

    assert envelope.status_code == 200
    assert store.get(envelope.provenance.raw_hash) == json.dumps(CUTOFF_BODY).encode("utf-8")


def test_transport_retries_429_then_succeeds_with_bounded_backoff(store: Any) -> None:
    client = FakeHttpClient(
        {
            "/markets": [
                (429, {"error": "too many requests"}),
                ok(MARKET_BODY),
            ]
        }
    )
    transport = make_transport(store, client)
    envelope = transport.get("https://example.test/markets", source="kalshi.markets")

    assert envelope.status_code == 200
    assert envelope.attempts == 2
    assert len(client.calls) == 2
    # Kalshi documents no Retry-After on 429, so exponential backoff is applied.
    assert transport.slept  # type: ignore[attr-defined]
    assert transport.slept[0] > 0  # type: ignore[attr-defined]


def test_transport_stops_after_bounded_attempts_and_records_blocked(store: Any) -> None:
    client = FakeHttpClient({"/markets": [(429, {}), (429, {}), (429, {})]})
    transport = make_transport(store, client, attempts=3)

    with pytest.raises(TransportError) as excinfo:
        transport.get("https://example.test/markets", source="kalshi.markets")

    records = excinfo.value.as_blocked_record()
    assert len(client.calls) == 3, "retry count must be bounded"
    assert records["recorded"] is True
    assert records["empty_result"] is False, (
        "an exhausted retry budget is an access failure, never an empty result"
    )
    assert records["status_code"] == 429


def test_transport_archives_the_body_of_a_terminal_failure(store: Any) -> None:
    client = FakeHttpClient({"/markets": [(503, {})]})
    transport = make_transport(store, client, attempts=1)

    with pytest.raises(TransportError) as excinfo:
        transport.get("https://example.test/markets", source="kalshi.markets")

    assert excinfo.value.reason == "http_status"
    blocked = excinfo.value.as_blocked_record()
    assert blocked["payload_hash"] is not None, "the failing body is archived too"
    assert store.get(blocked["payload_hash"]) == b"{}"


def test_transport_stops_rather_than_retrying_early_when_retry_after_exceeds_budget(
    store: Any,
) -> None:
    """Honouring Retry-After means not retrying sooner than the server asked."""
    client = FakeHttpClient(
        {"/markets": [(429, {"error": "too many requests"}, {"Retry-After": "300"})]}
    )
    transport = make_transport(store, client, attempts=5, max_retry_after_seconds=10.0)

    with pytest.raises(TransportError) as excinfo:
        transport.get("https://example.test/markets", source="kalshi.markets")

    assert excinfo.value.reason == "retry_after_exceeds_budget"
    assert excinfo.value.attempts == 1
    assert len(client.calls) == 1, "no early retry was attempted"
    assert transport.slept == []  # type: ignore[attr-defined]
    assert excinfo.value.payload_hash is not None


def test_retry_after_header_seconds_and_http_date_forms() -> None:
    now = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC)
    assert retry_after_seconds({"Retry-After": "30"}, now=now) == 30.0
    parsed = retry_after_seconds({"retry-after": "Sun, 13 Sep 2026 12:00:30 GMT"}, now=now)
    assert parsed == pytest.approx(30.0)
    assert retry_after_seconds({}, now=now) is None
    # A past date cannot ask for a negative wait.
    assert retry_after_seconds({"Retry-After": "Sun, 13 Sep 2026 11:00:00 GMT"}, now=now) == 0.0


def test_build_url_omits_unset_filters() -> None:
    url = build_url(
        "https://example.test/markets",
        {"series_ticker": "KXCPI", "status": None, "limit": 100},
    )
    assert "status" not in url
    assert "series_ticker=KXCPI" in url
    assert build_url("https://example.test/markets", {}) == "https://example.test/markets"


def test_transport_never_sends_credentials(store: Any) -> None:
    client = FakeHttpClient({"/historical/cutoff": ok(CUTOFF_BODY)})
    transport = make_transport(store, client)
    transport.get("https://example.test/historical/cutoff", source="kalshi.historical.cutoff")
    for headers in client.headers_sent:
        assert headers is None or not any(
            key.lower() in {"authorization", "x-api-key", "api-key"} for key in headers
        )


def test_blocked_record_is_never_an_empty_success() -> None:
    record = blocked_record(
        url="https://example.test/x",
        status_code=403,
        reason="http_status",
        attempts=1,
        payload_hash=None,
        observed_at=dt.datetime(2026, 9, 13, tzinfo=dt.UTC),
    )
    assert record["empty_result"] is False
    assert record["recorded"] is True


def test_fixed_point_dollars_are_exact_and_optional() -> None:
    """``parse_fixed_point_dollars`` is the boundary where a binary float would lose the wire value.

    Each declared field carries a four-decimal wire value that no float can hold
    exactly, so a Decimal at this boundary round-trips the digits the venue sent.
    """
    assert parse_fixed_point_dollars("0.4300", "f") == Decimal("0.4300")
    assert parse_fixed_point_dollars("1.0000", "f") == Decimal("1.0000")
    assert parse_fixed_point_dollars(None, "f") is None
    assert parse_fixed_point_dollars("", "f") is None
    assert parse_fixed_point_dollars("-0.0500", "f") == Decimal("-0.0500")


def test_malformed_decimal_raises_rather_than_defaulting_to_zero() -> None:
    with pytest.raises(WireShapeError):
        parse_fixed_point_dollars("not-a-price", "yes_bid_dollars")


def test_trade_prices_and_sizes_stay_decimal(store: Any) -> None:
    trade = normalize_kalshi_trade(TRADE_BODY, clock=make_clock(), provenance=make_provenance())
    assert trade.price == Decimal("0.9200")
    assert trade.size == Decimal("10.00")
    assert isinstance(trade.price, Decimal)
    assert trade.trade_id == TRADE_BODY["trade_id"]
    assert trade.is_block is False
    assert trade.aggressor is None, "direction fields describe the taker's position, not aggression"


def test_contract_normalization_uses_strike_not_price_as_threshold(store: Any) -> None:
    contract = normalize_kalshi_contract(MARKET_BODY, provenance=make_provenance())
    assert isinstance(contract, Contract)
    assert contract.venue == "kalshi"
    assert contract.contract_id == "KXCPI-26AUG-T1.0"
    assert contract.threshold == Decimal("1")
    # 'greater' is a strict comparison and maps onto the frozen enum member.
    assert contract.operator is Operator.ABOVE
    assert contract.operator.is_strict is True
    assert contract.units == "percent_mom_change"
    assert contract.family == "cpi"
    assert contract.source == "BLS"
    assert contract.close_time == dt.datetime(2026, 9, 11, 12, 25, tzinfo=dt.UTC)
    # A market record publishes no instant for the version of its rule text, so the
    # field stays null rather than borrowing the market's own creation time.
    assert contract.rule_available_at is None
    # The lifecycle instants the record does state remain readable on their own
    # fields, which is where a market's dates belong.
    assert contract.open_time == dt.datetime(2026, 7, 23, 21, 0, tzinfo=dt.UTC)

    # Only an explicitly supplied rule-observation instant sets availability, and
    # the field then carries that instant rather than the market's creation time.
    observed = dt.datetime(2026, 7, 24, 9, 15, tzinfo=dt.UTC)
    observed_contract = normalize_kalshi_contract(
        MARKET_BODY, provenance=make_provenance(), rule_observed_at=observed
    )
    assert observed_contract.rule_available_at == observed
    assert observed_contract.rule_available_at != observed_contract.open_time


def test_contract_reads_rule_facts_and_leaves_bounds_unset() -> None:
    """Facts the rule text states are read; a threshold market carries no bounds."""
    contract = normalize_kalshi_contract(MARKET_BODY, provenance=make_provenance())
    assert contract.lower is None and contract.upper is None
    # The rule text names the source, so it is read rather than invented.
    assert contract.exceptional_policy == "binary_default"
    assert contract.settlement == "cash"
    assert contract.currency == "USD"


def test_audit_statistic_name_falls_back_for_an_unlisted_series() -> None:
    """The audit-side statistic label keeps its own fallback, unlike a matching field."""
    assert statistic_for_series("KXUNKNOWN") == UNMAPPED


def test_strike_type_without_a_frozen_operator_is_refused() -> None:
    """An outside-range payoff has no representable operator, so it is refused."""
    with pytest.raises(WireShapeError, match="no frozen operator"):
        normalize_kalshi_contract(
            {**MARKET_BODY, "strike_type": "not_between"},
            provenance=make_provenance(),
        )
    with pytest.raises(WireShapeError):
        normalize_kalshi_contract(
            {**MARKET_BODY, "strike_type": "brand_new_type"},
            provenance=make_provenance(),
        )


def test_bounded_market_normalizes_to_a_range_operator() -> None:
    bounded = {
        **MARKET_BODY,
        "ticker": "KXRANGE-26AUG-B",
        "strike_type": "between",
        "floor_strike": 2,
        "cap_strike": 4,
    }
    contract = normalize_kalshi_contract(bounded, provenance=make_provenance())
    assert contract.operator is Operator.RANGE
    assert contract.lower == Decimal("2") and contract.upper == Decimal("4")
    # A range expresses both ends as bounds, so no single threshold is left behind.
    assert contract.threshold is None


def test_rule_hash_ignores_lifecycle_times_but_catches_rule_edits() -> None:
    first = normalize_kalshi_contract(MARKET_BODY, provenance=make_provenance())
    moved = normalize_kalshi_contract(
        {**MARKET_BODY, "close_time": "2026-10-01T12:25:00Z"},
        provenance=make_provenance(),
    )
    edited = normalize_kalshi_contract(
        {**MARKET_BODY, "rules_primary": "If the CPI rises above 2.0%, this resolves YES."},
        provenance=make_provenance(),
    )

    assert first.rule_hash == moved.rule_hash
    assert first.rule_hash != edited.rule_hash

    lifecycle_only = compare_contract_versions(first, moved)
    assert lifecycle_only["rule_text_changed"] is False
    assert lifecycle_only["lifecycle_changed"] is True
    assert lifecycle_only["comparable"] is True

    rule_change = compare_contract_versions(first, edited)
    assert rule_change["rule_text_changed"] is True
    assert rule_change["comparable"] is False, (
        "a changed rule text means a different instrument, so histories must not be pooled"
    )


def test_closed_market_is_distinguishable_from_a_quiet_one() -> None:
    contract = normalize_kalshi_contract(MARKET_BODY, provenance=make_provenance())
    after_close = dt.datetime(2026, 9, 11, 13, 0, tzinfo=dt.UTC)
    before_close = dt.datetime(2026, 9, 11, 12, 0, tzinfo=dt.UTC)

    assert contract_is_closed(contract, at=after_close) is True
    assert contract_is_closed(contract, at=before_close) is False


def test_duplicate_trades_are_preserved_when_ids_differ(store: Any) -> None:
    client = FakeHttpClient(
        {"/historical/trades": ok({"trades": [TRADE_BODY, TRADE_BODY_TWIN], "cursor": None})}
    )
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    result = kalshi.get_historical_trades(ticker="KXCPI-26JUN-T-0.3", limit=2)

    assert len(result.items) == 2, (
        "two genuinely repeated records with distinct trade_ids must not collapse"
    )
    assert result.dups_total == 0
    assert result.dups_within_pages == 0
    assert result.repeated_ids_across_pages == 0
    assert result.complete is True


def test_same_id_twice_in_one_page_is_one_event_with_no_across_page_repeat(
    store: Any,
) -> None:
    """A retransmitted id is one event; it never counts as a boundary anomaly."""
    client = FakeHttpClient(
        {"/historical/trades": ok({"trades": [TRADE_BODY, TRADE_BODY], "cursor": None})}
    )
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    result = kalshi.get_historical_trades(ticker="KXCPI-26JUN-T-0.3", limit=2)

    assert len(result.items) == 1
    assert result.dups_within_pages == 1
    assert result.repeated_ids_across_pages == 0, (
        "the repeat never crossed a page boundary, so the audit's across-page metric must stay zero"
    )
    assert result.page_counts == (2,)


def test_pagination_counts_stay_consistent_across_a_boundary(store: Any) -> None:
    page_one = {"trades": [TRADE_BODY, TRADE_BODY_TWIN], "cursor": "CUR1"}
    page_two = {
        "trades": [
            {**TRADE_BODY, "trade_id": "third-id"},
            {**TRADE_BODY, "trade_id": "fourth-id"},
        ],
        "cursor": None,
    }
    client = FakeHttpClient({"/historical/trades": [ok(page_one), ok(page_two)]})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    result = kalshi.get_historical_trades(ticker="KXCPI-26JUN-T-0.3", limit=2)

    assert result.page_counts == (2, 2)
    assert cursor_sets_consistent(result) is True
    assert len(result.items) == 4
    assert result.distinct_ids_across_pages == 4
    assert result.repeated_ids_across_pages == 0
    assert result.dups_within_pages == 0
    assert result.dups_total == 0
    assert result.complete is True

    # The same ids reappearing on page two must be reported, not silently dropped.
    duplicated = FakeHttpClient(
        {
            "/historical/trades": [
                ok({"trades": [TRADE_BODY, TRADE_BODY_TWIN], "cursor": "CUR1"}),
                ok({"trades": [TRADE_BODY, TRADE_BODY_TWIN], "cursor": None}),
            ]
        }
    )
    kalshi2 = KalshiClient(store, transport=make_transport(store, duplicated))
    repeated = kalshi2.get_historical_trades(ticker="KXCPI-26JUN-T-0.3", limit=2)
    assert repeated.repeated_ids_across_pages == 2
    assert repeated.dups_within_pages == 0, (
        "both repeats are second deliveries of an id from page one"
    )
    assert repeated.dups_total == 2
    assert len(repeated.items) == 2


def test_repeating_cursor_stops_instead_of_looping(store: Any) -> None:
    body = {"trades": [TRADE_BODY], "cursor": "SAME_CURSOR"}
    client = FakeHttpClient({"/historical/trades": [ok(body), ok(body), ok(body), ok(body)]})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    result = kalshi.get_historical_trades(ticker="KXCPI-26JUN-T-0.3", limit=1, max_pages=10)

    assert result.stop_reason == "repeated_page_body"
    assert result.complete is False, "a repeated page is not a complete result set"
    assert len(client.calls) < 10, "loop detection must fire before the page bound"


def test_cursor_loop_detected_when_page_body_changes(store: Any) -> None:
    first = {"trades": [TRADE_BODY], "cursor": "LOOPED"}
    second = {"trades": [{**TRADE_BODY, "trade_id": "other"}], "cursor": "LOOPED"}
    client = FakeHttpClient({"/historical/trades": [ok(first), ok(second), ok(second)]})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    result = kalshi.get_historical_trades(ticker="KXCPI-26JUN-T-0.3", limit=1, max_pages=10)

    assert result.stop_reason == "cursor_loop"
    assert result.complete is False
    assert result.checkpoint.stop_reason == "cursor_loop"


def test_empty_page_without_cursor_is_a_complete_empty_result(store: Any) -> None:
    client = FakeHttpClient({"/historical/trades": ok({"trades": [], "cursor": None})})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    result = kalshi.get_historical_trades(ticker="KXCPI-26JUN-T-0.3")

    assert result.complete is True
    assert result.stop_reason == "exhausted"
    assert result.items == ()


def test_empty_page_that_still_advertises_a_cursor_is_not_complete(store: Any) -> None:
    client = FakeHttpClient({"/historical/trades": ok({"trades": [], "cursor": "MORE"})})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    result = kalshi.get_historical_trades(ticker="KXCPI-26JUN-T-0.3")

    assert result.complete is False
    assert result.stop_reason == "empty_page_with_cursor"


def test_page_count_exceeding_max_pages_is_reported_as_incomplete(store: Any) -> None:
    bodies = [
        {"trades": [{**TRADE_BODY, "trade_id": f"id-{i}"}], "cursor": f"CUR{i}"} for i in range(5)
    ]
    client = FakeHttpClient({"/historical/trades": [ok(b) for b in bodies]})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    result = kalshi.get_historical_trades(ticker="X", limit=1, max_pages=3)

    assert result.stop_reason == "max_pages"
    assert result.complete is False
    assert result.checkpoint.cursor is not None, "a resume point is retained"


def test_status_all_is_refused_before_it_reaches_the_network(store: Any) -> None:
    client = FakeHttpClient({"/markets": ok(MARKET_BODY)})
    kalshi = KalshiClient(store, transport=make_transport(store, client))

    with pytest.raises(ValueError):
        kalshi.list_markets(series_ticker="KXCPI", status="all")
    assert client.calls == [], "an undocumented status must not be sent at all"


def test_mutually_exclusive_historical_filters_are_refused_locally(store: Any) -> None:
    client = FakeHttpClient({"/historical/markets": ok({"markets": [], "cursor": None})})
    kalshi = KalshiClient(store, transport=make_transport(store, client))

    with pytest.raises(ValueError):
        kalshi.list_historical_markets(series_ticker="KXCPI", event_ticker="KXCPI-26JUN")
    assert client.calls == []


def test_cutoff_parses_iso_strings_despite_documented_int_names(store: Any) -> None:
    client = FakeHttpClient({"/historical/cutoff": ok(CUTOFF_BODY)})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    cutoff = kalshi.get_historical_cutoff()

    assert cutoff.market_settled_ts == dt.datetime(2026, 7, 15, tzinfo=dt.UTC)
    assert cutoff.trades_created_ts == dt.datetime(2026, 7, 15, tzinfo=dt.UTC)
    payload = cutoff.as_dict()
    assert payload["source_format"] == "iso8601_string"
    assert payload["documented_format"] == "int64_unix"


def test_cutoff_missing_boundary_refuses_to_default(store: Any) -> None:
    partial = {k: v for k, v in CUTOFF_BODY.items() if k != "market_settled_ts"}
    client = FakeHttpClient({"/historical/cutoff": ok(partial)})
    kalshi = KalshiClient(store, transport=make_transport(store, client))

    with pytest.raises(WireShapeError):
        kalshi.get_historical_cutoff()


def test_cutoff_null_boundary_is_unknown_rather_than_zero(store: Any) -> None:
    nulled = {**CUTOFF_BODY, "trades_created_ts": None}
    client = FakeHttpClient({"/historical/cutoff": ok(nulled)})
    kalshi = KalshiClient(store, transport=make_transport(store, client))

    with pytest.raises(WireShapeError):
        kalshi.get_historical_cutoff()


def test_advancing_cutoff_moves_a_window_between_partitions(store: Any) -> None:
    """The same window must resolve differently as the cutoff advances."""
    window_start = dt.datetime(2026, 7, 1, tzinfo=dt.UTC)
    window_end = dt.datetime(2026, 7, 10, tzinfo=dt.UTC)

    early = {**CUTOFF_BODY, "market_settled_ts": "2026-06-01T00:00:00Z"}
    client = FakeHttpClient({"/historical/cutoff": ok(early)})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    live_window = kalshi.resolve_partition(window_start, window_end)
    assert live_window.partition == "live"

    later = {**CUTOFF_BODY, "market_settled_ts": "2026-07-20T00:00:00Z"}
    client2 = FakeHttpClient({"/historical/cutoff": ok(later)})
    kalshi2 = KalshiClient(store, transport=make_transport(store, client2))
    historical_window = kalshi2.resolve_partition(window_start, window_end)
    assert historical_window.partition == "historical"
    assert historical_window.available is True


def test_straddling_window_requires_both_partitions(store: Any) -> None:
    straddle = {**CUTOFF_BODY, "market_settled_ts": "2026-07-05T00:00:00Z"}
    client = FakeHttpClient({"/historical/cutoff": ok(straddle)})
    kalshi = KalshiClient(store, transport=make_transport(store, client))

    decision = kalshi.resolve_partition(
        dt.datetime(2026, 7, 1, tzinfo=dt.UTC),
        dt.datetime(2026, 7, 10, tzinfo=dt.UTC),
    )
    assert decision.partition == "straddling"
    assert "historical/markets" in decision.endpoint
    assert "markets" in decision.endpoint.split(" and ")[1]
    assert decision.detail["partition_is_mutually_exclusive"] is False


def test_historical_and_live_candles_use_different_leaf_names(store: Any) -> None:
    provenance = make_provenance()
    historical = normalize_kalshi_candle(
        HISTORICAL_CANDLES["candlesticks"][2],
        contract_id="KXCPI-26JUN-T-0.3",
        interval_minutes=60,
        schema_flavour=CANDLE_SCHEMA_HISTORICAL,
        clock=make_clock(),
        provenance=provenance,
    )
    live = normalize_kalshi_candle(
        LIVE_CANDLES["candlesticks"][0],
        contract_id="KXCPI-26AUG-T1.0",
        interval_minutes=60,
        schema_flavour=CANDLE_SCHEMA_LIVE,
        clock=make_clock(),
        provenance=provenance,
    )

    assert historical.bid_close == Decimal("0.9000")
    assert historical.ask_close == Decimal("0.9900")
    assert historical.trade_close == Decimal("0.9900")
    assert historical.volume == Decimal("23.00")

    assert live.bid_close == Decimal("0.0000")
    assert live.ask_close == Decimal("0.0100")
    assert live.trade_close is None
    assert live.volume == Decimal("303.55")


def test_candle_distinguishes_executable_quotes_from_trade_ohlc(store: Any) -> None:
    no_trade = normalize_kalshi_candle(
        HISTORICAL_CANDLES["candlesticks"][0],
        contract_id="KXCPI-26JUN-T-0.3",
        interval_minutes=60,
        schema_flavour=CANDLE_SCHEMA_HISTORICAL,
        clock=make_clock(),
        provenance=make_provenance(),
    )
    assert no_trade.has_two_sided_quote is True
    assert no_trade.bid_close == Decimal("0.9000")
    assert no_trade.ask_close == Decimal("0.9900")
    assert no_trade.trade_ohlc_present is False
    assert no_trade.trade_close is None
    assert no_trade.volume == Decimal("0.00")
    assert no_trade.book_depth_available is False

    quotes = candles_to_quotes([no_trade])
    assert len(quotes) == 1
    assert quotes[0].bid == Decimal("0.9000")
    assert quotes[0].ask == Decimal("0.9900")
    # Depth is absent, not zero: a candle publishes no sizes.
    assert quotes[0].bid_size is None
    assert quotes[0].ask_size is None
    assert quotes[0].last_verified == no_trade.period_end


def test_absent_trade_keys_and_explicit_nulls_both_mean_no_trade(store: Any) -> None:
    live_no_trade = normalize_kalshi_candle(
        LIVE_CANDLES["candlesticks"][0],
        contract_id="X",
        interval_minutes=60,
        schema_flavour=CANDLE_SCHEMA_LIVE,
        clock=make_clock(),
        provenance=make_provenance(),
    )
    historical_no_trade = normalize_kalshi_candle(
        HISTORICAL_CANDLES["candlesticks"][0],
        contract_id="Y",
        interval_minutes=60,
        schema_flavour=CANDLE_SCHEMA_HISTORICAL,
        clock=make_clock(),
        provenance=make_provenance(),
    )
    assert live_no_trade.trade_close is None
    assert historical_no_trade.trade_close is None
    assert live_no_trade.trade_ohlc_present is historical_no_trade.trade_ohlc_present is False


def test_unsupported_candle_interval_is_refused() -> None:
    with pytest.raises(ValueError):
        inspect_candle_spacing([], interval_minutes=5)
    assert 5 not in CANDLE_INTERVALS_MINUTES


def test_candle_holes_are_detected_and_invalidate_replay() -> None:
    spacing = inspect_candle_spacing(
        HISTORICAL_CANDLES["candlesticks"],
        interval_minutes=60,
        start_ts=1781337600,
        end_ts=1781388000,
    )
    # The fixture skips 1781377200, a full grid period.
    assert spacing.holes_present is True
    assert 1781377200 in spacing.missing_periods
    assert spacing.usable_for_replay is False
    assert spacing.candle_count == 3
    assert spacing.requested_spacing_seconds == 3600


def test_contiguous_candles_are_usable_but_still_not_book_depth() -> None:
    contiguous = [
        {
            "end_period_ts": 1781337600,
            "price": {},
            "volume": "0.00",
            "yes_bid": {"close": "0.50"},
            "yes_ask": {"close": "0.51"},
            "open_interest": "1.00",
        },
        {
            "end_period_ts": 1781341200,
            "price": {},
            "volume": "0.00",
            "yes_bid": {"close": "0.50"},
            "yes_ask": {"close": "0.51"},
            "open_interest": "1.00",
        },
        {
            "end_period_ts": 1781344800,
            "price": {},
            "volume": "0.00",
            "yes_bid": {"close": "0.50"},
            "yes_ask": {"close": "0.51"},
            "open_interest": "1.00",
        },
    ]
    spacing = inspect_candle_spacing(
        contiguous, interval_minutes=60, start_ts=1781337600, end_ts=1781344800
    )
    assert spacing.holes_present is False
    assert spacing.usable_for_replay is True
    assert spacing.spacing_consistent is True
    assert spacing.covers_window is True
    assert "not a complete order-book history" in spacing.note


def test_off_grid_timestamps_are_reported() -> None:
    irregular = [
        {
            "end_period_ts": 1781337600,
            "price": {},
            "volume": "0",
            "yes_bid": {},
            "yes_ask": {},
            "open_interest": "0",
        },
        {
            "end_period_ts": 1781337900,
            "price": {},
            "volume": "0",
            "yes_bid": {},
            "yes_ask": {},
            "open_interest": "0",
        },
    ]
    spacing = inspect_candle_spacing(irregular, interval_minutes=60)
    assert spacing.off_grid
    assert spacing.spacing_consistent is False
    assert spacing.usable_for_replay is False


def test_candle_window_not_covered_is_reported_not_assumed_quiet() -> None:
    candles = [
        {
            "end_period_ts": 1781366400,
            "price": {},
            "volume": "0",
            "yes_bid": {},
            "yes_ask": {},
            "open_interest": "0",
        }
    ]
    spacing = inspect_candle_spacing(
        candles, interval_minutes=60, start_ts=1781337600, end_ts=1781388000
    )
    assert spacing.covers_window is False
    assert "not evidence that" in spacing.note


def test_candle_missing_timestamp_cannot_be_inferred(store: Any) -> None:
    client = FakeHttpClient(
        {
            "/historical/markets/KXCPI-26JUN-T-0.3/candlesticks": ok(
                {"candlesticks": [{"volume": "1.00"}]}
            )
        }
    )
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    with pytest.raises(WireShapeError):
        kalshi.get_historical_candles("KXCPI-26JUN-T-0.3", start_ts=1, end_ts=2, period_interval=60)


def test_candle_request_requires_both_timestamps_and_documented_interval(store: Any) -> None:
    client = FakeHttpClient({"/historical/markets/X/candlesticks": ok({"candlesticks": []})})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    with pytest.raises(ValueError):
        kalshi.get_historical_candles("X", start_ts=1, end_ts=2, period_interval=5)
    with pytest.raises(ValueError):
        kalshi.get_historical_candles("X", start_ts=2, end_ts=1, period_interval=60)
    assert client.calls == []


def test_orderbook_snapshot_derives_asks_from_no_bids(store: Any) -> None:
    events = normalize_kalshi_orderbook_snapshot(
        ORDERBOOK_BODY,
        contract_id="KXCPI-26AUG-T1.0",
        clock=make_clock(),
        provenance=make_provenance(),
    )
    event = events[0]
    assert isinstance(event, BookEvent)
    assert event.kind == "snapshot"
    assert event.bids == (
        (Decimal("0.4100"), Decimal("120.00")),
        (Decimal("0.4000"), Decimal("300.00")),
    )
    # A NO bid at 0.5700 is a YES ask at 0.4300.
    assert event.asks[0][0] == Decimal("0.4300")
    assert event.sequence is None, "a public snapshot publishes no sequence number"


def test_orderbook_shape_change_is_an_alarm(store: Any) -> None:
    with pytest.raises(WireShapeError):
        normalize_kalshi_orderbook_snapshot(
            {"unexpected": {}}, contract_id="X", clock=make_clock(), provenance=make_provenance()
        )


def test_missing_items_key_alarms_instead_of_returning_empty(store: Any) -> None:
    client = FakeHttpClient({"/markets": ok({"unexpected": []})})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    with pytest.raises(WireShapeError) as excinfo:
        kalshi.list_markets(series_ticker="KXCPI")
    assert "markets" in str(excinfo.value)


def test_items_of_wrong_type_alarms(store: Any) -> None:
    client = FakeHttpClient({"/markets": ok({"markets": "not-a-list"})})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    with pytest.raises(WireShapeError):
        kalshi.list_markets(series_ticker="KXCPI")


def test_non_json_body_alarms(store: Any) -> None:
    client = FakeHttpClient({"/markets": ok("<html>maintenance</html>")})
    kalshi = KalshiClient(store, transport=make_transport(store, client))
    with pytest.raises(WireShapeError):
        kalshi.list_markets(series_ticker="KXCPI")


def test_trade_without_price_is_refused_rather_than_zero_priced() -> None:
    broken = {k: v for k, v in TRADE_BODY.items() if k != "yes_price_dollars"}
    with pytest.raises(WireShapeError):
        normalize_kalshi_trade(broken, clock=make_clock(), provenance=make_provenance())


def test_calendar_times_resolve_through_the_timezone_database() -> None:
    entries = parse_calendar(CALENDAR_HTML, year=2026, calendar_url="https://www.bls.gov/x")
    by_family = {e.family: e for e in entries}

    employment = by_family["empsit"]
    assert employment.scheduled_at == dt.datetime(2026, 9, 4, 12, 30, tzinfo=dt.UTC)
    assert employment.utc_offset_seconds == -4 * 3600, "September is EDT"
    assert employment.reference_period == "August 2026"

    cpi = by_family["cpi"]
    assert cpi.scheduled_at == dt.datetime(2026, 9, 11, 12, 30, tzinfo=dt.UTC)
    assert cpi.timezone == "America/New_York"


def test_calendar_across_a_daylight_saving_change_shifts_utc_offset() -> None:
    january_html = (
        CALENDAR_HTML.replace("September 4, 2026", "January 9, 2026")
        .replace("September 11, 2026", "January 13, 2026")
        .replace("September 24, 2026", "January 22, 2026")
    )
    entries = parse_calendar(january_html, year=2026, calendar_url="https://www.bls.gov/x")
    employment = next(e for e in entries if e.family == "empsit")

    assert employment.scheduled_at == dt.datetime(2026, 1, 9, 13, 30, tzinfo=dt.UTC)
    assert employment.utc_offset_seconds == -5 * 3600, "January is EST"
    # The local time is identical; only the instant moves.
    assert (employment.local_time.hour, employment.local_time.minute) == (8, 30)


def test_calendar_without_a_timezone_declaration_is_refused() -> None:
    with pytest.raises(WireShapeError):
        parse_calendar(CALENDAR_HTML_NO_ZONE, year=2026, calendar_url="https://www.bls.gov/x")


def test_the_employment_masthead_period_is_read_from_its_double_dash_line() -> None:
    """The Employment Situation titles its masthead with two dashes, not one.

    ``THE EMPLOYMENT SITUATION -- MARCH 2025`` is the format both real captures
    use, and a pattern requiring exactly one dash read the period as unstated.
    The unstated period then became the recorded one, so a March release was
    identified by its publication date instead of the month it reports.
    """
    _, period, _, _, _, _, _, _ = parse_release_payload(
        "<pre>THE EMPLOYMENT SITUATION -- MARCH 2025\n"
        "Total nonfarm payroll employment rose by 228,000 in March.</pre>",
        family_slug="empsit",
        source_url="x",
    )
    assert period == "MARCH 2025"

    # The single-dash spelling is still read, so the widening did not trade one
    # format for the other.
    _, single, _, _, _, _, _, _ = parse_release_payload(
        "<pre>THE EMPLOYMENT SITUATION - MARCH 2025\n"
        "Total nonfarm payroll employment rose by 228,000 in March.</pre>",
        family_slug="empsit",
        source_url="x",
    )
    assert single == "MARCH 2025"


def test_the_release_family_is_stated_in_the_cohort_vocabulary(store: Any) -> None:
    """A release trace names ``employment``, not the venue's ``empsit`` slug.

    ``_canonical_family`` maps a *title* to a family, so feeding it an
    already-canonical slug returned that slug unchanged and the release carried a
    word no cohort row uses, breaking a join on the family field.
    """
    client = FakeHttpClient({"/news.release/archives/empsit_01102025.htm": ok(EMPSIT_2025_01_HTML)})
    bls = MacroReleaseClient(store, transport=make_transport(store, client))

    release, blocked = bls.get_initial_release(
        "empsit",
        dt.date(2025, 1, 10),
        scheduled_at=dt.datetime(2025, 1, 10, 13, 30, tzinfo=dt.UTC),
    )

    assert blocked is None
    assert release is not None
    assert release.family == "employment"
    assert release.family != "empsit", "the venue slug belongs at the boundary only"


def test_archive_index_extracts_publication_dates_and_gaps() -> None:
    published, unpublished = parse_archive_index(
        ARCHIVE_INDEX_HTML, family_slug="cpi", index_url="https://www.bls.gov/cpi"
    )
    assert published == [
        (dt.date(2026, 7, 14), "https://www.bls.gov/news.release/archives/cpi_07142026.htm"),
        (dt.date(2026, 8, 12), "https://www.bls.gov/news.release/archives/cpi_08122026.htm"),
    ]
    assert len(unpublished) == 1
    assert unpublished[0].reference_period == "October 2025"
    assert "appropriations" in unpublished[0].reason


def test_archive_url_matches_the_documented_filename_convention() -> None:
    assert (
        archive_url("cpi", dt.date(2026, 8, 12))
        == "https://www.bls.gov/news.release/archives/cpi_08122026.htm"
    )
    assert (
        archive_url("empsit", dt.date(2026, 9, 4))
        == "https://www.bls.gov/news.release/archives/empsit_09042026.htm"
    )


def test_cpi_first_release_values_are_parsed_from_the_payload() -> None:
    title, period, observed, values, revisions, statements, usdl, _ = parse_release_payload(
        CPI_HTML, family_slug="cpi", source_url="https://www.bls.gov/x"
    )
    assert title.startswith("Consumer Price Index")
    assert period == "JULY 2026"
    assert values["cpi_headline_sa_mom_pct"] == Decimal("0.1")
    assert values["cpi_core_sa_mom_pct"] == Decimal("0.2")
    assert values["cpi_headline_nsa_yoy_pct"] == Decimal("3.4")
    assert values["cpi_core_nsa_yoy_pct"] == Decimal("2.5")
    assert values["cpi_u_nsa_index_level"] == Decimal("333.918")
    # The same line states the 12-month percent change in the release's own units.
    assert values["cpi_u_nsa_yoy_pct"] == Decimal("3.4")
    assert revisions == {}, "CPI states no prior-month revision in this payload"
    assert usdl == "USDL-26-1378"
    assert observed == dt.datetime(2026, 8, 12, 12, 30, tzinfo=dt.UTC)
    assert statements["cpi_headline_sa_mom"]


def test_employment_release_parses_headline_fields_and_keeps_revisions_separate() -> None:
    _, period, _, values, revisions, _, usdl, _ = parse_release_payload(
        EMPSIT_HTML, family_slug="empsit", source_url="https://www.bls.gov/x"
    )
    assert period == "AUGUST 2026"
    # The release states 162,000 jobs; the thousands value is an explicit
    # conversion, and the release's own unit is retained alongside it.
    assert values["payrolls_change_thousands"] == Decimal("162")
    assert values["payrolls_change_jobs"] == Decimal("162000")
    assert values["unemployment_rate_pct"] == Decimal("4.1")
    assert values["avg_hourly_earnings_usd"] == Decimal("37.75")
    assert values["avg_hourly_earnings_mom_pct"] == Decimal("0.3")
    assert values["avg_hourly_earnings_yoy_pct"] == Decimal("3.1")
    assert values["avg_workweek_hours"] == Decimal("34.4")
    assert values["avg_workweek_change_hours"] == Decimal("0.1")

    # Revisions belong to June and July, and must never overwrite August's value.
    assert revisions["payrolls_change_thousands_revised_June"] == Decimal("31")
    assert revisions["payrolls_change_thousands_prior_June"] == Decimal("20")
    assert revisions["payrolls_change_thousands_revised_July"] == Decimal("21")
    assert revisions["payrolls_change_thousands_revision_combined"] == Decimal("55")
    assert revisions["payrolls_change_jobs_revision_combined"] == Decimal("55000")
    assert revisions["payrolls_change_jobs_revised_June"] == Decimal("31000")
    assert "payrolls_change_thousands" not in revisions
    assert usdl == "USDL-26-1435"


def test_absent_statistic_is_omitted_rather_than_defaulted() -> None:
    _, _, _, values, revisions, _, _, _ = parse_release_payload(
        "<pre>CONSUMER PRICE INDEX - JULY 2026\nNo figures were stated here.</pre>",
        family_slug="cpi",
        source_url="https://www.bls.gov/x",
    )
    assert values == {}, "a value the payload does not state must not be invented"
    assert revisions == {}


def test_schedule_agreement_is_reported_not_assumed() -> None:
    scheduled = dt.datetime(2026, 8, 12, 12, 30, tzinfo=dt.UTC)
    _, _, _, _, _, _, _, agrees = parse_release_payload(
        CPI_HTML, family_slug="cpi", source_url="x", scheduled_at=scheduled
    )
    assert agrees == "agrees_with_calendar"

    _, _, _, _, _, _, _, differs = parse_release_payload(
        CPI_HTML,
        family_slug="cpi",
        source_url="x",
        scheduled_at=dt.datetime(2026, 8, 12, 13, 30, tzinfo=dt.UTC),
    )
    assert differs == "differs_from_calendar"

    _, _, _, _, _, _, _, unverified = parse_release_payload(
        CPI_HTML, family_slug="cpi", source_url="x"
    )
    assert unverified == "unverified"


def test_release_availability_is_unknown_when_only_a_schedule_is_known(store: Any) -> None:
    client = FakeHttpClient({"/news.release/archives/cpi_08122026.htm": ok(CPI_HTML)})
    bls = MacroReleaseClient(store, transport=make_transport(store, client))
    release, blocked = bls.get_initial_release("cpi", dt.date(2026, 8, 12))

    assert blocked is None
    assert release is not None
    # A later fetch cannot establish when the payload first became public, so the
    # availability interval must stay unknown rather than being built from the
    # fetch instant.
    assert release.clock.availability.upper is None
    assert release.clock.availability.basis == "historical_without_receipt"
    assert release.clock.usable_time is None
    # The schedule is retained as source-time evidence, which is a different fact
    # from a usable time and must not be promoted to one.
    assert release.clock.source_time == dt.datetime(2026, 8, 12, 12, 30, tzinfo=dt.UTC)
    assert release.embargo_time_from_payload == dt.datetime(2026, 8, 12, 12, 30, tzinfo=dt.UTC)
    assert release.provenance.raw_hash
    assert store.get(release.provenance.raw_hash)


def test_blocked_bls_fetch_is_recorded_not_empty(store: Any) -> None:
    client = FakeHttpClient({"/news.release/archives/cpi_08122026.htm": (403, "Access Denied")})
    bls = MacroReleaseClient(store, transport=make_transport(store, client))
    release, blocked = bls.get_initial_release("cpi", dt.date(2026, 8, 12))

    assert release is None
    assert blocked is not None
    assert blocked["empty_result"] is False
    assert blocked["status_code"] == 403


def test_calendar_fetch_failure_is_not_an_empty_calendar(store: Any) -> None:
    client = FakeHttpClient({"/schedule/2026/09_sched_list.htm": (403, "Access Denied")})
    bls = MacroReleaseClient(store, transport=make_transport(store, client))
    entries, blocked = bls.get_calendar(2026, 9)

    assert entries == []
    assert blocked is not None and blocked["empty_result"] is False


def test_polymarket_is_fail_closed_when_unreachable(store: Any) -> None:
    class DeadClient:
        def get(self, url: str, headers: Any = None) -> Any:
            import httpx

            raise httpx.ConnectTimeout("timed out")

        def close(self) -> None:
            pass

    transport = HttpTransport(
        store,
        client=DeadClient(),  # type: ignore[arg-type]
        policy=RetryPolicy(attempts=2),
        sleep=lambda _: None,
    )
    poly = PolymarketPublicClient(store, transport=transport)

    report = poly.probe(max_attempts=2)
    assert report.reachable is False
    assert report.blocked is not None
    assert report.blocked["empty_result"] is False

    with pytest.raises(PolymarketUnreachable) as excinfo:
        poly.get_book("token")
    assert excinfo.value.blocked["empty_result"] is False


def test_price_history_preserves_time_precision_and_measures_spacing(store: Any) -> None:
    client = FakeHttpClient({"/prices-history": ok(PRICE_HISTORY_BODY)})
    poly = PolymarketPublicClient(store, transport=make_transport(store, client))
    points, spacing, raw_hash = poly.get_price_history("token-1", fidelity_minutes=60)

    assert [p.price for p in points] == [Decimal("0.42"), Decimal("0.43"), Decimal("0.41")]
    # Points keep source order, and each carries the exact instant its own
    # timestamp denotes rather than the fetch time.
    assert points[0].observed_at == dt.datetime(2026, 9, 10, 0, 26, 40, tzinfo=dt.UTC)
    assert points[2].observed_at == dt.datetime(2026, 9, 10, 2, 26, 40, tzinfo=dt.UTC)
    # A historical point fetched later has no provable public-availability time, so
    # its usable time must stay unknown rather than being the fetch instant.
    assert points[0].clock.availability.upper is None
    assert points[0].clock.availability.basis == "historical_without_receipt"
    assert spacing["distinct_spacing_seconds"] == [3600]
    assert spacing["matches_requested"] is True
    assert spacing["tick_complete"] is False
    assert raw_hash


def test_history_spacing_reports_a_fidelity_mismatch() -> None:
    clock = make_clock()
    results = normalize_price_history(
        {"history": [{"t": 1789000000, "p": 0.42}, {"t": 1789007200, "p": 0.41}]},
        contract_id="token",
        clock=clock,
        provenance=make_provenance(),
    )
    spacing = inspect_history_spacing(results, requested_fidelity_minutes=60)
    assert spacing["uniform"] is True
    assert spacing["matches_requested"] is False
    assert spacing["effective_resolution_seconds"] == 7200
    assert spacing["tick_complete"] is False


def test_polymarket_book_snapshot_keeps_sizes_and_drops_empty_levels() -> None:
    events = normalize_book_snapshot(
        POLYMARKET_BOOK_BODY,
        contract_id="token-1",
        clock=make_clock(),
        provenance=make_provenance(),
    )
    event = events[0]
    assert event.kind == "snapshot"
    assert event.bids[0] == (Decimal("0.41"), Decimal("120"))
    assert event.asks[0] == (Decimal("0.43"), Decimal("75"))
    assert event.operation == "replace"

    zeroed = {**POLYMARKET_BOOK_BODY, "bids": [{"price": "0.41", "size": "0"}]}
    events_zero = normalize_book_snapshot(
        zeroed, contract_id="token-1", clock=make_clock(), provenance=make_provenance()
    )
    assert events_zero[0].bids == (), "a zero size is a removal, not a resting level"


def test_channel_message_kind_unknown_is_an_alarm() -> None:
    assert classify_channel_message({"event_type": "book", "asset_id": "x"}) == "book"
    with pytest.raises(WireShapeError):
        classify_channel_message({"event_type": "brand_new_kind"})
    with pytest.raises(WireShapeError):
        classify_channel_message({"asset_id": "x"})


def test_mocked_channel_capture_reconnect_invalidates_and_stops_gracefully(store: Any) -> None:
    """Snapshot, delta, reconnect: the reopened connection must invalidate state."""

    book_message = json.dumps(
        {
            "event_type": "book",
            "asset_id": "token-A",
            "bids": [{"price": "0.40", "size": "100"}],
            "asks": [{"price": "0.45", "size": "50"}],
            "hash": "0xbook1",
            "timestamp": "1789000000",
        }
    )
    delta_message = json.dumps(
        {
            "event_type": "price_change",
            "asset_id": "token-A",
            "price_changes": [
                {
                    "asset_id": "token-A",
                    "side": "BUY",
                    "price": "0.41",
                    "size": "25",
                    "best_bid": "0.41",
                    "best_ask": "0.45",
                    "timestamp": "1789000060",
                }
            ],
            "timestamp": "1789000060",
        }
    )

    class FakeSocket:
        def __init__(self, messages: list[str]) -> None:
            self._messages = list(messages)

        async def send(self, _: str) -> None:
            return None

        async def recv(self) -> str:
            if not self._messages:
                raise RuntimeError("connection closed by fixture")
            return self._messages.pop(0)

    attempts = {"count": 0}

    class FakeConnect:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> Any:
            attempts["count"] += 1
            if attempts["count"] == 1:
                # First connection ends abruptly after a delta.
                return FakeSocket([book_message, delta_message])
            # Second connection delivers nothing new, so the run must stop bounded.
            return FakeSocket([book_message])

        async def __aexit__(self, *exc: Any) -> None:
            return None

    def connect(*args: Any, **kwargs: Any) -> FakeConnect:
        return FakeConnect(*args, **kwargs)

    import asyncio

    result = asyncio.run(
        capture_market_channel(
            store,
            ["token-A"],
            duration_seconds=30,
            connect=connect,
            max_reconnects=1,
        )
    )

    assert result.tick_complete is False
    assert result.snapshots_received >= 1
    assert result.events_emitted >= 1
    # Every emitted event and lifecycle marker resolves to archived bytes.
    assert result.raw_hashes
    for raw_hash in result.raw_hashes:
        assert store.get(raw_hash)


def test_websocket_capture_reports_synthetic_disconnect_as_archived_payload(store: Any) -> None:
    """A lifecycle marker has no wire bytes, so its own payload is archived."""
    import asyncio

    message = json.dumps(
        {
            "event_type": "book",
            "asset_id": "token-A",
            "bids": [{"price": "0.40", "size": "100"}],
            "asks": [],
            "hash": "0xbook",
            "timestamp": "1789000000",
        }
    )

    class FakeSocket:
        def __init__(self) -> None:
            self._sent = False

        async def send(self, _: str) -> None:
            return None

        async def recv(self) -> str:
            if not self._sent:
                self._sent = True
                return message
            raise RuntimeError("closed")

    attempts = {"count": 0}

    class FakeConnect:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> Any:
            attempts["count"] += 1
            if attempts["count"] > 1:
                raise RuntimeError("no more connections")
            return FakeSocket()

        async def __aexit__(self, *exc: Any) -> None:
            return None

    observed: list[Any] = []

    result = asyncio.run(
        capture_market_channel(
            store,
            ["token-A"],
            duration_seconds=20,
            connect=lambda *a, **k: FakeConnect(),
            max_reconnects=1,
            on_event=observed.append,
        )
    )

    # The socket carrying the snapshot fails, and the retry never connects, so the
    # boundary must have been archived when the first connection ended rather than
    # deferred to a connection that never opened.
    disconnects = [event for event in observed if getattr(event, "kind", None) == "disconnect"]
    assert len(disconnects) == 1

    # The disconnect resolves to archived bytes, and those bytes are the lifecycle
    # payload itself: parsed, not compared as an ordered prefix of the JSON.
    for event in disconnects:
        stored = store.get(event.provenance.raw_hash)
        assert stored
        payload = json.loads(stored)
        assert payload["reason"] == "disconnect"
        assert payload["asset_id"] == "token-A"
        assert payload["connection_id"] == event.connection_id

    # The invariant the callback exists for: every emitted event names archivable
    # bytes, whether it carried wire bytes or was synthesised at the boundary.
    for raw_hash in result.raw_hashes:
        assert store.get(raw_hash)
    for event in observed:
        assert store.get(event.provenance.raw_hash)

    # A failed connection consumes the reconnect budget instead of being reported
    # as a duration bound the run never reached.
    assert result.stopped_reason == "max_reconnects"


def test_polling_capture_is_labelled_snapshots_and_bounded(store: Any) -> None:
    client = FakeHttpClient({"/book": ok(POLYMARKET_BOOK_BODY)})
    poly = PolymarketPublicClient(store, transport=make_transport(store, client))

    result = poly.capture_polled_snapshots(
        ["token-1"],
        duration_seconds=0.0,
        interval_seconds=1.0,
        should_stop=lambda: False,
    )
    assert result["tick_complete"] is False
    assert result["mode"] == "polling_snapshots"
    assert result["observation_count"] == 0, "a zero-duration capture observes nothing"
    assert result["stopped_reason"] == "duration_elapsed"


def test_polling_capture_records_observations_then_stops(store: Any) -> None:
    client = FakeHttpClient({"/book": ok(POLYMARKET_BOOK_BODY)})
    poly = PolymarketPublicClient(store, transport=make_transport(store, client))
    result = poly.capture_polled_snapshots(
        ["token-1"],
        duration_seconds=5.0,
        interval_seconds=1.0,
        sleep=lambda _: None,
    )
    assert result["observation_count"] >= 1
    assert result["tick_complete"] is False
    assert all(o["observation_kind"] == "snapshot" for o in result["observations"])
    assert all(o["tick_complete"] is False for o in result["observations"])
    assert result["resolution_seconds"] == 1.0


def _verified_origin() -> VerifiedOrigin:
    """A resolved origin for a candidate fixture, with no store read behind it.

    These fixtures exercise selection and counting, not provenance: the real
    resolution path is driven through the transport in the provenance tests, where
    the page is actually archived and read back. A ``VerifiedOrigin`` cannot be
    fabricated by the audit itself, so this stands in for the verifier's output.
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


def _candidate(**overrides: Any) -> Any:
    from market_propagation.ingest.audit import CandidateContract

    base = {
        "ticker": "KXCPI-26JUN-T-0.3",
        "event_ticker": "KXCPI-26JUN",
        "series_ticker": "KXCPI",
        "open_time": dt.datetime(2026, 6, 8, tzinfo=dt.UTC),
        "close_time": dt.datetime(2026, 7, 14, 12, 25, tzinfo=dt.UTC),
        "resolve_time": None,
        "status": "finalized",
        "active_at_release": True,
        "known_at_release": True,
        "volume_fp": Decimal("0.00"),
        "open_interest_fp": Decimal("0.00"),
        "strike_type": "greater",
        "floor_strike": Decimal("-0.3"),
        "cap_strike": None,
        "rule_hash": "h",
        "rule_available_at": dt.datetime(2026, 6, 8, tzinfo=dt.UTC),
        "partition": "historical",
        "origin": _verified_origin(),
    }
    base.update(overrides)
    # Mirror the audit's own derivation so the fixture cannot assert a liquidity
    # fact the real candidate construction would never have produced. Thinness is
    # reported, never an exclusion, so it depends only on volume and on the
    # lifecycle exclusions already present.
    if "thin_liquidity" not in overrides:
        volume = base["volume_fp"]
        base["thin_liquidity"] = bool(
            volume is not None and volume < THIN_VOLUME_FP and not base.get("exclusion_reasons", ())
        )
    return CandidateContract(**base)


#: A cohort row in the canonical ``configs/cohort.yaml`` shape: an aware
#: ``scheduled_at``, the canonical family name, and the URLs the date came from.
#: Tests build their own cohort from this rather than reaching for a default held
#: by the audit, because the audit deliberately holds none.
def _cohort_event(**overrides: Any) -> dict[str, Any]:
    base = {
        "event_id": "cpi_2025_01",
        "family": "cpi",
        "reference_period": "2024-12",
        "scheduled_at": "2025-01-15T13:30:00+00:00",
        "calendar_url": "https://www.bls.gov/schedule/2025/01_sched_list.htm",
        "initial_release_url": "https://www.bls.gov/news.release/archives/cpi_01152025.htm",
    }
    base.update(overrides)
    return base


def _employment_cohort_event(**overrides: Any) -> dict[str, Any]:
    return _cohort_event(
        event_id="empsit_2025_01",
        family="employment",
        reference_period="2024-12",
        scheduled_at="2025-01-10T13:30:00+00:00",
        initial_release_url="https://www.bls.gov/news.release/archives/empsit_01102025.htm",
        **overrides,
    )


def _cpi_market(**overrides: Any) -> dict[str, Any]:
    """A contract open before the January 2025 CPI release and spanning it."""
    base = {
        **MARKET_BODY,
        "ticker": "KXCPI-25JAN-T0.3",
        "event_ticker": "KXCPI-25JAN",
        "open_time": "2024-12-02T21:00:00Z",
        "close_time": "2025-02-15T13:25:00Z",
        "settlement_ts": "2025-02-15T14:30:00Z",
    }
    base.update(overrides)
    return base


def _monthly_candles() -> dict[str, Any]:
    """Three contiguous 60-minute candles spanning the window with two-sided quotes.

    The stamps are on the 60-minute grid and contiguous, and the last one is at or
    past the window end, so the series satisfies the on-grid and window-spanning
    gates rather than merely being returned.
    """
    return {
        "candlesticks": [
            {
                "end_period_ts": 1736946000,  # 2025-01-15T13:00:00Z
                "price": {
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "mean": None,
                    "previous": "0.90",
                },
                "volume": "0.00",
                "open_interest": "5.00",
                "yes_ask": {"open": "0.91", "high": "0.91", "low": "0.91", "close": "0.91"},
                "yes_bid": {"open": "0.89", "high": "0.89", "low": "0.89", "close": "0.89"},
            },
            {
                "end_period_ts": 1736949600,  # 2025-01-15T14:00:00Z
                "price": {
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "mean": None,
                    "previous": "0.91",
                },
                "volume": "0.00",
                "open_interest": "5.00",
                "yes_ask": {"open": "0.92", "high": "0.92", "low": "0.92", "close": "0.92"},
                "yes_bid": {"open": "0.90", "high": "0.90", "low": "0.90", "close": "0.90"},
            },
            {
                "end_period_ts": 1736953200,  # 2025-01-15T15:00:00Z
                "price": {
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "mean": None,
                    "previous": "0.92",
                },
                "volume": "0.00",
                "open_interest": "5.00",
                "yes_ask": {"open": "0.93", "high": "0.93", "low": "0.93", "close": "0.93"},
                "yes_bid": {"open": "0.91", "high": "0.91", "low": "0.91", "close": "0.91"},
            },
        ]
    }


def _auditor(
    store: Any,
    client: FakeHttpClient,
    *,
    max_contracts: int = 40,
    candle_contracts: int = 0,
    max_pages: int = 1,
) -> CohortAuditor:
    return CohortAuditor(
        store,
        kalshi=KalshiClient(store, transport=make_transport(store, client)),
        bls=MacroReleaseClient(store, transport=make_transport(store, client)),
        max_contracts_per_event=max_contracts,
        max_candle_contracts_per_event=candle_contracts,
        max_pages=max_pages,
    )


def _cpi_routes(markets: list[dict[str, Any]], *, candles: Any = None) -> dict[str, Any]:
    """Fixture routes for a CPI event.

    ``/historical/markets/{ticker}/candlesticks`` precedes ``/historical/markets``
    so the substring lookup cannot serve a listing body to a candle request.
    """
    routes: dict[str, Any] = {
        "/historical/cutoff": ok(CUTOFF_BODY),
        "/historical/markets/KXCPI-25JAN-T0.3/candlesticks": ok(candles or _monthly_candles()),
        "/historical/trades": ok({"trades": [], "cursor": None}),
        "/historical/markets": ok({"markets": markets, "cursor": None}),
        "/markets": ok({"markets": markets, "cursor": None}),
        "/news.release/archives": ok(CPI_2025_01_HTML),
    }
    return routes


def test_zero_volume_candidate_is_kept_and_flagged() -> None:
    zero_volume = _candidate(volume_fp=Decimal("0.00"))
    assert zero_volume.eligible is True, "post-event volume must never decide eligibility"
    assert zero_volume.thin_liquidity is True
    assert zero_volume.as_dict()["selection_basis"].endswith("no_post_event_volume_filter")


def test_selection_prefers_tradeable_contracts_but_keeps_the_others() -> None:
    """A pre-release close separates cohorts; it does not delete a candidate."""
    early = _candidate(
        ticker="EARLY",
        close_time=dt.datetime(2026, 6, 1, tzinfo=dt.UTC),
        window_overlap=False,
        cohort=COHORT_DIRECT_CLOSED_PRE_RELEASE,
    )
    live = _candidate(ticker="LIVE", window_overlap=True)
    selected = _select_candidates([early, live], limit=10)
    # The tradeable contract is ordered first because it is the only one that can
    # carry a post-release response, but both stay in the audited set.
    assert [c.ticker for c in selected] == ["LIVE", "EARLY"]


def test_selection_bound_drops_by_rank_never_by_cohort() -> None:
    live = _candidate(ticker="LIVE", window_overlap=True)
    early = _candidate(
        ticker="EARLY",
        window_overlap=False,
        cohort=COHORT_DIRECT_CLOSED_PRE_RELEASE,
    )
    # With room for one, the cost bound takes the primary cohort, and the direct
    # contract's absence from the deep audit is a reported bound rather than a rule.
    assert [c.ticker for c in _select_candidates([early, live], limit=1)] == ["LIVE"]


def test_dedupe_prefers_historical_record_and_flags_rule_disagreement() -> None:
    hist = _candidate(partition="historical", rule_hash="A")
    live = _candidate(partition="live", rule_hash="A")
    resolved = _dedupe_candidates([live, hist])
    assert len(resolved) == 1
    assert resolved[0].partition == "historical"

    disagreeing = _dedupe_candidates(
        [
            _candidate(partition="live", rule_hash="B"),
            _candidate(partition="historical", rule_hash="A"),
        ]
    )
    assert len(disagreeing) == 1
    assert "rule_text_differs_between_partitions" in disagreeing[0].exclusion_reasons


def test_event_cohort_reads_the_configuration_shape() -> None:
    cohort = event_cohort({"events": [_cohort_event(), _employment_cohort_event()]})
    assert [e["event_id"] for e in cohort] == ["cpi_2025_01", "empsit_2025_01"]
    # scheduled_at is resolved to an aware instant, so no local wall clock survives.
    assert cohort[0]["scheduled_at"] == dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC)
    assert cohort[1]["scheduled_at"] == dt.datetime(2025, 1, 10, 13, 30, tzinfo=dt.UTC)


def test_event_cohort_rejects_an_empty_or_absent_cohort() -> None:
    """An empty cohort has no complete state, so it must not run silently."""
    with pytest.raises(ValueError, match="no events"):
        event_cohort({"events": []})
    with pytest.raises(ValueError, match="'events' sequence"):
        event_cohort({})


def test_event_cohort_rejects_a_hard_schema_mismatch() -> None:
    """A missing schedule instant is a configuration fault, not a silent gap."""
    row = _cohort_event()
    del row["scheduled_at"]
    with pytest.raises(ValueError, match="scheduled_at"):
        event_cohort({"events": [row]})


def test_event_cohort_rejects_a_flat_local_time_schedule() -> None:
    """A local-time form without an offset is refused, never assigned a zone."""
    with pytest.raises(ValueError):
        event_cohort(
            {
                "events": [
                    _cohort_event(
                        scheduled_at="2025-01-15T08:30:00",
                        publication_date="2025-01-15",
                    )
                ]
            }
        )


def test_event_cohort_rejects_duplicate_event_ids() -> None:
    with pytest.raises(ValueError, match="appears twice"):
        event_cohort({"events": [_cohort_event(), _cohort_event()]})


def test_event_cohort_rejects_a_family_with_no_release_slug() -> None:
    with pytest.raises(ValueError, match="no BLS release slug"):
        event_cohort({"events": [_cohort_event(family="gdp")]})


def test_audit_cohort_requires_a_cohort_and_refuses_to_default_one(
    store: Any, tmp_path: pathlib.Path
) -> None:
    client = FakeHttpClient({"/historical/cutoff": ok(CUTOFF_BODY)})
    auditor = _auditor(store, client)
    with pytest.raises(ValueError, match="exactly one cohort source"):
        auditor.audit_cohort(tmp_path / "out")


def test_audit_cohort_refuses_two_cohort_sources_at_once(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """Passing both must raise rather than silently picking one."""
    client = FakeHttpClient({"/historical/cutoff": ok(CUTOFF_BODY)})
    auditor = _auditor(store, client)
    with pytest.raises(ValueError, match="exactly one cohort source"):
        auditor.audit_cohort(
            tmp_path / "both",
            events=[_cohort_event()],
            config={"events": [_employment_cohort_event()]},
        )


def test_audit_cohort_accepts_the_parsed_config_and_the_configured_window(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """The window comes from the configuration, not from a module constant."""
    auditor = _auditor(store, FakeHttpClient(_cpi_routes([_cpi_market()])))
    result = auditor.audit_cohort(
        tmp_path / "window",
        config={"events": [_cohort_event()]},
        discover_series=False,
        before_seconds=1800,
        after_seconds=3600,
    )
    event = result.events[0]
    assert event.event_id == "cpi_2025_01"
    assert event.scheduled_at == dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC)
    assert event.window_start == event.scheduled_at - dt.timedelta(seconds=1800)
    assert event.window_end == event.scheduled_at + dt.timedelta(seconds=3600)
    # The canonical study window is the default, and a caller can widen it.
    assert (DEFAULT_BEFORE_SECONDS, DEFAULT_AFTER_SECONDS) == (1800, 3600)

    widened = _auditor(store, FakeHttpClient(_cpi_routes([_cpi_market()])))
    wider = widened.audit_cohort(
        tmp_path / "window-wide",
        events=[_cohort_event()],
        discover_series=False,
        before_seconds=7200,
        after_seconds=7200,
    )
    assert wider.events[0].window_start == wider.events[0].scheduled_at - dt.timedelta(seconds=7200)
    assert wider.events[0].window_start != event.window_start


def test_employment_family_reaches_bls_through_the_empsit_slug(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """The cohort says 'employment'; only the BLS boundary says 'empsit'."""
    market = _cpi_market(
        ticker="KXPAYROLLS-25JAN-T100",
        event_ticker="KXPAYROLLS-25JAN",
    )
    client = FakeHttpClient(
        {
            "/historical/cutoff": ok(CUTOFF_BODY),
            "/historical/markets": ok({"markets": [market], "cursor": None}),
            "/markets": ok({"markets": [market], "cursor": None}),
            "/news.release/archives/empsit_01102025.htm": ok(EMPSIT_2025_01_HTML),
        }
    )
    auditor = _auditor(store, client)
    result = auditor.audit_cohort(
        tmp_path / "empsit",
        events=[_employment_cohort_event()],
        discover_series=False,
    )
    event = result.events[0]
    assert event.family == "employment"
    assert event.release is not None
    # The archive slug came from the family at the boundary, and the payload's own
    # values came back rather than an empty release.
    assert any("empsit_01102025" in call for call in client.calls)
    assert event.release["values"]["payrolls_change_thousands"] == "256"


def test_release_embargo_is_labelled_source_evidence_not_observation(
    store: Any, tmp_path: pathlib.Path
) -> None:
    auditor = _auditor(store, FakeHttpClient(_cpi_routes([_cpi_market()])))
    auditor.audit_cohort(
        tmp_path / "embargo",
        events=[_cohort_event()],
        discover_series=False,
        after_seconds=3600,
    )
    knowability = json.loads((tmp_path / "embargo" / "event_card.json").read_text())[
        "when_the_system_could_know"
    ]
    # The embargo instant is the payload's own claim about its schedule, and it is
    # never promoted to an observed publication time.
    assert knowability["release_embargo_timestamp"] == "2025-01-15T13:30:00+00:00"
    assert knowability["release_embargo_is_not_observed_publication"] is True
    assert knowability["release_observed_publication_at"] is None
    assert "source_claim_about_schedule" in knowability["release_embargo_evidence_kind"]


def test_direct_closed_contracts_are_separate_from_the_downstream_cohort(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A contract closed before the release is direct material, not a null."""
    direct = _cpi_market(
        ticker="KXCPI-25JAN-T0.5",
        close_time="2025-01-15T13:25:00Z",  # five minutes before the 13:30 release
    )
    downstream = _cpi_market(
        ticker="KXCPI-25JAN-POLICY",
        close_time="2025-03-15T13:25:00Z",
    )
    auditor = _auditor(store, FakeHttpClient(_cpi_routes([direct, downstream])))
    result = auditor.audit_cohort(
        tmp_path / "direct",
        events=[_cohort_event()],
        discover_series=False,
    )
    event = result.events[0]
    assert {c.ticker for c in event.direct_closed_pre_release} == {"KXCPI-25JAN-T0.5"}
    assert {c.ticker for c in event.downstream_candidates} == {"KXCPI-25JAN-POLICY"}

    # The closed contract keeps its own cohort label and its lifecycle exclusion,
    # and is never reported as an observation of no movement.
    closed = event.direct_closed_pre_release[0]
    assert closed.cohort == COHORT_DIRECT_CLOSED_PRE_RELEASE
    assert closed.closed_before_release is True
    assert "closed_before_release" in closed.exclusion_reasons
    assert closed not in event.eligible_candidates
    assert event.cohort_size["direct_closed_pre_release"] == 1
    assert event.cohort_size["downstream_candidates"] == 1
    assert event.as_dict()["direct_closed_pre_release_count"] == 1
    gate = {g.name: g for g in event.gates}
    assert gate["direct_closed_contracts_separated"].satisfied is True


def test_a_trade_candle_is_not_empirical_eligibility(store: Any, tmp_path: pathlib.Path) -> None:
    """A returned candle with no two-sided quote must not read as eligible."""
    trade_only = {
        "candlesticks": [
            {
                "end_period_ts": 1736946000,
                "price": {
                    "open": "0.90",
                    "high": "0.90",
                    "low": "0.90",
                    "close": "0.90",
                    "mean": "0.90",
                    "previous": "0.90",
                },
                "volume": "12.00",
                "open_interest": "5.00",
            }
        ]
    }
    auditor = _auditor(
        store, FakeHttpClient(_cpi_routes([_cpi_market()], candles=trade_only)), candle_contracts=1
    )
    result = auditor.audit_cohort(
        tmp_path / "trade-only",
        events=[_cohort_event()],
        discover_series=False,
    )
    event = result.events[0]
    # The read succeeded and the event is audited, but the empirical gate is not.
    assert event.status == "audited"
    assert result.complete is False, (
        "a successful request must not complete an empirical gate on its own"
    )
    gate = {g.name: g for g in event.gates}
    assert gate["candle_quotes_two_sided"].satisfied is False
    assert any(
        a.trade_only_candles == 1 and a.two_sided_quote_candles == 0 for a in event.candle_audits
    )
    assert "candle_quotes_two_sided" in event.as_dict()["unsatisfied_gates"]
    assert ("cpi_2025_01", "candle_quotes_two_sided") in result.unsatisfied_gates
    assert result.status == "partial"


def test_partial_pagination_blocks_the_complete_universe_claim(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """A truncated page walk is a coverage limit, not a contract universe."""
    market = _cpi_market()
    routes = _cpi_routes([market])
    # A page that still advertises a cursor stops early under max_pages=1.
    routes["/historical/markets"] = ok({"markets": [market], "cursor": "MORE"})
    routes["/markets"] = ok({"markets": [market], "cursor": "MORE"})
    auditor = _auditor(store, FakeHttpClient(routes))
    result = auditor.audit_cohort(
        tmp_path / "partial",
        events=[_cohort_event()],
        discover_series=False,
    )
    event = result.events[0]
    gate = {g.name: g for g in event.gates}
    assert gate["listing_pagination_complete"].satisfied is False
    assert "contract universe" in " ".join(gate["listing_pagination_complete"].blocks)
    assert result.complete is False


def test_bounded_candidate_selection_does_not_become_a_coverage_claim(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """The candidate bound is a cost bound and is reported as one."""
    markets = [
        _cpi_market(ticker=f"KXCPI-25JAN-T{i}", event_ticker=f"KXCPI-25JAN{i}") for i in range(3)
    ]
    auditor = _auditor(store, FakeHttpClient(_cpi_routes(markets)), max_contracts=2)
    result = auditor.audit_cohort(
        tmp_path / "bounded",
        events=[_cohort_event()],
        discover_series=False,
    )
    event = result.events[0]
    gate = {g.name: g for g in event.gates}
    assert gate["candidate_selection_unbounded"].satisfied is False
    assert event.cohort_size["selected_for_deep_audit"] == 2
    assert event.cohort_size["deduped_candidates"] == 3
    assert result.complete is False


def test_discovery_cannot_claim_a_complete_universe(store: Any, tmp_path: pathlib.Path) -> None:
    series = [
        {"ticker": "KXCPI", "title": "CPI threshold", "category": "Economics"},
        {"ticker": "KXFED", "title": "Fed decision", "category": "Economics"},
    ]
    routes = _cpi_routes([_cpi_market()])
    routes["/series"] = ok({"series": series, "cursor": None})
    auditor = _auditor(store, FakeHttpClient(routes))
    result = auditor.audit_cohort(
        tmp_path / "discovery",
        events=[_cohort_event()],
        discover_series=True,
    )
    basis = result.as_dict()["discovery_basis"]
    assert basis["complete_universe_claimed"] is False
    assert basis["method"] == "exchange_listing_keyword_filter"
    # The keyword set is recorded so a reader can see what bounded the result.
    assert "inflation" in basis["keyword_terms"]["cpi"]
    discovery_doc = json.loads((tmp_path / "discovery" / "series_discovery.json").read_text())
    assert discovery_doc["complete_universe_claimed"] is False
    assert discovery_doc["matches"]["cpi"] == ["KXCPI"]


def test_audit_writes_machine_readable_outputs_and_fails_closed(
    store: Any, tmp_path: pathlib.Path
) -> None:
    """With every endpoint blocked, the audit must not claim success."""
    client = FakeHttpClient(
        {
            "/historical/cutoff": ok(CUTOFF_BODY),
            "/historical/markets": ok({"markets": [], "cursor": None}),
            "/markets": ok({"markets": [], "cursor": None}),
            "/series": ok({"series": [], "cursor": None}),
            "/news.release/archives": (403, "Access Denied"),
        }
    )
    auditor = _auditor(store, client)
    result = auditor.audit_cohort(
        tmp_path / "out",
        events=[_cohort_event(), _employment_cohort_event()],
        discover_series=False,
    )

    assert result.complete is False
    assert result.status in {"no_candidates", "partial", "blocked"}
    assert result.as_dict()["no_post_event_volume_selection"] is True

    coverage = json.loads((tmp_path / "out" / "coverage.json").read_text())
    assert coverage["complete"] is False
    assert coverage["event_count"] == 2
    assert all(e["status"] != "audited" for e in coverage["events"])
    # A reachable listing that returns nothing is zero attempts to acquire, which
    # is distinct from a blocked request.
    assert coverage["cohort_size"]["acquired_candidates"] == 0
    assert coverage["cohort_size"]["attempted_records"] == 0
    assert (tmp_path / "out" / "raw_hashes.json").exists()
    card = json.loads((tmp_path / "out" / "event_card.json").read_text())
    assert card["status"] == "no_audited_event", (
        "an event card cannot be fabricated from an access failure"
    )


def test_interior_candle_hole_blocks_reconstruction(store: Any, tmp_path: pathlib.Path) -> None:
    """A hole inside the returned range invalidates the affected window."""
    holed = {
        "candlesticks": [
            {
                "end_period_ts": 1736946000,
                "price": {
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "mean": None,
                    "previous": "0.90",
                },
                "volume": "0.00",
                "open_interest": "5.00",
                "yes_ask": {"open": "0.91", "high": "0.91", "low": "0.91", "close": "0.91"},
                "yes_bid": {"open": "0.89", "high": "0.89", "low": "0.89", "close": "0.89"},
            },
            # 14:00 is missing, so the 60-minute grid has an interior hole.
            {
                "end_period_ts": 1736953200,
                "price": {
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "mean": None,
                    "previous": "0.92",
                },
                "volume": "0.00",
                "open_interest": "5.00",
                "yes_ask": {"open": "0.93", "high": "0.93", "low": "0.93", "close": "0.93"},
                "yes_bid": {"open": "0.91", "high": "0.91", "low": "0.91", "close": "0.91"},
            },
        ]
    }
    auditor = _auditor(
        store, FakeHttpClient(_cpi_routes([_cpi_market()], candles=holed)), candle_contracts=1
    )
    result = auditor.audit_cohort(
        tmp_path / "holed",
        events=[_cohort_event()],
        discover_series=False,
        candle_intervals=(60,),
    )
    event = result.events[0]
    gate = {g.name: g for g in event.gates}
    assert gate["candle_series_on_grid_without_interior_holes"].satisfied is False
    audit = next(a for a in event.candle_audits if a.interval_minutes == 60)
    assert audit.spacing["holes_present"] is True
    assert audit.spacing["usable_for_replay"] is False
    # The two-sided quote gate is still satisfied: a hole and a missing side are
    # different failures.
    assert gate["candle_quotes_two_sided"].satisfied is True
    assert result.complete is False


def test_audit_candidate_with_no_expectation_records_unavailability(
    store: Any, tmp_path: pathlib.Path
) -> None:
    auditor = _auditor(store, FakeHttpClient(_cpi_routes([_cpi_market()])), candle_contracts=1)
    result = auditor.audit_cohort(
        tmp_path / "out2",
        events=[_cohort_event()],
        discover_series=False,
        # The fixture series is on the 60-minute grid, so only that resolution is
        # requested. Asking for 1 minute against it would register a hole per
        # minute and the run would be incomplete for the wrong reason.
        candle_intervals=(60,),
    )
    event = result.events[0]
    assert event.missing_expectations["status"] == "unavailable"
    assert event.missing_expectations["vendor_consensus_assumed_free"] is False
    assert event.missing_expectations["revised_series_substituted"] is False
    assert event.release is not None
    assert event.release["revision_status"] == "initial"
    assert event.release["values"]["cpi_headline_sa_mom_pct"] == "0.1"
    assert event.release["raw_hash"]
    # The cohort's own source URLs are retained so the schedule is re-derivable.
    assert event.source_urls["initial_release_url"].endswith("cpi_01152025.htm")
    assert event.source_urls["calendar_url"].endswith("01_sched_list.htm")

    # The candidate was eligible before the release and is recorded as such.
    assert len(event.eligible_candidates) >= 1
    candidate = event.eligible_candidates[0]
    assert candidate.known_at_release is True
    assert candidate.window_overlap is True
    assert candidate.cohort == "downstream"
    assert candidate.as_dict()["selection_basis"] == (
        "series_lifecycle_only_no_post_event_volume_filter"
    )
    assert event.status == "audited"
    # Every gate this fixture can satisfy is satisfied, so the read itself finished.
    assert [g.name for g in event.unsatisfied_gates] == []
    assert result.acquisition_complete is True
    # Acquisition success is coverage of the requests, not eligibility of the cohort.
    # No verified rule-version record is configured here, so the cohort-level
    # scientific gates are genuinely unsatisfied and must be named rather than
    # folded into a green completion.
    eligibility = result.study_eligibility
    assert eligibility["verified_rule_version_record_count"] == 0
    for gate in ("rule_vintage_gate", "source_semantics_gate"):
        assert eligibility[gate]["satisfied"] is False
        assert eligibility[gate]["blocks"], f"{gate} is unsatisfied, so it cannot block nothing"
    assert set(result.unsatisfied_scientific_gates) == {
        "rule_vintage_gate",
        "source_semantics_gate",
    }
    assert result.complete is False
    assert result.status == "acquisition_complete_study_blocked"
