"""Operations test suite.

Everything here runs against the real :class:`CohortAuditor`, the real
:class:`RawStore` and the real public clients; only ``httpx.Client.get`` -- the
network boundary -- is replaced by a fixture that answers the documented URLs.
The real archival path therefore runs on every test, so each assertion is made
against bytes that were actually archived and read back rather than against a
mock's return value.

Three facts are pinned because each is otherwise invisible and would corrupt a
result silently:

* A 2xx response is not study eligibility: the audit can acquire contracts and
  still have no verified eligible market id, and no unsatisfied gate may be
  read as coverage.
* A capture poll is an independent snapshot: ``tick_complete`` stays false, no
  source clock is claimed, and the receipt clocks come from the envelope.
* A stored payload that is altered under the same hash must fail verification
  rather than be counted as a readable record.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
from collections.abc import Mapping
from typing import Any

import pytest
import yaml

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from market_propagation.ingest.transport import (
    HttpTransport,
)
from market_propagation.operations import (
    capture_snapshots,
    event_card,
    quality_report,
    run_audit,
)
from market_propagation.storage import RawStore


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


Entry = tuple


def ok(body: Any, headers: dict[str, str] | None = None) -> Entry:
    return (200, body, headers or {})


class FakeHttpClient:
    """Replays queued responses keyed by URL fragment, in declaration order.

    A value may be one entry or a list of entries. An unregistered URL is a hard
    error, which keeps a test from silently exercising an endpoint it never
    declared.
    """

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.calls: list[str] = []
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


CUTOFF_BODY = {
    "market_positions_last_updated_ts": "2025-06-15T00:00:00Z",
    "market_settled_ts": "2025-06-15T00:00:00Z",
    "orders_updated_ts": "2025-06-15T00:00:00Z",
    "trades_created_ts": "2025-06-15T00:00:00Z",
}

# A January 2025 contract: open before the release, close after the window.
MARKET_BODY = {
    "ticker": "KXCPI-25JAN-T0.3",
    "event_ticker": "KXCPI-25JAN",
    "series_ticker": "KXCPI",
    "market_type": "binary",
    "status": "finalized",
    "strike_type": "greater",
    "floor_strike": 0.3,
    "open_time": "2025-01-01T00:00:00Z",
    "close_time": "2025-01-31T00:00:00Z",
    "settlement_ts": "2025-01-31T14:00:00Z",
    "volume_fp": "3022.04",
    "open_interest_fp": "15842.78",
    "last_price_dollars": "0.4100",
    "rules_primary": "If the CPI rises above 0.3%, this market resolves YES.",
    "rules_secondary": "Resolves from the BLS Consumer Price Index release.",
}

# The same series, closed the day before the release: direct-resolution material.
CLOSED_MARKET_BODY = {
    **MARKET_BODY,
    "ticker": "KXCPI-25JAN-CLOSED",
    "close_time": "2025-01-14T00:00:00Z",
    "settlement_ts": "2025-01-14T12:00:00Z",
}

# The negative control: a market that never existed before the release, so it can
# neither be a candidate nor be counted as one.
FUTURE_MARKET_BODY = {
    **MARKET_BODY,
    "ticker": "KXCPI-25JAN-LATER",
    "open_time": "2025-03-01T00:00:00Z",
    "close_time": "2025-03-31T00:00:00Z",
}

HISTORICAL_CANDLES = {
    "ticker": "KXCPI-25JAN-T0.3",
    "candlesticks": [
        {
            "end_period_ts": 1736949600,  # 2025-01-15T14:00:00Z
            "open_interest": "576.00",
            "price": {
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "mean": None,
                "previous": "0.9100",
            },
            "volume": "0.00",
            "yes_ask": {"open": "0.9300", "high": "0.9300", "low": "0.9300", "close": "0.9300"},
            "yes_bid": {"open": "0.8900", "high": "0.8900", "low": "0.8900", "close": "0.8900"},
        },
        {
            "end_period_ts": 1736953200,  # 2025-01-15T15:00:00Z
            "open_interest": "576.00",
            "price": {
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "mean": None,
                "previous": "0.9200",
            },
            "volume": "0.00",
            "yes_ask": {"open": "0.9400", "high": "0.9400", "low": "0.9400", "close": "0.9400"},
            "yes_bid": {"open": "0.9000", "high": "0.9000", "low": "0.9000", "close": "0.9000"},
        },
        {
            "end_period_ts": 1736956800,  # 2025-01-15T16:00:00Z
            "open_interest": "576.00",
            "price": {
                "open": "0.9300",
                "high": "0.9400",
                "low": "0.9300",
                "close": "0.9400",
                "mean": "0.9300",
                "previous": "0.9200",
            },
            "volume": "23.00",
            "yes_ask": {"open": "0.9500", "high": "0.9500", "low": "0.9500", "close": "0.9500"},
            "yes_bid": {"open": "0.9100", "high": "0.9100", "low": "0.9100", "close": "0.9100"},
        },
    ],
}

SERIES_LISTING = {
    "series": [
        {"ticker": "KXCPI", "title": "Consumer Price Index"},
        {"ticker": "KXCPIYOY", "title": "CPI year over year"},
        {"ticker": "KXPAYROLLS", "title": "Nonfarm payrolls"},
    ]
}

# A real archived release page: the embargo line, the period, the values.
CPI_HTML = """
<title>Consumer Price Index News Release - 2025 M12 Results</title>
<pre>
Transmission of material in this release is embargoed until
8:30 a.m. (ET) Wednesday, January 15, 2025      USDL-25-0001
CONSUMER PRICE INDEX - DECEMBER 2024
The Consumer Price Index for All Urban Consumers (CPI-U) rose 0.4 percent in December on a seasonally adjusted basis.
The all items index rose 2.9 percent over the last 12 months.
</pre>
"""

ORDERBOOK_BODY = {
    "orderbook_fp": {
        "yes_dollars": [["0.4100", "120.00"], ["0.4000", "300.00"]],
        "no_dollars": [["0.5700", "50.00"]],
    }
}

POLYMARKET_BOOK_BODY = {
    "market": "0xabc",
    "asset_id": "token-1",
    "bids": [{"price": "0.41", "size": "120"}],
    "asks": [{"price": "0.43", "size": "80"}],
    "hash": "0xdeadbeef",
    "timestamp": "1736950000000",
}

CALENDAR_HTML = """
<table>
<tr><td>Wednesday, January 15, 2025</td><td>08:30 AM</td>
<td><b>Consumer Price Index</b> for December 2024</td></tr>
</table>
<p>NOTE: All times on calendar are Eastern Time.</p>
"""

ARCHIVE_INDEX_HTML = """
<a href="/news.release/archives/cpi_06052026.htm">May 2026 Consumer Price Index</a>
<a href="/news.release/archives/cpi_01152025.htm">December 2024 Consumer Price Index</a>
"""

# The archived Employment Situation page for the second cohort event. The
# embargo line names the same instant as the cohort schedule, which is how the
# card can state agreement without treating it as an observation.
EMPSIT_HTML = """
<title>Employment Situation News Release - 2024 M12 Results</title>
<pre>
Transmission of material in this news release is embargoed until
8:30 a.m. (ET) Friday, January 10, 2025      USDL-25-0002
THE EMPLOYMENT SITUATION - DECEMBER 2024
Total nonfarm payroll employment increased by 256,000 in December, and the unemployment rate
was unchanged at 4.1 percent.
</pre>
"""


def _write(path: pathlib.Path, payload: Any) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


@pytest.fixture()
def configs(tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    """A two-event study cohort and its own window, in the canonical shapes."""
    cohort = _write(
        tmp_path / "configs" / "cohort.yaml",
        {
            "cohort": {
                "cohort_id": "test_cohort",
                # Deliberately empty: acquisition must never imply eligibility.
                "verified_eligible_market_ids": [],
                "verified_eligible_market_id_count": 0,
            },
            "events": [
                {
                    "event_id": "cpi_2025_01",
                    "family": "cpi",
                    "reference_period": "DECEMBER 2024",
                    "scheduled_at": "2025-01-15T13:30:00Z",
                    "source_url": "https://www.bls.gov/schedule/2025/01_sched_list.htm",
                    "calendar_url": "https://www.bls.gov/schedule/2025/01_sched_list.htm",
                    "initial_release_url": "https://www.bls.gov/news.release/archives/cpi_01152025.htm",
                },
                {
                    "event_id": "empsit_2025_01",
                    "family": "employment",
                    "reference_period": "DECEMBER 2024",
                    "scheduled_at": "2025-01-10T13:30:00Z",
                    "source_url": "https://www.bls.gov/schedule/2025/01_sched_list.htm",
                },
            ],
        },
    )
    windows = _write(
        tmp_path / "configs" / "event_windows.yaml",
        {
            "windows": {
                "main": {
                    "window_id": "main",
                    "pre_event_seconds": 1800,
                    "post_event_seconds": 3600,
                    "pre_event_role": "baseline",
                    "post_event_role": "response",
                }
            },
            "alignment": {
                "reference_point": "scheduled_release_time",
                "reference_precision": "minute",
                "source_calendar_timezone": "America/New_York",
                "storage_timezone": "UTC",
            },
            "secondary_horizons_seconds": [300, 900],
            "contamination_stop": [{"event_id": "next_release", "stop_action": "truncate"}],
            "restrictions": ["no_expectation_source_available"],
        },
    )
    return cohort, windows


def _routes(*, markets: list[dict[str, Any]]) -> dict[str, Any]:
    """Fixture routes for one CPI event.

    The candle and cutoff paths precede the listing paths so the substring lookup
    cannot serve a listing body to a candle request.
    """
    return {
        "/orderbook": ok(ORDERBOOK_BODY),
        "/book": ok(POLYMARKET_BOOK_BODY),
        "/historical/markets/KXCPI-25JAN-T0.3/candlesticks": ok(HISTORICAL_CANDLES),
        "/historical/markets/KXCPI-25JAN-CLOSED/candlesticks": ok(HISTORICAL_CANDLES),
        "/historical/trades": ok({"trades": [], "cursor": None}),
        "/historical/cutoff": ok(CUTOFF_BODY),
        "/historical/markets": ok({"markets": markets, "cursor": None}),
        "/markets": ok({"markets": markets, "cursor": None}),
        "/series": ok(SERIES_LISTING),
        "/news.release/archives/cpi_": ok(CPI_HTML),
        "/news.release/archives/empsit_": ok(EMPSIT_HTML),
        "/news.release/archives": ok(ARCHIVE_INDEX_HTML),
    }


def _install_transport(monkeypatch: pytest.MonkeyPatch, routes: dict[str, Any]) -> FakeHttpClient:
    """Replace only the network boundary, leaving the real archival path intact.

    ``HttpTransport`` is constructed by the operations themselves, so the client
    is injected through the one argument it takes from outside: the ``httpx.Client``.
    """
    client = FakeHttpClient(routes)
    real_init = HttpTransport.__init__

    def patched(self: HttpTransport, store: Any, **kwargs: Any) -> None:
        kwargs["client"] = client
        kwargs["sleep"] = lambda _seconds: None
        real_init(self, store, **kwargs)

    monkeypatch.setattr(HttpTransport, "__init__", patched)
    return client


def test_run_audit_reads_the_study_config_and_reports_real_coverage(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cohort_path, windows_path = configs
    markets = [MARKET_BODY, CLOSED_MARKET_BODY, FUTURE_MARKET_BODY]
    client = _install_transport(monkeypatch, _routes(markets=markets))
    out = tmp_path / "audit"

    result = run_audit(
        out,
        cohort_path=cohort_path,
        windows_path=windows_path,
        max_contracts=12,
        max_candle_contracts=2,
    )

    # The window and the cohort come from the study's own files, and the audit
    # reports the ones it actually applied.
    assert result["window_basis"]["pre_event_seconds"] == 1800
    assert result["window_basis"]["post_event_seconds"] == 3600
    assert result["window_basis"]["source_config"] == str(windows_path)
    assert result["window_basis"]["candle_intervals_minutes"] == [60, 1]
    assert result["cohort_path"] == str(cohort_path)

    assert len(result["coverage"]["events"]) == 2
    cpi = next(e for e in result["coverage"]["events"] if e["event_id"] == "cpi_2025_01")
    assert cpi["status"] == "audited_with_errors" or cpi["status"] == "audited"

    # A contract whose own lifecycle postdates the release is kept and marked
    # rather than silently dropped: exclusion is a recorded fact about it.
    candidates = {c["ticker"]: c for c in cpi["candidates"]}
    live = candidates["KXCPI-25JAN-T0.3"]
    assert live["eligible"] is True
    assert live["window_overlap"] is True
    later = candidates["KXCPI-25JAN-LATER"]
    assert later["active_at_release"] is False
    assert "created_after_release" in later["exclusion_reasons"]
    assert later["eligible"] is False
    # The venue's own pre-release close separates direct-resolution material.
    assert cpi["direct_closed_pre_release_count"] == 1
    closed = candidates["KXCPI-25JAN-CLOSED"]
    assert closed["cohort"] == "direct_closed_pre_release"
    assert closed["window_overlap"] is False

    # 2xx responses were observed, and eligibility is still not established.
    assert result["eligibility"]["study_eligibility_established"] is False
    assert result["eligibility"]["verified_eligible_market_ids"] == []
    assert result["access"]["http_success_is_not_study_eligibility"] is True
    assert result["access"]["empty_result_substitution_used"] is False

    # The auditor's own artifacts are written through the auditor, and the raw
    # store holds the bytes its hashes name.
    outputs = result["outputs"]
    for key in ("coverage", "raw_hashes", "event_card", "series_discovery"):
        assert pathlib.Path(outputs[key]).exists(), key
    store = RawStore(out / "raw")
    manifest = json.loads(pathlib.Path(outputs["raw_hashes"]).read_text())
    assert manifest["count"] > 0
    for entry in manifest["raw_hashes"]:
        assert len(store.get(entry["raw_hash"])) > 0

    # Every gate is reported with its own detail, and an unsatisfied one names
    # the claims it blocks rather than being folded into a status.
    gate_names = {g["gate"] for g in cpi["coverage_gates"]}
    assert "contract_universe_attempted" in gate_names
    assert "release_payload_archived" in gate_names
    assert "candle_series_on_grid_without_interior_holes" in gate_names
    assert set(cpi["unsatisfied_gates"]) <= gate_names
    for gate in cpi["coverage_gates"]:
        assert gate["detail"]
    unsatisfied = [g for g in cpi["coverage_gates"] if not g["satisfied"]]
    assert unsatisfied, "the fixture leaves coverage gates unsatisfied on purpose"
    for gate in unsatisfied:
        assert gate["blocks"], gate["gate"]

    # The audit really walked the documented endpoints, and reached for the
    # release payload rather than assuming it.
    joined = "\n".join(client.calls)
    assert "/historical/cutoff" in joined
    assert "/historical/markets" in joined
    assert "/markets" in joined
    assert "/series" in joined
    assert "/historical/trades" in joined
    assert "candlesticks" in joined
    assert "/news.release/archives/cpi_01152025.htm" in joined
    assert joined.count("/orderbook") == 0, "the audit reads no order book"

    # Both cohort families were audited through their own BLS archive slug, and
    # each archived its own first-release payload.
    empsit = next(e for e in result["coverage"]["events"] if e["event_id"] == "empsit_2025_01")
    assert empsit["release"]["raw_hash"]
    assert empsit["release"]["values"]["payrolls_change_jobs"] == "256000"
    assert "/news.release/archives/empsit_01102025.htm" in joined

    # The payload's own embargo line agrees with the cohort's scheduled instant,
    # and that agreement is recorded as a comparison, not as an observation.
    assert cpi["release"]["schedule_agreement"] == "agrees_with_calendar"
    assert cpi["release"]["embargo_time_from_payload"] == "2025-01-15T13:30:00+00:00"
    assert cpi["release"]["revision_status"] == "initial"

    # A blocked endpoint is recorded as access, never substituted with an empty
    # result.
    assert result["access"]["blocked_count"] == 0


def test_run_audit_records_a_blocked_release_as_access_not_absence(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The archived release fails; the event must not read as 'no release existed'."""
    cohort_path, windows_path = configs
    routes = _routes(markets=[MARKET_BODY])
    routes["/news.release/archives/cpi_"] = (503, "upstream unavailable")
    client = _install_transport(monkeypatch, routes)
    out = tmp_path / "audit"

    result = run_audit(out, cohort_path=cohort_path, windows_path=windows_path)

    cpi = next(e for e in result["coverage"]["events"] if e["event_id"] == "cpi_2025_01")
    assert cpi["release"] is None
    release_gate = next(g for g in cpi["coverage_gates"] if g["gate"] == "release_payload_archived")
    assert release_gate["satisfied"] is False
    assert "release_payload_archived" in cpi["unsatisfied_gates"]

    blocked_urls = [b["url"] for b in cpi["blocked"]]
    assert any("cpi_01152025" in url for url in blocked_urls)
    assert result["access"]["blocked_count"] > 0
    # The failing body is still archived, so the failure names a real payload.
    assert any(b.get("payload_hash") for b in cpi["blocked"])
    assert any("cpi_01152025" in call for call in client.calls)


def test_run_audit_rejects_an_absent_or_shapeless_configuration(
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(FileNotFoundError, match="cohort configuration not found"):
        run_audit(tmp_path / "audit", cohort_path=tmp_path / "missing.yaml")

    # A cohort file that carries no events is refused before any request, because
    # an empty cohort has no complete state to report.
    empty = tmp_path / "empty_cohort.yaml"
    empty.write_text("cohort: {cohort_id: x}\n", encoding="utf-8")
    windows = tmp_path / "windows.yaml"
    windows.write_text(
        "windows:\n  main:\n    pre_event_seconds: 1800\n    post_event_seconds: 3600\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="events"):
        run_audit(tmp_path / "audit", cohort_path=empty, windows_path=windows)

    with pytest.raises(FileNotFoundError, match="event window configuration not found"):
        run_audit(
            tmp_path / "audit",
            cohort_path=empty,
            windows_path=tmp_path / "also_missing.yaml",
        )


def test_run_audit_refuses_to_invent_a_window_when_the_config_lacks_one(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
) -> None:
    cohort_path, _ = configs
    shapeless = tmp_path / "no_window.yaml"
    shapeless.write_text("windows: {}\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"windows\.main"):
        run_audit(tmp_path / "audit", cohort_path=cohort_path, windows_path=shapeless)


def test_capture_archives_real_snapshots_and_keeps_tick_complete_false(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _install_transport(monkeypatch, _routes(markets=[]))
    out = tmp_path / "capture"

    result = capture_snapshots(
        out,
        venue="kalshi",
        contract_id="KXCPI-25JAN-T0.3",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )

    assert result["mode"] == "snapshots"
    assert result["tick_complete"] is False
    assert result["orders_submitted"] == 0
    assert result["authenticated_channels_used"] is False
    assert result["observation_count"] == 1
    observation = result["observations"][0]
    assert observation["contract_id"] == "KXCPI-25JAN-T0.3"
    # The receipt clocks come from the response envelope, so they are real.
    assert isinstance(observation["monotonic_ns"], int)
    assert observation["received_time"].endswith("+00:00")
    assert observation["usable_time"] is not None
    # No source clock is claimed, and the unknown precision is stated.
    assert observation["source_time"] is None
    assert observation["source_time_precision"] == "unknown"
    assert observation["source_time_status"]
    # The kalshi book has no quoted asks; the derived basis is named.
    assert observation["ask_levels_basis"]
    assert observation["bid_levels"] == 2

    # The archived body is readable and is the fixture that was served.
    raw_hash = observation["raw_hash"]
    stored = RawStore(out / "raw").get(raw_hash)
    assert json.loads(stored) == ORDERBOOK_BODY
    assert any("/orderbook" in call for call in client.calls)

    # Normalized output is on disk, sealed as the two tables, and readable back.
    assert result["normalized"]["written"] is True
    events_ref = result["normalized"]["book_events"]
    assert events_ref["row_count"] == 1
    assert pathlib.Path(events_ref["path"]).exists()
    assert result["normalized"]["quotes"]["row_count"] == 1
    assert result["outputs"]["capture"] == str(out / "capture.json")
    assert json.loads((out / "capture.json").read_text())["tick_complete"] is False


def test_capture_stops_on_the_duration_bound_and_cannot_exceed_it(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_transport(monkeypatch, _routes(markets=[]))
    out = tmp_path / "capture"

    result = capture_snapshots(
        out,
        venue="kalshi",
        contract_id="KXCPI-25JAN-T0.3",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )
    assert result["stopped_reason"] == "duration_elapsed"
    assert result["interrupted"] is False
    # A zero duration is honoured: exactly one poll, never an unbounded loop.
    assert result["requests_completed"] == 1
    assert result["request_budget"] == 1


def test_capture_records_a_failing_public_read_as_a_blocked_outcome(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = _routes(markets=[])
    routes["/orderbook"] = (404, {"error": "market not found"})
    _install_transport(monkeypatch, routes)
    out = tmp_path / "capture"

    result = capture_snapshots(
        out,
        venue="kalshi",
        contract_id="NOT-A-MARKET",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )

    assert result["observation_count"] == 0
    assert result["stopped_reason"] == "blocked"
    assert result["blocked_count"] == 1
    blocked = result["blocked"][0]
    assert blocked["status_code"] == 404
    assert blocked["empty_result"] is False
    assert blocked["payload_hash"], "the failing body must still be archived"
    # Nothing was observed, so no dataset is sealed that could read as a quiet book.
    assert result["normalized"]["written"] is False
    assert result["normalized"]["reason"]


def test_capture_requires_a_market_and_a_known_venue(tmp_path: pathlib.Path) -> None:
    with pytest.raises(TypeError):
        capture_snapshots(tmp_path / "capture")  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="contract_id"):
        capture_snapshots(tmp_path / "capture", contract_id="   ")
    with pytest.raises(ValueError, match="venue must be one of"):
        capture_snapshots(tmp_path / "capture", venue="nasdaq", contract_id="X")
    with pytest.raises(ValueError, match="duration_seconds"):
        capture_snapshots(tmp_path / "capture", contract_id="X", duration_seconds=10_000)


def test_capture_uses_the_public_polymarket_book_and_its_normalizer(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _install_transport(monkeypatch, _routes(markets=[]))
    out = tmp_path / "capture"

    result = capture_snapshots(
        out,
        venue="polymarket",
        contract_id="token-1",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )

    assert result["venue"] == "polymarket"
    assert result["reachability"]["reachable"] is True
    observation = result["observations"][0]
    assert observation["contract_id"] == "token-1"
    assert observation["ask_levels_basis"].startswith("quoted_ask_levels")
    stored = RawStore(out / "raw").get(observation["raw_hash"])
    assert json.loads(stored) == POLYMARKET_BOOK_BODY
    # The documented book GET carries the token as a query parameter.
    assert any("book?token_id=token-1" in call for call in client.calls)


def test_capture_fails_closed_when_polymarket_is_unreachable(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreachable venue is an access outcome, never an empty capture."""
    # Only the probe hosts are declared, and both fail, so reachability is never
    # claimed and no snapshot is attempted.
    routes: dict[str, Any] = {
        "gamma-api.polymarket.com": (503, "unavailable"),
        "/book": (503, "unavailable"),
    }
    _install_transport(monkeypatch, routes)
    out = tmp_path / "capture"

    result = capture_snapshots(
        out,
        venue="polymarket",
        contract_id="token-1",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )

    assert result["stopped_reason"] == "unreachable"
    assert result["reachability"]["reachable"] is False
    assert result["observation_count"] == 0
    assert result["blocked_count"] >= 1
    assert result["normalized"]["written"] is False


def test_quality_report_verifies_stored_bytes_and_states_its_limits(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_transport(monkeypatch, _routes(markets=[]))
    out = tmp_path / "capture"
    capture_snapshots(
        out,
        venue="kalshi",
        contract_id="KXCPI-25JAN-T0.3",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )
    report_path = tmp_path / "quality.json"

    report = quality_report(out / "raw", report_path)

    assert report["receipts"]["count"] == 1
    assert report["receipts"]["verified_payloads"] == 1
    assert report["receipts"]["failed_verifications"] == 0
    assert report["receipts"]["byte_total"] > 0
    assert report["receipts"]["by_source"]["kalshi.orderbook"] == 1
    assert report["http"]["by_status"]["200"] == 1
    assert report["http"]["non_2xx_count"] == 0
    assert report["blobs"]["count"] == 1
    assert report["blobs"]["orphan_count"] == 0
    # No clock metadata exists, so no score is invented for one.
    assert report["clock_quality_score"] is None
    assert report["clock_quality_score_reason"]
    assert report["source_clock_evidence"]["source_time_recorded"] is False
    assert report["source_clock_evidence"]["timing_uncertainty_recorded"] is False
    # One receipt defines no interval, and the absence is stated as such.
    assert report["receipt_spacing_seconds"]["available"] is False
    assert report["receipt_spacing_seconds"]["reason"]
    assert report["completeness_limits"]
    assert json.loads(report_path.read_text())["receipts"]["count"] == 1


def test_quality_report_detects_an_altered_payload(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bytes changed under a stored hash must fail verification, not pass it."""
    _install_transport(monkeypatch, _routes(markets=[]))
    out = tmp_path / "capture"
    capture_snapshots(
        out,
        venue="kalshi",
        contract_id="KXCPI-25JAN-T0.3",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )
    store = RawStore(out / "raw")
    (raw_hash,) = store.stored_hashes()
    blob = out / "raw" / "blobs" / raw_hash[:2] / f"{raw_hash}.bin"
    blob.write_bytes(json.dumps({**ORDERBOOK_BODY, "orderbook_fp": {}}).encode("utf-8"))

    report = quality_report(out / "raw", tmp_path / "quality.json")

    assert report["receipts"]["verified_payloads"] == 0
    assert report["receipts"]["failed_verifications"] == 1
    failure = report["receipts"]["verification_failures"][0]
    assert failure["reason"] == "payload_hash_mismatch"
    assert failure["raw_hash"] == raw_hash


def test_quality_report_distinguishes_a_missing_payload_and_an_orphan(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_transport(monkeypatch, _routes(markets=[]))
    out = tmp_path / "capture"
    capture_snapshots(
        out,
        venue="kalshi",
        contract_id="KXCPI-25JAN-T0.3",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )
    store = RawStore(out / "raw")
    (raw_hash,) = store.stored_hashes()
    blob_dir = out / "raw" / "blobs" / raw_hash[:2]
    (blob_dir / f"{raw_hash}.bin").unlink()
    orphan = blob_dir / f"{'a' * 64}.bin"
    orphan.write_bytes(b"{}")

    report = quality_report(out / "raw", tmp_path / "quality.json")

    assert report["receipts"]["verified_payloads"] == 0
    assert report["receipts"]["verification_failures"][0]["reason"] == "payload_missing"
    # Orphaned bytes are visible rather than silently absent.
    assert report["blobs"]["orphan_hashes"] == ["a" * 64]
    assert report["blobs"]["orphan_note"]


def test_quality_report_counts_non_2xx_receipts_as_failures_not_coverage(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = _routes(markets=[])
    routes["/orderbook"] = (404, {"error": "not found"})
    _install_transport(monkeypatch, routes)
    out = tmp_path / "capture"
    capture_snapshots(
        out,
        venue="kalshi",
        contract_id="NOT-A-MARKET",
        duration_seconds=0.0,
        interval_seconds=0.1,
    )

    report = quality_report(out / "raw", tmp_path / "quality.json")

    # The failing body was archived, so it verifies as bytes while the HTTP
    # outcome is reported as a failure of the request.
    assert report["receipts"]["verified_payloads"] == 1
    assert report["http"]["by_status"]["404"] == 1
    assert report["http"]["non_2xx_count"] == 1
    assert report["http"]["note_by_receipt"]["http 404"] == 1
    assert report["receipts"]["failed_verifications"] == 0


def test_quality_report_requires_a_real_directory(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError, match="raw store directory not found"):
        quality_report(tmp_path / "absent", tmp_path / "quality.json")


def test_receipt_spacing_is_measured_when_several_receipts_exist(
    tmp_path: pathlib.Path,
) -> None:
    """Spacing is reported as observed, and never as a clock-accuracy claim."""
    store = RawStore(tmp_path / "raw")
    for index, second in enumerate((0, 2, 5)):
        store.put(
            f'{{"n": {index}}}'.encode(),
            source="kalshi.orderbook",
            received_time=dt.datetime(2025, 1, 15, 13, 30, second, tzinfo=dt.UTC),
            record_id=f"snap-{index}",
            metadata={"method": "GET", "http_status": 200},
        )

    report = quality_report(tmp_path / "raw", tmp_path / "quality.json")

    spacing = report["receipt_spacing_seconds"]
    assert spacing["available"] is True
    assert spacing["interval_count"] == 2
    assert spacing["min_seconds"] == 2.0
    assert spacing["max_seconds"] == 3.0
    assert report["receipt_spacing_note"]
    assert report["clock_quality_score"] is None


def _audit_fixture(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
    *,
    markets: list[dict[str, Any]],
) -> pathlib.Path:
    cohort_path, windows_path = configs
    _install_transport(monkeypatch, _routes(markets=markets))
    out = tmp_path / "audit"
    run_audit(out, cohort_path=cohort_path, windows_path=windows_path)
    return out


def test_the_release_time_basis_gate_fails_when_the_embargo_line_disagrees(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A disagreement is a failed comparison, not a satisfied gate.

    The agreement value is a categorical string, so ``differs_from_calendar`` and
    ``unverified`` are both truthy. Read as a boolean, the gate passed in exactly
    the two cases it exists to block, and the claim it should refuse was dropped
    from the card's blocked list.
    """
    cohort_path, windows_path = configs
    original = yaml.safe_load(cohort_path.read_text(encoding="utf-8"))
    # The archived payload's own embargo line names 2025-01-15T13:30Z, so an hour
    # later is a schedule the payload contradicts rather than one it confirms.
    for event in original["events"]:
        if event["event_id"] == "cpi_2025_01":
            event["scheduled_at"] = "2025-01-15T14:30:00+00:00"
    disagreed = tmp_path / "configs" / "cohort_offset.yaml"
    disagreed.parent.mkdir(parents=True, exist_ok=True)
    disagreed.write_text(yaml.safe_dump(original), encoding="utf-8")
    _install_transport(monkeypatch, _routes(markets=[MARKET_BODY]))
    out = tmp_path / "audit"

    result = run_audit(
        out,
        cohort_path=disagreed,
        windows_path=windows_path,
        max_contracts=1,
        max_candle_contracts=1,
    )

    cpi = next(e for e in result["coverage"]["events"] if e["event_id"] == "cpi_2025_01")
    assert cpi["release"]["schedule_agreement"] == "differs_from_calendar"
    gate = next(g for g in cpi["coverage_gates"] if g["gate"] == "release_time_basis_named")
    assert gate["satisfied"] is False
    assert gate["blocks"]
    assert "release_time_basis_named" in cpi["unsatisfied_gates"]

    # And the card must carry that failed gate's blocked claim rather than omit it.
    card = event_card(out, tmp_path / "card.json", event_id="cpi_2025_01")
    assert "release_time_basis_named" in card["claims"]["blocked_by_unsatisfied_gates"]
    recorded = next(g for g in card["coverage_gates"] if g["gate"] == "release_time_basis_named")
    assert recorded["satisfied"] is False


def test_event_card_verifies_its_evidence_and_keeps_absences_explicit(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_dir = _audit_fixture(
        tmp_path, configs, monkeypatch, markets=[MARKET_BODY, CLOSED_MARKET_BODY]
    )
    card_path = tmp_path / "card.json"

    card = event_card(audit_dir, card_path, event_id="cpi_2025_01")

    assert card["operation"] == "event_card"
    assert card["event_id"] == "cpi_2025_01"
    assert card["family"] == "cpi"
    assert card["event_status"]
    assert card["cohort_definition_hash"]

    # The card was assembled, and that artifact outcome is stated separately from
    # the audit's own standing: both are read from the coverage artifact the card
    # was built out of, so a partial audit yields a real card that says so.
    coverage = json.loads((audit_dir / "coverage.json").read_text())
    assert card["status"] == "created"
    assert card["audit_status"] == coverage["status"]
    assert card["audit_complete"] == coverage["complete"]
    event_record = next(event for event in coverage["events"] if event["event_id"] == "cpi_2025_01")
    assert card["unsatisfied_gates"] == event_record["unsatisfied_gates"]
    assert set(card["claims"]["blocked_by_unsatisfied_gates"]) == set(card["unsatisfied_gates"])

    # The archived first release is reported with the hash it came from, and the
    # embargo line is never read as an observed publication time.
    release = card["first_release"]
    assert release["retrieved"] is True
    assert release["raw_hash"]
    assert release["values"]
    assert release["embargo_is_not_observed_publication"] is True
    assert release["observed_publication_at"] is None
    assert release["observed_publication_unavailable_reason"]
    RawStore(audit_dir / "raw").get(release["raw_hash"])

    # A closed market is reported as its own cohort, not traded forward.
    closed = card["closed_markets"]
    assert closed["filled_forward"] is False
    assert closed["post_release_response_measured"] is False
    assert any(m["ticker"] == "KXCPI-25JAN-CLOSED" for m in closed["markets"])

    # Every gate is kept, and an unsatisfied one names what it blocks.
    assert card["coverage_gates"]
    for gate in card["coverage_gates"]:
        assert "blocks" in gate and "satisfied" in gate
    for name, blocks in card["claims"]["blocked_by_unsatisfied_gates"].items():
        assert name in card["unsatisfied_gates"]
        assert isinstance(blocks, list)

    # Cited hashes were re-read from the store and none failed.
    verification = card["evidence_verification"]
    assert verification["raw_store_available"] is True
    assert verification["cited_hash_count"] > 0
    assert verification["verified_hash_count"] == verification["cited_hash_count"]
    assert verification["failed_hash_count"] == 0
    assert json.loads(card_path.read_text())["event_id"] == "cpi_2025_01"

    # An expectation-relative claim is listed as unsupported, not estimated.
    assert any("expectation" in claim for claim in card["claims"]["not_supported"])


def test_event_card_names_the_audited_events_when_the_requested_one_is_absent(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_dir = _audit_fixture(tmp_path, configs, monkeypatch, markets=[MARKET_BODY])

    card = event_card(audit_dir, tmp_path / "card.json", event_id="not_an_event")

    assert card["status"] == "event_not_in_audit"
    assert card["requested_event_id"] == "not_an_event"
    assert "cpi_2025_01" in card["audited_event_ids"]
    assert card["status_reason"]
    assert card["first_release"] is None
    assert card["closed_markets"] is None


def test_event_card_reports_a_missing_release_as_an_access_failure(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cohort_path, windows_path = configs
    routes = _routes(markets=[MARKET_BODY])
    routes["/news.release/archives/cpi_"] = (503, "unavailable")
    _install_transport(monkeypatch, routes)
    audit_dir = tmp_path / "audit"
    run_audit(audit_dir, cohort_path=cohort_path, windows_path=windows_path)

    card = event_card(audit_dir, tmp_path / "card.json", event_id="cpi_2025_01")

    release = card["first_release"]
    assert release["retrieved"] is False
    assert release["values"] is None
    assert release["raw_hash"] is None
    assert release["absent_reason"]
    assert release["gate_satisfied"] is False
    # The failure is carried explicitly rather than as a null value.
    assert release["access_failures"]
    assert any("cpi_01152025" in f["url"] for f in release["access_failures"])
    assert card["access_failures"]
    # With no release read there is nothing to assert about first-release values, so
    # the card states null rather than a claim its own gate blocks.
    assert release["values_are_first_release"] is None
    assert release["revisions_kept_separate"] is None
    assert not any("initial release values" in claim for claim in card["claims"]["supported"])
    assert "no first-release payload was retrieved" in release["source"]["provenance_note"]


def test_event_card_reports_an_altered_cited_payload(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_dir = _audit_fixture(tmp_path, configs, monkeypatch, markets=[MARKET_BODY])
    coverage = json.loads((audit_dir / "coverage.json").read_text())
    event = next(e for e in coverage["events"] if e["event_id"] == "cpi_2025_01")
    (raw_hash,) = [h for h in event["raw_hashes"] if isinstance(h, str)][:1]
    blob = audit_dir / "raw" / "blobs" / raw_hash[:2] / f"{raw_hash}.bin"
    blob.write_bytes(b"{}")

    card = event_card(audit_dir, tmp_path / "card.json", event_id="cpi_2025_01")

    verification = card["evidence_verification"]
    assert verification["failed_hash_count"] >= 1
    failure = next(f for f in verification["failures"] if f["raw_hash"] == raw_hash)
    assert failure["reason"] == "payload_hash_mismatch"
    assert (
        verification["verified_hash_count"] + verification["failed_hash_count"]
        == (verification["cited_hash_count"])
    )


def test_event_card_requires_the_audit_artifact(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError, match="audit directory not found"):
        event_card(tmp_path / "absent", tmp_path / "card.json")

    hollow = tmp_path / "hollow"
    hollow.mkdir()
    with pytest.raises(FileNotFoundError, match=r"coverage\.json"):
        event_card(hollow, tmp_path / "card.json")


def test_written_json_is_standards_compliant_with_nulls_not_nan(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-finite number must become null; NaN is not JSON."""
    audit_dir = _audit_fixture(tmp_path, configs, monkeypatch, markets=[MARKET_BODY])
    card = event_card(audit_dir, tmp_path / "card.json", event_id="cpi_2025_01")
    coverage = json.loads((audit_dir / "coverage.json").read_text())

    def walk(value: Any, path: str) -> None:
        if isinstance(value, float):
            assert value == value, f"NaN survived at {path}"
            assert value not in (float("inf"), float("-inf")), f"infinity at {path}"
        elif isinstance(value, Mapping):
            for key, item in value.items():
                walk(item, f"{path}.{key}")
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}[{index}]")

    walk(card, "card")
    walk(coverage, "coverage")
    text = (tmp_path / "card.json").read_text()
    assert "NaN" not in text and "Infinity" not in text
