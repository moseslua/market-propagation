"""Clock provenance and point-in-time admissibility.

A capture observes a receipt instant and a monotonic counter. Both are real
readings on the collector's own time axis, and both are enough to make a record
usable there. Neither is evidence that the axis was synchronized to UTC, and
neither measures how long a source feed took to arrive. These tests hold the
three consequences of that split: an ordinary capture reports its
synchronization as unmeasured while keeping the readings that are real, a caller
that measured a bound keeps that bound, and an unmeasured clock still cannot
admit a record before the instant it was received.
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
    UTC,
    Availability,
    Clock,
    Provenance,
    Quote,
    QuoteValidity,
)
from market_propagation.ingest.transport import HttpTransport
from market_propagation.operations import capture_snapshots
from market_propagation.point_in_time import features_asof
from market_propagation.storage import read_parquet

CONTRACT_ID = "KXCPI-25JAN-T0.3"
MARKET_KEY = f"kalshi|{CONTRACT_ID}"

#: The documented Kalshi snapshot: bids only, so the ask side is derived from the
#: NO bids as ``1 - p``.
ORDERBOOK_BODY = {
    "orderbook_fp": {
        "yes_dollars": [["0.4100", "120.00"], ["0.4000", "300.00"]],
        "no_dollars": [["0.5700", "50.00"]],
    }
}


class FakeResponse:
    """Enough of the httpx response surface for the transport."""

    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content
        self.headers: dict[str, str] = {}


def ok(body: Any) -> tuple:
    return (200, body)


class FakeHttpClient:
    """Replays responses keyed by URL fragment; an undeclared URL is a hard error."""

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.calls: list[str] = []

    def get(self, url: str, headers: dict[str, str] | None = None) -> FakeResponse:
        self.calls.append(url)
        for key, value in self.routes.items():
            if key in url:
                status, body = value
                content = json.dumps(body).encode("utf-8") if isinstance(body, dict) else body
                return FakeResponse(status, content)
        raise AssertionError(f"unexpected request in test: {url}")

    def close(self) -> None:
        pass


def _install_transport(monkeypatch: pytest.MonkeyPatch, routes: dict[str, Any]) -> FakeHttpClient:
    """Replace only the network boundary, leaving the real archival path intact."""
    client = FakeHttpClient(routes)
    real_init = HttpTransport.__init__

    def patched(self: HttpTransport, store: Any, **kwargs: Any) -> None:
        kwargs["client"] = client
        kwargs["sleep"] = lambda _seconds: None
        real_init(self, store, **kwargs)

    monkeypatch.setattr(HttpTransport, "__init__", patched)
    return client


def quote(received: dt.datetime, clock: Clock) -> Quote:
    return Quote(
        venue="kalshi",
        contract_id=CONTRACT_ID,
        clock=clock,
        provenance=Provenance("a" * 64, "snapshot-00001", "kalshi.orderbook"),
        bid=Decimal("0.4100"),
        ask=Decimal("0.4300"),
        bid_size=Decimal("120.00"),
        ask_size=Decimal("50.00"),
        validity=QuoteValidity.VALID,
        last_price_change=received,
        last_verified=received,
        last_trade=None,
    )


def test_ordinary_capture_reports_unmeasured_synchronization_with_real_readings(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_transport(monkeypatch, {"/orderbook": ok(ORDERBOOK_BODY)})
    out = tmp_path / "capture"

    result = capture_snapshots(
        out,
        venue="kalshi",
        contract_id=CONTRACT_ID,
        duration_seconds=0.0,
        interval_seconds=0.1,
    )

    observation = result["observations"][0]
    received = dt.datetime.fromisoformat(observation["received_time"])
    assert received.tzinfo is not None
    # The receipt and the monotonic reading are genuine observations, so the
    # record keeps a usable time on this process's own axis.
    assert isinstance(observation["monotonic_ns"], int)
    assert observation["monotonic_ns"] > 0
    assert dt.datetime.fromisoformat(observation["usable_time"]) == received
    assert dt.datetime.fromisoformat(observation["availability_upper"]) == received

    # No synchronization audit ran, so none is claimed.
    assert observation["availability_quality"] == "unknown"
    assert observation["availability_quality"] in Availability.QUALITIES
    assert observation["availability_quality"] != "clock_synced"
    assert observation["availability_basis"]
    assert observation["source_time"] is None

    # The interval this process recorded is zero wide, and that width is reported
    # as the interval width rather than as a physical timing uncertainty.
    assert observation["availability_width_seconds"] == 0.0
    assert dt.datetime.fromisoformat(observation["availability_lower"]) == received
    assert observation["timing_uncertainty_seconds"] is None
    assert observation["timing_uncertainty_status"]

    # Exact quote values are untouched by the provenance correction.
    assert observation["bid"] == "0.4100"
    assert observation["ask"] == "0.4300"
    assert observation["validity"] == "valid"

    # The sealed quote carries the same unmeasured provenance and the same values.
    sealed = read_parquet(result["normalized"]["quotes"]["path"]).iloc[0]
    assert sealed["availability_quality"] == "unknown"
    assert sealed["availability_lower"] == sealed["availability_upper"] == sealed["usable_time"]
    assert sealed["bid"] == Decimal("0.4100")
    assert sealed["ask"] == Decimal("0.4300")

    # The capture document states the unmeasured clock, and an unmeasured
    # uncertainty stays null with its reason rather than becoming a zero.
    written = json.loads((out / "capture.json").read_text(encoding="utf-8"))
    policy = written["source_clock_policy"]
    assert policy["availability_quality"] == "unknown"
    assert policy["timing_uncertainty_seconds"] is None
    assert policy["timing_uncertainty_status"]
    assert written["observations"][0]["timing_uncertainty_seconds"] is None


def test_a_measured_or_synthetic_clock_keeps_its_stated_bound() -> None:
    received = dt.datetime(2026, 9, 13, 12, 0, tzinfo=UTC)

    # A caller that measured the offset between its own clock and the source's
    # states the measurement, and the stated bound survives.
    measured = Clock.captured(
        None,
        received,
        monotonic_ns=1234,
        uncertainty_seconds=2.5,
        quality="clock_synced",
        basis="measured_offset_bound_over_ntp",
    )
    assert measured.availability.quality == "clock_synced"
    assert measured.timing_uncertainty_seconds == pytest.approx(2.5)
    assert measured.availability.lower == received - dt.timedelta(seconds=2.5)
    # The bound widens the interval backwards. The receipt instant stays the
    # moment the record was certainly usable.
    assert measured.usable_time == received

    # A known synthetic process declares its own documented window.
    synthetic = Clock.captured(
        received - dt.timedelta(seconds=5),
        received,
        uncertainty_seconds=0.5,
        quality="clock_synced",
        basis="fixture_documented_receipt_window",
    )
    assert synthetic.availability.quality == "clock_synced"
    assert synthetic.timing_uncertainty_seconds == pytest.approx(0.5)

    explicit = Availability.captured(
        received, 1.0, quality="clock_synced", basis="measured_offset_bound_1s"
    )
    assert explicit.quality == "clock_synced"

    # The stated bound reaches the feature fold intact.
    entry = features_asof([quote(received, measured)], received + dt.timedelta(seconds=1))[
        MARKET_KEY
    ]
    assert entry["clock_quality"] == "clock_synced"
    assert entry["timing_uncertainty_seconds"] == pytest.approx(2.5)
    assert entry["valid"] is True


def test_an_unmeasured_clock_still_cannot_admit_a_record_before_its_receipt() -> None:
    received = dt.datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
    observed = Clock.captured(None, received, monotonic_ns=7, uncertainty_seconds=0.0)

    before = features_asof([quote(received, observed)], received - dt.timedelta(seconds=1))
    assert before[MARKET_KEY]["valid"] is False
    assert before[MARKET_KEY]["reason"] == "not_yet_available"

    # An unknown synchronization is not a reason to withhold a record this
    # process has already received.
    after = features_asof([quote(received, observed)], received)
    assert after[MARKET_KEY]["valid"] is True
    assert after[MARKET_KEY]["bid"] == Decimal("0.4100")
    assert after[MARKET_KEY]["clock_quality"] == "unknown"

    # A stated bound widens the interval and never moves the instant at which the
    # record became usable, so the cutoff check is unchanged by it.
    bounded = Clock.captured(
        None,
        received,
        monotonic_ns=7,
        uncertainty_seconds=30.0,
        quality="clock_synced",
        basis="measured_offset_bound_30s",
    )
    still_early = features_asof([quote(received, bounded)], received - dt.timedelta(seconds=1))
    assert still_early[MARKET_KEY]["valid"] is False
    assert still_early[MARKET_KEY]["reason"] == "not_yet_available"
