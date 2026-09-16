"""Occurrence-identity tests for the transport archival boundary.

``HttpTransport.get`` takes a ``record_id`` that is a *request label*: it names
the request or page the caller asked for ("cutoff", "hist-trades-00001"). It is
not an occurrence identity, because the same label recurs across genuine,
independent GETs whose bytes differ:

* ``/historical/cutoff`` advances, so the second GET of the same label returns
  different timestamps.
* A pagination run reissues page labels into the same store, so a later run's
  page one can differ from the earlier run's page one.

Keying the receipt on the label alone turns both into a
"same occurrence cannot have two different payloads" collision. These tests pin
the behaviour that fixes it: every received response is archived as its own
occurrence with a fresh identity, the label is retained in receipt metadata for
checkpoint auditing, and only content-addressed *blobs* deduplicate.

No test here reaches the network: the HTTP boundary is ``httpx.MockTransport``.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import httpx

from market_propagation.ingest.transport import HttpTransport, RetryPolicy
from market_propagation.storage import RawStore

CUTOFF_URL = "https://external-api.kalshi.com/trade-api/v2/historical/cutoff"
TRADES_URL = "https://external-api.kalshi.com/trade-api/v2/historical/trades"
CUTOFF_SOURCE = "kalshi.historical.cutoff"
TRADES_SOURCE = "kalshi.historical.trades"

#: Both occurrences in these tests share this clock. The fixed instant is only
#: the *receipt* time; it must not be what keeps two occurrences apart.
FIXED_NOW = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC)

CUTOFF_EARLY = json.dumps(
    {
        "market_positions_last_updated_ts": "2026-07-15T00:00:00Z",
        "market_settled_ts": "2026-06-01T00:00:00Z",
        "orders_updated_ts": "2026-07-15T00:00:00Z",
        "trades_created_ts": "2026-07-15T00:00:00Z",
    }
).encode("utf-8")

CUTOFF_LATER = json.dumps(
    {
        "market_positions_last_updated_ts": "2026-07-15T00:00:00Z",
        "market_settled_ts": "2026-07-20T00:00:00Z",
        "orders_updated_ts": "2026-07-15T00:00:00Z",
        "trades_created_ts": "2026-07-15T00:00:00Z",
    }
).encode("utf-8")


class RecordingHandler:
    """Replays queued bodies in order and records every request URL."""

    def __init__(self, bodies: list[bytes]) -> None:
        self.bodies = list(bodies)
        self.request_urls: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.request_urls.append(str(request.url))
        index = len(self.request_urls) - 1
        if index >= len(self.bodies):
            raise AssertionError(f"exhausted fixture responses for {request.url}")
        return httpx.Response(
            200,
            content=self.bodies[index],
            headers={"content-type": "application/json"},
        )


def test_advancing_cutoff_archives_each_response_as_its_own_occurrence(
    tmp_path: Path,
) -> None:
    """Two GETs of the same labeled request with changed bytes both archive."""
    store = RawStore(tmp_path / "raw")
    handler = RecordingHandler([CUTOFF_EARLY, CUTOFF_LATER])
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        transport = HttpTransport(
            store,
            client=client,
            policy=RetryPolicy(min_interval_seconds=0.0),
            sleep=lambda _seconds: None,
            now=lambda: FIXED_NOW,
        )
        first = transport.get(CUTOFF_URL, source=CUTOFF_SOURCE, record_id="cutoff")
        second = transport.get(CUTOFF_URL, source=CUTOFF_SOURCE, record_id="cutoff")

    assert first.body == CUTOFF_EARLY
    assert second.body == CUTOFF_LATER
    # Each envelope resolves to the exact bytes its own response carried.
    assert store.get(first.provenance.raw_hash) == CUTOFF_EARLY
    assert store.get(second.provenance.raw_hash) == CUTOFF_LATER
    assert first.provenance.raw_hash != second.provenance.raw_hash

    # Separate receipts, even though both responses were stamped with the same
    # receipt clock and carried the same request label.
    receipts = store.receipts()
    assert len(receipts) == 2
    assert {r["received_time"] for r in receipts} == {FIXED_NOW.isoformat()}
    assert {r["metadata"]["request_label"] for r in receipts} == {"cutoff"}
    assert {r["raw_hash"] for r in receipts} == {
        first.provenance.raw_hash,
        second.provenance.raw_hash,
    }
    assert first.provenance.record_id != second.provenance.record_id
    for occurrence in (first.provenance, second.provenance):
        # Looked up by occurrence identity, which is how a generated receipt is
        # keyed; the source-keyed path form exists only for source-supplied ids.
        receipt = store.receipt(occurrence.record_id)
        assert receipt is not None
        assert receipt["raw_hash"] == occurrence.raw_hash
        assert receipt["source"] == CUTOFF_SOURCE


def test_identical_bytes_and_equal_clock_are_still_distinct_occurrences(
    tmp_path: Path,
) -> None:
    """Equal bytes and an equal receipt clock do not collapse two GETs into one."""
    store = RawStore(tmp_path / "raw")
    handler = RecordingHandler([CUTOFF_EARLY, CUTOFF_EARLY])
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        transport = HttpTransport(
            store,
            client=client,
            policy=RetryPolicy(min_interval_seconds=0.0),
            sleep=lambda _seconds: None,
            now=lambda: FIXED_NOW,
        )
        first = transport.get(CUTOFF_URL, source=CUTOFF_SOURCE, record_id="cutoff")
        second = transport.get(CUTOFF_URL, source=CUTOFF_SOURCE, record_id="cutoff")

    # The blob deduplicates on content ...
    assert first.provenance.raw_hash == second.provenance.raw_hash
    assert store.stored_hashes() == [first.provenance.raw_hash]
    # ... while the occurrence does not.
    assert first.provenance.record_id != second.provenance.record_id
    assert len(store.receipts()) == 2
    assert len(store.receipts(raw_hash=first.provenance.raw_hash)) == 2
    assert store.get(first.provenance.raw_hash) == CUTOFF_EARLY
    assert store.get(second.provenance.raw_hash) == CUTOFF_EARLY


def test_reused_page_label_does_not_collide_with_a_changed_page_body(
    tmp_path: Path,
) -> None:
    """The pagination-repeat failure: a later run reissues a label it already used."""
    store = RawStore(tmp_path / "raw")
    first_run_page = json.dumps({"trades": [{"trade_id": "id-1"}], "cursor": "CUR1"}).encode(
        "utf-8"
    )
    second_run_page = json.dumps({"trades": [], "cursor": None}).encode("utf-8")
    handler = RecordingHandler([first_run_page, second_run_page])
    params = {"ticker": "KXCPI-26JUN-T-0.3", "limit": 2, "cursor": "CUR1"}

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        transport = HttpTransport(
            store,
            client=client,
            policy=RetryPolicy(min_interval_seconds=0.0),
            sleep=lambda _seconds: None,
            now=lambda: FIXED_NOW,
        )
        first_run = transport.get(
            TRADES_URL, params=params, source=TRADES_SOURCE, record_id="hist-trades-00001"
        )
        second_run = transport.get(
            TRADES_URL, params=params, source=TRADES_SOURCE, record_id="hist-trades-00001"
        )

    assert handler.request_urls[0] == handler.request_urls[1]
    assert store.get(first_run.provenance.raw_hash) == first_run_page
    assert store.get(second_run.provenance.raw_hash) == second_run_page
    assert first_run.provenance.record_id != second_run.provenance.record_id
    assert {receipt["metadata"]["request_label"] for receipt in store.receipts()} == {
        "hist-trades-00001"
    }


def test_request_label_is_not_an_occurrence_identity(tmp_path: Path) -> None:
    """The label audits the checkpoint; it never keys the receipt."""
    store = RawStore(tmp_path / "raw")
    handler = RecordingHandler([CUTOFF_EARLY])
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        transport = HttpTransport(
            store,
            client=client,
            policy=RetryPolicy(min_interval_seconds=0.0),
            sleep=lambda _seconds: None,
            now=lambda: FIXED_NOW,
        )
        envelope = transport.get(CUTOFF_URL, source=CUTOFF_SOURCE, record_id="cutoff")

    # "cutoff" is a request label, so no occurrence is keyed by it.
    assert store.receipt("cutoff") is None

    receipt = store.receipt(envelope.provenance.record_id)
    assert receipt is not None
    assert receipt["raw_hash"] == envelope.provenance.raw_hash
    assert receipt["metadata"]["request_label"] == "cutoff"
    assert receipt["metadata"]["url"] == CUTOFF_URL


def test_unlabeled_request_archives_without_a_request_label(tmp_path: Path) -> None:
    """A caller that names no request still archives the response."""
    store = RawStore(tmp_path / "raw")
    handler = RecordingHandler([CUTOFF_EARLY])
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        transport = HttpTransport(
            store,
            client=client,
            policy=RetryPolicy(min_interval_seconds=0.0),
            sleep=lambda _seconds: None,
            now=lambda: FIXED_NOW,
        )
        envelope = transport.get(CUTOFF_URL, source=CUTOFF_SOURCE)

    assert store.get(envelope.provenance.raw_hash) == CUTOFF_EARLY
    receipts = store.receipts()
    assert len(receipts) == 1
    assert "request_label" not in receipts[0]["metadata"]
