"""Pagination identity accounting at the ingest boundary.

A duplicate is decided by identity, never by content. These tests pin three
outcomes apart:

* Two deliveries of one unique id are one event, and the repeat is counted
  against the boundary it actually crossed: ``dups_within_pages`` for a repeat
  inside a single page, ``repeated_ids_across_pages`` for one that arrived on an
  earlier page. The audit reports the second as a boundary-consistency
  observation, so the two must never be conflated.
* Two records with distinct ids are two events even when the rest of the payload
  is byte-identical. ``trade_id`` is the exchange's own occurrence id, and the
  observed feeds really do repeat time, price and size.
* A record with no usable id has no identity to repeat *under*, so content
  equality collapses nothing and both deliveries survive.

Every page body is archived under its own occurrence, so a dropped duplicate
delivery is still auditable, and the surviving records keep the order the pages
supplied.

No test here reaches the network: the HTTP boundary is ``httpx.MockTransport``.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx

from market_propagation.ingest.pagination import (
    PaginationResult,
    checkpoint_params,
    cursor_sets_consistent,
    paginate,
)
from market_propagation.ingest.transport import HttpTransport, RetryPolicy
from market_propagation.storage import RawStore

TRADES_URL = "https://external-api.kalshi.com/trade-api/v2/historical/trades"
TRADES_SOURCE = "kalshi.historical.trades"

#: The receipt instant is fixed so a test never depends on the wall clock.
FIXED_NOW = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC)


def trade_identity(item: Any) -> Any:
    """The identity path the real historical-trades client uses."""
    return item.get("trade_id") if isinstance(item, Mapping) else None


class ReplayedPages:
    """Serves queued page bodies in request order and records every URL."""

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


def page_body(
    items: list[dict[str, Any]], cursor: str | None, *, cursor_key: str = "cursor"
) -> bytes:
    return json.dumps({"trades": items, cursor_key: cursor}).encode("utf-8")


def trade(trade_id: str) -> dict[str, Any]:
    """One historical trade, in the wire shape the live endpoint returned."""
    return {
        "trade_id": trade_id,
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


def run(
    store: RawStore,
    bodies: list[bytes],
    *,
    identity_keys: Any = (trade_identity,),
    **kwargs: Any,
) -> PaginationResult:
    handler = ReplayedPages(bodies)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        transport = HttpTransport(
            store,
            client=client,
            policy=RetryPolicy(min_interval_seconds=0.0),
            sleep=lambda _seconds: None,
            now=lambda: FIXED_NOW,
        )
        result = paginate(
            transport,
            TRADES_URL,
            items_key="trades",
            source=TRADES_SOURCE,
            identity_keys=identity_keys,
            record_prefix="hist-trades",
            **kwargs,
        )
    return result


def test_a_unique_id_repeated_within_one_page_is_one_event(tmp_path: Path) -> None:
    """A retransmission of one id inside a page is one event, not a new trade."""
    store = RawStore(tmp_path / "raw")
    result = run(store, [page_body([trade("t-1"), trade("t-1")], None)])

    assert [t["trade_id"] for t in result.items] == ["t-1"]
    assert result.dups_within_pages == 1, "the repeat happened inside one page"
    assert result.repeated_ids_across_pages == 0, (
        "no page boundary was crossed, so nothing may be reported as an across-page repeat"
    )
    assert result.dups_total == 1
    assert result.distinct_ids_across_pages == 1
    assert result.page_counts == (2,), "the page really did carry two records"
    assert result.complete is True

    payload = result.as_dict()
    assert payload["dups_within_pages"] == 1
    assert payload["repeated_ids_across_pages"] == 0
    assert payload["dups_total"] == 1
    assert payload["item_count"] == 1


def test_a_unique_id_repeated_on_a_later_page_is_across_page_accounting(
    tmp_path: Path,
) -> None:
    """The same id delivered again on page two is what the audit metric names."""
    store = RawStore(tmp_path / "raw")
    result = run(
        store,
        [
            page_body([trade("t-1")], "CUR1"),
            page_body([trade("t-1")], None),
        ],
    )

    assert [t["trade_id"] for t in result.items] == ["t-1"]
    assert result.repeated_ids_across_pages == 1
    assert result.dups_within_pages == 0
    assert result.dups_total == 1
    assert result.page_counts == (1, 1)
    assert cursor_sets_consistent(result) is True
    # Retained plus dropped accounts for exactly what the pages carried, which is
    # the check the page counts exist to support.
    assert len(result.items) + result.dups_total == sum(result.page_counts)


def test_a_bound_cutting_inside_a_page_accounts_for_the_records_it_excluded(
    tmp_path: Path,
) -> None:
    """A cap must not leave page sizes unaccounted for against the kept records.

    The pages are counted as the venue served them, before the cap is applied, so
    a cap that cuts mid-page leaves the accounting short unless the excluded
    records are named. Reported as a gap, a caller reconciling the two would read
    it as records that vanished.
    """
    store = RawStore(tmp_path / "raw")
    result = run(
        store,
        [
            page_body([trade("t-1"), trade("t-2")], "CUR1"),
            page_body([trade("t-3"), trade("t-4")], None),
        ],
        max_items=3,
    )

    assert [t["trade_id"] for t in result.items] == ["t-1", "t-2", "t-3"]
    assert result.page_counts == (2, 2), "the second page really did carry two records"
    assert result.dups_total == 0, "nothing here was a repeat"
    assert result.excluded_by_max_items == 1, "t-4 is the record the cap kept out"
    assert result.stop_reason == "max_items"
    # Retained, dropped as duplicates, and excluded by the cap together account for
    # every record the pages carried.
    assert len(result.items) + result.dups_total + result.excluded_by_max_items == sum(
        result.page_counts
    )
    assert result.as_dict()["excluded_by_max_items"] == 1


def test_an_unbinding_cap_excludes_nothing(tmp_path: Path) -> None:
    """A cap larger than the result set must not report an exclusion."""
    store = RawStore(tmp_path / "raw")
    result = run(store, [page_body([trade("t-1"), trade("t-2")], None)], max_items=10)

    assert result.excluded_by_max_items == 0
    assert result.page_counts == (2,)
    assert len(result.items) + result.dups_total + result.excluded_by_max_items == sum(
        result.page_counts
    )


def test_a_resume_request_names_the_cursor_the_way_the_walk_was_parameterized(
    tmp_path: Path,
) -> None:
    """The rebuilt cursor parameter must match the key ``paginate`` was given.

    The cursor's member name is a parameter of the walk, so writing it back under a
    fixed ``cursor`` key would hand a differently-spelled feed a parameter it
    ignores: the resumed walk would restart at page one, which is the failure a
    checkpoint exists to prevent.
    """
    store = RawStore(tmp_path / "raw")
    result = run(
        store,
        [
            page_body([trade("t-1")], "CUR1", cursor_key="next_cursor"),
            page_body([trade("t-2")], None, cursor_key="next_cursor"),
        ],
        cursor_key="next_cursor",
        max_pages=1,
    )

    assert result.complete is False
    assert result.checkpoint.cursor == "CUR1"
    assert result.checkpoint.cursor_key == "next_cursor", (
        "the checkpoint records the member name the walk used"
    )

    default = checkpoint_params(result.checkpoint)
    assert default["next_cursor"] == "CUR1"
    assert "cursor" not in default, "the walk never named its cursor parameter 'cursor'"
    assert result.as_dict()["checkpoint"]["cursor_key"] == "next_cursor"


def test_a_completed_walk_rebuilds_no_cursor_parameter(tmp_path: Path) -> None:
    """A finished walk has no cursor to continue from, so none is invented."""
    store = RawStore(tmp_path / "raw")
    result = run(store, [page_body([trade("t-1")], None)])

    assert result.complete is True
    params = checkpoint_params(result.checkpoint)
    assert "cursor" not in params


def test_distinct_ids_with_identical_payloads_both_survive(tmp_path: Path) -> None:
    """Time, price and size may repeat; the id is what separates two trades."""
    store = RawStore(tmp_path / "raw")
    twins = [trade("t-1"), trade("t-2")]
    result = run(store, [page_body(twins, None)])

    assert [t["trade_id"] for t in result.items] == ["t-1", "t-2"]
    assert result.dups_total == 0
    assert result.dups_within_pages == 0
    assert result.repeated_ids_across_pages == 0
    assert result.distinct_ids_across_pages == 2
    # Retention is the delivered records themselves, never a synthesized merge.
    assert list(result.items) == twins


def test_records_without_a_usable_id_are_not_collapsed_by_content(
    tmp_path: Path,
) -> None:
    """Without an id there is no identity to repeat under, so both deliveries stand."""
    store = RawStore(tmp_path / "raw")
    unidentified = [
        {key: value for key, value in trade("t-1").items() if key != "trade_id"},
        {key: value for key, value in trade("t-1").items() if key != "trade_id"},
    ]
    assert unidentified[0] == unidentified[1], "the fixture must be byte-identical"

    result = run(store, [page_body(unidentified, None)])

    assert list(result.items) == unidentified
    assert result.dups_total == 0
    assert result.dups_within_pages == 0
    assert result.repeated_ids_across_pages == 0
    assert result.distinct_ids_across_pages == 0, "no record carried an id"


def test_keyless_listing_keeps_identical_records_apart(tmp_path: Path) -> None:
    """A listing with no identity key collapses nothing, by content or otherwise."""
    store = RawStore(tmp_path / "raw")
    record = {"ticker": "KXCPI-26JUN-T-0.3", "status": "finalized"}
    result = run(store, [page_body([dict(record), dict(record)], None)], identity_keys=())

    assert list(result.items) == [record, record]
    assert result.dups_total == 0
    assert result.distinct_ids_across_pages == 0


def test_a_conflicting_repeat_never_becomes_a_fabricated_record(
    tmp_path: Path,
) -> None:
    """A same-id delivery with different fields is dropped, never merged."""
    store = RawStore(tmp_path / "raw")
    first = trade("t-1")
    conflicting = {**first, "yes_price_dollars": "0.0100"}
    assert conflicting != first

    result = run(store, [page_body([first, conflicting], None)])

    assert list(result.items) == [first], (
        "the retained record is exactly the first delivery; a field-wise merge "
        "would invent a trade the venue never sent"
    )
    assert result.dups_within_pages == 1


def test_page_boundary_counts_stay_consistent_across_pages(tmp_path: Path) -> None:
    """Four distinct ids over two full pages: no repeat, no boundary anomaly."""
    store = RawStore(tmp_path / "raw")
    result = run(
        store,
        [
            page_body([trade("t-1"), trade("t-2")], "CUR1"),
            page_body([trade("t-3"), trade("t-4")], None),
        ],
        max_pages=5,
    )

    assert result.page_counts == (2, 2)
    assert cursor_sets_consistent(result) is True
    assert [t["trade_id"] for t in result.items] == ["t-1", "t-2", "t-3", "t-4"]
    assert result.distinct_ids_across_pages == 4
    assert result.repeated_ids_across_pages == 0
    assert result.dups_within_pages == 0
    assert result.dups_total == 0
    assert result.complete is True


def test_every_page_body_is_archived_even_when_its_records_are_duplicates(
    tmp_path: Path,
) -> None:
    """A dropped duplicate is still evidence: its raw body survives in the store."""
    store = RawStore(tmp_path / "raw")
    bodies = [
        page_body([trade("t-1")], "CUR1"),
        page_body([trade("t-1")], None),
    ]
    result = run(store, bodies)

    assert len(result.pages) == 2
    assert result.raw_hashes == tuple(page.raw_hash for page in result.pages)
    for page in result.pages:
        assert store.get(page.raw_hash) == bodies[page.index]

    receipts = store.receipts()
    assert len(receipts) == 2, "one receipt per page, not one per retained record"
    assert {receipt["raw_hash"] for receipt in receipts} == set(result.raw_hashes)
