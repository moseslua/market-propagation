"""The second venue's own metadata, held per contract and cited to its own bytes.

The predicate layer needs one thing the second venue's cleaned local archive does not
carry: what the venue says the contract pays on. These tests defend the properties
that make the held record usable as evidence rather than merely present.

**The record is the venue's own statement, not this module's reading of it.**
``question`` and ``description`` are asserted verbatim, including the escaped form a
description with a curly quote appears in on the wire, and every declared record field
is asserted present — a record missing one is a record no reader can trust to be the
shape it agreed on.

**The instant is the serving system's, never this run's.** Both directions are
asserted: a response carrying a ``Date`` header yields that instant, and a response
carrying none yields ``null``. The second is the test that matters, because falling
back to this run's clock is exactly the confusion that would make a record look dated
when nothing observed it.

**One route answers.** The canned pages are the search route's shape only. The
by-id routes were measured returning an empty list for a settled market the search
route returns in full, so a test that drove them would be asserting a behaviour this
module deliberately does not have.

Fixtures reach no network: the one acquisition path runs through
``httpx.MockTransport``, the same offline boundary this package's other tests use, and
bytes are archived through the real
:class:`~market_propagation.storage.RawStore`.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import pathlib
import sys
from typing import Any

import httpx
import pytest
import yaml

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from market_propagation.ingest.polymarket_markets import (
    MATCH_CONFIG_PATH,
    METADATA_BLOCK,
    REASON_NO_RECORD_HELD,
    RECORD_FIELDS,
    RECORD_SUBJECT_FIELDS,
    RECORD_VERSION,
    SKIP_REASONS,
    AcquisitionSummary,
    PolymarketMarketRecord,
    PolymarketMetadataStore,
    capture,
    load_metadata_acquisition_settings,
    text_as_stored,
)
from market_propagation.ingest.transport import HttpTransport, RetryPolicy

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

#: Two contracts on one event, as the venue's search route states them. The March
#: 2024 fifty-basis-point market is the real record whose ``conditionId`` matches a
#: ``condition_id`` in the cleaned local candidate layer; the sibling strike beside
#: it is the shape a second market in the same event takes.
MARCH_2024 = "0xd4a957e7b51fc2e74c4f1909583972011772eda01aada975251aeaffbbc73f56"
JANUARY_2025 = "0x1f0a4b0e6c0f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3"

FIFTY_BPS_QUESTION = "Will the Fed decrease interest rates by 50+ bps after its March 2024 meeting?"
TWENTY_FIVE_BPS_QUESTION = (
    "Will the Fed decrease interest rates by 25 bps after its 2024 March meeting?"
)

#: The venue's own resolution wording, quoted from the live record. It carries the
#: typographic quote characters that make the archived page carry the escaped form.
FIFTY_BPS_DESCRIPTION = (
    "The FED interest rates are defined in this market by the upper bound of the target "
    "federal funds range. The decisions on the target federal fund range are made by the "
    "Federal Open Market Committee (FOMC) meetings.\n\n"
    "This market will resolve to \u201cYes\u201d if following the Federal Reserve's March 2024 "
    "meeting the upper bound of the target federal funds rate is decreased by 50 or more "
    "basis points below the level it was prior to the meeting. Otherwise, it will resolve "
    "to \u201cNo\u201d."
)
TWENTY_FIVE_BPS_DESCRIPTION = (
    "The FED interest rates are defined in this market by the upper bound of the target "
    "federal funds range.\n\n"
    "This market will resolve to \u201cYes\u201d if following the Federal Reserve's March 2024 "
    "meeting the upper bound of the target federal funds rate is decreased by exactly 25 "
    "basis points below the level it was prior to the meeting."
)

EVENT_SLUG = "fed-interest-rates-march-2024"
EVENT_TITLE = "Fed Interest Rates: March 2024"

#: The instant the serving system stated it answered at, in the form its own ``Date``
#: header carries it. It is deliberately not this run's clock.
SERVED_AT = dt.datetime(2026, 9, 17, 6, 50, 58, tzinfo=dt.UTC)
SERVED_AT_HEADER = "Thu, 17 Sep 2026 06:50:58 GMT"

#: When a run doing this work today would have fetched, kept far from ``SERVED_AT``
#: so a record that took its instant from the run would be visibly wrong.
TODAY = dt.datetime(2026, 9, 17, 11, 0, tzinfo=dt.UTC)


def market(
    *,
    condition_id: str,
    question: str,
    description: str,
    slug: str,
    group_item_title: str | None = "50+ bps decrease",
    group_item_threshold: str | None = "1",
) -> dict[str, Any]:
    """One nested market as the venue's search route states it."""
    return {
        "conditionId": condition_id,
        "slug": slug,
        "question": question,
        "description": description,
        "groupItemTitle": group_item_title,
        "groupItemThreshold": group_item_threshold,
        "outcomes": '["Yes", "No"]',
        "endDate": "2024-03-18T00:00:00Z",
        "closedTime": "2024-03-20 21:14:39+00",
        "closed": True,
    }


def page_body(
    *,
    markets: list[dict[str, Any]] | None = None,
    event_slug: str = EVENT_SLUG,
    event_title: str = EVENT_TITLE,
    has_more: bool = False,
    total: int = 4,
) -> dict[str, Any]:
    """One search response, with the two declared contracts under one event."""
    if markets is None:
        markets = [
            market(
                condition_id=MARCH_2024,
                question=FIFTY_BPS_QUESTION,
                description=FIFTY_BPS_DESCRIPTION,
                slug="will-the-fed-decrease-interest-rates-by-50-bps-after-its-march-2024-meeting",
                group_item_title="50+ bps decrease",
                group_item_threshold="1",
            ),
            market(
                condition_id=JANUARY_2025,
                question=TWENTY_FIVE_BPS_QUESTION,
                description=TWENTY_FIVE_BPS_DESCRIPTION,
                slug="will-the-fed-decrease-interest-rates-by-25-bps-after-its-2024-march-meeting",
                group_item_title="25 bps decrease",
                group_item_threshold="0",
            ),
        ]
    return {
        "events": [
            {
                "slug": event_slug,
                "title": event_title,
                "markets": markets,
            }
        ],
        "pagination": {"hasMore": has_more, "totalResults": total},
    }


def to_bytes(body: Any, *, ensure_ascii: bool = False) -> bytes:
    """The body as bytes, in the encoding convention the venue was measured sending.

    The live payload carries raw UTF-8 with its control characters escaped — a
    description's typographic quotes arrive as themselves and its newlines as ``\\n`` —
    so ``ensure_ascii`` is off by default and this fixture exercises that convention
    rather than a rounder one the venue does not use. The tests that need the other
    convention pass ``ensure_ascii=True`` on purpose.
    """
    return json.dumps(body, ensure_ascii=ensure_ascii).encode("utf-8")


def plan_at(root: pathlib.Path, **block_overrides: Any) -> tuple[pathlib.Path, Any]:
    """The committed acquisition plan, with a declared store root, written for a test.

    The real block is read from the committed configuration and re-declared into a
    temporary copy rather than restated in a fixture, so a change to the declared
    queries or the declared wire parameters is exercised here and not duplicated. The
    temporary copy is what the module's own loader reads, which keeps the test on the
    real boundary rather than around it.
    """
    payload = yaml.safe_load((REPO_ROOT / MATCH_CONFIG_PATH).read_text(encoding="utf-8"))
    venue = next(entry for entry in payload["venues"] if entry["id"] == "polymarket")
    block = dict(venue[METADATA_BLOCK])
    block["store_root"] = str(root / "polymarket-metadata")
    block.update(block_overrides)
    venue[METADATA_BLOCK] = block
    path = root / "matching_v1.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path, load_metadata_acquisition_settings(path)


def first_query_only() -> list[dict[str, str]]:
    """The committed query list reduced to its first entry, reason kept.

    Read from the committed configuration rather than restated, so the query the
    sweep is bounded to here is the query the plan declares.
    """
    committed = load_metadata_acquisition_settings(REPO_ROOT / MATCH_CONFIG_PATH)
    return [dict(committed.queries[0].as_dict())]


def plan_and_store(
    root: pathlib.Path, **block_overrides: Any
) -> tuple[pathlib.Path, Any, PolymarketMetadataStore]:
    """A written plan, its parsed settings and the store it declares."""
    path, settings = plan_at(root, **block_overrides)
    return path, settings, PolymarketMetadataStore(settings.store_root, settings=settings)


def sweep(path: pathlib.Path, store: PolymarketMetadataStore, handler: Any) -> AcquisitionSummary:
    """Run the acquisition over the store's archive against a canned handler."""
    with serve(store, handler) as transport:
        return capture(store.root, config_path=path, transport=transport)


def serve(
    store: PolymarketMetadataStore,
    handler: Any,
    **policy: Any,
) -> HttpTransport:
    """A transport over the store's own archive, driven by a canned handler."""
    options = {"min_interval_seconds": 0.0, "retry_statuses": frozenset()}
    options.update(policy)
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return HttpTransport(store.raw_store, client=client, policy=RetryPolicy(**options))


def one_page(store: PolymarketMetadataStore, body: Any, *, date: str | None = SERVED_AT_HEADER):
    """A transport that answers every request with one canned page."""
    headers = {"content-type": "application/json"}
    if date is not None:
        headers["date"] = date

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=to_bytes(body), headers=headers)

    return serve(store, handler)


def run_one_page(
    root: pathlib.Path,
    body: Any,
    *,
    date: str | None = SERVED_AT_HEADER,
    one_query: bool = True,
    **block_overrides: Any,
) -> tuple[PolymarketMetadataStore, AcquisitionSummary]:
    """Sweep the declared plan against one canned page and return the store and summary.

    The sweep runs one declared query by default, because the counts a test asserts
    are about what one page holds; ``test_the_sweep_reads_every_declared_query`` runs
    the whole declared list on purpose.
    """
    overrides = dict(block_overrides)
    if one_query:
        overrides.setdefault("queries", first_query_only())
    path, settings = plan_at(root, **overrides)
    store = PolymarketMetadataStore(settings.store_root, settings=settings)
    with one_page(store, body, date=date) as transport:
        summary = capture(settings.store_root, config_path=path, transport=transport)
    return store, summary


# ---------------------------------------------------------------------------
# The declared plan.
# ---------------------------------------------------------------------------


def test_the_committed_block_is_the_plan_this_module_reads() -> None:
    """The block, not a fixture, declares the host, the route, the bounds and the queries."""
    settings = load_metadata_acquisition_settings(REPO_ROOT / MATCH_CONFIG_PATH)

    assert settings.venue == "polymarket"
    assert settings.host == "https://gamma-api.polymarket.com"
    assert settings.search_path == "public-search"
    assert settings.search_url == "https://gamma-api.polymarket.com/public-search"
    assert settings.record_version == RECORD_VERSION
    assert settings.writes_captured_data_into_the_repository is False
    assert settings.max_records_per_run >= 1
    assert settings.page_size >= 1
    assert settings.max_pages_per_query >= 1
    assert settings.note.strip()

    payload = yaml.safe_load((REPO_ROOT / MATCH_CONFIG_PATH).read_text(encoding="utf-8"))
    venue = next(entry for entry in payload["venues"] if entry["id"] == "polymarket")
    declared = venue[METADATA_BLOCK]
    # The declared query list is the universe, so it is read and not restated: a run
    # cannot sweep a family the configuration does not name.
    assert settings.declared_queries == tuple(item["query"] for item in declared["queries"])
    assert all(item["why"].strip() for item in declared["queries"])
    assert "public-search" in declared["route_note"]
    assert declared["host"] == settings.host


def test_the_declared_block_is_additive_to_the_venue_entry() -> None:
    """The acquisition block adds keys beside the payout declaration and rescopes none of it.

    Acquiring the venue's metadata is how the payout declaration gets text to read; it
    is not a way to change what that declaration says. Both halves are pinned here: the
    entry still carries the keys a reader of the payout declaration reads, and the
    block sits beside them rather than replacing or rescoping one — the entry's keys
    with the block removed are exactly its keys as declared, less the block.

    The declaration itself is now the second venue's own grammar over the venue's two
    market-text columns. ``market_slug`` is deliberately not one of them: a slug is a
    name, the venue writes a name for a market whose rule pays on "50 *or more*", and
    this layer refuses to read a predicate from a name.
    """
    payload = yaml.safe_load((REPO_ROOT / MATCH_CONFIG_PATH).read_text(encoding="utf-8"))
    venue = next(entry for entry in payload["venues"] if entry["id"] == "polymarket")

    # The entry's own keys, read from the entry rather than restated, so a key dropped
    # while the block is present is caught here as well as a key the block introduces.
    without_block = {key: value for key, value in venue.items() if key != METADATA_BLOCK}
    assert set(without_block) <= set(venue)
    assert set(venue) - set(without_block) == {METADATA_BLOCK}
    assert {
        "id",
        "label",
        "payout_text_fields",
        "parser",
        "documentation_verified",
        "remote_status",
    } <= set(without_block)

    assert venue["parser"] == "declared_second_venue_market_text"
    assert venue["payout_text_fields"] == ["question", "description"]
    assert "market_slug" not in venue["payout_text_fields"]
    assert venue["documentation_verified"] is True
    assert venue["remote_status"] == "reachable_without_credentials"

    declared = venue[METADATA_BLOCK]
    assert declared["writes_captured_data_into_the_repository"] is False
    # The flag is a plan the declared path carries out: the store is acquired data
    # beside the sources rather than among them, so the path is relative to the
    # repository and resolves outside its source tree.
    store_root = pathlib.Path(declared["store_root"])
    assert not store_root.is_absolute()
    resolved = (REPO_ROOT / store_root).resolve()
    assert resolved.is_relative_to(REPO_ROOT)
    assert not resolved.is_relative_to(REPO_ROOT / "src")


def test_a_venue_entry_without_the_block_is_refused(tmp_path: pathlib.Path) -> None:
    """A configuration that declares no acquisition is refused, not defaulted."""
    payload = yaml.safe_load((REPO_ROOT / MATCH_CONFIG_PATH).read_text(encoding="utf-8"))
    for entry in payload["venues"]:
        entry.pop(METADATA_BLOCK, None)
    path = tmp_path / "matching_v1.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="carries no metadata_acquisition block"):
        load_metadata_acquisition_settings(path)


# ---------------------------------------------------------------------------
# The held record.
# ---------------------------------------------------------------------------


def test_a_page_holds_one_record_per_market_keyed_by_its_condition_id(
    tmp_path: pathlib.Path,
) -> None:
    store, summary = run_one_page(tmp_path, page_body())

    assert summary.markets_sighted == 2
    assert summary.markets_held == 2
    assert summary.distinct_condition_ids == 2
    assert store.condition_ids() == (JANUARY_2025, MARCH_2024)
    assert store.market_path(MARCH_2024).name == f"{MARCH_2024}.json"

    held = store.held(MARCH_2024)
    assert held.held is True
    assert held.reason is None
    assert held.record is not None
    assert held.record.question == FIFTY_BPS_QUESTION
    assert held.record.event_slug == EVENT_SLUG
    assert held.record.event_title == EVENT_TITLE
    assert held.record.slug == (
        "will-the-fed-decrease-interest-rates-by-50-bps-after-its-march-2024-meeting"
    )


def test_the_record_carries_every_declared_field_and_no_other(tmp_path: pathlib.Path) -> None:
    """The record contract is one list, asserted on the file and on the object."""
    store, _ = run_one_page(tmp_path, page_body())
    document = json.loads(store.market_path(MARCH_2024).read_text(encoding="utf-8"))

    assert sorted(document) == sorted(RECORD_FIELDS)
    assert document["record_version"] == RECORD_VERSION
    # A field the venue states no value for is null rather than an empty string, so an
    # absence stays an absence through the round trip.
    assert document["group_item_threshold"] == "1"
    assert document["outcomes"] == '["Yes", "No"]'
    assert document["end_date"] == "2024-03-18T00:00:00Z"
    assert document["closed_time"] == "2024-03-20 21:14:39+00"

    blank = PolymarketMarketRecord(
        **{
            **document,
            "group_item_title": None,
            "group_item_threshold": None,
            "outcomes": None,
            "end_date": None,
            "closed_time": None,
            "source_observed_at": None,
        }
    )
    assert blank.as_dict()["group_item_title"] is None
    assert blank.as_dict()["source_observed_at"] is None
    assert set(RECORD_SUBJECT_FIELDS) < set(RECORD_FIELDS)


def test_the_description_is_held_verbatim_including_the_form_the_page_carries(
    tmp_path: pathlib.Path,
) -> None:
    """The record states the venue's text, and the archived page is checked for it."""
    store, _ = run_one_page(tmp_path, page_body())
    record = store.held(MARCH_2024).record
    assert record is not None

    assert record.question == FIFTY_BPS_QUESTION
    assert "decreased by 50 or more basis points" in record.description
    assert "\u201cYes\u201d" in record.description
    # The record's text is what ``verify`` checks, and the page carries the venue's own
    # convention: control characters escaped, non-ASCII raw.
    page = store.raw_store.get(record.raw_hash).decode("utf-8")
    assert json.dumps(record.description, ensure_ascii=False)[1:-1] in page
    store.verify(record)

    # A payload whose encoder escapes non-ASCII as well is the third accepted form;
    # both are exact forms of the same text and neither is a near-match test.
    escaped_page = json.dumps(record.as_dict()["description"])[1:-1]
    assert text_as_stored(escaped_page, record.description) == escaped_page
    assert (
        text_as_stored(page, record.description)
        == json.dumps(record.description, ensure_ascii=False)[1:-1]
    )
    with pytest.raises(ValueError, match="does not occur in the page it cites"):
        text_as_stored("a page carrying something else entirely", record.description)


def test_the_observed_instant_is_the_instant_the_serving_system_states(
    tmp_path: pathlib.Path,
) -> None:
    store, summary = run_one_page(tmp_path, page_body())

    assert summary.markets_held_with_a_stated_instant == 2
    for condition_id in (MARCH_2024, JANUARY_2025):
        record = store.held(condition_id).record
        assert record is not None
        assert record.source_observed_at == SERVED_AT
        assert record.states_an_instant is True
        assert record.source_observed_at != TODAY


def test_a_response_that_states_no_instant_leaves_it_null(tmp_path: pathlib.Path) -> None:
    """This run's clock is never substituted for an instant nobody observed."""
    store, summary = run_one_page(tmp_path, page_body(), date=None)

    assert summary.markets_held == 2
    assert summary.markets_held_with_a_stated_instant == 0
    document = json.loads(store.market_path(MARCH_2024).read_text(encoding="utf-8"))
    assert document["source_observed_at"] is None
    record = store.held(MARCH_2024).record
    assert record is not None
    assert record.states_an_instant is False
    # The instant this run fetched is nowhere in the record: the fetch happened, and
    # the record states only what the page stated.
    assert TODAY.isoformat() not in json.dumps(document)


def test_the_archived_page_is_retrievable_by_the_hash_the_record_cites(
    tmp_path: pathlib.Path,
) -> None:
    store, _ = run_one_page(tmp_path, page_body())
    record = store.held(MARCH_2024).record
    assert record is not None

    body = store.raw_store.get(record.raw_hash)
    assert json.loads(body) == page_body()
    assert record.raw_hash == __import__("hashlib").sha256(body).hexdigest()
    # One page, archived once, cited by both records on it.
    other = store.held(JANUARY_2025).record
    assert other is not None
    assert other.raw_hash == record.raw_hash
    assert len(store.raw_store.receipts(raw_hash=record.raw_hash)) == 1
    store.verify(record)


def test_a_re_sighting_is_one_record_and_a_restatement_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """Identical pages write nothing new; a page that restates the contract is refused."""
    store, first = run_one_page(tmp_path, page_body())
    assert first.markets_held == 2
    before = store.market_path(MARCH_2024).read_bytes()
    held_before = store.held(MARCH_2024).record
    assert held_before is not None

    _, second = run_one_page(tmp_path, page_body())

    assert second.markets_held == 2
    assert second.skipped == ()
    assert store.condition_ids() == (JANUARY_2025, MARCH_2024)
    held_after = store.held(MARCH_2024).record
    assert held_after is not None
    # The same statement is one record: same citation, same file, same object.
    assert held_after == held_before
    assert store.market_path(MARCH_2024).read_bytes() == before

    # A page that states the same contract's fields differently is a restatement, and
    # it is refused rather than replacing the record of what was first seen.
    restated = page_body(event_slug="fed-interest-rates-march-2024-renamed")
    with pytest.raises(FileExistsError, match="already held"):
        run_one_page(tmp_path, restated)
    assert store.market_path(MARCH_2024).read_bytes() == before


def test_a_page_that_restates_the_contracts_own_fields_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    store, _ = run_one_page(tmp_path, page_body())
    restated = page_body(
        markets=[
            market(
                condition_id=MARCH_2024,
                question=FIFTY_BPS_QUESTION.replace("50+", "25"),
                description=FIFTY_BPS_DESCRIPTION,
                slug="will-the-fed-decrease-interest-rates-by-50-bps-after-its-march-2024-meeting",
            )
        ]
    )
    with pytest.raises(FileExistsError, match="already held"):
        run_one_page(tmp_path, restated)
    assert store.market_path(MARCH_2024).exists()


def test_the_accessor_names_the_reason_a_contract_is_not_held(tmp_path: pathlib.Path) -> None:
    store, _ = run_one_page(tmp_path, page_body())

    missing = store.held("0x" + "ab" * 32)
    assert missing.held is False
    assert missing.record is None
    assert missing.reason == REASON_NO_RECORD_HELD
    assert missing.as_dict()["reason"] == "the_venue_metadata_holds_no_record_for_this_contract"

    # An identifier that is not a contract key is a different fact and is refused
    # rather than answered with the not-held reason.
    with pytest.raises(ValueError, match="lowercase 32-byte hex"):
        store.held("not-a-condition-id")
    with pytest.raises(ValueError, match="lowercase 32-byte hex"):
        store.market_path("../../etc/passwd")


def test_a_held_record_whose_page_no_longer_carries_its_text_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """``verify`` re-reads the page, so a record cannot cite bytes that disagree."""
    store, _ = run_one_page(tmp_path, page_body())
    record = store.held(MARCH_2024).record
    assert record is not None

    tampered = dataclasses.replace(record, question="Will the Fed do something else?")
    with pytest.raises(ValueError, match="does not occur in the page it cites"):
        store.verify(tampered)
    with pytest.raises(FileNotFoundError):
        store.verify(dataclasses.replace(record, raw_hash="0" * 64))


# ---------------------------------------------------------------------------
# The sweep.
# ---------------------------------------------------------------------------


def test_the_sweep_reads_every_declared_query_and_counts_sightings_and_holds(
    tmp_path: pathlib.Path,
) -> None:
    """One page per query: sightings count repeats, holds count contracts."""
    store, summary = run_one_page(tmp_path, page_body(), one_query=False)
    settings = store.settings

    assert summary.requests_made == len(settings.queries)
    assert summary.pages_archived == len(settings.queries)
    assert summary.queries == settings.declared_queries
    assert summary.markets_sighted == 2 * len(settings.queries)
    assert summary.markets_held == 2
    assert summary.distinct_condition_ids == 2
    assert summary.blocked == ()
    assert summary.page_bounded_queries == ()
    assert summary.records_bound_reached is False
    # One statement sighted under every query is one held record, not one per sighting.
    assert store.condition_ids() == (JANUARY_2025, MARCH_2024)
    assert json.dumps(summary.as_dict())


def test_the_sweep_follows_the_pages_the_venue_offers(tmp_path: pathlib.Path) -> None:
    """A page that states more follow is followed, and the second page is held too."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        seen.append(f"{request.url.params['q']}:{page}")
        body = (
            page_body(markets=page_body()["events"][0]["markets"][:1], has_more=True)
            if page == 1
            else page_body(markets=page_body()["events"][0]["markets"][1:])
        )
        return httpx.Response(
            200,
            content=to_bytes(body),
            headers={"content-type": "application/json", "date": SERVED_AT_HEADER},
        )

    path, _, store = plan_and_store(tmp_path, queries=first_query_only())
    summary = sweep(path, store, handler)

    assert seen == [f"{first_query_only()[0]['query']}:1", f"{first_query_only()[0]['query']}:2"]
    assert summary.requests_made == 2
    assert summary.pages_archived == 2
    assert summary.distinct_condition_ids == 2
    assert summary.markets_held == 2
    assert summary.skipped == ()
    assert store.held(JANUARY_2025).held is True
    assert store.held(MARCH_2024).held is True
    # Each page is its own archived payload, cited by the records read from it.
    page_one = store.held(MARCH_2024).record
    page_two = store.held(JANUARY_2025).record
    assert page_one is not None
    assert page_two is not None
    assert page_one.raw_hash != page_two.raw_hash
    assert len(store.raw_store.receipts()) == 2


def test_a_page_that_refuses_access_is_a_blocked_query_not_an_empty_one(
    tmp_path: pathlib.Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"unavailable")

    path, _, store = plan_and_store(tmp_path, queries=first_query_only())
    summary = sweep(path, store, handler)

    assert summary.markets_held == 0
    assert summary.distinct_condition_ids == 0
    assert len(summary.blocked) == 1
    blocked = summary.blocked[0]
    assert blocked["query"] == store.settings.declared_queries[0]
    assert blocked["reason"] == "http_status"
    assert blocked["status_code"] == 503
    assert summary.blocked_queries == (store.settings.declared_queries[0],)
    # The failing body is archived before the raise, so the page is still evidence.
    assert blocked["payload_hash"] is not None
    assert store.raw_store.get(blocked["payload_hash"]) == b"unavailable"


def test_a_market_the_venue_states_no_text_for_is_skipped_by_name(
    tmp_path: pathlib.Path,
) -> None:
    """An unreadable market is a recorded null with its own reason, never a dropped row."""
    body = page_body(
        markets=[
            market(
                condition_id=MARCH_2024,
                question=FIFTY_BPS_QUESTION,
                description=FIFTY_BPS_DESCRIPTION,
                slug="will-the-fed-decrease-interest-rates-by-50-bps-after-its-march-2024-meeting",
            ),
            {
                **market(
                    condition_id=JANUARY_2025,
                    question=TWENTY_FIVE_BPS_QUESTION,
                    description=TWENTY_FIVE_BPS_DESCRIPTION,
                    slug="sibling",
                ),
                "description": "",
            },
            {"conditionId": "not-hex", "slug": "x", "question": "q", "description": "d"},
            {"slug": "no-key", "question": "q", "description": "d"},
            {
                **market(
                    condition_id="0x" + "cd" * 32,
                    question="q",
                    description="d",
                    slug="outcomes-as-list",
                ),
                "outcomes": ["Yes", "No"],
            },
        ]
    )
    store, summary = run_one_page(tmp_path, body)

    assert summary.markets_held == 1
    assert summary.markets_sighted == 1
    reasons = [(item["condition_id"], item["reason"]) for item in summary.skipped]
    assert (JANUARY_2025, "the_venue_metadata_states_no_description_for_this_contract") in reasons
    assert (None, "the_venue_metadata_states_a_condition_id_that_is_not_a_market_key") in reasons
    assert (None, "the_venue_metadata_names_no_condition_id_for_this_market") in reasons
    assert (
        "0x" + "cd" * 32,
        "the_venue_metadata_states_its_outcomes_in_a_form_this_record_does_not_carry",
    ) in reasons
    assert all(reason in SKIP_REASONS for _, reason in reasons)
    assert store.held(JANUARY_2025).reason == REASON_NO_RECORD_HELD


def test_a_wire_shape_change_is_refused_rather_than_read_as_no_markets(
    tmp_path: pathlib.Path,
) -> None:
    """A renamed field must raise, because a silent empty page reads as a venue with none."""
    shapes = (
        {"totalResults": 4, "hasMore": False},
        {"events": "not-a-list", "pagination": {"hasMore": False, "totalResults": 4}},
        {
            "events": [{"slug": "e", "title": "t"}],
            "pagination": {"hasMore": False, "totalResults": 4},
        },
        {"pagination": {"hasMore": True, "totalResults": 4}},
    )
    for index, body in enumerate(shapes):
        target = tmp_path / f"shape-{index}"
        target.mkdir()
        path, _, store = plan_and_store(target, queries=first_query_only())

        def handler(request: httpx.Request, body: Any = body) -> httpx.Response:
            return httpx.Response(
                200,
                content=to_bytes(body),
                headers={"content-type": "application/json", "date": SERVED_AT_HEADER},
            )

        with pytest.raises(ValueError) as excinfo:
            sweep(path, store, handler)
        detail = str(excinfo.value)
        assert "the wire shape changed and this module refuses" in detail
        if body.get("pagination", {}).get("hasMore"):
            assert "carries no 'events' list and states more results follow" in detail
        elif "events" not in body:
            assert "carries no 'events' list and states the undeclared keys" in detail
        else:
            assert "'events' that is not a list" in detail or "no 'markets' list" in detail
        # Nothing was held from a page this module could not read.
        assert store.condition_ids() == ()


def test_the_payload_the_venue_returns_past_the_end_of_a_result_set_ends_the_sweep(
    tmp_path: pathlib.Path,
) -> None:
    """Past the last page the venue returns only its pagination, stating no more results.

    That payload is an end of results rather than a shape change: the absence of an
    ``events`` list there is accompanied by ``hasMore: false``, which is the venue
    saying there are no results to list. It ends the sweep without inventing a market,
    and the pages read before it still stand.
    """
    pages: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        pages.append(page)
        body = page_body(has_more=True) if page == 1 else {"pagination": {"hasMore": False}}
        return httpx.Response(
            200,
            content=to_bytes(body),
            headers={"content-type": "application/json", "date": SERVED_AT_HEADER},
        )

    path, _, store = plan_and_store(tmp_path, queries=first_query_only())
    summary = sweep(path, store, handler)

    assert pages == [1, 2]
    assert summary.requests_made == 2
    assert summary.markets_held == 2
    assert summary.blocked == ()
    assert summary.skipped == ()


def test_the_record_bound_stops_the_sweep_and_says_so(tmp_path: pathlib.Path) -> None:
    """A bounded sweep is reported as bounded rather than as a complete universe."""
    store, summary = run_one_page(tmp_path, page_body(), max_records_per_run=1)

    assert summary.records_bound_reached is True
    assert summary.markets_held == 1
    assert summary.requests_made == 1
    assert summary.as_dict()["records_bound_reached"] is True
    assert summary.as_dict()["markets_held"] == 1
    assert store.condition_ids() == (MARCH_2024,)


def test_a_query_bounded_by_pages_is_named_in_the_summary(tmp_path: pathlib.Path) -> None:
    """A query still offering pages at the bound is named, so the sweep is not read as total."""
    seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(int(request.url.params["page"]))
        return httpx.Response(
            200,
            content=to_bytes(page_body(has_more=True)),
            headers={"content-type": "application/json", "date": SERVED_AT_HEADER},
        )

    path, settings, store = plan_and_store(
        tmp_path, queries=first_query_only(), max_pages_per_query=2
    )
    summary = sweep(path, store, handler)

    assert seen == [1, 2]
    assert summary.page_bounded_queries == (settings.declared_queries[0],)
    assert summary.blocked == ()
    assert summary.markets_held == 2
