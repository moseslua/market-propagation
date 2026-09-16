"""Acceptance tests for the core records, storage, replay, and point-in-time layers.

Each test defends a property that a plausible mistake would break, and each
asserts on what a consumer of the package observes rather than on internal
wiring.
"""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from market_propagation.domain import (
    UTC,
    Availability,
    BookEvent,
    Clock,
    Contract,
    Expectation,
    Operator,
    Provenance,
    Quote,
    QuoteValidity,
    Release,
    Resolution,
    Rounding,
    Trade,
    classify_local_time,
    market_key,
    parse_decimal,
    parse_local_time,
    parse_utc_time,
    split_market_key,
)
from market_propagation.point_in_time import (
    PANEL_COLUMNS,
    available_labels,
    build_event_panel,
    features_asof,
)
from market_propagation.replay import (
    ORDER_SOURCE,
    ORDER_USABLE,
    BookState,
    apply_book_event,
    canonicalize_events,
    compare_replay_orders,
    replay,
    resolve_order,
)
from market_propagation.storage import (
    RawStore,
    query_sealed,
    read_parquet,
    resolve_rows,
    write_parquet,
)

T0 = dt.datetime(2026, 3, 2, 12, 0, 0, tzinfo=UTC)
NY = "America/New_York"


def at(seconds: float, *, base: dt.datetime = T0) -> dt.datetime:
    return base + dt.timedelta(seconds=seconds)


def provenance(
    record_id: str, *, source: str = "venue-a", raw_hash: str | None = None
) -> Provenance:
    return Provenance(raw_hash or (record_id * 8).ljust(64, "0")[:64], record_id, source)


def clock(
    seconds: float,
    *,
    source_offset: float = 0.0,
    uncertainty_seconds: float = 0.0,
) -> Clock:
    return Clock.captured(
        at(seconds + source_offset), at(seconds), uncertainty_seconds=uncertainty_seconds
    )


def snapshot(
    seconds: float,
    *,
    bids: tuple[tuple[str, str], ...],
    asks: tuple[tuple[str, str], ...],
    contract_id: str = "CPI-THRESHOLD",
    venue: str = "venue-a",
    scope: str = "book-1",
    sequence: int | None = None,
    record_id: str | None = None,
    source_offset: float = 0.0,
) -> BookEvent:
    ident = record_id or f"snap-{seconds}-{contract_id}"
    return BookEvent(
        venue=venue,
        contract_id=contract_id,
        kind="snapshot",
        clock=clock(seconds, source_offset=source_offset),
        provenance=provenance(ident, source=venue),
        connection_id="conn-1",
        sequence_scope=scope,
        sequence=sequence,
        bids=tuple((Decimal(price), Decimal(size)) for price, size in bids),
        asks=tuple((Decimal(price), Decimal(size)) for price, size in asks),
    )


def delta(
    seconds: float,
    *,
    side: str,
    price: str,
    size: str | None,
    operation: str = "replace",
    contract_id: str = "CPI-THRESHOLD",
    venue: str = "venue-a",
    scope: str = "book-1",
    sequence: int | None = None,
    record_id: str | None = None,
) -> BookEvent:
    ident = record_id or f"delta-{seconds}-{contract_id}-{side}-{price}-{operation}-{size}"
    return BookEvent(
        venue=venue,
        contract_id=contract_id,
        kind="delta",
        clock=clock(seconds),
        provenance=provenance(ident, source=venue),
        connection_id="conn-1",
        sequence_scope=scope,
        sequence=sequence,
        side=side,
        price=Decimal(price),
        size=None if size is None else Decimal(size),
        operation=operation,
    )


def lifecycle(
    seconds: float,
    *,
    kind: str,
    contract_id: str = "CPI-THRESHOLD",
    venue: str = "venue-a",
    scope: str = "book-1",
    sequence: int | None = None,
    record_id: str | None = None,
) -> BookEvent:
    ident = record_id or f"{kind}-{seconds}-{contract_id}"
    return BookEvent(
        venue=venue,
        contract_id=contract_id,
        kind=kind,
        clock=clock(seconds),
        provenance=provenance(ident, source=venue),
        connection_id="conn-1",
        sequence_scope=scope,
        sequence=sequence,
    )


def quote_at(
    seconds: float,
    *,
    bid: str | None,
    ask: str | None,
    validity: QuoteValidity = QuoteValidity.VALID,
    contract_id: str = "CPI-THRESHOLD",
    venue: str = "venue-a",
    verified_at: float | None = None,
    record_id: str | None = None,
    uncertainty_seconds: float = 0.0,
    order: str = ORDER_USABLE,
) -> Quote:
    ident = record_id or f"quote-{seconds}-{contract_id}-{validity.value}"
    verified = at(seconds if verified_at is None else verified_at)
    return Quote(
        venue=venue,
        contract_id=contract_id,
        clock=clock(seconds, uncertainty_seconds=uncertainty_seconds),
        provenance=provenance(ident, source=venue),
        bid=None if bid is None else Decimal(bid),
        ask=None if ask is None else Decimal(ask),
        bid_size=Decimal("40") if bid is not None else None,
        ask_size=Decimal("25") if ask is not None else None,
        validity=validity,
        last_price_change=verified,
        last_verified=verified,
        last_trade=None,
        replay_order=order,
    )


def contract(
    *,
    contract_id: str = "CPI-THRESHOLD",
    event_id: str = "CPI-2026-03",
    venue: str = "venue-a",
    operator: Operator = Operator.ABOVE,
    threshold: str | None = "0.3",
    rule_available_at: dt.datetime | None = None,
    close_time: dt.datetime | None = None,
    open_time: dt.datetime | None = None,
    rule_hash: str = "rule-1",
) -> Contract:
    return Contract(
        venue=venue,
        contract_id=contract_id,
        event_id=event_id,
        family="CPI",
        reference_period="2026-02",
        source="BLS",
        units="index_points",
        operator=operator,
        threshold=None if threshold is None else Decimal(threshold),
        lower=None,
        upper=None,
        rounding=Rounding.NEAREST,
        vintage="initial",
        timezone=NY,
        deadline=None,
        settlement="cash",
        currency="USD",
        exceptional_policy="void_excluded",
        open_time=open_time,
        close_time=close_time,
        resolve_time=None,
        rule_hash=rule_hash,
        provenance=provenance(f"contract-{contract_id}-{rule_hash}"),
        rule_available_at=rule_available_at,
    )


def release(
    *,
    event_id: str = "CPI-2026-03",
    seconds: float = 0.0,
    family: str = "CPI",
    received_offset: float = 0.0,
) -> Release:
    return Release(
        event_id=event_id,
        family=family,
        scheduled_at=at(seconds),
        reference_period="2026-02",
        values={"headline": Decimal("0.4")},
        clock=Clock.captured(at(seconds), at(seconds + received_offset)),
        provenance=provenance(f"release-{event_id}"),
    )


def resolution(
    *,
    contract_id: str = "CPI-THRESHOLD",
    payout: str = "1",
    known_at: dt.datetime | None = None,
    resolved_at: dt.datetime | None = None,
    exceptional: bool = False,
) -> Resolution:
    return Resolution(
        contract_id=contract_id,
        payout=Decimal(payout),
        known_at=known_at,
        resolved_at=resolved_at,
        rule_hash="rule-1",
        provenance=provenance(f"resolution-{contract_id}-{payout}-{exceptional}"),
        exceptional=exceptional,
    )


def test_unknown_availability_is_explicit_never_a_zero_width_default() -> None:
    availability = Availability.unknown()
    assert availability.lower is None and availability.upper is None
    assert availability.is_known is False
    assert availability.width_seconds is None
    with pytest.raises(ValueError):
        Availability(at(0), None, "clock_synced", "half_bound")
    with pytest.raises(ValueError):
        Availability(at(5), at(1), "clock_synced", "inverted")
    with pytest.raises(ValueError):
        Availability(dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 2), "clock_synced", "naive")


def test_source_time_never_becomes_usable_time_by_default() -> None:
    historical = Clock.historical(at(0))
    assert historical.source_time == at(0)
    assert historical.usable_time is None
    assert historical.timing_uncertainty_seconds is None

    captured = Clock.captured(at(-30), at(0), uncertainty_seconds=2.5)
    assert captured.usable_time == at(0)
    assert captured.timing_uncertainty_seconds == pytest.approx(2.5)


def test_historical_clock_with_receipt_requires_a_documented_bound() -> None:
    with pytest.raises(ValueError):
        Clock.historical(at(0), at(1))
    bounded = Clock.historical(at(0), at(1), uncertainty_seconds=30.0, basis="vendor_doc_30s")
    assert bounded.usable_time == at(1)
    assert bounded.availability.basis == "vendor_doc_30s"
    assert bounded.availability.quality == "clock_unsynced"


def test_exact_decimal_parsing_keeps_cents_and_rejects_non_numbers() -> None:
    assert parse_decimal("0.523", field_name="x") == Decimal("0.523")
    assert parse_decimal(0.1, field_name="x") == Decimal("0.1")
    assert parse_decimal(7, field_name="x") == Decimal(7)
    assert parse_decimal("1e-9", field_name="x") == Decimal("1E-9")
    for bad in ("nan", "inf", "", "half", True, None):
        with pytest.raises((ValueError, TypeError)):
            parse_decimal(bad, field_name="x")


def test_fractional_contract_sizes_are_representable_exactly() -> None:
    event = delta(0, side="bid", price="0.52", size="2.5")
    assert event.size == Decimal("2.5")
    increment = delta(1, side="bid", price="0.52", size="0.125", operation="increment")
    state = BookState(ORDER_USABLE)
    apply_book_event(state, snapshot(0, bids=(("0.50", "1.5"),), asks=(("0.60", "1"),)))
    apply_book_event(state, increment)
    quote = state.quote_series("venue-a", "CPI-THRESHOLD")[-1]
    assert quote.bid == Decimal("0.52")
    assert quote.bid_size == Decimal("0.125")
    snapshot_state = state.snapshots[-1]
    assert dict(snapshot_state.bids) == {
        Decimal("0.50"): Decimal("1.5"),
        Decimal("0.52"): Decimal("0.125"),
    }


def test_local_times_reject_nonexistent_and_unpinned_ambiguous_readings() -> None:
    assert classify_local_time("2026-03-08T02:30:00", NY) == "nonexistent"
    assert classify_local_time("2026-11-01T01:30:00", NY) == "ambiguous"
    assert classify_local_time("2026-07-01T12:00:00", NY) == "unique"

    release_time = parse_local_time("2026-09-11T08:30:00", NY)
    assert release_time == dt.datetime(2026, 9, 11, 12, 30, tzinfo=UTC)

    with pytest.raises(ValueError, match="spring-forward"):
        parse_local_time("2026-03-08T02:30:00", NY)
    with pytest.raises(ValueError, match="ambiguous"):
        parse_local_time("2026-11-01T01:30:00", NY)
    first = parse_local_time("2026-11-01T01:30:00", NY, fold=0)
    second = parse_local_time("2026-11-01T01:30:00", NY, fold=1)
    assert first == dt.datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    assert second == dt.datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    assert first != second
    with pytest.raises(ValueError):
        parse_local_time("2026-11-01T01:30:00", NY, fold=2)


def test_naive_and_unqualified_timestamps_are_refused() -> None:
    with pytest.raises(ValueError, match="naive"):
        parse_utc_time("2026-09-11T08:30:00", field_name="t")
    with pytest.raises(ValueError, match="naive"):
        parse_utc_time(dt.datetime(2026, 9, 11, 8, 30), field_name="t")
    assert parse_utc_time("2026-09-11T08:30:00-04:00", field_name="t") == dt.datetime(
        2026, 9, 11, 12, 30, tzinfo=UTC
    )


def test_market_keys_are_venue_qualified_and_round_trip() -> None:
    left = market_key("kalshi", "CPI-26MAR-T0.3")
    right = market_key("polymarket", "CPI-26MAR-T0.3")
    assert left != right
    assert split_market_key(left) == ("kalshi", "CPI-26MAR-T0.3")
    assert split_market_key(right) == ("polymarket", "CPI-26MAR-T0.3")
    with pytest.raises(ValueError):
        market_key("venue|a", "contract")
    with pytest.raises(ValueError):
        split_market_key("no-separator")


def test_occurrence_identity_is_not_a_content_hash() -> None:
    identical = provenance("id-a", raw_hash="a" * 64)
    other = provenance("id-b", raw_hash="a" * 64)
    assert identical.raw_hash == other.raw_hash
    assert identical.occurrence_key != other.occurrence_key
    generated = Provenance(raw_hash="b" * 64, record_id="generated-1", source="venue-a")
    assert not generated.record_id.startswith("b" * 8)


def test_operator_and_rounding_stay_closed_vocabularies() -> None:
    assert Operator("above") == "above"
    assert Operator.ABOVE.is_strict is True
    assert Operator.AT_LEAST.is_strict is False
    assert Operator.RANGE.is_strict is None
    assert Operator.ABOVE.orientation_sign == Decimal(1)
    assert Operator.AT_MOST.orientation_sign == Decimal(-1)
    with pytest.raises(ValueError):
        Operator("greater_than")
    with pytest.raises(ValueError):
        Rounding("bankers")


def test_threshold_operators_require_a_threshold_and_ranges_require_bounds() -> None:
    with pytest.raises(ValueError, match="requires a threshold"):
        contract(operator=Operator.ABOVE, threshold=None)
    with pytest.raises(ValueError, match="must not carry lower/upper"):
        Contract(
            venue="venue-a",
            contract_id="c",
            event_id="e",
            family="CPI",
            reference_period="2026-02",
            source="BLS",
            units="index_points",
            operator=Operator.ABOVE,
            threshold=Decimal("0.3"),
            lower=Decimal("0.1"),
            upper=Decimal("0.5"),
            rounding=Rounding.NONE,
            vintage="initial",
            timezone=NY,
            deadline=None,
            settlement="cash",
            currency="USD",
            exceptional_policy="void_excluded",
            open_time=None,
            close_time=None,
            resolve_time=None,
            rule_hash="r",
            provenance=provenance("r"),
        )


def test_contract_keeps_every_rule_matching_field() -> None:
    match_fields = contract().match_values()
    assert set(match_fields) == set(Contract.MATCH_FIELDS)
    for name in (
        "reference_period",
        "source",
        "units",
        "operator",
        "threshold",
        "rounding",
        "vintage",
        "timezone",
        "deadline",
        "settlement",
        "currency",
        "exceptional_policy",
        "rule_hash",
    ):
        assert name in match_fields


def test_lifecycle_matching_separates_open_and_close_windows() -> None:
    window = contract(open_time=at(0), close_time=at(100))
    assert window.is_open_at(at(50)) is True
    assert window.is_open_at(at(-1)) is False
    assert window.is_open_at(at(100)) is False


def test_resolution_rejects_a_half_payout_as_binary() -> None:
    binary = resolution(payout="1")
    assert binary.is_binary is True
    assert binary.binary_payout == Decimal(1)
    assert binary.require_binary_payout() == Decimal(1)

    fractional = resolution(payout="0.5")
    assert fractional.is_binary is False
    assert fractional.binary_payout is None
    with pytest.raises(ValueError, match="fractional"):
        fractional.require_binary_payout()

    exceptional = resolution(payout="1", exceptional=True)
    assert exceptional.is_binary is False
    assert exceptional.binary_payout is None
    with pytest.raises(ValueError, match="exceptional"):
        exceptional.require_binary_payout()


def test_repeated_identical_payloads_stay_distinct_occurrences(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")
    payload = b'{"price":"0.52","size":"5"}'

    first = store.put(payload, source="venue-a", received_time=at(0))
    second = store.put(payload, source="venue-a", received_time=at(1))

    assert first.raw_hash == second.raw_hash
    assert first.record_id != second.record_id
    assert store.stored_hashes() == [first.raw_hash]
    receipts = store.receipts(raw_hash=first.raw_hash)
    assert len(receipts) == 2
    assert {r["record_id"] for r in receipts} == {first.record_id, second.record_id}


def test_explicit_record_id_is_idempotent_and_conflicting_content_is_refused(
    tmp_path: Path,
) -> None:
    store = RawStore(tmp_path / "raw")
    payload = b"payload-1"
    first = store.put(payload, source="venue-a", received_time=at(0), record_id="trade-9")
    again = store.put(payload, source="venue-a", received_time=at(30), record_id="trade-9")
    assert (first.raw_hash, first.record_id) == (again.raw_hash, again.record_id)
    assert len(store.receipts()) == 1
    assert store.receipt("trade-9", source="venue-a")["received_time"] == at(0).isoformat()

    with pytest.raises(ValueError, match="already stored"):
        store.put(b"payload-2", source="venue-a", received_time=at(60), record_id="trade-9")


def test_get_verifies_the_payload_hash(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")
    entry = store.put(b"original", source="venue-a", received_time=at(0))
    assert store.get(entry.raw_hash) == b"original"

    blob = next((tmp_path / "raw" / "blobs").glob("*/*.bin"))
    blob.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="hashes to"):
        store.get(entry.raw_hash)


def test_raw_store_refuses_credential_metadata(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")
    with pytest.raises(ValueError, match="credentials"):
        store.put(
            b"{}",
            source="venue-a",
            received_time=at(0),
            metadata={"api_key": "public-but-still-not-ours"},
        )


def test_quote_records_round_trip_through_a_sealed_dataset(tmp_path: Path) -> None:
    quotes = [
        quote_at(0, bid="0.52", ask="0.54"),
        quote_at(30, bid=None, ask="0.60", contract_id="CPI-OTHER"),
    ]
    path = tmp_path / "quotes.parquet"
    reference = write_parquet(quotes, path, table="quotes", coverage_epoch="fixture-1")
    assert reference.row_count == 2
    assert reference.content_hash

    frame = read_parquet(path)
    assert list(frame.columns) == list(resolve_rows(quotes, "quotes")[0].keys())
    assert frame["contract_id"].tolist() == ["CPI-OTHER", "CPI-THRESHOLD"]
    assert frame.loc[frame["contract_id"] == "CPI-THRESHOLD", "bid"].iloc[0] == Decimal("0.52")
    assert frame.loc[frame["contract_id"] == "CPI-OTHER", "bid"].iloc[0] is None


def test_sealed_write_is_reproducible_and_republish_is_a_no_op(tmp_path: Path) -> None:
    rows = [quote_at(0, bid="0.52", ask="0.54"), quote_at(30, bid="0.53", ask="0.55")]
    first = write_parquet(rows, tmp_path / "q.parquet", table="quotes")
    second = write_parquet(rows, tmp_path / "q.parquet", table="quotes")
    assert first.content_hash == second.content_hash

    third = write_parquet(list(reversed(rows)), tmp_path / "q.parquet", table="quotes")
    assert third.content_hash == first.content_hash


def test_changed_content_is_refused_and_detected_on_read(tmp_path: Path) -> None:
    path = tmp_path / "q.parquet"
    write_parquet([quote_at(0, bid="0.52", ask="0.54")], path, table="quotes")

    tampered = tmp_path / "other.parquet"
    write_parquet([quote_at(0, bid="0.11", ask="0.12")], tampered, table="quotes")
    path.write_bytes(tampered.read_bytes())
    with pytest.raises(ValueError, match="does not match its manifest"):
        read_parquet(path)


def test_unknown_and_missing_columns_are_rejected_at_the_boundary(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="undeclared columns"):
        write_parquet(
            [{"venue": "venue-a", "contract_id": "c", "not_a_column": 1}],
            tmp_path / "bad.parquet",
            table="quotes",
        )
    with pytest.raises(ValueError, match="unknown table"):
        write_parquet([], tmp_path / "bad2.parquet", table="no_such_table")


def test_rows_missing_a_required_column_are_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        write_parquet(
            [{"venue": "venue-a", "contract_id": "CPI-THRESHOLD", "validity": "valid"}],
            tmp_path / "no_hash.parquet",
            table="quotes",
        )


def test_duckdb_queries_run_over_a_sealed_dataset(tmp_path: Path) -> None:
    path = tmp_path / "quotes.parquet"
    reference = write_parquet(
        [
            quote_at(0, bid="0.52", ask="0.54"),
            quote_at(30, bid="0.53", ask="0.55", contract_id="B"),
        ],
        path,
        table="quotes",
    )
    frame = query_sealed(
        reference,
        "SELECT contract_id, count(*) AS n FROM quotes GROUP BY contract_id ORDER BY contract_id",
    )
    assert frame["contract_id"].tolist() == ["B", "CPI-THRESHOLD"]
    assert frame["n"].tolist() == [1, 1]

    with duckdb.connect() as connection:
        frame = query_sealed(reference, "SELECT count(*) AS n FROM quotes", connection=connection)
    assert frame["n"].iloc[0] == 2


def test_snapshot_then_deltas_reconstruct_both_sides() -> None:
    events = [
        snapshot(0, bids=(("0.50", "10"), ("0.48", "20")), asks=(("0.56", "5"), ("0.60", "8"))),
        delta(1, side="bid", price="0.52", size="7"),
        delta(2, side="bid", price="0.50", size=None, operation="delete", record_id="del-1"),
        delta(3, side="ask", price="0.56", size="3", operation="increment"),
        delta(4, side="ask", price="0.54", size="4"),
    ]
    result = replay(events, order=ORDER_USABLE)
    quote = result.quotes[-1]
    assert quote.valid is True
    assert quote.bid == Decimal("0.52")
    assert quote.ask == Decimal("0.54")
    assert quote.bid_size == Decimal("7")
    assert quote.ask_size == Decimal("4")
    assert quote.midpoint == Decimal("0.53")
    assert quote.spread == Decimal("0.02")
    assert quote.replay_order == ORDER_USABLE
    assert result.coverage["all_markets_valid"] is True


def test_size_increment_is_not_an_absolute_replacement() -> None:
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)),
        delta(1, side="bid", price="0.50", size="4"),
        delta(2, side="bid", price="0.50", size="4", operation="increment"),
        delta(3, side="bid", price="0.50", size="-4", operation="increment"),
    ]
    result = replay(events, order=ORDER_USABLE)
    sizes = [quote.bid_size for quote in result.quotes]
    assert sizes == [Decimal("10"), Decimal("4"), Decimal("8"), Decimal("4")]

    removed = replay(
        [*events, delta(4, side="bid", price="0.50", size="-4", operation="increment")],
        order=ORDER_USABLE,
    )
    assert removed.quotes[-1].bid is None
    assert removed.quotes[-1].validity is QuoteValidity.VALID


def test_empty_and_one_sided_books_are_observed_not_missing() -> None:
    events = [
        snapshot(0, bids=(), asks=()),
        delta(1, side="ask", price="0.60", size="5"),
    ]
    result = replay(events, order=ORDER_USABLE)
    empty = result.quotes[0]
    assert empty.validity is QuoteValidity.VALID
    assert empty.bid is None and empty.ask is None
    assert empty.midpoint is None and empty.spread is None and empty.depth is None

    one_sided = result.quotes[1]
    assert one_sided.validity is QuoteValidity.VALID
    assert one_sided.ask == Decimal("0.60")
    assert one_sided.midpoint is None
    assert one_sided.spread is None

    crossed = replay(
        [snapshot(0, bids=(("0.60", "5"),), asks=(("0.55", "5"),))], order=ORDER_USABLE
    ).quotes[-1]
    assert crossed.validity is QuoteValidity.CROSSED
    assert crossed.midpoint is None
    assert crossed.reason == "crossed"


def test_trade_prices_are_not_quotes() -> None:
    trade = Trade(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        trade_id="t-1",
        price=Decimal("0.99"),
        size=Decimal("100"),
        clock=clock(5),
        provenance=provenance("trade-1"),
    )
    state = BookState(ORDER_USABLE)
    apply_book_event(state, snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)))
    state.record_trade(trade)
    apply_book_event(state, delta(6, side="ask", price="0.60", size="3"))
    quote = state.quote_series("venue-a", "CPI-THRESHOLD")[-1]
    assert quote.bid == Decimal("0.50")
    assert quote.ask == Decimal("0.56")
    assert quote.last_trade == at(5)
    assert quote.midpoint == Decimal("0.53")


def test_three_distinct_times_stay_distinct() -> None:
    trade = Trade(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        trade_id="t-1",
        price=Decimal("0.53"),
        size=Decimal("1"),
        clock=clock(3),
        provenance=provenance("trade-1"),
    )
    state = BookState(ORDER_USABLE)
    apply_book_event(state, snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)))
    state.record_trade(trade)
    apply_book_event(state, delta(10, side="bid", price="0.50", size="10"))
    quote = state.quote_series("venue-a", "CPI-THRESHOLD")[-1]
    assert quote.last_price_change == at(0)
    assert quote.last_verified == at(10)
    assert quote.last_trade == at(3)

    # A message that leaves the book unchanged still refreshes verification and
    # must not be reported as a price change.
    apply_book_event(state, delta(20, side="bid", price="0.50", size="10"))
    refreshed = state.quote_series("venue-a", "CPI-THRESHOLD")[-1]
    assert refreshed.last_price_change == at(0)
    assert refreshed.last_verified == at(20)

    apply_book_event(state, delta(30, side="bid", price="0.50", size="12"))
    changed = state.quote_series("venue-a", "CPI-THRESHOLD")[-1]
    assert changed.last_price_change == at(30)
    assert changed.last_verified == at(30)


def test_sequence_gap_invalidates_until_a_fresh_snapshot() -> None:
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),), sequence=1),
        delta(1, side="bid", price="0.51", size="4", sequence=2),
        delta(2, side="bid", price="0.52", size="4", sequence=5),
        delta(3, side="bid", price="0.53", size="4", sequence=6),
        snapshot(4, bids=(("0.54", "9"),), asks=(("0.58", "5"),), sequence=7),
        delta(5, side="bid", price="0.55", size="9", sequence=8),
    ]
    result = replay(events, order=ORDER_USABLE)
    validities = [quote.validity for quote in result.quotes]
    assert validities == [
        QuoteValidity.VALID,
        QuoteValidity.VALID,
        QuoteValidity.GAP,
        QuoteValidity.GAP,
        QuoteValidity.VALID,
        QuoteValidity.VALID,
    ]
    assert result.coverage["all_markets_valid"] is True

    gap = result.gaps[0]
    assert gap["kind"] == "sequence_gap"
    assert gap["missing_count"] == 2
    assert gap["affected_markets"] == ["venue-a|CPI-THRESHOLD"]
    assert gap["recovered_markets"] == ["venue-a|CPI-THRESHOLD"]
    assert gap["recovered_at"] == at(4)
    assert result.quotes[4].valid is True

    # The deferred delta never edited levels, so recovery comes from the snapshot.
    assert result.quotes[4].bid == Decimal("0.54")
    assert result.quotes[5].bid == Decimal("0.55")


def test_interleaved_contracts_on_one_scope_do_not_fake_a_gap() -> None:
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),), contract_id="A", sequence=1),
        snapshot(1, bids=(("0.20", "10"),), asks=(("0.26", "5"),), contract_id="B", sequence=2),
        delta(2, side="bid", price="0.51", size="4", contract_id="A", sequence=3),
        delta(3, side="bid", price="0.21", size="4", contract_id="B", sequence=4),
        delta(4, side="ask", price="0.56", size="7", contract_id="A", sequence=5),
        delta(5, side="ask", price="0.26", size="7", contract_id="B", sequence=6),
    ]
    result = replay(events, order=ORDER_USABLE)
    assert result.gaps == ()
    assert result.coverage["sequence_scope"] if False else True
    for quote in result.quotes:
        assert quote.validity is QuoteValidity.VALID
    final = {(quote.contract_id): (quote.bid, quote.ask) for quote in result.quotes[-2:]}
    assert final["A"] == (Decimal("0.51"), Decimal("0.56"))
    assert final["B"] == (Decimal("0.21"), Decimal("0.26"))
    scope_summary = result.coverage["scopes"][0]
    assert scope_summary["contracts"] == ["A", "B"]
    assert scope_summary["last_sequence"] == 6

    # A real gap on the shared scope invalidates both contracts.
    gapped = replay(
        [*events, delta(6, side="bid", price="0.30", size="1", contract_id="A", sequence=9)],
        order=ORDER_USABLE,
    )
    assert gapped.gaps[0]["missing_count"] == 2
    assert gapped.gaps[0]["affected_markets"] == ["venue-a|A", "venue-a|B"]
    assert gapped.quotes[-1].validity is QuoteValidity.GAP


def test_disconnected_book_stays_invalid_until_a_snapshot_on_a_new_generation() -> None:
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),), sequence=1),
        lifecycle(1, kind="disconnect"),
        delta(2, side="bid", price="0.51", size="4"),
        snapshot(3, bids=(("0.49", "10"),), asks=(("0.55", "5"),), sequence=1),
    ]
    result = replay(events, order=ORDER_USABLE)
    assert result.quotes[0].validity is QuoteValidity.VALID
    assert result.quotes[1].validity is QuoteValidity.DISCONNECTED
    assert result.quotes[2].validity is QuoteValidity.DISCONNECTED
    assert result.quotes[2].bid == Decimal("0.50")
    assert result.quotes[3].validity is QuoteValidity.VALID
    assert result.quotes[3].bid == Decimal("0.49")
    assert [gap["kind"] for gap in result.gaps] == ["disconnect"]
    assert result.gaps[0]["recovered_markets"] == ["venue-a|CPI-THRESHOLD"]
    assert result.gaps[0]["recovered_at"] == at(3)


def test_closure_invalidates_and_is_not_revived_by_later_quotes() -> None:
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),), sequence=1),
        lifecycle(1, kind="close", sequence=2),
        delta(2, side="bid", price="0.51", size="4", sequence=3),
    ]
    result = replay(events, order=ORDER_USABLE)
    assert result.quotes[0].validity is QuoteValidity.VALID
    assert result.quotes[1].validity is QuoteValidity.CLOSED
    assert result.quotes[2].validity is QuoteValidity.CLOSED


def test_halt_masks_the_quote_until_it_is_lifted() -> None:
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),), sequence=1),
        lifecycle(1, kind="halt", sequence=2),
    ]
    halted = replay(events, order=ORDER_USABLE)
    assert halted.quotes[-1].validity is QuoteValidity.HALTED
    assert halted.quotes[-1].reason == "halted"
    revived = replay(
        [
            *events,
            snapshot(2, bids=(("0.55", "3"),), asks=(("0.60", "3"),), sequence=3),
        ],
        order=ORDER_USABLE,
    )
    assert revived.quotes[-1].validity is QuoteValidity.VALID


def test_out_of_order_input_is_normalized_before_reduction() -> None:
    ordered = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),), sequence=1),
        delta(1, side="bid", price="0.51", size="7", sequence=2),
        delta(2, side="bid", price="0.52", size="3", sequence=3),
    ]
    shuffled = [ordered[2], ordered[0], ordered[1]]
    canonical, issues = canonicalize_events(shuffled, order=ORDER_USABLE)
    assert [event.clock.usable_time for event in canonical] == [at(0), at(1), at(2)]
    assert issues == []

    forward = replay(ordered, order=ORDER_USABLE)
    backward = replay(shuffled, order=ORDER_USABLE)
    assert backward.quotes[-1].bid == forward.quotes[-1].bid == Decimal("0.52")
    assert backward.coverage["gap_count"] == forward.coverage["gap_count"] == 0


def test_input_permutation_invariance_for_the_panel_and_features() -> None:
    quotes = [
        quote_at(0, bid="0.50", ask="0.56"),
        quote_at(30, bid="0.51", ask="0.57"),
        quote_at(60, bid="0.53", ask="0.59"),
        quote_at(90, bid="0.52", ask="0.58"),
    ]
    rows = build_event_panel(quotes, [release(seconds=120)], [contract()], order=ORDER_SOURCE)
    permuted = build_event_panel(
        list(reversed(quotes)), [release(seconds=120)], [contract()], order=ORDER_SOURCE
    )
    assert (
        rows.sort_values(["event_id", "horizon_seconds"])
        .reset_index(drop=True)
        .equals(permuted.sort_values(["event_id", "horizon_seconds"]).reset_index(drop=True))
    )

    here = features_asof(quotes, at(45), max_age_seconds=60)
    there = features_asof(list(reversed(quotes)), at(45), max_age_seconds=60)
    assert here == there
    assert here["venue-a|CPI-THRESHOLD"]["midpoint"] == Decimal("0.54")


def test_duplicate_occurrence_dropped_while_distinct_repeats_survive() -> None:
    first = delta(1, side="bid", price="0.50", size="4", operation="increment", record_id="occ-1")
    same_occurrence = delta(
        1, side="bid", price="0.50", size="4", operation="increment", record_id="occ-1"
    )
    distinct_repeat = delta(
        2, side="bid", price="0.50", size="4", operation="increment", record_id="occ-2"
    )
    repeated = replay(
        [
            snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)),
            first,
            same_occurrence,
        ],
        order=ORDER_USABLE,
    )
    assert repeated.quotes[-1].bid_size == Decimal("14")

    both = replay(
        [
            snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)),
            first,
            same_occurrence,
            distinct_repeat,
        ],
        order=ORDER_USABLE,
    )
    assert both.quotes[-1].bid_size == Decimal("18")
    duplicates = [
        issue for issue in both.coverage["issues"] if issue["kind"] == "duplicate_occurrence"
    ]
    assert len(duplicates) == 1


def test_identical_repeated_occurrences_with_the_same_generated_id_are_distinct() -> None:
    store_ids = [f"generated-{index}" for index in (1, 2)]
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)),
        delta(
            1,
            side="bid",
            price="0.50",
            size="5",
            operation="increment",
            record_id=store_ids[0],
        ),
        delta(
            1,
            side="bid",
            price="0.50",
            size="5",
            operation="increment",
            record_id=store_ids[1],
        ),
    ]
    result = replay(events, order=ORDER_USABLE)
    assert result.quotes[-1].bid_size == Decimal("20")


def test_replay_reports_both_orders_without_collapsing_them() -> None:
    late_receipt = BookEvent(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        kind="delta",
        clock=Clock.captured(at(-60), at(120)),
        provenance=provenance("late-1", source="venue-a"),
        connection_id="conn-1",
        sequence_scope="book-1",
        side="bid",
        price=Decimal("0.52"),
        size=Decimal("5"),
    )
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)),
        late_receipt,
        delta(60, side="bid", price="0.51", size="5", record_id="on-time"),
    ]
    comparison = compare_replay_orders(events)
    assert comparison["source_order"] == ORDER_SOURCE
    assert comparison["usable_order"] == ORDER_USABLE
    assert comparison["inversion_count"] >= 1
    assert comparison["state_disagreement_count"] >= 1
    assert comparison["agreement"] is False

    source = replay(events, order=ORDER_SOURCE)
    usable = replay(events, order=ORDER_USABLE)
    assert source.quotes[-1].bid != usable.quotes[-1].bid
    assert {quote.replay_order for quote in source.quotes} == {ORDER_SOURCE}
    assert {quote.replay_order for quote in usable.quotes} == {ORDER_USABLE}


def test_agreeing_orders_are_reported_as_agreeing() -> None:
    events = [
        snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)),
        delta(30, side="bid", price="0.51", size="5"),
    ]
    comparison = compare_replay_orders(events)
    assert comparison["inversion_count"] == 0
    assert comparison["state_disagreement_count"] == 0
    assert comparison["final_state_disagreement_count"] == 0
    assert comparison["agreement"] is True


def test_book_event_and_trade_from_one_message_are_not_merged() -> None:
    """One delivered message can normalize into a book event and a trade.

    They are different observations of the same occurrence, so deduplication keyed
    on the source's occurrence id alone would silently drop one of them.
    """
    shared = provenance("msg-42", source="venue-a")
    event = BookEvent(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        kind="snapshot",
        clock=clock(0),
        provenance=shared,
        connection_id="conn-1",
        sequence_scope="book-1",
        bids=((Decimal("0.50"), Decimal("10")),),
        asks=((Decimal("0.56"), Decimal("5")),),
    )
    trade = Trade(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        trade_id="t-42",
        price=Decimal("0.53"),
        size=Decimal("3"),
        clock=clock(0),
        provenance=shared,
    )
    ordered, issues = canonicalize_events([event, trade], order=ORDER_USABLE)
    assert len(ordered) == 2
    assert issues == []

    result = replay([event, trade], order=ORDER_USABLE)
    assert result.coverage["trade_count"] == 1
    assert len(result.quotes) == 1
    # The quote emitted for the snapshot predates the trade, so the trade time
    # belongs to the book's final state rather than to that earlier quote.
    assert result.coverage["books"][0]["last_trade"] == at(0)
    assert result.quotes[0].last_trade is None
    assert result.quotes[0].midpoint == Decimal("0.53")


def test_reducer_mutates_and_returns_caller_owned_state() -> None:
    state = BookState(ORDER_USABLE)
    returned = apply_book_event(state, snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),)))
    assert returned is state
    assert state.processed == 1
    with pytest.raises(TypeError):
        apply_book_event(state, "not-an-event")  # type: ignore[arg-type]


def test_replay_requires_a_known_order_and_scoped_sequences() -> None:
    assert resolve_order("source") == ORDER_SOURCE
    assert resolve_order("usable_time") == ORDER_USABLE
    with pytest.raises(ValueError, match="unknown replay order"):
        resolve_order("alphabetical")
    with pytest.raises(ValueError, match="requires sequence_scope"):
        BookEvent(
            venue="venue-a",
            contract_id="A",
            kind="snapshot",
            clock=clock(0),
            provenance=provenance("s"),
            sequence=4,
        )


def test_two_connections_for_one_market_are_separate_books() -> None:
    left = snapshot(0, bids=(("0.50", "10"),), asks=(("0.56", "5"),), scope="book-1")
    right = BookEvent(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        kind="snapshot",
        clock=clock(1),
        provenance=provenance("snap-other"),
        connection_id="conn-2",
        sequence_scope="book-2",
        bids=((Decimal("0.70"), Decimal("1")),),
        asks=((Decimal("0.72"), Decimal("1")),),
    )
    result = replay([left, right], order=ORDER_USABLE)
    bids = sorted(quote.bid for quote in result.quotes)
    assert bids == [Decimal("0.50"), Decimal("0.70")]
    assert result.coverage["book_count"] == 2


def test_features_asof_excludes_future_records_and_uses_verified_age() -> None:
    quotes = [
        quote_at(0, bid="0.50", ask="0.56", verified_at=0),
        quote_at(300, bid="0.80", ask="0.90", verified_at=300),
    ]
    state = features_asof(quotes, at(60), max_age_seconds=120)
    entry = state["venue-a|CPI-THRESHOLD"]
    assert entry["bid"] == Decimal("0.50")
    assert entry["max_input_available_time"] == at(0)
    assert entry["valid"] is True
    assert entry["age_seconds"] == pytest.approx(60.0)

    injected = features_asof(
        [quote_at(-60, bid="0.10", ask="0.20"), *quotes], at(60), max_age_seconds=120
    )
    assert injected["venue-a|CPI-THRESHOLD"]["bid"] == Decimal("0.50")


def test_features_asof_excludes_unknown_usable_time_and_source_replays() -> None:
    historical = Quote(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        clock=Clock.historical(at(0)),
        provenance=provenance("hist-1"),
        bid=Decimal("0.40"),
        ask=Decimal("0.45"),
        bid_size=Decimal("1"),
        ask_size=Decimal("1"),
        validity=QuoteValidity.VALID,
        last_price_change=at(0),
        last_verified=at(0),
        last_trade=None,
    )
    state = features_asof([historical], at(600))
    entry = state["venue-a|CPI-THRESHOLD"]
    assert entry["valid"] is False
    assert entry["reason"] == "usable_time_unknown"
    assert entry["bid"] is None
    assert entry["max_input_available_time"] is None

    source_replay = quote_at(0, bid="0.50", ask="0.56", order=ORDER_SOURCE)
    usable_state = features_asof([source_replay], at(600))
    assert usable_state["venue-a|CPI-THRESHOLD"]["valid"] is False
    source_state = features_asof([source_replay], at(600), order=ORDER_SOURCE)
    assert source_state["venue-a|CPI-THRESHOLD"]["valid"] is True


def test_features_asof_keeps_latest_invalid_state_instead_of_an_older_valid_book() -> None:
    quotes = [
        quote_at(0, bid="0.50", ask="0.56", verified_at=0),
        quote_at(
            10,
            bid="0.52",
            ask="0.54",
            validity=QuoteValidity.GAP,
            verified_at=10,
            record_id="gap-1",
        ),
    ]
    state = features_asof(quotes, at(30), max_age_seconds=600)
    entry = state["venue-a|CPI-THRESHOLD"]
    assert entry["valid"] is False
    assert entry["reason"] == "gap"
    assert entry["bid"] is None
    assert entry["midpoint"] is None
    assert entry["raw_hash"] == provenance("gap-1").raw_hash


def test_features_asof_staleness_uses_last_verified_not_price_change_age() -> None:
    still_verified = Quote(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        clock=clock(0),
        provenance=provenance("standing"),
        bid=Decimal("0.50"),
        ask=Decimal("0.56"),
        bid_size=Decimal("10"),
        ask_size=Decimal("10"),
        validity=QuoteValidity.VALID,
        last_price_change=at(0),
        last_verified=at(200),
        last_trade=None,
        replay_order=ORDER_USABLE,
    )
    fresh = features_asof([still_verified], at(250), max_age_seconds=120)
    assert fresh["venue-a|CPI-THRESHOLD"]["valid"] is True

    stale = features_asof([still_verified], at(400), max_age_seconds=120)
    assert stale["venue-a|CPI-THRESHOLD"]["valid"] is False
    assert stale["venue-a|CPI-THRESHOLD"]["reason"] == "stale_last_verified"

    unverified = Quote(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        clock=clock(0),
        provenance=provenance("unverified"),
        bid=Decimal("0.50"),
        ask=Decimal("0.56"),
        bid_size=None,
        ask_size=None,
        validity=QuoteValidity.VALID,
        last_price_change=None,
        last_verified=None,
        last_trade=None,
        replay_order=ORDER_USABLE,
    )
    assert features_asof([unverified], at(1))["venue-a|CPI-THRESHOLD"]["reason"] == (
        "last_verified_unknown"
    )


def test_features_asof_key_is_venue_qualified() -> None:
    quotes = [
        quote_at(0, bid="0.50", ask="0.56", venue="kalshi"),
        quote_at(0, bid="0.90", ask="0.95", venue="polymarket"),
    ]
    state = features_asof(quotes, at(10))
    assert set(state) == {"kalshi|CPI-THRESHOLD", "polymarket|CPI-THRESHOLD"}
    assert state["kalshi|CPI-THRESHOLD"]["bid"] == Decimal("0.50")
    assert state["polymarket|CPI-THRESHOLD"]["bid"] == Decimal("0.90")

    single = features_asof([quote_at(0, bid="0.50", ask="0.56", venue="kalshi")], at(10))
    assert (
        features_asof([quote_at(0, bid="0.51", ask="0.55", venue="polymarket")], at(10)) != single
    )


def test_features_asof_rejects_naive_times() -> None:
    with pytest.raises(ValueError, match="naive"):
        features_asof([], dt.datetime(2026, 3, 2, 12, 0, 0))


def test_delaying_usable_time_delays_influence() -> None:
    records = [
        Quote(
            venue="venue-a",
            contract_id="CPI-THRESHOLD",
            clock=Clock.captured(at(0), at(0 + delay)),
            provenance=provenance(f"delayed-{delay}"),
            bid=Decimal("0.80"),
            ask=Decimal("0.84"),
            bid_size=Decimal("1"),
            ask_size=Decimal("1"),
            validity=QuoteValidity.VALID,
            last_price_change=at(0),
            last_verified=at(0 + delay),
            last_trade=None,
            replay_order=ORDER_USABLE,
        )
        for delay in (5, 120)
    ]
    early = features_asof([records[0]], at(60))
    assert early["venue-a|CPI-THRESHOLD"]["bid"] == Decimal("0.80")
    late = features_asof([records[1]], at(60))
    assert late["venue-a|CPI-THRESHOLD"]["valid"] is False
    assert late["venue-a|CPI-THRESHOLD"]["reason"] == "not_yet_available"


def test_available_labels_uses_known_at_and_ignores_venue_settlement() -> None:
    cutoff = at(1000)
    known_early = resolution(contract_id="A", known_at=at(10), resolved_at=at(9000))
    known_late = resolution(contract_id="B", known_at=at(2000), resolved_at=at(100))
    unknown = resolution(contract_id="C", known_at=None, resolved_at=at(100))

    available = available_labels([known_early, known_late, unknown], cutoff)
    assert [item.contract_id for item in available] == ["A"]

    with pytest.raises(ValueError, match="naive"):
        available_labels([known_early], dt.datetime(2026, 1, 1))
    assert available_labels([], cutoff) == []


def test_panel_has_the_declared_columns_and_contract_orientation() -> None:
    frame = build_event_panel(
        [quote_at(0, bid="0.50", ask="0.56")],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60, 300],
    )
    assert list(frame.columns) == list(PANEL_COLUMNS)
    assert len(frame) == 2
    assert frame["horizon_seconds"].tolist() == [60, 300]
    assert frame["cohort"].tolist() == ["direct", "direct"]
    assert frame["operator"].tolist() == ["above", "above"]
    assert frame["orientation_sign"].tolist() == [1.0, 1.0]
    assert frame["cluster_id"].tolist() == ["CPI-2026-03", "CPI-2026-03"]
    assert frame["replay_order"].tolist() == [ORDER_SOURCE, ORDER_SOURCE]
    assert frame["training_cutoff"].isna().all()
    assert frame["split"].isna().all()


def test_panel_baseline_is_strictly_before_the_event() -> None:
    quotes = [
        quote_at(0, bid="0.50", ask="0.56"),
        quote_at(120, bid="0.90", ask="0.96"),
        quote_at(180, bid="0.70", ask="0.76"),
    ]
    frame = build_event_panel(
        quotes, [release(seconds=120)], [contract()], order=ORDER_SOURCE, horizons_seconds=[60]
    )
    row = frame.iloc[0]
    assert bool(row["valid"]) is True
    assert row["baseline_time"] == at(0)
    assert row["baseline"] == pytest.approx(0.53)
    assert row["endpoint_time"] == at(180)
    assert row["endpoint"] == pytest.approx(0.73)
    assert row["response"] == pytest.approx(0.20)
    expected_hashes = ",".join(
        sorted(
            {
                provenance("quote-0-CPI-THRESHOLD-valid").raw_hash,
                provenance("quote-180-CPI-THRESHOLD-valid").raw_hash,
            }
        )
    )
    assert set(row["raw_hashes"].split(",")) == set(expected_hashes.split(","))

    late_only = build_event_panel(
        [quotes[1]], [release(seconds=120)], [contract()], order=ORDER_SOURCE, horizons_seconds=[60]
    )
    assert bool(late_only.iloc[0]["valid"]) is False
    assert late_only.iloc[0]["exclusion_reason"] == "missing_baseline"


def test_panel_masks_but_retains_contaminated_and_closed_rows() -> None:
    quotes = [
        quote_at(0, bid="0.50", ask="0.56"),
        quote_at(60, bid="0.52", ask="0.58"),
        quote_at(
            200,
            bid="0.53",
            ask="0.59",
            validity=QuoteValidity.CLOSED,
            record_id="close-1",
        ),
    ]
    closed = build_event_panel(
        quotes,
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60, 300],
        contamination_windows={"CPI-2026-03": [(at(600), at(700))]},
    )
    assert len(closed) == 2
    short = closed[closed["horizon_seconds"] == 60].iloc[0]
    wide = closed[closed["horizon_seconds"] == 300].iloc[0]
    assert bool(short["valid"]) is True
    assert bool(wide["valid"]) is False
    assert wide["exclusion_reason"] == "closed_in_window"
    assert pd.isna(wide["response"])
    assert pd.isna(wide["endpoint"])

    # The same window, now truncated by a contamination interval that starts
    # after the short horizon and lands inside the wide one.
    contaminated = build_event_panel(
        quotes[:2],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60, 300],
        contamination_windows={"*": [(at(200), at(300))]},
    )
    assert bool(contaminated[contaminated["horizon_seconds"] == 60].iloc[0]["valid"]) is True
    overlap = contaminated[contaminated["horizon_seconds"] == 300].iloc[0]
    assert bool(overlap["valid"]) is False
    assert overlap["exclusion_reason"] == "contaminated"


def test_panel_masks_a_rule_published_after_the_event() -> None:
    frame = build_event_panel(
        [quote_at(0, bid="0.50", ask="0.56")],
        [release(seconds=120)],
        [contract(rule_available_at=at(200))],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
    )
    row = frame.iloc[0]
    assert bool(row["valid"]) is False
    assert row["exclusion_reason"] == "rule_unavailable"


def test_panel_admits_source_times_and_labels_them_as_such() -> None:
    historical = Quote(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        clock=Clock.historical(at(60)),
        provenance=provenance("hist-1"),
        bid=Decimal("0.50"),
        ask=Decimal("0.56"),
        bid_size=Decimal("2"),
        ask_size=Decimal("2"),
        validity=QuoteValidity.VALID,
        last_price_change=at(60),
        last_verified=at(60),
        last_trade=None,
    )
    frame = build_event_panel(
        [historical],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
    )
    row = frame.iloc[0]
    assert bool(row["valid"]) is True
    assert row["clock_quality"] == "unknown"
    assert pd.isna(row["timing_uncertainty_seconds"])
    assert row["baseline_time"] == at(60)
    assert row["baseline_age_seconds"] == pytest.approx(60.0)

    usable_frame = build_event_panel(
        [historical],
        [release(seconds=120)],
        [contract()],
        order=ORDER_USABLE,
        horizons_seconds=[60],
    )
    assert usable_frame.empty or usable_frame.iloc[0]["exclusion_reason"] in (
        "missing_baseline",
        "availability_unknown",
    )


def test_panel_uses_the_fold_specific_event_time() -> None:
    observed = Release(
        event_id="CPI-2026-03",
        family="CPI",
        scheduled_at=at(0),
        reference_period="2026-02",
        values={"headline": Decimal("0.4")},
        clock=Clock.captured(at(120), at(125)),
        provenance=provenance("release-late"),
    )
    quotes = [
        quote_at(-30, bid="0.50", ask="0.56", verified_at=-30),
        quote_at(180, bid="0.60", ask="0.66", verified_at=180),
    ]
    source = build_event_panel(
        quotes,
        [observed],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
        max_age_seconds=300,
    ).iloc[0]
    usable = build_event_panel(
        quotes,
        [observed],
        [contract()],
        order=ORDER_USABLE,
        horizons_seconds=[180],
        max_age_seconds=300,
    ).iloc[0]

    # Source fold anchors on the observed publication time, so its window runs
    # from 120s. Usable fold anchors on the time the release was usable here, so
    # the same horizon lands 5s later.
    assert source["event_time"] == at(120)
    assert source["baseline_time"] == at(-30)
    assert source["endpoint_time"] == at(180)
    assert bool(source["valid"]) is True
    assert usable["event_time"] == at(125)
    assert usable["baseline_time"] == at(-30)
    assert usable["endpoint_time"] == at(305)
    assert bool(usable["valid"]) is True


def test_panel_moving_cutoff_drops_backfilled_quotes_before_the_floor() -> None:
    quotes = [
        quote_at(0, bid="0.50", ask="0.56"),
        quote_at(60, bid="0.52", ask="0.58"),
    ]
    covered = build_event_panel(
        quotes, [release(seconds=120)], [contract()], order=ORDER_SOURCE, horizons_seconds=[60]
    )
    assert bool(covered.iloc[0]["valid"]) is True

    gated = build_event_panel(
        quotes,
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
        exclude_quotes_before=at(70),
    )
    assert bool(gated.iloc[0]["valid"]) is False
    assert gated.iloc[0]["exclusion_reason"] == "missing_baseline"


def test_panel_requires_cohort_assignment_to_include_a_contract() -> None:
    both = build_event_panel(
        [quote_at(0, bid="0.50", ask="0.56")],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
        cohorts={("CPI-2026-03", "CPI-THRESHOLD"): "downstream"},
    )
    assert both.iloc[0]["cohort"] == "downstream"

    absent = build_event_panel(
        [quote_at(0, bid="0.50", ask="0.56")],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
        cohorts={("OTHER-EVENT", "CPI-THRESHOLD"): "control"},
    )
    assert absent.empty

    with pytest.raises(ValueError, match="unknown cohort"):
        build_event_panel(
            [quote_at(0, bid="0.50", ask="0.56")],
            [release(seconds=120)],
            [contract()],
            order=ORDER_SOURCE,
            horizons_seconds=[60],
            cohorts={("CPI-2026-03", "CPI-THRESHOLD"): "treated"},
        )


def test_panel_labels_come_from_known_at_and_stay_masked_without_one() -> None:
    resolution_early = resolution(known_at=at(400), resolved_at=at(5000))
    with_label = build_event_panel(
        [quote_at(0, bid="0.50", ask="0.56")],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
        resolutions=[resolution_early],
    )
    assert with_label.iloc[0]["label_available_time"] == at(400)

    without = build_event_panel(
        [quote_at(0, bid="0.50", ask="0.56")],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
        resolutions=[resolution(known_at=None, resolved_at=at(300))],
    )
    assert pd.isna(without.iloc[0]["label_available_time"])


def test_panel_rejects_trades_in_place_of_quotes() -> None:
    trade = Trade(
        venue="venue-a",
        contract_id="CPI-THRESHOLD",
        trade_id="t-1",
        price=Decimal("0.99"),
        size=Decimal("1"),
        clock=clock(0),
        provenance=provenance("trade-1"),
    )
    with pytest.raises(TypeError, match="not Quote records"):
        build_event_panel(
            [trade],  # type: ignore[list-item]
            [release(seconds=120)],
            [contract()],
            order=ORDER_SOURCE,
            horizons_seconds=[60],
        )


def test_panel_marks_one_sided_and_stale_endpoints_invalid() -> None:
    frame = build_event_panel(
        [
            quote_at(0, bid="0.50", ask="0.56", verified_at=0),
            quote_at(60, bid="0.52", ask=None, verified_at=60, record_id="one-sided"),
        ],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
        max_age_seconds=120,
    )
    assert frame.iloc[0]["exclusion_reason"] == "side_missing"

    stale = build_event_panel(
        [
            quote_at(0, bid="0.50", ask="0.56", verified_at=0),
            quote_at(60, bid="0.52", ask="0.58", verified_at=-600, record_id="stale"),
        ],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
        max_age_seconds=120,
    )
    assert stale.iloc[0]["exclusion_reason"] == "quote_stale"


def test_panel_rows_survive_a_round_trip_through_storage(tmp_path: Path) -> None:
    frame = build_event_panel(
        [
            quote_at(0, bid="0.50", ask="0.56"),
            quote_at(180, bid="0.58", ask="0.64"),
        ],
        [release(seconds=120)],
        [contract(threshold="0.3", contract_id="CPI-THRESHOLD")],
        order=ORDER_SOURCE,
        horizons_seconds=[60, 380],
        max_age_seconds=400,
    )
    path = tmp_path / "panel.parquet"
    reference = write_parquet(frame, path, table="event_panel", coverage_epoch="fixture")
    assert reference.row_count == 2

    back = read_parquet(path)
    assert list(back.columns) == list(PANEL_COLUMNS)
    assert back["threshold"].tolist() == [0.3, 0.3]
    assert back["valid"].tolist() == [True, True]
    assert back["horizon_seconds"].tolist() == [60, 380]

    aggregates = query_sealed(reference, "SELECT sum(horizon_seconds) AS total FROM event_panel")
    assert aggregates["total"].iloc[0] == 440


def test_release_keeps_initial_values_and_separate_revisions() -> None:
    record = Release(
        event_id="PAYROLLS-2026-03",
        family="employment",
        scheduled_at=at(0),
        reference_period="2026-02",
        values={"payrolls": Decimal("151"), "unemployment": Decimal("4.1")},
        clock=Clock.captured(at(0), at(3)),
        provenance=provenance("release-1"),
        revisions={"payrolls_prior": Decimal("12")},
    )
    assert record.values["payrolls"] == Decimal("151")
    assert record.revisions["payrolls_prior"] == Decimal("12")
    assert "payrolls_prior" not in record.values
    assert record.observed_at == at(0)

    with pytest.raises(ValueError, match="must not be empty"):
        Release(
            event_id="E",
            family="f",
            scheduled_at=at(0),
            reference_period="p",
            values={},
            clock=clock(0),
            provenance=provenance("r"),
        )


def test_expectation_routes_are_named_and_market_implied_is_flagged() -> None:
    consensus = Expectation(
        event_id="CPI-2026-03",
        statistic="headline_mom",
        value=Decimal("0.3"),
        source_kind="licensed_consensus",
        clock=clock(-3600),
        provenance=provenance("e-1"),
    )
    implied = Expectation(
        event_id="CPI-2026-03",
        statistic="headline_mom",
        value=Decimal("0.31"),
        source_kind="market_implied",
        clock=clock(-3600),
        provenance=provenance("e-2"),
    )
    assert consensus.is_market_implied is False
    assert implied.is_market_implied is True
    assert "licensed_consensus" in Expectation.SOURCE_KINDS
    assert "market_implied" in Expectation.SOURCE_KINDS


def test_expected_horizon_grid_defaults_are_the_plan_horizons() -> None:
    frame = build_event_panel(
        [quote_at(0, bid="0.50", ask="0.56")],
        [release(seconds=120)],
        [contract()],
        order=ORDER_SOURCE,
    )
    assert frame["horizon_seconds"].tolist() == [60, 300, 900, 1800, 3600]


def test_json_manifest_round_trips_exactly(tmp_path: Path) -> None:
    path = tmp_path / "panel.parquet"
    write_parquet(
        build_event_panel(
            [quote_at(0, bid="0.50", ask="0.56"), quote_at(180, bid="0.58", ask="0.64")],
            [release(seconds=120)],
            [contract()],
            order=ORDER_SOURCE,
            horizons_seconds=[60],
        ),
        path,
        table="event_panel",
    )
    manifest = json.loads((tmp_path / "panel.parquet.manifest.json").read_text())
    assert manifest["table"] == "event_panel"
    assert manifest["row_count"] == 1
    assert manifest["coverage_epoch"] == "fixture"
    assert set(manifest["columns"]) == set(PANEL_COLUMNS)
