"""Behavioural tests for the prospective market capture and the two-source rule.

Every shard here is synthetic and written to ``tmp_path``: the vendor archive is
tens of gigabytes and the venue's live listing is not available to a test. What the
synthetic rows defend is the set of facts a wrong implementation would get wrong
silently, so each test exists because a plausible bug survives otherwise.

* A window exactly one layer covers is governed by that layer; a window both cover
  is **refused**. Globbing both and unioning them counts one contract twice, which
  inflates the denominator every rate is measured against. The union is the bug the
  rule prevents, so the test plants a contract in both layers and asserts the
  refusal names it rather than asserting on a chosen layer.
* A window no layer covers is refused as uncovered, and is never served from a
  layer that does not cover it. A "pick the first glob that matched" fallback is
  silent here and produces a candidate universe from the wrong vintage.
* A layer absent from the checkout is not an overlap. An implementation that treats
  "no shards" as "covers" refuses every window on a fresh checkout, which reads as a
  passing guard and is a broken pipeline.
* A fractional count is preserved exactly. The venue states fixed-point counts and
  really publishes fractional ones; rounding stores a quantity nobody reported.
* An unstated count is null, never zero. Zero is a value the venue did not state,
  and the vendor archive's own ``volume`` column holds a zero where the venue states
  a real value, so a zero here would read as agreement with a number it never
  published.
* The captured rule text is carried verbatim. It is a later rule-vintage check's
  only evidence, so truncating or reformatting it destroys what the capture exists
  to preserve.
* A sub-cent price is refused by name. The shard's price columns are integer cents
  because the archive mapper requires integers, so rounding would store a price the
  venue never printed.

Nothing asserts on internal call structure: the assertions are on the resolution a
caller reads, the rows, and the sealed shard's own schema.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_propagation.domain import UTC
from market_propagation.ingest.market_capture import (
    BASIS_BOTH_LAYERS_COVER_THE_WINDOW,
    BASIS_LAYER_NAMED_EXPLICITLY,
    BASIS_NO_LAYER_COVERS_THE_WINDOW,
    BASIS_SOLE_COVERING_LAYER,
    MARKET_COLUMNS,
    TRADE_COLUMNS,
    MarketCaptureError,
    declared_market_layers,
    declared_series,
    market_row_from_record,
    resolve_market_layer,
    trade_row_from_record,
    write_capture_shards,
)
from market_propagation.ingest.transport import WireShapeError

VENDOR_LAYER = "kalshi_markets"
OWN_LAYER = "kalshi_own_markets"
SERIES = ("KXFED", "FED", "FEDDECISION", "KXFEDDECISION")

WINDOW_START = dt.datetime(2026, 1, 1, tzinfo=UTC)
WINDOW_END = dt.datetime(2026, 2, 1, tzinfo=UTC)

_MARKET_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("open_time", pa.timestamp("us", tz="UTC")),
        ("close_time", pa.timestamp("us", tz="UTC")),
    ]
)

#: A listing record the venue's live listing really serves, narrowed to the fields
#: the market row reads. The counts are fixed-point strings, as the venue sends them.
LISTING_RECORD = {
    "ticker": "KXFED-27APR-T4.25",
    "event_ticker": "KXFED-27APR",
    "market_type": "binary",
    "title": "Will the Fed's upper bound be above 4.25%?",
    "yes_sub_title": "Above 4.25%",
    "no_sub_title": "Not above 4.25%",
    "status": "active",
    "yes_bid_dollars": "0.4400",
    "yes_ask_dollars": "0.4500",
    "no_bid_dollars": "0.5500",
    "no_ask_dollars": "0.5600",
    "last_price_dollars": "0.4500",
    "volume_fp": "10369.92",
    "volume_24h_fp": "12.50",
    "open_interest_fp": "6579.63",
    "result": "",
    "rules_primary": "If the upper bound of the target federal funds rate\nis above 4.25%.",
    "rules_secondary": "Payouts follow the published criteria.",
    "created_time": "2025-08-06T14:29:01.961028Z",
    "open_time": "2025-10-13T14:00:00Z",
    "close_time": "2027-04-28T17:55:00Z",
}

#: A trade print the venue's own trade route serves. The count is fractional, which
#: the venue really publishes.
TRADE_RECORD = {
    "trade_id": "48ea409d-04c6-4de5-8de8-211888255e16",
    "ticker": "KXFED-27APR-T4.25",
    "count_fp": "13.75",
    "yes_price_dollars": "0.4500",
    "no_price_dollars": "0.5500",
    "taker_side": "yes",
    "taker_outcome_side": "yes",
    "created_time": "2026-06-17T13:35:44.248896Z",
}


# --- Two-source authority -----------------------------------------------------


def _config(tmp_path: pathlib.Path) -> pathlib.Path:
    """A pipeline configuration declaring two market layers and nothing else."""
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        "inputs:\n"
        "  root: archive\n"
        "  layers:\n"
        "    - name: kalshi_trades\n"
        "      path_pattern: kalshi-trades/trades-*.parquet\n"
        "      input_class: external_historical_archive\n"
        "      role: source_trades\n"
        "    - name: kalshi_markets\n"
        "      path_pattern: kalshi-trades/markets-*.parquet\n"
        "      input_class: external_historical_archive\n"
        "      role: market_metadata_snapshot\n"
        "    - name: kalshi_own_markets\n"
        "      path_pattern: kalshi-own/markets/markets-*.parquet\n"
        "      input_class: locally_captured_public_data\n"
        "      role: market_metadata_snapshot\n",
        encoding="utf-8",
    )
    return path


def _write_markets(
    root: pathlib.Path,
    pattern_dir: str,
    name: str,
    rows: list[tuple[str, dt.datetime, dt.datetime]],
) -> pathlib.Path:
    target = root / pattern_dir / name
    target.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [{"ticker": t, "open_time": o, "close_time": c} for t, o, c in rows],
        schema=_MARKET_SCHEMA,
    )
    pq.write_table(table, target)
    return target


def _spanning(ticker: str = "KXFED-26FEB") -> list[tuple[str, dt.datetime, dt.datetime]]:
    """One contract whose listing interval reaches the test window."""
    return [(ticker, dt.datetime(2025, 12, 1, tzinfo=UTC), dt.datetime(2026, 3, 1, tzinfo=UTC))]


def _resolve(root: pathlib.Path, config: pathlib.Path, **overrides: object):
    options: dict[str, object] = {
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
        "series": SERIES,
        "config_path": config,
    }
    options.update(overrides)
    return resolve_market_layer(root, **options)  # type: ignore[arg-type]


def test_a_window_only_the_vendor_layer_covers_is_governed_by_it(
    tmp_path: pathlib.Path,
) -> None:
    config = _config(tmp_path)
    root = tmp_path / "archive"
    _write_markets(root, "kalshi-trades", "markets-0000.parquet", _spanning())

    resolution = _resolve(root, config)

    assert resolution.available is True
    assert resolution.layer == VENDOR_LAYER
    assert resolution.basis == BASIS_SOLE_COVERING_LAYER
    # The resolution reports what it saw on both layers, so a reader can tell a
    # governed window from one resolved by an absent pattern.
    by_layer = {coverage.layer: coverage for coverage in resolution.coverages}
    assert by_layer[VENDOR_LAYER].shard_count == 1
    assert by_layer[OWN_LAYER].shard_count == 0


def test_a_window_only_our_own_capture_covers_is_governed_by_it(
    tmp_path: pathlib.Path,
) -> None:
    config = _config(tmp_path)
    root = tmp_path / "archive"
    _write_markets(root, "kalshi-own/markets", "markets-20260201T000000Z.parquet", _spanning())

    resolution = _resolve(root, config)

    assert resolution.available is True
    assert resolution.layer == OWN_LAYER
    assert resolution.basis == BASIS_SOLE_COVERING_LAYER


def test_a_window_both_layers_cover_is_refused_and_names_the_shared_contracts(
    tmp_path: pathlib.Path,
) -> None:
    """The double-count guard: one contract in both layers must refuse, not union."""
    config = _config(tmp_path)
    root = tmp_path / "archive"
    _write_markets(root, "kalshi-trades", "markets-0000.parquet", _spanning("KXFED-SHARED"))
    _write_markets(
        root,
        "kalshi-own/markets",
        "markets-20260201T000000Z.parquet",
        _spanning("KXFED-SHARED"),
    )

    resolution = _resolve(root, config)

    assert resolution.available is False
    assert resolution.layer is None
    assert resolution.basis == BASIS_BOTH_LAYERS_COVER_THE_WINDOW
    # The refusal names the contract that would have been counted twice, so the
    # reason is checkable rather than a restatement of the policy.
    assert resolution.overlapping_contracts == ("KXFED-SHARED",)


def test_naming_a_layer_overrides_the_refusal_and_records_the_divergence(
    tmp_path: pathlib.Path,
) -> None:
    config = _config(tmp_path)
    root = tmp_path / "archive"
    _write_markets(root, "kalshi-trades", "markets-0000.parquet", _spanning("KXFED-SHARED"))
    _write_markets(
        root,
        "kalshi-own/markets",
        "markets-20260201T000000Z.parquet",
        _spanning("KXFED-SHARED"),
    )

    resolution = _resolve(root, config, layer=OWN_LAYER)

    assert resolution.available is True
    assert resolution.layer == OWN_LAYER
    assert resolution.basis == BASIS_LAYER_NAMED_EXPLICITLY
    # Both coverages still travel, so the choice is visible as a choice.
    assert {coverage.layer for coverage in resolution.coverages if coverage.covers} == {
        VENDOR_LAYER,
        OWN_LAYER,
    }


def test_an_unknown_layer_name_is_refused_rather_than_ignored(
    tmp_path: pathlib.Path,
) -> None:
    config = _config(tmp_path)
    root = tmp_path / "archive"

    with pytest.raises(MarketCaptureError, match="not a declared market layer"):
        _resolve(root, config, layer="kalshi_somewhere_else")


def test_a_window_no_layer_covers_is_refused_rather_than_served_from_one_that_does_not(
    tmp_path: pathlib.Path,
) -> None:
    config = _config(tmp_path)
    root = tmp_path / "archive"
    # A layer holds a contract, but its listing interval does not reach the window.
    _write_markets(
        root,
        "kalshi-trades",
        "markets-0000.parquet",
        [("KXFED-25MAY", dt.datetime(2025, 4, 1, tzinfo=UTC), dt.datetime(2025, 5, 5, tzinfo=UTC))],
    )

    resolution = _resolve(root, config)

    assert resolution.available is False
    assert resolution.layer is None
    assert resolution.basis == BASIS_NO_LAYER_COVERS_THE_WINDOW
    # The reason states the shard counts, so a missing layer and a non-covering
    # layer are distinguishable rather than both reading as "nothing matched".
    assert "kalshi_markets=1 shard(s)" in resolution.reason


def test_an_absent_layer_is_not_treated_as_covering(tmp_path: pathlib.Path) -> None:
    """A fresh checkout has no capture yet; that must not refuse every window."""
    config = _config(tmp_path)
    root = tmp_path / "archive"
    _write_markets(root, "kalshi-trades", "markets-0000.parquet", _spanning())

    resolution = _resolve(root, config)

    assert resolution.available is True
    assert resolution.basis == BASIS_SOLE_COVERING_LAYER


def test_a_layer_outside_the_declared_series_is_not_coverage(
    tmp_path: pathlib.Path,
) -> None:
    """A listing row for another series must not make a layer look like it covers."""
    config = _config(tmp_path)
    root = tmp_path / "archive"
    _write_markets(
        root,
        "kalshi-trades",
        "markets-0000.parquet",
        [
            (
                "KXCPI-26JAN",
                dt.datetime(2025, 12, 1, tzinfo=UTC),
                dt.datetime(2026, 3, 1, tzinfo=UTC),
            )
        ],
    )

    resolution = _resolve(root, config)

    assert resolution.basis == BASIS_NO_LAYER_COVERS_THE_WINDOW


def test_the_declared_layers_are_discovered_from_the_configuration(tmp_path: pathlib.Path) -> None:
    config = _config(tmp_path)

    layers = declared_market_layers(config)

    # Only the two market layers are returned: the trade layer declares a different
    # role and must not reach the market-authority decision.
    assert layers == (
        (VENDOR_LAYER, "kalshi-trades/markets-*.parquet"),
        (OWN_LAYER, "kalshi-own/markets/markets-*.parquet"),
    )


def test_a_configuration_with_no_market_layer_is_refused(tmp_path: pathlib.Path) -> None:
    config = tmp_path / "empty.yaml"
    config.write_text(
        "inputs:\n"
        "  layers:\n"
        "    - name: kalshi_trades\n"
        "      path_pattern: kalshi-trades/trades-*.parquet\n"
        "      input_class: external_historical_archive\n"
        "      role: source_trades\n",
        encoding="utf-8",
    )

    with pytest.raises(MarketCaptureError, match="no layer with role"):
        declared_market_layers(config)


def test_a_window_ending_before_it_starts_is_refused(tmp_path: pathlib.Path) -> None:
    config = _config(tmp_path)
    root = tmp_path / "archive"

    with pytest.raises(MarketCaptureError, match="must not precede"):
        _resolve(root, config, window_start=WINDOW_END, window_end=WINDOW_START)


# --- The declared series ------------------------------------------------------


def test_the_capture_universe_comes_from_the_cohort_declaration(tmp_path: pathlib.Path) -> None:
    cohort = tmp_path / "cohort.yaml"
    cohort.write_text("policy_series: [KXFED, FEDDECISION]\n", encoding="utf-8")

    assert declared_series(cohort) == ("KXFED", "FEDDECISION")


def test_a_cohort_declaring_no_series_is_refused_rather_than_swept(tmp_path: pathlib.Path) -> None:
    cohort = tmp_path / "cohort.yaml"
    cohort.write_text("config_version: cohort_v2\n", encoding="utf-8")

    with pytest.raises(MarketCaptureError, match="declares no policy_series"):
        declared_series(cohort)


# --- Row construction ---------------------------------------------------------


def _clock_and_provenance():
    from market_propagation.domain import Clock, Provenance

    return (
        Clock.historical(dt.datetime(2026, 6, 17, 13, 35, 44, tzinfo=UTC)),
        Provenance(raw_hash="a" * 64, record_id="KXFED-27APR-T4.25.1", source="capture.kalshi"),
    )


def test_a_market_row_keeps_the_published_rule_text_verbatim() -> None:
    row = market_row_from_record(LISTING_RECORD)

    # The rule text is the evidence a later vintage check reads, so it is carried
    # exactly, newline included, rather than stripped or truncated.
    assert row["rules_primary"] == LISTING_RECORD["rules_primary"]
    assert "\n" in row["rules_primary"]
    assert row["rules_secondary"] == LISTING_RECORD["rules_secondary"]


def test_a_market_row_stores_fixed_point_dollars_as_exact_integer_cents() -> None:
    row = market_row_from_record(LISTING_RECORD)

    assert row["yes_bid"] == 44
    assert row["yes_ask"] == 45
    assert row["no_bid"] == 55
    assert row["last_price"] == 45


def test_a_fractional_activity_count_is_kept_exact_rather_than_rounded() -> None:
    """The venue publishes fractional counts; 10369.92 must not become 10370."""
    row = market_row_from_record(LISTING_RECORD)

    assert row["volume"] == Decimal("10369.92")
    assert row["volume_24h"] == Decimal("12.50")
    assert row["open_interest"] == Decimal("6579.63")


def test_an_unstated_count_is_null_rather_than_zero() -> None:
    """Zero is a value the venue did not state, and reads as agreement with one."""
    absent = {k: v for k, v in LISTING_RECORD.items() if not k.endswith("_fp")}

    row = market_row_from_record(absent)

    assert row["volume"] is None
    assert row["volume_24h"] is None
    assert row["open_interest"] is None


def test_a_sub_cent_price_is_refused_rather_than_rounded_to_a_whole_cent() -> None:
    record = {**LISTING_RECORD, "yes_bid_dollars": "0.4450"}

    with pytest.raises(MarketCaptureError, match="yes_bid_dollars"):
        market_row_from_record(record)


def test_a_market_record_without_a_ticker_is_refused() -> None:
    with pytest.raises(MarketCaptureError, match="ticker"):
        market_row_from_record({"status": "active"})


def test_a_trade_row_preserves_a_fractional_count_exactly() -> None:
    clock, provenance = _clock_and_provenance()

    row = trade_row_from_record(TRADE_RECORD, provenance=provenance, clock=clock)

    assert row["count"] == Decimal("13.75")
    assert row["yes_price"] == 45
    assert row["no_price"] == 55
    assert row["taker_side"] == "yes"


def test_a_trade_whose_side_fields_contradict_each_other_is_refused() -> None:
    """The venue stated two different sides for one print, so neither is trusted."""
    clock, provenance = _clock_and_provenance()
    contradictory = {**TRADE_RECORD, "taker_outcome_side": "no"}

    with pytest.raises(MarketCaptureError, match="side"):
        trade_row_from_record(contradictory, provenance=provenance, clock=clock)


def test_a_trade_without_a_price_is_refused_rather_than_zero_priced() -> None:
    clock, provenance = _clock_and_provenance()
    broken = {k: v for k, v in TRADE_RECORD.items() if k != "yes_price_dollars"}

    with pytest.raises(WireShapeError):
        trade_row_from_record(broken, provenance=provenance, clock=clock)


def test_a_trade_without_a_count_is_refused() -> None:
    """The refusal comes from the venue's own normalizer, which this path reuses."""
    clock, provenance = _clock_and_provenance()
    broken = {k: v for k, v in TRADE_RECORD.items() if k != "count_fp"}

    with pytest.raises(WireShapeError, match="count_fp"):
        trade_row_from_record(broken, provenance=provenance, clock=clock)


# --- The sealed shard layout --------------------------------------------------


def test_the_written_shards_carry_the_archive_layout_the_pipeline_reads(
    tmp_path: pathlib.Path,
) -> None:
    """The capture is only useful if the existing extraction path can read it."""
    clock, provenance = _clock_and_provenance()
    markets, trades = write_capture_shards(
        tmp_path,
        market_rows=[market_row_from_record(LISTING_RECORD)],
        trade_rows=[trade_row_from_record(TRADE_RECORD, provenance=provenance, clock=clock)],
        stamp="20260917T000000Z",
    )

    assert len(markets) == 1
    assert len(trades) == 1
    # The two kinds are in separate directories, so a market glob cannot silently
    # pick up trade rows.
    assert pathlib.Path(markets[0]).parent.name == "markets"
    assert pathlib.Path(trades[0]).parent.name == "trades"

    market_schema = pq.read_schema(markets[0])
    trade_schema = pq.read_schema(trades[0])
    assert [name for name, _ in MARKET_COLUMNS] == market_schema.names
    assert [name for name, _ in TRADE_COLUMNS] == trade_schema.names
    # The time column and its unit are what the extraction path dispatches on.
    assert market_schema.field("created_time").type == pa.timestamp("us", tz="UTC")
    assert trade_schema.field("created_time").type == pa.timestamp("us", tz="UTC")
    # Prices are integer cents because the archive mapper requires integers, and the
    # count is exact decimal because the venue really publishes fractional ones.
    assert trade_schema.field("yes_price").type == pa.int64()
    assert not pa.types.is_integer(trade_schema.field("count").type)


def test_rewriting_the_same_rows_produces_a_byte_identical_shard(tmp_path: pathlib.Path) -> None:
    """A re-capture of an unchanged window adds no second copy of the same rows."""
    from market_propagation.storage import hash_file

    market = market_row_from_record(LISTING_RECORD)
    first, _ = write_capture_shards(
        tmp_path / "one", market_rows=[market], trade_rows=[], stamp="20260917T000000Z"
    )
    second, _ = write_capture_shards(
        tmp_path / "two", market_rows=[market], trade_rows=[], stamp="20260917T000000Z"
    )

    assert hash_file(first[0]) == hash_file(second[0])
