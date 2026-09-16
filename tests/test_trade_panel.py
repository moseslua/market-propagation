"""Acceptance tests for the source-time transaction panel.

The panel's job is to refuse the substitutions that would quietly turn a missing
observation into a measurement. Each test here defends one of them:

* An event whose contract never traded again is ``no_post_release_trade`` with a
  null response. Carrying the baseline forward would report a zero the archive
  does not contain, and the tests assert the reason holds for every horizon.
* A zero response is only real when two distinct valid prints at the same price
  bracket the release, which is a different fact from no print at all.
* Rule evidence and lifecycle are not waived by observed prices. A row whose rule
  version is unknown or whose contract closed before publication stays invalid,
  and the closed case gets a null response rather than a structural zero.

A masked row keeps the prices it genuinely observed: masking applies to the
estimand, so ``response`` is null while a pre-release price the tape holds stays
in the panel for the plan's missingness diagnostics. The remaining tests cover the
tie-group aggregation, the ``usable`` clock mode that must never masquerade as
``source``, the seal round trip through storage, and the release loader that
supplies the events a panel is built over.
"""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from market_propagation.domain import UTC, Clock, HistoricalTrade, Provenance
from market_propagation.storage import TRADE_PANEL_COLUMNS, read_parquet, write_parquet
from market_propagation.trade_panel import (
    CLOCK_MODES,
    DEFAULT_COHORT,
    SOURCE_AVAILABILITY_STATUS,
    USABLE_AVAILABILITY_STATUS,
    EventSpec,
    PanelSettings,
    TradePanel,
    build_trade_panel,
    event_spec_from_row,
    load_event_specs,
    load_panel_settings,
)

EVENT_TIME = dt.datetime(2025, 3, 12, 12, 30, 0, tzinfo=UTC)
CONTRACT = "KXCPI-26SEP-T0.4"
SHARD_HASH = "c7a2" * 16
PRIMARY = 300


def at(seconds: float) -> dt.datetime:
    return EVENT_TIME + dt.timedelta(seconds=seconds)


def _event(**overrides: object) -> EventSpec:
    fields: dict[str, object] = {
        "event_id": "cpi-2025-02",
        "cluster_id": "cpi-2025-02",
        "family": "cpi",
        "event_time": EVENT_TIME,
        "rule_version": "rules-2025-02-11",
        "rule_evidence_quality": "archived_public_rule",
    }
    fields.update(overrides)
    return EventSpec(**fields)  # type: ignore[arg-type]


def _print(
    offset: float,
    axis_price: str,
    occurrence: str,
    *,
    size: str = "4",
    size_quality: str = "verified_source_quantity",
    contract_id: str = CONTRACT,
    event_axis: str | None = "yes_price_is_event_axis",
) -> HistoricalTrade:
    """One Kalshi-shaped print at ``offset`` seconds from the release.

    ``axis_price`` is the event-axis price. ``event_axis=None`` produces a print
    with no documented axis at all, which is how an unprojectable row reaches the
    panel.
    """
    price = Decimal(axis_price)
    return HistoricalTrade(
        venue="kalshi",
        contract_id=contract_id,
        trade_id=occurrence,
        price=price,
        raw_price_units="cents",
        price_precision="exact_integer_cents",
        raw_price=price * 100,
        secondary_price=(1 - price) * 100,
        direction="yes",
        event_direction=1,
        event_price=None if event_axis is None else price,
        event_axis=event_axis,
        size=Decimal(size),
        size_quality=size_quality,
        clock=Clock.historical(at(offset)),
        provenance=Provenance(
            raw_hash=SHARD_HASH,
            record_id=f"{SHARD_HASH}:{occurrence}",
            source="kalshi-trades",
        ),
    )


def _polymarket_print(offset: float, axis_price: str, occurrence: str) -> HistoricalTrade:
    """A cleaned-layer print whose quantity the archive does not carry."""
    price = Decimal(axis_price)
    return HistoricalTrade(
        venue="polymarket",
        contract_id="0xcondition",
        token_id="7788",
        outcome_seq=1,
        price=price,
        raw_price_units="dollars",
        price_precision="float64_source_precision",
        raw_price=price,
        event_price=price,
        event_axis="outcome_seq_1_is_price",
        direction="BUY",
        event_direction=1,
        size=None,
        size_quality="unavailable_in_cleaned_layer",
        clock=Clock.historical(at(offset)),
        provenance=Provenance(
            raw_hash=SHARD_HASH,
            record_id=f"{SHARD_HASH}:{occurrence}",
            source="polymarket-daily-aligned",
        ),
    )


def _settings(**overrides: object) -> PanelSettings:
    return PanelSettings(**overrides)  # type: ignore[arg-type]


def _row(panel: TradePanel, *, horizon: int = PRIMARY, contract_id: str = CONTRACT) -> dict:
    matches = [
        row
        for row in panel.rows
        if row["horizon_seconds"] == horizon and row["contract_id"] == contract_id
    ]
    assert len(matches) == 1, f"expected one row for {contract_id} at horizon {horizon}"
    return matches[0]


def test_every_row_carries_exactly_the_declared_panel_columns() -> None:
    """The declared column list is the only column list, and no quote column exists."""
    panel = build_trade_panel(
        [_print(-60, "0.42", "a"), _print(240, "0.50", "b")],
        [_event()],
        settings=_settings(),
    )
    assert panel.rows
    for row in panel.rows:
        assert list(row) == list(TRADE_PANEL_COLUMNS)
    declared = set(TRADE_PANEL_COLUMNS)
    for quote_only in ("bid", "ask", "spread", "depth", "bid_before", "ask_after"):
        assert quote_only not in declared


def test_missing_baseline_is_named_rather_than_defaulted() -> None:
    """An event whose contract only traded after the release has no baseline."""
    panel = build_trade_panel(
        [_print(240, "0.55", "post")],
        [_event()],
        settings=_settings(),
    )
    row = _row(panel)
    assert row["baseline_source_time"] is None
    assert row["baseline"] is None
    assert row["response"] is None
    assert row["valid"] is False
    assert row["exclusion_reason"] == "missing_baseline"
    assert row["post_release_trade_observed"] is True
    assert "candidate_from_post_release_activity_only" in row["flags_json"]["flags"]
    # Candidacy rested on post-release activity alone, which the counts report
    # separately because post-event activity must not select the universe.
    assert panel.counts["overall"]["candidate_pairs_from_post_release_activity_only"] == 1


def test_no_new_print_is_not_a_zero() -> None:
    """Only a pre-release print exists, so the endpoint is unobserved, never zero."""
    panel = build_trade_panel([_print(-60, "0.42", "only")], [_event()], settings=_settings())
    row = _row(panel)
    assert row["exclusion_reason"] == "no_post_release_trade"
    assert row["post_release_trade_observed"] is False
    assert row["valid"] is False
    assert row["response"] is None
    assert row["response"] != 0
    assert row["endpoint"] is None
    assert row["endpoint_source_time"] is None
    # The observed pre-release price stays in the panel; the response does not.
    assert row["baseline"] == pytest.approx(0.42)
    assert row["baseline_age_seconds"] == pytest.approx(60.0)


def test_no_new_print_is_reported_for_every_horizon() -> None:
    panel = build_trade_panel([_print(-60, "0.42", "only")], [_event()], settings=_settings())
    horizons = {row["horizon_seconds"]: row for row in panel.rows}
    assert sorted(horizons) == [60, 300, 900, 1800, 3600]
    assert all(row["response"] is None for row in horizons.values())
    assert all(row["exclusion_reason"] == "no_post_release_trade" for row in horizons.values())
    # Five horizons, one pair: the pair counts must not count rows.
    overall = panel.counts["overall"]
    assert overall["candidate_pairs"] == 1
    assert overall["rows"] == 5
    assert overall["endpoint_observed_pairs"] == 0


def test_endpoint_beyond_the_cap_is_masked_with_a_null_response() -> None:
    """A print exists after the release but is too stale to be the endpoint."""
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(30, "0.50", "stale")],
        [_event()],
        settings=_settings(endpoint_max_age_seconds=120),
    )
    row = _row(panel)
    assert row["post_release_trade_observed"] is True
    assert row["endpoint_age_seconds"] == pytest.approx(270.0)
    assert row["exclusion_reason"] == "endpoint_beyond_cap"
    assert row["valid"] is False
    assert row["response"] is None
    # Both legs were observed, so both prices stay visible beside the mask.
    assert row["baseline"] == pytest.approx(0.42)
    assert row["endpoint"] == pytest.approx(0.50)


def test_horizon_past_the_declared_post_window_is_masked_specifically() -> None:
    """A horizon the observation window does not cover is not a measured endpoint."""
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(250, "0.50", "post")],
        [_event()],
        settings=_settings(
            post_window_seconds=900, horizons_seconds=(300, 1800), primary_horizon_seconds=300
        ),
    )
    row = _row(panel, horizon=1800)
    assert row["exclusion_reason"] == "endpoint_beyond_cap"
    assert "horizon_beyond_declared_post_window" in row["flags_json"]["flags"]
    assert row["valid"] is False
    assert row["response"] is None
    # The horizon the window does cover is still measured.
    inside = _row(panel, horizon=300)
    assert inside["valid"] is True
    assert inside["response"] == pytest.approx(0.08)


def test_baseline_beyond_the_cap_is_distinct_from_a_missing_baseline() -> None:
    """A pre-release print outside the cap is a different fact from none at all."""
    panel = build_trade_panel(
        [_print(-300, "0.42", "old"), _print(240, "0.50", "post")],
        [_event()],
        settings=_settings(pre_window_seconds=1800, baseline_max_age_seconds=120),
    )
    row = _row(panel)
    assert row["baseline_source_time"] is not None
    assert row["baseline_age_seconds"] == pytest.approx(300.0)
    assert row["exclusion_reason"] == "baseline_beyond_cap"
    assert row["valid"] is False
    assert row["response"] is None
    # The price was observed even though it is too old to anchor the baseline.
    assert row["baseline"] == pytest.approx(0.42)


def test_two_distinct_prints_at_one_price_are_a_genuine_observed_zero() -> None:
    """Two valid prints at the same price bracket the release, so the zero is measured."""
    panel = build_trade_panel(
        [_print(-45, "0.42", "before"), _print(285, "0.42", "after")],
        [_event()],
        settings=_settings(),
    )
    row = _row(panel)
    assert row["valid"] is True
    assert row["exclusion_reason"] is None
    assert row["post_release_trade_observed"] is True
    assert row["response"] == pytest.approx(0.0)
    assert row["baseline"] == pytest.approx(0.42)
    assert row["endpoint"] == pytest.approx(0.42)
    locators = row["provenance_locators_json"]
    assert locators["baseline"]["occurrences"] != locators["endpoint"]["occurrences"]


def test_tie_group_is_the_unweighted_mean_with_a_retained_envelope() -> None:
    """Prints sharing the finest timestamp become one observation at their mean."""
    prints = [
        _print(-30, "0.30", "base"),
        _print(100, "0.10", "earlier"),
        _print(290, "0.30", "tie-1"),
        _print(290, "0.40", "tie-2"),
        _print(290, "0.50", "tie-3"),
    ]
    panel = build_trade_panel(prints, [_event()], settings=_settings())
    row = _row(panel)
    assert row["valid"] is True
    assert row["endpoint"] == pytest.approx(0.40)
    assert row["response"] == pytest.approx(0.10)
    assert row["tie_group_size"] == 3
    # The envelope is taken against the same baseline the point estimate uses.
    assert row["tie_group_response_min"] == pytest.approx(0.0)
    assert row["tie_group_response_max"] == pytest.approx(0.20)
    assert row["endpoint_envelope_low"] == pytest.approx(0.0)
    assert row["endpoint_envelope_high"] == pytest.approx(0.20)
    # The earlier distinct timestamp stays outside the group, but is still counted.
    assert row["endpoint_trade_count"] == 4
    assert "endpoint_tie_group_aggregated" in row["flags_json"]["flags"]


def test_tie_group_selects_the_latest_qualifying_timestamp() -> None:
    """A later timestamp outranks an earlier one, and only the tie at it is grouped."""
    prints = [
        _print(-30, "0.30", "base"),
        _print(200, "0.20", "earlier"),
        _print(290, "0.60", "tie-1"),
        _print(290, "0.60", "tie-2"),
    ]
    panel = build_trade_panel(prints, [_event()], settings=_settings())
    row = _row(panel)
    assert row["endpoint"] == pytest.approx(0.60)
    assert row["tie_group_size"] == 2
    assert row["endpoint_source_time"] == at(290)


def test_envelope_is_suppressed_when_settings_say_so() -> None:
    """Suppressing the envelope must null it rather than leave a stale value."""
    prints = [
        _print(-30, "0.30", "base"),
        _print(290, "0.30", "tie-1"),
        _print(290, "0.50", "tie-2"),
    ]
    panel = build_trade_panel(
        prints, [_event()], settings=_settings(report_endpoint_envelope=False)
    )
    row = _row(panel)
    assert row["endpoint_envelope_low"] is None
    assert row["endpoint_envelope_high"] is None
    assert row["tie_group_size"] == 2


def test_closed_contract_yields_a_null_response_and_not_zero() -> None:
    """A direct contract that settled before publication cannot answer the release."""
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(300, "0.42", "post")],
        [_event(closed_before_release=True)],
        settings=_settings(),
    )
    row = _row(panel)
    assert row["exclusion_reason"] == "contract_closed_before_release"
    assert row["valid"] is False
    assert row["response"] is None
    assert row["response"] != 0
    # The reason is about the response. The observed legs stay in the panel.
    assert row["baseline"] == pytest.approx(0.42)
    assert row["endpoint"] == pytest.approx(0.42)
    assert panel.counts["overall"]["lifecycle_eligible_pairs"] == 0


def test_unknown_rule_version_is_not_waived_by_observed_prices() -> None:
    """Both legs are observed, and the row is still invalid."""
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(300, "0.50", "post")],
        [_event(rule_version=None)],
        settings=_settings(),
    )
    row = _row(panel)
    assert row["post_release_trade_observed"] is True
    assert row["baseline_source_time"] is not None
    assert row["exclusion_reason"] == "rule_version_unknown"
    assert row["valid"] is False
    assert row["response"] is None
    assert panel.counts["overall"]["rule_verified_pairs"] == 0


def test_missing_rule_evidence_is_a_distinct_reason() -> None:
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(300, "0.50", "post")],
        [_event(rule_evidence_quality=None, rule_version=None)],
        settings=_settings(),
    )
    row = _row(panel)
    assert row["exclusion_reason"] == "rule_evidence_missing"
    assert row["valid"] is False
    assert row["response"] is None


def test_ambiguous_outcome_axis_is_invalid() -> None:
    """A print with no documented event axis has no place on the event axis."""
    axisless = _print(-60, "0.42", "base", event_axis=None)
    post = _print(300, "0.50", "post")
    panel = build_trade_panel([axisless, post], [_event()], settings=_settings())
    row = _row(panel)
    assert row["exclusion_reason"] == "ambiguous_outcome_axis"
    assert row["valid"] is False
    assert row["response"] is None
    assert row["event_axis"] is None
    # An unprojectable price is not averaged into a partial one.
    assert row["baseline"] is None


def test_usable_clock_mode_never_becomes_source() -> None:
    """A usable fold has no interval to read, so no row claims one."""
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(300, "0.50", "post")],
        [_event()],
        settings=_settings(),
        clock_mode="usable",
    )
    assert panel.clock_mode == "usable"
    assert {row["clock_mode"] for row in panel.rows} == {"usable"}
    assert {row["availability_status"] for row in panel.rows} == {USABLE_AVAILABILITY_STATUS}
    assert SOURCE_AVAILABILITY_STATUS not in {row["availability_status"] for row in panel.rows}
    row = _row(panel)
    assert row["exclusion_reason"] == "availability_unidentifiable"
    assert row["valid"] is False
    assert row["response"] is None
    assert row["label_time_basis"] is None
    assert "usable_time_unavailable_no_interval_established" in panel.flags
    # No interval is invented on any row, so no usable time appears anywhere.
    assert all(row["label_time_basis"] is None for row in panel.rows)


def test_source_clock_mode_records_source_time_only() -> None:
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(300, "0.50", "post")],
        [_event()],
        settings=_settings(),
    )
    row = _row(panel)
    assert row["availability_status"] == SOURCE_AVAILABILITY_STATUS
    assert row["label_time_basis"] == "retrospective_source_time_alignment"
    assert row["baseline_time_basis"] == "source_time"
    assert row["valid"] is True


def test_assumed_delay_records_the_declared_delay() -> None:
    """The delay scenario is an explicit assumption, and it is recorded as one."""
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(240, "0.50", "post")],
        [_event()],
        settings=_settings(assumed_delay_seconds=45),
        clock_mode="assumed_delay",
    )
    row = _row(panel)
    assert row["flags_json"]["assumed_delay_seconds"] == 45
    assert row["baseline_time_basis"] == "source_time_plus_assumed_delay"
    assert row["label_time_basis"] == "assumption_conditional_source_time_plus_assumed_delay"
    assert "assumed_delay_45_seconds" in panel.flags


def test_assumed_delay_requires_a_declared_delay() -> None:
    with pytest.raises(ValueError, match="declared delay"):
        build_trade_panel(
            [_print(-60, "0.42", "base")],
            [_event()],
            settings=_settings(),
            clock_mode="assumed_delay",
        )


def test_unknown_clock_mode_is_refused() -> None:
    with pytest.raises(ValueError, match="clock_mode"):
        build_trade_panel(
            [_print(-60, "0.42", "base")], [_event()], settings=_settings(), clock_mode="replay"
        )
    assert CLOCK_MODES == ("source", "usable", "assumed_delay")


def test_counts_separate_the_pair_classes_and_report_missingness() -> None:
    """Counts are measured against the preselected candidate universe."""
    prints = [
        _print(-45, "0.42", "obs-base", contract_id="KXCPI-A"),
        _print(285, "0.44", "obs-post", contract_id="KXCPI-A"),
        _print(-45, "0.60", "quiet", contract_id="KXCPI-B"),
    ]
    events = [_event(), _event(event_id="cpi-2025-03", cluster_id="cpi-2025-03")]
    panel = build_trade_panel(prints, events, settings=_settings())
    overall = panel.counts["overall"]
    # Two contracts answer two releases, so four candidate pairs and twenty rows.
    assert overall["candidate_pairs"] == 4
    assert overall["rows"] == 20
    assert overall["pre_event_observed_pairs"] == 4
    assert overall["rule_verified_pairs"] == 4
    assert overall["lifecycle_eligible_pairs"] == 4
    assert overall["baseline_observed_pairs"] == 4
    # One of the two contracts has a post-release print, in each event.
    assert overall["endpoint_observed_pairs"] == 2
    assert overall["distinct_release_clusters"] == 2
    assert overall["release_clusters"] == ["cpi-2025-02", "cpi-2025-03"]
    # The fractions are measured at the primary horizon, one row per pair.
    assert overall["primary_horizon_rows"] == 4
    assert overall["primary_horizon_endpoint_observed_rows"] == 2
    assert overall["primary_horizon_valid_rows"] == 2
    assert overall["observed_fraction"] == pytest.approx(0.5)
    assert overall["missing_fraction"] == pytest.approx(0.5)
    # Twenty rows, of which only the two primary-horizon rows for KXCPI-A are valid.
    assert overall["valid_rows"] == 2
    assert overall["invalid_rows"] == 18
    # KXCPI-B has five no-trade horizons per event and KXCPI-A one, at horizon 60.
    assert overall["exclusion_reason_counts"]["no_post_release_trade"] == 12
    assert overall["exclusion_reason_counts"]["endpoint_beyond_cap"] == 6
    assert panel.counts["cohort"] == DEFAULT_COHORT
    assert (
        panel.counts["preselected_denominator"] == "candidate_pairs_in_declared_observation_window"
    )
    assert sorted(panel.counts["by_event"]) == ["cpi-2025-02", "cpi-2025-03"]
    assert panel.counts["by_event"]["cpi-2025-02"]["family"] == "cpi"
    assert panel.counts["by_event"]["cpi-2025-02"]["candidate_pairs"] == 2


def test_declared_candidate_grid_keeps_a_pair_that_never_traded() -> None:
    """A declared candidate with no window trade is a missing cell, not an absent pair."""
    prints = [_print(-45, "0.42", "traded", contract_id="KXCPI-A")]
    panel = build_trade_panel(
        prints,
        [_event()],
        settings=_settings(),
        candidates={"cpi-2025-02": [("kalshi", "KXCPI-A"), ("kalshi", "KXCPI-B")]},
    )
    assert {row["contract_id"] for row in panel.rows} == {"KXCPI-A", "KXCPI-B"}
    quiet = _row(panel, contract_id="KXCPI-B")
    assert quiet["baseline"] is None
    assert quiet["endpoint"] is None
    assert quiet["valid"] is False
    assert quiet["exclusion_reason"] == "missing_baseline"
    overall = panel.counts["overall"]
    assert overall["candidate_pairs"] == 2
    assert overall["declared_candidate_pairs"] == 2
    assert overall["pairs_without_any_window_trade"] == 1
    assert panel.counts["candidate_universe"] == "declared_listing_grid"
    assert "candidate_universe_from_declared_listing_grid" in panel.flags


def test_declared_candidate_grid_refuses_an_event_it_does_not_cover() -> None:
    """Falling back to window activity for a missing event would silently reselect."""
    with pytest.raises(ValueError, match="cpi-2025-02"):
        build_trade_panel(
            [_print(-45, "0.42", "traded")],
            [_event()],
            settings=_settings(),
            candidates={"cpi-2025-09": [("kalshi", CONTRACT)]},
        )


def test_activity_selected_universe_is_labelled_as_such() -> None:
    """Without a declared grid the panel says the universe came from activity."""
    panel = build_trade_panel([_print(-45, "0.42", "traded")], [_event()], settings=_settings())
    assert panel.counts["candidate_universe"] == "window_activity"
    assert "candidate_universe_from_window_activity" in panel.flags
    assert panel.counts["overall"]["declared_candidate_pairs"] == 0


def test_missingness_fraction_is_null_and_not_zero_without_a_denominator() -> None:
    """A panel with no candidate pair reports no fraction rather than 0.0."""
    quiet = _print(-60, "0.42", "far-outside-window", contract_id="KXCPI-QUIET")
    panel = build_trade_panel([quiet], [_event()], settings=_settings(pre_window_seconds=10))
    assert panel.rows == ()
    overall = panel.counts["overall"]
    assert overall["candidate_pairs"] == 0
    assert overall["primary_horizon_rows"] == 0
    assert overall["observed_fraction"] is None
    assert overall["missing_fraction"] is None
    assert "no_candidate_pairs" in panel.flags
    assert "primary_horizon_not_reported" in panel.flags


def test_undated_prints_are_reported_rather_than_placed() -> None:
    """A print with no source time cannot sit on the transaction axis at all."""
    undated = _print(-60, "0.42", "undated")
    object.__setattr__(undated, "clock", Clock.historical(None))
    panel = build_trade_panel(
        [undated, _print(300, "0.50", "post")], [_event()], settings=_settings()
    )
    assert "undated_prints_not_placeable_on_the_transaction_axis" in panel.flags
    row = _row(panel)
    assert row["exclusion_reason"] == "missing_baseline"


def test_panel_seals_and_reads_back_through_storage(tmp_path: Path) -> None:
    """The sealed panel round trips, and a re-seal of the same content is the same dataset."""
    prints = [
        _print(-45, "0.42", "base"),
        _print(285, "0.44", "post"),
        _polymarket_print(200, "0.30", "poly"),
    ]
    panel = build_trade_panel(prints, [_event()], settings=_settings())
    path = tmp_path / "trade_panel.parquet"
    reference = panel.write(path)

    assert reference.table == "trade_panel"
    assert reference.row_count == len(panel.rows)
    assert reference.coverage_epoch == panel.coverage_epoch

    frame = read_parquet(path, table="trade_panel")
    assert list(frame.columns) == list(TRADE_PANEL_COLUMNS)
    assert len(frame) == len(panel.rows)

    def selected(contract_id: str, horizon: int) -> pd.Series:
        rows = frame[(frame["contract_id"] == contract_id) & (frame["horizon_seconds"] == horizon)]
        assert len(rows) == 1
        return rows.iloc[0]

    kalshi = selected(CONTRACT, PRIMARY)
    assert bool(kalshi["valid"]) is True
    assert kalshi["clock_mode"] == "source"
    assert kalshi["availability_status"] == SOURCE_AVAILABILITY_STATUS
    assert kalshi["response"] == pytest.approx(0.02)
    # A JSON column comes back as the object it was encoded from.
    assert kalshi["flags_json"]["flags"] == []
    assert kalshi["provenance_locators_json"]["endpoint"]["occurrences"] == [f"{SHARD_HASH}:post"]
    assert "clock_mode_source" in panel.flags

    # The cleaned layer carries no quantity, so the row says so and never invents one.
    polymarket = selected("0xcondition", PRIMARY)
    assert polymarket["size_quality"] == "unavailable_in_cleaned_layer"
    assert bool(polymarket["size_verified"]) is False
    assert polymarket["exclusion_reason"] == "missing_baseline"

    # A masked row's response stays null through the round trip: a null is not a zero.
    invalid = frame[frame["valid"] == False]  # noqa: E712 - a nullable Arrow bool column
    assert len(invalid) > 0
    assert invalid["response"].isna().all()
    assert frame[frame["response"].isna()]["exclusion_reason"].notna().all()

    second = panel.write(path)
    assert second.content_hash == reference.content_hash
    assert second.manifest == reference.manifest


def test_sealed_panel_keeps_json_columns_reparseable(tmp_path: Path) -> None:
    """Structured evidence round trips as an object with the locators it recorded."""
    panel = build_trade_panel(
        [_print(-60, "0.42", "base"), _print(300, "0.50", "post")],
        [_event()],
        settings=_settings(),
    )
    path = tmp_path / "trade_panel.parquet"
    panel.write(path)
    frame = read_parquet(path, table="trade_panel")
    row = frame[(frame["horizon_seconds"] == PRIMARY) & (frame["valid"] == True)].iloc[0]  # noqa: E712
    locators = row["provenance_locators_json"]
    assert isinstance(locators, dict)
    assert locators["endpoint"]["occurrences"] == [f"{SHARD_HASH}:post"]
    assert json.dumps(locators)  # the decoded value is re-encodable, so nothing was lost


def test_settings_come_from_the_pipeline_configuration() -> None:
    """The measurement window is read from the configuration, not redefined here."""
    settings = load_panel_settings("configs/external_history_v1.yaml")
    assert settings.pre_window_seconds == 1800
    assert settings.post_window_seconds == 3600
    assert settings.baseline_max_age_seconds == 120
    assert settings.endpoint_max_age_seconds == 120
    assert settings.horizons_seconds == (60, 300, 900, 1800, 3600)
    assert settings.primary_horizon_seconds == 300
    assert settings.require_post_release_trade is True
    assert settings.report_endpoint_envelope is True
    # The configured clock mode is `source`, so no delay is declared.
    assert settings.assumed_delay_seconds is None


def test_settings_refuse_a_configuration_without_a_response_block() -> None:
    with pytest.raises(ValueError, match="response"):
        PanelSettings.from_config({"clock": {"mode": "source"}})


def test_settings_reject_a_declared_delay_of_zero() -> None:
    with pytest.raises(ValueError, match="assumed_delay_seconds"):
        PanelSettings(assumed_delay_seconds=0)


def test_panel_rejects_a_row_that_does_not_match_the_declared_columns() -> None:
    with pytest.raises(ValueError, match="declared trade panel columns"):
        TradePanel(
            rows=({"event_id": "cpi-2025-02"},),
            clock_mode="source",
            cohort=DEFAULT_COHORT,
            settings=_settings(),
            counts={},
            flags=(),
        )


def test_constructor_rejects_rows_that_are_not_historical_trades() -> None:
    """A row mapping is a stored shape, not an input: it carries no clock or provenance."""
    with pytest.raises(TypeError, match="HistoricalTrade"):
        build_trade_panel(
            [{"venue": "kalshi", "contract_id": CONTRACT}],  # type: ignore[list-item]
            [_event()],
            settings=_settings(),
        )


def _release_row(**overrides: object) -> dict[str, object]:
    # Both declared JSON columns are populated. A null JSON column is not what this
    # test is about, and the real archived dataset fills them.
    row: dict[str, object] = {
        "event_id": "cpi_2025_01",
        "family": "cpi",
        "scheduled_at": dt.datetime(2025, 1, 15, 13, 30, tzinfo=UTC),
        "reference_period": "2024-12",
        "values_json": "{}",
        "revisions_json": "[]",
        "raw_hash": SHARD_HASH,
    }
    row.update(overrides)
    return row


def _seal_releases(rows: list[dict[str, object]], path: Path) -> Path:
    write_parquet(rows, path, table="releases", coverage_epoch="test_fixture")
    return path


def test_event_specs_are_built_from_a_sealed_release_dataset(tmp_path: Path) -> None:
    """The loader verifies the sealed dataset and returns one spec per release."""
    path = _seal_releases(
        [
            _release_row(),
            _release_row(
                event_id="empsit_2025_01",
                family="employment",
                scheduled_at=dt.datetime(2025, 1, 10, 13, 30, tzinfo=UTC),
            ),
        ],
        tmp_path / "releases.parquet",
    )
    specs = load_event_specs(path)
    assert [spec.event_id for spec in specs] == ["empsit_2025_01", "cpi_2025_01"]
    assert [spec.family for spec in specs] == ["employment", "cpi"]
    # One release is one cluster: the release is the equal-weight aggregation unit.
    assert [spec.cluster_id for spec in specs] == [spec.event_id for spec in specs]
    assert specs[1].event_time == dt.datetime(2025, 1, 15, 13, 30, tzinfo=UTC)


def test_release_dataset_without_rule_evidence_still_yields_specs(tmp_path: Path) -> None:
    """Absent rule evidence is a normal state, and the specs say the evidence is absent."""
    path = _seal_releases([_release_row()], tmp_path / "releases.parquet")
    specs = load_event_specs(path)
    assert len(specs) == 1
    assert specs[0].rule_version is None
    assert specs[0].rule_evidence_quality is not None
    assert "absent" in specs[0].rule_evidence_quality
    assert specs[0].closed_before_release is False


def test_absent_rule_evidence_masks_the_rows_the_panel_builds(tmp_path: Path) -> None:
    """The loader's absent evidence reaches the panel as a mask, not as an admission."""
    path = _seal_releases([_release_row()], tmp_path / "releases.parquet")
    spec = load_event_specs(path)[0]
    trades = [
        HistoricalTrade(
            venue="kalshi",
            contract_id="KXCPI-25JAN-T0.4",
            trade_id="base",
            price=Decimal("0.42"),
            raw_price_units="cents",
            price_precision="exact_integer_cents",
            raw_price=Decimal("42"),
            event_price=Decimal("0.42"),
            event_axis="yes_price_is_event_axis",
            direction="yes",
            event_direction=1,
            size=Decimal("3"),
            size_quality="verified_source_quantity",
            clock=Clock.historical(spec.event_time - dt.timedelta(seconds=60)),
            provenance=Provenance(SHARD_HASH, f"{SHARD_HASH}:base", "kalshi-trades"),
        ),
        HistoricalTrade(
            venue="kalshi",
            contract_id="KXCPI-25JAN-T0.4",
            trade_id="post",
            price=Decimal("0.50"),
            raw_price_units="cents",
            price_precision="exact_integer_cents",
            raw_price=Decimal("50"),
            event_price=Decimal("0.50"),
            event_axis="yes_price_is_event_axis",
            direction="yes",
            event_direction=1,
            size=Decimal("3"),
            size_quality="verified_source_quantity",
            clock=Clock.historical(spec.event_time + dt.timedelta(seconds=120)),
            provenance=Provenance(SHARD_HASH, f"{SHARD_HASH}:post", "kalshi-trades"),
        ),
    ]
    panel = build_trade_panel(trades, [spec], settings=_settings())
    row = _row(panel, contract_id="KXCPI-25JAN-T0.4")
    assert row["rule_version"] is None
    assert row["exclusion_reason"] == "rule_version_unknown"
    assert row["valid"] is False
    assert row["response"] is None


def test_rule_evidence_supplies_a_version_for_a_named_event(tmp_path: Path) -> None:
    path = _seal_releases([_release_row()], tmp_path / "releases.parquet")
    evidence = tmp_path / "rule_evidence.json"
    evidence.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "event_id": "cpi_2025_01",
                        "rule_version": "rules-2025-01-05",
                        "rule_evidence_quality": "archived_rule_text",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    spec = load_event_specs(path, rule_evidence_path=evidence)[0]
    assert spec.rule_version == "rules-2025-01-05"
    assert spec.rule_evidence_quality == "archived_rule_text"


def test_conflicting_rule_versions_leave_the_version_unknown(tmp_path: Path) -> None:
    """Two versions for one event cannot be summarized by picking one."""
    path = _seal_releases([_release_row()], tmp_path / "releases.parquet")
    evidence = tmp_path / "rule_evidence.json"
    evidence.write_text(
        json.dumps(
            {
                "events": [
                    {"event_id": "cpi_2025_01", "rule_version": "rules-a"},
                    {"event_id": "cpi_2025_01", "rule_version": "rules-b"},
                ]
            }
        ),
        encoding="utf-8",
    )
    spec = load_event_specs(path, rule_evidence_path=evidence)[0]
    assert spec.rule_version is None
    assert "conflicting" in spec.rule_evidence_quality


def test_closure_is_read_from_coverage_evidence_and_never_inferred(tmp_path: Path) -> None:
    """Closure comes from stated lifecycle evidence, not from the last trade."""
    path = _seal_releases([_release_row()], tmp_path / "releases.parquet")
    coverage = tmp_path / "coverage.json"
    coverage.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "event_id": "cpi_2025_01",
                        "direct_closed_pre_release_count": 1,
                        "candidates": [
                            {
                                "ticker": "KXCPI-25JAN-T0.4",
                                "close_time": "2025-01-15T13:00:00+00:00",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    spec = load_event_specs(path, audit_coverage_path=coverage)[0]
    assert spec.closed_before_release is True

    # The same document with no stated closure is no evidence of closure.
    open_coverage = tmp_path / "open_coverage.json"
    open_coverage.write_text(
        json.dumps({"events": [{"event_id": "cpi_2025_01"}]}), encoding="utf-8"
    )
    assert (
        load_event_specs(path, audit_coverage_path=open_coverage)[0].closed_before_release is False
    )


def test_a_naive_scheduled_at_is_refused_rather_than_assumed_utc() -> None:
    """Guessing the zone of a release time would move the whole window around it."""
    naive = _release_row(scheduled_at=dt.datetime(2025, 1, 15, 13, 30))
    with pytest.raises(ValueError, match="offset"):
        event_spec_from_row(naive)


def test_an_unverified_release_dataset_is_refused(tmp_path: Path) -> None:
    """A release dataset whose bytes no longer match its manifest is not read."""
    path = _seal_releases([_release_row()], tmp_path / "releases.parquet")
    manifest_path = path.with_name(path.name + ".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["content_hash"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="content hash"):
        load_event_specs(path)
