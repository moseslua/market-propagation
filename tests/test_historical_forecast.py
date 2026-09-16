"""The source-time forecast panel: its clock, its masking and its determinism."""

from __future__ import annotations

import datetime as dt
import tempfile
from decimal import Decimal
from pathlib import Path

import pytest

from market_propagation import storage
from market_propagation.domain import Clock, HistoricalTrade, Provenance
from market_propagation.historical_forecast import (
    REASON_ANCHOR_BEYOND_CAP,
    REASON_NEIGHBOR_GRAPH_NOT_SUPPLIED,
    REASON_NO_ADMISSIBLE_NEIGHBOR,
    REASON_NO_DONOR_SIGNAL,
    REASON_NO_FORWARD_TARGET,
    REASON_NO_RECIPIENT_ANCHOR,
    ForecastSettings,
    build_forecast_rows,
)

RELEASE = dt.datetime(2025, 1, 10, 13, 30, tzinfo=dt.UTC)
TAU = RELEASE + dt.timedelta(seconds=300)
RECEIVER = "R"
DONOR = "D"


class _Event:
    """The subset of an event spec this module reads."""

    def __init__(self, event_id: str = "ev1", family: str = "employment") -> None:
        self.event_id = event_id
        self.cluster_id = event_id
        self.family = family
        self.event_time = RELEASE


class _Graph:
    def __init__(self, donors: dict[str, tuple[str, ...]] | None = None) -> None:
        self._donors = donors or {}

    def donors_for(self, contract_id: str) -> tuple[str, ...]:
        return self._donors.get(contract_id, ())


def _print(
    contract_id: str,
    offset_seconds: float,
    price_cents: int,
    *,
    occurrence: str,
) -> HistoricalTrade:
    instant = RELEASE + dt.timedelta(seconds=offset_seconds)
    price = Decimal(price_cents) / Decimal(100)
    return HistoricalTrade(
        venue="kalshi",
        contract_id=contract_id,
        price=price,
        raw_price_units="cents",
        price_precision="exact_integer_cents",
        size_quality="verified_source_quantity",
        clock=Clock.historical(instant),
        provenance=Provenance(raw_hash="a" * 64, record_id=occurrence, source="probe"),
        event_price=price,
        event_axis="yes_price_is_event_axis",
        size=Decimal(10),
    )


_UNSET = object()


def _build(trades, *, donors=None, settings=None, receivers=None, graph=_UNSET):
    return build_forecast_rows(
        trades,
        [_Event()],
        _Graph(donors) if graph is _UNSET else graph,
        receivers=receivers,
        settings=settings or ForecastSettings(),
    )


def _row(panel, contract: str = RECEIVER) -> dict:
    """The row for one contract, so a fixture donor cannot be mistaken for it."""
    for row in panel.rows:
        if row["receiver_contract_id"] == contract:
            return row
    raise AssertionError(f"no row for {contract}")


def test_a_complete_row_carries_the_five_to_ten_minute_increment() -> None:
    """The target is tau to tau+H, not the first five minutes' absorption."""
    trades = [
        _print(RECEIVER, 240, 45, occurrence="r1"),
        _print(RECEIVER, 540, 55, occurrence="r2"),
        _print(DONOR, -600, 30, occurrence="d0"),
        _print(DONOR, 200, 60, occurrence="d1"),
    ]
    panel = _build(trades, donors={RECEIVER: (DONOR,)})
    row = _row(panel)
    assert row["valid"] is True
    assert row["recipient_anchor"] == pytest.approx(0.45)
    assert row["recipient_target"] == pytest.approx(0.55)
    assert row["target"] == pytest.approx(0.10)
    assert row["neighbor_lag"] == pytest.approx(0.30)
    assert row["clock_basis"] == "source"
    assert row["exclusion_reason"] is None


def test_a_missing_donor_signal_is_null_and_never_zero() -> None:
    """A recipient with no donor print leaves the lag null while keeping its own legs."""
    trades = [
        _print(RECEIVER, 240, 45, occurrence="r1"),
        _print(RECEIVER, 540, 55, occurrence="r2"),
        _print(DONOR, -600, 30, occurrence="d0"),
    ]
    row = _row(_build(trades, donors={RECEIVER: (DONOR,)}))
    assert row["neighbor_lag"] is None
    assert row["donor_signal"] is None
    assert REASON_NO_DONOR_SIGNAL in row["exclusion_reason"]
    # The recipient's own observation survives the donor's absence.
    assert row["target"] == pytest.approx(0.10)
    assert row["valid"] is True


def test_a_genuine_zero_is_distinguishable_from_a_missing_signal() -> None:
    """An unchanged fresh price is evidence; an absent print is not."""
    unchanged = _row(
        _build(
            [
                _print(RECEIVER, 240, 50, occurrence="r1"),
                _print(RECEIVER, 540, 50, occurrence="r2"),
            ]
        )
    )
    absent = _row(_build([_print(RECEIVER, 240, 50, occurrence="r1")]))
    assert unchanged["target"] == pytest.approx(0.0)
    assert unchanged["target"] is not None
    assert absent["target"] is None
    assert absent["exclusion_reason"] == REASON_NO_FORWARD_TARGET
    assert absent["valid"] is False


def test_the_donor_signal_stops_at_the_lag_guard() -> None:
    """A donor print inside the guard is not read, so the target cannot leak in."""
    trades = [
        _print(RECEIVER, 240, 45, occurrence="r1"),
        _print(RECEIVER, 540, 55, occurrence="r2"),
        _print(DONOR, -600, 30, occurrence="d0"),
        _print(DONOR, 200, 60, occurrence="d1"),
        _print(DONOR, 280, 90, occurrence="d2"),
    ]
    row = _row(_build(trades, donors={RECEIVER: (DONOR,)}))
    assert row["donor_signal"] == pytest.approx(0.60)
    assert row["neighbor_lag"] == pytest.approx(0.30)
    assert row["donor_signal_time"] < row["forecast_origin"]


def test_no_admissible_neighbor_is_recorded_rather_than_skipped() -> None:
    """An absent donor is a recorded decision that blocks the network rung only."""
    row = _row(
        _build(
            [
                _print(RECEIVER, 240, 45, occurrence="r1"),
                _print(RECEIVER, 540, 55, occurrence="r2"),
            ],
            donors={},
        )
    )
    assert row["donor_contract_id"] is None
    assert row["neighbor_lag"] is None
    assert row["exclusion_reason"] == REASON_NO_ADMISSIBLE_NEIGHBOR
    # The recipient observed its own increment, so the row is usable by the
    # news rung and excluded only from the network comparison.
    assert row["valid"] is True
    assert row["target"] == pytest.approx(0.10)


def test_a_stale_anchor_is_excluded_rather_than_used() -> None:
    """An anchor older than the cap is not a forecast origin observation."""
    row = _row(
        _build(
            [
                _print(RECEIVER, -400, 40, occurrence="r0"),
                _print(RECEIVER, 540, 55, occurrence="r2"),
            ]
        )
    )
    assert row["exclusion_reason"] == REASON_ANCHOR_BEYOND_CAP
    assert row["valid"] is False


def test_an_event_with_no_recipient_anchor_is_named() -> None:
    row = _row(_build([_print(RECEIVER, 540, 55, occurrence="r2")]))
    assert row["exclusion_reason"] == REASON_NO_RECIPIENT_ANCHOR


def test_tied_timestamps_form_one_group_and_survive_input_order() -> None:
    """Ties aggregate by unweighted mean and the result is order independent."""
    shared = 240.0
    trades = [
        _print(RECEIVER, shared, 40, occurrence="a"),
        _print(RECEIVER, shared, 60, occurrence="b"),
        _print(RECEIVER, 540, 55, occurrence="c"),
    ]
    first = _row(_build(trades))
    shuffled = _row(_build(list(reversed(trades))))
    assert first["recipient_anchor"] == pytest.approx(0.50)
    assert first["recipient_anchor_tie_group_size"] == 2
    assert first["recipient_anchor_occurrences"] == ["a", "b"]
    assert first == shuffled


def test_input_cutoff_never_exceeds_the_forecast_origin() -> None:
    """Every feature a row reads is available at its own origin."""
    trades = [
        _print(RECEIVER, 240, 45, occurrence="r1"),
        _print(RECEIVER, 540, 55, occurrence="r2"),
        _print(DONOR, -600, 30, occurrence="d0"),
        _print(DONOR, 200, 60, occurrence="d1"),
    ]
    row = _row(_build(trades, donors={RECEIVER: (DONOR,)}))
    assert row["max_input_source_time"] <= row["forecast_origin"]
    assert row["label_source_time"] > row["forecast_origin"]


def test_a_built_panel_seals_and_reads_back() -> None:
    """Rows round trip through the sealed table with nulls preserved."""
    panel = _build(
        [
            _print(RECEIVER, 240, 45, occurrence="r1"),
            _print(RECEIVER, 540, 55, occurrence="r2"),
        ]
    )
    target = Path(tempfile.mkdtemp()) / "forecast.parquet"
    reference = panel.write(target)
    assert reference.table == "historical_forecast"
    frame = storage.read_parquet(target, table="historical_forecast")
    assert len(frame) == len(panel.rows)
    assert frame["neighbor_lag"].isna().all()


def test_a_declared_receiver_that_never_traded_keeps_its_masked_row() -> None:
    """A declared receiver with no print is a missing observation, not an absent row."""
    panel = _build(
        [_print(RECEIVER, 240, 45, occurrence="r1")],
        receivers={"ev1": [RECEIVER, "QUIET"]},
    )

    assert sorted(row["receiver_contract_id"] for row in panel.rows) == ["QUIET", RECEIVER]
    quiet = _row(panel, "QUIET")
    assert quiet["valid"] is False
    assert quiet["exclusion_reason"] == REASON_NO_RECIPIENT_ANCHOR
    assert quiet["recipient_anchor"] is None
    assert quiet["target"] is None
    counts = panel.counts
    assert counts["receiver_universe"] == "declared_per_release_set"
    assert counts["declared_receiver_slots"] == 2
    assert counts["rows"] == 2
    assert "receiver_universe_from_declared_set" in panel.flags


def test_a_declared_receiver_printing_only_outside_the_window_keeps_its_row() -> None:
    """Prints outside the observation window leave the row masked rather than deleting it."""
    panel = _build(
        [_print("LATE", 3000, 45, occurrence="late")],
        receivers={"ev1": ["LATE"]},
    )

    late = _row(panel, "LATE")
    assert late["exclusion_reason"] == REASON_NO_RECIPIENT_ANCHOR
    assert late["recipient_anchor"] is None
    assert panel.counts["rows"] == 1


def test_a_declared_receiver_set_must_cover_every_release() -> None:
    """An uncovered release has no receiver universe, so it is refused rather than guessed."""
    with pytest.raises(ValueError, match="ev1"):
        _build([_print(RECEIVER, 240, 45, occurrence="r1")], receivers={"other": [RECEIVER]})


def test_a_missing_graph_is_reported_as_a_missing_input() -> None:
    """An unsupplied graph is not the same fact as a graph with no admissible donor."""
    panel = _build(
        [_print(RECEIVER, 240, 45, occurrence="r1"), _print(RECEIVER, 540, 55, occurrence="r2")],
        graph=None,
    )

    assert _row(panel)["exclusion_reason"] == REASON_NEIGHBOR_GRAPH_NOT_SUPPLIED
    assert "neighbor_graph_not_supplied" in panel.flags
    # The recipient's own legs are still measured, so only the network rung is blocked.
    assert _row(panel)["target"] is not None
    assert _row(panel)["valid"] is True


def test_a_graph_without_donors_for_is_refused() -> None:
    """A mapping cannot answer which donors are admissible, so it is not read as one."""
    with pytest.raises(TypeError, match="donors_for"):
        build_forecast_rows(
            [_print(RECEIVER, 240, 45, occurrence="r1")],
            [_Event()],
            {"R": ("D",)},
            settings=ForecastSettings(),
        )
