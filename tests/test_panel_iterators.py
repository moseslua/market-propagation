"""The event panel reads its quote input once, whatever iterable carries it.

``build_event_panel`` classifies the quotes argument in a single pass, so a
generator is neither exhausted before it is read nor accepted more leniently
than a list. These tests pin the three observable consequences: a generator of
non-quotes fails with the same error as the equivalent list, a valid generator
produces the same rows as the equivalent list, and the order of accepted quotes
is the order the caller supplied.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Iterator
from decimal import Decimal

import pandas as pd
import pytest

from market_propagation.domain import (
    UTC,
    Clock,
    Contract,
    Operator,
    Provenance,
    Quote,
    QuoteValidity,
    Release,
    Rounding,
    Trade,
)
from market_propagation.point_in_time import build_event_panel
from market_propagation.replay import ORDER_SOURCE

T0 = dt.datetime(2026, 3, 2, 12, 0, 0, tzinfo=UTC)
CONTRACT_ID = "CPI-THRESHOLD"
EVENT_ID = "CPI-2026-03"


def at(seconds: float) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


def provenance(record_id: str) -> Provenance:
    return Provenance((record_id * 8).ljust(64, "0")[:64], record_id, "venue-a")


def quote_at(seconds: float, *, bid: str, ask: str) -> Quote:
    return Quote(
        venue="venue-a",
        contract_id=CONTRACT_ID,
        clock=Clock.captured(at(seconds), at(seconds)),
        provenance=provenance(f"quote-{seconds}"),
        bid=Decimal(bid),
        ask=Decimal(ask),
        bid_size=Decimal("40"),
        ask_size=Decimal("25"),
        validity=QuoteValidity.VALID,
        last_price_change=at(seconds),
        last_verified=at(seconds),
        last_trade=None,
        replay_order=ORDER_SOURCE,
    )


def trade(record_id: str) -> Trade:
    return Trade(
        venue="venue-a",
        contract_id=CONTRACT_ID,
        trade_id=record_id,
        price=Decimal("0.99"),
        size=Decimal("1"),
        clock=Clock.captured(at(0), at(0)),
        provenance=provenance(record_id),
    )


def release() -> Release:
    return Release(
        event_id=EVENT_ID,
        family="CPI",
        scheduled_at=at(120),
        reference_period="2026-02",
        values={"headline": Decimal("0.4")},
        clock=Clock.captured(at(120), at(120)),
        provenance=provenance("release-1"),
    )


def contract() -> Contract:
    return Contract(
        venue="venue-a",
        contract_id=CONTRACT_ID,
        event_id=EVENT_ID,
        family="CPI",
        reference_period="2026-02",
        source="BLS",
        units="index_points",
        operator=Operator.ABOVE,
        threshold=Decimal("0.3"),
        lower=None,
        upper=None,
        rounding=Rounding.NEAREST,
        vintage="initial",
        timezone="America/New_York",
        deadline=None,
        settlement="cash",
        currency="USD",
        exceptional_policy="void_excluded",
        open_time=None,
        close_time=None,
        resolve_time=None,
        rule_hash="rule-1",
        provenance=provenance("contract-1"),
    )


def panel(quotes: Iterable[Quote | Trade]) -> pd.DataFrame:
    return build_event_panel(
        quotes, [release()], [contract()], order=ORDER_SOURCE, horizons_seconds=[60]
    )


def streamed(entries: list[Quote | Trade]) -> Iterator[Quote | Trade]:
    """The same records behind a one-shot iterable, which is what a caller streams."""
    return iter(entries)


def test_panel_refuses_a_generator_of_non_quotes_instead_of_returning_an_empty_frame() -> None:
    with pytest.raises(TypeError, match="1 entries that are not Quote records"):
        panel(streamed([trade("trade-1")]))


def test_panel_reports_the_same_invalid_count_for_a_generator_as_for_a_list() -> None:
    entries = [trade("trade-1"), quote_at(0, bid="0.50", ask="0.56"), trade("trade-2")]

    with pytest.raises(TypeError, match="2 entries that are not Quote records") as listed:
        panel(entries)

    with pytest.raises(TypeError, match="2 entries that are not Quote records") as generated:
        panel(streamed(entries))

    assert type(generated.value) is type(listed.value)
    assert str(generated.value) == str(listed.value)


def test_panel_reads_a_valid_generator_to_the_same_rows_as_a_list() -> None:
    quotes = [quote_at(0, bid="0.50", ask="0.56"), quote_at(180, bid="0.58", ask="0.64")]

    from_list = panel(quotes)
    from_generator = panel(streamed(quotes))

    assert from_list.equals(from_generator)

    row = from_generator.iloc[0]
    assert bool(row["valid"]) is True
    assert row["baseline_time"] == at(0)
    assert row["endpoint_time"] == at(180)


def test_panel_keeps_the_order_of_accepted_quotes_from_a_generator() -> None:
    latest_first = [quote_at(180, bid="0.58", ask="0.64"), quote_at(0, bid="0.50", ask="0.56")]

    row = panel(streamed(latest_first)).iloc[0]
    assert row["baseline_time"] == at(0)
    assert row["endpoint_time"] == at(180)
    assert bool(row["valid"]) is True
