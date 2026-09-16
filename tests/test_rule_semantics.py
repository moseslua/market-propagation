"""Regression tests for unknown rule semantics across the ingest/matcher boundary.

The defect these pin: a real Kalshi market record publishes no statistic vintage
and no rounding rule, and the normalizer used to write the placeholder string
``"vintage_not_published_on_market_record"`` into that matching field. Two such
records then compared equal on ``vintage``, so ``exact_match(c, c)`` returned
``matches=True`` for a rule whose vintage was never published. The same held for
an unlisted series, which received ``"unmapped"`` in ``units``, ``source`` and
``family``.

An unknown fact is now ``None``. A null survives storage as a null, and the
primary panel excludes a contract carrying one with a named reason instead of
treating the record as verified.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

import market_propagation.coherence as coherence
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
)
from market_propagation.ingest.normalize import normalize_kalshi_contract
from market_propagation.point_in_time import build_event_panel
from market_propagation.replay import ORDER_SOURCE
from market_propagation.storage import read_parquet, write_parquet

T0 = dt.datetime(2026, 8, 12, 12, 0, 0, tzinfo=UTC)
NY = "America/New_York"

#: A real ``/markets`` row: it carries ``price_level_structure='linear_cent'``
#: and no rule text stating a rounding rule, and it carries no vintage field at
#: all.
MARKET_BODY = {
    "ticker": "KXCPI-26AUG-T1.0",
    "event_ticker": "KXCPI-26AUG",
    "market_type": "binary",
    "status": "finalized",
    "strike_type": "greater",
    "floor_strike": 1,
    "close_time": "2026-09-11T12:25:00Z",
    "created_time": "2026-07-23T20:33:23.485958Z",
    "open_time": "2026-07-23T21:00:00Z",
    "settlement_ts": "2026-09-11T14:30:00Z",
    "latest_expiration_time": "2026-12-11T13:56:00Z",
    "rules_primary": "If the CPI rises above 1.0%, this market resolves YES.",
    "rules_secondary": "Resolves from the BLS Consumer Price Index release.",
    "price_level_structure": "linear_cent",
    "price_ranges": [{"start": "0.00", "end": "1.00", "step": "0.01"}],
}


def at(seconds: float) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


def provenance(record_id: str, *, source: str = "kalshi") -> Provenance:
    return Provenance("a" * 64, record_id, source)


def clock(seconds: float) -> Clock:
    return Clock.captured(at(seconds), at(seconds))


def quote_at(
    seconds: float,
    *,
    contract_id: str,
    bid: str,
    ask: str,
    venue: str = "kalshi",
) -> Quote:
    return Quote(
        venue=venue,
        contract_id=contract_id,
        clock=clock(seconds),
        provenance=provenance(f"quote-{seconds}-{contract_id}", source=venue),
        bid=Decimal(bid),
        ask=Decimal(ask),
        bid_size=Decimal("40"),
        ask_size=Decimal("25"),
        validity=QuoteValidity.VALID,
        last_price_change=at(seconds),
        last_verified=at(seconds),
        last_trade=None,
    )


def release_at(seconds: float, *, event_id: str, family: str) -> Release:
    return Release(
        event_id=event_id,
        family=family,
        scheduled_at=at(seconds),
        reference_period="2026-08",
        values={"headline": Decimal("0.4")},
        clock=clock(seconds),
        provenance=provenance(f"release-{event_id}", source="bls"),
    )


def complete_contract(
    *,
    contract_id: str = "CPI-THRESHOLD",
    event_id: str = "CPI-2026-03",
    venue: str = "kalshi",
    deadline: dt.datetime | None = None,
) -> Contract:
    return Contract(
        venue=venue,
        contract_id=contract_id,
        event_id=event_id,
        family="cpi",
        reference_period="2026-08",
        source="BLS",
        units="percent_mom_change",
        operator=Operator.ABOVE,
        threshold=Decimal("1.0"),
        lower=None,
        upper=None,
        rounding=Rounding.NONE,
        vintage="initial",
        timezone=NY,
        deadline=deadline if deadline is not None else at(0),
        settlement="cash",
        currency="USD",
        exceptional_policy="binary_default",
        open_time=None,
        close_time=None,
        resolve_time=None,
        rule_hash="rule-complete",
        provenance=provenance(f"contract-{contract_id}"),
    )


def test_unpublished_vintage_and_rounding_normalize_to_null() -> None:
    contract = normalize_kalshi_contract(
        MARKET_BODY, provenance=provenance("p"), reference_period="2026-08"
    )
    assert contract.vintage is None
    assert contract.rounding is None
    # The series is listed, so the facts the venue does publish are preserved.
    assert contract.family == "cpi"
    assert contract.source == "BLS"
    assert contract.units == "percent_mom_change"
    assert contract.reference_period == "2026-08"
    # ``price_level_structure='linear_cent'`` is a price tick grid, not a
    # statistic rounding rule, so it must not become one.
    assert contract.rounding is not Rounding.NEAREST
    assert not isinstance(contract.vintage, str)


def test_unlisted_series_leaves_matching_fields_null() -> None:
    contract = normalize_kalshi_contract(
        {**MARKET_BODY, "ticker": "KXUNKNOWN-26AUG-T1.0", "event_ticker": "KXUNKNOWN-26AUG"},
        provenance=provenance("p"),
        reference_period="2026-08",
    )
    assert contract.units is None
    assert contract.source is None
    assert contract.family is None
    assert contract.vintage is None


def test_rule_text_stating_no_rounding_keeps_that_fact() -> None:
    stated = normalize_kalshi_contract(
        {**MARKET_BODY, "rules_primary": "The statistic is compared without rounding."},
        provenance=provenance("p"),
        reference_period="2026-08",
    )
    assert stated.rounding is Rounding.NONE
    nearest = normalize_kalshi_contract(
        {**MARKET_BODY, "rules_primary": "The value is rounded to the nearest 0.1."},
        provenance=provenance("p"),
        reference_period="2026-08",
    )
    assert nearest.rounding is Rounding.NEAREST
    # A stated rounding rule does not conjure a vintage the venue never published.
    assert stated.vintage is None and nearest.vintage is None


def test_absent_rule_text_leaves_exceptional_policy_null() -> None:
    silent = normalize_kalshi_contract(
        {**MARKET_BODY, "rules_primary": None, "rules_secondary": None},
        provenance=provenance("p"),
        reference_period="2026-08",
    )
    assert silent.exceptional_policy is None


def test_real_normalization_then_exact_match_rejects_unknown_vintage() -> None:
    contract = normalize_kalshi_contract(
        MARKET_BODY, provenance=provenance("p"), reference_period="2026-08"
    )
    result = coherence.exact_match(contract, contract)
    assert result["matches"] is False
    assert "vintage" in result["missing_fields"]
    assert "rounding" in result["missing_fields"]
    assert any("missing-required-semantic-field" in reason for reason in result["missing_reasons"])
    assert result["blocking_differences"] == []


def test_complete_identical_rules_still_match() -> None:
    rule = complete_contract()
    result = coherence.exact_match(rule, rule)
    assert result["matches"] is True
    assert result["missing_fields"] == []
    assert result["blocking_differences"] == []
    assert "vintage" in result["matched_fields"]


def test_a_known_value_against_an_unknown_one_blocks_the_match() -> None:
    known = complete_contract()
    unknown = replace(known, vintage=None, rounding=None)
    result = coherence.exact_match(known, unknown)
    assert result["matches"] is False
    assert "vintage" in result["blocking_differences"]
    assert "rounding" in result["blocking_differences"]
    assert result["missing_fields"] == []


def test_unknown_semantics_round_trip_through_storage_as_nulls(tmp_path: Path) -> None:
    unknown = normalize_kalshi_contract(
        MARKET_BODY, provenance=provenance("p"), reference_period="2026-08"
    )
    complete = complete_contract(contract_id="COMPLETE-RULE")
    path = tmp_path / "contracts.parquet"
    write_parquet([unknown, complete], path, table="contracts", coverage_epoch="fixture")

    frame = read_parquet(path)
    assert len(frame) == 2
    unknown_row = frame.loc[frame["contract_id"] == "KXCPI-26AUG-T1.0"].iloc[0]
    complete_row = frame.loc[frame["contract_id"] == "COMPLETE-RULE"].iloc[0]

    assert pd.isna(unknown_row["vintage"])
    assert pd.isna(unknown_row["rounding"])
    assert "vintage_not_published_on_market_record" not in set(frame["vintage"].dropna())
    assert "unmapped" not in set(frame["units"].dropna())
    # A known value beside the nulls is preserved rather than blanked.
    assert unknown_row["units"] == "percent_mom_change"
    assert complete_row["vintage"] == "initial"
    assert complete_row["rounding"] == "none"


def test_panel_excludes_a_contract_with_unknown_rule_semantics() -> None:
    contract = normalize_kalshi_contract(
        MARKET_BODY, provenance=provenance("p"), reference_period="2026-08"
    )
    frame = build_event_panel(
        [
            quote_at(0, contract_id=contract.contract_id, bid="0.50", ask="0.56"),
            quote_at(180, contract_id=contract.contract_id, bid="0.58", ask="0.64"),
        ],
        [release_at(120, event_id="KXCPI-26AUG", family="cpi")],
        [contract],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
    )
    assert len(frame) == 1
    row = frame.iloc[0]
    assert bool(row["valid"]) is False
    assert row["exclusion_reason"] == "unknown_rule_semantics"
    assert pd.isna(row["rounding"])
    assert row["units"] == "percent_mom_change"
    assert pd.isna(row["response"])


def test_panel_admits_a_complete_rule_contract() -> None:
    contract = complete_contract(
        contract_id="CPI-THRESHOLD", event_id="CPI-2026-03", venue="kalshi"
    )
    frame = build_event_panel(
        [
            quote_at(0, contract_id="CPI-THRESHOLD", bid="0.50", ask="0.56"),
            quote_at(180, contract_id="CPI-THRESHOLD", bid="0.58", ask="0.64"),
        ],
        [release_at(120, event_id="CPI-2026-03", family="cpi")],
        [contract],
        order=ORDER_SOURCE,
        horizons_seconds=[60],
    )
    assert len(frame) == 1
    row = frame.iloc[0]
    assert bool(row["valid"]) is True
    assert row["exclusion_reason"] is None
    assert row["rounding"] == "none"
    assert row["response"] == pytest.approx(0.08)
