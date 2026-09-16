"""Perpetual-futures reference data collected from PerpDexList.

This subpackage exists because the programme's fourth study needs a
cross-venue funding and basis series, and the only public source of it here
publishes rolling snapshots rather than an archive. Nothing it serves is
back-fillable, which is why collection starts before the analysis is built.

The modules are separated by what can fail independently:

* :mod:`~market_propagation.perp.parse` reads a page. It refuses on a shape or
  figure it cannot trust rather than guessing.
* :mod:`~market_propagation.perp.universe` enumerates what exists to read.
* :mod:`~market_propagation.perp.differentials` turns one build's cross-section
  into cross-venue spreads, and refuses to call any of them an arbitrage.
* :mod:`~market_propagation.perp.store` persists a build idempotently, keyed on
  the source's own build stamp rather than our clock.

The governing constraint across all four: an unobserved figure is a null with a
named reason code, never a zero.
"""

from __future__ import annotations

from .differentials import (
    REFUSAL_APR_MISSING,
    REFUSAL_EXECUTION_COST_UNAVAILABLE,
    REFUSAL_PRICE_MISSING,
    CrossVenueBasis,
    FundingDifferential,
    cross_venue_basis,
    funding_differentials,
)
from .parse import (
    MarketSnapshot,
    PageShapeError,
    VenueQuote,
    parse_build_stamp,
    parse_market_page,
)
from .store import DEFAULT_ROOT, SnapshotStore
from .universe import AssetRef, Universe, parse_priority, parse_universe

__all__ = [
    "DEFAULT_ROOT",
    "REFUSAL_APR_MISSING",
    "REFUSAL_EXECUTION_COST_UNAVAILABLE",
    "REFUSAL_PRICE_MISSING",
    "AssetRef",
    "CrossVenueBasis",
    "FundingDifferential",
    "MarketSnapshot",
    "PageShapeError",
    "SnapshotStore",
    "Universe",
    "VenueQuote",
    "cross_venue_basis",
    "funding_differentials",
    "parse_build_stamp",
    "parse_market_page",
    "parse_priority",
    "parse_universe",
]
