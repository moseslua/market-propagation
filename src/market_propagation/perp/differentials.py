"""Cross-venue funding differentials and basis, from one parsed snapshot.

This module deliberately produces **gross quoted spreads, never arbitrage**. The
distinction is the whole point of the study it feeds, so it is enforced here
rather than left to a reader's judgement.

Three things are true of this source and each is recorded rather than papered
over:

1. **No execution cost is available.** The source publishes an execution-cost
   page, but its server-rendered body carries no table, so the fee and slippage
   layer cannot be observed. A gross spread with the cost layer missing is not
   an arbitrage, because it says nothing about whether a position could be
   opened at those prices. Every differential therefore carries
   :data:`REFUSAL_EXECUTION_COST_UNAVAILABLE` and the caller must not promote a
   spread to an executable opportunity on this data alone.
2. **The annualised column needs no interval.** ``Funding APR`` is already
   annualised by the source, so a cross-venue APR comparison is interval-free
   and is reported regardless of whether the schedule could be derived. The
   per-hour figure does need the interval, and is withheld by name when the two
   derivations in :mod:`~market_propagation.perp.parse` disagree.
3. **A quoted rate is not a settled payment.** The live rate column is what a
   venue charges next; the ``paid`` columns are what it actually settled. Both
   are carried side by side and never merged, because the study's claims about
   realised carry rest on the second.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from itertools import combinations

from .parse import (
    REFUSAL_INTERVAL_DISAGREES,
    REFUSAL_INTERVAL_UNDERIVABLE,
    FundingInterval,
    MarketSnapshot,
    VenueQuote,
    resolve_interval,
)

#: The cost layer cannot be observed from this source's server-rendered pages.
#: Present on every differential so a consumer cannot mistake a quoted spread
#: for an executable one.
REFUSAL_EXECUTION_COST_UNAVAILABLE = "execution_cost_not_observable_from_this_source"
#: One side of the pair reported no annualised rate, so no spread exists.
REFUSAL_APR_MISSING = "funding_apr_missing_on_one_side"
#: One side reported no price, so no basis exists.
REFUSAL_PRICE_MISSING = "price_missing_on_one_side"
#: The two sides report different symbols, so the pair is not the same contract.
REFUSAL_SAME_SYMBOL_PAIR = "pair_is_the_same_contract"


@dataclass(frozen=True, slots=True)
class FundingDifferential:
    """An ordered cross-venue funding pair for one asset.

    ``long_venue`` is the side paying the lower annualised rate, so a
    delta-neutral position is long where funding is cheap and short where it is
    expensive and collects ``apr_spread`` a year before costs.
    """

    asset_class: str
    asset: str
    long_venue: str
    long_symbol: str
    short_venue: str
    short_symbol: str
    long_apr: Decimal | None
    short_apr: Decimal | None
    apr_spread: Decimal | None
    per_hour_spread: Decimal | None
    long_interval: FundingInterval | None
    short_interval: FundingInterval | None
    settled_30d_spread: Decimal | None
    refusals: tuple[str, ...]

    @property
    def pair(self) -> tuple[str, str]:
        return (f"{self.long_venue}:{self.long_symbol}", f"{self.short_venue}:{self.short_symbol}")


@dataclass(frozen=True, slots=True)
class CrossVenueBasis:
    """``log(price_high_venue) - log(price_low_venue)`` for one asset.

    A positive basis means the second venue prints higher. This is a *level*
    difference between two venues' marks, not a tradeable spread: without the
    cost layer it cannot be turned into a return.
    """

    asset_class: str
    asset: str
    lower_venue: str
    lower_symbol: str
    higher_venue: str
    higher_symbol: str
    lower_price: Decimal
    higher_price: Decimal
    log_basis: float


def _interval_or_refusal(
    quote: VenueQuote, settlements: int | None
) -> tuple[FundingInterval | None, str | None]:
    resolved = resolve_interval(quote.funding_rate, quote.funding_apr, settlements)
    if isinstance(resolved, tuple):
        return None, resolved[1]
    return resolved, None


def funding_differentials(
    snapshot: MarketSnapshot,
    *,
    settlement_counts: dict[str, int] | None = None,
) -> tuple[FundingDifferential, ...]:
    """Every ordered venue pair for the snapshot's asset, cheapest long first.

    ``settlement_counts`` maps a *venue* to its settled-payment count: the
    funding-history page reports one row per venue while a market page reports
    one row per ``(venue, symbol)``. The count therefore keys on venue alone,
    because a venue settles all its contracts on one schedule.

    Pairs are formed on ``(venue, symbol)`` because one venue lists several
    contracts per asset, and two entries sharing a symbol are refused: they are
    the same contract, so a "spread" between them is an accounting artefact
    rather than a cross-venue position.
    """
    counts = settlement_counts or {}
    quotes = [q for q in snapshot.quotes if q.funding_apr is not None]
    out: list[FundingDifferential] = []

    for left, right in combinations(quotes, 2):
        if left.symbol == right.symbol and left.venue == right.venue:
            continue
        ordered = (left, right) if left.funding_apr <= right.funding_apr else (right, left)
        long_side, short_side = ordered
        refusals: list[str] = [REFUSAL_EXECUTION_COST_UNAVAILABLE]

        long_interval, long_refusal = _interval_or_refusal(long_side, counts.get(long_side.venue))
        short_interval, short_refusal = _interval_or_refusal(
            short_side, counts.get(short_side.venue)
        )
        if long_refusal is not None:
            refusals.append(f"long_interval:{long_refusal}")
        if short_refusal is not None:
            refusals.append(f"short_interval:{short_refusal}")

        apr_spread = short_side.funding_apr - long_side.funding_apr

        per_hour: Decimal | None = None
        if long_interval is not None and short_interval is not None:
            long_hourly = (
                long_side.funding_rate / Decimal(long_interval.hours)
                if (long_side.funding_rate is not None)
                else None
            )
            short_hourly = (
                short_side.funding_rate / Decimal(short_interval.hours)
                if (short_side.funding_rate is not None)
                else None
            )
            if long_hourly is not None and short_hourly is not None:
                per_hour = short_hourly - long_hourly
        else:
            if not any(
                code in refusals
                for code in (REFUSAL_INTERVAL_DISAGREES, REFUSAL_INTERVAL_UNDERIVABLE)
            ) and (long_refusal or short_refusal):
                refusals.append(REFUSAL_INTERVAL_UNDERIVABLE)

        settled_spread: Decimal | None = None
        if long_side.paid_30d is not None and short_side.paid_30d is not None:
            settled_spread = short_side.paid_30d - long_side.paid_30d

        out.append(
            FundingDifferential(
                asset_class=snapshot.asset_class,
                asset=snapshot.asset,
                long_venue=long_side.venue,
                long_symbol=long_side.symbol,
                short_venue=short_side.venue,
                short_symbol=short_side.symbol,
                long_apr=long_side.funding_apr,
                short_apr=short_side.funding_apr,
                apr_spread=apr_spread,
                per_hour_spread=per_hour,
                long_interval=long_interval,
                short_interval=short_interval,
                settled_30d_spread=settled_spread,
                refusals=tuple(refusals),
            )
        )
    out.sort(key=lambda d: d.apr_spread if d.apr_spread is not None else Decimal(0), reverse=True)
    return tuple(out)


def cross_venue_basis(snapshot: MarketSnapshot) -> tuple[CrossVenueBasis, ...]:
    """Every ordered venue pair with an observable price on both sides."""
    priced = [q for q in snapshot.quotes if q.price is not None and q.price > 0]
    out: list[CrossVenueBasis] = []
    for left, right in combinations(priced, 2):
        lower, higher = (left, right) if left.price <= right.price else (right, left)
        out.append(
            CrossVenueBasis(
                asset_class=snapshot.asset_class,
                asset=snapshot.asset,
                lower_venue=lower.venue,
                lower_symbol=lower.symbol,
                higher_venue=higher.venue,
                higher_symbol=higher.symbol,
                lower_price=lower.price,
                higher_price=higher.price,
                log_basis=math.log(float(higher.price)) - math.log(float(lower.price)),
            )
        )
    return tuple(out)
