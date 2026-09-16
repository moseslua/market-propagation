"""Parse PerpDexList's server-rendered market and funding pages.

Four facts about this source drive every rule below, and each was read off the
live pages rather than assumed:

1. **The whole site rebuilds as one unit.** Every page carries the same
   ``Data generated at <ISO8601Z>`` stamp. A sweep is therefore valid once per
   build, and a second fetch inside the same build returns byte-identical
   figures. The stamp, not the local clock, is the observation time.
2. **The unit is ``(venue, symbol)``, not venue.** One venue lists several
   contracts for the same asset: ETH carries four ``binance`` rows and three
   ``gmx`` rows distinguished by collateral. Collapsing to venue would silently
   merge contracts that settle differently, so the pair is the key.
3. **An unobserved figure is an em dash.** ``—`` means the source did not
   observe the number. It is never zero, and it never becomes a zero here: an
   em dash parses to ``None`` and the caller records a named refusal.
4. **The funding interval is not printed.** The column is headed
   ``Funding / interval`` but the cell holds only the per-settlement rate. The
   interval is recoverable two independent ways -- annualising the rate, and
   dividing the settled-settlement count by the window -- and this module
   derives it from both and refuses when they disagree rather than picking one.

Content negotiation is used deliberately: the source serves ``text/markdown``,
so parsing targets pipe tables instead of HTML. The HTML table was examined and
carries ``&#43;`` for ``+``; the markdown route carries a literal ``+``, which is
one fewer escaping rule to get wrong.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

#: The source rebuilds and stamps every page with this sentence. It is the only
#: trustworthy observation time: the local receive clock measures our latency,
#: not the data's vintage, and the two are recorded separately.
BUILD_STAMP_RE = re.compile(r"Data generated at (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)")

#: Characters the source uses for "we did not observe this". The first is an em
#: dash (U+2014), the second an en dash (U+2013); both are written as escapes so
#: they cannot be mistaken for a stray hyphen by a reader or a linter.
DASHES = frozenset({"\u2014", "\u2013", "-", "--", ""})

#: Size suffixes used in the volume and open-interest columns.
SIZE_SUFFIXES: dict[str, Decimal] = {
    "K": Decimal(1_000),
    "M": Decimal(1_000_000),
    "B": Decimal(1_000_000_000),
    "T": Decimal(1_000_000_000_000),
}

#: A markdown table row: leading pipe, then cells, then trailing pipe.
TABLE_ROW_RE = re.compile(r"^\|(.+)\|\s*$")
#: The separator row that follows a markdown header: ``| --- | --- |``.
TABLE_RULE_RE = re.compile(r"^\|[\s:|-]+\|\s*$")

#: Supported funding schedules, in hours, as the source documents them. The FAQ
#: states venues settle on 1h, 4h or 8h.
FUNDING_INTERVALS_HOURS = (1, 4, 8)

HOURS_PER_YEAR = Decimal(8760)

#: Tolerances for the two interval derivations. The annualised column is printed
#: to two decimals, so a short-interval rate carries little relative precision;
#: 5% is wide enough for that rounding and far too narrow to confuse 1h with 4h.
APR_RATIO_TOLERANCE = Decimal("0.05")
SETTLEMENT_COUNT_TOLERANCE = Decimal("0.15")

#: Refusal codes. Each is a distinct reason a figure could not be observed; the
#: caller counts by code rather than dropping the row.
REFUSAL_UNOBSERVED = "source_reported_no_figure"
REFUSAL_UNPARSEABLE = "figure_not_parseable"
REFUSAL_INTERVAL_UNDERIVABLE = "funding_interval_not_derivable"
REFUSAL_INTERVAL_DISAGREES = "funding_interval_derivations_disagree"
REFUSAL_NO_BUILD_STAMP = "page_carried_no_build_stamp"


class PageShapeError(RuntimeError):
    """A page did not match the shape this parser was written against.

    Raised rather than returning a short table, because a silently truncated
    table reads downstream as a venue that stopped trading.
    """


@dataclass(frozen=True, slots=True)
class FundingInterval:
    """A funding schedule resolved from two independent derivations."""

    hours: int
    from_apr_hours: int | None
    from_settlements_hours: int | None


def parse_build_stamp(text: str) -> dt.datetime:
    """The ``Data generated at`` instant, in UTC.

    Raises rather than defaulting to the local clock: an unstamped page cannot
    be placed in time, and inventing a timestamp would put an observation in the
    series that the source never made.
    """
    match = BUILD_STAMP_RE.search(text)
    if match is None:
        raise PageShapeError(REFUSAL_NO_BUILD_STAMP)
    return dt.datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.UTC)


def parse_markdown_tables(text: str) -> list[tuple[list[str], list[list[str]]]]:
    """Every pipe table in ``text`` as ``(header, rows)`` pairs.

    Only the first row of a table is treated as its header; a row of dashes is
    recognised as the markdown separator and skipped rather than parsed as data.
    """
    tables: list[tuple[list[str], list[list[str]]]] = []
    header: list[str] | None = None
    rows: list[list[str]] = []

    def flush() -> None:
        nonlocal header, rows
        if header is not None:
            tables.append((header, rows))
        header, rows = None, []

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            flush()
            continue
        if TABLE_RULE_RE.match(stripped):
            continue
        cells = [cell.strip() for cell in TABLE_ROW_RE.match(stripped).group(1).split("|")]
        if header is None:
            header = cells
            continue
        rows.append(cells)
    flush()
    return tables


def _cell(cells: list[str], index: int) -> str:
    return cells[index] if index < len(cells) else ""


def parse_size(text: str) -> Decimal | None:
    """A dollar figure with an optional K/M/B/T suffix. ``None`` when unobserved.

    ``None`` rather than ``Decimal(0)`` for an em dash is the whole point: a
    venue with ``$0.00`` volume really did trade nothing, while ``—`` means the
    source did not look. Collapsing the two would report absent data as a dead
    venue.
    """
    cleaned = text.strip()
    if cleaned in DASHES:
        return None
    cleaned = cleaned.lstrip("$").replace(",", "").strip()
    if not cleaned:
        return None
    suffix = cleaned[-1].upper()
    multiplier = SIZE_SUFFIXES.get(suffix)
    if multiplier is not None:
        cleaned = cleaned[:-1]
    else:
        multiplier = Decimal(1)
    try:
        return Decimal(cleaned) * multiplier
    except (InvalidOperation, ArithmeticError):
        return None


def parse_percent(text: str) -> Decimal | None:
    """A percent cell as a decimal *fraction*, or ``None`` when unobserved.

    ``+0.98%`` becomes ``Decimal('0.0098')``. A fraction rather than a
    percentage keeps annualising and differentials free of a stray factor of
    one hundred.
    """
    cleaned = text.strip()
    if cleaned in DASHES:
        return None
    cleaned = cleaned.rstrip("%").replace(",", "").replace("+", "").strip()
    if not cleaned:
        return None
    try:
        return Decimal(cleaned) / Decimal(100)
    except (InvalidOperation, ArithmeticError):
        return None


def parse_plain_decimal(text: str) -> Decimal | None:
    """A bare decimal cell such as a price. ``None`` when unobserved."""
    cleaned = text.strip().replace(",", "")
    if cleaned in DASHES:
        return None
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ArithmeticError):
        return None


def _snap_to_interval(hours: Decimal) -> int | None:
    """Snap an hours estimate to a documented schedule, or ``None``."""
    best: int | None = None
    best_error: Decimal | None = None
    for candidate in FUNDING_INTERVALS_HOURS:
        error = abs(hours - Decimal(candidate)) / Decimal(candidate)
        if best_error is None or error < best_error:
            best, best_error = candidate, error
    if best is None or best_error is None or best_error > SETTLEMENT_COUNT_TOLERANCE:
        return None
    return best


def interval_from_apr(rate: Decimal | None, apr: Decimal | None) -> int | None:
    """Funding interval implied by annualising the per-settlement rate.

    ``apr / (rate * periods) == 1`` where ``periods = 8760 / hours``, so the
    hours follow from the ratio. Returns ``None`` when either input is missing
    or the ratio does not sit near a documented schedule.
    """
    if rate is None or apr is None or rate == 0:
        return None
    hours = apr / (rate * HOURS_PER_YEAR) * HOURS_PER_YEAR
    # apr = rate * (8760 / hours)  =>  hours = rate * 8760 / apr
    try:
        hours = rate * HOURS_PER_YEAR / apr
    except (InvalidOperation, ArithmeticError):
        return None
    if hours <= 0:
        return None
    return _snap_to_interval(hours)


def interval_from_settlements(settlements: int | None, window_days: int = 30) -> int | None:
    """Funding interval implied by a settled-payment count over a window."""
    if settlements is None or settlements <= 0:
        return None
    hours = Decimal(window_days * 24) / Decimal(settlements)
    return _snap_to_interval(hours)


def resolve_interval(
    rate: Decimal | None,
    apr: Decimal | None,
    settlements: int | None,
) -> FundingInterval | tuple[None, str]:
    """The funding interval, or a refusal code explaining why it is unavailable.

    Both derivations are required to agree. They are independent -- one uses the
    annualised column, the other the settled count -- so agreement is real
    corroboration and disagreement means one of them is not measuring what we
    think. Refusing on disagreement is preferred to choosing, because the
    interval sets the per-hour funding normalisation that every cross-venue
    differential depends on.
    """
    from_apr = interval_from_apr(rate, apr)
    from_settlements = interval_from_settlements(settlements)
    if from_apr is not None and from_settlements is not None:
        if from_apr != from_settlements:
            return None, REFUSAL_INTERVAL_DISAGREES
        return FundingInterval(from_apr, from_apr, from_settlements)
    resolved = from_apr if from_apr is not None else from_settlements
    if resolved is None:
        return None, REFUSAL_INTERVAL_UNDERIVABLE
    return FundingInterval(resolved, from_apr, from_settlements)


@dataclass(frozen=True, slots=True)
class VenueQuote:
    """One ``(venue, symbol)`` row of a market page, with refusals carried."""

    asset_class: str
    asset: str
    venue: str
    symbol: str
    price: Decimal | None
    volume_24h_usd: Decimal | None
    open_interest_usd: Decimal | None
    funding_rate: Decimal | None
    funding_apr: Decimal | None
    paid_24h: Decimal | None
    paid_7d: Decimal | None
    paid_30d: Decimal | None
    refusals: tuple[str, ...]

    @property
    def key(self) -> tuple[str, str]:
        return (self.venue, self.symbol)


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """One market page at one build."""

    asset_class: str
    asset: str
    build_time: dt.datetime
    quotes: tuple[VenueQuote, ...]
    received_at: dt.datetime


_MARKET_HEADER = (
    "Exchange",
    "Symbol",
    "Price",
    "24h volume",
    "Open interest",
    "Funding / interval",
    "Funding APR",
    "Paid 24h",
    "Paid 7d",
    "Paid 30d",
)

#: Column position per parsed figure. Kept beside the header so a refusal can
#: quote the raw cell without re-deriving the position at each use site.
_FIELD_INDEX = {
    "price": 2,
    "volume_24h_usd": 3,
    "open_interest_usd": 4,
    "funding_rate": 5,
    "funding_apr": 6,
    "paid_24h": 7,
    "paid_7d": 8,
    "paid_30d": 9,
}


def parse_market_page(
    text: str,
    *,
    asset_class: str,
    asset: str,
    received_at: dt.datetime,
) -> MarketSnapshot:
    """Parse a ``/markets/{class}/{asset}`` page into one snapshot.

    The table is located by its exact declared header rather than by position,
    so a new section appearing above it cannot shift the columns silently. A
    page whose header does not match raises, because a mis-keyed table would
    store one quantity under another's name.
    """
    build_time = parse_build_stamp(text)
    quotes: list[VenueQuote] = []
    for header, rows in parse_markdown_tables(text):
        if tuple(header) != _MARKET_HEADER:
            continue
        for cells in rows:
            venue_cell = _cell(cells, 0)
            # The venue cell is a markdown link when the venue has its own page
            # and a bare name when it is a CEX carried only as a counterparty.
            link = re.search(r"\[([^\]]+)\]\(([^)]+)\)", venue_cell)
            venue = (link.group(1) if link else venue_cell).strip()
            if not venue:
                continue
            refusals: list[str] = []
            parsed: dict[str, Decimal | None] = {
                "price": parse_plain_decimal(_cell(cells, 2)),
                "volume_24h_usd": parse_size(_cell(cells, 3)),
                "open_interest_usd": parse_size(_cell(cells, 4)),
                "funding_rate": parse_percent(_cell(cells, 5)),
                "funding_apr": parse_percent(_cell(cells, 6)),
                "paid_24h": parse_percent(_cell(cells, 7)),
                "paid_7d": parse_percent(_cell(cells, 8)),
                "paid_30d": parse_percent(_cell(cells, 9)),
            }
            # Every figure that came back unobserved or unreadable is named here.
            # An em dash on a settled column means the source did not observe a
            # payment, which is not the same as a payment of zero, so it must not
            # reach storage as a bare null a reader would have to guess about.
            for name, value in parsed.items():
                if value is None:
                    raw = _cell(cells, _FIELD_INDEX[name])
                    code = REFUSAL_UNOBSERVED if raw.strip() in DASHES else REFUSAL_UNPARSEABLE
                    refusals.append(f"{name}:{code}")
            price = parsed["price"]
            volume = parsed["volume_24h_usd"]
            open_interest = parsed["open_interest_usd"]
            rate = parsed["funding_rate"]
            apr = parsed["funding_apr"]
            quotes.append(
                VenueQuote(
                    asset_class=asset_class,
                    asset=asset,
                    venue=venue,
                    symbol=_cell(cells, 1),
                    price=price,
                    volume_24h_usd=volume,
                    open_interest_usd=open_interest,
                    funding_rate=rate,
                    funding_apr=apr,
                    paid_24h=parsed["paid_24h"],
                    paid_7d=parsed["paid_7d"],
                    paid_30d=parsed["paid_30d"],
                    refusals=tuple(refusals),
                )
            )
        break
    if not quotes:
        raise PageShapeError(f"no market table with the declared header on {asset_class}/{asset}")
    return MarketSnapshot(
        asset_class=asset_class,
        asset=asset,
        build_time=build_time,
        quotes=tuple(quotes),
        received_at=received_at,
    )


@dataclass(frozen=True, slots=True)
class VenueFundingSummary:
    """One ``(venue)`` row of a funding-history page over its trailing window."""

    venue: str
    settlements: int | None
    total_paid: Decimal | None
    average_apr: Decimal | None
    largest: Decimal | None
    smallest: Decimal | None
    last_settled: dt.datetime | None
    refusals: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FundingHistorySnapshot:
    """One ``/markets/{class}/{asset}/funding`` page at one build."""

    asset_class: str
    asset: str
    build_time: dt.datetime
    window_days: int
    summaries: tuple[VenueFundingSummary, ...]
    received_at: dt.datetime


_FUNDING_HEADER = (
    "Exchange",
    "Settlements",
    "Total paid",
    "Average APR",
    "Largest",
    "Smallest",
    "Last settled",
)


def parse_funding_page(
    text: str,
    *,
    asset_class: str,
    asset: str,
    received_at: dt.datetime,
    window_days: int = 30,
) -> FundingHistorySnapshot:
    """Parse a ``/markets/{class}/{asset}/funding`` settled-funding page.

    These are settled payments, not quoted rates, which is why the module keeps
    them separate from the market page's live columns instead of merging them.
    """
    build_time = parse_build_stamp(text)
    summaries: list[VenueFundingSummary] = []
    for header, rows in parse_markdown_tables(text):
        if tuple(header) != _FUNDING_HEADER:
            continue
        for cells in rows:
            venue_cell = _cell(cells, 0)
            link = re.search(r"\[([^\]]+)\]\(([^)]+)\)", venue_cell)
            venue = (link.group(1) if link else venue_cell).strip()
            if not venue:
                continue
            refusals: list[str] = []
            settlements_raw = _cell(cells, 1).replace(",", "").strip()
            settlements: int | None
            if settlements_raw in DASHES:
                settlements = None
                refusals.append(f"settlements:{REFUSAL_UNOBSERVED}")
            else:
                try:
                    settlements = int(settlements_raw)
                except ValueError:
                    settlements = None
                    refusals.append(f"settlements:{REFUSAL_UNPARSEABLE}")
            last_raw = _cell(cells, 6).strip()
            last_settled: dt.datetime | None = None
            if last_raw in DASHES:
                # The same rule as every figure above: an unobserved instant is named
                # rather than left as a bare null a reader has to interpret.
                refusals.append(f"last_settled:{REFUSAL_UNOBSERVED}")
            else:
                try:
                    last_settled = dt.datetime.strptime(last_raw, "%Y-%m-%d %H:%M").replace(
                        tzinfo=dt.UTC
                    )
                except ValueError:
                    refusals.append(f"last_settled:{REFUSAL_UNPARSEABLE}")
            figures = {
                "total_paid": parse_percent(_cell(cells, 2)),
                "average_apr": parse_percent(_cell(cells, 3)),
                "largest": parse_percent(_cell(cells, 4)),
                "smallest": parse_percent(_cell(cells, 5)),
            }
            # As on the market page, an unobserved figure is named here so it
            # cannot reach storage as a bare null that a reader has to interpret.
            for name, value in figures.items():
                if value is None:
                    refusals.append(f"{name}:{REFUSAL_UNOBSERVED}")
            summaries.append(
                VenueFundingSummary(
                    venue=venue,
                    settlements=settlements,
                    total_paid=figures["total_paid"],
                    average_apr=figures["average_apr"],
                    largest=figures["largest"],
                    smallest=figures["smallest"],
                    last_settled=last_settled,
                    refusals=tuple(refusals),
                )
            )
        break
    if not summaries:
        raise PageShapeError(f"no funding table with the declared header on {asset_class}/{asset}")
    return FundingHistorySnapshot(
        asset_class=asset_class,
        asset=asset,
        build_time=build_time,
        window_days=window_days,
        summaries=tuple(summaries),
        received_at=received_at,
    )
