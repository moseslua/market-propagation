"""Normalization from archived wire payloads into domain records.

Every function here takes a payload that has *already* been archived and a
:class:`~market_propagation.storage.Provenance` pointing at it, so each emitted
record resolves back to the bytes it came from. Nothing in this module performs
I/O.

Schema branching is keyed off the **endpoint path**, not off a "live versus
historical" vintage. That distinction was checked against real responses and the
naive axis does not hold:

* ``GET /historical/markets`` returns the modern ``yes_bid_dollars`` /
  ``volume_fp`` schema.
* ``GET /historical/markets/{ticker}/candlesticks`` returns legacy plain names,
  ``yes_bid.open``.
* ``GET /series/{series}/markets/{ticker}/candlesticks`` returns the modern names
  *nested inside the same keys*, ``yes_bid.open_dollars``.

Two further observed asymmetries are handled rather than smoothed over:

* In a historical candle with no trade in the period, all six ``price`` keys are
  present and explicitly ``null``. In a live candle in the same situation,
  ``price`` contains only ``previous_dollars`` and the other five keys are
  *absent*. Absent and null therefore mean the same thing here — no trade — but
  only one of them is detectable by key presence, so both are read as "no trade".
* A candle carries ``yes_bid`` and ``yes_ask`` even when no trade occurred, so a
  candle is evidence about quotes and about trades independently. It is never
  evidence about book depth: it has no size, no level count and no sequence.

Candle *frequency* is also not trades. Kalshi documents intervals of 1, 60 and
1440 minutes only. Anything derived from a candle is labelled at that resolution,
and :func:`candles_to_quotes` refuses to invent a tick stream from it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..domain import (
    BookEvent,
    Clock,
    Contract,
    Operator,
    Provenance,
    Quote,
    QuoteValidity,
    Release,
    Resolution,
    Rounding,
    Trade,
)
from .kalshi_rest import (
    CANDLE_INTERVALS_MINUTES,
    COUNT_ALIASES,
    DOLLAR_ALIASES,
    parse_fixed_point_count,
    parse_fixed_point_dollars,
    pick,
)
from .macro_releases import MacroRelease
from .transport import WireShapeError

#: Candle field flavour, selected by the endpoint that was called.
CANDLE_SCHEMA_HISTORICAL = "historical_legacy_plain"
CANDLE_SCHEMA_LIVE = "live_fixed_point"

#: Kalshi's ``strike_type`` vocabulary mapped onto the frozen
#: :class:`~market_propagation.domain.Operator` enum. ``not_between`` and
#: ``custom`` have no representable member: an outside-range payoff is the
#: complement of ``range`` and a custom settlement rule is not a threshold at
#: all. They are refused rather than approximated onto a neighbouring operator,
#: because the operator is a matching field and a wrong one silently pools two
#: different claims.
_KALSHI_OPERATORS: Mapping[str, Operator] = {
    "greater": Operator.ABOVE,
    "greater_or_equal": Operator.AT_LEAST,
    "less": Operator.BELOW,
    "less_or_equal": Operator.AT_MOST,
    "equal": Operator.EQUAL,
    "between": Operator.RANGE,
}

#: Kalshi strike types the frozen operator vocabulary cannot express.
_KALSHI_UNREPRESENTABLE_STRIKES: Mapping[str, str] = {
    "not_between": "an outside-range payoff is the complement of 'range'",
    "custom": "a custom settlement rule is not a threshold comparison",
}

#: Publication source and statistic per Kalshi series. The series is a lead, not
#: a verified match, so an unlisted series publishes no ``units`` or ``source``
#: at all: both matching fields stay null rather than taking a plausible guess.
#:
#: ``Contract`` owns ``units`` and ``source``; the statistic name is audit
#: metadata, because the frozen contract record has no field for it, and it is
#: the one position where an unlisted series is named ``unmapped``.
KALSHI_SERIES_UNITS: Mapping[str, tuple[str, str, str]] = {
    "KXCPI": ("percent_mom_change", "cpi_headline_sa_mom_pct", "BLS"),
    "KXCPIYOY": ("percent_yoy_change", "cpi_headline_nsa_yoy_pct", "BLS"),
    "KXPAYROLLS": ("thousands_of_jobs", "payrolls_change_thousands", "BLS"),
    "KXU3": ("percent_level", "unemployment_rate_pct", "BLS"),
    "KXFED": ("percent_level", "fed_funds_target_upper_pct", "Federal Reserve"),
}

#: All five series settle on a US release published on an Eastern-Time schedule,
#: and Kalshi states the rule and the times in that zone.
KALSHI_RULE_TIMEZONE = "America/New_York"

#: Kalshi's binary markets pay a cash amount in US dollars.
KALSHI_SETTLEMENT = "cash"
KALSHI_CURRENCY = "USD"

#: Series to release family. The family is a matching field and its source of
#: truth is the cohort configuration, so this maps a *read* series onto that
#: vocabulary and never invents one: an unlisted series leaves the field null.
KALSHI_SERIES_FAMILY: Mapping[str, str] = {
    "KXCPI": "cpi",
    "KXCPIYOY": "cpi",
    "KXPAYROLLS": "employment",
    "KXU3": "employment",
    "KXFED": "monetary_policy",
}

#: Stated when the venue's market record carries no statistic name for a series,
#: which only the audit-side :func:`statistic_for_series` reports. The matching
#: fields a ``Contract`` owns stay null instead, because a placeholder string in
#: a matching field can be read as agreement between two unknown rules.
UNMAPPED = "unmapped"

#: The documented reciprocal relationship a public Kalshi order-book snapshot
#: implies: a NO bid at ``p`` is a YES ask at ``1 - p``. Written here because
#: ``BookEvent`` has no field for a derived side, so a reader of the emitted
#: snapshot cannot otherwise tell a derived ask from a quoted one.
KALSHI_ASKS_DERIVED_FROM_NO_BIDS = "yes_ask_derived_as_one_minus_no_bid"


def statistic_for_series(series_ticker: str | None) -> str:
    """Audit-side statistic name for a series, or ``unmapped``.

    Kept as a module function rather than a ``Contract`` field: the statistic is
    a label for reporting and family separation, not part of the frozen payoff
    record.
    """
    return KALSHI_SERIES_UNITS.get(series_ticker or "", (UNMAPPED, UNMAPPED, UNMAPPED))[1]


def rule_hash(
    rules_primary: str | None,
    rules_secondary: str | None,
    *,
    extra: Mapping[str, Any] | None = None,
) -> str:
    """Deterministic digest of the exact contract rule text.

    The digest covers only rule text, so a pure lifecycle change (a moved
    ``close_time``) does not masquerade as a rule change. That separation is what
    makes :func:`compare_contract_versions` interpretable: a settlement rule that
    silently changed mid-history invalidates comparability, while a moved close
    time usually does not.
    """
    payload = "\u241f".join([rules_primary or "", rules_secondary or ""])
    if extra:
        for key in sorted(extra):
            payload += f"\u241f{key}={extra[key]}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def stable_record_id(*parts: Any, prefix: str = "") -> str:
    """Occurrence identity from source-visible fields.

    Used only where the venue supplies no identifier of its own. A content hash
    must never replace a real per-occurrence identifier, because two genuinely
    distinct events can be byte-identical.
    """
    joined = "\u241f".join("" if p is None else str(p) for p in parts)
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()
    return f"{prefix}{digest}" if prefix else digest


def normalize_kalshi_contract(
    record: Mapping[str, Any],
    *,
    provenance: Provenance,
    series_ticker: str | None = None,
    reference_period: str | None = None,
    event_id: str | None = None,
    rule_observed_at: dt.datetime | str | None = None,
) -> Contract:
    """Normalize one ``/markets`` or ``/historical/markets`` market record.

    Accepts both the modern fixed-point schema and legacy plain names, preferring
    the modern name when both appear. The threshold is taken from ``floor_strike``
    — or ``cap_strike`` for a bounded market — and never from a price field,
    because a strike is a contract term while a price is an observation.

    ``rule_available_at`` is the instant the rule text was **observed**, and it is
    set only from ``rule_observed_at``. A market record publishes no timestamp for
    the version of its rule text, so the field defaults to ``None`` and stays there:
    ``created_time`` and ``open_time`` date the market's creation, not the rule a
    later fetch returned, and promoting either into this field would assert a
    publication time the source never stated. A caller that has read the rule text
    from a versioned source passes the instant it read it. ``rule_hash`` still covers
    the rule text alone, and the lifecycle times keep their own fields.

    The venue's own ``created_time`` is deliberately **not** preserved as a contract
    field: :class:`~market_propagation.domain.Contract` has no field for a market's
    creation instant, and adding a rule-shaped stand-in for it is the substitution
    this parameter exists to prevent. The archived record still carries it.

    Every field is constructed from a frozen domain type. A market record does
    not carry a study family, a reference period, a publication source, a
    statistic vintage or a rounding rule, so every matching field it does not
    publish stays ``None``. A null states that the venue's record is silent, and
    the caller that knows the release supplies the value; no placeholder string
    is written, because one would be read by a matcher as agreement between two
    rules that were never seen.
    A strike type the frozen operator vocabulary cannot express is refused,
    because the operator is a matching field and approximating it would silently
    pool two different claims.

    There is no ``clock`` parameter: a contract is a term rather than an
    observation, and :class:`~market_propagation.domain.Contract` has no place to
    put source or receipt times.
    """
    ticker = record.get("ticker")
    if not isinstance(ticker, str) or not ticker:
        raise WireShapeError("market record has no usable 'ticker'")

    strike_type = _as_text(record.get("strike_type"))
    if strike_type in _KALSHI_UNREPRESENTABLE_STRIKES:
        raise WireShapeError(
            f"strike_type {strike_type!r} has no frozen operator: "
            f"{_KALSHI_UNREPRESENTABLE_STRIKES[strike_type]}. The operator is a "
            "matching field, so it is refused rather than approximated."
        )
    operator = _KALSHI_OPERATORS.get(strike_type or "")
    if operator is None:
        raise WireShapeError(
            f"market {ticker!r} carries strike_type {strike_type!r}, which is not "
            f"one of {sorted(_KALSHI_OPERATORS)}"
        )

    threshold = _optional_decimal(record.get("floor_strike"), "floor_strike")
    cap = _optional_decimal(record.get("cap_strike"), "cap_strike")
    if operator is Operator.RANGE:
        # A bounded market expresses both ends as strikes and no single threshold.
        lower, upper = threshold, cap
        threshold = None
        if lower is None or upper is None:
            raise WireShapeError(
                f"market {ticker!r} is a range but carries floor_strike={lower} and "
                f"cap_strike={upper}; both bounds are required"
            )
    else:
        lower, upper = None, None
        if threshold is None:
            raise WireShapeError(
                f"market {ticker!r} is {strike_type!r} but carries no floor_strike; "
                "a threshold operator requires one"
            )

    resolved_series = (
        series_ticker or _as_text(record.get("series_ticker")) or _series_from_ticker(ticker)
    )
    mapped = KALSHI_SERIES_UNITS.get(resolved_series or "")
    units, _statistic, source = mapped if mapped is not None else (None, None, None)
    family = KALSHI_SERIES_FAMILY.get(resolved_series or "")
    rules_primary = _as_text(record.get("rules_primary"))
    rules_secondary = _as_text(record.get("rules_secondary"))
    rounding, _rounding_basis = _rounding_from_rules(rules_primary, rules_secondary)

    return Contract(
        venue="kalshi",
        contract_id=ticker,
        event_id=event_id or _as_text(record.get("event_ticker")) or UNMAPPED,
        family=family,
        reference_period=reference_period,
        source=source,
        units=units,
        operator=operator,
        threshold=threshold,
        lower=lower,
        upper=upper,
        rounding=rounding,
        # The market record publishes no statistic vintage, and a null states
        # that absence rather than asserting "initial" from a status string.
        vintage=None,
        timezone=KALSHI_RULE_TIMEZONE,
        # The latest instant the claim can expire is the payoff deadline. Trading
        # close is a separate lifecycle fact and stays in ``close_time``.
        deadline=_parse_optional_time(
            record.get("latest_expiration_time") or record.get("expected_expiration_time")
        ),
        settlement=KALSHI_SETTLEMENT,
        currency=KALSHI_CURRENCY,
        exceptional_policy=_exceptional_policy(rules_primary),
        open_time=_parse_optional_time(record.get("open_time")),
        close_time=_parse_optional_time(record.get("close_time")),
        resolve_time=_parse_optional_time(record.get("settlement_ts")),
        rule_hash=rule_hash(rules_primary, rules_secondary),
        provenance=provenance,
        # The rule text's own observation instant, never the market's creation time.
        rule_available_at=_parse_optional_time(rule_observed_at),
    )


def compare_contract_versions(left: Contract, right: Contract) -> dict[str, Any]:
    """Report whether two vintages of one contract differ in rule or lifecycle.

    The distinction matters for comparability. A changed rule text means the
    contract is not the same instrument and its history cannot be pooled with the
    other vintage. A changed ``close_time`` or ``resolve_time`` under identical
    rule text is a lifecycle edit and is reported separately.
    """
    rule_fields = ("rule_hash", "threshold", "lower", "upper", "operator", "units", "rounding")
    lifecycle_fields = ("open_time", "close_time", "resolve_time", "deadline")
    rule_differences = {
        name: (getattr(left, name, None), getattr(right, name, None))
        for name in rule_fields
        if getattr(left, name, None) != getattr(right, name, None)
    }
    lifecycle_differences = {
        name: (getattr(left, name, None), getattr(right, name, None))
        for name in lifecycle_fields
        if getattr(left, name, None) != getattr(right, name, None)
    }
    return {
        "contract_id": getattr(left, "contract_id", None),
        "rule_text_changed": bool(rule_differences),
        "lifecycle_changed": bool(lifecycle_differences),
        "rule_differences": {k: [str(v[0]), str(v[1])] for k, v in rule_differences.items()},
        "lifecycle_differences": {
            k: [str(v[0]), str(v[1])] for k, v in lifecycle_differences.items()
        },
        "comparable": not rule_differences,
        "note": (
            "rule text changed: treat as a different instrument"
            if rule_differences
            else (
                "identical rule text; lifecycle times differ"
                if lifecycle_differences
                else "identical"
            )
        ),
    }


def contract_is_closed(contract: Contract, *, at: dt.datetime) -> bool:
    """True when the contract's own lifecycle rejected trades at ``at``.

    Used by the audit to classify a post-release window with no quotes as a
    closed-market exclusion rather than as a null observation. The two look
    identical in the data and mean opposite things.
    """
    close_time = getattr(contract, "close_time", None)
    if close_time is None:
        return False
    return _as_utc(at) > _as_utc(close_time)


def normalize_kalshi_trade(
    record: Mapping[str, Any],
    *,
    clock: Clock,
    provenance: Provenance,
    venue: str = "kalshi",
) -> Trade:
    """Normalize one trade record from ``/historical/trades`` or ``/markets/trades``.

    Direction comes from ``taker_outcome_side`` or ``taker_book_side``, which
    Kalshi documents as canonical; ``taker_side`` is deprecated and is compared
    against them by :func:`trade_direction_conflicts` rather than stored here.

    The aggressor is left ``None`` rather than inferred from the side of the
    book, because the documented fields describe the taker's position, not
    whether the taker was the aggressor on a given fill. The frozen
    :class:`~market_propagation.domain.Trade` has no field for a schema flavour,
    a deprecated side or a NO price, and none of them is needed to read a print:
    the archived payload named by ``provenance.raw_hash`` still carries them.
    """
    ticker = record.get("ticker")
    if not isinstance(ticker, str) or not ticker:
        raise WireShapeError("trade record has no usable 'ticker'")

    price = parse_fixed_point_dollars(
        pick(record, DOLLAR_ALIASES["last_price"] + ("yes_price_dollars", "yes_price")),
        "yes_price_dollars",
    )
    if price is None:
        raise WireShapeError(
            f"trade {record.get('trade_id')!r} carries no yes price; a trade "
            "without a price cannot be normalized"
        )
    size = parse_fixed_point_count(pick(record, COUNT_ALIASES["count"]), "count_fp")
    if size is None:
        raise WireShapeError(f"trade {record.get('trade_id')!r} carries no count_fp")

    trade_id = record.get("trade_id")
    if trade_id is not None and not isinstance(trade_id, str):
        trade_id = str(trade_id)

    is_block = record.get("is_block_trade")
    if not isinstance(is_block, bool):
        is_block = None

    return Trade(
        venue=venue,
        contract_id=ticker,
        trade_id=trade_id,
        price=price,
        size=size,
        clock=clock,
        provenance=provenance,
        aggressor=None,
        is_block=is_block,
    )


def trade_direction_conflicts(record: Mapping[str, Any]) -> dict[str, Any]:
    """Compare the deprecated side field with the canonical ones.

    Reporting a disagreement is the point. The deprecated field is documented as
    preserving its old meaning until it is removed, and a mismatch between it and
    ``taker_outcome_side`` is a real data-quality signal rather than something to
    reconcile silently.
    """
    legacy = _as_text(record.get("taker_side"))
    outcome = _as_text(record.get("taker_outcome_side"))
    book = _as_text(record.get("taker_book_side"))
    book_implied = {"bid": "yes", "ask": "no"}.get(book or "")
    agents = {"legacy": legacy, "outcome": outcome, "book_implied": book_implied}
    present = {k: v for k, v in agents.items() if v}
    distinct = set(present.values())
    return {
        "agents": agents,
        "present": sorted(present),
        "conflict": len(distinct) > 1,
        "note": (
            "deprecated and canonical side fields disagree; direction is not usable for this record"
            if len(distinct) > 1
            else "direction fields agree"
        ),
    }


@dataclass(frozen=True, slots=True)
class CandleObservation:
    """One candle, with quote and trade legs kept apart.

    ``yes_bid``/``yes_ask`` are *quotes* — offer prices Kalshi reports for the
    period. ``trade_*`` is the trade-price distribution, volume-weighted mean
    included. They are separate because a candle can have quotes and no trades,
    which the real payloads show as a populated ``yes_bid`` next to a ``null``
    ``price``.

    ``book_depth_available`` is always ``False``. A candle carries no sizes, no
    level count and no sequence, so nothing derived from it may be described as
    an order book.
    """

    venue: str
    contract_id: str
    interval_minutes: int
    end_period_ts: int
    period_end: dt.datetime
    clock: Clock
    provenance: Provenance
    bid_open: Decimal | None = None
    bid_high: Decimal | None = None
    bid_low: Decimal | None = None
    bid_close: Decimal | None = None
    ask_open: Decimal | None = None
    ask_high: Decimal | None = None
    ask_low: Decimal | None = None
    ask_close: Decimal | None = None
    trade_open: Decimal | None = None
    trade_high: Decimal | None = None
    trade_low: Decimal | None = None
    trade_close: Decimal | None = None
    trade_mean: Decimal | None = None
    trade_previous: Decimal | None = None
    volume: Decimal | None = None
    open_interest: Decimal | None = None
    schema_flavour: str = CANDLE_SCHEMA_HISTORICAL
    trade_ohlc_present: bool = False
    trade_keys_present: bool = False
    book_depth_available: bool = False
    resolution_seconds: int = 60

    @property
    def has_two_sided_quote(self) -> bool:
        return self.bid_close is not None and self.ask_close is not None

    @property
    def crossed(self) -> bool:
        if not self.has_two_sided_quote:
            return False
        assert self.bid_close is not None and self.ask_close is not None
        return self.bid_close > self.ask_close

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "contract_id": self.contract_id,
            "interval_minutes": self.interval_minutes,
            "end_period_ts": self.end_period_ts,
            "period_end": self.period_end.isoformat(),
            "bid_close": _dec(self.bid_close),
            "ask_close": _dec(self.ask_close),
            "trade_close": _dec(self.trade_close),
            "volume": _dec(self.volume),
            "open_interest": _dec(self.open_interest),
            "schema_flavour": self.schema_flavour,
            "trade_ohlc_present": self.trade_ohlc_present,
            "trade_keys_present": self.trade_keys_present,
            "book_depth_available": self.book_depth_available,
            "resolution_seconds": self.resolution_seconds,
            "usable_time": self.clock.usable_time.isoformat() if self.clock.usable_time else None,
            "raw_hash": self.provenance.raw_hash,
        }


def normalize_kalshi_candle(
    candle: Mapping[str, Any],
    *,
    contract_id: str,
    interval_minutes: int,
    schema_flavour: str,
    clock: Clock,
    provenance: Provenance,
    venue: str = "kalshi",
) -> CandleObservation:
    """Normalize one candle from either candle endpoint.

    ``schema_flavour`` selects the field naming and must be passed explicitly,
    because the two endpoints use identical top-level keys with different leaf
    names. Guessing would silently produce ``None`` prices for one of them.
    """
    if interval_minutes not in CANDLE_INTERVALS_MINUTES:
        raise ValueError(
            f"interval_minutes must be one of {CANDLE_INTERVALS_MINUTES}; got {interval_minutes}"
        )
    if schema_flavour not in (CANDLE_SCHEMA_HISTORICAL, CANDLE_SCHEMA_LIVE):
        raise ValueError(f"unknown candle schema flavour {schema_flavour!r}")

    raw_end = candle.get("end_period_ts")
    if raw_end is None:
        raise WireShapeError("candle has no end_period_ts; its time is unknown")
    end_ts = int(raw_end)
    period_end = dt.datetime.fromtimestamp(end_ts, tz=dt.UTC)

    legacy = schema_flavour == CANDLE_SCHEMA_HISTORICAL
    bid = candle.get("yes_bid")
    ask = candle.get("yes_ask")
    price = candle.get("price")
    bid = bid if isinstance(bid, Mapping) else {}
    ask = ask if isinstance(ask, Mapping) else {}
    price = price if isinstance(price, Mapping) else {}

    def leg(node: Mapping[str, Any], name: str) -> Decimal | None:
        key = name if legacy else f"{name}_dollars"
        return parse_fixed_point_dollars(node.get(key), key)

    trade_keys_present = bool(price) and any((name in price) for name in ("open", "open_dollars"))
    trade_open = leg(price, "open")
    trade_high = leg(price, "high")
    trade_low = leg(price, "low")
    trade_close = leg(price, "close")
    trade_mean = leg(price, "mean")
    trade_previous = leg(price, "previous")
    trade_ohlc_present = trade_close is not None

    volume_key = "volume" if legacy else "volume_fp"
    oi_key = "open_interest" if legacy else "open_interest_fp"

    return CandleObservation(
        venue=venue,
        contract_id=contract_id,
        interval_minutes=interval_minutes,
        end_period_ts=end_ts,
        period_end=period_end,
        clock=clock,
        provenance=provenance,
        bid_open=leg(bid, "open"),
        bid_high=leg(bid, "high"),
        bid_low=leg(bid, "low"),
        bid_close=leg(bid, "close"),
        ask_open=leg(ask, "open"),
        ask_high=leg(ask, "high"),
        ask_low=leg(ask, "low"),
        ask_close=leg(ask, "close"),
        trade_open=trade_open,
        trade_high=trade_high,
        trade_low=trade_low,
        trade_close=trade_close,
        trade_mean=trade_mean,
        trade_previous=trade_previous,
        volume=parse_fixed_point_count(candle.get(volume_key), volume_key),
        open_interest=parse_fixed_point_count(candle.get(oi_key), oi_key),
        schema_flavour=schema_flavour,
        trade_ohlc_present=trade_ohlc_present,
        trade_keys_present=trade_keys_present,
        book_depth_available=False,
        resolution_seconds=interval_minutes * 60,
    )


def normalize_kalshi_candles(
    candles: Iterable[Mapping[str, Any]],
    *,
    contract_id: str,
    interval_minutes: int,
    schema_flavour: str,
    provenance: Provenance,
    received_time: dt.datetime,
    monotonic_ns: int | None = None,
    venue: str = "kalshi",
    uncertainty_seconds: float = 0.0,
) -> list[CandleObservation]:
    """Normalize a candle series, stamping each candle with its period-end time.

    Availability is ``captured``: the observation's source time is the period
    end, which the venue states, and its receipt time is the actual response
    time, which this process observed. Both are real, so the usable upper bound
    is the later of receipt and period end. A candle that describes a period
    ending before the response arrived is published history, and its upper bound
    is the receipt instant, because that is when this system could first have
    seen it.
    """
    out: list[CandleObservation] = []
    for candle in candles:
        raw_end = candle.get("end_period_ts")
        period_end = (
            dt.datetime.fromtimestamp(int(raw_end), tz=dt.UTC) if raw_end is not None else None
        )
        source = period_end or received_time
        clock = Clock.captured(
            source,
            received_time,
            monotonic_ns=monotonic_ns,
            uncertainty_seconds=uncertainty_seconds,
        )
        out.append(
            normalize_kalshi_candle(
                candle,
                contract_id=contract_id,
                interval_minutes=interval_minutes,
                schema_flavour=schema_flavour,
                clock=clock,
                provenance=provenance,
                venue=venue,
            )
        )
    return out


def candles_to_quotes(
    candles: Sequence[CandleObservation],
    *,
    require_two_sided: bool = True,
) -> list[Quote]:
    """Convert candle quote legs into :class:`Quote` records at candle resolution.

    The emitted quote is stamped with the candle's ``replay_order`` of
    ``candle_close`` and a validity of ``valid`` only when both sides are present
    and the market is not crossed. Depth is left empty, because a candle has no
    size information, and a consumer that needs depth must not read zeros here as
    an empty book.

    This is explicitly *not* a reconstruction of a tick feed. The caller is told
    the resolution through ``last_verified`` and the returned record count.
    """
    quotes: list[Quote] = []
    for candle in candles:
        if require_two_sided and not candle.has_two_sided_quote:
            validity = QuoteValidity.MISSING
        elif candle.crossed:
            validity = QuoteValidity.CROSSED
        else:
            validity = QuoteValidity.VALID
        quotes.append(
            Quote(
                venue=candle.venue,
                contract_id=candle.contract_id,
                clock=candle.clock,
                provenance=candle.provenance,
                bid=candle.bid_close,
                ask=candle.ask_close,
                bid_size=None,
                ask_size=None,
                validity=validity,
                last_price_change=None,
                last_verified=candle.period_end,
                last_trade=candle.period_end if candle.trade_ohlc_present else None,
                replay_order="candle_close",
            )
        )
    return quotes


def normalize_kalshi_orderbook_snapshot(
    payload: Mapping[str, Any],
    *,
    contract_id: str,
    clock: Clock,
    provenance: Provenance,
    connection_id: str = "",
    venue: str = "kalshi",
) -> list[BookEvent]:
    """Turn a public order-book snapshot into a single snapshot ``BookEvent``.

    Kalshi documents that this response contains bids only: a NO bid at price
    ``p`` is a YES ask at ``1 - p``. Both sides are therefore derived, and
    :data:`KALSHI_ASKS_DERIVED_FROM_NO_BIDS` names that derivation, because the
    frozen snapshot carries no field in which to label a side as derived.

    There is no sequence number in the response, so ``sequence`` stays ``None``.
    A snapshot cannot close a gap, and pretending otherwise would mark a
    reconstruction valid on no evidence.
    """
    book = payload.get("orderbook_fp")
    flavour = "orderbook_fp"
    if not isinstance(book, Mapping):
        book = payload.get("orderbook")
        flavour = "orderbook"
    if not isinstance(book, Mapping):
        raise WireShapeError("orderbook response carries neither 'orderbook_fp' nor 'orderbook'")

    yes_key = "yes_dollars" if flavour == "orderbook_fp" else "yes"
    no_key = "no_dollars" if flavour == "orderbook_fp" else "no"
    bids = _levels(book.get(yes_key), "yes")
    no_bids = _levels(book.get(no_key), "no")

    # Selling YES is buying NO at one minus the price, per the documented
    # reciprocal relationship.
    derived_asks: list[tuple[Decimal, Decimal]] = []
    for price, size in no_bids:
        ask = Decimal(1) - price
        if ask <= 0 or size <= 0:
            continue
        derived_asks.append((ask, size))
    derived_asks.sort(key=lambda level: level[0])

    return [
        BookEvent(
            venue=venue,
            contract_id=contract_id,
            kind="snapshot",
            clock=clock,
            provenance=provenance,
            connection_id=connection_id,
            sequence_scope=f"{venue}:{contract_id}:snapshot",
            sequence=None,
            bids=tuple(bids),
            asks=tuple(derived_asks),
            side=None,
            price=None,
            size=None,
            operation="replace",
        )
    ]


def _levels(node: Any, field: str) -> list[tuple[Decimal, Decimal]]:
    if node is None:
        return []
    if not isinstance(node, list):
        raise WireShapeError(f"order book {field} side is not a list")
    levels: list[tuple[Decimal, Decimal]] = []
    for entry in node:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            raise WireShapeError(f"malformed {field} book level: {entry!r}")
        price = parse_fixed_point_dollars(entry[0], f"{field}_price")
        size = parse_fixed_point_count(entry[1], f"{field}_size")
        # A snapshot level is only a resting order when both terms are positive.
        # A zero size is a removal observation and a zero price is not a tradeable
        # level; neither is a book level, and the frozen BookEvent refuses both.
        if price is None or size is None or price <= 0 or size <= 0:
            continue
        levels.append((price, size))
    return levels


_KINDS = (
    "kalshi_trade",
    "kalshi_orderbook",
    "kalshi_candle",
    "polymarket_price_history",
    "polymarket_book",
)


def normalize(
    raw: Any,
    *,
    venue: str,
    clock: Clock,
    provenance: Provenance,
    kind: str | None = None,
    **options: Any,
) -> list[Any]:
    """Normalize an archived payload into domain records.

    ``kind`` is inferred from the payload shape when omitted. Inference covers
    trades, order books and candle series; a market record has no single-record
    domain type and raises with the function to use instead, rather than
    returning an empty list that a caller could read as success.

    Candle normalization requires ``interval_minutes`` and ``schema_flavour``
    because the endpoint, not the payload, determines the field naming.
    """
    resolved = kind or _infer_kind(raw, venue)
    if resolved == "kalshi_trade":
        if not isinstance(raw, Mapping):
            raise WireShapeError("a trade payload must be a JSON object")
        return [
            normalize_kalshi_trade(
                raw,
                clock=clock,
                provenance=provenance,
                venue=venue,
            )
        ]
    if resolved == "kalshi_orderbook":
        if not isinstance(raw, Mapping):
            raise WireShapeError("an order book payload must be a JSON object")
        contract_id = options.get("contract_id") or raw.get("ticker")
        if not contract_id:
            raise WireShapeError("order book normalization needs a contract_id")
        return normalize_kalshi_orderbook_snapshot(
            raw,
            contract_id=str(contract_id),
            clock=clock,
            provenance=provenance,
            connection_id=options.get("connection_id", ""),
            venue=venue,
        )
    if resolved == "kalshi_candle":
        candles = raw
        if isinstance(raw, Mapping):
            candles = raw.get("candlesticks")
        if not isinstance(candles, list):
            raise WireShapeError("a candle payload must carry a 'candlesticks' list")
        contract_id = options.get("contract_id")
        if not contract_id:
            raise WireShapeError("candle normalization needs a contract_id")
        interval = options.get("interval_minutes")
        if interval is None:
            raise WireShapeError(
                "candle normalization needs interval_minutes; the interval is a "
                "property of the request, not of the response"
            )
        flavour = options.get("schema_flavour")
        if flavour is None:
            raise WireShapeError(
                "candle normalization needs schema_flavour; the two candle endpoints "
                "share key names with different leaf names"
            )
        observations = normalize_kalshi_candles(
            candles,
            contract_id=str(contract_id),
            interval_minutes=int(interval),
            schema_flavour=str(flavour),
            provenance=provenance,
            received_time=options.get("received_time")
            or clock.received_time
            or dt.datetime.now(dt.UTC),
            monotonic_ns=options.get("monotonic_ns", clock.monotonic_ns),
            venue=venue,
            uncertainty_seconds=options.get("uncertainty_seconds", 0.0),
        )
        if options.get("as_quotes"):
            return candles_to_quotes(observations)
        return list(observations) + candles_to_quotes(observations)
    if resolved == "polymarket_price_history":
        from .polymarket_public import normalize_price_history

        contract_id = options.get("contract_id")
        if not contract_id:
            raise WireShapeError("price history normalization needs a contract_id")
        return list(
            normalize_price_history(
                raw,
                contract_id=str(contract_id),
                clock=clock,
                provenance=provenance,
                **{k: v for k, v in options.items() if k != "contract_id"},
            )
        )
    if resolved == "polymarket_book":
        from .polymarket_public import normalize_book_snapshot

        contract_id = options.get("contract_id")
        if not contract_id:
            raise WireShapeError("book normalization needs a contract_id")
        return list(
            normalize_book_snapshot(
                raw,
                contract_id=str(contract_id),
                clock=clock,
                provenance=provenance,
                **{k: v for k, v in options.items() if k not in ("contract_id",)},
            )
        )
    if resolved in ("kalshi_market", "polymarket_market", "contract"):
        raise TypeError(
            "market records normalize to a Contract through "
            "normalize_kalshi_contract(...), not through normalize(); a contract is "
            "a term rather than an observation"
        )
    raise WireShapeError(
        f"cannot infer a normalization kind for venue {venue!r}; pass kind= one of {_KINDS}"
    )


def _infer_kind(raw: Any, venue: str) -> str:
    if isinstance(raw, list):
        first = next((item for item in raw if isinstance(item, Mapping)), None)
        if first is not None and {"t", "p"} <= set(first):
            return "polymarket_price_history"
        if first is not None and ("end_period_ts" in first):
            return "kalshi_candle"
        return "unknown"
    if not isinstance(raw, Mapping):
        return "unknown"
    if "trade_id" in raw and "ticker" in raw:
        return "kalshi_trade"
    if "orderbook_fp" in raw or "orderbook" in raw:
        return "kalshi_orderbook"
    if "candlesticks" in raw:
        return "kalshi_candle"
    if "history" in raw:
        return "polymarket_price_history"
    if "bids" in raw and "asks" in raw:
        return "polymarket_book"
    if "ticker" in raw and "status" in raw:
        return "kalshi_market"
    return "unknown"


def resolve_resolution_payout(record: Mapping[str, Any], *, provenance: Provenance) -> Resolution:
    """Build a :class:`Resolution` from a settled market record.

    ``known_at`` is left ``None`` unless the venue states when the outcome became
    known. A settlement timestamp is a venue action, not proof of when the
    information was public, so it is recorded as ``resolved_at`` only.
    """
    ticker = _as_text(record.get("ticker")) or ""
    result = _as_text(record.get("result"))
    if result == "yes":
        payout = Decimal(1)
    elif result == "no":
        payout = Decimal(0)
    else:
        settled = parse_fixed_point_dollars(
            record.get("settlement_value_dollars"), "settlement_value_dollars"
        )
        if settled is None:
            raise WireShapeError(f"settled market {ticker!r} has no result and no settlement value")
        payout = settled
    rules_primary = _as_text(record.get("rules_primary"))
    rules_secondary = _as_text(record.get("rules_secondary"))
    return Resolution(
        contract_id=ticker,
        payout=payout,
        known_at=None,
        resolved_at=_parse_optional_time(record.get("settlement_ts")),
        rule_hash=rule_hash(rules_primary, rules_secondary),
        provenance=provenance,
        exceptional=payout not in (Decimal(0), Decimal(1)),
    )


def release_from_payload(release: MacroRelease) -> Release:
    """Adapt a parsed BLS release into the frozen :class:`Release` record.

    ``scheduled_at`` is the release's scheduled instant, while the clock's
    availability carries when the payload could have been seen. The two are not
    collapsed: a scheduled time is a plan and an availability time is an
    observation. The observed publication instant stays reachable as
    ``Release.observed_at``, which derives from ``clock.source_time``.

    Audit metadata the frozen record does not own — the payload's own embargo
    agreement, its USDL number, the unit map, the sentence each value came from
    and the source URL — is not attached here. It remains on the parsed
    :class:`~market_propagation.ingest.macro_releases.MacroRelease` and in the
    archived payload named by ``provenance.raw_hash``.
    """
    return Release(
        event_id=release.event_id,
        family=release.family,
        scheduled_at=release.scheduled_at,
        reference_period=release.reference_period,
        values=dict(release.values),
        clock=release.clock,
        provenance=release.provenance,
        revisions=dict(release.revisions),
    )


def _optional_decimal(value: Any, field: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise WireShapeError(f"{field!r} is a boolean")
    return parse_fixed_point_dollars(value, field)


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _parse_optional_time(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = dt.datetime.fromtimestamp(float(value), tz=dt.UTC)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise WireShapeError(f"unparseable timestamp {value!r}") from exc
    else:
        raise WireShapeError(f"unsupported timestamp type {type(value).__name__}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(dt.UTC)


def _series_from_ticker(ticker: str) -> str:
    """Recover the series prefix from a Kalshi market ticker.

    Kalshi tickers are ``SERIES-EVENT-STRIKE``; the series is everything before
    the first hyphen.
    """
    return ticker.split("-", 1)[0]


def _rounding_from_rules(
    rules_primary: str | None, rules_secondary: str | None
) -> tuple[Rounding | None, str]:
    """Read the statistic rounding rule from the contract's own rule text.

    ``Rounding`` governs how the reference statistic is quantized before it is
    compared with the threshold, which is why it is a matching field: a different
    rule moves the payoff boundary.

    The market record's ``price_level_structure`` is deliberately *not* used
    here. That field describes the price tick grid the venue quotes on
    (``linear_cent``), which is a statement about prices rather than about the
    statistic being compared, and mapping one onto the other would invent a rule
    the venue never published. It stays in the archived payload named by
    ``provenance.raw_hash``.

    When neither rule text states a rounding rule the rounding itself is
    ``None``, because the market publishes no rounding rule. That is a different
    fact from a rule text that states rounding is not applied, which yields
    ``Rounding.NONE``; the returned basis keeps the two distinguishable.
    """
    text = " ".join(part for part in (rules_primary, rules_secondary) if part).lower()
    if text:
        for marker, rounding in (
            ("rounded to the nearest", Rounding.NEAREST),
            ("rounded up", Rounding.UP),
            ("rounded down", Rounding.DOWN),
            ("without rounding", Rounding.NONE),
            ("not rounded", Rounding.NONE),
        ):
            if marker in text:
                return rounding, f"rule_text_states:{marker.replace(' ', '_')}"
    return None, "no_rounding_rule_published_on_market_record"


def _exceptional_policy(rules_primary: str | None) -> str | None:
    """Record how the rule text treats a non-binary outcome.

    With no rule text at all there is nothing to read, so the answer is ``None``:
    the literal string ``unknown`` would sit in a required matching field and
    read as agreement between two contracts whose rules were never seen.
    """
    if not rules_primary:
        return None
    lowered = rules_primary.lower()
    for marker, policy in (
        ("not applicable", "not_applicable"),
        ("void", "void"),
        ("no payout", "zero_payout"),
        ("0.5", "half_payout"),
    ):
        if marker in lowered:
            return policy
    return "binary_default"


def _dec(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "CANDLE_SCHEMA_HISTORICAL",
    "CANDLE_SCHEMA_LIVE",
    "KALSHI_ASKS_DERIVED_FROM_NO_BIDS",
    "KALSHI_SERIES_FAMILY",
    "KALSHI_SERIES_UNITS",
    "UNMAPPED",
    "CandleObservation",
    "candles_to_quotes",
    "compare_contract_versions",
    "contract_is_closed",
    "normalize",
    "normalize_kalshi_candle",
    "normalize_kalshi_candles",
    "normalize_kalshi_contract",
    "normalize_kalshi_orderbook_snapshot",
    "normalize_kalshi_trade",
    "release_from_payload",
    "resolve_resolution_payout",
    "rule_hash",
    "stable_record_id",
    "statistic_for_series",
    "trade_direction_conflicts",
]
