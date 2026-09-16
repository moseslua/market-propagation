"""BLS macro-release calendar and archived first-release payloads.

Three distinct data needs, deliberately kept separate:

1. **Schedule.** The release calendar stamps every entry in Eastern Time and
   states so explicitly. Times are resolved through the ``zoneinfo`` database
   rather than a fixed UTC offset, so the November and March daylight-saving
   transitions are handled by the tz database.
2. **First release.** The archived payload for the publication date is the
   initial-release text. Its numbers are what a market could first have seen,
   and it is archived before parsing.
3. **Revisions.** The Employment Situation archive states revised values for
   prior months inside a later release. Those are captured as ``revisions`` and
   never substituted for the first-release value of the month they revise.

Three upstream quirks are handled rather than papered over:

* A stated change carries its direction in a verb, not in the number: the
  releases print "decreased 0.1 percent" and "fell 2.4 percent", never
  "-0.1 percent". The verb is read as the release's own statement of direction,
  so a decrease is signed negative rather than read as a rise, and a statistic
  the release says was unchanged is recorded as a stated zero rather than as a
  missing value. An index level stays a level and is never re-signed by a
  change's direction, and monthly and 12-month statements are told apart by their
  own wording so an annual figure is never read into a monthly field. A statistic
  the release does not state stays absent.

* The calendar is JS-free HTML with a table of ``MM:SS AM`` times and no zone
  abbreviation, and a trailing note confirming Eastern Time. The parser requires
  that note; without it the times would be ambiguous and are reported as such.
* Some months were never published. October 2025 CPI and October 2025 Employment
  Situation are documented on the archive indexes as unpublished because of the
  2025 lapse in federal appropriations. A missing release is reported as an
  explicit gap with that reason, never as an empty release.

Nothing here fetches or invents a consensus forecast. A vendor consensus is not
assumed free or licensed, and this module does not substitute a revised series
or a convenient midpoint for one.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..storage import RawStore, read_parquet
from .transport import HttpTransport, TransportError, WireShapeError, blocked_record

BLS_CALENDAR_PATH = "/schedule/{year}/{month:02d}_sched_list.htm"
BLS_ARCHIVE_INDEX = {
    "cpi": "https://www.bls.gov/bls/news-release/cpi.htm",
    "empsit": "https://www.bls.gov/bls/news-release/empsit.htm",
}
BLS_ARCHIVE_PAYLOAD = "https://www.bls.gov/news.release/archives/{slug}.htm"
BLS_ICS_FEED = "https://www.bls.gov/schedule/news_release/bls.ics"
BLS_CALENDAR_NOTE = "All times on calendar are Eastern Time"

#: The tz database name the calendar resolves against. The ICS feed's own
#: ``X-WR-TIMEZONE`` is ``US-Eastern`` with daylight rules inline, which agrees
#: with ``America/New_York``.
BLS_TIMEZONE = "America/New_York"

#: Documented reason for a genuinely absent release.
UNPUBLISHED_REASON = "not published because of 2025 lapse in federal government appropriations"

#: Release families in scope, mapped to the archive slug naming convention.
RELEASE_FAMILIES: Mapping[str, str] = {
    "Consumer Price Index": "cpi",
    "Employment Situation": "empsit",
}

#: How a parsed first release reached this process. A live fetch and a payload read
#: back out of a sealed dataset leave their bytes in different raw stores, so the
#: record names which one produced them rather than leaving it to a caller's note.
ACQUISITION_NETWORK = "network_get"
ACQUISITION_SEALED_DATASET = "sealed_release_dataset"

#: The table a sealed archived-release dataset must declare.
RELEASE_DATASET_TABLE = "releases"

_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_CALENDAR_ROW = re.compile(
    r"<tr>\s*<td[^>]*>(?P<date>[^<]*)</td>\s*<td[^>]*>(?P<time>[^<]*)</td>\s*"
    r"<td[^>]*>(?P<release>.*?)</td>\s*</tr>",
    re.IGNORECASE | re.DOTALL,
)
_TIME = re.compile(r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*(?P<ampm>[AaPp][Mm])")
_DATE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s*(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})"
)
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
# "October 2025 Consumer Price Index - Not published because of ...". The period
# is this entry's own leading "<Month> <Year>" and the title is the rest of the
# entry up to the dash. The title may not run into a later entry: a second
# "<Month> <Year>" token is a boundary, which is what stops a match started at an
# earlier entry from absorbing every entry between it and the dash.
_UNPUBLISHED = re.compile(
    r"(?P<reference_period>[A-Z][a-z]+ \d{4})"
    r"(?P<title>(?:(?!\s*\b[A-Z][a-z]+ \d{4}\b)[^<])*?)"
    r"\s*[\u2013-]\s*Not published",
    re.IGNORECASE,
)
_ARCHIVE_LINK = re.compile(
    r'href="(?P<href>/news\.release/archives/(?P<slug>[a-z]+)_(?P<stamp>\d{8})\.htm)"',
    re.IGNORECASE,
)

# The release states a change as a direction verb plus an unsigned magnitude, so
# "decreased 0.1 percent" is a fall of one tenth and the verb is the only thing
# that says so. Each statistic is signed by the verb it was read with, and one
# table backs both the alternation and the sign lookup so the two cannot drift
# apart.
_DIRECTION_PHRASES: tuple[tuple[str, int], ...] = (
    ("remained unchanged", 0),
    ("changed little", 0),
    ("was unchanged", 0),
    ("were unchanged", 0),
    ("edged up", 1),
    ("edged down", -1),
    ("increased", 1),
    ("advanced", 1),
    ("gained", 1),
    ("decreased", -1),
    ("declined", -1),
    ("dropped", -1),
    ("rose", 1),
    ("fell", -1),
)

_DIRECTION: Mapping[str, int] = dict(_DIRECTION_PHRASES)

# The phrasings that state no change at all. These carry no magnitude, so they
# are read as a stated zero. ``changed little`` is deliberately not one of them:
# it is a small change of unstated size, so treating it as zero would invent the
# magnitude the release declined to state.
_UNCHANGED_PHRASES: tuple[str, ...] = ("was unchanged", "remained unchanged", "were unchanged")
_UNCHANGED_VERB = "(?:" + "|".join(_UNCHANGED_PHRASES) + ")"

# Longest phrase first, so a multi-word direction is never shadowed by a shorter
# one that prefixes it.
_DIRECTION_VERB = (
    "(?:"
    + "|".join(
        re.escape(phrase)
        for phrase, _ in sorted(_DIRECTION_PHRASES, key=lambda pair: -len(pair[0]))
    )
    + ")"
)

# An amount whose own tail names a 12-month span is an annual figure, and the
# monthly and annual statements share both verbs and unit, so an annual figure
# must never be read into a monthly field.
_NOT_ANNUAL = (
    r"(?!\s*(?:over the (?:last|past|prior|year)|for the 12 months|for 12 months|12-month))"
)

_NUMBER = r"\d+(?:\.\d+)?"
_PERCENT_UNIT = rf"(?P<value>{_NUMBER})\s*percent"
_COUNT_UNIT = r"(?P<value>[\d,]+(?:\.\d+)?)"


def resolve_timezone(name: str = BLS_TIMEZONE) -> ZoneInfo:
    """Return the release timezone, failing loudly if the tz database is absent.

    A missing tz database is a real deployment fault: silently falling back to a
    fixed offset would misdate every release by an hour across a DST change.
    """
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:  # pragma: no cover - environment fault
        raise RuntimeError(
            f"timezone database entry {name!r} is unavailable; release times cannot "
            "be resolved safely without it"
        ) from exc


@dataclass(frozen=True, slots=True)
class CalendarEntry:
    """One scheduled release. ``scheduled_at`` is exact-instant UTC."""

    family: str
    release_title: str
    scheduled_at: dt.datetime
    local_time: dt.datetime
    timezone: str
    reference_period: str | None
    calendar_url: str
    raw_hash: str | None = None
    preliminary_or_revised: bool = False

    @property
    def utc_offset_seconds(self) -> int:
        offset = self.local_time.utcoffset()
        return int(offset.total_seconds()) if offset is not None else 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "release_title": self.release_title,
            "scheduled_at": self.scheduled_at.isoformat(),
            "local_time": self.local_time.isoformat(),
            "timezone": self.timezone,
            "utc_offset_seconds": self.utc_offset_seconds,
            "reference_period": self.reference_period,
            "calendar_url": self.calendar_url,
            "raw_hash": self.raw_hash,
            "preliminary_or_revised": self.preliminary_or_revised,
            "time_precision": "minute",
        }


@dataclass(frozen=True, slots=True)
class UnpublishedRelease:
    """A month the source index documents as never published."""

    family: str
    reference_period: str
    reason: str
    index_url: str
    raw_hash: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "reference_period": self.reference_period,
            "reason": self.reason,
            "index_url": self.index_url,
            "raw_hash": self.raw_hash,
            "status": "unpublished",
        }


@dataclass(frozen=True, slots=True)
class MacroRelease:
    """A parsed first-release payload with its archived provenance.

    ``values`` holds only statistics the release actually states in its own
    lead text. ``revisions`` holds revised values the release discloses for
    *earlier* months, keyed by statistic, and is never merged into ``values``.

    ``scheduled_at`` is the release's scheduled instant, from the cohort
    configuration's calendar. ``embargo_time_from_payload`` is the instant named by
    the payload's **own embargo line**, which is a source claim about the
    schedule rather than an observation of publication: the document was fetched
    after the fact, so reading it cannot establish when the material first became
    public. ``schedule_agreement`` records whether the two agree, and nothing here
    treats agreement as evidence of observation.

    ``clock`` carries availability separately from both. A historical clock
    built from a scheduled instant has an **unknown** availability interval
    (:meth:`~market_propagation.domain.Clock.historical` with no receipt time), so
    the scheduled time cannot leak into a point-in-time feature as if the system
    had observed the payload at publication.

    ``acquisition_method`` and ``input_dataset_hash`` name how the payload behind
    this parse was reached. A live fetch is ``"network_get"`` with no dataset, and
    a payload read back out of a sealed archived-release dataset is
    ``"sealed_release_dataset"`` with that dataset's content hash. The distinction
    is carried on the record rather than left to a caller's notes, because the two
    route to different raw stores and a later reader cannot recover which one
    produced the bytes from the value alone.
    """

    event_id: str
    family: str
    release_title: str
    scheduled_at: dt.datetime
    embargo_time_from_payload: dt.datetime | None
    reference_period: str
    values: Mapping[str, Decimal]
    revisions: Mapping[str, Decimal]
    clock: Any
    provenance: Any
    source_url: str
    unit_map: Mapping[str, str]
    statements: Mapping[str, str] = field(default_factory=dict)
    schedule_agreement: str = "unverified"
    usdl_number: str | None = None
    acquisition_method: str = ACQUISITION_NETWORK
    input_dataset_hash: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "family": self.family,
            "release_title": self.release_title,
            "scheduled_at": self.scheduled_at.isoformat(),
            "embargo_time_from_payload": (
                self.embargo_time_from_payload.isoformat()
                if self.embargo_time_from_payload
                else None
            ),
            "reference_period": self.reference_period,
            "values": {k: str(v) for k, v in self.values.items()},
            "revisions": {k: str(v) for k, v in self.revisions.items()},
            "unit_map": dict(self.unit_map),
            "statements": dict(self.statements),
            "source_url": self.source_url,
            "raw_hash": getattr(self.provenance, "raw_hash", None),
            "schedule_agreement": self.schedule_agreement,
            "usdl_number": self.usdl_number,
            "acquisition_method": self.acquisition_method,
            "input_dataset_hash": self.input_dataset_hash,
            "revision_status": "initial",
            "time_precision": "minute",
        }


def parse_calendar(
    html: str,
    *,
    year: int,
    calendar_url: str,
    raw_hash: str | None = None,
) -> list[CalendarEntry]:
    """Parse a BLS month calendar page into exact-instant scheduled releases.

    Every row is interpreted in the calendar's declared zone. The page's own
    note that all times are Eastern is required; if it is absent the page is
    rejected, because an unzoned ``08:30 AM`` is not a usable instant.
    """
    if BLS_CALENDAR_NOTE.lower() not in html.lower():
        raise WireShapeError(
            f"calendar at {calendar_url} does not state its timezone "
            f"({BLS_CALENDAR_NOTE!r} absent); refusing to guess an offset"
        )
    tz = resolve_timezone()
    entries: list[CalendarEntry] = []
    for match in _CALENDAR_ROW.finditer(html):
        date_text = _clean(match.group("date"))
        time_text = _clean(match.group("time"))
        release_html = match.group("release")
        release_text = _clean(release_html)
        if not date_text or not time_text or not release_text:
            continue
        date_match = _DATE.search(date_text)
        time_match = _TIME.search(time_text)
        if not date_match or not time_match:
            continue
        month = _MONTHS.get(date_match.group("month").lower())
        if month is None:
            raise WireShapeError(f"unrecognised month in calendar row: {date_text!r}")
        hour = int(time_match.group("hour")) % 12
        if time_match.group("ampm").lower() == "pm":
            hour += 12
        local = dt.datetime(
            int(date_match.group("year")),
            month,
            int(date_match.group("day")),
            hour,
            int(time_match.group("minute")),
            tzinfo=tz,
        )
        title, period = _split_release_text(release_text)
        entries.append(
            CalendarEntry(
                family=_canonical_family(title),
                release_title=title,
                scheduled_at=local.astimezone(dt.UTC),
                local_time=local,
                timezone=BLS_TIMEZONE,
                reference_period=period,
                calendar_url=calendar_url,
                raw_hash=raw_hash,
                preliminary_or_revised="(R)" in release_text,
            )
        )
    return entries


def parse_archive_index(
    html: str,
    *,
    family_slug: str,
    index_url: str,
    raw_hash: str | None = None,
) -> tuple[list[tuple[dt.date, str]], list[UnpublishedRelease]]:
    """Parse an archive index into ``(publication_date, url)`` pairs and gaps.

    The publication date is the date embedded in the archive filename, which the
    index itself uses as the archive key. Reference periods are not inferred from
    it: a delayed release keeps the month it reports in its own title.
    """
    published: list[tuple[dt.date, str]] = []
    seen: set[str] = set()
    for match in _ARCHIVE_LINK.finditer(html):
        slug = match.group("slug").lower()
        if slug != family_slug:
            continue
        stamp = match.group("stamp")
        try:
            when = dt.date(int(stamp[4:]), int(stamp[0:2]), int(stamp[2:4]))
        except ValueError as exc:
            raise WireShapeError(f"archive filename carries an invalid date: {stamp}") from exc
        href = match.group("href")
        if href in seen:
            continue
        seen.add(href)
        published.append((when, f"https://www.bls.gov{href}"))

    unpublished: list[UnpublishedRelease] = []
    index_text = _WS.sub(" ", _strip_tags_preserving_text(html))
    for match in _UNPUBLISHED.finditer(index_text):
        title = _WS.sub(" ", match.group("title")).strip()
        unpublished.append(
            UnpublishedRelease(
                # The family comes from the entry's own title; the index this entry
                # was read from is the honest fallback when it names none.
                family=_canonical_family(title) or family_slug,
                reference_period=_WS.sub(" ", match.group("reference_period")).strip(),
                reason=UNPUBLISHED_REASON,
                index_url=index_url,
                raw_hash=raw_hash,
            )
        )
    published.sort(key=lambda pair: pair[0])
    return published, unpublished


def archive_url(family_slug: str, publication_date: dt.date) -> str:
    """Build the archived release URL from the documented filename convention."""
    return BLS_ARCHIVE_PAYLOAD.format(slug=f"{family_slug}_{publication_date.strftime('%m%d%Y')}")


def parse_release_payload(
    html: str,
    *,
    family_slug: str,
    source_url: str,
    scheduled_at: dt.datetime | None = None,
    provenance: Any = None,
) -> tuple[
    str,
    str,
    dt.datetime | None,
    dict[str, Decimal],
    dict[str, Decimal],
    dict[str, str],
    str | None,
    str,
]:
    """Parse an archived release into title, period, embargo time, values, revisions.

    Returns ``(title, reference_period, embargo_time_from_payload, values,
    revisions, statements, usdl_number, agreement)``.

    Only the release's own lead text is read. Values are taken from fixed phrases
    the release uses for each statistic, so a statistic the release does not state
    is absent from ``values`` rather than defaulted. This is why a first-release
    vector has explicit holes: inventing a zero or reusing a revised number would
    fabricate the shock the whole study measures.
    """
    text = _strip_tags_preserving_text(html)
    text = _WS.sub(" ", text)

    title_match = re.search(r"#\s*(?P<title>[A-Z][A-Za-z ]+News Release)", html) or re.search(
        r"<title>\s*(?P<title>[^<]+?)\s*</title>", html, re.IGNORECASE
    )
    title = _WS.sub(" ", title_match.group("title")).strip() if title_match else family_slug

    usdl = None
    usdl_match = re.search(r"\bUSDL-\d{2}-\d{3,5}\b", text)
    if usdl_match:
        usdl = usdl_match.group(0)

    observed = _embargo_instant(html, text)

    # The release names its own reference period in the masthead line, e.g.
    # "CONSUMER PRICE INDEX - JULY 2026". The publication date is not used as a
    # substitute: a delayed release keeps the period it reports. The separator is
    # one-or-more dashes because the Employment Situation prints
    # "SITUATION -- MARCH 2025"; requiring exactly one read a real release's
    # period as unstated.
    period_match = re.search(
        r"(?:THE\s+)?(?:CONSUMER PRICE INDEX|EMPLOYMENT SITUATION)\s*[-\u2013]+\s*"
        r"(?P<period>[A-Z][A-Za-z]+ \d{4})",
        text,
    )
    reference_period = period_match.group("period") if period_match else ""
    if not reference_period:
        fallback = re.search(
            r"(?P<period>[A-Z][a-z]+ \d{4}) (?:Results|Employment Situation)", title
        )
        reference_period = fallback.group("period") if fallback else ""

    if family_slug == "cpi":
        values, statements = _parse_cpi_values(text)
    elif family_slug == "empsit":
        values, statements = _parse_empsit_values(text)
    else:
        values, statements = {}, {}

    revisions = _parse_revisions(text) if family_slug == "empsit" else {}

    agreement = "unverified"
    if scheduled_at is not None and observed is not None:
        agreement = (
            "agrees_with_calendar"
            if abs((observed - scheduled_at).total_seconds()) <= 1
            else "differs_from_calendar"
        )
    return (
        title,
        reference_period,
        observed,
        values,
        revisions,
        statements,
        usdl,
        agreement,
    )


def _directed_value(
    text: str,
    prefix: str,
    unit: str,
    tail: str = "",
    *,
    allow_unchanged: bool = False,
) -> re.Match[str] | None:
    """Read a statistic whose direction and period the release states itself.

    ``unit`` names the magnitude and ``tail`` is what the release prints after it,
    which is where the period is stated. Returned as the match, so the caller
    signs the value with the direction it was read with. An explicitly unchanged
    statement carries no magnitude, so where the caller accepts that phrasing the
    statistic is recorded as the zero the release states; elsewhere an unchanged
    statement yields no match and the statistic stays absent rather than invented.
    """
    if allow_unchanged:
        unchanged = re.search(rf"{prefix}\s*{_UNCHANGED_VERB}{tail}", text, re.DOTALL)
        if unchanged:
            return unchanged
    return re.search(
        rf"{prefix}\s*(?P<direction>{_DIRECTION_VERB})\s*(?:by\s*)?(?:to\s*)?(?:at\s*)?"
        rf"{unit}{tail}{_NOT_ANNUAL}",
        text,
        re.IGNORECASE | re.DOTALL,
    )


def _signed_value(match: re.Match[str]) -> Decimal:
    """Apply the direction the sentence states to the magnitude it states.

    The releases state a change as a direction verb plus an unsigned magnitude,
    so the verb is the sign. A statement that names no direction but does name a
    magnitude is read at face value: the release means the value it printed, and
    a minus invented for it would be a value the release never stated. An
    unchanged statement names no magnitude at all, and its own words say the
    statistic did not move, so it is the stated zero rather than an absent value.
    """
    groups = match.groupdict()
    if "value" not in groups:
        return Decimal(0)
    value = _decimal(groups["value"])
    if groups.get("direction") is None:
        return value
    direction = _DIRECTION.get(_WS.sub(" ", groups["direction"]).lower())
    if direction is None:
        raise WireShapeError(f"unrecognised direction verb: {groups['direction']!r}")
    return value if direction > 0 else -value


def _parse_cpi_values(text: str) -> tuple[dict[str, Decimal], dict[str, str]]:
    """Extract the headline and core CPI changes the release itself states."""
    values: dict[str, Decimal] = {}
    statements: dict[str, str] = {}

    # "The Consumer Price Index for All Urban Consumers (CPI-U) decreased 0.1
    # percent on a seasonally adjusted basis in March", or "was unchanged on a
    # seasonally adjusted basis". The "All Urban Consumers" opening keeps this off
    # the not-seasonally-adjusted section, which opens with the same series name
    # but states a 12-month change before seasonal adjustment, and requiring the
    # seasonal phrase keeps the two apart.
    headline = _directed_value(
        text,
        r"All Urban Consumers \(CPI-U\)",
        _PERCENT_UNIT,
        r"\s*on a seasonally adjusted basis",
        allow_unchanged=True,
    )
    if headline:
        values["cpi_headline_sa_mom_pct"] = _signed_value(headline)
        statements["cpi_headline_sa_mom"] = _sentence_containing(text, headline.start())

    core = _directed_value(
        text,
        r"(?:all items less food and energy|core)(?: index)?",
        _PERCENT_UNIT,
        allow_unchanged=True,
    )
    if core:
        values["cpi_core_sa_mom_pct"] = _signed_value(core)
        statements["cpi_core_sa_mom"] = _sentence_containing(text, core.start())

    # The 12-month statements are the ones whose own tail names the span, so they
    # are read from that tail rather than from a gap in the middle of the
    # sentence.
    year = _directed_value(
        text,
        r"all items index",
        _PERCENT_UNIT,
        r"\s*(?:for the (?:last )?12 months|over the (?:last|past|year|prior)"
        r"|over the 12 months)",
    )
    if year:
        values["cpi_headline_nsa_yoy_pct"] = _signed_value(year)
        statements["cpi_headline_nsa_yoy"] = _sentence_containing(text, year.start())

    core_year = _directed_value(
        text,
        r"all items less food and energy index",
        _PERCENT_UNIT,
        r"\s*(?:over the (?:last|past) 12 months|over the year|over the last year"
        r"|for the 12 months|over the 12 months)",
    )
    if core_year:
        values["cpi_core_nsa_yoy_pct"] = _signed_value(core_year)
        statements["cpi_core_nsa_yoy"] = _sentence_containing(text, core_year.start())

    # The national-data line states an index level as well as a 12-month percent
    # change. The level is the level the release states: it is never re-signed by
    # the direction verb, which belongs to the change and not to the index.
    index_level = _directed_value(
        text,
        r"\(CPI-U\)",
        _PERCENT_UNIT,
        r"\s*over the last 12 months to an index level\s*of\s*"
        r"(?P<level>[\d,]+(?:\.\d+)?)",
    )
    if index_level:
        values["cpi_u_nsa_index_level"] = _decimal(index_level.group("level"))
        values["cpi_u_nsa_yoy_pct"] = _signed_value(index_level)
        statements["cpi_u_nsa_index_level"] = _sentence_containing(text, index_level.start())

    return values, statements


def _parse_empsit_values(text: str) -> tuple[dict[str, Decimal], dict[str, str]]:
    """Extract payroll, unemployment, earnings and hours the release states."""
    values: dict[str, Decimal] = {}
    statements: dict[str, str] = {}

    # "increased by 256,000", and in a month that lost jobs "declined by
    # 140,000". The direction verb signs the count, so a fall is not read as a
    # gain. "was unchanged" states no magnitude, so that month's change is the
    # zero the release states rather than an absent value.
    payrolls = re.search(
        rf"Total nonfarm payroll employment\s*(?P<direction>{_DIRECTION_VERB})"
        rf"\s*(?:by\s*)?(?:to\s*)?{_COUNT_UNIT}",
        text,
    ) or re.search(rf"Total nonfarm payroll employment\s*{_UNCHANGED_VERB}", text)
    if payrolls:
        # The release states a count of jobs; this study reports thousands. The
        # conversion is explicit rather than implied by the key name.
        jobs = _signed_value(payrolls)
        values["payrolls_change_thousands"] = jobs / Decimal(1000)
        values["payrolls_change_jobs"] = jobs
        statements["payrolls_change"] = _sentence_containing(text, payrolls.start())

    # "changed little at 4.1 percent" states a level without claiming a change,
    # so the level is read and no change is invented for it.
    rate = re.search(
        rf"unemployment rate\s*(?P<direction>{_DIRECTION_VERB})"
        rf"\s*(?:at|to)?\s*(?:by\s*)?{_PERCENT_UNIT}",
        text,
    )
    if rate:
        values["unemployment_rate_pct"] = _decimal(rate.group("value"))
        statements["unemployment_rate"] = _sentence_containing(text, rate.start())

    earnings = _directed_value(
        text,
        r"average hourly earnings for all employees on private nonfarm payrolls",
        rf"(?:\d[\d,]* cents,\s*or\s*)?{_PERCENT_UNIT}",
        r"[^.]{0,40}?to\s*\$(?P<level>\d[\d,]*(?:\.\d+)?)",
    )
    if earnings:
        values["avg_hourly_earnings_mom_pct"] = _signed_value(earnings)
        values["avg_hourly_earnings_usd"] = _decimal(earnings.group("level"))
        statements["avg_hourly_earnings"] = _sentence_containing(text, earnings.start())

    earnings_yoy = re.search(
        rf"average hourly earnings have\s*(?P<direction>{_DIRECTION_VERB})"
        rf"\s*(?:by\s*)?{_PERCENT_UNIT}",
        text,
    )
    if earnings_yoy:
        values["avg_hourly_earnings_yoy_pct"] = _signed_value(earnings_yoy)
        statements["avg_hourly_earnings_yoy"] = _sentence_containing(text, earnings_yoy.start())

    workweek = _directed_value(
        text,
        r"average workweek for all employees on private nonfarm payrolls",
        r"(?P<value>\d+(?:\.\d+)?) hours?",
        r"[^.]{0,20}?to\s*(?P<level>\d+(?:\.\d+)?) hours",
    )
    if workweek:
        # Only a stated change is recorded. "was 34.3 hours" names the level and
        # no change, so the month's change is left absent rather than set to zero.
        values["avg_workweek_hours"] = _decimal(workweek.group("level"))
        values["avg_workweek_change_hours"] = _signed_value(workweek)
        statements["avg_workweek"] = _sentence_containing(text, workweek.start())

    return values, statements


_REVISION_CLAUSE = re.compile(
    r"(?:change in total nonfarm payroll employment for|the change(?:s)? (?:for|in))\s*"
    r"(?P<month>[A-Z][a-z]+)\s+was\s+revised\s*(?:up|down)?\s*(?:by\s*\d[\d,]*)?\s*,?\s*"
    r"from\s*(?P<prior>[-+]?\d[\d,]*)\s*to\s*(?P<revised>[-+]?\d[\d,]*)",
    re.IGNORECASE,
)
_COMBINED_REVISION = re.compile(
    r"employment in (?P<months>[A-Z][a-z]+(?:,\s*|\s+and\s+)[A-Z][a-z]+|[A-Z][a-z]+)\s*"
    r"combined is\s*(?P<amount>[\d,]+)\s*(?P<direction>higher|lower) than previously reported",
    re.IGNORECASE,
)


def _parse_revisions(text: str) -> dict[str, Decimal]:
    """Extract disclosed revisions to prior months.

    The Employment Situation states two months of revisions in one sentence and
    conjoins them grammatically, e.g. "The change in total nonfarm payroll
    employment for June was revised up by 11,000, from +20,000 to +31,000, and the
    change for July was revised up by 44,000, from -23,000 to +21,000". The first
    clause names the series and its month, and the second elides the series and
    names the month after "the change for". Both are one statement about the same
    series, so the clause is matched on its own grammar, one month per clause,
    which captures the second clause as well as the first.

    A revised level belongs to the earlier month it revises, so it is keyed by
    that month and kept out of the current reference period's ``values``. The
    month comes from the clause and is never assumed from the release's own
    reference period. A revision the release does not state is absent rather than
    filled in, and a phrasing that names months without a from/to pair for each
    states no level, so it yields nothing rather than one pair duplicated.
    """
    revisions: dict[str, Decimal] = {}
    for match in _REVISION_CLAUSE.finditer(text):
        month = match.group("month").capitalize()
        # Same count-to-thousands conversion as the headline, plus the raw job
        # counts so the release's own units are recoverable.
        revised_jobs = _decimal(match.group("revised"))
        prior_jobs = _decimal(match.group("prior"))
        revisions[f"payrolls_change_thousands_revised_{month}"] = revised_jobs / Decimal(1000)
        revisions[f"payrolls_change_thousands_prior_{month}"] = prior_jobs / Decimal(1000)
        revisions[f"payrolls_change_jobs_revised_{month}"] = revised_jobs
        revisions[f"payrolls_change_jobs_prior_{month}"] = prior_jobs
    combined = _COMBINED_REVISION.search(text)
    if combined:
        signed_jobs = _decimal(combined.group("amount"))
        if combined.group("direction").lower() == "lower":
            signed_jobs = -signed_jobs
        revisions["payrolls_change_thousands_revision_combined"] = signed_jobs / Decimal(1000)
        revisions["payrolls_change_jobs_revision_combined"] = signed_jobs
    return revisions


def _embargo_instant(html: str, text: str) -> dt.datetime | None:
    """Read the release's own embargo line as an exact instant.

    The archive states the original embargo time, which is the only in-document
    evidence of when the material became public. It is the payload's own claim,
    not an independent observation, and the caller records the agreement with
    the calendar explicitly.

    The time and the ``USDL`` number share one header block, but their positions
    inside it differ between families: CPI prints the time line first and the
    number at its end, while the Employment Situation prints the number at the
    end of the line that introduces the block. The block is therefore read as a
    window that starts at the "embargoed until" phrase and ends at its first
    blank line, and the number is removed from that window before the time is
    read. Where the identifier sits never decides whether the time is found.
    """
    marker = re.search(r"(?:embargoed until|for release at)", text, re.IGNORECASE)
    if marker is None:
        return None
    block = text[marker.end() :]
    block_end = re.search(r"\n\s*\n", block)
    if block_end is not None:
        block = block[: block_end.start()]
    header = _WS.sub(" ", re.sub(r"\bUSDL-\d{2}-\d{3,5}\b", " ", block))

    match = re.search(
        r"(?P<time>\d{1,2}:\d{2}\s*[ap]\.?m\.?)\s*(?:\(ET\)|ET|Eastern Time)?\s*"
        r"(?P<weekday>[A-Za-z]+),?\s*(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),?\s*(?P<year>\d{4})",
        header,
        re.IGNORECASE,
    )
    if not match:
        return None
    time_match = _TIME.search(match.group("time").replace(".", ""))
    month = _MONTHS.get(match.group("month").lower())
    if not time_match or month is None:
        return None
    hour = int(time_match.group("hour")) % 12
    if time_match.group("ampm").lower() == "pm":
        hour += 12
    local = dt.datetime(
        int(match.group("year")),
        month,
        int(match.group("day")),
        hour,
        int(time_match.group("minute")),
        tzinfo=resolve_timezone(),
    )
    return local.astimezone(dt.UTC)


#: Cohort family name to archive slug, for reading a stored row's ``family``
#: column back. The cohort's own vocabulary is canonical and the BLS slug is a
#: boundary detail of the archive, so the translation is stated once here.
_RELEASE_SLUG_BY_FAMILY: Mapping[str, str] = {
    "cpi": "cpi",
    "employment": "empsit",
}

#: The inverse of :data:`_RELEASE_SLUG_BY_FAMILY`. ``_canonical_family`` maps a
#: *title* to a family, so passing it an already-canonical slug round-trips
#: ``cpi`` but returns ``empsit`` unchanged, which put the venue's slug in a
#: field every other record states in the cohort's own vocabulary.
_FAMILY_BY_RELEASE_SLUG: Mapping[str, str] = {
    slug: family for family, slug in _RELEASE_SLUG_BY_FAMILY.items()
}


def _reference_period_label(reference_period: str) -> str | None:
    """``MONTH YYYY`` for a stored ``YYYY-MM`` reference period, or ``None``."""
    match = re.fullmatch(r"(?P<year>\d{4})-(?P<month>\d{2})", str(reference_period).strip())
    if match is None:
        return None
    month = int(match.group("month"))
    if not 1 <= month <= 12:
        return None
    name = next(
        (name for name, number in _MONTHS.items() if number == month and len(name) > 3), None
    )
    return f"{name.upper()} {match.group('year')}" if name else None


def _optional_instant(value: Any) -> dt.datetime | None:
    """An aware UTC instant, or ``None`` for a missing or null column value.

    A stored null comes back from the table as ``NaT``, which is a ``datetime``
    instance but compares unequal to itself, so identity checks would read it as a
    real instant.
    """
    if value is None or isinstance(value, str) or value != value:
        return None
    instant = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if not isinstance(instant, dt.datetime):
        return None
    return instant if instant.tzinfo else instant.replace(tzinfo=dt.UTC)


def _optional_int(value: Any) -> int | None:
    """An int column value, or ``None`` for a missing or null one.

    ``monotonic_ns`` is a null column for these captures: a reading taken on a
    different process's monotonic clock is not comparable here, so it stays absent
    rather than being reconstructed.
    """
    if value is None or isinstance(value, str) or value != value:
        return None
    return int(value)


def _archive_blocked(
    *,
    url: str,
    reason: str,
    dataset_path: pathlib.Path,
    detail: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """A blocked archived-release record: an error outcome, never a quiet gap.

    ``empty_result`` is ``False`` for the same reason a transport failure sets it:
    a record that could not be verified is not an observation of an absent release.
    """
    record = blocked_record(
        url=url,
        status_code=None,
        reason=reason,
        attempts=0,
        payload_hash=None,
        observed_at=dt.datetime.now(dt.UTC),
    )
    record.update(
        {
            "source_kind": ACQUISITION_SEALED_DATASET,
            "archive_dataset_path": str(dataset_path),
            "archive_verified": False,
            "network_fallback_attempted": False,
        }
    )
    if detail:
        record["detail"] = dict(detail)
    return record


_USABLE_TIME_NOTE = (
    "the original capture happened long after publication, so no interval in "
    "which the payload was certainly usable is established and none is invented"
)


def _release_trace(
    evidence: Mapping[str, Any],
    *,
    family: str,
    audit_raw_hash: str | None,
) -> dict[str, Any]:
    """The per-record trace of one verified release, in one shape both paths emit.

    The audit path and the read-only verification path both summarize the same
    verified record. Two builders let a field reach one consumer and not the
    other, so the union is emitted here and each caller supplies only the values
    it actually holds: ``audit_raw_hash`` is the copy this run stored, which the
    read-only path does not make, and ``original_source_url`` is the record's own
    source, which the copy path keeps in its receipt metadata instead.
    """
    return {
        "event_id": evidence["event_id"],
        "family": family,
        "reference_period": evidence["stored_period"],
        "scheduled_at": evidence["stored_scheduled"].isoformat(),
        "archived_raw_hash": evidence["raw_hash"],
        "audit_raw_hash": audit_raw_hash,
        "original_source_url": evidence["url"],
        "original_receipt_present": evidence["receipt"] is not None,
        "original_acquisition_method": evidence["metadata"].get("acquisition_method"),
        "original_received_time": evidence["received"].isoformat(),
        "embargo_time_from_payload": (
            evidence["observed"].isoformat() if evidence["observed"] else None
        ),
        "schedule_agreement": evidence["agreement"],
        "usdl_number": evidence["usdl"],
        "values_verified_against_original_bytes": True,
        "values_key_count": len(evidence["values"]),
        "revisions_key_count": len(evidence["revisions"]),
        "revisions_kept_separate": True,
        "usable_time": None,
        "usable_time_note": _USABLE_TIME_NOTE,
    }


class ArchivedReleaseSource:
    """First releases read from a sealed archived-release dataset and its raw store.

    The dataset is the authority for *which* releases exist and what their
    normalized first-release values are; its sibling :class:`RawStore` is the
    authority for the original bytes. Neither is trusted on its filename: the
    dataset is read through :func:`~market_propagation.storage.read_parquet`,
    which re-verifies the sealed content hash against the manifest and refuses a
    schema it does not declare, and every selected payload is re-read out of the
    raw store (which re-hashes it) and reparsed through
    :func:`parse_release_payload`.

    A payload is accepted only when the reparse agrees with the record on all
    three of the facts a mismatch would silently corrupt:

    * the first-release ``values`` and the disclosed ``revisions``, compared
      exactly, so a dataset whose row disagrees with the bytes it cites is
      refused rather than carried forward;
    * the reference period, compared between the payload's own masthead line and
      the stored value;
    * the scheduled release, compared between the requested instant and the
      stored one.

    Accepted bytes are copied into the caller's own raw store under the
    **original** source URL, record identity and receipt instant, with the archive
    provenance recorded beside them, so an audit's event card can re-read them.
    This class issues no request and has no network path: an explicitly selected
    archive that lacks a record yields a blocked record, never a fallback fetch.
    """

    def __init__(
        self,
        dataset_path: str | pathlib.Path,
        *,
        dest_store: RawStore | None = None,
        raw_root: str | pathlib.Path | None = None,
    ) -> None:
        self._dataset_path = pathlib.Path(dataset_path)
        self._dest_store = dest_store
        if not self._dataset_path.exists():
            raise FileNotFoundError(f"archived release dataset not found: {self._dataset_path}")
        # The dataset's own identity is established first, so a file that is not a
        # releases dataset is refused as that rather than as a missing sidecar.
        manifest_path = self._dataset_path.with_name(self._dataset_path.name + ".manifest.json")
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"archived release dataset {self._dataset_path} carries no manifest at "
                f"{manifest_path}; an unmanifested dataset is not read"
            )
        self._manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        declared = self._manifest.get("table")
        if declared != RELEASE_DATASET_TABLE:
            raise ValueError(
                f"archived release dataset {self._dataset_path} declares table {declared!r}, "
                f"but an archived release source reads {RELEASE_DATASET_TABLE!r}"
            )
        # Verifies the declared schema, schema version, manifest presence, the
        # sealed content hash and the manifest's own table name.
        frame = read_parquet(self._dataset_path, table=RELEASE_DATASET_TABLE)

        self._raw_root = (
            pathlib.Path(raw_root) if raw_root is not None else (self._dataset_path.parent / "raw")
        )
        if not self._raw_root.is_dir():
            raise FileNotFoundError(
                f"archived release dataset {self._dataset_path} has no sibling raw store at "
                f"{self._raw_root}; the original payloads are not addressable without it"
            )
        self._raw_store = RawStore(self._raw_root)
        if self._manifest.get("row_count") != len(frame):
            raise ValueError(
                f"archived release dataset {self._dataset_path} manifest records "
                f"row_count={self._manifest.get('row_count')!r} but {len(frame)} row(s) read"
            )
        coverage_epoch = self._manifest.get("coverage_epoch")
        if not isinstance(coverage_epoch, str) or not coverage_epoch:
            raise ValueError(
                f"archived release dataset {self._dataset_path} records no coverage_epoch; two "
                "acquisition vintages would be indistinguishable"
            )
        self._content_hash = str(self._manifest.get("content_hash"))
        self._coverage_epoch = coverage_epoch
        self._row_count = len(frame)
        self._by_identity: dict[tuple[str, str], dict[str, Any]] = {}
        for row in frame.to_dict(orient="records"):
            scheduled = _optional_instant(row.get("scheduled_at"))
            event_id = row.get("event_id")
            if scheduled is None or not event_id:
                raise ValueError(
                    f"archived release dataset {self._dataset_path} carries a row without an "
                    f"event_id or a scheduled instant: event_id={event_id!r}"
                )
            key = (str(event_id), scheduled.date().isoformat())
            if key in self._by_identity:
                # Two rows sharing an identity would collapse to one with no error,
                # and every downstream count is derived from this index, so the
                # dataset would report all rows verified while one is gone. Which
                # same-day capture is the original is ambiguous, so neither is served.
                raise ValueError(
                    f"archived release dataset {self._dataset_path} carries two rows for "
                    f"event_id={event_id!r} on {scheduled.date().isoformat()}; which "
                    "capture is the original is ambiguous, so neither is served silently"
                )
            self._by_identity[key] = row
        self._verified: dict[str, dict[str, Any]] = {}

    @property
    def dataset(self) -> dict[str, Any]:
        return {
            "path": str(self._dataset_path),
            "raw_root": str(self._raw_root),
            "table": RELEASE_DATASET_TABLE,
            "schema_version": self._manifest.get("schema_version"),
            "coverage_epoch": self._coverage_epoch,
            "content_hash": self._content_hash,
            "row_count": self._row_count,
        }

    def records(self) -> Sequence[dict[str, Any]]:
        """Every verified record this source has loaded, deterministically ordered."""
        return [self._verified[key] for key in sorted(self._verified)]

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": ACQUISITION_SEALED_DATASET,
            "dataset": self.dataset,
            "network_used": False,
            "records_loaded": len(self._verified),
            "records": self.records(),
            "selection": (
                "the caller named this dataset explicitly; a record that is absent or does "
                "not verify yields a blocked record and no request is issued"
            ),
        }

    def _row_for(self, family_slug: str, publication_date: dt.date) -> dict[str, Any] | None:
        """The row for one release, matched on the archive slug and publication date.

        The dataset stores the study's canonical family name (``employment``), while
        the caller addresses the archive by slug (``empsit``), so the comparison goes
        through the one slug translation rather than assuming the two vocabularies
        are the same string.
        """
        for row in self._by_identity.values():
            stored = _optional_instant(row.get("scheduled_at"))
            if (
                _RELEASE_SLUG_BY_FAMILY.get(str(row.get("family"))) == family_slug
                and stored is not None
                and stored.date() == publication_date
            ):
                return row
        return None

    def verify_all(self) -> dict[str, Any]:
        """Verify every record against its own bytes, copying nothing.

        This is the read-only counterpart of :meth:`get_initial_release` and runs the
        same checks over the same bytes through the same entry points. It exists so a
        consumer that only needs to *cite* the archive verifies exactly what the audit
        path verifies, instead of a second, weaker check that could disagree with it.
        """
        verified: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for key in sorted(self._by_identity):
            row = self._by_identity[key]
            family_slug = _RELEASE_SLUG_BY_FAMILY.get(str(row.get("family")))
            if family_slug is None:
                blocked.append(
                    _archive_blocked(
                        url=str(row.get("source") or ""),
                        reason="archived_record_names_no_known_family",
                        dataset_path=self._dataset_path,
                        detail={"family": row.get("family"), "event_id": row.get("event_id")},
                    )
                )
                continue
            evidence, failure = self._verify_row(
                row,
                family_slug=family_slug,
                scheduled_at=_optional_instant(row.get("scheduled_at")),
                expected_event_id=str(row.get("event_id")),
            )
            if failure is not None:
                blocked.append(failure)
                continue
            assert evidence is not None
            verified.append(
                _release_trace(
                    evidence,
                    family=str(row.get("family")),
                    audit_raw_hash=None,
                )
            )
        return {
            "kind": ACQUISITION_SEALED_DATASET,
            "dataset": self.dataset,
            "network_used": False,
            "records_verified": len(verified),
            "records_blocked": len(blocked),
            "records": verified,
            "blocked_records": blocked,
            "all_records_verified": bool(verified) and not blocked,
            "usable_time": None,
            "usable_time_note": (
                "the original captures happened long after publication, so no interval in "
                "which a payload was certainly usable is established for any release"
            ),
            "interpretation": (
                "each verified record means the archival bytes the dataset cites were re-read "
                "and reparsed, and the record's own values, revisions, reference period and "
                "scheduled instant agree with them. This certifies the release values a "
                "published payload states; it does not certify which market rule version was "
                "in force at the release, nor the quote coverage of any contract"
            ),
        }

    def _verify_row(
        self,
        row: Mapping[str, Any],
        *,
        family_slug: str,
        scheduled_at: dt.datetime | None,
        expected_event_id: str | None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Verify one archived row against its own bytes, or say why it does not hold."""
        url = str(row.get("source") or "")
        event_id = str(row["event_id"])
        if expected_event_id is not None and event_id != str(expected_event_id):
            return None, _archive_blocked(
                url=url,
                reason="archived_record_event_id_mismatch",
                dataset_path=self._dataset_path,
                detail={
                    "requested_event_id": str(expected_event_id),
                    "archived_event_id": event_id,
                },
            )

        stored_scheduled = _optional_instant(row.get("scheduled_at"))
        if (
            scheduled_at is not None
            and stored_scheduled is not None
            and stored_scheduled.astimezone(dt.UTC) != scheduled_at.astimezone(dt.UTC)
        ):
            return None, _archive_blocked(
                url=url,
                reason="archived_record_schedule_mismatch",
                dataset_path=self._dataset_path,
                detail={
                    "requested_scheduled_at": scheduled_at.astimezone(dt.UTC).isoformat(),
                    "archived_scheduled_at": stored_scheduled.astimezone(dt.UTC).isoformat(),
                },
            )
        if stored_scheduled is None:
            return None, _archive_blocked(
                url=url,
                reason="archived_record_carries_no_scheduled_instant",
                dataset_path=self._dataset_path,
                detail={"event_id": event_id},
            )

        raw_hash = str(row.get("raw_hash") or "")
        try:
            body = self._raw_store.get(raw_hash)
        except (FileNotFoundError, ValueError) as exc:
            return None, _archive_blocked(
                url=url,
                reason="original_payload_unreadable_in_archive_raw_store",
                dataset_path=self._dataset_path,
                detail={
                    "archive_raw_hash": raw_hash or None,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )

        receipt = self._raw_store.receipt(str(row.get("record_id") or ""), source=row.get("source"))
        metadata = (receipt or {}).get("metadata") or {}
        if receipt is not None and receipt.get("raw_hash") != raw_hash:
            return None, _archive_blocked(
                url=url,
                reason="archived_receipt_names_a_different_payload",
                dataset_path=self._dataset_path,
                detail={
                    "receipt_raw_hash": receipt.get("raw_hash"),
                    "record_raw_hash": raw_hash,
                },
            )

        html = body.decode("utf-8", errors="replace")
        (
            title,
            _stated_period,
            observed,
            values,
            revisions,
            statements,
            usdl,
            agreement,
        ) = parse_release_payload(
            html,
            family_slug=family_slug,
            source_url=url,
            scheduled_at=scheduled_at,
            provenance=None,
        )

        value_mismatches = _value_differences(dict(row.get("values_json") or {}), values)
        revision_mismatches = _value_differences(dict(row.get("revisions_json") or {}), revisions)
        if value_mismatches or revision_mismatches:
            return None, _archive_blocked(
                url=url,
                reason="archived_record_values_do_not_match_its_own_payload",
                dataset_path=self._dataset_path,
                detail={
                    "value_mismatches": value_mismatches,
                    "revision_mismatches": revision_mismatches,
                    "verified_against_original_bytes": False,
                },
            )

        stored_period = str(row.get("reference_period") or "")
        label = _reference_period_label(stored_period)
        # The period comes from the same parser call that produced the values above,
        # so the comparison cannot drift from the payload the way a second reader
        # of the masthead could.
        masthead = _stated_period.strip().upper() or None
        if label is None:
            return None, _archive_blocked(
                url=url,
                reason="archived_record_reference_period_is_not_a_month",
                dataset_path=self._dataset_path,
                detail={"stored_reference_period": stored_period},
            )
        if masthead is None:
            return None, _archive_blocked(
                url=url,
                reason="payload_states_no_reference_period_in_its_masthead",
                dataset_path=self._dataset_path,
                detail={"stored_reference_period": stored_period},
            )
        if masthead != label:
            return None, _archive_blocked(
                url=url,
                reason="payload_masthead_period_disagrees_with_archived_record",
                dataset_path=self._dataset_path,
                detail={
                    "payload_masthead_period": masthead,
                    "archived_reference_period": stored_period,
                    "archived_reference_period_label": label,
                },
            )

        stored_observed = _optional_instant(row.get("observed_at"))
        if observed is not None and stored_observed is not None and observed != stored_observed:
            return None, _archive_blocked(
                url=url,
                reason="payload_embargo_instant_disagrees_with_archived_record",
                dataset_path=self._dataset_path,
                detail={
                    "payload_embargo_time": observed.isoformat(),
                    "archived_observed_at": stored_observed.isoformat(),
                },
            )

        received = _optional_instant(row.get("received_time"))
        if received is None:
            return None, _archive_blocked(
                url=url,
                reason="archived_record_carries_no_receipt_instant",
                dataset_path=self._dataset_path,
                detail={"original_receipt_present": receipt is not None},
            )

        return {
            "url": url,
            "event_id": event_id,
            "body": body,
            "raw_hash": raw_hash,
            "receipt": receipt,
            "metadata": metadata,
            "received": received,
            "stored_scheduled": stored_scheduled,
            "stored_observed": stored_observed,
            "stored_period": stored_period,
            "title": title,
            "observed": observed,
            "values": values,
            "revisions": revisions,
            "statements": statements,
            "usdl": usdl,
            "agreement": agreement,
        }, None

    def _record_fact(self, evidence: Mapping[str, Any], release: MacroRelease) -> dict[str, Any]:
        """The per-record trace an audit reports for one loaded release."""
        return _release_trace(
            evidence,
            family=release.family,
            audit_raw_hash=release.provenance.raw_hash,
        )

    def get_initial_release(
        self,
        family_slug: str,
        publication_date: dt.date,
        *,
        scheduled_at: dt.datetime | None = None,
        clock: Any = None,
        expected_event_id: str | None = None,
    ) -> tuple[MacroRelease | None, dict[str, Any] | None]:
        """Load, verify and re-store one archived first release, or block on it."""
        row = self._row_for(family_slug, publication_date)
        if row is None:
            return None, _archive_blocked(
                url=archive_url(family_slug, publication_date),
                reason="no_record_for_release_in_sealed_dataset",
                dataset_path=self._dataset_path,
                detail={
                    "family_slug": family_slug,
                    "publication_date": publication_date.isoformat(),
                    "requested_scheduled_at": (
                        scheduled_at.isoformat() if scheduled_at is not None else None
                    ),
                },
            )

        evidence, failure = self._verify_row(
            row,
            family_slug=family_slug,
            scheduled_at=scheduled_at,
            expected_event_id=expected_event_id,
        )
        if failure is not None:
            return None, failure
        assert evidence is not None

        # ``dest_store`` is the caller's own raw store, the one an audit's event card
        # later re-reads. Without it there is nowhere to copy the payload, and the
        # bare AttributeError that surfaced here named neither the argument nor the
        # path, so the requirement is stated instead.
        if self._dest_store is None:
            raise ValueError(
                f"loading a release from {self._dataset_path} needs a destination raw "
                "store to copy the verified bytes into; construct this source with "
                "dest_store=..., or call verify_all() to cite the archive without "
                "copying it"
            )
        provenance = self._dest_store.put(
            evidence["body"],
            source=str(row["source"]),
            received_time=evidence["received"],
            record_id=str(row.get("record_id") or evidence["event_id"]),
            metadata=self._destination_metadata(
                row=row, metadata=evidence["metadata"], url=evidence["url"]
            ),
        )

        if clock is not None:
            release_clock = clock
        else:
            from ..domain import Availability, Clock

            release_clock = Clock(
                source_time=evidence["stored_observed"],
                received_time=evidence["received"],
                availability=Availability.unknown(
                    basis=str(row.get("availability_basis") or "late_archived_release_dataset")
                ),
                monotonic_ns=_optional_int(row.get("monotonic_ns")),
            )

        release = MacroRelease(
            event_id=evidence["event_id"],
            family=_FAMILY_BY_RELEASE_SLUG[family_slug],
            release_title=evidence["title"],
            scheduled_at=(
                scheduled_at.astimezone(dt.UTC)
                if scheduled_at is not None
                else (
                    evidence["stored_scheduled"]
                    or evidence["observed"]
                    or _midnight_utc(publication_date)
                )
            ),
            embargo_time_from_payload=evidence["observed"],
            reference_period=evidence["stored_period"],
            values=evidence["values"],
            revisions=evidence["revisions"],
            clock=release_clock,
            provenance=provenance,
            source_url=str(row["source"]),
            unit_map=_UNIT_MAP,
            statements=evidence["statements"],
            schedule_agreement=evidence["agreement"],
            usdl_number=evidence["usdl"],
            acquisition_method=ACQUISITION_SEALED_DATASET,
            input_dataset_hash=self._content_hash,
        )
        key = f"{evidence['event_id']}@{evidence['stored_scheduled'].date().isoformat()}"
        self._verified[key] = self._record_fact(evidence, release)
        return release, None

    def _destination_metadata(
        self,
        *,
        row: Mapping[str, Any],
        metadata: Mapping[str, Any],
        url: str,
    ) -> dict[str, Any]:
        """Original-source metadata for the copied bytes, plus how they were verified.

        The receipt's own fields are carried under ``original_`` names so a reader
        sees the acquisition this study inherited, and the archive identity is
        recorded separately so the copy is never mistaken for this run's fetch.
        Integrity is asserted only where this class actually checked it: a
        ``status`` or ``payload_complete`` flag is repeated as inherited metadata,
        never promoted to the reason the payload is trusted.
        """
        return {
            "acquisition_method": ACQUISITION_SEALED_DATASET,
            "original_acquisition_method": metadata.get("acquisition_method"),
            "original_source_url": row.get("source") or url,
            "original_record_id": row.get("record_id"),
            "original_received_time": metadata.get("received_time"),
            "original_status": metadata.get("status"),
            "original_payload_complete": metadata.get("payload_complete"),
            "original_receipt_present": bool(metadata),
            "source_availability": "unknown_historical",
            "archive_dataset_path": str(self._dataset_path),
            "archive_dataset_content_hash": self._content_hash,
            "archive_dataset_table": RELEASE_DATASET_TABLE,
            "archive_dataset_coverage_epoch": self._coverage_epoch,
            "archive_raw_hash": row.get("raw_hash"),
            "archive_reference_period": row.get("reference_period"),
            "archive_scheduled_at": (
                _optional_instant(row.get("scheduled_at")).isoformat()
                if _optional_instant(row.get("scheduled_at"))
                else None
            ),
            "verified_by": (
                "re-read from the sealed dataset's raw store (which re-hashes the bytes) and "
                "reparsed; values, revisions and masthead reference period all agree with the "
                "archived record"
            ),
        }


def _value_differences(stored: Mapping[str, Any], reparsed: Mapping[str, Any]) -> dict[str, Any]:
    """Per-key differences between a stored value map and a reparsed one.

    Compared as exact decimal strings, so a rounded or reformatted value is a
    difference rather than a near miss.
    """
    differences: dict[str, Any] = {}
    for key in sorted(set(stored) | set(reparsed)):
        left = stored.get(key)
        right = reparsed.get(key)
        if left is None or right is None or str(left) != str(right):
            differences[key] = {
                "archived": None if left is None else str(left),
                "reparsed": None if right is None else str(right),
            }
    return differences


class MacroReleaseClient:
    """Read-only BLS client. GET only, no registration key, no consensus data."""

    def __init__(
        self,
        store: Any,
        *,
        transport: HttpTransport | None = None,
        base_url: str = "https://www.bls.gov",
        release_dataset: str | pathlib.Path | None = None,
        archive_raw_root: str | pathlib.Path | None = None,
    ) -> None:
        """A client whose release payloads come from the network, or from an archive.

        ``release_dataset`` names a sealed archived-release Parquet explicitly. When
        it is given, :meth:`get_initial_release` reads and verifies that dataset and
        its sibling raw store instead of fetching, and no request is issued for a
        release at all. When it is absent the network path is unchanged.

        The dataset is opened here rather than per release, so a missing dataset, a
        wrong table, a broken schema or a content hash that disagrees with its
        manifest fails once, at construction, instead of being reported as ten
        separate blocked events.
        """
        self._store = store
        self._owns_transport = transport is None
        self._transport = transport or HttpTransport(store)
        self._base = base_url.rstrip("/")
        self._archive = (
            ArchivedReleaseSource(release_dataset, dest_store=store, raw_root=archive_raw_root)
            if release_dataset is not None
            else None
        )

    @property
    def archive(self) -> ArchivedReleaseSource | None:
        """The archived release source this client reads, or ``None`` when it fetches."""
        return self._archive

    def close(self) -> None:
        if self._owns_transport:
            self._transport.close()

    def __enter__(self) -> MacroReleaseClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def get_calendar(
        self, year: int, month: int
    ) -> tuple[list[CalendarEntry], dict[str, Any] | None]:
        """Fetch and parse one month of the official release calendar.

        Publicly readable and unauthenticated. Recorded failures are returned as
        a blocked status rather than an empty calendar, because an unreachable
        calendar must never be read as a month with no releases.
        """
        url = f"{self._base}{BLS_CALENDAR_PATH.format(year=year, month=month)}"
        try:
            envelope = self._transport.get(
                url,
                source="bls.calendar",
                record_id=f"{year}-{month:02d}",
                accept="text/html",
            )
        except TransportError as exc:
            return [], exc.as_blocked_record()
        entries = parse_calendar(
            envelope.text,
            year=year,
            calendar_url=url,
            raw_hash=envelope.provenance.raw_hash,
        )
        return entries, None

    def get_archive_index(
        self, family_slug: str
    ) -> tuple[list[tuple[dt.date, str]], list[UnpublishedRelease], dict[str, Any] | None]:
        """Fetch an archive index, including documented non-publications."""
        index_url = BLS_ARCHIVE_INDEX[family_slug]
        try:
            envelope = self._transport.get(
                index_url,
                source="bls.archive.index",
                record_id=family_slug,
                accept="text/html",
            )
        except TransportError as exc:
            return [], [], exc.as_blocked_record()
        published, unpublished = parse_archive_index(
            envelope.text,
            family_slug=family_slug,
            index_url=index_url,
            raw_hash=envelope.provenance.raw_hash,
        )
        return published, unpublished, None

    def get_initial_release(
        self,
        family_slug: str,
        publication_date: dt.date,
        *,
        scheduled_at: dt.datetime | None = None,
        clock: Any = None,
        event_id: str | None = None,
    ) -> tuple[MacroRelease | None, dict[str, Any] | None]:
        """Read one archived first-release payload, or record why it is unavailable.

        With no ``release_dataset`` this fetches and the transport archives the raw
        HTML before parsing, so ``release.provenance.raw_hash`` always resolves to
        the stored payload. With one, :class:`ArchivedReleaseSource` verifies and
        copies the original bytes instead, and a record it cannot verify is returned
        as a blocked result: no request is issued, because a silently substituted
        live fetch would report success for a payload nothing verified.
        """
        if self._archive is not None:
            return self._archive.get_initial_release(
                family_slug,
                publication_date,
                scheduled_at=scheduled_at,
                clock=clock,
                expected_event_id=event_id,
            )

        url = archive_url(family_slug, publication_date)
        try:
            envelope = self._transport.get(
                url,
                source=f"bls.release.{family_slug}",
                record_id=publication_date.strftime("%Y-%m-%d"),
                accept="text/html",
            )
        except TransportError as exc:
            return None, exc.as_blocked_record()

        (
            title,
            period,
            observed,
            values,
            revisions,
            statements,
            usdl,
            agreement,
        ) = parse_release_payload(
            envelope.text,
            family_slug=family_slug,
            source_url=url,
            scheduled_at=scheduled_at,
            provenance=envelope.provenance,
        )

        if clock is not None:
            release_clock = clock
        else:
            from ..domain import Availability, Clock

            release_clock = (
                Clock.historical(scheduled_at)
                if scheduled_at
                else Clock(
                    source_time=observed,
                    received_time=envelope.received_time,
                    availability=Availability.unknown(basis="historical_without_receipt"),
                    monotonic_ns=envelope.monotonic_ns,
                )
            )

        event_id = (
            f"bls.{family_slug}.{period.replace(' ', '_').lower() or publication_date.isoformat()}"
        )
        release = MacroRelease(
            event_id=event_id,
            family=_FAMILY_BY_RELEASE_SLUG[family_slug],
            release_title=title,
            scheduled_at=scheduled_at.astimezone(dt.UTC)
            if scheduled_at
            else (observed or _midnight_utc(publication_date)),
            embargo_time_from_payload=observed,
            reference_period=period,
            values=values,
            revisions=revisions,
            clock=release_clock,
            provenance=envelope.provenance,
            source_url=url,
            unit_map=_UNIT_MAP,
            statements=statements,
            schedule_agreement=agreement,
            usdl_number=usdl,
        )
        return release, None

    def get_ics_feed(self) -> tuple[str, dict[str, Any] | None]:
        """Fetch the documented ICS feed for cross-checking calendar times."""
        try:
            envelope = self._transport.get(
                BLS_ICS_FEED,
                source="bls.calendar.ics",
                record_id="bls.ics",
                accept="text/calendar",
            )
        except TransportError as exc:
            return "", exc.as_blocked_record()
        return envelope.text, None


_UNIT_MAP: Mapping[str, str] = {
    "cpi_headline_sa_mom_pct": "percent_change_sa_mom",
    "cpi_core_sa_mom_pct": "percent_change_sa_mom",
    "cpi_headline_nsa_yoy_pct": "percent_change_nsa_yoy",
    "cpi_core_nsa_yoy_pct": "percent_change_nsa_yoy",
    "cpi_u_nsa_index_level": "index_level_1982_84_100",
    "cpi_u_nsa_yoy_pct": "percent_change_nsa_yoy",
    "payrolls_change_thousands": "thousands_of_jobs",
    "payrolls_change_jobs": "jobs",
    "unemployment_rate_pct": "percent_level",
    "avg_hourly_earnings_usd": "usd_per_hour",
    "avg_hourly_earnings_mom_pct": "percent_change_mom",
    "avg_hourly_earnings_yoy_pct": "percent_change_yoy",
    "avg_workweek_hours": "hours_per_week",
    "avg_workweek_change_hours": "hours_change_mom",
}


def _decimal(raw: str) -> Decimal:
    try:
        return Decimal(raw.replace(",", "").lstrip("+"))
    except InvalidOperation as exc:
        raise WireShapeError(f"not a decimal value: {raw!r}") from exc


def _sentence_containing(text: str, position: int) -> str:
    start = text.rfind(".", 0, position)
    start = 0 if start < 0 else start + 1
    end = text.find(".", position)
    end = len(text) if end < 0 else end + 1
    return _WS.sub(" ", text[start:end]).strip()


def _clean(fragment: str) -> str:
    return _WS.sub(" ", _TAG.sub(" ", fragment)).strip()


def _strip_tags_preserving_text(html: str) -> str:
    without_scripts = re.sub(
        r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.IGNORECASE | re.DOTALL
    )
    return _TAG.sub(" ", without_scripts)


def _split_release_text(release_text: str) -> tuple[str, str | None]:
    """Split a calendar row into a release title and its reference period."""
    text = _WS.sub(" ", release_text).strip()
    period = None
    period_match = re.search(r"for (?:the )?(?P<period>[A-Za-z]+ ?-? ?\d{4}|[A-Za-z]+ \d{4})", text)
    if period_match:
        period = period_match.group("period")
        text = text[: period_match.start()].strip()
    text = re.sub(r"\s*\((?:R|P)\)\s*$", "", text).strip()
    return text, period


def _canonical_family(title: str) -> str:
    lowered = title.lower()
    if "consumer price index" in lowered or lowered.strip() == "cpi":
        return "cpi"
    if "employment situation" in lowered or lowered.strip() == "empsit":
        return "empsit"
    return title.strip().lower().replace(" ", "_")


def _midnight_utc(day: dt.date) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, tzinfo=dt.UTC)


__all__ = [
    "ACQUISITION_NETWORK",
    "ACQUISITION_SEALED_DATASET",
    "BLS_ARCHIVE_INDEX",
    "BLS_ARCHIVE_PAYLOAD",
    "BLS_CALENDAR_NOTE",
    "BLS_ICS_FEED",
    "BLS_TIMEZONE",
    "RELEASE_DATASET_TABLE",
    "RELEASE_FAMILIES",
    "UNPUBLISHED_REASON",
    "ArchivedReleaseSource",
    "CalendarEntry",
    "MacroRelease",
    "MacroReleaseClient",
    "UnpublishedRelease",
    "archive_url",
    "parse_archive_index",
    "parse_calendar",
    "parse_release_payload",
    "resolve_timezone",
]
