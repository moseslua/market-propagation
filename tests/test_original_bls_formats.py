"""Regressions for two archived BLS first-release formats the parser misread.

Both cases are copied from the payloads the study actually captured rather than
written from an idealised example, and each fixture records the source URL and
the line numbers its excerpt was taken from, so a later reader can check the
text against the archive. Only the lead text is carried: the captures are about
a megabyte each, and the statistics under test are all stated in the first
paragraphs.

Two traps are pinned deliberately, because each one passes silently:

* The releases state a change as a direction **verb** plus an unsigned
  magnitude. "decreased 0.1 percent" is a fall of one tenth, and reading the
  magnitude without the verb turns a decline into a rise.
* The embargo time and the ``USDL`` number share one header block, but the
  Employment Situation prints the number at the end of the line that introduces
  the block while CPI prints it at the end of the time line. Treating the
  number's position as part of the time's grammar loses the embargo entirely.

The monthly-versus-annual boundary is pinned alongside them: a 12-month figure
shares the monthly statement's verbs and unit, so it must never be read into a
monthly field.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import sys
from decimal import Decimal

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from market_propagation.ingest.macro_releases import (
    parse_release_payload,
)

# Consumer Price Index for the March 2025 reference period, published April 10,
# 2025. Line 552-553 is the embargo header, 560-562 the headline paragraph and
# 573-576 the 12-month paragraph of the capture. The fixed-width padding inside
# the header is trimmed here; the parser normalises whitespace before reading it.
CPI_APRIL_2025_SOURCE = "https://www.bls.gov/news.release/archives/cpi_04102025.htm"
CPI_APRIL_2025_HTML = """
<PRE>Transmission of material in this release is embargoed until
8:30 a.m. (ET) Thursday, April 10, 2025        USDL-25-0459

Technical information: (202) 691-7000  *  cpi_info@bls.gov  *  www.bls.gov/cpi
Media contact:         (202) 691-5902  *  PressOffice@bls.gov

CONSUMER PRICE INDEX - MARCH 2025

The Consumer Price Index for All Urban Consumers (CPI-U) decreased 0.1 percent on a seasonally adjusted basis in
March, after rising 0.2 percent in February, the U.S. Bureau of Labor Statistics reported today. Over the last 12
months, the all items index increased 2.4 percent before seasonal adjustment.

The index for all items less food and energy rose 0.1 percent in March, following a 0.2-percent increase in February.

The all items index rose 2.4 percent for the 12 months ending March, after rising 2.8 percent over the 12 months
ending February. The all items less food and energy index rose 2.8 percent over the last 12 months, the smallest
12-month increase since March 2021.

The Consumer Price Index for All Urban Consumers (CPI-U) increased 2.4 percent over the last 12 months to an index
level of 319.799 (1982-84=100). For the month, the index increased 0.2 percent prior to seasonal adjustment.
</PRE>
"""

# Employment Situation for the December 2024 reference period, published January
# 10, 2025. Lines 578-579 are the embargo header, with the USDL number at the end
# of the first line and the time on the second, and 591-592 is the headline.
EMPSIT_JANUARY_2025_SOURCE = "https://www.bls.gov/news.release/archives/empsit_01102025.htm"
EMPSIT_JANUARY_2025_HTML = """
<pre>
Transmission of material in this news release is embargoed until                       USDL-25-0003
8:30 a.m. (ET) Friday, January 10, 2025

Technical information:
 Household data:     (202) 691-6378  *  cpsinfo@bls.gov  *  www.bls.gov/cps
 Establishment data: (202) 691-6555  *  cesinfo@bls.gov  *  www.bls.gov/ces

Media contact:      (202) 691-5902  *  PressOffice@bls.gov

                        THE EMPLOYMENT SITUATION -- DECEMBER 2024

Total nonfarm payroll employment increased by 256,000 in December, and the unemployment rate
changed little at 4.1 percent, the U.S. Bureau of Labor Statistics reported today. Employment
trended up in health care, government, and social assistance.
</pre>
"""

# The source schedule for that employment release, from the cohort configuration.
EMPSIT_JANUARY_2025_SCHEDULED_AT = dt.datetime(2025, 1, 10, 13, 30, tzinfo=dt.UTC)


def _values(html: str, family_slug: str) -> dict[str, Decimal]:
    return parse_release_payload(html, family_slug=family_slug, source_url="https://www.bls.gov/x")[
        3
    ]


def test_archived_cpi_decrease_is_signed_negative() -> None:
    """The release prints "decreased 0.1 percent"; the parser must not flip it.

    The capture reports the headline as a fall of one tenth, and a positive
    value here would invert the shock the study measures.
    """
    values = _values(CPI_APRIL_2025_HTML, "cpi")

    assert values["cpi_headline_sa_mom_pct"] == Decimal("-0.1")
    # The direction is not applied to the figures the release states as rising.
    assert values["cpi_core_sa_mom_pct"] == Decimal("0.1")
    assert values["cpi_headline_nsa_yoy_pct"] == Decimal("2.4")
    assert values["cpi_core_nsa_yoy_pct"] == Decimal("2.8")


def test_archived_cpi_index_level_keeps_its_own_sign() -> None:
    """The same line states a level, which no direction verb can re-sign."""
    values = _values(CPI_APRIL_2025_HTML, "cpi")

    assert values["cpi_u_nsa_index_level"] == Decimal("319.799")


def test_archived_employment_embargo_follows_the_header_block() -> None:
    """The USDL number on the first header line must not hide the embargo time.

    The number sits before the time line in this family, the reverse of the CPI
    header, and the time is on the next line of the same block.
    """
    _, _, observed, _, _, _, usdl, agreement = parse_release_payload(
        EMPSIT_JANUARY_2025_HTML,
        family_slug="empsit",
        source_url=EMPSIT_JANUARY_2025_SOURCE,
        scheduled_at=EMPSIT_JANUARY_2025_SCHEDULED_AT,
    )

    assert observed == EMPSIT_JANUARY_2025_SCHEDULED_AT
    assert agreement == "agrees_with_calendar"
    assert usdl == "USDL-25-0003"


def test_embargo_is_source_evidence_and_absent_when_the_header_is_absent() -> None:
    """Without an embargo line there is no source evidence, so none is invented.

    The schedule the caller already holds must not be echoed back as though the
    payload had stated it, and the filename carries no time to fall back on.
    """
    _, _, observed, _, _, _, _, agreement = parse_release_payload(
        "<pre>THE EMPLOYMENT SITUATION -- DECEMBER 2024\n"
        "Total nonfarm payroll employment increased by 256,000 in December.</pre>",
        family_slug="empsit",
        source_url="https://www.bls.gov/news.release/archives/empsit_01102025.htm",
        scheduled_at=EMPSIT_JANUARY_2025_SCHEDULED_AT,
    )

    assert observed is None
    assert agreement == "unverified"


def test_embargo_time_is_found_whichever_line_carries_the_usdl_number() -> None:
    """Both real header shapes yield the same instant, and neither is a fluke."""
    time_line_first = (
        "<PRE>Transmission of material in this release is embargoed until\n"
        "8:30 a.m. (ET) Thursday, April 10, 2025        USDL-25-0459\n"
        "\nTechnical information: (202) 691-7000\n</PRE>"
    )
    number_line_first = (
        "<pre>Transmission of material in this news release is embargoed until    USDL-25-0003\n"
        "8:30 a.m. (ET) Friday, January 10, 2025\n"
        "\nTechnical information:\n</pre>"
    )

    for html in (time_line_first, number_line_first):
        observed = parse_release_payload(html, family_slug="cpi", source_url="x")[2]
        assert observed is not None
    # April 10 is in daylight time and January 10 in standard time, so the same
    # 8:30 a.m. Eastern resolves to two different UTC instants.
    assert parse_release_payload(time_line_first, family_slug="cpi", source_url="x")[2] == (
        dt.datetime(2025, 4, 10, 12, 30, tzinfo=dt.UTC)
    )
    assert parse_release_payload(number_line_first, family_slug="cpi", source_url="x")[2] == (
        dt.datetime(2025, 1, 10, 13, 30, tzinfo=dt.UTC)
    )


def test_cpi_headline_decline_is_negative_for_each_declining_verb() -> None:
    for verb in ("decreased", "fell", "declined"):
        values = _values(
            "<pre>CONSUMER PRICE INDEX - OCTOBER 2025\n"
            f"The Consumer Price Index for All Urban Consumers (CPI-U) {verb} 0.4 percent "
            "on a seasonally adjusted basis in October.\n</pre>",
            "cpi",
        )
        assert values["cpi_headline_sa_mom_pct"] == Decimal("-0.4"), verb


def test_cpi_headline_increase_is_positive_for_each_rising_verb() -> None:
    """A rising month stays positive; the sign comes from the release's verb."""
    for verb in ("increased", "rose", "advanced"):
        values = _values(
            "<pre>CONSUMER PRICE INDEX - OCTOBER 2025\n"
            f"The Consumer Price Index for All Urban Consumers (CPI-U) {verb} 0.4 percent "
            "on a seasonally adjusted basis in October.\n</pre>",
            "cpi",
        )
        assert values["cpi_headline_sa_mom_pct"] == Decimal("0.4"), verb


def test_payroll_change_is_signed_by_its_direction() -> None:
    """A month that lost jobs states a fall, and the fall must stay negative."""
    falling = _values(
        "<pre>THE EMPLOYMENT SITUATION -- MARCH 2025\n"
        "Total nonfarm payroll employment declined by 140,000 in March, and the "
        "unemployment rate edged up to 4.3 percent.\n</pre>",
        "empsit",
    )
    assert falling["payrolls_change_thousands"] == Decimal("-140")
    assert falling["payrolls_change_jobs"] == Decimal("-140000")

    rising = _values(
        "<pre>THE EMPLOYMENT SITUATION -- DECEMBER 2024\n"
        "Total nonfarm payroll employment increased by 256,000 in December, and the "
        "unemployment rate changed little at 4.1 percent.\n</pre>",
        "empsit",
    )
    assert rising["payrolls_change_thousands"] == Decimal("256")


def test_unemployment_rate_level_is_not_signed_by_its_direction() -> None:
    """A rate is a level: "edged down to 4.0 percent" is four, not minus four."""
    values = _values(
        "<pre>THE EMPLOYMENT SITUATION -- JANUARY 2025\n"
        "Total nonfarm payroll employment rose by 143,000 in January, and the "
        "unemployment rate edged down to 4.0 percent.\n</pre>",
        "empsit",
    )

    assert values["unemployment_rate_pct"] == Decimal("4.0")


def test_explicitly_unchanged_statistics_are_stated_zeros() -> None:
    """The release's own words say the statistic did not move, so zero is stated.

    Both the CPI headline and the payroll count print an unchanged month with no
    magnitude at all; the zero comes from the words, not from a default.
    """
    cpi = _values(
        "<pre>CONSUMER PRICE INDEX - SEPTEMBER 2025\n"
        "The Consumer Price Index for All Urban Consumers (CPI-U) was unchanged on a "
        "seasonally adjusted basis in September.\n"
        "The index for all items less food and energy was unchanged in September.\n</pre>",
        "cpi",
    )
    assert cpi["cpi_headline_sa_mom_pct"] == Decimal("0")
    assert cpi["cpi_core_sa_mom_pct"] == Decimal("0")

    empsit = _values(
        "<pre>THE EMPLOYMENT SITUATION -- APRIL 2025\n"
        "Total nonfarm payroll employment was unchanged in April, and the unemployment "
        "rate was unchanged at 4.2 percent.\n</pre>",
        "empsit",
    )
    assert empsit["payrolls_change_thousands"] == Decimal("0")
    assert empsit["payrolls_change_jobs"] == Decimal("0")


def test_annual_only_payload_leaves_the_monthly_change_absent() -> None:
    """A payload stating only the 12-month change states no monthly change.

    The annual sentence shares the monthly statement's verbs and unit, so a
    parser that reads the first matching figure substitutes an annual rise for
    the missing monthly value.
    """
    values = _values(
        "<pre>CONSUMER PRICE INDEX - JULY 2026\n"
        "The all items index rose 3.4 percent for the 12 months ending July, after "
        "rising 3.5 percent over the 12 months ending June.\n"
        "The all items less food and energy index rose 2.5 percent over the last 12 months.\n</pre>",
        "cpi",
    )

    assert "cpi_headline_sa_mom_pct" not in values
    assert "cpi_core_sa_mom_pct" not in values
    assert values["cpi_headline_nsa_yoy_pct"] == Decimal("3.4")
    assert values["cpi_core_nsa_yoy_pct"] == Decimal("2.5")


def test_monthly_and_annual_changes_are_read_from_their_own_statements() -> None:
    """Both figures are stated side by side, and each must land in its own field."""
    values = _values(
        "<pre>CONSUMER PRICE INDEX - APRIL 2025\n"
        "The Consumer Price Index for All Urban Consumers (CPI-U) increased 0.2 percent "
        "on a seasonally adjusted basis in April.\n"
        "The all items index rose 2.3 percent for the 12 months ending April.\n"
        "The all items less food and energy index rose 2.8 percent over the last 12 months.\n</pre>",
        "cpi",
    )

    assert values["cpi_headline_sa_mom_pct"] == Decimal("0.2")
    assert values["cpi_headline_nsa_yoy_pct"] == Decimal("2.3")
