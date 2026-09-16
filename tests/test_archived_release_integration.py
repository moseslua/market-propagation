"""The explicit archived-source path, proved against a real sealed release dataset.

Every test here builds its own sealed ``releases`` Parquet and raw store out of the
real archived payload bytes that follow, then drives the real audit, event-card and
reproduction paths over it with the network refused at the transport boundary. What
is being defended is a single property: a first release the study reports must come
from bytes that were re-read and reparsed against the record citing them, and an
explicitly selected archive that fails must be a blocked result rather than a quiet
fallback to a live fetch.

The two payload excerpts are copied from the captures the study acquired, not written
from an idealised example. Only the lead text is carried: the full captures are about
a megabyte each, and every statistic under test is stated in the first paragraphs.

Six failures are pinned because each one passes silently:

* A dataset whose stored normalized values disagree with the payload it cites must be
  refused. Trusting the row would report first-release values no archived byte states.
* A payload whose own masthead period disagrees with the stored reference period is a
  different release than the row claims, and joining it to the wrong release is
  invisible downstream.
* A row whose receipt names a different payload than the row cites must be refused.
  The hash, the blob behind it and the stored values can all agree with each other
  while the receipt says those bytes were never received for that identity, so the
  release would verify while citing a capture it does not own.
* A stored embargo instant that disagrees with the instant the payload itself states
  must be refused. A dropped timezone is the ordinary way this happens, and it moves
  the measurement window relative to the release with nothing else in the row to show.
* A release the dataset has no record for must block its own event and must not fetch.
  A network fallback here would report success for a payload nothing verified.
* An audit that successfully loads original releases must still report no empirical
  eligibility, and the reproduction must refuse to fit those releases as if they were
  its synthetic sample. That separation is proved by comparing the sealed panels of a
  dataset-named run and an unnamed one byte for byte, since a regression that reached
  the sample would leave every prose claim about it unchanged.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
from decimal import Decimal
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from market_propagation.domain import Availability, Clock, Release
from market_propagation.ingest.macro_releases import (
    ACQUISITION_SEALED_DATASET,
    ArchivedReleaseSource,
    archive_url,
    parse_release_payload,
)
from market_propagation.ingest.transport import HttpTransport
from market_propagation.operations import event_card, run_audit
from market_propagation.storage import RawStore, hash_bytes, hash_file, write_parquet

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# Consumer Price Index for the December 2024 reference period, published January 15,
# 2025, from the capture at
# https://www.bls.gov/news.release/archives/cpi_01152025.htm. The lead ``<PRE>`` block
# carries the embargo header, the masthead and the headline and core statements; the
# "Not seasonally adjusted CPI measures" paragraph carries the index level. Both are
# needed for all six stored statistics.
CPI_2025_01_SOURCE = "https://www.bls.gov/news.release/archives/cpi_01152025.htm"
CPI_2025_01_HTML = """\
<PRE>Transmission of material in this release is embargoed until
8:30 a.m. (ET) Wednesday, January 15, 2025     USDL-25-0021

Technical information: (202) 691-7000  *  cpi_info@bls.gov  *  www.bls.gov/cpi
Media contact:         (202) 691-5902  *  PressOffice@bls.gov

CONSUMER PRICE INDEX - DECEMBER 2024

The Consumer Price Index for All Urban Consumers (CPI-U) increased 0.4 percent on a seasonally adjusted basis
in December, after rising 0.3 percent in November, the U.S. Bureau of Labor Statistics reported today. Over the
last 12 months, the all items index increased 2.9 percent before seasonal adjustment.

The index for energy rose 2.6 percent in December, accounting for over forty percent of the monthly all items
increase. The gasoline index increased 4.4 percent over the month. The index for food also increased in December,
rising 0.3 percent as both the index for food at home and the index for food away from home increased 0.3
percent each.

The index for all items less food and energy rose 0.2 percent in December, after increasing 0.3 percent in each
of the previous 4 months. Indexes that increased in December include shelter, airline fares, used cars and trucks,
new vehicles, motor vehicle insurance, and medical care. The indexes for personal care, communication, and
alcoholic beverages were among the few major indexes that decreased over the month.

The all items index rose 2.9 percent for the 12 months ending December, after rising 2.7 percent over the 12
months ending November. The all items less food and energy index rose 3.2 percent over the last 12 months. The
energy index decreased 0.5 percent for the 12 months ending December. The food index increased 2.5 percent over
the last year.

</PRE>
Not seasonally adjusted CPI measures

The Consumer Price Index for All Urban Consumers (CPI-U) increased 2.9 percent over the last 12 months to an
index level of 315.605 (1982-84=100). For the month, the index was unchanged prior to seasonal adjustment.
"""

# Employment Situation for the March 2025 reference period, published April 4, 2025,
# from the capture at
# https://www.bls.gov/news.release/archives/empsit_04042025.htm. This family prints its
# whole release inside one ``<PRE>`` block, so the excerpt runs to the end of the
# disclosed revisions and no further.
EMPSIT_2025_04_SOURCE = "https://www.bls.gov/news.release/archives/empsit_04042025.htm"
EMPSIT_2025_04_HTML = """\
<pre>
Transmission of material in this news release is embargoed until	               USDL-25-0452
8:30 a.m. (ET) Friday, April 4, 2025

Technical information:
 Household data:      (202) 691-6378  *  cpsinfo@bls.gov  *  www.bls.gov/cps
 Establishment data:  (202) 691-6555  *  cesinfo@bls.gov  *  www.bls.gov/ces

Media contact:	      (202) 691-5902  *  PressOffice@bls.gov


                             THE EMPLOYMENT SITUATION -- MARCH 2025


Total nonfarm payroll employment rose by 228,000 in March, and the unemployment rate changed
little at 4.2 percent, the U.S. Bureau of Labor Statistics reported today. Job gains occurred
in health care, in social assistance, and in transportation and warehousing. Employment
declined in federal government.

In March, average hourly earnings for all employees on private nonfarm payrolls rose by 9
cents, or 0.3 percent, to $36.00. Over the past 12 months, average hourly earnings have
increased by 3.8 percent.

The change in total nonfarm payroll employment for January was revised down by 14,000, from
+125,000 to +111,000, and the change for February was revised down by 34,000, from +151,000 to
+117,000. With these revisions, employment in January and February combined is 48,000 lower
than previously reported.
</pre>
"""

CPI_SCHEDULED_AT = dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC)
EMPSIT_SCHEDULED_AT = dt.datetime(2025, 4, 4, 12, 30, tzinfo=dt.UTC)
CPI_RECEIVED_AT = dt.datetime(2026, 9, 13, 13, 3, 27, 644000, tzinfo=dt.UTC)

#: One archived release as this test's dataset stores it: the raw body, the study's
#: canonical family, the archive slug, the reference period and the instant the
#: original capture was received.
ARCHIVED = (
    {
        "event_id": "cpi_2025_01",
        "family": "cpi",
        "slug": "cpi",
        "reference_period": "2024-12",
        "scheduled_at": CPI_SCHEDULED_AT,
        "received_at": CPI_RECEIVED_AT,
        "html": CPI_2025_01_HTML,
        "source": CPI_2025_01_SOURCE,
    },
    {
        "event_id": "empsit_2025_04",
        "family": "employment",
        "slug": "empsit",
        "reference_period": "2025-03",
        "scheduled_at": EMPSIT_SCHEDULED_AT,
        "received_at": CPI_RECEIVED_AT,
        "html": EMPSIT_2025_04_HTML,
        "source": EMPSIT_2025_04_SOURCE,
    },
)


def _forbid_network(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Refuse every outbound request, recording the URL that attempted it.

    The hosts still resolve and the clock still runs; what is refused is the request
    itself. A test that passes with this installed passed using only local bytes, and
    a test that reaches for the network fails with the URL it tried.
    """
    attempted: list[str] = []

    def refuse(self: Any, url: str, *args: Any, **kwargs: Any) -> Any:
        attempted.append(url)
        raise httpx.ConnectError(f"network is forbidden in this test: {url}")

    monkeypatch.setattr(httpx.Client, "get", refuse)
    return attempted


def _sealed_release_row(
    record: dict[str, Any],
    *,
    store: RawStore,
    values: dict[str, Decimal],
    revisions: dict[str, Decimal],
    observed: dt.datetime,
) -> Release:
    """One release row built by the real parser from the record's own raw bytes."""
    provenance = store.put(
        record["html"].encode("utf-8"),
        source=record["source"],
        received_time=record["received_at"],
        record_id=record["event_id"],
        metadata={
            "acquisition_method": "standard_browser_http_response",
            "content_type": "text/html",
            "event_id": record["event_id"],
            "family": record["family"],
            "source_url": record["source"],
            "source_availability": "unknown_historical",
            "status": 200,
            "payload_complete": True,
        },
    )
    return Release(
        event_id=record["event_id"],
        family=record["family"],
        scheduled_at=record["scheduled_at"],
        reference_period=record["reference_period"],
        values=values,
        clock=Clock(
            observed,
            record["received_at"],
            Availability.unknown(basis="late_archived_browser_capture"),
        ),
        provenance=provenance,
        revisions=revisions,
    )


def _archive_fixture(
    tmp_path: pathlib.Path,
    *,
    records: tuple[dict[str, Any], ...] = ARCHIVED,
    overrides: dict[str, dict[str, Any]] | None = None,
) -> pathlib.Path:
    """Seal a real releases dataset and its raw store, returning the dataset path.

    ``overrides`` replaces stored columns for one event before sealing, which is how
    a genuinely corrupt archive is produced: the bytes are real and the seal is real,
    only the record is wrong.
    """
    root = tmp_path / "bls-normalized"
    root.mkdir(parents=True, exist_ok=True)
    store = RawStore(root / "raw")
    rows: list[Release] = []
    for record in records:
        _title, _period, observed, values, revisions, _statements, _usdl, _agreement = (
            parse_release_payload(
                record["html"],
                family_slug=record["slug"],
                source_url=record["source"],
                scheduled_at=record["scheduled_at"],
            )
        )
        assert values, f"{record['event_id']} yielded no values; the excerpt is unusable"
        row = _sealed_release_row(
            record,
            store=store,
            values=values,
            revisions=revisions,
            observed=observed or record["scheduled_at"],
        )
        rows.append(row)
    frame_rows: Any = rows
    if overrides:
        from market_propagation.storage import resolve_rows

        # ``resolve_rows`` encodes the table's JSON columns to text for sealing, so
        # they are decoded back here before they are handed to ``write_parquet``,
        # which does that encoding itself. Passing the text through would store a
        # JSON string of a JSON document.
        plain = resolve_rows(rows, "releases")
        for row in plain:
            for column in ("values_json", "revisions_json"):
                row[column] = json.loads(row[column])
        for index, record in enumerate(records):
            for column, value in overrides.get(record["event_id"], {}).items():
                plain[index][column] = value
        frame_rows = plain
    write_parquet(frame_rows, root / "releases.parquet", table="releases")
    return root / "releases.parquet"


def _routes(*, markets: list[dict[str, Any]]) -> dict[str, Any]:
    """Listing fixtures for the venue side, so the audit reaches its release step."""
    return {
        "/historical/markets/KXCPI-25JAN-T0.3/candlesticks": (
            200,
            {"candlesticks": []},
            {},
        ),
        "/historical/trades": (200, {"trades": [], "cursor": None}, {}),
        "/historical/cutoff": (
            200,
            {
                "market_positions_last_updated_ts": "2025-06-15T00:00:00Z",
                "market_settled_ts": "2025-06-15T00:00:00Z",
                "orders_updated_ts": "2025-06-15T00:00:00Z",
                "trades_created_ts": "2025-06-15T00:00:00Z",
            },
            {},
        ),
        "/historical/markets": (200, {"markets": markets, "cursor": None}, {}),
        "/markets": (200, {"markets": markets, "cursor": None}, {}),
        "/series": (200, {"series": []}, {}),
    }


@pytest.fixture()
def configs(tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    """A two-event cohort covering both archived families, with its own window."""
    import yaml

    def write(path: pathlib.Path, payload: Any) -> pathlib.Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(payload), encoding="utf-8")
        return path

    cohort = write(
        tmp_path / "configs" / "cohort.yaml",
        {
            "cohort": {
                "cohort_id": "archived_release_test",
                "verified_eligible_market_ids": [],
                "verified_eligible_market_id_count": 0,
            },
            "events": [
                {
                    "event_id": "cpi_2025_01",
                    "family": "cpi",
                    "reference_period": "2024-12",
                    "scheduled_at": CPI_SCHEDULED_AT.isoformat(),
                    "source_url": "https://www.bls.gov/schedule/2025/01_sched_list.htm",
                    "initial_release_url": CPI_2025_01_SOURCE,
                },
                {
                    "event_id": "empsit_2025_04",
                    "family": "employment",
                    "reference_period": "2025-03",
                    "scheduled_at": EMPSIT_SCHEDULED_AT.isoformat(),
                    "source_url": "https://www.bls.gov/schedule/2025/04_sched_list.htm",
                    "initial_release_url": EMPSIT_2025_04_SOURCE,
                },
            ],
        },
    )
    windows = write(
        tmp_path / "configs" / "event_windows.yaml",
        {
            "windows": {
                "main": {
                    "window_id": "main",
                    "pre_event_seconds": 1800,
                    "post_event_seconds": 3600,
                    "pre_event_role": "baseline",
                    "post_event_role": "response",
                }
            },
            "alignment": {
                "reference_point": "scheduled_release_time",
                "reference_precision": "minute",
                "source_calendar_timezone": "America/New_York",
                "storage_timezone": "UTC",
            },
            "secondary_horizons_seconds": [300],
            "restrictions": ["no_expectation_source_available"],
        },
    )
    return cohort, windows


def test_a_sealed_archive_restores_the_first_release_values_its_bytes_state(
    tmp_path: pathlib.Path,
) -> None:
    """The values a card reports are the ones the archived payload itself states."""
    dataset = _archive_fixture(tmp_path)
    store = RawStore(tmp_path / "audit-raw")
    source = ArchivedReleaseSource(dataset, dest_store=store)

    release, blocked = source.get_initial_release(
        "cpi",
        CPI_SCHEDULED_AT.date(),
        scheduled_at=CPI_SCHEDULED_AT,
        expected_event_id="cpi_2025_01",
    )

    assert blocked is None
    assert release is not None
    # The six values, at the exact figures the payload prints: the monthly headline
    # and core, the 12-month headline and core, the index level and the unadjusted
    # 12-month change. A missing or substituted one would be a fabricated shock.
    assert release.values == {
        "cpi_headline_sa_mom_pct": Decimal("0.4"),
        "cpi_core_sa_mom_pct": Decimal("0.2"),
        "cpi_headline_nsa_yoy_pct": Decimal("2.9"),
        "cpi_core_nsa_yoy_pct": Decimal("3.2"),
        "cpi_u_nsa_index_level": Decimal("315.605"),
        "cpi_u_nsa_yoy_pct": Decimal("2.9"),
    }
    # CPI discloses no revision, and an empty map is the absence of a claim.
    assert release.revisions == {}
    assert release.reference_period == "2024-12"
    assert release.usdl_number == "USDL-25-0021"
    assert release.schedule_agreement == "agrees_with_calendar"
    assert release.embargo_time_from_payload == CPI_SCHEDULED_AT
    assert release.acquisition_method == ACQUISITION_SEALED_DATASET
    assert release.input_dataset_hash == hash_file(dataset)

    # The original capture happened long after publication, so the interval in which
    # the payload was usable stays unknown; the schedule is never promoted to one.
    assert release.clock.usable_time is None
    assert release.clock.availability.upper is None
    assert release.clock.source_time == CPI_SCHEDULED_AT


def test_the_original_bytes_are_retrievable_from_the_audits_own_raw_store(
    tmp_path: pathlib.Path,
) -> None:
    """The card's evidence chain ends in bytes this run can re-read, not a citation."""
    dataset = _archive_fixture(tmp_path)
    store = RawStore(tmp_path / "audit-raw")
    source = ArchivedReleaseSource(dataset, dest_store=store)

    release, blocked = source.get_initial_release(
        "cpi", CPI_SCHEDULED_AT.date(), scheduled_at=CPI_SCHEDULED_AT
    )
    assert blocked is None and release is not None

    # The payload the audit holds is byte-identical to the archive's original, and
    # the store re-hashes it on read, so an altered copy cannot pass.
    stored = store.get(release.provenance.raw_hash)
    assert stored == CPI_2025_01_HTML.encode("utf-8")
    with pytest.raises(FileNotFoundError, match="no payload stored"):
        store.get("0" * 64)

    receipt = store.receipt(release.provenance.record_id, source=release.source_url)
    assert receipt is not None
    metadata = receipt["metadata"]
    # The original acquisition is carried as inherited metadata, and the archive
    # identity is recorded separately so the copy is not mistaken for this run's fetch.
    assert metadata["acquisition_method"] == ACQUISITION_SEALED_DATASET
    assert metadata["original_acquisition_method"] == "standard_browser_http_response"
    assert metadata["original_status"] == 200
    assert metadata["archive_dataset_content_hash"] == hash_file(dataset)
    assert metadata["archive_raw_hash"] == release.provenance.raw_hash
    assert metadata["source_availability"] == "unknown_historical"


def test_verify_all_restores_every_record_and_names_their_own_provenance(
    tmp_path: pathlib.Path,
) -> None:
    """Every record resolves to its own payload, with no record quietly skipped."""
    dataset = _archive_fixture(tmp_path)
    source = ArchivedReleaseSource(dataset)

    verified = source.verify_all()

    assert verified["records_verified"] == len(ARCHIVED)
    assert verified["records_blocked"] == 0
    assert verified["all_records_verified"] is True
    assert verified["network_used"] is False
    assert verified["usable_time"] is None
    by_event = {record["event_id"]: record for record in verified["records"]}
    assert set(by_event) == {record["event_id"] for record in ARCHIVED}
    for record in ARCHIVED:
        row = by_event[record["event_id"]]
        assert row["values_verified_against_original_bytes"] is True
        assert row["reference_period"] == record["reference_period"]
        assert row["original_source_url"] == record["source"]
        assert row["usdl_number"]
    # The Employment Situation states revisions; they stay in their own field rather
    # than being merged into the first-release values.
    assert by_event["empsit_2025_04"]["values_key_count"] == 6
    assert by_event["empsit_2025_04"]["revisions_key_count"] > 0
    assert by_event["empsit_2025_04"]["revisions_kept_separate"] is True


def test_the_two_release_traces_describe_a_record_with_the_same_keys(
    tmp_path: pathlib.Path,
) -> None:
    """The load path and the verify-only path must not summarize a record differently.

    Both describe the same verified record to different consumers, so a field
    reaching one and not the other is invisible until a reader of the narrower
    trace asks for it.
    """
    dataset = _archive_fixture(tmp_path)
    source = ArchivedReleaseSource(dataset, dest_store=RawStore(tmp_path / "dest"))
    loaded = source.get_initial_release(
        "cpi", CPI_SCHEDULED_AT.date(), scheduled_at=CPI_SCHEDULED_AT
    )[0]
    assert loaded is not None

    audit_trace = next(
        record for record in source.as_dict()["records"] if record["event_id"] == "cpi_2025_01"
    )
    verify_trace = next(
        record for record in source.verify_all()["records"] if record["event_id"] == "cpi_2025_01"
    )

    assert set(audit_trace) == set(verify_trace)
    # The two differ only in the facts each path actually holds.
    for record in (audit_trace, verify_trace):
        assert record["family"] == "cpi"
        assert record["original_source_url"] == CPI_2025_01_SOURCE
        assert record["archived_raw_hash"]
        assert record["schedule_agreement"] == "agrees_with_calendar"
        assert record["values_verified_against_original_bytes"] is True
    assert audit_trace["audit_raw_hash"] == loaded.provenance.raw_hash
    assert verify_trace["audit_raw_hash"] is None, (
        "the read-only path stores no copy, so it names no audit-side hash"
    )


def test_an_archived_record_disagreeing_with_its_own_bytes_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """A stored value the payload does not state is refused, not reported.

    The override changes one figure to a plausible neighbour. Nothing about the seal
    is broken and the payload still parses, so only re-parsing the bytes catches it.
    """
    dataset = _archive_fixture(
        tmp_path,
        overrides={
            "cpi_2025_01": {
                "values_json": {
                    "cpi_headline_sa_mom_pct": Decimal("0.3"),
                    "cpi_core_sa_mom_pct": Decimal("0.2"),
                    "cpi_headline_nsa_yoy_pct": Decimal("2.9"),
                    "cpi_core_nsa_yoy_pct": Decimal("3.2"),
                    "cpi_u_nsa_index_level": Decimal("315.605"),
                    "cpi_u_nsa_yoy_pct": Decimal("2.9"),
                }
            }
        },
    )
    source = ArchivedReleaseSource(dataset)

    release, blocked = source.get_initial_release(
        "cpi", CPI_SCHEDULED_AT.date(), scheduled_at=CPI_SCHEDULED_AT
    )

    assert release is None
    assert blocked is not None
    assert blocked["recorded"] is True
    assert blocked["empty_result"] is False
    assert blocked["network_fallback_attempted"] is False
    assert blocked["reason"] == "archived_record_values_do_not_match_its_own_payload"
    mismatch = blocked["detail"]["value_mismatches"]["cpi_headline_sa_mom_pct"]
    assert mismatch == {"archived": "0.3", "reparsed": "0.4"}
    # The record that did verify is unaffected; one bad row blocks one release.
    assert source.verify_all()["records_verified"] == len(ARCHIVED) - 1


def test_a_payload_whose_masthead_period_disagrees_with_the_record_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """The stored period and the payload's own masthead must name the same month.

    A mismatch means the row points at a different release than it claims, and the
    join that follows would silently attach April's values to March's event.
    """
    dataset = _archive_fixture(
        tmp_path, overrides={"empsit_2025_04": {"reference_period": "2025-04"}}
    )
    source = ArchivedReleaseSource(dataset)

    release, blocked = source.get_initial_release(
        "empsit", EMPSIT_SCHEDULED_AT.date(), scheduled_at=EMPSIT_SCHEDULED_AT
    )

    assert release is None
    assert blocked is not None
    assert blocked["reason"] == "payload_masthead_period_disagrees_with_archived_record"
    assert blocked["detail"]["payload_masthead_period"] == "MARCH 2025"
    assert blocked["detail"]["archived_reference_period"] == "2025-04"
    assert blocked["detail"]["archived_reference_period_label"] == "APRIL 2025"


def test_a_release_at_a_different_scheduled_instant_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """A requested instant the record disagrees with is a different release, so it blocks.

    The cohort schedule is the instant the window is built from, so accepting a row
    whose own scheduled time differs would place the measurement window on the wrong
    side of the release.
    """
    dataset = _archive_fixture(tmp_path)
    source = ArchivedReleaseSource(dataset)

    release, blocked = source.get_initial_release(
        "cpi",
        CPI_SCHEDULED_AT.date(),
        scheduled_at=CPI_SCHEDULED_AT + dt.timedelta(minutes=1),
    )

    assert release is None
    assert blocked is not None
    assert blocked["reason"] == "archived_record_schedule_mismatch"
    assert (
        blocked["detail"]["requested_scheduled_at"]
        == (CPI_SCHEDULED_AT + dt.timedelta(minutes=1)).isoformat()
    )
    assert blocked["detail"]["archived_scheduled_at"] == CPI_SCHEDULED_AT.isoformat()
    assert blocked["network_fallback_attempted"] is False


def test_a_record_for_a_different_event_than_the_one_requested_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """The archive row must be the event the cohort asked for, not merely its neighbour."""
    dataset = _archive_fixture(tmp_path)
    source = ArchivedReleaseSource(dataset)

    release, blocked = source.get_initial_release(
        "cpi",
        CPI_SCHEDULED_AT.date(),
        scheduled_at=CPI_SCHEDULED_AT,
        expected_event_id="cpi_2025_09",
    )

    assert release is None
    assert blocked is not None
    assert blocked["reason"] == "archived_record_event_id_mismatch"
    assert blocked["detail"] == {
        "requested_event_id": "cpi_2025_09",
        "archived_event_id": "cpi_2025_01",
    }


def test_a_release_the_archive_lacks_is_blocked_rather_than_fetched(
    tmp_path: pathlib.Path,
) -> None:
    """An explicitly selected archive with no record for a release blocks that event."""
    dataset = _archive_fixture(tmp_path, records=(ARCHIVED[0],))
    source = ArchivedReleaseSource(dataset)

    release, blocked = source.get_initial_release(
        "empsit", EMPSIT_SCHEDULED_AT.date(), scheduled_at=EMPSIT_SCHEDULED_AT
    )

    assert release is None
    assert blocked is not None
    assert blocked["reason"] == "no_record_for_release_in_sealed_dataset"
    assert blocked["source_kind"] == ACQUISITION_SEALED_DATASET
    assert blocked["network_fallback_attempted"] is False
    assert blocked["empty_result"] is False
    assert blocked["detail"]["publication_date"] == "2025-04-04"


def test_a_corrupted_archive_payload_is_blocked_not_replaced(
    tmp_path: pathlib.Path,
) -> None:
    """A stored blob that no longer matches its hash is a blocked record."""
    dataset = _archive_fixture(tmp_path)
    raw_root = dataset.parent / "raw"
    source = ArchivedReleaseSource(dataset)
    row = next(
        record for record in source.verify_all()["records"] if record["event_id"] == "cpi_2025_01"
    )
    blob = raw_root / "blobs" / row["archived_raw_hash"][:2] / f"{row['archived_raw_hash']}.bin"
    assert blob.exists()
    blob.write_bytes(b"<html>this is not the archived release</html>")

    release, blocked = ArchivedReleaseSource(dataset).get_initial_release(
        "cpi", CPI_SCHEDULED_AT.date(), scheduled_at=CPI_SCHEDULED_AT
    )

    assert release is None
    assert blocked is not None
    assert blocked["reason"] == "original_payload_unreadable_in_archive_raw_store"
    assert blocked["empty_result"] is False
    assert "error" in blocked["detail"]


def test_a_row_whose_receipt_names_a_different_payload_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """A row reusing another capture's identity is refused, not accepted as verified bytes.

    The receipt index is keyed by source and record identity, so a stale identity
    resolves to *that* capture's receipt instead of its own. Everything else about
    the row still agrees: the hash, the blob behind it and the stored values are all
    self-consistent, so the receipt is the only thing that names the substitution.
    Without this check the release verifies and reports success while citing bytes
    its own receipt says it never received.
    """
    dataset = _archive_fixture(
        tmp_path,
        overrides={
            "cpi_2025_01": {
                "record_id": "empsit_2025_04",
                "source": EMPSIT_2025_04_SOURCE,
            }
        },
    )
    # The destination store is supplied so that a guard this row trips is the only
    # thing standing between it and an accepted release: without it, dropping the
    # check would fail on the missing store instead of on the acceptance.
    source = ArchivedReleaseSource(dataset, dest_store=RawStore(tmp_path / "audit-raw"))

    release, blocked = source.get_initial_release(
        "cpi", CPI_SCHEDULED_AT.date(), scheduled_at=CPI_SCHEDULED_AT
    )

    assert release is None
    assert blocked is not None
    assert blocked["recorded"] is True
    assert blocked["empty_result"] is False
    assert blocked["network_fallback_attempted"] is False
    assert blocked["reason"] == "archived_receipt_names_a_different_payload"
    # Both hashes are real and are reported: the receipt names the Employment
    # Situation payload, while the row cites the Consumer Price Index one.
    receipt_hash = hash_bytes(EMPSIT_2025_04_HTML.encode("utf-8"))
    record_hash = hash_bytes(CPI_2025_01_HTML.encode("utf-8"))
    assert receipt_hash != record_hash
    assert blocked["detail"] == {
        "receipt_raw_hash": receipt_hash,
        "record_raw_hash": record_hash,
    }
    # One row that cannot be traced to its own receipt blocks one release; the
    # record whose identity was not reused still verifies.
    assert source.verify_all()["records_verified"] == len(ARCHIVED) - 1


def test_a_stored_embargo_instant_disagreeing_with_its_payload_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """The stored embargo instant must be the instant the payload states, or it blocks.

    The override stores 8:30 in UTC where the payload's own header says 8:30 a.m.
    Eastern, which is what a dropped timezone looks like once the row is written.
    Nothing else about the row changes, so only the comparison against the reparsed
    payload catches it, and both instants are reported so the disagreement is
    legible rather than merely fatal.
    """
    stored = dt.datetime(2025, 1, 15, 8, 30, tzinfo=dt.UTC)
    dataset = _archive_fixture(tmp_path, overrides={"cpi_2025_01": {"observed_at": stored}})
    # As above: with a destination store supplied, a dropped check surfaces as the
    # release being accepted rather than as a missing destination.
    source = ArchivedReleaseSource(dataset, dest_store=RawStore(tmp_path / "audit-raw"))

    release, blocked = source.get_initial_release(
        "cpi", CPI_SCHEDULED_AT.date(), scheduled_at=CPI_SCHEDULED_AT
    )

    assert release is None
    assert blocked is not None
    assert blocked["recorded"] is True
    assert blocked["empty_result"] is False
    assert blocked["network_fallback_attempted"] is False
    assert blocked["reason"] == "payload_embargo_instant_disagrees_with_archived_record"
    assert blocked["detail"] == {
        "payload_embargo_time": CPI_SCHEDULED_AT.isoformat(),
        "archived_observed_at": stored.isoformat(),
    }


def test_the_audit_reports_the_archive_and_still_establishes_no_eligibility(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An audit over a real archive reports its source, its values, and its limits."""
    attempted = _forbid_network(monkeypatch)
    dataset = _archive_fixture(tmp_path)
    cohort_path, windows_path = configs
    out = tmp_path / "audit"

    result = run_audit(
        out,
        cohort_path=cohort_path,
        windows_path=windows_path,
        max_contracts=1,
        max_candle_contracts=1,
        release_dataset=dataset,
    )

    source = result["release_source"]
    assert source["kind"] == ACQUISITION_SEALED_DATASET
    assert source["explicitly_selected"] is True
    assert source["network_release_requests_issued"] is False
    assert source["fallback_to_network_used"] is False
    assert source["dataset"]["content_hash"] == hash_file(dataset)
    assert source["dataset"]["row_count"] == len(ARCHIVED)
    assert source["archive"]["records_loaded"] == len(ARCHIVED)
    # No request was issued for any release URL, in the archive path.
    assert not [url for url in attempted if "news.release" in url]

    events = {event["event_id"]: event for event in result["coverage"]["events"]}
    assert len(events) == 2
    for record in ARCHIVED:
        event = events[record["event_id"]]
        release = event["release"]
        assert release is not None, record["event_id"]
        assert release["acquisition_method"] == ACQUISITION_SEALED_DATASET
        assert release["input_dataset_hash"] == hash_file(dataset)
        assert release["reference_period"] == record["reference_period"]
        gate = next(
            gate for gate in event["coverage_gates"] if gate["gate"] == "release_payload_archived"
        )
        assert gate["satisfied"] is True
        assert "release_payload_archived" not in event["unsatisfied_gates"]

    # Loading original releases certifies the release, never the cohort: eligibility
    # is still read from the study's own verified market ids, here deliberately empty.
    assert result["eligibility"]["study_eligibility_established"] is False
    assert result["eligibility"]["verified_eligible_market_ids"] == []
    assert result["access"]["http_success_is_not_study_eligibility"] is True
    assert source["historical_market_rule_versions_certified"] is False

    # The source is written as its own artifact, so an older audit is untouched.
    written = pathlib.Path(result["outputs"]["release_source"])
    assert written.name == "release_source.json"
    assert json.loads(written.read_text())["dataset"]["content_hash"] == hash_file(dataset)


def test_the_event_card_cites_the_archive_the_audit_actually_used(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The card's first-release block names its archived source and its true values."""
    _forbid_network(monkeypatch)
    dataset = _archive_fixture(tmp_path)
    cohort_path, windows_path = configs
    out = tmp_path / "audit"
    result = run_audit(
        out,
        cohort_path=cohort_path,
        windows_path=windows_path,
        max_contracts=1,
        max_candle_contracts=1,
        release_dataset=dataset,
    )
    assert result["complete"] is False

    card = event_card(out, tmp_path / "card.json", event_id="empsit_2025_04")

    assert card["status"] == "created"
    first = card["first_release"]
    assert first["retrieved"] is True
    assert first["values"] == {
        "payrolls_change_jobs": "228000",
        "payrolls_change_thousands": "228",
        "unemployment_rate_pct": "4.2",
        "avg_hourly_earnings_mom_pct": "0.3",
        "avg_hourly_earnings_usd": "36.00",
        "avg_hourly_earnings_yoy_pct": "3.8",
    }
    # Revisions stay separate and carry the months the payload names.
    assert first["revisions"]["payrolls_change_jobs_revised_February"] == "117000"
    assert first["revisions"]["payrolls_change_jobs_prior_February"] == "151000"
    assert "payrolls_change_jobs" not in first["revisions"]
    assert first["gate_satisfied"] is True

    source = first["source"]
    assert source["kind"] == ACQUISITION_SEALED_DATASET
    assert source["explicitly_selected"] is True
    assert source["network_release_requests_issued"] is False
    assert source["dataset"]["content_hash"] == hash_file(dataset)
    assert source["input_dataset_hash"] == hash_file(dataset)
    assert source["acquisition_method"] == ACQUISITION_SEALED_DATASET
    assert source["values_verified_against_original_bytes"] is True
    assert source["archived_raw_hash"] == first["raw_hash"]
    # The card states what the release alone cannot establish.
    assert "the market rule version in force at this release" in source["not_evidence_of"]
    assert "the quote coverage of any contract at this release" in source["not_evidence_of"]

    verification = card["evidence_verification"]
    assert verification["failed_hash_count"] == 0
    assert verification["verified_hash_count"] >= 1
    assert verification["release_source"]["kind"] == ACQUISITION_SEALED_DATASET
    assert verification["release_source"]["records_loaded"] == len(ARCHIVED)


def test_a_dataset_whose_rows_share_an_identity_is_refused(tmp_path: pathlib.Path) -> None:
    """Two rows for one event on one day cannot both be the original capture.

    The identity index is what every count is derived from, so an unannounced
    collision would drop a row and still report the dataset fully verified.
    """
    dataset = _archive_fixture(tmp_path, records=(*ARCHIVED, ARCHIVED[0]))

    with pytest.raises(ValueError, match="carries two rows for"):
        ArchivedReleaseSource(dataset)


def test_a_release_the_dataset_has_no_record_for_blocks_its_own_event(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A named dataset that lacks an event blocks it rather than fetching it."""
    _forbid_network(monkeypatch)
    dataset = _archive_fixture(tmp_path, records=(ARCHIVED[1],))
    cohort_path, windows_path = configs
    out = tmp_path / "audit"

    result = run_audit(
        out,
        cohort_path=cohort_path,
        windows_path=windows_path,
        max_contracts=1,
        max_candle_contracts=1,
        release_dataset=dataset,
    )

    assert result["release_source"]["network_release_requests_issued"] is False
    coverage = json.loads((out / "coverage.json").read_text())
    cpi = next(e for e in coverage["events"] if e["event_id"] == "cpi_2025_01")
    assert cpi["release"] is None

    # The card must not attribute first-release values to an event whose release it
    # never read, and the note must describe the absence rather than a read.
    card = event_card(out, tmp_path / "card.json", event_id="cpi_2025_01")
    release = card["first_release"]
    assert release["retrieved"] is False
    assert release["values_are_first_release"] is None
    assert release["revisions_kept_separate"] is None
    note = release["source"]["provenance_note"]
    assert "no first-release payload was retrieved" in note
    assert "verified against the original archived bytes it cites" not in note
    assert release["source"]["acquisition_method"] is None


def test_run_audit_without_a_dataset_keeps_the_network_release_path(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Omitting the dataset leaves the fetch path, and its failures, unchanged."""
    monkeypatch.setattr(
        HttpTransport,
        "__init__",
        _install_client(
            {
                "/orderbook": (200, {}, {}),
                "news.release/archives/cpi_": (403, "Access Denied", {}),
                "news.release/archives/empsit_": (403, "Access Denied", {}),
                **_routes(markets=[]),
            }
        ),
    )
    cohort_path, windows_path = configs
    out = tmp_path / "audit"

    result = run_audit(
        out,
        cohort_path=cohort_path,
        windows_path=windows_path,
        max_contracts=1,
        max_candle_contracts=1,
    )

    source = result["release_source"]
    assert source["kind"] == "network_get"
    assert source["explicitly_selected"] is False
    assert source["dataset"] is None
    assert source["archive"] is None
    assert source["network_release_requests_issued"] is True

    cpi = next(
        event for event in result["coverage"]["events"] if event["event_id"] == "cpi_2025_01"
    )
    assert cpi["release"] is None
    gate = next(
        gate for gate in cpi["coverage_gates"] if gate["gate"] == "release_payload_archived"
    )
    assert gate["satisfied"] is False
    assert "release_payload_archived" in cpi["unsatisfied_gates"]
    blocked_urls = [record["url"] for record in cpi["blocked"]]
    assert any("cpi_01152025" in str(url) for url in blocked_urls)
    assert result["access"]["blocked_count"] > 0


def _install_client(routes: dict[str, Any]) -> Any:
    """Replace only the network boundary, leaving the real archival path intact."""

    class FakeResponse:
        def __init__(self, status: int, body: Any, headers: dict[str, str]) -> None:
            self.status_code = status
            self.content = (
                json.dumps(body).encode("utf-8")
                if isinstance(body, (dict, list))
                else str(body).encode("utf-8")
            )
            self.headers = {key.lower(): value for key, value in headers.items()}

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get(self, url: str, headers: Any = None) -> FakeResponse:
            self.calls.append(url)
            for key, entry in routes.items():
                if key in url:
                    status, body, extra = (*entry, {})[:3]
                    return FakeResponse(status, body, extra)
            raise AssertionError(f"unexpected request in test: {url}")

        def close(self) -> None:
            pass

    client = FakeClient()
    real_init = HttpTransport.__init__

    def patched(self: HttpTransport, store: Any, **kwargs: Any) -> None:
        kwargs["client"] = client
        kwargs["sleep"] = lambda _seconds: None
        real_init(self, store, **kwargs)

    return patched


def test_the_reproduction_verifies_a_named_dataset_and_never_fits_it(
    tmp_path: pathlib.Path,
) -> None:
    """A named dataset is verified, cited in every report, and kept out of the fit."""
    from market_propagation import reporting

    dataset = _archive_fixture(tmp_path)
    out = tmp_path / "reproduce"

    record = reporting.reproduce(
        out,
        spec_path=REPO_ROOT / "configs" / "study_v1.yaml",
        n_events=4,
        repetitions=20,
        bootstrap_samples=2,
        release_dataset=dataset,
    )

    metrics = json.loads((out / reporting.METRICS_NAME).read_text())
    external = metrics["external_evidence"]
    # Nothing selects an audit on the caller's behalf: only what was named is cited.
    assert external["audit"] is None
    releases = external["releases"]
    assert releases["explicitly_selected"] is True
    assert releases["verified"] is True
    assert releases["records_verified"] == len(ARCHIVED)
    assert releases["records_blocked"] == 0
    assert releases["used_as_a_fitted_input"] is False
    assert releases["entered_synthetic_fit"] is False
    assert releases["certifies_historical_market_rule_versions"] is False
    assert releases["certifies_quote_coverage"] is False

    # The citation is real: path, hash and per-record figures all appear.
    assert releases["dataset"]["content_hash"] == hash_file(dataset)
    assert releases["dataset"]["row_count"] == len(ARCHIVED)
    assert any(path.endswith("releases.parquet") for path in external["paths"])
    assert {row["event_id"] for row in releases["records"]} == {
        record["event_id"] for record in ARCHIVED
    }

    # The empirical status stays blocked, and it says why the release values do not
    # change that: they certify neither rule versions nor quote coverage.
    assert metrics["empirical"]["status"] == "blocked"
    assert metrics["empirical"]["synthetic_results_are_empirical_results"] is False
    assert metrics["empirical"]["release_values_are_not_market_evidence"] is True
    assert metrics["empirical"]["referenced_release_dataset"] == releases["path"]
    assert metrics["gates"]["G0"]["status"] == "blocked"

    # The manifest carries the named input, its source, and its content hash.
    manifest = json.loads((out / reporting.MANIFEST_NAME).read_text())
    assert manifest["inputs"]["release_dataset"] == str(dataset)
    assert "reproduce(release_dataset=)" in manifest["inputs"]["release_dataset_source"]
    assert manifest["inputs"]["real_inputs_used_as_fitted_inputs"] is False
    assert manifest["hashes"]["release_dataset"] == hash_file(dataset)
    assert manifest["hashes"]["real_audit_coverage"] is None

    card = (out / reporting.DATA_CARD_NAME).read_text(encoding="utf-8")
    paper = (out / reporting.PAPER_NAME).read_text(encoding="utf-8")
    for text in (card, paper):
        assert "Real inputs cited" in text
        assert "archived release dataset" in text
        assert hash_file(dataset) in text
        assert "cpi_2025_01" in text and "empsit_2025_04" in text
        assert "not certify which market rule version" in text
    assert record["status"] in {"ok", "blocked"}


def test_omitting_the_real_inputs_cites_nothing_and_stays_synthetic_only(
    tmp_path: pathlib.Path,
) -> None:
    """With nothing named, no real input is cited even though one sits in the checkout."""
    from market_propagation import reporting

    out = tmp_path / "reproduce"

    record = reporting.reproduce(
        out,
        spec_path=REPO_ROOT / "configs" / "study_v1.yaml",
        n_events=4,
        repetitions=20,
        bootstrap_samples=2,
    )

    metrics = json.loads((out / reporting.METRICS_NAME).read_text())
    external = metrics["external_evidence"]
    assert external["audit"] is None
    assert external["releases"] is None
    assert external["paths"] == []
    assert "cites no real input at all" in external["note"]
    assert metrics["empirical"]["referenced_real_audit"] is None
    assert metrics["empirical"]["referenced_release_dataset"] is None
    assert metrics["empirical"]["status"] == "blocked"

    card = (out / reporting.DATA_CARD_NAME).read_text(encoding="utf-8")
    paper = (out / reporting.PAPER_NAME).read_text(encoding="utf-8")
    for text in (card, paper):
        assert "No real input was named for this run" in text
        # The audit that does exist under data/public is never silently picked up.
        assert "data/public/g0" not in text
    assert record["classification"] == "synthetic_software_methods_reproduction"


def test_naming_a_release_dataset_leaves_the_synthetic_sample_bit_identical(
    tmp_path: pathlib.Path,
) -> None:
    """Citing a real archive must not reach the fitted sample, and this proves it by bytes.

    The separation is behavioural rather than a declaration in a report: the same
    spec, sample settings and seeds are run twice and the sealed panels are compared
    by content hash. A regression that let a named release dataset seed, subset or
    perturb the synthetic sample would move one of these hashes while every prose
    claim stayed as it is. The named run must also still record that its release
    values never entered the fit, and the unnamed run must cite no dataset at all.
    """
    from market_propagation import reporting

    dataset = _archive_fixture(tmp_path)
    named_out = tmp_path / "named"
    synthetic_out = tmp_path / "synthetic"
    settings = {
        "spec_path": REPO_ROOT / "configs" / "study_v1.yaml",
        "n_events": 4,
        "repetitions": 20,
        "bootstrap_samples": 2,
    }

    reporting.reproduce(named_out, release_dataset=dataset, **settings)
    reporting.reproduce(synthetic_out, **settings)

    named = json.loads((named_out / reporting.METRICS_NAME).read_text())
    synthetic = json.loads((synthetic_out / reporting.METRICS_NAME).read_text())

    # The packaged synthetic fixture is the same bytes whether or not a real archive
    # was cited, and so is each sealed sample panel derived from it.
    assert named["sample"]["fixture_hash"] == synthetic["sample"]["fixture_hash"]
    for fold in ("source", "usable"):
        named_panel = named["coverage"][fold]["content_hash"]
        synthetic_panel = synthetic["coverage"][fold]["content_hash"]
        assert named_panel == synthetic_panel, (
            f"the {fold} panel moved when a real archive was named"
        )

    # The named run cites the dataset and states that it was never fitted.
    releases = named["external_evidence"]["releases"]
    assert releases["entered_synthetic_fit"] is False
    assert releases["used_as_a_fitted_input"] is False
    assert releases["dataset"]["content_hash"] == hash_file(dataset)

    # The unnamed run cites nothing, so there is no path by which a real release
    # could reach the sample without being named first.
    omitted = synthetic["external_evidence"]
    assert omitted["releases"] is None
    assert omitted["paths"] == []
    synthetic_manifest = json.loads((synthetic_out / reporting.MANIFEST_NAME).read_text())
    assert synthetic_manifest["inputs"]["release_dataset"] is None
    assert "not supplied" in synthetic_manifest["inputs"]["release_dataset_source"]


def test_a_named_audit_that_cannot_verify_is_cited_as_unverified(
    tmp_path: pathlib.Path,
) -> None:
    """A named input that does not verify counts for nothing and says so."""
    from market_propagation import reporting

    dataset = _archive_fixture(tmp_path)
    broken = dataset.parent / "breaks.parquet"
    broken.write_bytes(dataset.read_bytes())

    record = reporting.reproduce(
        tmp_path / "reproduce",
        spec_path=REPO_ROOT / "configs" / "study_v1.yaml",
        n_events=4,
        repetitions=20,
        bootstrap_samples=2,
        release_dataset=broken,
    )

    metrics = json.loads((tmp_path / "reproduce" / reporting.METRICS_NAME).read_text())
    releases = metrics["external_evidence"]["releases"]
    assert releases["verified"] is False
    assert releases["reason"] is not None
    assert releases["records_verified"] == 0
    assert releases["records"] == []
    assert releases["all_records_verified"] is False
    assert metrics["empirical"]["referenced_release_dataset"] is None
    assert metrics["empirical"]["status"] == "blocked"
    assert record["status"] in {"ok", "blocked"}


def test_the_audit_cli_threads_the_release_dataset_flag(
    tmp_path: pathlib.Path,
    configs: tuple[pathlib.Path, pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``audit --release-dataset`` reads the real originals and never fetches one."""
    from market_propagation import cli

    attempted = _forbid_network(monkeypatch)
    dataset = _archive_fixture(tmp_path)
    cohort_path, windows_path = configs
    out = tmp_path / "cli-audit"

    code = cli.main(
        [
            "audit",
            "--output",
            str(out),
            "--cohort",
            str(cohort_path),
            "--windows",
            str(windows_path),
            "--timeout",
            "0.1",
            "--max-pages",
            "1",
            "--max-contracts",
            "1",
            "--max-candle-contracts",
            "1",
            "--release-dataset",
            str(dataset),
        ]
    )

    # Exit 2 is the blocked result this audit earns: releases loaded, coverage gates
    # unsatisfied. It is not an exception.
    assert code == cli.EXIT_BLOCKED
    assert not [url for url in attempted if "news.release" in url]
    report = json.loads((out / "release_source.json").read_text())
    assert report["kind"] == ACQUISITION_SEALED_DATASET
    assert report["archive"]["records_loaded"] == len(ARCHIVED)
    assert report["network_release_requests_issued"] is False
    coverage = json.loads((out / "coverage.json").read_text())
    loaded = [event for event in coverage["events"] if event.get("release")]
    assert len(loaded) == len(ARCHIVED)
    err = capsys.readouterr().err
    assert "sealed dataset" in err
    assert "not certify the market rule versions" in err


def test_the_reproduce_cli_threads_real_audit_and_release_dataset_flags(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``reproduce --real-audit --release-dataset`` cites both and fits neither."""
    from market_propagation import cli, reporting

    dataset = _archive_fixture(tmp_path)
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()
    (audit_dir / "coverage.json").write_text(
        json.dumps({"status": "partial", "complete": False, "events": []}), encoding="utf-8"
    )
    out = tmp_path / "cli-reproduce"

    code = cli.main(
        [
            "reproduce",
            "--output",
            str(out),
            "--spec",
            str(REPO_ROOT / "configs" / "study_v1.yaml"),
            "--events",
            "4",
            "--repetitions",
            "20",
            "--bootstrap",
            "2",
            "--real-audit",
            str(audit_dir),
            "--release-dataset",
            str(dataset),
        ]
    )

    assert code == cli.EXIT_OK
    capsys.readouterr()
    metrics = json.loads((out / reporting.METRICS_NAME).read_text())
    external = metrics["external_evidence"]
    assert external["audit"]["status"] == "partial"
    assert external["releases"]["verified"] is True
    assert external["releases"]["records_verified"] == len(ARCHIVED)
    assert external["releases"]["entered_synthetic_fit"] is False
    assert metrics["empirical"]["status"] == "blocked"
    assert metrics["gates"]["G0"]["status"] == "blocked"
    # The named inputs appear; the unrelated audit in the checkout does not.
    assert str(dataset) in external["paths"] or "releases.parquet" in " ".join(external["paths"])
    assert "data/public/g0/" not in " ".join(external["paths"])


def test_a_named_audit_directory_that_is_absent_raises_rather_than_falling_back(
    tmp_path: pathlib.Path,
) -> None:
    """A named path must exist; nothing is discovered in its place."""
    from market_propagation import reporting

    with pytest.raises(FileNotFoundError, match="real audit directory not found"):
        reporting.reproduce(
            tmp_path / "reproduce",
            spec_path=REPO_ROOT / "configs" / "study_v1.yaml",
            n_events=4,
            repetitions=20,
            bootstrap_samples=2,
            real_audit_dir=tmp_path / "no-such-audit",
        )


def test_a_named_audit_is_cited_at_the_status_it_recorded(
    tmp_path: pathlib.Path,
) -> None:
    """A named real audit is linked at its own status and never upgraded by the run."""
    from market_propagation import reporting

    audit_dir = tmp_path / "policy-audit"
    audit_dir.mkdir(parents=True)
    coverage = {
        "status": "partial",
        "complete": False,
        "gate": "G0",
        "events": [],
        "unsatisfied_gates": [],
    }
    (audit_dir / "coverage.json").write_text(json.dumps(coverage), encoding="utf-8")
    (audit_dir / "event_card.json").write_text(json.dumps({"status": "created"}), encoding="utf-8")

    reporting.reproduce(
        tmp_path / "reproduce",
        spec_path=REPO_ROOT / "configs" / "study_v1.yaml",
        n_events=4,
        repetitions=20,
        bootstrap_samples=2,
        real_audit_dir=audit_dir,
    )

    metrics = json.loads((tmp_path / "reproduce" / reporting.METRICS_NAME).read_text())
    audit = metrics["external_evidence"]["audit"]
    assert audit["explicitly_selected"] is True
    assert audit["status"] == "partial"
    assert audit["complete"] is False
    assert audit["eligible_cohort_established"] is False
    assert audit["sha256"] == hash_file(audit_dir / "coverage.json")
    assert audit["recorded_fields"]["complete"] is False
    # A partial audit leaves G0 blocked; it is cited, not promoted.
    assert metrics["gates"]["G0"]["status"] == "blocked"
    assert metrics["empirical"]["referenced_real_audit"] == audit["path"]


def test_the_release_dataset_archive_url_matches_the_documented_convention() -> None:
    """The archive URL the source builds is the one the payloads were captured under."""
    assert archive_url("cpi", dt.date(2025, 1, 15)) == CPI_2025_01_SOURCE
    assert archive_url("empsit", dt.date(2025, 4, 4)) == EMPSIT_2025_04_SOURCE


def test_the_sealed_dataset_is_read_through_the_verifying_reader(
    tmp_path: pathlib.Path,
) -> None:
    """A dataset whose declared hash no longer matches its bytes is refused outright."""
    dataset = _archive_fixture(tmp_path)
    manifest_path = dataset.with_name(dataset.name + ".manifest.json")
    manifest = json.loads(manifest_path.read_text())
    manifest["content_hash"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="does not match its manifest"):
        ArchivedReleaseSource(dataset)


def test_a_dataset_of_the_wrong_table_is_refused(tmp_path: pathlib.Path) -> None:
    """A sealed table that is not the releases table is not read as one."""
    other = tmp_path / "quotes.parquet"
    write_parquet(
        [
            {
                "venue": "kalshi",
                "contract_id": "KXCPI-25JAN-T0.3",
                "bid": Decimal("0.40"),
                "ask": Decimal("0.42"),
                "bid_size": Decimal("100"),
                "ask_size": Decimal("100"),
                "validity": "valid",
                "replay_order": 0,
                "source_time": dt.datetime(2025, 1, 15, tzinfo=dt.UTC),
                "received_time": dt.datetime(2025, 1, 15, tzinfo=dt.UTC),
                "availability_quality": "unknown",
                "availability_basis": "probe",
                "raw_hash": "1" * 64,
                "record_id": "x",
                "source": "kalshi.public",
                "schema_version": "1",
            }
        ],
        other,
        table="quotes",
    )

    # ``read_parquet`` and this source agree that a malformed sealed dataset is a
    # ValueError, so the CLI reports it as an error rather than crashing.
    with pytest.raises(ValueError, match="declares table 'quotes'"):
        ArchivedReleaseSource(other)


def test_a_dataset_with_no_sibling_raw_store_is_refused(tmp_path: pathlib.Path) -> None:
    """Without the original payloads the archive cannot be verified, so it is refused."""
    dataset = _archive_fixture(tmp_path)
    import shutil

    shutil.rmtree(dataset.parent / "raw")

    with pytest.raises(FileNotFoundError, match="has no sibling raw store"):
        ArchivedReleaseSource(dataset)


def test_the_client_opens_the_archive_once_at_construction(
    tmp_path: pathlib.Path,
) -> None:
    """A malformed archive fails at construction, not as one blocked event per release."""
    from market_propagation.ingest.macro_releases import MacroReleaseClient

    dataset = _archive_fixture(tmp_path)
    store = RawStore(tmp_path / "dest-raw")
    client = MacroReleaseClient(store, release_dataset=dataset)
    try:
        assert client.archive is not None
        assert client.archive.dataset["content_hash"] == hash_file(dataset)
    finally:
        client.close()

    missing = tmp_path / "absent.parquet"
    with pytest.raises(FileNotFoundError, match="archived release dataset not found"):
        MacroReleaseClient(store, release_dataset=missing)
