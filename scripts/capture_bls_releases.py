"""Write verifiable receipts for browser-captured BLS release pages.

`import_bls_archives.py` refuses anything it cannot verify, and it can only verify a
*complete published page*: it raises unless the receipt says ``status: 200`` and
``payload_complete: true``, unless the body ends with ``</html>``, and unless the
page's own embargo line agrees with the cohort calendar. That strictness has one
consequence this script exists to handle: **a release row cannot be created before
its release publishes**, so the forward cohort is built by running a capture after
each scheduled release rather than by declaring every event in advance.

**The page has to come from a browser, and that is not a convenience.** Measured
from this checkout on 2026-09-17, a direct HTTP GET of an archive page returns
``403`` on every header set tried — the repository's research user agent, a current
browser user agent, with and without ``Accept``/``Accept-Language``, and with no
user agent at all. The same URL through a browser session returns ``200`` with the
full page. The sealed 2025 captures record exactly this, which is why they carry
``acquisition_method: standard_browser_http_response`` and live in a directory named
``bls-browser``. There is deliberately no direct-fetch mode here: one that returned
403 would be a mode that cannot work, and offering it would invite a reader to
believe the source refused the request rather than the client being unable to make
it.

So the operator's part is to obtain the page; this script's part is to refuse
anything that is not a release:

1. read ``<directory>/<event_id>.html``, the body a browser session received;
2. require it to end with ``</html>`` and to be substantial;
3. parse it with the SAME parser the importer uses
   (:func:`market_propagation.ingest.macro_releases.parse_release_payload`) and
   require that it yields first-release values and that its own embargo line agrees
   with the cohort calendar;
4. only then write ``<directory>/<event_id>.json``, the receipt the importer reads.

Steps 3 and 4 are the point. A receipt written here is one the importer accepts,
because the same check was already applied, and the recorded ``status`` is a
statement about a page that was read rather than about a status line this script
never saw. ``status_basis`` and ``payload_evidence`` record what was actually
observed, so a later reader can tell this receipt from one written beside a real
HTTP response.

Run::

    uv run --no-sync python scripts/capture_bls_releases.py \\
      --cohort configs/cohort.yaml --directory data/public/bls-browser-prospective \\
      --event empsit_2025_01
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
from typing import Any

import yaml

from market_propagation.ingest.macro_releases import parse_release_payload

EXIT_OK = 0
EXIT_BLOCKED = 2

#: Capture directory of the sealed 2025 arm. Writing receipts there would pollute the
#: source the sealed dataset was built from, so the path is refused by name: the next
#: import over that directory would rewrite the sealed 2025 dataset.
SEALED_CAPTURE_ROOT = pathlib.Path("data/public/bls-browser")

#: A release page is around a megabyte. A shell, an error page or a redirect stub is
#: orders of magnitude smaller, so a floor separates "a page arrived" from "something
#: arrived" without pretending to validate content, which step 3 does instead.
MINIMUM_BODY_BYTES = 50_000


def load_events(cohort_path: pathlib.Path, wanted: tuple[str, ...]) -> list[dict[str, Any]]:
    """Every selected event, in declaration order.

    The archive URL is read from the declaration rather than built from the date: the
    declaration is where a human recorded it, and a URL this script constructed would
    silently point somewhere the calendar does not.
    """
    document = yaml.safe_load(cohort_path.read_text(encoding="utf-8"))
    events = document.get("events")
    if not isinstance(events, list) or not events:
        raise ValueError(
            f"{cohort_path} declares no events; a receipt run with no event has nothing to "
            "describe and is refused rather than reported as a complete empty run"
        )
    selected = [event for event in events if not wanted or str(event["event_id"]) in wanted]
    missing = sorted(set(wanted) - {str(event["event_id"]) for event in selected})
    if missing:
        raise ValueError(f"{cohort_path} declares no event named {missing}")
    unaddressed = [
        str(event["event_id"])
        for event in selected
        if not str(event.get("initial_release_url") or "")
    ]
    if unaddressed:
        raise ValueError(
            f"events {unaddressed} declare no `initial_release_url`, so there is no page to "
            "describe; the archive URL is a declaration, not something this script derives"
        )
    return selected


def capture_directory(directory: pathlib.Path) -> pathlib.Path:
    """``directory``, refusing the sealed arm's capture directory and anything inside it."""
    resolved = directory.resolve()
    sealed = SEALED_CAPTURE_ROOT.resolve()
    if resolved == sealed or sealed in resolved.parents:
        raise ValueError(
            f"{directory} is the sealed 2025 capture directory or inside it. A receipt written "
            "here would be picked up by the next import over that directory and would replace "
            "the sealed 2025 dataset with one this arm's captures contributed to"
        )
    return resolved


def _family_slug(family: str) -> str:
    slug = "cpi" if str(family) == "cpi" else "empsit"
    if str(family) not in {"cpi", "employment"}:
        raise ValueError(
            f"family {family!r} has no declared BLS release slug; this script covers the two "
            "families the cohort declares and refuses to guess a slug for another"
        )
    return slug


def describe_event(event: dict[str, Any], *, directory: pathlib.Path) -> dict[str, Any]:
    """One receipt for one captured page, or a refusal naming what was wrong."""
    event_id = str(event["event_id"])
    body_path = directory / f"{event_id}.html"
    receipt_path = directory / f"{event_id}.json"
    if receipt_path.exists():
        raise FileExistsError(
            f"{receipt_path} already exists. A page captured again after the venue revised it "
            "is a second occurrence rather than a correction, so the first receipt is kept and "
            "a new capture belongs in a different directory"
        )
    if not body_path.exists():
        return {
            "event_id": event_id,
            "written": False,
            "reason": "no_captured_page",
            "detail": f"{body_path} does not exist; a browser session has to save the page first",
        }
    body = body_path.read_bytes()
    if len(body) < MINIMUM_BODY_BYTES:
        return {
            "event_id": event_id,
            "written": False,
            "reason": "body_too_small_to_be_a_release_page",
            "detail": f"{body_path} is {len(body)} bytes, under the {MINIMUM_BODY_BYTES} floor",
        }
    if body.rstrip()[-7:].lower() != b"</html>":
        return {
            "event_id": event_id,
            "written": False,
            "reason": "body_is_not_a_complete_html_document",
            "detail": f"{body_path} does not end with </html>, so the page did not arrive whole",
        }
    source_url = str(event["initial_release_url"])
    scheduled = dt.datetime.fromisoformat(str(event["scheduled_at_utc"]))
    try:
        _title, period, embargo, values, revisions, _statements, usdl, agreement = (
            parse_release_payload(
                body.decode("utf-8"),
                family_slug=_family_slug(str(event["family"])),
                source_url=source_url,
                scheduled_at=scheduled,
            )
        )
    except Exception as error:  # any parse failure is a refusal, named below
        return {
            "event_id": event_id,
            "written": False,
            "reason": "payload_did_not_parse_as_a_release",
            "detail": f"{type(error).__name__}: {error}",
        }
    if not values:
        return {
            "event_id": event_id,
            "written": False,
            "reason": "payload_states_no_release_values",
            "detail": f"{body_path} parsed to no values for period {period!r}",
        }
    if agreement != "agrees_with_calendar":
        return {
            "event_id": event_id,
            "written": False,
            "reason": "payload_embargo_disagrees_with_the_declared_calendar",
            "detail": (
                f"the page's own embargo line is {embargo.isoformat() if embargo else None} and "
                f"the declaration schedules {scheduled.isoformat()}: {agreement}"
            ),
        }
    received = dt.datetime.fromtimestamp(body_path.stat().st_mtime, tz=dt.UTC)
    receipt = {
        "event_id": event_id,
        "family": str(event["family"]),
        "reference_period": str(event["reference_period"]),
        "source_url": source_url,
        "status": 200,
        # What the 200 above is a statement about: a complete release page was read,
        # not a status line this process observed.
        "status_basis": "complete_published_page_read_through_a_browser_session",
        "payload_complete": True,
        "acquisition_method": "standard_browser_http_response",
        "source_availability": "unknown_historical",
        "received_time": received.isoformat(),
        "received_time_basis": "local_file_mtime_of_the_saved_page",
        "scheduled_at": scheduled.isoformat(),
        "payload_evidence": {
            "stored_period": period,
            "embargo_time_from_payload": embargo.isoformat() if embargo else None,
            "calendar_agreement": agreement,
            "usdl": usdl,
            "value_keys": sorted(values),
            "revision_keys": sorted(revisions),
            "body_bytes": len(body),
        },
        "credentials": "none",
    }
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "event_id": event_id,
        "written": True,
        "received_time": receipt["received_time"],
        "source_url": source_url,
        "body_bytes": len(body),
        "stored_period": period,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", required=True, help="release declaration file to read")
    parser.add_argument(
        "--directory",
        required=True,
        help="directory holding the captured <event_id>.html pages; receipts are written here",
    )
    parser.add_argument(
        "--event",
        action="append",
        default=None,
        help="event id to describe; repeatable (default: every event the file declares)",
    )
    args = parser.parse_args()

    directory = capture_directory(pathlib.Path(args.directory))
    if not directory.is_dir():
        print(
            json.dumps(
                {"error": f"{directory} is not a directory; nothing was captured there"},
                indent=2,
                sort_keys=True,
            )
        )
        return 1
    try:
        events = load_events(pathlib.Path(args.cohort), tuple(args.event or ()))
    except (OSError, ValueError) as error:
        print(json.dumps({"error": str(error)}, indent=2, sort_keys=True))
        return 1

    written: list[dict[str, Any]] = []
    refused: list[dict[str, Any]] = []
    for event in events:
        try:
            outcome = describe_event(event, directory=directory)
        except FileExistsError as error:
            print(json.dumps({"error": str(error)}, indent=2, sort_keys=True))
            return 1
        (written if outcome["written"] else refused).append(outcome)

    document = {
        "produced_by": "capture_bls_releases",
        "cohort": str(args.cohort),
        "capture_root": str(directory),
        "events_requested": len(events),
        "receipts_written": len(written),
        "events_refused": len(refused),
        "written": written,
        "refused": refused,
    }
    print(json.dumps(document, indent=2, sort_keys=True))
    for outcome in refused:
        print(f"refused: {outcome['event_id']} ({outcome['reason']}) {outcome['detail']}")
    print(
        f"{len(written)} receipt(s) written under {directory}; {len(refused)} refused. "
        "Import only a directory whose pages all produced receipts."
    )
    return EXIT_OK if written and not refused else EXIT_BLOCKED


if __name__ == "__main__":
    raise SystemExit(main())
