"""The refusals in `scripts/capture_bls_releases.py`.

The script's whole job is to refuse: it writes a receipt only for a page that is a
complete published release whose own embargo line agrees with the cohort calendar.
Every test here is a way that could go wrong and would be invisible afterwards — a
fabricated receipt for a page that never arrived, a receipt written into the sealed
2025 source directory so the next import rewrites the sealed dataset, a capture
overwritten so an already-imported row cites bytes that are no longer there.

The one thing a test here cannot check is that the page really is a release: that
needs a real captured page, and it was verified against the sealed dataset itself —
a fresh capture of `cpi_2025_01` parsed to the same six values and the same
`raw_hash` as the row already sealed for it.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
from typing import Any

import pytest
import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location(
        "capture_bls_releases", REPO_ROOT / "scripts" / "capture_bls_releases.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CAPTURE = _load_script()

EVENT = {
    "event_id": "cpi_2026_10",
    "family": "cpi",
    "reference_period": "2026-09",
    "scheduled_at_utc": "2026-10-14T12:30:00+00:00",
    "initial_release_url": "https://www.bls.gov/news.release/archives/cpi_10142026.htm",
}


def cohort_file(tmp_path: pathlib.Path, events: list[dict[str, Any]] | None = None) -> pathlib.Path:
    path = tmp_path / "cohort.yaml"
    path.write_text(
        yaml.safe_dump({"events": [EVENT] if events is None else events}, sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_the_sealed_capture_directory_is_refused(tmp_path: pathlib.Path) -> None:
    """A receipt written there would make the next import rewrite the sealed dataset."""
    with pytest.raises(ValueError, match="sealed 2025 capture directory"):
        CAPTURE.capture_directory(CAPTURE.SEALED_CAPTURE_ROOT)
    with pytest.raises(ValueError, match="sealed 2025 capture directory"):
        CAPTURE.capture_directory(CAPTURE.SEALED_CAPTURE_ROOT / "prospective")


def test_an_event_with_no_captured_page_is_refused_not_invented(tmp_path: pathlib.Path) -> None:
    outcome = CAPTURE.describe_event(EVENT, directory=tmp_path)
    assert outcome["written"] is False
    assert outcome["reason"] == "no_captured_page"
    assert not (tmp_path / "cpi_2026_10.json").exists()


def test_a_body_that_is_not_a_complete_document_is_refused(tmp_path: pathlib.Path) -> None:
    (tmp_path / "cpi_2026_10.html").write_bytes(b"<html><body>error</body>")
    outcome = CAPTURE.describe_event(EVENT, directory=tmp_path)
    assert outcome["written"] is False
    assert outcome["reason"] == "body_too_small_to_be_a_release_page"
    assert not (tmp_path / "cpi_2026_10.json").exists()


def test_a_large_body_that_is_not_a_release_is_refused(tmp_path: pathlib.Path) -> None:
    """Size and a closing tag are not evidence; the parser decides, and it must say no.

    The parser is tolerant of a body it cannot read, so junk fails the *values*
    guard rather than raising. Both are refusals and neither writes a receipt, which
    is the invariant; the reason is asserted so the refusal stays legible.
    """
    (tmp_path / "cpi_2026_10.html").write_bytes(b"x" * 60_000 + b"</html>")
    outcome = CAPTURE.describe_event(EVENT, directory=tmp_path)
    assert outcome["written"] is False
    assert outcome["reason"] == "payload_states_no_release_values"
    assert not (tmp_path / "cpi_2026_10.json").exists()


def test_an_existing_receipt_is_never_written_over(tmp_path: pathlib.Path) -> None:
    (tmp_path / "cpi_2026_10.json").write_text(
        json.dumps({"event_id": "cpi_2026_10"}), encoding="utf-8"
    )
    with pytest.raises(FileExistsError, match="already exists"):
        CAPTURE.describe_event(EVENT, directory=tmp_path)


def test_an_unknown_family_has_no_slug_and_is_refused() -> None:
    with pytest.raises(ValueError, match="no declared BLS release slug"):
        CAPTURE._family_slug("ppi")


def test_an_undeclared_event_id_is_refused(tmp_path: pathlib.Path) -> None:
    path = cohort_file(tmp_path)
    with pytest.raises(ValueError, match="declares no event named"):
        CAPTURE.load_events(path, ("cpi_2099_01",))


def test_an_event_without_an_archive_url_is_refused(tmp_path: pathlib.Path) -> None:
    """The URL is a declaration; a constructed one would point somewhere unrecorded."""
    without_url = {key: value for key, value in EVENT.items() if key != "initial_release_url"}
    path = cohort_file(tmp_path, [without_url])
    with pytest.raises(ValueError, match="no `initial_release_url`"):
        CAPTURE.load_events(path, ())
