import datetime as dt
import importlib.util
import json
import re
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from market_propagation.ingest.expectations import ReleaseFacts
from market_propagation.storage import RawStore


@pytest.fixture
def replay_module(monkeypatch):
    scripts = Path(__file__).parents[1] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location(
        "archived_replay", scripts / "replay_archived_expectations.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source(
    tmp_path,
    *,
    timestamp="20250403234746",
    publication="2025-04-03T22:02:44+00:00",
    family="employment",
):
    root = tmp_path / "source"
    store = RawStore(root / "raw")
    url = "https://example.org/forecast"
    quote = "Dow Jones expects March nonfarm payrolls to rise by 140,000 jobs."
    if family == "cpi":
        quote = "Dow Jones expects March CPI to rise by 0.2% month over month."
    body = (
        f'<meta property="article:published_time" content="{publication}">'
        f'<script>__wm.wombat({json.dumps(url)},"{timestamp}");</script>'
        f"<article>{quote}</article>"
    ).encode()
    observed = dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
    captured = store.put(body, source="archive", received_time=observed)
    index = store.put(
        (
            "Markdown Content:\n"
            + json.dumps(
                {
                    "url": url,
                    "archived_snapshots": {
                        "closest": {
                            "available": True,
                            "status": "200",
                            "timestamp": timestamp,
                            "url": f"http://web.archive.org/web/{timestamp}/{url}",
                        }
                    },
                }
            )
        ).encode(),
        source="index",
        received_time=observed,
    )
    statistic = "payrolls_change_thousands" if family == "employment" else "cpi_headline_sa_mom_pct"
    fact = ReleaseFacts(
        "event",
        family,
        dt.datetime(2025, 4, 4, 12, 30, tzinfo=dt.UTC),
        "2025-03",
        {statistic: Decimal("228") if family == "employment" else Decimal("-0.1")},
        (),
        "f" * 64,
    )
    value = "140,000" if family == "employment" else "0.2"
    candidate = {
        "event_id": "event",
        "reference_period": "2025-03",
        "source_root": "source",
        "original_url": url,
        "raw_hash": captured.raw_hash,
        "index_raw_hash": index.raw_hash,
        "quote": quote,
        "value_pattern": re.escape(quote).replace(re.escape(value), "(?P<value>[0-9,.]+)"),
        "consensus_token": "Dow Jones",
        "consensus_id": "Dow Jones economist poll",
        "statistic_mapping_basis": "explicit monthly payroll change",
    }
    return candidate, fact, store


def test_payroll_replay_converts_jobs_and_preserves_source_and_archive_times(
    tmp_path, replay_module
):
    candidate, fact, _ = source(tmp_path)
    result = replay_module.archived_record(candidate, fact, tmp_path)["record"]
    assert result["value"] == "140"
    assert result["published_at"] == "2025-04-03T22:02:44+00:00"
    assert result["archive_observed_at"] == "2025-04-03T23:47:46+00:00"
    assert result["publication_or_latency_measurement_eligible"] is False


def test_unzoned_publication_uses_explicit_conservative_bound(tmp_path, replay_module):
    candidate, fact, _ = source(tmp_path, publication="2025-04-03 10:00:00")
    result = replay_module.archived_record(candidate, fact, tmp_path)["record"]
    assert result["original_publication_instant"] is None
    assert result["published_at"] == result["archive_observed_at"]
    assert result["timing_basis"] == "archive_capture_upper_bound"


def test_post_release_archive_is_refused_despite_earlier_publication(tmp_path, replay_module):
    candidate, fact, _ = source(tmp_path, timestamp="20250404123000")
    with pytest.raises(ValueError, match="strictly before"):
        replay_module.archived_record(candidate, fact, tmp_path)


@pytest.mark.parametrize("key", ["raw_hash", "index_raw_hash"])
def test_both_source_and_index_corruption_are_refused(tmp_path, replay_module, key):
    candidate, fact, store = source(tmp_path)
    digest = candidate[key]
    (store.root / "blobs" / digest[:2] / f"{digest}.bin").write_bytes(b"altered")
    with pytest.raises(ValueError, match="corrupt"):
        replay_module.archived_record(candidate, fact, tmp_path)


def test_changed_quotation_and_reference_period_are_refused(tmp_path, replay_module):
    candidate, fact, _ = source(tmp_path)
    with pytest.raises(ValueError, match="quotation is absent"):
        replay_module.archived_record({**candidate, "quote": "150,000 jobs"}, fact, tmp_path)
    with pytest.raises(ValueError, match="reference_period_does_not_match"):
        replay_module.archived_record({**candidate, "reference_period": "2025-02"}, fact, tmp_path)


def test_empty_archive_and_mismatched_wrapper_are_refused(tmp_path, replay_module):
    candidate, fact, store = source(tmp_path)
    body = (
        f'<script>__wm.wombat({json.dumps(candidate["original_url"])},"20250403234746");</script>'
    )
    raw = store.put(
        body.encode(), source="test", received_time=dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
    )
    with pytest.raises(ValueError, match="quotation is absent"):
        replay_module.archived_record({**candidate, "raw_hash": raw.raw_hash}, fact, tmp_path)
    other = body.replace("20250403234746", "20250405234746")
    raw = store.put(
        other.encode(), source="test", received_time=dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
    )
    with pytest.raises(ValueError, match="wrapper does not match"):
        replay_module.archived_record({**candidate, "raw_hash": raw.raw_hash}, fact, tmp_path)


def test_monthly_cpi_without_seasonal_definition_is_not_admitted(tmp_path, replay_module):
    candidate, fact, _ = source(tmp_path, family="cpi")
    result = replay_module.archived_record(candidate, fact, tmp_path)
    assert "record" not in result
    assert result["parsed_forecast"]["value"] == "0.2"
    assert result["parsed_forecast"]["statistic"] is None
    assert result["parsed_forecast"]["seasonal_adjustment"] == "unknown"
    assert result["reason"] == "forecast_seasonal_adjustment_unverified"
    with pytest.raises(ValueError, match="reference_period_does_not_match"):
        replay_module.archived_record({**candidate, "reference_period": "1999-01"}, fact, tmp_path)
    with pytest.raises(ValueError, match="consensus_identity_is_not_named"):
        replay_module.archived_record({**candidate, "consensus_id": ""}, fact, tmp_path)


def test_replay_preserves_missing_release_and_uses_first_print(
    tmp_path, replay_module, monkeypatch
):
    candidate, fact, _ = source(tmp_path)
    releases = tmp_path / "releases.parquet"
    releases.write_bytes(b"test release table")
    raw = RawStore(tmp_path / "raw").put(
        b"original release", source="test", received_time=dt.datetime(2025, 4, 4, tzinfo=dt.UTC)
    )
    fact = replace(fact, raw_hash=raw.raw_hash)
    monkeypatch.setattr(
        replay_module,
        "load_release_facts",
        lambda _: {"event": fact, "missing": replace(fact, event_id="missing")},
    )
    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps([candidate]))
    report = replay_module.replay(registry, releases, tmp_path, tmp_path / "report")
    assert report["declared_releases"] == 2
    assert report["validated_expectations"] == 1
    assert report["outcomes"][0]["surprise"] == "88"
    assert report["outcomes"][1] == {
        "event_id": "missing",
        "reference_period": "2025-03",
        "state": "uncovered",
        "reason": "no_selected_archived_forecast",
    }
    assert report["all_release_consensus_complete"] is False
    for pattern in ("(", re.escape(candidate["quote"]), "(?P<value>.*)"):
        registry.write_text(json.dumps([{**candidate, "value_pattern": pattern}]))
        refused = replay_module.replay(registry, releases, tmp_path, tmp_path / "refused")
        assert refused["validated_expectations"] == 0
        assert len(refused["outcomes"]) == 2
        assert refused["outcomes"][0]["state"] == "refused"
        assert "invalid forecast value extraction" in refused["outcomes"][0]["reason"]
    registry.write_text(json.dumps([candidate, candidate]))
    with pytest.raises(ValueError, match="duplicate"):
        replay_module.replay(registry, releases, tmp_path, tmp_path / "duplicate")
    assert not (tmp_path / "duplicate").exists()


def test_statistic_definition_must_match_release_url_and_seasonal_basis(
    tmp_path, replay_module, monkeypatch
):
    candidate, fact, store = source(tmp_path, family="cpi")
    quote = "The seasonally adjusted monthly CPI was compared with the Dow Jones forecast."
    url = "https://example.org/result"
    raw = store.put(
        quote.encode(),
        source="definition",
        received_time=dt.datetime(2026, 9, 17, tzinfo=dt.UTC),
        metadata={"request_url": url, "http_status": 200},
    )
    definition = {
        "source_root": "source",
        "event_id": "event",
        "reference_period": "2025-03",
        "raw_hash": raw.raw_hash,
        "quote": quote,
        "url": url,
    }
    monkeypatch.setitem(
        replay_module.REVIEWED_CPI_DEFINITIONS, "event", ("2025-03", "Dow Jones", url, raw.raw_hash)
    )
    candidate["seasonal_adjustment_evidence"] = definition
    assert replay_module.archived_record(candidate, fact, tmp_path)["record"]["value"] == "0.2"
    definition["event_id"] = "different_event"
    with pytest.raises(ValueError, match="different release"):
        replay_module.archived_record(candidate, fact, tmp_path)
    definition["event_id"] = "event"
    definition["url"] = "https://example.org/other"
    with pytest.raises(ValueError, match="reviewed event/poll/source"):
        replay_module.archived_record(candidate, fact, tmp_path)
    definition["url"] = url
    definition["quote"] = "Dow Jones monthly CPI is not seasonally adjusted."
    raw = store.put(
        definition["quote"].encode(),
        source="definition",
        received_time=dt.datetime(2026, 9, 17, tzinfo=dt.UTC),
        metadata={"request_url": url, "http_status": 200},
    )
    definition["raw_hash"] = raw.raw_hash
    with pytest.raises(ValueError, match="reviewed event/poll/source"):
        replay_module.archived_record(candidate, fact, tmp_path)
    monkeypatch.setitem(
        replay_module.REVIEWED_CPI_DEFINITIONS, "event", ("2025-03", "Dow Jones", url, raw.raw_hash)
    )
    with pytest.raises(ValueError, match="does not bind seasonal adjustment"):
        replay_module.archived_record(candidate, fact, tmp_path)
