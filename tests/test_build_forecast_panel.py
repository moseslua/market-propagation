"""The rule-vintage records the exposure graph's own loader reads.

``rule_records`` is the single place the capture pipeline's output meets the graph, and
the two disagree about the open interval unless this file holds them together. The
pipeline writes ``"in_force_to": null`` for a rule version no later observation has
superseded, and the graph's declared requirement names ``in_force_to`` as a required
field. Reading "required" as "non-null" dropped every such record, so a capture of a
rule text that never changed — which is the ordinary case — contributed nothing to the
graph even after the record existed.

The document shape under test is the one ``market-propagation attest-rules
--emit-graph-records`` writes, and the field names are read from
``configs/neighbor_graph_v2.yaml`` rather than restated here, so the producer and the
requirement are checked against each other rather than against a copy.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
from typing import Any

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

GRAPH_CONFIG = REPO_ROOT / "configs" / "neighbor_graph_v2.yaml"


def _load_builder() -> Any:
    """The script itself, imported without running its ``main``."""
    spec = importlib.util.spec_from_file_location(
        "build_forecast_panel", REPO_ROOT / "scripts" / "build_forecast_panel.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PANEL = _load_builder()

CONTRACT = "KXFED-26DEC-T2.75"


def record(**overrides: Any) -> dict[str, Any]:
    """One complete record, shaped as ``attest-rules`` emits it."""
    entry: dict[str, Any] = {
        "contract_id": CONTRACT,
        "rule_hash": "a" * 64,
        "source_url": "https://external-api.kalshi.com/trade-api/v2/markets?series_ticker=KXFED",
        "verified_by": "dated_observations_of_one_archived_rule_text_sha256_digest",
        "in_force_from": "2026-09-16T16:15:16+00:00",
        "in_force_to": None,
        "observed_at": "2026-09-16T16:15:16+00:00",
        "settlement_semantics": (
            "If the upper bound of the target federal funds rate published on the Federal "
            "Reserve's official website is greater than 2.75% following the Federal "
            "Reserve's Dec 9, 2026 meeting, then the market resolves to Yes."
        ),
    }
    entry.update(overrides)
    return entry


def config_for(tmp_path: pathlib.Path, entries: list[dict[str, Any]]) -> dict[str, Any]:
    """The graph's own declaration, pointed at a document this test wrote."""
    path = tmp_path / "attested_contract_rules.json"
    path.write_text(
        json.dumps({"document_version": "1", "contract_rules": entries}),
        encoding="utf-8",
    )
    config = yaml.safe_load(GRAPH_CONFIG.read_text(encoding="utf-8"))
    config["rule_vintage"]["evidence_source"] = str(path)
    return config


def test_an_open_interval_is_read_rather_than_dropped(tmp_path: pathlib.Path) -> None:
    """The shape the pipeline writes has to survive the graph's loader."""
    config = config_for(tmp_path, [record()])
    assert "in_force_to" in config["rule_vintage"]["required_record_fields"]

    records = PANEL.rule_records(config)

    assert list(records) == [CONTRACT]
    assert records[CONTRACT]["in_force_to"] is None


def test_a_record_missing_a_required_field_is_still_refused(tmp_path: pathlib.Path) -> None:
    """An unstated end is not the same fact as a stated open end."""
    without_the_key = record()
    del without_the_key["in_force_to"]
    empty_field = record(contract_id="KXFED-26DEC-T3.00", settlement_semantics="")

    assert PANEL.rule_records(config_for(tmp_path, [without_the_key, empty_field])) == {}


def test_a_closed_interval_is_still_read(tmp_path: pathlib.Path) -> None:
    """Admitting an open end must not narrow what the loader already read."""
    closed = record(in_force_to="2026-10-01T00:00:00+00:00")
    records = PANEL.rule_records(config_for(tmp_path, [closed]))
    assert records[CONTRACT]["in_force_to"] == "2026-10-01T00:00:00+00:00"
