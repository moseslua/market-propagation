"""Acceptance tests for the packaged synthetic replay sample.

These defend the properties the sample exists to prove: one call over one
immutable packaged fixture runs the real raw-to-replay-to-panel-to-Parquet path,
the two replay orders are genuinely different computations, a record that could
not have been read before the event is excluded from the usable fold while the
source fold still anchors on it, and neither the fixture nor a sealed panel can
be rewritten by a later build.

Every assertion is on what a consumer observes in the returned panels, the
sealed files, or the raw store. Nothing here pins a docstring, an internal
helper, or a private wiring detail.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from importlib import resources
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_propagation.domain import parse_utc_time
from market_propagation.point_in_time import PANEL_COLUMNS
from market_propagation.replay import ORDER_SOURCE, ORDER_USABLE
from market_propagation.sample import (
    DISAGREEMENTS_NAME,
    RAW_DIRECTORY_NAME,
    SOURCE_PANEL_NAME,
    USABLE_PANEL_NAME,
    SampleArtifacts,
    build_sample,
)
from market_propagation.storage import RawStore, hash_file, read_parquet

FIXTURE_RESOURCE = ("fixtures", "replay.json")
FIXTURE_ID = "syn_cpi_2031_01"
FIXTURE_ID_GAP = "syn_cpi_2031_03"
FIXTURE_ID_REORDER = "syn_cpi_2031_05"
LATE_RECORD = "0010"
HORIZONS = (60, 300, 900, 1800, 3600)
FAMILY_COUNT = {"cpi": 5, "employment": 5}


def packaged_bytes() -> bytes:
    return resources.files("market_propagation").joinpath(*FIXTURE_RESOURCE).read_bytes()


@pytest.fixture(scope="module")
def sample(tmp_path_factory: pytest.TempPathFactory) -> SampleArtifacts:
    return build_sample(tmp_path_factory.mktemp("sample"))


@pytest.fixture(scope="module")
def store(sample: SampleArtifacts) -> RawStore:
    return RawStore(sample.raw_root)


def row(frame: pd.DataFrame, event_id: str, horizon: int) -> pd.Series:
    selected = frame[(frame["event_id"] == event_id) & (frame["horizon_seconds"] == horizon)]
    assert len(selected) == 1, f"{event_id} has no single row at horizon {horizon}"
    return selected.iloc[0]


def hashes(cell: object) -> list[str]:
    return str(cell).split(",")


def values(series: pd.Series) -> list[object]:
    """One column as plain values, with every null spelling normalized to ``None``.

    The in-memory frame and the sealed file spell a missing value differently,
    and that difference is not part of what the panel claims.
    """
    return [None if pd.isna(value) else value for value in series.tolist()]


def test_one_build_produces_both_folds_from_the_packaged_fixture(sample: SampleArtifacts) -> None:
    assert list(sample.source_panel.columns) == list(PANEL_COLUMNS)
    assert list(sample.usable_panel.columns) == list(PANEL_COLUMNS)
    assert not sample.source_panel.empty
    assert not sample.usable_panel.empty
    assert sample.release_count == sum(FAMILY_COUNT.values())

    # Each panel is one fold's own output, labelled with the fold that produced
    # it, so neither can be mistaken for the other.
    assert set(sample.source_panel["replay_order"]) == {ORDER_SOURCE}
    assert set(sample.usable_panel["replay_order"]) == {ORDER_USABLE}

    # Both folds are real, independent replays of the same stream, so they see
    # the same records and the same event grid.
    assert sample.quote_count > 0
    assert sample.quote_count == sample.disagreements["coverage"][ORDER_SOURCE]["quote_count"]
    assert sample.quote_count == sample.disagreements["coverage"][ORDER_USABLE]["quote_count"]
    assert set(sample.source_panel["horizon_seconds"]) == set(HORIZONS)
    assert sorted(sample.source_panel["family"].unique()) == sorted(FAMILY_COUNT)
    for frame in (sample.source_panel, sample.usable_panel):
        counts = frame.groupby("family")["event_id"].nunique().to_dict()
        assert counts == FAMILY_COUNT


def test_panels_are_auditable_and_mask_instead_of_zeroing(sample: SampleArtifacts) -> None:
    for frame in (sample.source_panel, sample.usable_panel):
        assert bool(frame["valid"].any())
        masked = frame[~frame["valid"]]
        assert not masked.empty
        # A masked row keeps its reason and keeps its measurement absent. A zero
        # would be indistinguishable from an observed zero response.
        assert masked["exclusion_reason"].notna().all()
        assert masked[["baseline", "endpoint", "response"]].isna().all().all()
        # Every row stays identifiable and carries the payoff orientation its
        # rule needs, whether or not it was admitted.
        assert frame[["event_id", "contract_id", "event_time"]].notna().all().all()
        assert frame[["operator", "rounding", "units", "orientation_sign"]].notna().all().all()
        assert frame["raw_hashes"].notna().all()


def test_every_row_points_at_bytes_that_are_really_stored(
    sample: SampleArtifacts, store: RawStore
) -> None:
    referenced = {
        digest
        for frame in (sample.source_panel, sample.usable_panel)
        for cell in frame["raw_hashes"]
        for digest in hashes(cell)
    }
    assert referenced
    assert referenced <= set(sample.raw_hashes)
    for digest in sorted(referenced):
        assert hashlib.sha256(store.get(digest)).hexdigest() == digest


def test_the_panels_are_sealed_where_the_returned_paths_say(sample: SampleArtifacts) -> None:
    out = sample.raw_root.parent
    assert sample.raw_root == out / RAW_DIRECTORY_NAME
    assert sorted(path.name for path in out.iterdir()) == sorted(
        [
            RAW_DIRECTORY_NAME,
            DISAGREEMENTS_NAME,
            SOURCE_PANEL_NAME,
            USABLE_PANEL_NAME,
            f"{SOURCE_PANEL_NAME}.manifest.json",
            f"{USABLE_PANEL_NAME}.manifest.json",
        ]
    )

    # The returned frames and the sealed files are the same data: reading the
    # files back through the verifying reader reproduces them. Missing values are
    # compared as missing rather than as a particular null spelling, because the
    # on-disk representation is not the in-memory one.
    for name, frame in (
        (SOURCE_PANEL_NAME, sample.source_panel),
        (USABLE_PANEL_NAME, sample.usable_panel),
    ):
        back = read_parquet(out / name)
        assert list(back.columns) == list(PANEL_COLUMNS)
        assert len(back) == len(frame)
        for column in PANEL_COLUMNS:
            assert values(back[column]) == values(frame[column]), column


def test_a_late_record_anchors_the_source_fold_and_is_excluded_from_the_usable_fold(
    sample: SampleArtifacts, store: RawStore
) -> None:
    receipt = store.receipt(
        f"{sample.fixture_hash}-stream-book-{FIXTURE_ID}-{LATE_RECORD}",
        source="synthetic_replay_fixture",
    )
    assert receipt is not None, "the late record was never archived"
    late_hash = str(receipt["raw_hash"])
    received = parse_utc_time(receipt["received_time"], field_name="receipt.received_time")
    # The archived bytes are the record's own definition, so its source instant
    # is derived from them rather than restated here.
    archived = json.loads(store.get(late_hash).decode("utf-8"))
    scheduled = parse_utc_time(
        fixture_event(sample, FIXTURE_ID)["scheduled_at_utc"], field_name="scheduled_at_utc"
    )
    late_source_time = scheduled + pd.Timedelta(seconds=int(archived["source_offset_seconds"]))
    assert int(archived["latency_seconds"]) > 0

    source = row(sample.source_panel, FIXTURE_ID, 60)
    usable = row(sample.usable_panel, FIXTURE_ID, 60)

    # Its source stamp precedes the event, so the source fold may anchor the
    # baseline on it, and it does: it is the first hash on the baseline.
    assert late_source_time < source["event_time"]
    assert hashes(source["raw_hashes"])[0] == late_hash

    # It was not received until after the event was usable here, so the usable
    # fold cannot anchor its baseline on it: an observation that arrived late is
    # not evidence of the state at the event. The two folds therefore anchor the
    # same row on different observations.
    assert received > usable["event_time"]
    assert hashes(usable["raw_hashes"])[0] != late_hash
    assert usable["baseline_time"] < late_source_time
    assert usable["baseline_time"] != source["baseline_time"]
    assert usable["baseline"] != source["baseline"]

    # The exclusion is about the as-of instant, not the record: once its receipt
    # has happened the record is admissible again, so the usable fold does read
    # it at a later endpoint. Excluding it everywhere would be a different, and
    # wrong, claim.
    assert received <= usable["endpoint_time"]
    assert late_hash in hashes(usable["raw_hashes"])


def fixture_event(sample: SampleArtifacts, event_id: str) -> dict:
    """One event as the packaged fixture states it."""
    document = json.loads(packaged_bytes().decode("utf-8"))
    matches = [event for event in document["events"] if event["event_id"] == event_id]
    assert len(matches) == 1
    assert sample.fixture_hash == hashlib.sha256(packaged_bytes()).hexdigest()
    return matches[0]


def test_a_sequence_gap_masks_the_horizon_inside_it_and_recovers_afterward(
    sample: SampleArtifacts,
) -> None:
    frame = sample.usable_panel[sample.usable_panel["event_id"] == FIXTURE_ID_GAP].set_index(
        "horizon_seconds"
    )
    # The gap opens at +2 minutes and a fresh snapshot rebuilds the book at +10,
    # so only the horizon whose endpoint falls between them is unverifiable. The
    # earlier horizon closed before the gap, and the later ones fall after the
    # recovery, so neither may be masked.
    masked = sorted(frame.index[~frame["valid"]])
    admitted = sorted(frame.index[frame["valid"]])
    assert masked == [300]
    assert admitted == [60, 900, 1800, 3600]
    assert frame.loc[300, "exclusion_reason"] == "gap"
    assert pd.isna(frame.loc[300, "baseline"]) and pd.isna(frame.loc[300, "endpoint"])

    # The gap is a fact of this stream, so both folds report it.
    assert sample.disagreements["gaps"][ORDER_SOURCE]
    assert "sequence_gap" in sample.disagreements["gaps"][ORDER_SOURCE]
    assert "sequence_gap" in sample.disagreements["gaps"][ORDER_USABLE]


def test_the_two_folds_reconstruct_this_book_at_different_prices(
    sample: SampleArtifacts,
) -> None:
    # Two mutations of one price level swap places between the folds, so the
    # same bytes rebuild different books. Reporting that is the point: a single
    # sorted copy of one fold could not have produced it.
    assert sample.disagreements["inversion_count"] >= 1
    assert sample.disagreements["inversions_truncated"] is False
    assert sample.disagreements["state_disagreement_count"] >= 1
    assert sample.disagreements["final_state_disagreement_count"] >= 1
    assert sample.disagreements["agreement"] is False

    # The book this release's stream feeds is named by the target contract the
    # fixture pairs with it, so the key is read off the panel rather than typed.
    market = f"synthetic_venue|{row(sample.source_panel, FIXTURE_ID_REORDER, 900)['contract_id']}"
    finals = {
        entry["market_key"]: entry for entry in sample.disagreements["final_state_disagreements"]
    }
    assert market in finals
    assert finals[market]["source"]["bid"] != finals[market]["usable"]["bid"]

    # The disagreement reaches the panel: the same horizon of the same event
    # carries a different endpoint price under each fold. It is the same
    # occurrence that is reconstructed differently, which is exactly why sorting
    # one fold's output could not produce the other.
    source = row(sample.source_panel, FIXTURE_ID_REORDER, 900)
    usable = row(sample.usable_panel, FIXTURE_ID_REORDER, 900)
    assert source["valid"] and usable["valid"]
    assert source["endpoint"] != usable["endpoint"]
    assert source["raw_hashes"] == usable["raw_hashes"]
    assert finals[market]["source"]["bid"] != finals[market]["usable"]["bid"]


def fixture_document() -> dict:
    """The packaged fixture parsed as the tests read it."""
    return json.loads(packaged_bytes().decode("utf-8"))


def exposures_by_pair() -> dict[tuple[str, str], dict]:
    """The fixture's own exposure list, keyed by (release_event_id, contract_id).

    This is the declared mapping the sample hands to the panel builder, so the
    tests read it from the fixture rather than restating either side of a pair.
    """
    return {
        (str(entry["release_event_id"]), str(entry["contract_id"])): entry
        for entry in fixture_document()["exposure"]
    }


def test_each_target_contract_answers_the_next_period_rather_than_its_release() -> None:
    document = fixture_document()
    events = {str(event["event_id"]): event for event in document["events"]}
    contracts = {str(contract["contract_id"]): contract for contract in document["contracts"]}
    exposures = exposures_by_pair()
    assert len(exposures) == len(events)

    for (release_id, contract_id), entry in exposures.items():
        contract = contracts[contract_id]
        release = events[release_id]

        # The payoff is a later outcome: a different event, a later reference
        # period, and a print that has not happened at the release. A contract
        # whose outcome is its own release would be direct material mislabelled.
        assert entry["outcome_event_id"] != release_id
        assert contract["event_id"] == entry["outcome_event_id"]
        assert contract["reference_period"] == entry["outcome_reference_period"]
        assert entry["outcome_reference_period"] > release["reference_period"]

        # It is also a claim the market could hold at that release: readable and
        # already trading before it, still trading after the longest window, and
        # stopped before the print whose value settles it.
        assert contract["rule_available_at"] <= release["scheduled_at_utc"]
        assert contract["open_time"] <= release["scheduled_at_utc"]
        assert contract["close_time"] > release["scheduled_at_utc"]
        assert contract["close_time"] <= entry["outcome_published_at"]
        assert contract["resolve_time"] >= entry["outcome_published_at"]
        assert contract["deadline"] >= contract["resolve_time"]

        # Whether the fixture also carries the outcome print as an event is a
        # stated fact, not something a reader has to infer from the ids.
        assert isinstance(entry["outcome_event_packaged"], bool)
        assert (entry["outcome_event_id"] in events) == entry["outcome_event_packaged"]
        assert entry["basis"].strip()


def test_a_later_target_is_unresolved_at_the_information_release(
    sample: SampleArtifacts,
) -> None:
    exposures = exposures_by_pair()
    for frame in (sample.source_panel, sample.usable_panel):
        assert not frame.empty
        for _, record in frame.iterrows():
            entry = exposures[(record["event_id"], record["contract_id"])]
            # Every window this panel measures closes before the print that
            # determines the contract, so no row spans the outcome it pays on.
            # The label-time assertion for that fact lives with the panel's own
            # label columns rather than being restated here.
            assert record["endpoint_time"] < pd.Timestamp(entry["outcome_published_at"])
            assert record["event_time"] < pd.Timestamp(entry["outcome_published_at"])


def test_built_panels_are_downstream_by_their_declared_exposure(
    sample: SampleArtifacts,
) -> None:
    exposures = exposures_by_pair()
    for frame in (sample.source_panel, sample.usable_panel):
        # The panel is the declared mapping: one row per exposure pair per
        # horizon, with no pair invented and none dropped.
        assert set(zip(frame["event_id"], frame["contract_id"], strict=True)) == set(exposures)
        assert set(frame["horizon_seconds"]) == set(HORIZONS)
        for _, record in frame.iterrows():
            entry = exposures[(record["event_id"], record["contract_id"])]
            assert entry["cohort"] == "downstream"
            assert record["cohort"] == entry["cohort"]
            # The row's event is the release it answers, not the outcome it pays on.
            assert record["event_id"] == entry["release_event_id"]
            assert record["contract_id"] == entry["contract_id"]
        assert sorted(frame["cohort"].unique()) == ["downstream"]


def test_the_packaged_fixture_is_archived_verbatim_and_never_written(
    sample: SampleArtifacts, store: RawStore
) -> None:
    before = packaged_bytes()
    assert sample.fixture_hash == hashlib.sha256(before).hexdigest()
    assert sample.fixture_hash in sample.raw_hashes
    # The fixture's bytes are retrievable from the store, byte for byte.
    assert store.get(sample.fixture_hash) == before
    # Rebuilding does not touch the packaged resource.
    build_sample(sample.raw_root.parent)
    assert packaged_bytes() == before


def test_a_repeat_build_is_byte_identical_and_a_changed_one_is_refused(
    tmp_path: Path,
) -> None:
    out = tmp_path / "sample"
    first = build_sample(out)
    sealed = {name: hash_file(out / name) for name in (SOURCE_PANEL_NAME, USABLE_PANEL_NAME)}

    second = build_sample(out)
    assert second.fixture_hash == first.fixture_hash
    assert second.raw_hashes == first.raw_hashes
    assert second.source_panel.equals(first.source_panel)
    assert second.usable_panel.equals(first.usable_panel)
    assert {name: hash_file(out / name) for name in sealed} == sealed

    # A different measurement bound would change the sealed panels, so it must
    # raise rather than replace what an earlier build recorded.
    with pytest.raises(FileExistsError):
        build_sample(out, max_age_seconds=1)


def test_a_modified_panel_is_refused_by_the_verifying_reader(
    tmp_path: Path, sample: SampleArtifacts
) -> None:
    forged = tmp_path / "forged"
    forged.mkdir()
    table = pq.read_table(sample.raw_root.parent / USABLE_PANEL_NAME)
    index = table.schema.get_field_index("endpoint")
    values = table.column("endpoint").to_pylist()
    values[0] = 0.999999
    field = table.schema.field(index)
    forged_table = table.set_column(index, field, pa.array(values, type=field.type))
    pq.write_table(forged_table, forged / USABLE_PANEL_NAME)
    shutil.copy(
        sample.raw_root.parent / f"{USABLE_PANEL_NAME}.manifest.json",
        forged / f"{USABLE_PANEL_NAME}.manifest.json",
    )
    assert hash_file(forged / USABLE_PANEL_NAME) != hash_file(
        sample.raw_root.parent / USABLE_PANEL_NAME
    )
    with pytest.raises(ValueError, match="does not match"):
        read_parquet(forged / USABLE_PANEL_NAME)


def test_the_report_names_the_panels_it_sealed(sample: SampleArtifacts) -> None:
    report = json.loads((sample.raw_root.parent / DISAGREEMENTS_NAME).read_text(encoding="utf-8"))
    assert report["fixture"]["hash"] == sample.fixture_hash
    assert report["fixture"]["synthetic"] is True
    assert report["max_age_seconds"] == 120
    assert report["horizons_seconds"] == list(HORIZONS)
    assert set(report["panels"]) == {ORDER_SOURCE, ORDER_USABLE}
    for order, name in ((ORDER_SOURCE, SOURCE_PANEL_NAME), (ORDER_USABLE, USABLE_PANEL_NAME)):
        entry = report["panels"][order]
        assert entry["file"] == name
        assert entry["content_hash"] == hash_file(sample.raw_root.parent / name)
        assert entry["row_count"] == len(sample.source_panel)
    assert report["disagreements"]["inversion_count"] == sample.disagreements["inversion_count"]


def test_no_realized_outcome_is_packaged_so_no_label_time_is_readable(
    sample: SampleArtifacts,
) -> None:
    # The target contracts stay unresolved, so every row's label time is absent
    # rather than filled with a settle time the study never observed.
    for frame in (sample.source_panel, sample.usable_panel):
        assert frame["label_available_time"].isna().all()
        assert frame["training_cutoff"].isna().all()

    document = json.loads(packaged_bytes().decode("utf-8"))
    assert document["resolutions"] == []


def test_a_nonpositive_measurement_bound_is_refused(tmp_path: Path) -> None:
    # A zero bound would exclude every quote and then report the resulting empty
    # panel as a measured one.
    for bound in (0, -1):
        with pytest.raises(ValueError, match="must be positive"):
            build_sample(tmp_path / f"bound-{bound}", max_age_seconds=bound)
