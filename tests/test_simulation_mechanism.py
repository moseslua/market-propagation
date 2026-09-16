"""Mechanism tests for the simulator's delayed-copy communication process.

These defend the property the forecast ladder depends on. The fast venue shows a
persistent, privately created level before the forecast cutoff. A scenario with a
declared edge delivers a copy of that level to the target venue only after the
cutoff and inside the forecast window, so the fast venue's observed change
anticipates a target movement that has not happened yet. The common-news null
carries the same private level from the same random stream and delivers none of
it, so a neighbour lead observed there has no information behind it.

Only the mechanism and the frame contract are asserted here. Whether the
estimator converts this into ensemble predictive power is a separate claim and is
not re-estimated in this module.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import market_propagation.simulation as simulation

#: Recorded experiment size; the mechanism must hold at this event count.
EVENTS = 120
SEED = 20260913
#: Recorded forecast cutoff, in seconds after the release.
CUTOFF = 60.0


def _primary(frame: pd.DataFrame) -> pd.DataFrame:
    """Primary-target rows in a canonical, aligned order."""
    panel = simulation.primary_target(frame)
    return panel.sort_values(["event_id", "horizon_seconds"], kind="stable").reset_index(drop=True)


@pytest.fixture(scope="module")
def talk() -> pd.DataFrame:
    return simulation.simulate_scenario(
        "communication", seed=SEED, n_events=EVENTS, forecast_delay=CUTOFF
    )


@pytest.fixture(scope="module")
def null() -> pd.DataFrame:
    return simulation.simulate_scenario(
        "shared_news_delay", seed=SEED, n_events=EVENTS, forecast_delay=CUTOFF
    )


def test_delayed_copy_lands_after_the_cutoff_inside_the_forecast_window(talk):
    """The copy arrives after prediction, not before it.

    The prediction cutoff sits 60s after the release, so a window that had
    already received the copy could not attribute anything to the forecast. The
    declared lag must therefore fall beyond the cutoff, and the 60s horizon
    endpoint, which precedes it, must contain no delivered signal at all.
    """
    truth = talk.attrs["ground_truth"]
    edge = truth["communication_edges"][0]
    cutoff = truth["forecast_delay_seconds"]
    assert edge["lag_seconds"] > cutoff
    assert edge["lag_seconds"] < cutoff + 300
    assert edge["arrival_seconds_after_cutoff"] > 0.0
    assert edge["arrival_inside_forecast_window"] is True

    panel = _primary(talk)
    early = panel.loc[panel["horizon_seconds"] == 60]
    late = panel.loc[panel["horizon_seconds"] == 300]
    # endpoint at +120s, before the +300s lag: nothing has been delivered yet
    assert (early["transmitted_signal"] == 0.0).all()
    # endpoint at +360s, after the lag: the copy has landed
    assert late["transmitted_signal"].abs().gt(0.0).mean() > 0.9


def test_observed_lead_anticipates_the_delivered_copy(talk):
    """The pre-prediction neighbour change carries the level later delivered.

    Both sides read the same realised fast-venue level, so the observed change
    and the delivered copy move together across events. This is a property of the
    process, not of any fitted estimator.
    """
    panel = _primary(talk)
    late = panel.loc[panel["valid"] & (panel["horizon_seconds"] == 300)]
    assert len(late) > 50
    observed = late["neighbor_lag"].to_numpy(float)
    delivered = late["transmitted_signal"].to_numpy(float)
    # a non-degenerate lead is what makes the comparison meaningful at all
    assert observed.std() > 0.02
    # The lead is the fast venue's change over the trailing window and the copy is
    # the level it later delivers, so the two agree in direction without being the
    # same quantity: the shared news response is also inside the observed change.
    correlation = float(np.corrcoef(observed, delivered)[0, 1])
    assert correlation > 0.4
    high = delivered[observed >= np.quantile(observed, 0.75)]
    low = delivered[observed <= np.quantile(observed, 0.25)]
    assert np.median(high) > np.median(low)


def test_null_shares_the_private_path_and_delivers_none_of_it(null, talk):
    """The null differs from the treatment only by the declared edge.

    Every fast-venue observation is bit-identical across the pair, so the two are
    matched on the private level, the release shock, the priors, the sensitivities
    and the update schedule. The null delivers nothing, which is exactly why a
    network edge found there is a false positive rather than propagation.
    """
    null_truth = null.attrs["ground_truth"]
    talk_truth = talk.attrs["ground_truth"]
    assert null_truth["communication"] is False
    assert null_truth["communication_edges"] == []
    assert talk_truth["communication"] is True
    # matched amplitude, drawn from the same stream
    assert null_truth["private_component"]["sigma"] > 0.0
    assert null_truth["private_component"]["sigma"] == talk_truth["private_component"]["sigma"]

    assert (null["transmitted_signal"] == 0.0).all()
    assert talk["transmitted_signal"].abs().max() > 0.0

    left = _primary(null)
    right = _primary(talk)
    pd.testing.assert_series_equal(left["event_id"], right["event_id"])
    # the neighbour columns come from the fast venue, so they are identical
    for column in (*simulation.NEIGHBOR_COLUMNS, "shock", "delayed_shock"):
        pd.testing.assert_series_equal(left[column], right[column])
    # the null's lead is genuine and non-degenerate, it simply predicts nothing
    assert left["neighbor_lag"].std() > 0.02

    # Nothing has been delivered at the cutoff, so the target venue's own
    # pre-arrival quotes are matched too. That is the matched-nuisance claim: the
    # baseline state the estimator conditions on is identical across the pair.
    pd.testing.assert_series_equal(left["current_price"], right["current_price"])

    # The copy arrives inside the 300s window, so only the horizons whose endpoint
    # falls past the lag can differ, and they must.
    early_left = left.loc[left["horizon_seconds"] == 60, "target"]
    early_right = right.loc[right["horizon_seconds"] == 60, "target"]
    pd.testing.assert_series_equal(early_left, early_right)
    late_left = left.loc[left["horizon_seconds"] == 300, "target"]
    late_right = right.loc[right["horizon_seconds"] == 300, "target"]
    assert not np.allclose(late_left, late_right)
    assert np.abs(late_right.to_numpy(float) - late_left.to_numpy(float)).max() > 0.0


def test_only_the_communicating_scenario_delivers_a_copy():
    """Every other registered scenario keeps its declared absence of an edge."""
    names = simulation.scenario_names()
    assert "communication" in names
    for name in names:
        frame = simulation.simulate_scenario(name, seed=99, n_events=4, horizons=[300])
        truth = frame.attrs["ground_truth"]
        assert truth["synthetic"] is True
        assert truth["process"] == "synthetic_software_simulation"
        delivered = float(frame["transmitted_signal"].abs().max())
        if name == "communication":
            assert delivered > 0.0
        else:
            assert delivered == 0.0


def test_frame_availability_and_row_order_are_invariants(talk):
    """The frozen table keeps its contract, its cutoff and its canonical order."""
    assert list(talk.columns[: len(simulation.FORECAST_COLUMNS)]) == list(
        simulation.FORECAST_COLUMNS
    )
    assert set(simulation.TRUTH_ONLY_COLUMNS) <= set(talk.columns)
    assert set(simulation.TRUTH_ONLY_COLUMNS).isdisjoint(simulation.FORECAST_COLUMNS)
    assert (talk["max_input_available_time"] <= talk["prediction_time"]).all()

    key = ["event_time", "contract_id", "horizon_seconds"]
    ordered = talk.sort_values(key, kind="stable").reset_index(drop=True)
    pd.testing.assert_frame_equal(talk.reset_index(drop=True), ordered)

    shuffled = talk.sample(frac=1.0, random_state=3)
    restore = ["event_id", "contract_id", "horizon_seconds"]
    restored = shuffled.sort_values(restore, kind="stable").reset_index(drop=True)
    expected = talk.sort_values(restore, kind="stable").reset_index(drop=True)
    pd.testing.assert_frame_equal(restored, expected)


def test_primary_target_keeps_the_cutoff_and_the_scenario_metadata(talk):
    panel = simulation.primary_target(talk)
    assert panel["contract_id"].str.contains("-C0-").all()
    assert set(panel["cohort"]) == {"downstream"}
    assert (panel["max_input_available_time"] <= panel["prediction_time"]).all()
    assert panel.attrs["scenario"] == "communication"
    assert panel.attrs["ground_truth"]["n_events"] == EVENTS
