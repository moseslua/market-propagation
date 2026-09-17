"""Regression tests for D6: every declared null contributes a comparison row.

D6 required that each declared null scenario contribute a *complete* nested
comparison row, so the family-wise simultaneous bound is certified over the whole
declaration rather than over whichever members happened to be estimable. Two nulls
previously contributed none, for two independent reasons:

``spread_only``
    declares ``news_active=False``, so no contract carries a declared news
    sensitivity. The recovered per-release shock mapping therefore came back empty
    and the ladder's ``shock``/``delayed_shock`` columns were ``None``.
``resolution_pause``
    declared a halt spanning every primary measurement window, so every primary
    target was null.

These tests guard the fixes by their observable effect rather than by their
implementation: an unestimable null silently leaves the family's bound without a
member, and that is the failure mode they exist to catch.
"""

from __future__ import annotations

import pytest

from market_propagation import calibration, historical_forecast, simulated_tapes, simulation

#: Small enough that the whole file stays well under a minute. The paired design is
#: over release clusters, so the count only has to leave the declared folds non-empty.
N_EVENTS = 24
SEED = calibration.DEFAULT_SEED
#: Draws for the paired release-clustered interval. These tests read whether a
#: repetition reached a decision at all, not the value of its bound.
BOOTSTRAP_SAMPLES = 50

#: A null that declares no news at all, the null that declares news, and the null
#: whose declared halt is the mechanism.
NO_NEWS_SCENARIO = "spread_only"
NEWS_SCENARIO = "shared_news_delay"
HALT_SCENARIO = "resolution_pause"


def _repetition(scenario: str) -> calibration.RepetitionOutcome:
    """One repetition on the declared seed list's first entry."""
    return calibration._repetition(
        scenario,
        seed=calibration._repetition_seed(SEED, 0),
        n_releases=N_EVENTS,
        minimum_mae_gain=calibration.DEFAULT_MINIMUM_MAE_GAIN,
        bootstrap_samples=BOOTSTRAP_SAMPLES,
    )


def _declared_releases(scenario: str) -> set[str]:
    """The releases the simulated frame declares, which the control has to cover."""
    frame = simulation.simulate_scenario(scenario, seed=SEED, n_events=N_EVENTS)
    return {str(event_id) for event_id in frame["event_id"]}


def test_a_scenario_without_news_declares_an_exact_zero_for_every_release() -> None:
    """An unmeasured shock and an exactly-zero shock are different facts.

    The process publishes no release package, so the estimator's news control is the
    generator's own per-release shock. A scenario declaring no news has an exact zero
    common news term for each release, and that zero has to reach the control column:
    an empty recovery leaves the ladder's shock columns null, which is what made this
    null unestimable. Coverage is asserted against the frame's own release set, so the
    test proves one value per declared release rather than mere non-emptiness.
    """
    declared = _declared_releases(NO_NEWS_SCENARIO)
    shocks = simulated_tapes.simulated_release_shocks(
        NO_NEWS_SCENARIO, seed=SEED, n_events=N_EVENTS
    )
    assert set(shocks) == declared
    assert set(shocks.values()) == {0.0}


def test_a_scenario_with_news_still_recovers_a_nonzero_shock() -> None:
    """The control that stops the zero above from being a blanket zero.

    A process with a live common news term must recover a non-zero shock for the
    releases it declares. Without this test, filling every release with ``0.0`` would
    satisfy the no-news case while destroying the news control's meaning everywhere.
    """
    declared = _declared_releases(NEWS_SCENARIO)
    shocks = simulated_tapes.simulated_release_shocks(NEWS_SCENARIO, seed=SEED, n_events=N_EVENTS)
    assert set(shocks) == declared
    assert any(value != 0.0 for value in shocks.values())


def test_spread_only_contributes_a_complete_comparison_row() -> None:
    """A null with no news at all is still estimable, and is still not promoted."""
    outcome = _repetition(NO_NEWS_SCENARIO)
    assert outcome.status == calibration.STATUS_COMPLETE
    assert outcome.reason is None
    assert outcome.promoted is False


def test_resolution_pause_contributes_a_complete_comparison_row() -> None:
    """A null whose halt invalidates some windows still contributes a row."""
    outcome = _repetition(HALT_SCENARIO)
    assert outcome.status == calibration.STATUS_COMPLETE
    assert outcome.reason is None
    assert outcome.promoted is False


def test_the_declared_halt_opens_after_the_primary_window_closes() -> None:
    """The halt must invalidate windows that span it without consuming all of them.

    The primary comparison's window is ``event + forecast_origin`` through
    ``event + forecast_origin + future_horizon``. While the halt covered that window
    end to end, every primary row was marked invalid and its target was null, so the
    scenario contributed no row. The halt therefore has to open at or after the primary
    window closes — and it still has to bite somewhere, or the scenario would no longer
    be about a halt at all, so a window spanning it must still be excluded.
    """
    settings = historical_forecast.ForecastSettings()
    window_end = settings.forecast_origin_seconds + settings.future_horizon_seconds
    spec = simulation.SCENARIOS[HALT_SCENARIO]
    assert spec.pause is not None
    assert spec.pause[0] >= window_end

    # Read the exclusion off the frame rather than off the simulator's own horizon
    # tuple: what matters is that the halt is still a declared exclusion and that some
    # row survives it, not which private constant the horizon grid holds.
    frame = simulation.simulate_scenario(HALT_SCENARIO, seed=SEED, n_events=N_EVENTS)
    reasons = frame["exclusion_reason"].fillna("").astype(str)
    assert reasons.str.contains("halted").any()
    assert bool(frame["valid"].any())


# The parametrized case below is the slowest test in this file by design: it is the D6
# acceptance criterion itself, evaluated once per declared null. Keep it last so a
# failure in the cheap guards above is reported first.
@pytest.mark.parametrize("scenario", calibration.declared_scenarios()[0])
def test_every_declared_null_contributes_a_complete_comparison_row(scenario: str) -> None:
    """The D6 criterion: no declared null may silently become unestimable.

    A null that blocks contributes no promotion rate to the family's simultaneous
    bound, so the bound is certified over a subset of the declaration while reading as
    though it covered all of it. This is the regression guard for that: every name the
    declaration lists as a null has to reach a decision.
    """
    outcome = _repetition(scenario)
    assert outcome.status == calibration.STATUS_COMPLETE, outcome.reason
