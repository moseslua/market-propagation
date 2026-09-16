# Measuring information propagation in macro prediction markets under asynchronous observation

Research draft. This is a feasibility and methods result. The empirical estimation
the plan specifies is blocked, and the section titled The unmet prerequisite names
the reason.

## Status and classification

| field | value |
| --- | --- |
| Document | Research draft, methods and feasibility |
| Specification | `configs/study_v1.yaml`, version v1, frozen locally on 2026-09-13 |
| Specification status | `frozen_local_unregistered`; no external registry, no human sign-off claimed |
| Empirical status | `unestimated`, anchored to the freeze date |
| Real cohort consumed | Ten release events, 400 lifecycle-eligible policy contracts, 0 study eligible |
| Empirical estimation | Blocked |
| Synthetic software result | Complete, and it is not an empirical result |

Every number in this draft carries a label. Values from the seeded simulator are
labelled synthetic wherever they appear and are not read as evidence about a real
venue, release or contract. No claim here is about causal transmission in a real
market, statistical power in a real sample, profitability, or priority over earlier
work.

## The question

The plan's question is stated narrowly:

> After a public information shock, which parts of the subsequent market response
> reflect direct updating, transmission between markets, and mechanical delays in
> observation or quoting?

The initial domain is US CPI and Employment Situation releases. The design keeps
three evidence levels apart. Descriptive statements report how quotes and dispersion
change around a release. Predictive statements report whether one contract's past
information improves an out-of-sample forecast of another beyond shared news and
own-market state. Causal statements need an intervention or a defended natural
experiment. This draft fixes descriptive and predictive claims only. A positive
predictive result would not be read as causal transmission, and no coefficient in it
would be called a propagator.

Directly resolved release contracts and downstream unresolved contracts are separate
studies. The downstream cohort is the primary one, because a contract that resolves
at the release measures settlement rather than propagation.

The specification states that a negative finding is a valid outcome: if apparent
propagation disappears once observation clocks are aligned, that is the result.

## Observables and the estimand

The observable is a quote midpoint on a two-sided book. Midpoint and spread are
defined only for a valid two-sided book; outside that, a quote is awaiting a
snapshot, in a gap, disconnected, halted, closed, crossed, or missing. The three
times stay distinct, because the estimand depends on the distinction.

| time | meaning |
| --- | --- |
| Source time | When the venue or publisher says the record occurred |
| Receipt time | When this system read the response |
| Usable time | `availability.upper`, the only time forecast features admit |

Two replay orders are computed and published separately, source-time order for the
economic panel and usable-time order for the forecast panel. Disagreements between
them are reported rather than resolved by picking the more attractive order. In the
synthetic sample the two folds disagree, in one final book state: `agreement: false`
with `final_state_disagreement_count: 1`, on a bid of `0.455` under source order
against `0.46` under usable order. That disagreement is a property of the replay
machinery and is reported at its observed value.

The response measure is a finite-horizon midpoint change,
`R(h) = q(t + h) - q(t-)`, over the prespecified horizons 1, 5, 15, 30 and 60
minutes. The primary historical outcome is the five-minute change. It is one
outcome, not the smallest p-value among five. Ratios and settling-time diagnostics
are reported only where a denominator is prespecified and stable, and an unstable
curve is reported as unresolved within the window rather than as a precise
assimilation half-life.

Thresholds are absolute probability units on the 0 to 1 scale. The smallest relevant
response is 0.01, which is 1 percentage point, and the smallest relevant MAE gain is
0.005, which is 0.5 percentage points. Stored values are read as they are and never
rescaled.

## Data and contract construction

The cohort is the first five 2025 CPI releases and the first five 2025 Employment
Situation releases, ten events, listed in `reports/data_audit.md`. The recorded
selection basis is the first five monthly releases of 2025 with official calendars
read, selected without reference to outcomes. The window is 30 minutes before the
scheduled instant to 60 minutes after it.

Contract construction runs in three steps, and each step keeps its output separate
from the next.

Acquisition reads documented public listings and records every attempt. Eligibility
then applies the venue's own lifecycle facts to those records. Study eligibility
requires one further piece of evidence: a configured record naming the contract,
matching the fetched rule hash, and covering the release instant.

That last step is where the real cohort stops. No such record is configured for this
cohort, so all 400 lifecycle-eligible contracts carry `rule_version_verified: false`
and `rule_available_at: null`, and the study-eligible count is 0. The reason is
recorded in the gate detail: a market's own creation and open times date the market,
not the rule text a later fetch returned, so with nothing binding a fetched rule hash
to an interval in force at the release, no lifecycle-eligible contract becomes
verified settlement semantics.

The policy cohort's relation is economic exposure, not equivalence. A CPI or
Employment Situation release updates the information set a later policy decision is
made against, so a contract paying on that later decision carries exposure to the
release. The configuration states that this is a hypothesis about a later payoff and
never a verified equivalence between the release and the policy contract.

Since no contract becomes study eligible, the measurement step of the plan's ladder
runs on the packaged synthetic sample and not on the real cohort. The synthetic
sample holds 10 releases and 10 contracts, 50 panel rows in each replay fold, of
which 49 are valid and one is masked for a gap.

## Identification problem

The starting structure separates observed news, unobserved shared information, and
per-market observation delays. The hypothesized mechanism is that one market's
information influences another's beyond shared news and own-market state.

Known release time improves alignment. It does not make every associated price move
causal, and it does not make the macro surprise a valid instrument for one market
when the same surprise directly affects another. Three consequences follow, and the
design carries each as a constraint rather than a caveat.

A predictive edge over a shared-news model is not evidence of propagation, because
omitted common information produces the same edge. A held-out gain therefore gates
model promotion but does not license a transmission claim. Post-event liquidity is a
potential mediator, so it is used for pre-event heterogeneity and analyzed separately
in a mechanism model rather than controlled in the total response. And two
observation processes over the same latent price produce apparent lead-lag with no
communication at all, which is why the null in the next section is built first.

## Baselines and the model ladder

The plan specifies a ladder, and the implementation honours its order. One item of
the ladder is not run at all, and that is stated where it belongs rather than
reported as an empty result.

**Level 0, diagnostics and baselines.** Response plots, missingness and quote-age
maps, no-change forecasts, own-market autoregressions, and a shared-news model with
heterogeneous delays. A network model must beat the shared-news model, not only a
naive last-price baseline.

**Level 1, event-study local projections.** The production estimator is implemented
and was invoked on the synthetic sample. Its result record is empty, and the reason
is recorded in the record itself: the packaged sample carries no release
expectation, so `shock_column` is null and no slope is estimated. The recorded
identification is `descriptive only`, with the independent unit being the economic
release. Reporting a slope here would require manufacturing a consensus, which the
specification prohibits.

**Level 2, conditional propagation.** The production comparison is a nested
news-versus-network forecast with a validation-selected ridge engine and a bounded
link function. The target is the future probability change, the metric is
probability-point MAE, and the unit is the economic release. The network feature set
adds admissible lagged neighbour information to own-market state and the shared
release term.

**Level 3, observation-aware latent state.** Not implemented. The plan makes it
conditional on Level 2 leaving systematic residual structure, and Level 2 has not
cleared its own gate.

**Level 4, path-distribution model.** Optional, and not implemented. The plan makes
it conditional on sufficient independent variation, which ten releases do not
supply.

## Synthetic falsification

The falsification audit runs before any learned graph is interpreted. It is the
production estimator, split authority, metric and threshold on simulated processes
only. Its recorded interpretation is that it bounds the gate's false-positive
behaviour and its recovery rate at the stated event count, and is not a real-market
causal claim.

Two processes are simulated. `shared_news_delay` is the null: a common latent price
with different observation delays and no communication. `communication` adds genuine
delayed communication. Each repetition shares one seed across both frames, so the
paired difference isolates the declared mechanism rather than the noise. Truth-only
simulator columns are never predictors.

At 120 events and 40 repetitions:

| quantity | value |
| --- | --- |
| Null scenario | `shared_news_delay` |
| Null false positives | 0 of 40 |
| Null mean gain | -0.000183 |
| Recovery scenario | `communication` |
| Recovery count | 21 of 40 |
| Power | 0.525 |
| Power, Wilson one-sided lower bound | 0.3848 |
| Target power | 0.8 |
| Status | `inconclusive` |
| Paired mean difference | 0.005304 |
| Paired difference interval | 0.004403 to 0.006205 |
| Repetitions with a positive difference | 40 of 40 |

The audit's own inconclusive reason is that recovery is not bounded above 0.8 at 0.95
confidence by 40 repetitions, with a one-sided lower bound of 0.3848. Power is taken
from the observed recovery count across fixed seeds and is never inferred from the
simulator's declared mechanism.

A wider grid shows the same picture at larger event counts, and both rows stay
inconclusive:

| events | status | false-positive rate | power | power lower bound | mean gain |
| --- | --- | --- | --- | --- | --- |
| 240 | `inconclusive` | 0.0 | 0.35 | 0.2255 | 0.004416 |
| 480 | `inconclusive` | 0.0 | 0.40 | 0.2694 | 0.004551 |

These are resolution bounds at synthetic event counts. They are not an empirical
power estimate for the study's cohort, whose real independent unit is ten releases.

The held-out comparison on the two simulated processes is likewise gated:

| process | news MAE | network MAE | reduction | verdict |
| --- | --- | --- | --- | --- |
| `shared_news_delay` | 0.010346 | 0.010688 | -0.000342 | Gated: does not beat the baseline |
| `communication` | 0.014745 | 0.010868 | 0.003877 | Gated: reduction is below the 0.005 threshold, and the null assessment does not match this estimator |

On the communication process the network model does reduce held-out MAE, and the
reduction is smaller than the prespecified 0.005 threshold. The promotion record
marks `meets_minimum_mae_gain: false` and `null_assessment_matches_and_ok: false`,
with the note that without a matching assessment the candidate is gated even when it
beats the baseline, because a predictive edge over the shared-news model is not
evidence of propagation. That reasoning is the point of the gate, and the gate holds
on synthetic data where the mechanism is known.

A separate response-slope power assessment is recorded and is deliberately not
reused for the forecast gate. Its thresholds and rates are in slope units for a
different estimand, and its residual scale is calibrated from the named synthetic
process rather than from observed data. The record states that this rate bounds the
study's resolution at the stated event count and is not an empirical power estimate.

## Coherence diagnostic

The payoff-coherence diagnostic computes the minimum maximum distance from the
coherent simplex image to the quoted box, through a linear program over simplex
weights. It is the diagnostic that distinguishes genuine joint-price inconsistency
from a charting artifact, and the plan requires the comparison against a
moment-of-midpoint diagnostic rather than substituting projected prices for raw
observations.

The implementation is checked on three constructed boxes, each labelled
`input_class: constructed_illustrative`:

| example | bids | asks | box distance | midpoint distance | box feasible |
| --- | --- | --- | --- | --- | --- |
| `feasible_box` | 0.4, 0.2 | 0.6, 0.4 | 0.0 | 0.0 | true |
| `feasible_inconsistent_midpoints` | 0.48, 0.52, 0.44 | 0.52, 0.56, 0.48 | 0.0 | 0.02 | true |
| `infeasible_box` | 0.2, 0.7 | 0.3, 0.8 | 0.2 | 0.25 | false |

The two cases that disagree with their midpoints are the ones the diagnostic exists
to separate. `feasible_inconsistent_midpoints` has a feasible quoted box, and its
midpoints alone would suggest an inconsistency of 0.02 that the box does not have.
`infeasible_box` has a box distance of 0.2 against a midpoint distance of 0.25.
Reporting a midpoint distance as the inconsistency would overstate the first case
and misstate the second, which is why the plan requires the bid-ask-aware distance
rather than projected prices or raw midpoints.

The solver reports a successful optimal termination on each case. These are
constructed inputs on a synthetic run, so they exercise the diagnostic's
implementation and do not measure any real market's coherence.

The diagnostic's planned use, coherence recovery around a release compared against a
quote-age and spread baseline, is not run. It needs real synchronized quotes from
eligible logical families, and no contract in the real cohort reached study
eligibility.

## Gates

| gate | name | status | evidence class |
| --- | --- | --- | --- |
| G0 | `cohort_and_access` | Blocked | Real public acquisition audit referenced |
| G1 | `replay_and_availability` | Ok | Synthetic software experiment |
| G2 | `measurement` | Ok | Synthetic software experiment |
| G3 | `falsification_and_power` | Blocked | Synthetic software experiment |
| G4 | `held_out_gain` | Blocked | Synthetic software experiment |
| G5 | `robustness_and_replication` | Blocked | None |
| G6 | `claims_and_package` | Ok | Synthetic software experiment |

G0 is blocked because the real acquisition audit at
`data/public/final-audit/coverage.json` records `partial` with `complete: false` and
establishes no eligible empirical cohort. That audit reached the public venue
read-only and read its first releases from the explicitly named archived BLS
dataset, and it reports zero access blockers and zero refused candidate records. G1 and G2 record
software properties: two real replay folds over the same archived bytes disagree as
reported, and the sealed panels are counted through the storage query layer with
masked rows carrying a reason and a null measurement. G3 and G4 are blocked on the
synthetic findings above. G5 records that endpoint and leave-one-release-out
sensitivities are reported but no independent replication cohort exists and no
verified cross-venue equivalent pair is present. G6 records that the generated
reports are produced from observed values in their own output directory and that
every claim is labelled synthetic.

## The unmet prerequisite

The empirical estimation this plan describes is blocked on one prerequisite: a
verified rule-version record per contract, naming the contract, matching the fetched
rule hash, and covering the release instant, together with settlement semantics read
from a named source by a stated method.

Without it, 400 lifecycle-eligible contracts remain at 0 study-eligible, no
post-release quote response is measurable for the specified cohort, and no
conditional propagation or coherence-recovery result can be produced. The gate
detail names both blocked claims directly: no payoff-equivalence claim between a
release contract and a policy contract, and no statement that a policy contract's
payoff is the release outcome.

Three further prerequisites follow from the same audit and are needed for a full
result rather than for any result at all. A point-in-time expectation source is
required before a surprise slope is estimated, and its absence does not by itself
bar the event-timing study. An unbounded or otherwise representative candidate set
is required before a coverage count can be presented as the universe; the
`candidate_selection_unbounded` gate is unsatisfied for all ten events. And an
independent replication cohort is required before H5 is evaluated; cross-venue
matching is recorded as unverified.

The primary audit queries all four configured policy series, and its
`release_source.json` records `network_release_requests_issued: false` with
`fallback_to_network_used: false`. The earlier offline replay narrowed its own query
to two of the four and recorded that as `override_is_narrowing_only: true`; that
narrowing is a limit of the archived store and not a property of the primary audit.

## Limitations

Release-count resolution. Ten events bound the precision of any release-level
statement. The independent unit is the release, so more contracts do not add
independent information, and the specification prohibits treating them as if they
did.

Observation limits. Candles are available at 1, 60 and 1440 minutes, so no finer
resolution than one minute exists. A candle close is a frequency observation and not
order-book depth, and it carries no intra-candle timestamp. The public order-book
snapshot carries no sequence number, so it cannot close a sequence gap. Of 200
candle audits across the ten events, 87 contain interior holes and 177 do not span
the requested window.

Thin trade activity. Trade counts inside the measurement window are near zero for
the audited policy contracts. A quote can update without a trade, so a zero count is
not evidence of no activity, and an unchanged candle is not evidence of no news.

Measurement-induced transmission. Two observation processes over one latent price
produce apparent lead-lag with no communication. The synthetic null exists to
bound that failure, and it is the reason a held-out forecast gain is not read as
propagation.

Synthetic mechanisms. The simulated processes are declared, matched across the null
and recovery frames by shared seed, and never read from truth-only columns as
predictors. Their rates bound the machinery's resolution and say nothing about a real
market.

Replication. None exists. No verified cross-venue equivalent pair is present, and a
failed rule match would block cross-venue pooling.

Novelty. This draft claims no priority. The literature matrix records the closest
predecessors and their overlap, including a Federal Reserve working paper on macro
markets and a working paper on information and noise transmission to traditional
assets, and it records the retrieval status of each source honestly, including the
sources that could not be read in full. A contribution is proposed in
`prediction_market_information_diffusion_plan.md` and is not a verified claim.

Scope. This project studies information aggregation. It uses read-only public data,
holds no funded account, places no order, and spends nothing. Success is a research
quality criterion. Nothing here supports a profitability or commercial-feasibility
claim, and any execution or commercial work would need its own eligibility, legal,
data-license, and transaction-cost review.

## Reproduction and evidence map

The reproduction is deterministic and offline. It rebuilds from the packaged fixture
and the seeded simulator, and it reports blocking separately in its own gates.

| document or artifact | what it holds |
| --- | --- |
| `reports/reproduction_guide.md` | Install, the synthetic rebuild, hash and quality checks, the offline release import, the audit and event card, and what a clean non-editable rebuild verifies |
| `reports/data_audit.md` | The ten-release cohort, the two real audits and their provenance, coverage, gates and missing rule evidence |
| `reports/data_card.md` | The inputs, their provenance chain, and what each cannot support |
| `reports/preregistration.md` | The locally frozen specification and its freeze-time status |
| `reports/literature_matrix.csv` | The 19 sources with venue, identification strategy, novelty overlap, retrieval status and source URLs |
| `data/public/final-audit/verification.json` | The primary audit's own run summary |
| `data/public/final-audit/release_source.json` | The sealed release dataset and its ten verified release records |
| `data/public/delivery-audit/replay-verification.json` | The earlier offline archived-response replay summary |
| `.audit/falsification-result.json` | The 40-seed, 120-event synthetic falsification audit |
| `.audit/power-grid.json` | The 240-event and 480-event synthetic grid |
| `data/synthetic/reproduction/` | The generated synthetic run: panels, metrics, manifest, reports and figures |

Source URLs for the literature and for the official release calendars are in
`reports/literature_matrix.csv` and in the per-event card fields, not restated here.
The specification names `reports/literature_matrix.csv` as its literature records
and `reports/contract_rule_registry.json` as its rule registry.

## What a passing build establishes

A complete synthetic reproduction shows that the software and the methods run end to
end from the packaged fixture. It is not evidence about any real release, venue, or
contract. A passing test suite validates the properties it tests, and it establishes
neither economic identification nor profitability.

The empirical project reaches the plan's completion criterion when the prespecified
cohort, estimators, null tests, held-out evaluation, uncertainty analysis and
reproduction package support a classified result: evidence for conditional
propagation, evidence excluding effects above the prespecified relevant size, or a
precise inconclusive or identification-limited finding. At this revision the third
of those is the standing result, and the unmet prerequisite above is what blocks the
first two.
