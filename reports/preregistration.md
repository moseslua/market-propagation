# Preregistration

Locally frozen analysis specification for the market-propagation study.

## Status of this document

This specification is a locally frozen implementation specification, fixed on
2026-09-13 within the implementation workstream. It is not an externally registered
protocol. This artifact does not claim human review, sign-off, or approval, and it does
not claim registration with any external registry, preregistration service, or
institutional review body. The phrase
"preregistration" here names the local freeze of the analysis choices, not a registered
protocol. The frozen artifact is `configs/study_v1.yaml`, with
`configs/event_windows.yaml` and `configs/cohort.yaml` supplying the window rules and the
event cohort.

The specification was frozen before any locked empirical evaluation. At the freeze time,
no empirical result existed behind this document. No market panel had been built, no model
had been fit on market data, no coverage audit had completed, and no power calculation had
been run. Every number below is a design choice or an eligibility threshold. None of them
is an estimate.

That statement is anchored to the freeze time and is not revised by later work. Synthetic
software experiments run afterwards exercise code paths on generated data. They are not
empirical results about the events in this study. They can satisfy software and simulator
gates, whose required evidence is generated data. They can never satisfy a gate whose
required evidence is real data, a real empirical effect estimate, or replication, and they
do not make the freeze-time statement false or incomplete. The empirical status of the
study remains unestimated until real data produce real estimates.

A change to any choice below after this freeze is exploratory. Changing the
specification creates a new local version and consumes a new test cohort.

## Question and claim hierarchy

The question is which parts of a prediction market response to a public information
release reflect direct updating, transmission between contracts, and mechanical delay in
observation or quoting.

Three evidence levels stay distinct. Descriptive statements report how quotes, flow, and
dispersion change around a release. Predictive statements report whether one contract's
past information improves an out-of-sample forecast of another. Causal statements
require an intervention or an explicitly defended natural experiment. This specification
fixes descriptive and predictive claims only. A positive H2 result is not read as causal
transmission, and no coefficient is called a propagator.

A negative finding is a valid outcome. If apparent propagation disappears once observation
clocks are aligned, that is the result.

## Hypotheses, cohorts, estimators, thresholds, and failure readings

Each hypothesis below has one primary outcome. Everything else is secondary or
exploratory. The smallest scientifically relevant effect is fixed before fitting, at
0.01 absolute probability units for a response and 0.005 absolute probability units for an
MAE gain. Both thresholds are expressed on the 0 to 1 probability scale used in the panel,
so 0.01 equals 1 percentage point and 0.005 equals 0.5 percentage points. They are not
percentages, not values on a 0 to 100 scale, and not relative or standardized effect sizes.
The stored numbers are read as they are and are never rescaled. Both numbers are
prespecified design choices. They require a power analysis before they can be called
defensible, and that analysis has not been run.

| Hypothesis | Primary outcome | Cohort | Estimator | Threshold | Uncertainty | Failure reading |
| --- | --- | --- | --- | --- | --- | --- |
| H1. Responses are heterogeneous in speed and shape | Response curve across 1, 5, 15, 30, and 60 minutes | Downstream unresolved | Local projections pooled by release family, clustered by release date | 0.01 absolute probability units (1 percentage point) | Cluster bootstrap with simultaneous bands over the curve | A flat or unstable curve is reported as unresolved within the window, not as a precise assimilation half-life |
| H2. Other contracts add predictive information | Midpoint change from one minute after the release to six minutes after it | Downstream unresolved | Nested held-out comparison of the network model against the news model | 0.005 absolute probability units of MAE gain (0.5 percentage points) | Event level cluster bootstrap | No held-out gain blocks model promotion and blocks any propagation claim |
| H3. Some apparent transmission is measurement induced | Apparent lead-lag under synchronization and under the synthetic null | Downstream unresolved | Source-time replay against usable-time replay plus a common-news null | 0.01 absolute probability units (1 percentage point) | Replay disagreement report plus null false-positive rate | Shrinkage below the relevant size reframes the effect as measurement induced |
| H4. Rule-related contracts regain consistency after news | Bid-ask-aware coherence distance before and after the release | Logical payoff families | Minimum maximum distance to the quoted box | Recovery measured against the quote-age and spread baseline | Cluster bootstrap of whole-release synchronized coherence curves, resampled by economic release | No recovery beyond the asynchrony baseline leaves the inconsistency unexplained |
| H5. Results transfer across venues or later regimes | Separately evaluated replication cohort result | Replication cohort | Independent replication of the primary estimator | 0.005 absolute probability units of MAE gain (0.5 percentage points) | Replication cohort cluster bootstrap | A failed rule match blocks cross-venue pooling and leaves H5 unevaluated |

The primary historical outcome is the five minute midpoint change. It is one outcome, not
the smallest p-value among five horizons. The other horizons are part of the same
prespecified curve, and the curve is reported with simultaneous uncertainty.

H4 uncertainty resamples whole economic releases. Each release contributes one
synchronized coherence curve, the bid-ask-aware coherence distance trajectory across the
prespecified horizons built from quotes that are synchronized and valid at the same
instant. The bootstrap draws releases and keeps each drawn release's curve intact, so the
release-level synchronization structure and the dependence across horizons are preserved.
Individual quote snapshots, rows, and contracts inside a release are dependent observations
of that one release, so resampling them at the snapshot level is pseudo replication. It
inflates apparent precision, and it would violate this specification.

Surprise slopes and the timing study are gated differently. A pre-release expectation
source is required only before a surprise slope or a consensus-surprise-conditional
response is estimated. If no such source exists for an event, that surprise slope is
dropped and no independent consensus claim is made. The event-timing study is unaffected.
Its outcomes, the response curve, the coherence trajectory, and the replay comparisons, are
defined from the scheduled release time and observed quotes alone and need no expectation
source. An absent expectation source never makes a release ineligible, never removes an
event from the cohort, and never blocks the descriptive analysis. Expectation values are
never synthesized to fill the gap, and a late or revised figure is never treated as a
point-in-time expectation.

## Cohort

The cohort is the ten monthly releases in `configs/cohort.yaml`, five Consumer Price
Index releases and five Employment Situation releases, January through May 2025. CPI and
employment are estimated separately and are never pooled into one shock.

Every date comes from the official BLS release calendar for its month, read on
2026-09-13. Each calendar states that all times shown are Eastern Time. Every release
time is 08:30 ET. Every event carries an aware `scheduled_at` value in the source
calendar timezone and the same instant after `America/New_York` conversion, which the
IANA timezone database resolves to 13:30 UTC before the March 2025 daylight saving change
and 12:30 UTC afterwards. Each event also carries the archived initial-release URL, and
each archived page was read to confirm that its reference period matches the cohort
entry.

Three contract cohorts are defined. Downstream unresolved contracts are primary, meaning
their payoff depends on an event later than the release. Direct-resolution contracts, whose
payoff the release economically determines, are secondary and are reserved for a
settlement and assimilation study. Matched controls require a written argument of small
exposure and similar pre-release activity before they are used.

Peer feasibility observation, recorded on 2026-09-13 and treated as feasibility evidence
rather than a result, is that sampled direct contracts on the observed venue closed
before the release time, at 08:25 ET for the CPI series and 08:29 ET for the payrolls
series. Because a closed book has no post-release quote to measure, direct-resolution
contracts are excluded from the post-release quote response outcome of this study. Their
use is limited to the pre-release diagnostic window and to the separate settlement study.
This resolves a measurement question only. It says nothing about the size of any response.

No eligible market identifier is recorded in the cohort file. Verified matches are empty
by design rather than left unstated, so that absence of evidence is not readable as
evidence of an eligible market. Candidate discovery and every endpoint or market
determination remain the responsibility of the ingestion workstream.

## Point-in-time discipline

Source time is never usable time. A historical record without a receipt timestamp cannot
support a latency claim and is given an availability interval or a documented coarse-time
reading instead of invented precision.

Every record carries source time, receipt time, a monotonic local clock value where one
exists, usable time, clock quality, and a stable deduplication key. A content hash alone
does not collapse genuinely repeated identical events when the feed supplies no unique
identifier.

Two replay orders are computed, source-time order for the economic panel and
usable-time order for the forecast panel. Disagreements between them are published rather
than resolved by picking the more attractive order. Forecast features admit only records
whose usable time is at or before the prediction time. Historical economic panels admit
source time and retain explicit clock-quality and uncertainty columns.

## Quote validity and the observation model

A quote is valid, awaiting a snapshot, in a gap, disconnected, halted, closed, crossed, or
missing. Midpoint and spread are defined only for a valid two-sided book and are
undefined otherwise. A sequence gap invalidates the reconstruction until a fresh snapshot
restores it, and an invalid quote cannot be reused across a gap. Staleness is assessed
from gap detection and documented refresh behaviour, with `last_verified` as the freshness
field. Age since the last price change is not a staleness proxy, because an unchanged
standing quote is not automatically stale.

The baseline is the last valid two-sided quote strictly before the event time, with its
age retained. A stale last trade is never carried across the release and presented as a
current quote.

## Event windows and contamination

The main window is a 30 minute pre-event diagnostic interval and a 60 minute post-event
interval, with the 1, 5, 15, 30, and 60 minute horizons nested inside it. Wider windows
are sensitivity analyses only.

A window stops at the first of a market close, a sequence gap, a halt, a disconnect, a
coincident scheduled release, or a quality boundary. A stopped window is truncated
without fill. A closed market is never filled forward so that instant assimilation can be
inferred. Coincident scheduled releases are jointly coded, explicitly excluded, or
treated as one combined package. Contamination rules are frozen before outcomes are
inspected, and an inclusion and exclusion ledger with reasons is retained.

Post-event liquidity is a plausible mediator. It is not controlled for while the estimate
is called a total shock effect. Pre-event liquidity is used for heterogeneity, and
post-event flow is modelled separately as a mechanism.

## Quality and endpoint gates

| Gate | Required evidence | Failure action |
| --- | --- | --- |
| G0 cohort and access | An eligible cohort with observed frequency and permissible read access, including inactive contracts, with a written coverage report | Narrow the estimand, or restrict claims to the documented data resolution |
| G1 replay and availability | Replay and availability tests pass | Block historical latency claims |
| G2 measurement | The frozen event panel passes the visual and adversarial audit | Block model promotion |
| G3 falsification and power | The synthetic null produces no false discovery at the chosen resolution | Block network interpretation |
| G4 held-out gain | Held-out gain exists beyond the common-news and own-market baseline | Keep the model at baseline level |
| G5 robustness and replication | No dependence on a single event, clock, or rule error | Report the single-sample limit |
| G6 claims and package | Claims match evidence and stated uncertainty | Publish a feasibility report instead of an economic conclusion |

G0 is not satisfied. Whether each endpoint supports ticker and time filters, pagination,
and complete historical coverage is unknown until the ingestion workstream reports.
Moving live and historical cutoffs mean coverage must be re-verified against the cutoff in
force at the time of extraction rather than assumed from a past reading.

## Training-only transformations and locked-test protection

Splits are chronological and allocate whole events, never individual rows. The
proportions are 60 percent training, 20 percent validation, and 20 percent locked test, in
that order, with the actual cutoff recorded per row. Every contract tied to one release,
including cross-venue equivalents, stays in the same split. Unassigned split and cutoff
values are null until evaluation assigns a whole release. They are never filled with a
default.

Purging is driven by the actual horizon and label availability. A label whose
availability is null or later than the split cutoff is unavailable regardless of venue
settlement, and overlapping forecast and label windows are purged. A ceremonial fixed
embargo is prohibited, because it does not track the real overlap.

Feature scaling, the shock scaling matrix, graph edges, calibration maps, lag length,
regularization strength, and every hyperparameter are fitted inside the training period
only. The following are prohibited. Full-history scaling, future graph construction,
interpolated future quotes, revised macro values substituted for initial releases,
market selection on post-event volume, and participant labels built from later outcomes
and used as real-time features.

The locked test is evaluated once per locally frozen version of this specification. The
consumed event identifiers are recorded in the local experiment registry so that a later
specification version cannot reuse the same locked-test cohort. Reusing that cohort is
treated as a specification failure, not as a robustness check.

## Estimators and uncertainty

Local projections at each prespecified horizon are the initial response estimator, pooled
within release family. Threshold contracts from one release are dependent observations,
so uncertainty is clustered by release date, with finite-sample sensitivity and broad
intervals reported where clusters are few.

Sparse conditional propagation is the stage two estimator. The baseline includes the same
flexible direct-response and observation-delay structure as the network model, because
otherwise another contract can proxy an omitted delayed direct response. The target
contract's contemporaneous price is present in every model. Prediction timestamps and
information cutoffs are fixed. Missing feature rows cannot change the comparison sample
across nested models.

Raw probability-point responses are the baseline estimand. Every response, target, and loss
value in this study is measured in absolute probability units on the 0 to 1 scale, so a
reported 0.01 is 1 percentage point and a reported 0.005 is 0.5 percentage points. Values
are read and reported as stored, never rescaled to a 0 to 100 scale. Probability forecasts
use a documented bounded link fitted inside training, clipping is reported, and any interior
log-odds cohort is documented with its sensitivity. Stable short-run increment dynamics
do not imply that event probabilities revert to one half, and no positive mass-conserving
graph Laplacian is imposed.

Payoff coherence uses the minimum maximum distance from the quoted box to the coherent
state price set, under scipy's linear programming solver with simplex weights. Only
synchronized valid quotes enter the diagnostic. Projected coherent prices are reported
separately from raw observations, because a projection enforces coherence and therefore
cannot itself demonstrate market coherence. Infeasibility is a quote-coherence finding
under stated assumptions, not automatically an executable arbitrage.

Uncertainty over the whole response curve is reported with prespecified contrasts, plus
event-level cluster bootstrap and leave-one-event-out sensitivity. The coherence trajectory
is bootstrapped the same way, by resampling whole releases and keeping each release's
synchronized curve intact; snapshots and rows inside a release are never resampled as if
independent. Inferential status is event-clustered.

## Power and interpretation

The independent unit is the release, not the message, and not the quote snapshot. Ten
events is a pilot cohort, not a sample that guarantees power. After the pilot, event-level
residual variance and dependence are estimated, and cluster-aware simulation estimates
power and false-positive rates at the available event count. No universal event-count rule
is invoked.

No power calculation had been run at the freeze time, and no power or model result is
claimed until a real estimation run produces one. Synthetic simulation checks software
behaviour on generated data. They are not empirical evidence about these releases, and a
passing synthetic check does not satisfy G3 for a claim about market behaviour.

If the data cannot separate zero from the smallest relevant effect, the result is
reported as inconclusive. If the interval excludes effects above that threshold, an
informative bound is reported instead of a bare non-significant p-value. Model
complexity is collapsed when independent events are scarce.

## Interpretation limits and integrity constraints

No causal claim is made without a separate identification design. No independent
consensus claim is made without a demonstrably pre-release expectation source. No exact
cross-venue pooling is performed without a verified rule match. A half or partial payout
is never relabelled as a binary resolution.

A quote is not identified with an objective conditional probability. Calibration,
efficiency, and commercial usefulness are not equated.

The study is read-only. It places no orders, holds no funded accounts, and performs no
market interventions. Paid spending is capped at zero. Access restrictions are not
circumvented, and no credential enters the raw archive. Wallet-level analysis, if it
happens at all, stays optional and secondary, and no participant label from later outcomes
becomes a real-time feature.

## Open data unknowns

These are unresolved facts that this document does not assume either way.

Which contracts were open, valid, and two-sided at each of the ten event times.
Observed publication times from the archived payloads, as distinct from scheduled times.
The intraday spacing actually returned by the historical endpoints.
Whether any point-in-time expectation source exists for these ten events.
Whether any exact cross-venue match passes the rule checklist.
What the moving live and historical cutoffs were at the relevant extraction time.

Each unknown is a required input to a named gate above. None of them is treated as
satisfied.
