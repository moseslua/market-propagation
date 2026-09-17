# Preregistration for the four declared studies

Locally frozen design specification for studies A, B, C and D. Frozen on
2026-09-16, before any result from any of the four was read.

This document explains the design spine the four study files share.
`configs/studies/study_a_absorption.yaml`,
`configs/studies/study_b_crossvenue.yaml`,
`configs/studies/study_c_crossmeeting.yaml` and
`configs/studies/study_d_perp.yaml` are the declarations; this file
states the conventions they inherit in common, the tiers a claim can occupy, how a
claim is promoted between them, and which studies can run today.

Like `reports/preregistration.md` and `reports/preregistration_v2.md`, this is a
locally frozen implementation specification. It is not an externally registered
protocol. It does not claim human review, sign-off or approval, and it claims no
registration with any registry, preregistration service or review body. The phrase
"preregistration" here names the local freeze of the analysis choices.

Nothing in this document is an estimate. Every number is a measured count or a
declared design choice, and each is labelled as one where it appears.

## 1. Why four studies rather than one

The programme's question is which parts of a prediction-market response to a public
information release reflect direct updating, transmission between contracts, and
mechanical delay in observation or quoting. That question has four different
estimation problems inside it, and each has its own unit, its own control group and
its own failure mode. Pooling them would hide which one failed.

| Study | Unit | Question | Estimand |
| --- | --- | --- | --- |
| A. Release absorption | contract x release | How fast does a release enter its own market? | `p(last trade ≤ release+h) − p(last trade < release)` |
| B. Cross-venue propagation | matched event x venue pair | Does discovery on one venue predict repricing on the other? | held-out gain from the leader's lagged change |
| C. Cross-meeting propagation | strike x meeting pair x release | Does a nearer contract's shock reach same-strike later contracts? | `MAE_news − MAE_network` over admissible edges |
| D. Perp arbitrage and absorption | asset x venue pair x time | When do quoted cross-venue differentials survive costs, how much can they carry, how fast are they competed away? | spread distribution and build-to-build decay |

The four units are genuinely different, and none is a special case of another. A is
one contract and one instant. C is two contracts at two meetings. B is two contracts
at two venues. D has no contract and no release at all.

## 1.1 The two cohort arms, and which study they reach

The study population has two arms, declared in `configs/cohort_v2.yaml` under `arms:`
and never pooled. This is a subsection of section 1 rather than a new numbered section
so that no existing section number or cross-reference in this document moves.

| Arm | File | Cohort id | Releases | What it is |
| --- | --- | --- | --- | --- |
| Retrospective | `configs/cohort.yaml` | `core_2025h1` | 10 | the frozen 2025 cohort every result so far is measured on |
| Forward | `configs/cohort_forward.yaml` | `forward_2026h2` | 6, growing | the prospective arm, extended by a declared calendar rule |

**Each study file declares its own relation to the arms** in a top-level
`cohort_arms:` block that names the arm files rather than copying them. Nothing in any
such block changes a claim the retrospective arm already makes, and the claims section
of this document names the arm each claim is measured on.

| Study | Retrospective arm | Forward arm |
| --- | --- | --- |
| A. Release absorption | the arm it is measured on; blocked on the rule-vintage record | the same design applies unchanged; not estimable today because no release in it has published |
| B. Cross-venue propagation | blocked on a missing matched instrument | the design applies unchanged; still needs a verified match, which this arm does not supply |
| C. Cross-meeting propagation | blocked on the rule-vintage record; this is the arm whose refusal is the measured finding | the arm its windows can be certified on, because a capture opens its interval at the instant the serving system states, before the release it certifies |
| D. Perp arbitrage and absorption | reads no release cohort at all | reads no release cohort at all; its blockers are a one-build series and an unobservable cost layer, which forward release instants do not touch |

Three rules keep the arms from merging silently.

1. **A result names its arm.** Every figure computed on one arm is reported with that
   arm's cohort id. A count from one arm is never reported as a count for the other,
   and the two are never pooled into one estimate or one denominator.
2. **The retrospective arm stays the reported blocked arm.** It is retained, it is not
   deleted, and it is not rewritten. A forward release publishing does not change a
   single count measured on it. In particular, a rule capture taken in 2026 cannot
   certify a window that closed in 2025, so the retrospective arm's refusal is a
   property of the archive rather than a gap that time closes.
3. **An unblocked forward arm does not unblock the retrospective arm's claim.** The
   two blockers are different kinds: one is a set of missing observations for windows
   already past, the other is a cadence that has to run before windows yet to come. The
   forward arm is the demonstration that the rule-vintage requirement can be met; it is
   not evidence that it was met for the 2025 releases.

The forward arm's extension rule, its peeking hazard and the reason its release rows
cannot exist until each release publishes are stated in
`reports/preregistration_v2.md` section 2.2, with the rule itself declared in
`configs/cohort_forward.yaml`.

## 2. The event-time convention

Every study that has a release uses one convention, inherited from the v2 freeze
rather than re-chosen:

| Element | Value | Authority |
| --- | --- | --- |
| Clock mode | `source` | `configs/study_v2.yaml` |
| Basis | venue-recorded transaction time | `configs/study_v2.yaml` |
| Reference point | scheduled release time | `configs/event_windows_v2.yaml` |
| Forecast origin `tau` | `release_time + 300 s` | `configs/study_v2.yaml` |
| Lag guard `L` | `60 s` | `configs/study_v2.yaml` |
| Future horizon `H` | `300 s` | `configs/study_v2.yaml` |
| Declared horizons | 60, 300, 900, 1800, 3600 s | `configs/event_windows_v2.yaml` |
| Primary horizon | 300 s | `configs/study_v2.yaml` |
| Observation caps | 120 s anchor, target and donor | `configs/study_v2.yaml` |
| Timezone | `America/New_York` source, UTC storage | `configs/event_windows_v2.yaml` |

Three consequences follow, and each is load-bearing.

**The clock is retrospective, and that is a limitation rather than a detail.** An
archived transaction carries no receipt. `usable_time` is null on every row of the
sealed release dataset, so `usable` is unidentifiable and source alignment is the
only defensible mode. No result in this programme is a claim about what a live
participant knew at the instant the venue recorded a print.

**A forecast origin is not an arbitrary offset.** `tau = release_time + 300 s` is the
instant the donor is read to and the target opens from. `L = 60 s` stops the donor's
read before the target's window opens, so the same prints never inform both sides of
a comparison. The three instants — donor close, origin, target close — define the
design, and changing any of them changes what is being measured.

**Every horizon is one curve.** Five horizons are declared and reported together with
simultaneous bands. The primary horizon is one pre-declared member of that curve,
never the smallest p-value among five.

Study D has no release and therefore no forecast origin. It uses the source's own
build stamp instead, and its horizons are the declared collection cadences and the
documented funding schedules. That is a genuinely different time convention, and the
study file states it rather than contorting D onto A's clock.

## 3. Price construction, and the prohibition on mixing paths

### 3.1 The transaction path

Studies A and C build their prices from **executed prints**. The rule is:

> `p_i(t)` is the venue-recorded transaction price of the **last valid trade at or
> before** `t` on the contract's declared event axis, read only when that print is
> inside the declared age cap.

Four clauses of that sentence are enforced structurally in
`src/market_propagation/trade_panel.py` rather than left to a reader's discipline:

* The baseline print is the last trade **strictly before** `release_time`; the
  endpoint is the last trade **at or before** `release_time + h`. A row therefore
  needs `s_plus > tau_e`, so a contract that never traded again is
  `no_post_release_trade` rather than a zero produced by holding the baseline.
* **A carried anchor is never an endpoint.** `fill_forward_forbidden` and
  `carry_anchor_into_target_forbidden` are declared in
  `configs/event_windows_v2.yaml`. A genuine observed zero needs two distinct valid
  prints at the same price, which is a different fact and stays visible as one.
* **A missing observation is null, never zero.** `missing_is_null: true` and
  `observed_zero_is_evidence: true` are declared together, because the two states are
  different facts and merging them reports absent data as a measured absence of
  movement.
* Every mask is written **in place** with its reason. A row is never dropped, because
  missingness measured against the declared denominator is itself an outcome.

### 3.2 The quote path

A quote is not a trade. A candle close carries a quote and neither an intra-candle
timestamp nor a depth level. `src/market_propagation/trade_panel.py` declares **no**
bid, ask, spread or depth column at all, on the grounds that a column which could
only ever be null is worse than an absent one.

### 3.3 The prohibition

> **A quantity measured on the trade path is never combined with, calibrated
> against, or reported as a substitute for a quantity measured on the quote path.**

The reasoning is specific. The two paths have different failure modes: a trade tape
records executions and is silent about standing liquidity, while a quote series
records intentions that may never execute. Mixing them lets a quoted move stand in for
a realised one, which is exactly the error that would make an unexecutable price
appear tradeable. The consequences in force across these four studies:

* A response built on prints is never supplemented with a quote to reach a horizon.
* An infeasible quoted box is a **quote-coherence finding under stated assumptions**,
  never an executable arbitrage. `src/market_propagation/coherence.py` says so in its
  own module docstring: fees, inventory constraints, finite size, differing cashflows
  and non-simultaneous execution are all outside the calculation.
* Study D's differentials are quoted spreads, and every one carries
  `execution_cost_not_observable_from_this_source`. The cost layer is a third thing
  again, and its absence is recorded rather than filled.

### 3.4 Study B's added constraint

Study B spans two venues with **different time axes**: Kalshi records exchange
transaction time, Polymarket records blockchain inclusion time. Unifying them is a
declared transform, not an assumption that the axes carry the same meaning. Neither
archive carries a synchronization certificate, so no sub-second ordering claim between
the two venues is supported, and study B's file says so.

## 4. Evidence tiers

Four tiers, and they are ordered. A study's permitted tier is declared in its own
`status` block; it is never inferred from a result.

| Tier | What it asserts | What it requires |
| --- | --- | --- |
| **Descriptive** | how prices, flow and dispersion changed around an event, on a stated denominator | a measured observation and its refusal accounting. No model, no counterfactual |
| **Predictive** | one quantity's past improves an out-of-sample forecast of another, beyond a complete declared baseline | a nested comparison on one identical sample, a held-out gain, and release-clustered uncertainty on that gain |
| **Mechanism-consistent** | the pattern is what a named mechanism implies, and the plausible mechanical alternatives are excluded | the predictive tier, plus the falsification design of section 5 passing, plus the alternative mechanisms tested and reported rather than asserted away |
| **Causal** | the release, or a neighbour's move, **caused** the change in the other quantity | an intervention or an explicitly defended natural experiment |

### 4.1 What promotes a claim

| From | To | Requirement |
| --- | --- | --- |
| — | descriptive | the observation exists, sits on the declared denominator, and its masks carry named reasons. Nothing more |
| descriptive | predictive | a nested held-out comparison where the candidate term earns its place beyond the complete baseline, with a one-sided 95 % release-clustered lower bound above zero |
| predictive | "the effect exceeds the smallest relevant size" | the **same** bound additionally above the declared threshold; the point estimate never suffices |
| predictive | mechanism-consistent | the falsification design of section 5 passes, **and** each named alternative mechanism is tested and reported. A shared news shock, a mechanical partition and a measurement artefact are all live alternatives and each must be excluded rather than assumed away |
| mechanism-consistent | causal | an intervention or a defended natural experiment. **No amount of predictive evidence reaches this tier**, and no study file in this programme declares it reachable by more data of the same kind |

Three rules keep the ladder honest:

1. **A negative finding is a valid outcome.** If apparent propagation disappears once
   observation clocks are aligned, that is the result and it is reported as one.
2. **A blocked rung is reported as blocked with the column named**, never fitted on a
   substitute. The forecast panel's four rungs are today reported blocked on named
   columns (`shock`, `delayed_shock`, `neighbor_lag`, `neighbor_lag_control`), and a
   column that exists and is null on every row is reported as *unsupplied* rather than
   fitted as a constant.
3. **Inconclusive is a distinct verdict from pass and from fail.** A result that
   cannot separate zero from the smallest relevant effect is reported as such rather
   than rounded to a pass. The rule calibration was reported `inconclusive` while two
   of ten declared nulls emitted no comparison row at any repetition count, because a
   family bound cannot be certified over a declaration part of which contributes
   nothing. Both causes turned out to be defects in the two scenarios' own
   declarations — `spread_only`'s declared zero sensitivity, skipped by the shock
   recovery instead of read as the exact value it is, and `resolution_pause`'s halt
   interval, which covered every primary window — and with both fixed the calibration
   now returns `pass` over all ten estimable nulls.

### 4.2 What each study may occupy today

| Study | Permitted tier today | Why not higher |
| --- | --- | --- |
| A | descriptive, exploratory only | `rule_version_unknown` masks all 3,925 rows; 0 of 785 pairs rule-verified |
| B | descriptive, on unmatched markets only | 0 matched event x venue pairs exist |
| C | none for the primary claim | 0 admissible edges, and the news rung is unestimable |
| D | descriptive, cross-sectional only | one build held; the cost layer is unobservable |

## 5. The placebo and falsification design

A positive result is only informative if the design can fail. Each study declares what
would make it fail, and the falsification work is prespecified rather than ad hoc.

### 5.1 Placebo releases

**Design.** Apply the identical window construction to non-release instants matched on
time of day, session and weekday, and report the same estimand. Matched instants are
drawn from the same contract and the same regime as the real release, so a comparison
is not confounded by the contract simply being more liquid on release days.

**These are not run today.** The paper reports them `not_run: no fitted primary
estimate`, and they stay reported that way rather than being omitted. A placebo that
cannot run is not evidence that the design passes.

**What it falsifies.** If the matched non-release instants produce a comparable
"response", the pattern is a calendar artefact and propagation interpretation is
blocked — exactly as `configs/study_v1.yaml`'s stop-or-pivot rules prescribe.

### 5.2 Synchronization and measurement-artefact nulls

Apparent lead-lag between two contracts can be manufactured by asynchronous clocks, by
stale prints and by the baseline age caps. The declared counter is a source-time replay
against a usable-time replay plus a common-news null, with the same synthetic null that
`src/market_propagation/falsification.py` implements.

**The rule.** Apparent transmission that shrinks below the relevant size once clocks
are aligned is reframed as measurement-induced, and the shrinkage is the result.

### 5.3 The reversed edge

The reversed direction is a **prespecified diagnostic, not a required null**. Feedback
and a shared news shock can produce prediction in either direction, so a reverse edge
is evidence about mechanism rather than a falsification. It is reported either way, and
it is not run today because no edge exists in either direction.

### 5.4 Mechanical dependence is not propagation

Mutually exclusive strikes of one meeting partition a single outcome, so their prices
must sum to one **by construction**. A relation between them is an accounting identity,
not transmission. `configs/neighbor_graph_v2.yaml` declares `mechanical_dependence` and
`payoff_identity` as relations that are named and deliberately **not built**, so no
consumer can read one as the other. Study C carries same-expiry adjacent strikes as a
sensitivity, never as a donor edge.

### 5.5 Rule calibration, and what it does not establish

The promotion rule's operating characteristics were calibrated on simulated transaction
tapes passed through the same graph, observation, feature, fitting, tuning,
paired-uncertainty and promotion code as real data, at 200 repetitions per primary
scenario with simultaneous null bounds.

| Quantity | Value |
| --- | --- |
| Declared null scenarios | 10 |
| Estimable nulls | 10, promoted in 0 of 200 repetitions each |
| Null one-sided upper bound, simultaneous level 0.995 | 0.02614 against a 0.05 ceiling |
| Recovery `communication` | promoted 196 of 200; rate 0.98; one-sided lower bound 0.9548 against a 0.80 target |
| Verdict | `pass` |
| Certificate | `data/calibration/calibration_certificate.json` |

The verdict is a `pass` over all ten declared nulls. Two of them previously produced
no comparison row at all, which blocked 200 of 200 repetitions and left the family
bound uncertifiable; both causes were defects in the scenarios' own declarations, and
both are fixed. `spread_only` declares `news_active=False`, so every contract's
sensitivity is 0, and the shock recovery skipped any event whose strength was falsy
rather than reading a declared zero as the exact value it is; such an event now
receives an explicit `0.0` shock. `resolution_pause` declared a halt of
`(300.0, 900.0)`, which covered every primary window of `[event+300s, event+600s]`;
the declared halt is now `(700.0, 1000.0)`, which opens after the primary window
closes, so it still invalidates every window that spans it without consuming all of
them. With both fixed, all ten nulls are estimable and the family bound certifies.

**This calibrates a decision rule on a synthetic process and nothing else.** It is not
evidence about any real venue, release or contract, and it does not unblock the primary
graph, which still admits no edge. Study D cites it as `not_applicable`: D has no ladder
and no promotion rule, so the certificate does not transfer.

### 5.6 Sensitivity analyses, reported including their absences

| Analysis | State |
| --- | --- |
| Exclusion accounting by release | run; every declared release and pair stays in the denominator |
| Declared-grid enforcement | run; a grid omitting a declared release is refused, exit 1 |
| Altered sealed bytes | run; a one-byte change is rejected by content hash, exit 1 |
| Age-cap sensitivity | **not run**: no valid rows |
| Endpoint and tie envelopes | carried on every masked row, unexploited |
| Reversed-edge diagnostic | **not run**: no edge in either direction |
| Leave-one-release-out | **not run**: no fitted primary estimate |
| Placebo releases matched on time of day | **not run**: no fitted primary estimate |
| Multi-null Bonferroni certificate | run inside the calibration; simultaneous level 0.995 over 10 estimable nulls |
| Transaction observation process | run; the calibration's tapes pass through the declared transaction observation path |
| Study D build-to-build persistence | **not run**: one build held |

## 6. The rule that a primary specification is never changed after seeing results

Each study file declares exactly one primary specification, marked
`immutable_after_results: true`.

**The rule.** Once results have been read, the primary specification does not change.
If it must change, the change creates a new `config_version`, the new version consumes
a new test cohort, and the new version's result is reported as the result of *that*
version. It is never reported as this study's primary result, and the earlier version's
result is never withdrawn from the record.

Four specific consequences:

1. **No threshold reduction after results.** All four files declare
   `no_post_result_threshold_reduction: true`. A design whose smallest relevant effect
   moves after the estimate is seen has no smallest relevant effect.
2. **No primary-horizon switch.** The primary horizon is fixed at 300 s in A, B and C.
   A finding at 900 s is a finding on the declared curve and is reported with the
   curve's simultaneous bands; it never becomes "the primary horizon was 900 s".
3. **Everything else is exploratory and labelled.** Each file declares an
   `exploratory_namespace` whose members carry a `claim_ceiling` stating what they may
   never be promoted into. `configs/study_v1.yaml` fixes the same rule: a label change
   after the freeze makes a claim exploratory.
4. **A blocked primary specification is reported blocked.** A, B and C each declare
   `status: blocked` or `not_runnable_today` with the reason. None is replaced by a
   weaker claim dressed as the same one, which is the failure mode this rule exists to
   prevent.

## 7. Multiplicity and uncertainty, declared per study

| Study | Primary family | Curve correction | Secondary | Exploratory | Clustering | Bootstrap unit |
| --- | --- | --- | --- | --- | --- | --- |
| A | 1 | Bonferroni, 5 horizons | Bonferroni over 2 families (0.975) | BH, q=0.05 | release | whole release |
| B | 1 | Bonferroni, 5 horizons | Bonferroni over 2 families (0.975) | BH, q=0.05 | release | whole release |
| C | 1 | Bonferroni, 5 horizons | Bonferroni over 2 families (0.975) | BH, q=0.05 | release | whole release |
| D | 1 | Bonferroni, 7 cost-grid points | BH over 24 assets; BH over 18547 pairs | BH, q=0.05 | source build | whole build |

Two rules are absolute across all four.

**The bootstrap unit is the independent unit, and never a row.** In A, B and C it is
the whole release; in D it is the whole build. `iid_resampling_of_individual_quote_snapshots`
and `row_level_bootstrap_within_a_release` are prohibited by name. Many contracts from
one release are dependent observations of that one release, and many pairs from one
build share that build's market-wide move; resampling either as independent units is
pseudo replication, it inflates apparent precision, and it violates every one of these
specifications.

**Every primary contrast is reported with its point estimate and a two-sided interval
regardless of the decision.** A bare non-significant p-value is not a result. If the
interval excludes sizes above the declared threshold, that informative bound is the
finding.

## 8. Dependencies: what exists and what does not

Measured on this checkout. `absent` and `zero` are different states and are never
merged: an empty verified-match set means no match was verified, not that no match
exists.

| Dependency | Status | Measured | Blocks |
| --- | --- | --- | --- |
| Declared listing grid | present | 785 declared pairs; 697 never traded; 3,925 panel rows | — |
| Extracted transaction tape | present | 163,795 trades, 16 shards, `bounded: false` | — |
| Readable payout predicate | present | 155 contracts; 534 refused (533 month not dated) | — |
| Declared decision calendar | present (declared, not fetched) | 10 FOMC dates, 2024-11-07 to 2025-12-10 | — |
| Panel rows with both legs observed | partial | 136 of 785 pairs | A |
| **Rule-vintage record** | **absent** | **0 of 785 rule-verified; 0 valid rows** | **A, B, C** |
| Structurally admissible edges | diagnostic only | 623, under a sentinel that is explicitly not evidence | C |
| **Point-in-time expectation source** | **absent** | 9 of 10 releases have a pre-release capture; none is bound as a validated record | A (news rung), B, C |
| Polymarket transaction archive | present | 1,248 shards, 2022-11-21 to 2026-04-28 | — |
| **Verified cross-venue rule match** | **absent** | 0 matches; no per-meeting policy-decision market live at any release | **B** |
| Transaction-tape calibration | measured, pass | 10 nulls at 0/200; recovery 196/200 | — |
| Perp market and funding pages | present | 24 assets, 889 quotes, 47 venues, 18,547 differentials | — |
| **Perp build-to-build series** | **partial** | **1 distinct build held** | **D** |
| **Perp execution-cost layer** | **absent** | refusal on 18,547 of 18,547 differentials | **D** |

### 8.1 The rule-vintage gap, stated exactly

This is the programme's binding constraint and its size is measured.

* `reports/contract_rule_registry.json` is a specification document
  (`status: frozen_local_unregistered`) carrying no per-contract rule record.
* The graph therefore refuses every edge: 785 `rule_vintage_unverified` decisions in
  `.audit/study-v3/graph_decisions.json`. That count is the receiver's own rule check,
  which runs before any donor is sought, so it does not by itself say whether a donor
  exists.
* Sized separately with a **diagnostic sentinel that is explicitly not evidence**:
  **623 edges are structurally admissible** under the declared calendar, predicates,
  liveness and window rules — about 62 per release — and every one is withheld by the
  rule-vintage requirement alone.
* The barrier is not the venue and not the trading session. The historical endpoint
  returns rule text, but its `updated_time` is months after settlement, so retrieved
  text is a current-state record rather than a contemporaneous artifact. That is direct
  evidence for `recorded_open_time_is_not_a_rule_bound`, not merely an assertion of it.
* Inadmissible substitutes, declared in `configs/neighbor_graph_v2.yaml`: settlement
  outcomes, current rule text, a contract's own listing window, and a regulatory filing
  that names no strike, event or market.

### 8.2 The expectation gap, stated exactly

`src/market_propagation/ingest/expectations.py` implements the point-in-time contract
and refuses, with its own code, a forecast published at or after the release, one
scored against a revision, a unit disagreeing with the release's own declared statistic,
an unnamed consensus, a market-implied value, an incomplete news vector, and evidence
bytes whose digest no longer matches.

No source exists to validate, so `load_expectations` raises
`expectation_source_is_absent`. The absence is a **refusal code, not a zero-valued
surprise**. The narrower remaining step is not a licensed feed: it is to bind the nine
existing pre-release captures as validated expectation records and to verify the
employment vector including first-published payroll revisions.

### 8.3 Study D's two blockers are not the other studies' blockers

D is not blocked on rule vintage, and more rule work does not help it. Its rules are
current, its source is read live, and its blockers are a one-build series and an
unobservable cost layer. The two causes are unrelated in both directions: collection
time cannot close the rule gap, and rule evidence cannot produce a second build.

## 9. What each study is permitted to claim

Each study file carries a `claim_limits` block with a `may_claim` and a `may_not_claim`
list. The refusals that recur across all four:

| Refusal | Why it holds everywhere |
| --- | --- |
| Causal transmission | no intervention and no defended natural experiment. The registry's `exposure` relation permits cohort membership and heterogeneity analysis and explicitly does **not** permit a causal label |
| Executable arbitrage or profitability | no book, no depth, no fee layer, no funded account. No order is placed anywhere in this programme and paid spend is capped at zero |
| What a live participant knew | source alignment is retrospective |
| Pooling across venues without a verified match | `reports/contract_rule_registry.json` prohibits it and no match is verified |
| Relabelling a half or partial payout as a binary resolution | declared prohibited |
| Cross-study comparison of incommensurable quantities | an annualised funding fraction and a probability on [0,1] are different quantities and are never pooled or reported side by side |
| A universal event-count rule | a power assessment derives power from the available sample; no universal rule is invoked, and ten releases is a pilot cohort, not a powered sample |

The rule the programme runs on: **a blocked estimand is reported blocked with the
reason and the measured size of the gap, and it is never replaced by a weaker claim
wearing the same name.**

## 10. Runnability today

| Study | Runs today | Runnable as declared | Blocker | Measured size of the gap | Would unblock |
| --- | --- | --- | --- | --- | --- |
| **A. Release absorption** | Exploratory descriptive only | No | `rule_version_unknown` masks every row | 0 of 785 pairs rule-verified; 0 of 3,925 rows valid; the exploratory fit is degenerate (cpi: 0 design rows; employment: 21 rows, 1 held-out release) | A per-contract rule-vintage record |
| **B. Cross-venue propagation** | Descriptive only, on unmatched markets | No | No matched event x venue pair is live at a declared release instant | 0 of 10 releases matched; the archive itself is present (1,248 shards, intraday rows in all 10 release windows) | A per-meeting policy-decision market on the second venue, **and** the rule-vintage record, **and** a validated expectation source |
| **C. Cross-meeting propagation** | Nothing on the primary claim | No | No admissible donor under the rule-vintage requirement | 0 edges admitted; 623 structurally admissible under a sentinel that is not evidence; 785 receiver-side refusals; 480 resolved before origin; 285 not open at origin | The rule-vintage record; then the news rung, which is blocked separately on the expectation source |
| **D. Perp arbitrage and absorption** | Cross-sectional descriptive only | No | One source build held, so no build-to-build series exists; the cost layer is unobservable | 1 distinct build; 2 sweep-log entries (1 performed, 1 skipped); `execution_cost_not_observable_from_this_source` on 18,547 of 18,547 differentials; `capacity_estimable: false` | Collection time closes the persistence blocker. The cost layer is **not** closeable from this source by any amount of time. |

Three of the four studies are blocked on one missing document — a per-contract
rule-vintage record — and that is the programme's single highest-value acquisition.
Study D is blocked on two things that document cannot supply, and its longest pole,
the build series, is already closing because the collector is running.

### 10.1 The narrowest runnable work today

| Work | Study | Status |
| --- | --- | --- |
| Declared-grid census with missingness accounting | A | run |
| Exploratory rule-reason-only panel | A | run; degenerate, reported as such |
| Calibration of the promotion rule on synthetic tapes | A, C | run; verdict `pass` |
| Aggregate cross-venue release-window activity | B | runnable, exploratory |
| Perp cross-sectional spread census and break-even curve | D | runnable, exploratory, with the cost refusal attached |
| Perp interval-derivation reconciliation | D | runnable |
| Perp build-to-build persistence | D | accumulates with collection time |

## 11. Artifacts these declarations rest on

| Artifact | What it supplies |
| --- | --- |
| `reports/study_execution_status.md` | the measured state, the withdrawal table, the terminal findings and the stage log |
| `reports/source_feasibility.md` | which inputs exist and which do not, with every retrieval attempt and its exact result |
| `reports/empirical_study/paper.md` sections 5 and 6 | the rule calibration's measured rates and the exploratory absorption measurement, including what is estimable and what is blocked |
| `.audit/astra-repair/evidence_three_gates.md` | the per-release admissibility funnel, the rule-vintage demonstration, and the pre-release capture coverage |
| `reports/contract_rule_registry.json` | the cross-venue match checklist and the empty verified-match sets |
| `reports/data_card.md` | the acquired archives and their measured sizes and spans |
| `.audit/external-measurements.md` | the archive facts: shard partitioning, field semantics, measured in-window activity |
| `configs/study_v2.yaml` | the clock, estimands, caps, ladder, splits, seeds and calibration |
| `configs/event_windows_v2.yaml` | the windows, the observation floor and the missingness rules |
| `configs/cohort_v2.yaml` | the two declared arms, the declared series, the candidate-universe rule and the rule-vintage requirement |
| `configs/cohort_forward.yaml` | the forward arm's six releases and the mechanical rule that extends it |
| `configs/neighbor_graph_v2.yaml` | the decision calendar, the predicate match fields and the rule-vintage record shape |
| `configs/perp_arbitrage_v1.yaml` | the perp collection cadence, universe rules and claim limits |
| `reports/preregistration.md`, `reports/preregistration_v2.md` | the v1 and v2 freezes these four studies sit beside and inherit from |

## 12. Status of this document

Frozen locally on 2026-09-16, before any result from studies A, B, C or D was read. No
study in this document has produced an estimate. Every number here is a measured count
from the artifacts in section 11 or a declared design choice, and the two are labelled
wherever they appear.

A change to any choice in section 2 through section 7 after this freeze is exploratory.
Changing a primary specification creates a new version and consumes a new test cohort.
The four study files are the declarations; this document explains the spine they share.
Where the two ever disagree, the study file governs its own study and this document
governs only the conventions it states as common.
