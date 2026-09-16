# Preregistration, v2

This is the analysis specification the v2 run is measured against. It was frozen
before any v2 estimate was read. It supersedes nothing: the v1 files
(`configs/study_v1.yaml`, `configs/cohort.yaml`, `configs/event_windows.yaml`,
`configs/external_history_v1.yaml`) stay frozen and unchanged beside it.

Frozen files: `configs/study_v2.yaml`, `configs/cohort_v2.yaml`,
`configs/event_windows_v2.yaml`, `configs/neighbor_graph_v2.yaml`.

One arm of the population is prospective and is extended by a declared calendar rule
after this freeze: `configs/cohort_forward.yaml`, named by `configs/cohort_v2.yaml`
under `arms:`. The rule and everything it may not read are stated in section 2.2, which
is part of this freeze. What the rule appends later is more rows for an already-declared
arm, not a change to an analysis choice, and the arm it appends to is never pooled with
the retrospective arm.

## 1. What is being estimated

Two claims, kept apart because they rest on different evidence.

**Absorption.** For each release `e`, contract `i` and horizon `h`:

```
absorption(e, i, h) = p_i(last valid trade ≤ release_time + h)
                    − p_i(last valid trade < release_time)
```

It is measured on the sealed response panel from the contract's own trades. It
does not need the exposure graph and it does not need a news vector.

**Conditional predictive propagation.** For a receiver `i` and its matched donor
`j`:

```
network_target(e, i) = p_i(last trade ≤ tau + H) − p_i(last trade ≤ tau)
neighbor_lag(e, j)   = p_j(last trade ≤ tau − L) − p_j(last trade < release_time)
own_lag(e, i)        = p_i(last trade ≤ tau) − p_i(last trade ≤ tau − H)
```

with `tau = release_time + 300s`, `L = 60s`, `H = 300s`. The estimand is the
five-to-ten-minute increment, not the first five minutes' absorption.

Neither claim is an intervention effect. Both are conditional associations built
only from lagged, source-time information, and neither is reported as causal.

## 2. The population, declared before the releases

The population has **two arms**, declared in `configs/cohort_v2.yaml` under `arms:` and
never pooled. They are not versions of one arm and they are not a time split of one
sample. A result names its arm, an arm is its own denominator, and the two arms have
different blockers, so a forward-arm result is never read as the retrospective arm's
result or the reverse.

| Arm | File | Cohort id | Releases | Rule vintage certifiable |
| --- | --- | --- | --- | --- |
| Retrospective, 2025 | `configs/cohort.yaml` | `core_2025h1` | 10 | no |
| Forward, 2026 H2 | `configs/cohort_forward.yaml` | `forward_2026h2` | 6, growing | yes |

### 2.1 The retrospective arm, unchanged

The ten releases are the v1 cohort, unchanged, selected from official calendars
without reference to outcomes. Nothing in this section changes anything that arm
already declares, and the arm is retained and reported as blocked for want of
rule-vintage evidence rather than deleted or replaced.

### 2.2 The forward arm, and the mechanical rule that extends it

The forward arm's six releases are the next three scheduled CPI and the next three
scheduled Employment Situation releases, October through December 2026:

| Event id | Family | Reference period | Scheduled (ET) | Scheduled (UTC) |
| --- | --- | --- | --- | --- |
| `empsit_2026_10` | employment | September 2026 | 2026-10-02 08:30 EDT | 2026-10-02 12:30Z |
| `cpi_2026_10` | cpi | September 2026 | 2026-10-14 08:30 EDT | 2026-10-14 12:30Z |
| `empsit_2026_11` | employment | October 2026 | 2026-11-06 08:30 EST | 2026-11-06 13:30Z |
| `cpi_2026_11` | cpi | October 2026 | 2026-11-10 08:30 EST | 2026-11-10 13:30Z |
| `empsit_2026_12` | employment | November 2026 | 2026-12-04 08:30 EST | 2026-12-04 13:30Z |
| `cpi_2026_12` | cpi | November 2026 | 2026-12-10 08:30 EST | 2026-12-10 13:30Z |

US daylight saving ended on 2026-11-01, so the October events are UTC−04:00 and the
November and December events are UTC−05:00. `configs/event_windows_v2.yaml` sets
`fixed_utc_offset_allowed: false`, so the offset is carried per event.

**The extension rule, stated mechanically.** The arm is not declared complete and
cannot be, because the BLS schedule runs only about three months ahead and is updated
as needed. It is extended by `next_scheduled_release_per_family`, declared in
`configs/cohort_forward.yaml`:

1. Read the family's own by-release schedule table
   (`https://www.bls.gov/schedule/news_release/cpi.htm` and `.../empsit.htm`) and its
   monthly selected-release calendar
   (`https://www.bls.gov/schedule/{year}/{month:02d}_sched_list.htm`).
2. Take that family's **earliest scheduled release not already declared in the arm**.
   One release per family is appended per round, so the two families advance
   independently.
3. Record the calendar's own date, its own stated release time converted from
   `America/New_York` to UTC, the reference period it names, the URL it was read from,
   and the weekday it states.
4. Leave the archive URL unverified and the observed publication time null until the
   release has published and its page has been read.

**Selection is fixed before the release's outcome is known.** Membership is a function
of a published BLS calendar that this repository does not write, read before the
release instant. Nothing about any contract's liveness, trading or repricing is read
at selection time; nothing about the release's own first print, revision or surprise is
read at all; and an appended release is **never removed** once the arm has been run
against it, so a release that yields no row stays in the denominator with the reason it
yielded none.

**The peeking hazard, and what prevents it.** Appending a release after its own outcome
is known — or letting any fact about the release's market decide whether it is appended
— would make the arm's membership a function of its results. Every estimate from such
an arm would be selected on the outcome, the event count would no longer be a
denominator, and no interval computed over it would mean what it states. Five things
prevent it: the calendar is published in advance and is not ours to write; the rule
names the release as the earliest not-yet-declared one, so this study does not choose
it; no outcome-correlated quantity is read at selection time; an appended release is
never removed; and the rule capture's own interval opens at the instant the serving
system states, so a capture taken after a window cannot certify that window even by
mistake (section 9, item 1). Each append round records its own date and per-release
source URL, so a reader can check that a release entered the arm before its instant.

**Why no forward release row exists yet, and cannot.** A release row is built from a
*published* release page: `scripts/import_bls_archives.py` requires a complete captured
page whose own embargo line agrees with the calendar, and refuses to write a row when
the parsed values or that agreement are missing. The pipeline then reads those rows from
a sealed parquet, verified by content hash against its sibling manifest
(`src/market_propagation/trade_panel.py:load_event_specs`), so a hand-written row is not
a row. A forward release has therefore published nothing to build a row from, and each
forward archive URL returned HTTP 404 when read on 2026-09-17. Declaring a forward
event in `configs/cohort_forward.yaml` and making it measurable are different acts:
this preregistration does the first, and the second happens at the publication cadence,
one release at a time. That is why the arm is extended by a rule rather than declared
complete, and why its `event_count: 6` is a dated snapshot rather than a target.

The candidate universe is declared per release from **pre-event information only**:
a contract is a candidate for a release when its series is one of the four declared
policy series (`FED`, `FEDDECISION`, `KXFED`, `KXFEDDECISION`) and its own recorded
listing interval covers the release instant. The grid is the denominator. A declared
pair that never traded keeps its masked rows, so a missing cell stays in the grid,
and a grid that omits a declared release is an error rather than a fallback to what
traded.

No post-release quantity and no post-release activity selects the universe. A
candidate that only traded after the release is recorded as such and counted
separately.

## 3. The exposure graph, declared before the releases

An edge runs from a donor to a receiver and exists only when all of the following
hold:

1. the donor's decision date is the date the declared calendar places immediately
   before the receiver's decision date, and that date is dated by the calendar;
2. the two contracts state the identical payout predicate, compared on
   `rate_definition`, `threshold`, `inequality`, `yes_axis` and `orientation`. The
   predicate is read from the venue's own archived contract text, never from a
   ticker;
3. both contracts are listed at the forecast origin and still listed at
   `tau + H`;
4. both contracts carry a rule version verified to be in force across that whole
   interval, with a named verification method.

A calendar date that holds no contract is `calendar_date_holds_no_contract`, never
a reason to reach back a further meeting. Mutually exclusive strikes of one meeting
are never each other's donor. A near-match threshold is never substituted. The
donor's read stops one guard before the target opens, so the same prints never
inform both sides of the comparison.

`configs/neighbor_graph_v2.yaml` declares the decision calendar, the readable payout
forms and the rule-vintage record shape.

## 4. The news vector

The surprise for a release is its first-print actual minus a forecast that was
public before the release, validated by `ingest/expectations.py`. That module
refuses, with its own code: a forecast published at or after the release; a forecast
scored against a revised actual rather than a first print; a unit that disagrees with
the release's own declared statistic; an unnamed consensus or verification method; a
market-implied value; an incomplete news vector; and evidence bytes whose digest no
longer matches the record.

The surprise per family is declared in `configs/study_v2.yaml`:
`cpi_headline_sa_mom_pct` for CPI and `payrolls_change_thousands` for employment.

## 5. The model ladder

Four nested rungs, each rung's feature set containing the previous rung's:
`no_change` ⊂ `own` ⊂ `news` ⊂ `network`, where `network` adds `neighbor_lag` and
`neighbor_lag_control` to the shared `own_lag`, `current_price`, `shock`,
`delayed_shock` set. Loss is MAE on the bounded link, the ridge penalty is selected
on validation rows over the declared grid, the baseline is `news` and the candidate
is `network`, and the promotion threshold is an MAE reduction of 0.005 probability
points.

Every rung is fitted on one identical sample: a column one rung needs removes those
rows from all the rungs. A rung whose declared column is absent, or present and null
on every row, is reported blocked with the column named and is never fitted on a
substitute.

## 6. Splits, seeds and uncertainty

Folds come from `evaluation.chronological_splits`, which is the one split authority.
The independent unit is the release cluster: rows sharing a release share its shock
and its error component, so every resampling is over whole releases. Seeds are
declared in `configs/study_v2.yaml`. No p-value or interval is reported from a
row-level resample.

## 7. Calibration, declared and run

The decision rule's operating characteristics were calibrated on **simulated
transaction tapes passed through the same graph, observation, feature, fitting,
tuning, paired-uncertainty and promotion code as real data**, at 200 repetitions per
primary scenario, with simultaneous null bounds.

**The calibration has run, and its verdict is `inconclusive`.** Of the ten declared
null scenarios, eight are estimable and promoted in **0 of 200 repetitions each**, a
one-sided upper bound of 0.0251 at simultaneous level 0.99375 against the 0.05
ceiling. The recovery scenario `communication` promoted in 196 of 200, a rate of 0.98
with a one-sided lower bound of 0.9548 against the 0.80 target. The verdict is not a
pass because `resolution_pause` and `spread_only` emit no complete comparison row by
construction and so have no estimable rate; the family bound is therefore not
certified for the declaration as written, and the run reports the two unestimable
nulls rather than dropping them. Certificate
`data/calibration/calibration_certificate.json`; registry record
`calibration-2856211b642d-fadcc9c34813`.

The earlier 48-repeat `falsification.network_falsification` call is recorded in the v2
configuration as superseded and insufficient: it counts a gain-threshold event on the
quote/simulator path rather than the declared transaction observation process, and at
48 repetitions its one-sided bounds cannot separate a 0.05 false-positive ceiling from
a 0.80 power target. The calibration measures the rule on a synthetic process; it is
not evidence about any real contract or release.

## 8. Claims this preregistration does not permit

Each row names the arm it is measured on. A count from one arm is never a count for the
other, and the two arms are never pooled into one figure.

| Claim | Arm | Status |
| --- | --- | --- |
| The release moves its own market (absorption) | retrospective `core_2025h1` | not claimed: 0 of 785 declared pairs carries verified rule evidence, so every panel row is masked |
| Information diffuses between policy-rate contracts | retrospective `core_2025h1` | not claimed: 623 structurally admissible edges are withheld by the rule-vintage requirement, and the news vector is absent |
| The release moves its own market (absorption) | forward `forward_2026h2` | not claimed, and nothing is estimable yet: no release in this arm has published, so it has no panel rows at all. Its blocker is the calendar, not the evidence |
| Information diffuses between policy-rate contracts | forward `forward_2026h2` | not claimed: no release in this arm has published, and its rule vintage is certifiable only by a capture taken before each release instant |
| The decision rule's false-positive rate or power | neither `arm`; synthetic process | measured on a synthetic process only: 8 of 10 nulls at 0 of 200 repetitions each, recovery 196 of 200, verdict `inconclusive`; not claimed for any real release or contract, in either arm |
| Anything about what a live participant knew | both arms | not claimed: source-time alignment is retrospective |

## 9. Blocking prerequisites, named exactly

1. **A per-contract rule-vintage record** for each candidate contract: contract id,
   the sha256 of the rule text, the source it was read from, the method that verified
   it, and the instants bounding which version was in force.
   `reports/contract_rule_registry.json` is a specification document
   (`status: frozen_local_unregistered`) and carries no such record, so this
   requirement is unmet for every contract. Settlement outcomes and current rule text
   are not admissible substitutes.

   This blocker has **two different fates, one per arm**, and the difference is the
   reason the forward arm exists.

   For the **retrospective arm** it does not close. Every candidate contract's window
   ended before 2026-05-13, no dated rule capture exists from anywhere near it, and a
   capture taken now records the instant it was taken, so it cannot open an interval
   before a window that has already closed. Those ten releases stay reported as blocked
   and are never deleted or rewritten.

   For the **forward arm** it closes by construction rather than by acquisition: a
   capture of the venue's own live listing opens its interval at the instant the
   serving system states, taken before the release instant, so it certifies a window
   that begins after it. The capture must therefore be taken ahead of each release,
   which is a cadence and not a one-off retrieval. A record whose interval opens after
   the window it would certify is refused by the consumer's own check, so no forward
   release can be certified retrospectively even by mistake. Until a release has
   published, this prerequisite is stated as *pending* for it rather than met.
2. **A point-in-time expectation source** covering the declared news vector for each
   release. None exists on this checkout, and `ingest/expectations.py` reports the
   absence rather than supplying a zero. This blocker applies to both arms and is
   unchanged by the forward arm: a release in the future has no easier expectation
   source than a release in the past, and a forward release's forecast must be captured
   before its own instant to be admissible at all.
3. **The transaction-tape calibration** at 200 repetitions per primary scenario. Run
   and reported in section 7; it no longer blocks, though its verdict stays
   `inconclusive` until a majority of its declared nulls are estimable.
