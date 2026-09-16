# Preregistration, v2

This is the analysis specification the v2 run is measured against. It was frozen
before any v2 estimate was read. It supersedes nothing: the v1 files
(`configs/study_v1.yaml`, `configs/cohort.yaml`, `configs/event_windows.yaml`,
`configs/external_history_v1.yaml`) stay frozen and unchanged beside it.

Frozen files: `configs/study_v2.yaml`, `configs/cohort_v2.yaml`,
`configs/event_windows_v2.yaml`, `configs/neighbor_graph_v2.yaml`.

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

The ten releases are the v1 cohort, unchanged, selected from official calendars
without reference to outcomes.

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

| Claim | Status |
| --- | --- |
| The release moves its own market (absorption) | not claimed: 0 of 785 declared pairs carries verified rule evidence, so every panel row is masked |
| Information diffuses between policy-rate contracts | not claimed: 623 structurally admissible edges are withheld by the rule-vintage requirement, and the news vector is absent |
| The decision rule's false-positive rate or power | measured on a synthetic process only: 8 of 10 nulls at 0 of 200 repetitions each, recovery 196 of 200, verdict `inconclusive`; not claimed for any real release or contract |
| Anything about what a live participant knew | not claimed: source-time alignment is retrospective |

## 9. Blocking prerequisites, named exactly

1. **A per-contract rule-vintage record** for each candidate contract: contract id,
   the sha256 of the rule text, the source it was read from, the method that verified
   it, and the instants bounding which version was in force.
   `reports/contract_rule_registry.json` is a specification document
   (`status: frozen_local_unregistered`) and carries no such record, so this
   requirement is unmet for every contract. Settlement outcomes and current rule text
   are not admissible substitutes.
2. **A point-in-time expectation source** covering the declared news vector for each
   release. None exists on this checkout, and `ingest/expectations.py` reports the
   absence rather than supplying a zero.
3. **The transaction-tape calibration** at 200 repetitions per primary scenario. Run
   and reported in section 7; it no longer blocks, though its verdict stays
   `inconclusive` until a majority of its declared nulls are estimable.
