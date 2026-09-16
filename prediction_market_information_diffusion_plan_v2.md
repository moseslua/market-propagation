# Information Propagation in Prediction Markets
## Historical-Data Expansion Plan

**Version:** 2.0
**Prepared:** 14 September 2026
**Supersedes:** `prediction_market_information_diffusion_plan.md` v1.0 (12 September 2026) in the data
and identification sections. v1 remains the authority on contract algebra, payoff coherence,
the model ladder, and research integrity.
**Status:** Data acquisition complete. Empirical hypotheses still untested. No model has been fit.
**Operating scope:** Read-only public-data research and offline simulation. No funded accounts,
real-money order placement, or market interventions.

---

## 0. What changed, and what did not

v1's feasibility gate was blocked on the sentence: *real-cohort claims stay blocked until real data
are acquired.* Three external acquisitions now sit in `data/external/`. 45 GB of the repository's
45 GB is those acquisitions.

The honest version of the status change is narrower than "the gate is unblocked":

> The gate that was blocked — **is there enough observable market activity at a release to estimate
> a response at all?** — is now answerable from data rather than from an endpoint probe. The gate
> that was blocked for a different reason — **can a quote-midpoint latency claim be made?** — is
> still blocked, and this data does not unblock it.

### 0.1 What is now possible

Measured, not assumed. Reproduce with `uv run python /tmp/probe_density.py`.

| Observation | Evidence |
| --- | --- |
| The trade tape contains a real release response | Kalshi whole-venue trades per minute around CPI 2025-05-13 (release 20:30 SGT / 08:30 ET): 49, **116**, 49, 45, 54, 34, 43, 29, 80, 54 |
| Direct CPI threshold ladders exist, with depth | `KXCPI-25APR-T{-0.1,0.0,0.1,0.2,0.3,0.4,0.5}`: 496 / 689 / 1528 / 1632 / 1041 / 560 / 224 trades |
| A downstream policy cohort exists and trades after the release | `FED-*` / `KXFED*`: 1,054,088 trades, 1,110 tickers, 513M contracts, 2021-07 to 2026-01 |
| The release instant is discoverable inside the tape | the density spike lands exactly on the computed BLS instant, EST/EDT resolved through `America/New_York` |
| A Polymarket macro cohort exists | 356 `fed`-slug markets, 180 `inflation`, 38 `interest-rate`; e.g. `will-the-fed-decrease-interest-rates-by-25-bps-after-its-may-meeting` |
| Volume is concentrated in the recent regime | whole-venue trades/month: 116k (2024-08) → 1.31M (2025-01) → 48.5M (2026-01) |

### 0.2 What is still blocked, and cannot be fixed by more data of this kind

| Blocked claim | Why |
| --- | --- |
| Quote-midpoint response at 60/300/900/1800/3600 s (v1's `midpoint_change_5min`) | Neither acquisition contains quotes, books, or sizes. Kalshi is trades-only; Polymarket's README states plainly that no layer includes order-book snapshots, quote updates, cancellations, or resting depth |
| Latency and clock claims | No receipt timestamps. `tick_complete` stays `false`. A historical record with no receipt timestamp cannot support a latency claim (README, "Reading the data") |
| Spread and depth outcomes (v1 `spread_change`, `depth_change`) | No two-sided book, so `midpoint_defined_only_when: two_sided_and_valid` is never satisfiable |
| The `M_{i,e}(u)` term as a measured quantity | Observation delay is unobservable here; it becomes a *bounded* nuisance modelled through staleness, not a measured component |
| Resolution of the mechanical-vs-real question at sub-block granularity on Polymarket | Fills carry `block_timestamp` in whole seconds and many fills share a timestamp; the finest honest resolution is the block, not the millisecond |

**Decision:** the primary outcome changes from a quote-midpoint change to a **trade-price change with a
latched last trade and a staleness cap**. This is a weaker estimand and every downstream claim must
inherit the weakening. The `trade_price_status: separate_robustness_layer` line in
`configs/study_v1.yaml` promoted to primary is the single most consequential change in this plan.

---

## 1. Frozen inventory

The three acquisitions become immutable read-only inputs. Nothing in this repository writes to
`data/external/`.

| Acquisition | Location | Rows | Span | Grain | Contains |
| --- | --- | --- | --- | --- | --- |
| Kalshi trades + markets | `data/external/kalshi-trades/` | 154,505,005 trades; 17,464,713 markets | 2021-06 → 2026-01 | trade | price, count, taker side, UTC source time |
| Polymarket v1 | `data/external/polymarket-v1/` | ~1.2B `OrderFilled`; 1248 daily files | 2022-11-21 → 2026-04-28 | on-chain fill | price, size, maker/taker, event-normalized `p_event` and aggressor `D` |
| Forecast snapshots | `data/external/forecast-snapshots-kalshi_events-768472771c/` | ~900 snapshots | snapshot instants | market snapshot | community prediction, forward predictions, resolution |

All three are CC-BY-4.0 (the snapshot dataset is MIT) and are third-party. **Attribution is a
deliverable, not a courtesy**: `reports/data_card.md` must name all three producers, and any released
artifact must carry their licenses.

### 1.1 Required quantity re-estimate

The binding constraint is **not** dataset size. It is in-window macro trades per event.

- CPI-ticker trades: **289,289** total across **1,786** tickers over 4.5 years — roughly 160 trades
  per ticker for its whole life.
- Whole-venue release-minute density: ~116 trades; the macro-filtered subset of that minute is
  smaller.

So "70 GB" is a statement about entertainment and sports markets. The macro cohort is a thin,
expensive slice. Phase 2's job is to measure it per event, and the power calculation in §8 is
downstream of that number, not of the byte count.

---

## 2. Leakage quarantine (do this before any panel is built)

The acquisitions are third-party scrapes, and two of them carry **point-in-time metadata that is
actually as-of-download**. This is the highest-severity hazard in the project.

### Q1 — Kalshi `markets` is a snapshot, not a history

Per the dataset README, `markets` was backfilled from `/markets`. Therefore these columns are
**as-of-fetch (≈ September 2026) for every market, including 2021 markets**:

`yes_bid`, `yes_ask`, `no_bid`, `no_ask`, `last_price`, `volume`, `volume_24h`, `open_interest`,
`status`, `result`.

- Using `volume` or `last_price` as an event-time feature is **full-history look-ahead**: it
  contains trading that had not happened yet.
- Using `status` to decide whether a market was open at event time is **retrospective
  classification**. Point-in-time liveness must come from `open_time` and `close_time` alone —
  which is exactly what `configs/endpoints.yaml` already concluded when it found mixed strike
  lifecycles inside one event.
- `result` is admissible **only** as a resolution label, never as a feature.

Encode as a frozen exclusion list, and add a test that fails if any name on it reaches a feature
builder. Mirrors v1's existing prohibited list (`market_selection_on_post_event_volume`,
`retrospective_participant_labels_as_realtime_features`).

### Q2 — Polymarket's joined metadata is full-history

`winning_outcome_label`, `resolution_status`, `resolved_at`, and `p_event` where it is derived from
a resolved condition are admissible for **labels and boundary discovery only**. `close_at` and
`opens_at` are admissible for the point-in-time universe.

### Q3 — No receipt clock exists anywhere

Every record gets `receipt_time = null`. Availability is an **interval**, per v1's rule that a
historical record without a receipt timestamp is given an availability interval rather than invented
precision. The usable-time replay path therefore becomes **unidentifiable on real data**, and the
`replay_disagreements.json` artefact must say so rather than reporting an empty disagreement set
that reads as agreement.

### Q4 — The datasets are not the venue

Kalshi's README asserts zero duplicate trades, zero null prices, 100% join coverage, and that
21,950 duplicates were removed in cleaning. Those are the **uploader's** claims. This repository
verifies: duplicate `trade_id` count, null-price count, ticker-join rate, monotonicity of
`created_time` within shard and across shard boundaries, and whether shards overlap. Any failure is
recorded in the data card, not silently repaired.

### Q5 — Cross-venue overlap window

Kalshi ends 2026-01-29; Polymarket ends 2026-04-28. Cross-venue work (H5) is confined to the
**overlap 2022-11-21 → 2026-01-29**, and Polymarket's macro depth in that window is thin. Report H5
on the overlap or not at all.

---

## 3. Data layer

Reuse the existing separation — immutable raw, normalized records, derived panels — with the
external data as a fourth input class. Add it to the README's three-input table rather than
pretending it is `data/public/`.

```
data/external/                      read-only, never written, hash-frozen
  <acquisition>/
data/derived/
  external_inventory.parquet        file, bytes, sha256, row count, min/max time per shard
  trades_utc/                       venue-partitioned, trade_parquet, sorted by source time
  contracts/                        Contract records with rule hashes
  releases/                        Release records from archived BLS payloads
reports/generated/
  external_admissibility.json       the Q1-Q5 decisions as machine-readable gates
```

**Loader shape.** `src/market_propagation/ingest/external_history.py`, exposing
`ExternalTradeLoader` and `ExternalContractLoader` that yield **existing domain types**
(`Trade`, `Contract`, `Provenance`), so `storage.write_parquet`, `point_in_time.build_event_panel`,
`coherence`, `models` and `evaluation` work unchanged. Do not fork the domain.

Each row maps as:

| Domain field | Kalshi source | Polymarket source |
| --- | --- | --- |
| `Trade.price` | `yes_price / 100` → exact `Decimal` | `price` |
| `Trade.size` | `count` | `token_amount` |
| `Trade.venue` | `kalshi` | `polymarket` |
| `Provenance.source_time` | `created_time` | `to_timestamp(block_timestamp)` |
| `Provenance.receipt_time` | `null` | `null` |
| `Provenance.record_id` | `trade_id` | `OrderFilled.id` |
| `Provenance.raw_hash` | shard sha256 | shard sha256 |

Two normalizations that must not be skipped:

- **Kalshi prices are cents, integer 1–99.** Convert with `Decimal(cents) / 100`, never float
  division. `domain.parse_decimal` already refuses bools and non-finite values; the integer-cents
  step belongs in the loader.
- **Polymarket `price` is per-leg; `p_event` is the reference axis.** Per the README's own warning,
  `price` is not always the probability of the reference event. Use `p_event` for event-probability
  work and `D` for normalized aggressor direction, and never pool `neg_risk` classes.

**Read strategy.** DuckDB over Parquet through `storage.duckdb_connection` and
`storage.query_sealed`, with window predicates pushed down. Never `read_parquet` a whole
acquisition. Derived windows are small: a ±2 h window across a 5-year tape is megabytes.

---

## 4. Identity and cohort

`domain.market_key(venue, contract_id)` already venue-qualifies identity. Map
Kalshi `ticker` and Polymarket `condition_id` onto it, and keep the venue-local id intact.

### 4.1 Three relation types, kept separate

v1 forbids mixing them, and this data makes the temptation strong because everything is keyword-
reachable.

1. **Logical payoff.** `KXCPI-25APR-T0.0 ⊂ T0.1 ⊂ T0.2 ⊂ …` — strictly nested thresholds are a real
   monotone payout relation. `KXCPICOMBO` conjunctions are real logical relations. These give the
   `A` matrix for the coherence diagnostic.
2. **Economic exposure.** `KXCPI` → `FED-25JUN-T*`. The release does not determine the policy
   payoff; it moves the distribution over it. `configs/cohort.yaml` already states this correctly
   and must not be upgraded to an equivalence.
3. **Predictive.** Anything discovered from price lead-lag. Training-only, never relabelled.

### 4.2 Cohort definitions with measured counts

| Cohort | Definition | Measured |
| --- | --- | --- |
| `direct_threshold_ladder` | `KXCPI-<YYMON>-T*`, `KXCPIYOY-<YYMON>-T*`, `KXPAYROLLS-*` | 7 strikes/event, 224–1632 trades/strike |
| `direct_resolution` (settlement only) | same, but **closes before the release** | KXCPI last trades 20:08–20:23 SGT vs release 20:30 |
| `downstream_unresolved` (primary) | `FED-<YYMON>-T*`, `KXFEDDECISION-*`, `KXFED*` | 1,054,088 trades / 1,110 tickers |
| `polymarket_macro` | slug/keyword-matched macro markets | 356 fed, 180 inflation, 38 interest-rate |
| `mention_control` | *word-mention* markets: `will-powell-say-inflation-60-times-…` | present in quantity |

The `direct_resolution: excluded` line in `configs/cohort.yaml` is now **confirmed by data** rather
than by a peer report: the 08:25 ET close means post-release quote response is unmeasurable, and
the trade tape agrees (last trades 5–22 minutes before the release). The settlement/assimilation
study becomes the honest use of this family.

The `mention_control` cohort is a genuinely valuable find. A market paying on *whether the word
"inflation" is uttered 60 times in a press conference* has near-zero economic exposure to a CPI
print, similar pre-event activity, and identical venue mechanics. v1 requires negative-control
validity to be **argued in writing before estimation**; that argument is easy and strong here.

### 4.3 Boundary discovery

The direct ladders resolve before the release, so the propagation cohort's "resolves before release"
boundary `b(t)` must be found empirically per contract from the tape: last trade before the release
instant, and close time from the point-in-time universe. Contracts whose boundary falls inside the
measurement window are excluded with a recorded reason, per the existing exclusion-ledger rule.

---

## 5. Panel and estimand

### 5.1 New primary estimand

```
R^T_{i,e}(h) = p^T_i(τ_e + h) − p^T_i(τ_e^-)
```

where `p^T_i` is the **last trade price at or before the timestamp**, with
`p^T_i(τ_e^-)` = last trade strictly before `τ_e`, latched. Estimable at 1 s and 5 s and at every
prespecified v1 horizon (60, 300, 900, 1800, 3600 s) because it needs only the tape.

### 5.2 Three regressors, and why each is defensible

The response is not a clean function of a macro surprise because the contracts are threshold
contracts, not level contracts.

1. **`S_k`, the surprise.** Required by `expectation_policy` and **currently unavailable** —
   `configs/endpoints.yaml` records `expectations.status: unavailable`, no licensed consensus, no
   pre-release archive. If a pre-release consensus cannot be sourced and archived, the surprise
   slope is dropped and only the timing study runs, exactly as `stop_or_pivot_rules` prescribes.
2. **`|p − K|`, distance to the strike.** Load-bearing. A 25 bp cut priced at 0.85 can only move
   ~0.15; the same news at 0.50 can move 0.50. Without this covariate the response curve is a
   mixture over boundary proximity and will look like heterogeneity for mechanical reasons.
3. **`staleness`, time since last trade at baseline.** This is the `M_{i,e}` proxy. v1 permits
   staleness only from gap detection and documented refresh, and **prohibits price-change age as a
   staleness proxy** (`price_change_age_as_staleness_proxy: prohibited`). On a trade tape with no
   books, `last_verified` does not exist and gap detection has no refresh events, so the prohibition
   and the fallback collide. **This is the one place where this plan must knowingly depart from the
   frozen spec**, and it must be recorded as a spec correction with `exploratory` labels, not
   quietly redefined.

### 5.3 The built-in falsifier

`R^T` is defined from baseline to endpoint. If the baseline quote is already stale by the release
instant and the market has closed, the estimand is **measured as zero by construction** — exactly
what the data shows for the direct ladder. Compute `R^T` on the direct ladder anyway and publish it.
It is a real-data demonstration of the mechanical-observation failure mode that v1's H3 asserts, and
it costs nothing.

### 5.4 Two clocks, honestly labelled

| Order | Source | Status on real data |
| --- | --- | --- |
| `source_time_order` | venue timestamps | identified |
| `usable_time_order` | availability upper bound | **unidentifiable** — no receipt clock |

Publish the disagreement artefact as `unidentifiable`, not empty. `source_time_is_usable_time`
stays `false`.

Known clock risks to test rather than assume: batch-submitted Polymarket fills sharing a
`block_timestamp`; the 2025 US federal appropriations lapse (CPI October 2025 not published,
September 2025 published late on 2025-10-24; empsit September 2025 on 2025-11-20) which breaks any
"monthly on schedule" assumption; and DST transitions, handled through `America/New_York` rather
than a fixed offset, since 6 of 10 events in the current cohort straddle the transition.

---

## 6. Falsification suite on real data

v1's falsification suite was largely synthetic because real data was absent. Most of it is now
realizable.

| Falsifier | Data | Status |
| --- | --- | --- |
| **Placebo release times** — shift ±1..24 h, matched on weekday and hour | full tape | realizable, strongest available |
| **Mention controls** — word-mention markets as semantic negatives | Polymarket + Kalshi | realizable, newly available |
| **Pre-release window** — estimate `R^T` in (−120, 0) min | full tape | realizable, must be ≈ 0 |
| **Direct-ladder null** — `R^T` on contracts closed before the release | `KXCPI` ladder | realizable, expected exactly 0 |
| **Quote-age stratification** | staleness bins | realizable in trade space |
| **Reversed directions** | edges | realizable |
| **Leave-one-release-out** | panel | realizable |
| **Cross-venue disagreement** | overlap window | realizable but thin |
| **Synthetic shared-news null** | simulator | already built and passing |
| **Positive-recovery in simulation** | simulator | already built: `network_recovery_power: 0.525` |

The existing registry already records, for the synthetic line, `network_null_false_positive_rate:
0.0` and `response_slope_power: 1.0` across 120 synthetic events with 200 bootstrap samples. Those
are software results and must keep their `synthetic_software_experiments_count_as_empirical_results:
false` label.

`mention_control` deserves the design care: the claim that a market about *uttering* a word is
unexposed to a CPI print is an **assumption to be argued**, not a fact, and it fails if traders in
the mention market read the CPI print as changing the odds that the Fed talks about inflation. That
argument, pro and con, goes in the preregistration before the estimate.

---

## 7. Specification v2 and the locked test

### 7.1 v1's locked test is intact

Verified: `reports/generated/registry-review-2026-09-14.json` shows `event_claims: []`,
`reservations: []`, and three runs all `classification:
synthetic_software_methods_reproduction`, all on `rel-*` / `syn_*` event ids with the packaged
fixture. **No real cohort has been touched and no locked test has been consumed.**

That is worth protecting. v1's cohort (`configs/cohort.yaml`, five CPI and five empsit releases
January–May 2025) currently sits at `eligibility_status: candidate_not_eligible` with
`verified_eligible_market_ids: []` — the exact state that this data now changes.

### 7.2 Two options, decided explicitly

The hazard: v1's primary outcome is a quote midpoint at 60–3600 s, which real data cannot support.
Evaluating "v1 on real data" would either silently change the estimand or fail for a measurement
reason that looks like a null finding.

- **Option A — amend within v1.** Change `outcomes.primary_historical` to trade-price and record it
  as a spec correction, keeping v1's event cohort and locked test. Cheaper. But it edits a frozen
  spec after seeing data, which is exactly the practice the freeze exists to prevent — even though
  no *outcome* has been inspected.
- **Option B (recommended) — register v2.** Leave v1 frozen and intact, its locked test preserved
  for the synthetic methods line. Write `configs/study_v2.yaml` with the trade-space estimand, the
  trade-price staleness rule, the coarse-availability rule, the new horizons, and a **new** locked
  test cohort, and let the registry's `spec_change_effect: new_version_and_new_test_cohort` and
  `cohort_reuse_protection` machinery enforce it.

Option B costs one file and one registry entry and keeps the integrity story clean. v2 must carry a
`specification_corrections` block in v1's own style, listing at minimum: primary outcome moved from
quote midpoint to latched trade price; staleness redefined from refresh-based to
latched-gap-based with the consequence for `price_change_age_as_staleness_proxy` recorded
explicitly; usable-time replay marked unidentifiable; the direct ladder moved to
settlement-only; response horizons extended to 1 s and 5 s.

### 7.3 What v2 inherits unchanged

Clustering by release date; the event-cluster bootstrap; `simultaneous_bands_over_response_curve`;
the exclusion ledger; chronological whole-event splits with cross-venue equivalents sharing a split;
purge by label availability; `no_fill_forward_after_close`; the coherence diagnostic on nested
thresholds; the model ladder and the promotion gates G0–G6. Do not re-derive any of it.

---

## 8. Power

No universal event count. Derive from the actual cohort.

Cohort size available: BLS CPI and Employment Situation, monthly, **2021-07 → 2026-01** for Kalshi,
≈ 40 events per family, plus CDIAC-style co-scheduled releases to code or exclude. That is roughly
**4× v1's ten-event cohort**, and the dense regime (2025-08 onward, 4–48M trades/month) covers only
the last ~6 releases per family.

The power calculation that v1 lists as `run_status_at_freeze: not_run` becomes runnable:

1. Measure per-event in-window macro trade counts (the Phase 2 deliverable — this is the number
   that binds).
2. Estimate event-level residual variance from the pilot events.
3. Simulate cluster-aware, at the measured in-window count, for the prespecified effect threshold
   (0.01 absolute probability units) and the MAE threshold (0.005).
4. Report power **by regime**. A 2022 release with three in-window trades and a 2026 release with
   thousands are not the same experiment, and pooling them into one power number would be a
   presentational choice that hides the binding constraint.

Honest expectation: the recent dense regime supports the timing study; the early regime supports
descriptive coverage statements only. State which results rest on which regime.

---

## 9. Phases and gates

| Phase | Work | Gate to pass |
| --- | --- | --- |
| **P0 Inventory and quarantine** | hash every shard; `external_inventory.parquet`; encode Q1–Q5; verify the uploader's quality claims | inventory reproducible from hashes; leakage exclusion list enforced by a test |
| **P1 Loader and domain mapping** | `ingest/external_history.py`; exact-cent price conversion; `p_event`/`D` discipline; `Trade`/`Contract` emission | existing test suite still green; new loader tests with fixture tape |
| **P2 Coverage — the real G0** | per-event in-window trades per contract; liveness from `open_time`/`close_time` only; boundary discovery; exclusion ledger | **eligible cohort with observed frequency**; if the in-window count is too thin, narrow the estimand rather than widen the window |
| **P3 Panel** | `build_event_panel` on trade space; the three regressors; `R^T` at 1 s/5 s/60 s/300 s/900 s/1800 s/3600 s | measurement passes visual and adversarial audit; the pre-release window and the direct-ladder null both read ~0 |
| **P4 Falsification** | placebo times, mention controls, age stratification | synthetic-null false discovery at the chosen resolution |
| **P5 Spec v2** | `configs/study_v2.yaml`; registry entry; explicit non-consumption of v1's test | spec corrections recorded before any fit |
| **P6 Estimation** | local projections in trade space; nested held-out comparison; coherence on nested ladders | held-out gain beyond common news and own-market state |
| **P7 Robustness** | placebos, leave-one-out, regime split, cross-venue on the overlap | no dependence on a single event, clock, or rule error |
| **P8 Paper and release** | data card, preregistration, reproduction guide, attribution | claims match evidence and remaining uncertainty |

P2 is the real gate and it is the one most likely to fail. It should be attempted and reported
before any model code is written.

---

## 10. Failure modes and stop rules

| Failure | Response |
| --- | --- |
| In-window macro trades per event are single digits | coarser buckets, trade-count-weighted estimand, or a documented inconclusive result |
| No pre-release consensus can be sourced or archived | drop the surprise slope; run the timing study only (v1 already prescribes this) |
| Polymarket macro cohort too thin in the overlap | no cross-venue pooling; H5 unevaluated, not failed |
| Placebo release times show comparable "response" | the effect is calendar artefact; report it and block propagation interpretation |
| Direct-ladder null is not ≈ 0 | the panel construction is wrong; fix the panel, not the interpretation |
| Rule matching between venues fails the checklist | no pooling; report as an unmatched cohort |
| Latching creates spurious lead-lag | shorten the staleness cap, re-estimate, publish both |

A negative or inconclusive result remains a valid outcome. An informative upper bound on remaining
predictability is a publishable finding.

---

## 11. Open decisions

1. **Spec version — Option A or B** (§7.2). Recommendation: B.
2. **Prospective capture.** Kalshi's historical WebSocket is authenticated, so live capture yields
   snapshots only with `tick_complete: false`. The frozen spec caps paid spend at zero. Whether a
   prospective capture line is worth running alongside the historical study, given it would be
   the only source of quotes and receipt clocks, is a scope decision.
3. **Traditional-asset confirmation.** Realized Treasury futures or SPY at 1-minute resolution
   would give the clean external response that would make the "does anyone mark the probability
   differently" claim strong rather than suggestive. That is a separate data acquisition with its
   own licensing and point-in-time problems, exactly as v1's Experiment E describes.
4. **Point-in-time consensus source.** This is the single highest-value remaining acquisition:
   it converts the study from a timing study into a surprise study. Without it, most of v1's
   hypothesis H1's estimator has no `S_k`.
5. **Attribution and redistribution.** All three datasets are third-party. Decide now whether the
   released package ships derived windows, acquisition instructions and hashes, or both.

---

## Sources

Same source list as v1 §Sources, plus the three acquisitions:

- Kalshi Prediction Market Trades & Markets, dataset `TrevorJS/kalshi-trades` (CC-BY-4.0), built on
  the original collection by Andrew Becker, from the Kalshi public API.
- Polymarket-v1 database, Boka Qin and Rui Yang, arXiv:2606.04217 (CC-BY-4.0), from on-chain event
  logs on Polygon, 2022-11-21 to 2026-04-28.
- Forecast Snapshot Dataset, source `kalshi_events`, hash `768472771c` (MIT).
