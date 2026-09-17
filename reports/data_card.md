# Data card

What the inputs to this repository are, where each came from, and what each can and
cannot support. Read it with `reports/data_audit.md`, which reports what the
artifacts contain, and with `reports/reproduction_guide.md`, which rebuilds them.

## Classification

Study inputs are feasibility and identification limited at this revision. The real
inputs supply ten original releases and a bounded policy-contract candidate set, and
they supply no study-eligible contract, no independent point-in-time expectation,
and no replication cohort.

## The inputs

| input | path | role | evidence class |
| --- | --- | --- | --- |
| Packaged synthetic fixture | `src/market_propagation/fixtures/replay.json` | Synthetic sample the reproduction builds | Generated |
| Study specification | `configs/study_v1.yaml` | Frozen settings and thresholds | Configuration |
| Event windows | `configs/event_windows.yaml` | Window and prediction-delay rules | Configuration |
| Cohort | `configs/cohort.yaml` | The ten prespecified release events | Configuration |
| Browser captures | `data/public/bls-browser/` | Ten original BLS release payloads with receipts | Real, acquired |
| Normalized releases | `data/public/bls-normalized/releases.parquet` | Ten first releases, parsed and hash-verified | Real, derived |
| Primary audit | `data/public/final-audit/` | Live read-only venue requests with the explicit archived BLS dataset | Real, acquired |
| Archived-response replay | `data/public/delivery-audit/` | Offline replay of previously archived real HTTP responses | Real, re-verified |
| External history configuration | `configs/external_history_v1.yaml` | Names every archive layer, the extraction bounds, clock mode, age caps, horizons and masking rules | Configuration |
| Kalshi trades | `data/external/kalshi-trades/trades-*.parquet` | 16 shards, 154,505,005 trade rows, 2021-06-30 through 2026-01-29 UTC | Real, acquired, CC-BY-4.0 |
| Kalshi market metadata, vendor archive | `data/external/kalshi-trades/markets-*.parquet` | 4 shards, 17,464,713 snapshot rows. Layer `kalshi_markets`, one of the two declared observation paths for the contract universe rather than its sole source | Real, acquired, retrospective metadata only |
| Kalshi market metadata, own live capture | `data/external/kalshi-own/markets/markets-*.parquet` | 1 shard. Layer `kalshi_own_markets`, the second declared observation path; capture began 2026-09-16 | Real, locally captured public data |
| Polymarket v1 | `data/external/polymarket-v1/` | `OrderFilled` (1,201,580,990 raw fills), `daily_aligned` (601,934,424 cleaned Standard Binary rows), `daily_aligned_multi` (144,175,988 cleaned Neg Risk rows), `CTF` (838,688,922 lifecycle records) | Real, acquired, CC-BY-4.0 |
| Forecast snapshots | `data/external/forecast-snapshots-kalshi_events-768472771c/snapshot_dataset.parquet` | 20,259 rows, 738 distinct markets, 102 snapshot dates, 2025-01-01 through 2025-10-31 | Real, low-frequency examples, MIT |

`data/` is git-ignored and never packaged. Raw data stay local in this project. No
redistribution or license is assumed for them here.

The primary audit and the replay are both real-input artifacts and they differ in
how they were produced. The primary audit reached the public venue over the network
and read first releases from the named sealed dataset. The replay read previously
archived responses from `data/public/g0-policy/raw` and opened no socket. The
primary audit is the reference for the cohort and the reproduction.

## Original releases

Ten releases: five CPI and five Employment Situation, January through May 2025.
`data/public/bls-normalized/releases.parquet` carries one row per event with
`row_count: 10` in its manifest, `schema_version: "1"`, and content hash
`bfa14ecf7247dba20c35fe349496167aab28b63589e94902add92eccaf59c191` under coverage
epoch `original_bls_browser_captures`.

Each row holds the published statistics, the reference period, the scheduled
instant, and the raw hash of the payload the values were parsed from. For
`cpi_2025_01` the values are the headline and core CPI changes and the CPI-U index
level, all read as first-release values with `revision_status: initial` and an empty
revisions record.

Two times are recorded per row, and they are far apart. The scheduled instant for
`cpi_2025_01` is `2025-01-15T13:30:00+00:00`. The receipt time is
`2026-09-13T13:03:27.644Z`. The row's `availability_quality` is `unknown`, its
`availability_basis` is `late_archived_browser_capture`, and its `usable_time` is
null.

The payloads came through a standard browser HTTP response, recorded per receipt as
`acquisition_method: standard_browser_http_response` with `payload_complete: true`
and `source_availability: unknown_historical`. The importer refuses a questionable
payload rather than importing it: it raises when the receipt status is not 200, when
the payload is incomplete, when the body does not end with `</html>`, when no values
parse, or when the payload's own embargo line disagrees with the cohort calendar.

## Primary audit

`data/public/final-audit/` is a bounded CLI `audit` run. Its own record in
`verification.json` names the mode
`live_read_only_venue_requests_with_explicit_archived_bls`, ten events, ten original
releases, 400 candidate pointers and record hashes verified, 64 unique candidate
tickers, seven raw candidate pages verified, zero study eligible, status `partial`,
and `complete: false`. The CLI exited 2 because the run's own result reports a
blocked G0, not because a request failed.

The release path is explicit and recorded. `release_source.json` reports
`kind: sealed_release_dataset`, `explicitly_selected: true`,
`network_release_requests_issued: false`, `fallback_to_network_used: false`, and
`historical_market_rule_versions_certified: false`. Its ten records each carry
`values_verified_against_original_bytes: true` and
`schedule_agreement: agrees_with_calendar`.

Store contents: 712 receipts over 431 distinct payloads, 421 from Kalshi public GET
responses and 10 release records from the sealed dataset. `raw_hashes.json` names
427 hashes, all labelled `kalshi_or_bls_public_get`, and all 10 release payload
hashes are among them. Four stored payloads are not named there; all four are Kalshi
listing pages. The quality report records 431 blobs, 431 referenced by a receipt,
zero orphans, and HTTP statuses of 702 at 200 and 10 unrecorded, the ten being the
release records no request produced.

## Scoped policy-market acquisition

The primary policy cohort is configured rather than discovered, and all four
configured series were queried: `KXFED`, `FED`, `KXFEDDECISION` and `FEDDECISION`.

The configured relation is `economic_exposure`: a release updates the information
set a later policy decision is made against, so a contract paying on that later
decision carries exposure to the release. The configuration states that this is a
hypothesis about a later payoff and not a verified equivalence between the release
and the policy contract. No payoff equivalence is claimed.

Coverage: 52,895 attempted records across the ten events, 51,700 after per-event
deduplication, 400 deep-audited market-event candidates covering 64 unique tickers,
and 400 lifecycle-eligible policy-series contracts. All 400 are study-eligible count
0. The selection is bounded by the audit's own caps, and the
`candidate_selection_unbounded` gate is unsatisfied for every event, so the selected
contracts cannot be called representative of the candidate set.

## External historical archives

The four external archive layers were measured locally rather than taken from their
READMEs, and three findings change how they may be used. The live capture layer
`kalshi_own_markets` is this repository's own acquisition rather than an external
archive, and it is measured below with the candidate universe.

**Kalshi trade shards are not time partitions.** The dataset README states the 16
shards are sorted by `created_time`, which reads as a calendar partition. They are
source batches with overlapping ranges: `trades-0000.parquet` spans 2022-07-01 to
2024-12-23 and `trades-0001.parquet` spans 2021-12-17 to 2026-01-03. Selecting
shards by footer bounds therefore prunes nothing for a 2025 window, and the real
pruning happens at row-group level inside each shard. The extraction cost is
recorded as a measured cost rather than promised away.

**The archived price range disagrees with the README.** The Kalshi README documents
`yes_price`/`no_price` as 1-99 cents. In a single shard there are 37,918 zero-cent
rows and 358,480 rows where `yes_price + no_price != 100`. The loader reports these
as flags and never clips a price to a documented range.

**Substring series matching is unsafe.** Kalshi event identifiers embed hexadecimal
suffixes, so matching candidates on `FED` returns mostly sports markets, for example
`KXMVENFLSINGLEGAME-S2025FED4B0DA5B1` (Monday Night Football). The coverage stage
matches on exact ticker prefix identity instead.

**Polymarket orientation is derivable and was verified.** On a sampled
`daily_aligned` partition, `outcome_seq` is 1 or 2, `D` is -1 or 1, and every row
agrees with the documented rule (`outcome_seq == 1 -> price`, else `1 - price`). The
loader recomputes and validates the event axis anyway, and derives nothing from
`winning_outcome_label` or `resolution_status`.

**What is quarantined.** The vendor archive's market metadata layer
(`kalshi_markets`) carries `status`, `result`, `yes_bid`/`yes_ask`, `last_price`,
`volume` and `open_interest` that are retrospective with no receipt record, so none
may become a historical feature and no metadata fetch date is asserted for that
layer. Its `created_time` is a snapshot time on heterogeneous batches, not a market
lifetime: `markets-0001.parquet` covers a single day. The forecast snapshots'
`community_pred_*` columns, resolution flags and `resolution` are future labels, all
20,259 `model_pred_now` values are null, and the layer is kept as a separate
low-frequency example set outside the intraday panel.

**What these archives do not contain.** No layer holds order-book snapshots,
quotes, cancellations or resting depth, so spread, depth and quote-coherence
recovery stay disabled. No trade row carries receipt evidence, so availability is
unknown and no usable interval is established. The cleaned Polymarket layers omit
token quantity, so quantity-weighted flow stays disabled until a defensible
reconstruction exists.

**What study execution measured on these archives.** Three facts, each recorded
in an artifact rather than inferred, and each one narrows what this data can carry.

*The declared policy series are four, and the candidate universe is declared per
release.* `FED`, `FEDDECISION`, `KXFED`, `KXFEDDECISION` — 689 contracts in total.
A contract is a candidate for a release when its own recorded listing interval
covers the release instant, which is pre-event information only. The universe is the
union of the two declared observation paths, `kalshi_own_markets` and
`kalshi_markets`, never their intersection: a contract either path observed is a
candidate, and membership is not conditioned on presence in the vendor archive. Each
row carries which path or paths observed it, as `archived_only`, `live_only` or
`archived_and_live`. Measured on this checkout the 689 split **526 `archived_only`,
0 `live_only`, 163 `archived_and_live`**. The zero `live_only` count is why the
change is inert for the retrospective 2025 cohort while remaining necessary for a
forward window the vendor archive cannot cover.
`src/market_propagation/ingest/kalshi_universe.py` is the single definition of this
universe and decides no eligibility; the membership rule above is unchanged and was
already observation-source agnostic. Measured over the ten development releases:
**785 declared release-contract pairs**, of which 697 never traded in the window.
The grid is the denominator, so an untraded contract keeps its masked rows.

*The exposure graph is built over real contracts, and the blocker is rule
evidence.* `neighbors.build_neighbor_graph` now requires a calendar declared
independently of the contracts (`configs/neighbor_graph_v2.yaml`), and the
predicate is read from the venue's own archived contract text
(`ingest/policy_predicates.py`) rather than from a ticker. Measured: 155 contracts
carry a readable predicate and a dated month; 1,550 receiver decisions; **785
`rule_vintage_unverified`**, 480 `resolved_before_forecast_origin`, 285
`receiver_not_open_at_origin`; **0 edges**. The 785 refusals come from the
receiver's own rule check, which runs before any donor is sought, so they do not
establish that a donor exists. That was measured separately: with the rule
requirement satisfied by a diagnostic sentinel that is explicitly not evidence,
the same declared calendar, predicates, liveness and window rules admit **623
edges** (`.audit/study-v3/structural_probe.json`). All 623 are withheld by the
rule-vintage requirement alone.

*Rule text is obtainable; an in-force interval is not.* The historical endpoint
serves the settled contracts: `GET
/trade-api/v2/historical/markets?event_ticker=KXFEDDECISION-25JAN` returns HTTP 200
with all five January 2025 contracts and their `rules_primary` text. That is the
rule text read in 2026, not evidence of which version was in force during a 2025
release window, and `reports/contract_rule_registry.json`
(`status: frozen_local_unregistered`) carries no per-contract rule record. The
rule-vintage gate therefore stays blocked on an external prerequisite rather than
on an implementation gap, and the block is recorded per contract rather than as an
absent graph.

The complete study panel built from the declared candidate population is 3,925 rows
over **785 declared pairs** and 10 of 10 releases, extracted without a row cap. Every
row is masked: `rule_verified_pairs: 0` of 785 and `valid_rows: 0`. The forecast
panel built on the declared receivers is 785 rows over 107 receivers, of which 12
are valid and none carries a neighbour signal.

## What these inputs cannot support

Each limit below is recorded in an artifact rather than inferred here.

| limit | recorded as |
| --- | --- |
| No verified rule version in force at any release | `verified_rule_version_record_count: 0`; `rule_vintage_gate` unsatisfied |
| No settlement semantics read from a named source | `source_semantics_gate` unsatisfied |
| No study-eligible contract | `study_eligible_downstream: 0`; lifecycle eligible is not study eligible |
| No independent expectation | `consensus_available: false`; `route_attempted: none` |
| No surprise slope | no point-in-time expectation source is available to this project |
| Candle holes and short series | 87 of 200 candle audits have interior holes; 177 do not span the window |
| Candles are not book depth | `candle_resolution_is_not_book_depth: true` |
| No replication cohort | `G5` blocked, evidence class `none` |
| Cross-venue match unverified | `cross_venue_match_status: unverified_polymarket_access_timed_out` |
| Acquisition is not eligibility | `acquisition_is_not_study_eligibility: true` |
| Releases do not certify market rules | `historical_market_rule_versions_certified: false` |

Candles come at 1, 60 and 1440 minutes, the venue's documented resolutions, with no
finer interval available. A candle close carries a quote, not an intra-candle
timestamp or a depth level. Trade counts inside the measurement window are near
zero for the audited policy contracts, and a zero trade count is not evidence that
nothing happened.

A market's own creation and open times date the market, not the rule text a later
fetch returned. Each candidate therefore records `rule_available_at: null` and
`rule_version_verified: false`.

## Provenance chain

Every panel- and card-referenced payload is re-read from its store and re-hashed
before it counts. A record that disagrees with the bytes it cites is reported as
unverified and counts for nothing.

| check | result | artifact |
| --- | --- | --- |
| Candidate pointers resolved against archived bytes, primary audit | 400 verified, 0 refused | `final-audit/verification.json` |
| Candidate origins resolved, all acquired records | 52,895 verified, 0 refused | `final-audit/coverage.json` |
| Release records verified against the original bytes | 10 of 10 | `final-audit/release_source.json` |
| Release payload hashes named by the store manifest | 10 of 10 | `final-audit/raw_hashes.json` |
| Cited hashes re-verified for the assembled `cpi_2025_01` card | 187 cited, 0 failed | `reports/event_card.json` |
| Replayed candidate pointers resolved, earlier offline replay | 400 of 400 | `delivery-audit/replay-verification.json` |
| Raw-store blob integrity, primary audit | 431 blobs, 431 referenced, 0 orphans | `final-audit/quality.json` |

## Synthetic inputs, kept separate

The reproduction's sample is generated. Its fixture states that every date,
statistic, price and size in it is invented, that it is not observed BLS, market or
venue data, and that no record in it replaces one of the ten cohort releases.

The manifest classifies each run. This reproduction records
`classification: synthetic_software_methods_reproduction` with `synthetic: true`,
`study.empirical_status: unestimated`, and
`study.synthetic_experiments_are_empirical: false`. Its registry holds no locked-test
reservation and no event claim, so no real locked cohort was consumed.

Synthetic and real data stay in separate output paths and are never substituted. A
synthetic run can satisfy a gate whose required evidence is generated data. It
cannot satisfy a gate whose required evidence is real data or a real effect
estimate.

## Rights and access

No paid data purchase, funded account, credential, or order route is used. Market
access is read-only and the public clients issue documented GET requests. Recorded
paid spend is zero. Redistribution and licensing of the acquired raw data are not
assumed here; the raw bytes stay local and are not packaged.

## Where to read more

| document | what it covers |
| --- | --- |
| `reports/data_audit.md` | What the acquired artifacts hold and which gates stay unsatisfied |
| `reports/paper.md` | The methods draft and the blocked empirical prerequisites |
| `reports/reproduction_guide.md` | Rebuilding the synthetic reproduction and re-verifying the real artifacts |
| `reports/preregistration.md` | The locally frozen specification and its freeze-time status |
| `reports/literature_matrix.csv` | The 19 sources with retrieval status and source URLs |
