# Data audit

What the acquired artifacts actually contain, which of them were read by this
repository, and what the resulting coverage can support. Every count and hash below
was read from an artifact named in the section that reports it. Nothing here is an
estimate of a market response.

## Classification

The study is feasibility and identification limited at this revision. The real-input
side reaches ten release events and 400 lifecycle-eligible policy contracts, and it
records zero study-eligible contracts, no independent point-in-time expectation, and
no replication cohort. No empirical response, propagation, power, or profitability
claim follows from the artifacts audited here.

## The two real audits

Two audit directories exist, and the difference between them decides what each can
support.

| directory | how it was produced | what it establishes |
| --- | --- | --- |
| `data/public/final-audit/` | A bounded CLI `audit` run that reached the public venue over the network and read first releases from the named archived BLS dataset | The primary evidence. What the venue returned at audit time, with the original release bytes verified offline |
| `data/public/delivery-audit/` | An offline replay of previously archived real HTTP responses, with no network | The earlier replay. What those archived responses contain, re-verified |

`final-audit` is the primary reference for the cohort and the reproduction. It is a
live read-only acquisition alongside an explicitly named archived release dataset,
recorded in `data/public/final-audit/verification.json` as
`live_read_only_venue_requests_with_explicit_archived_bls`.

`delivery-audit` records the earlier offline replay in
`data/public/delivery-audit/replay-verification.json`, with mode
`archived_real_http_response_replay_no_network` and input raw store
`data/public/g0-policy/raw`. It is kept as the archived-response replay and is not
the primary cohort evidence.

Both audits reach the same standing: status `partial`, `complete: false`, the same
cohort definition hash `e3914ccfaa1b48ea4ec3973f5849058eabd0e121d92a0349ff45a1132985338d`,
zero study-eligible contracts, and the same three per-event gate names unsatisfied.

## The cohort

The cohort is the first five 2025 CPI releases and the first five 2025 Employment
Situation releases, ten events in total, from `configs/cohort.yaml`. The selection
basis recorded there is the first five monthly releases of 2025 with official
calendars read, selected without reference to outcomes.

| event_id | family | reference period | scheduled instant (UTC) | release title |
| --- | --- | --- | --- | --- |
| `cpi_2025_01` | cpi | 2024-12 | 2025-01-15T13:30:00+00:00 | Consumer Price Index News Release, 2024 M12 Results |
| `cpi_2025_02` | cpi | 2025-01 | 2025-02-12T13:30:00+00:00 | Consumer Price Index News Release, 2025 M01 Results |
| `cpi_2025_03` | cpi | 2025-02 | 2025-03-12T12:30:00+00:00 | Consumer Price Index News Release, 2025 M02 Results |
| `cpi_2025_04` | cpi | 2025-03 | 2025-04-10T12:30:00+00:00 | Consumer Price Index News Release, 2025 M03 Results |
| `cpi_2025_05` | cpi | 2025-04 | 2025-05-13T12:30:00+00:00 | Consumer Price Index News Release, 2025 M04 Results |
| `empsit_2025_01` | employment | 2024-12 | 2025-01-10T13:30:00+00:00 | Employment Situation News Release, 2024 M13 Results |
| `empsit_2025_02` | employment | 2025-01 | 2025-02-07T13:30:00+00:00 | Employment Situation News Release, 2025 M01 Results |
| `empsit_2025_03` | employment | 2025-02 | 2025-03-07T13:30:00+00:00 | Employment Situation News Release, 2025 M02 Results |
| `empsit_2025_04` | employment | 2025-03 | 2025-04-04T12:30:00+00:00 | Employment Situation News Release, 2025 M03 Results |
| `empsit_2025_05` | employment | 2025-04 | 2025-05-02T12:30:00+00:00 | Employment Situation News Release, 2025 M04 Results |

The cohort spans the March 2025 daylight-saving transition. Releases from
`cpi_2025_03` onward carry `daylight_saving_in_effect: true` and an offset of
`-04:00`, and the earlier releases carry `-05:00`. A scheduler that fires on a fixed
host time does not follow that transition. See the capture template in
`configs/scheduled-reports.cron`.

The independent unit is the release. There are ten of them, so the effective sample
for any release-level statement is ten, not the number of contracts or records
listed below.

## Original sources and the two times

Each event cites its own official BLS sources. For `cpi_2025_01` the card records:

| field | value |
| --- | --- |
| calendar URL | `https://www.bls.gov/schedule/2025/01_sched_list.htm` |
| initial release URL | `https://www.bls.gov/news.release/archives/cpi_01152025.htm` |
| scheduled instant | `2025-01-15T13:30:00+00:00` |
| embargo line read from the payload | `2025-01-15T13:30:00+00:00` |
| schedule agreement | `agrees_with_calendar` |
| observed publication instant | `null` |
| revision status | `initial` |
| time precision | `minute` |
| USDL number | `USDL-25-0021` |

Two distinct times matter, and neither is a publication observation.

The embargo instant comes from the archived payload's own embargo line. The card
records `embargo_is_not_observed_publication: true` and
`release_embargo_evidence_kind: source_claim_about_schedule`, with
`observed_publication_unavailable_reason` stating that a payload fetched after the
event cannot establish when the material first became public. The scheduled instant
is admitted only as a documented schedule, and only where the payload's own embargo
line agrees with the calendar. It does, for all ten events.

The receipt time is when this project read the bytes. The browser archive receipts
under `data/public/bls-browser/` record receipt times on 2026-09-13, between
13:03:27Z and 13:07:38Z, and each carries `status: 200`,
`payload_complete: true`, and `source_availability: unknown_historical`. The
normalized dataset records the same instants per event, with
`availability_quality: unknown` and `availability_basis:
late_archived_browser_capture`.

The gap between the release and the receipt is roughly twenty months. A capture made
after the event cannot support a latency claim, and this audit makes none. Each
release record in `release_source.json` leaves `usable_time` null with the note that
the original capture happened long after publication, so no interval in which the
payload was certainly usable is established and none is invented.

The venue records carry a different clock. The `cpi_2025_01` event reports a live
`latency_seconds` of 88.67, which is how long that event's own acquisition took
during the run on 2026-09-14.

## How the original releases reach the event card

The original BLS first releases appear in the event card because the audit was
pointed at the archived dataset explicitly.

`audit --release-dataset data/public/bls-normalized/releases.parquet` makes the
release client read first releases from that sealed dataset and its sibling `raw/`
store instead of requesting them from the network. The dataset is not discovered by
the audit; with no `--release-dataset` the network path is unchanged.

The evidence this leaves behind is explicit. `data/public/final-audit/release_source.json`
records `kind: sealed_release_dataset`, `explicitly_selected: true`,
`network_release_requests_issued: false`, `fallback_to_network_used: false`, and
`historical_market_rule_versions_certified: false`. Each of its ten records carries
`values_verified_against_original_bytes: true` and
`schedule_agreement: agrees_with_calendar`.

What the network path leaves behind is different. An audit run without the dataset
requests each release from the public archive, is answered with HTTP 403, and records
those refusals as non-2xx receipts rather than replacing a release with an empty one.
That is what the earlier live audit at `data/public/g0-policy/` shows: 20 refusals,
all BLS archive requests, with `release: null` and an unsatisfied
`release_payload_archived` gate.

In the primary audit the BLS archive is not requested at all, so its quality report
records no non-2xx receipts. `data/public/final-audit/quality.json` reports 712
receipts over 431 content-addressed blobs, 431 referenced by a receipt, zero orphan
blobs, and HTTP statuses of 702 at 200 and 10 unrecorded. The 10 unrecorded are the
sealed-dataset release records, which carry no HTTP status because no request
produced them.

Naming the dataset changes where a release comes from. It changes nothing about
market rule versions, candle coverage, or study eligibility, and
`release_source.json` states that directly: the original BLS bytes are evidence about
the release alone, and do not certify which market rule version was in force at the
release or the quote coverage of any contract.

## How the archived responses were replayed

The earlier `delivery-audit` is an offline replay of previously archived real HTTP
responses, and it is not a new live acquisition.

The replay harness at `.audit/replay-delivery-audit.py` reads the receipts already
stored under `data/public/g0-policy/raw`, rebuilds an `httpx.Response` for each one
with its archived status, content type and body, and hands that client to the same
`CohortAuditor` the `audit` command uses. The result is written to
`data/public/delivery-audit/`. No socket is opened during the replay.

The same harness passes `release_dataset=data/public/bls-normalized/releases.parquet`
explicitly, so that replay also reads first releases from the sealed dataset rather
than requesting them. That is why its release records carry
`acquisition_method: sealed_release_dataset` and no HTTP status.

`data/public/delivery-audit/replay-verification.json` reports the replay's own
summary:

| field | value |
| --- | --- |
| mode | `archived_real_http_response_replay_no_network` |
| input raw store | `data/public/g0-policy/raw` |
| events | 10 |
| original releases | 10 |
| candidate pointers verified | 400 |
| study eligible | 0 |
| status | `partial` |
| complete | `false` |

After the audit body ran, the harness re-resolved the RFC 6901 pointer of every
candidate against the archived page its record cites and asserted the resolved
record's own ticker matched. All 400 resolved.

That replay narrowed its series query to two of the four configured policy series:
`coverage.json` in that directory records `series_override_applied: true` with
values `KXFED` and `KXFEDDECISION`. The configured `FED` and `FEDDECISION` series
were not queried by that replay. That is a coverage limit of the archived store, and
it is not a change to the configuration. The primary audit queries all four.

## What the primary store holds

`data/public/final-audit/verification.json` reports the run:

| field | value |
| --- | --- |
| entrypoint | `market-propagation audit` |
| mode | `live_read_only_venue_requests_with_explicit_archived_bls` |
| CLI exit code | 2 |
| events | 10 |
| original releases | 10 |
| candidate pointers and record hashes verified | 400 |
| unique candidate tickers | 64 |
| raw candidate pages verified | 7 |
| study eligible | 0 |
| status | `partial` |
| complete | `false` |

Exit 2 here means the run completed and reported a blocked result, not that access
failed. The verification record shows zero access blockers and no per-event errors.

`data/public/final-audit/raw/` holds 712 receipts over 431 distinct
content-addressed payloads: 421 from Kalshi public GET responses, covering the
policy listings, the series listings and the candles, and 10 release records read
from the sealed dataset.

`data/public/final-audit/raw_hashes.json` names 427 hashes and labels every one
`kalshi_or_bls_public_get`. All 10 release payload hashes are named in that list. Of
the 431 stored payloads, 4 are not named there, and all 4 are Kalshi listing pages
(three event listings and one series listing). A hash being absent from
`raw_hashes.json` means the audit did not cite that payload for the cohort; it does
not mean the bytes are missing, because all four are retrievable from the store.

Candidate provenance is recorded per run in `coverage.json`:

| field | value |
| --- | --- |
| candidates verified | 52895 |
| records refused | 0 |
| distinct refusal reasons | 0 |
| page bodies parsed | 215 |
| verification | `RawStore.get(page raw_hash)`, `json.loads`, RFC 6901 pointer resolution, then the resolved record's own digest compared against the cited record |
| scope | `single_run_in_memory` |

The `cpi_2025_01` card reports 187 cited hashes re-verified with 0 failures. The card
command re-reads each cited payload against its content hash and issues no request.

The run also records its own request reuse: 391 distinct request identities and 1993
requests served from the in-memory cache. Nothing is persisted between runs, and the
partition cutoff is deliberately not cached, so a cutoff that moves during a run is
visible in the result.

## Scoped policy-market acquisition and its limits

The primary policy cohort is configured, not discovered. Its record in
`coverage.json` states the relation and the limit together.

| field | value |
| --- | --- |
| family | `policy_rate_decision` |
| relation type | `economic_exposure` |
| strike dependency | `later_policy_decision` |
| configured series | `KXFED`, `FED`, `KXFEDDECISION`, `FEDDECISION` |
| queried series | `KXFED`, `FED`, `KXFEDDECISION`, `FEDDECISION` |
| verified contract count | 0 |
| verified payoff equivalence claimed | `false` |

The exposure mechanism recorded there is that a release updates the information set
a later policy decision is made against, so a contract paying on that later decision
carries economic exposure to the release. The same record states that this is a
hypothesis about a later payoff, not a verified equivalence between the release and
the policy contract.

A series joins the policy cohort only on positive evidence. Discovery admits a
candidate on a US Federal Reserve settlement source together with an economics
category and a policy-rate or policy-meeting title.
`data/public/final-audit/series_discovery.json` records the same 295 verdicts, 22
admitted series, and 273 exclusions as the replay, with reasons such as
`not_us_policy_source` and `foreign_or_non_us_inflation`. Discovery states
`complete_universe_claimed: false` and describes itself as a keyword candidate set
over the exchange's own listing rather than a proven complete universe.

The four direct-release series `KXCPI`, `KXCPIYOY`, `KXPAYROLLS` and `KXU3` are kept
separate from the policy cohort rather than pooled into it. The other candidate
families are excluded with the reason that they are not the primary policy cohort.

Coverage counts for the primary store, from `coverage.json`:

| field | value |
| --- | --- |
| attempted records | 52895 |
| deduped candidates | 51700 |
| downstream candidates | 400 |
| eligible downstream | 400 |
| policy series lifecycle eligible | 400 |
| policy series study eligible | 0 |
| selected for deep audit | 400 |
| candidate bound | `candidate_selection_unbounded` gate unsatisfied |

The run applied the bounded caps `max_pages=2`, `max_contracts_per_event=40` and
`max_candle_contracts_per_event=10`. The 400 downstream candidates are 40 per event
over 10 events, selected under those caps, and the verification record reports 64
unique tickers across them. They are not 400 independent observations, and the
audit's own `candidate_selection_unbounded` gate is unsatisfied for every event,
blocking any claim that the deepest-audited contracts represent the whole candidate
set.

The 52,895 attempted records are acquisition attempts across the ten events, not
observations, and 51,700 remain after per-event deduplication. The audit states that
`acquisition_is_not_study_eligibility: true`, and its acquired-coverage gate says
explicitly that the count is acquisition, not empirical eligibility. The run
completed in 2026-09-14T07:28:03Z to 07:31:24Z.

## Missing rule-version evidence

No contract reached study eligibility, and the reason is recorded rather than
inferred.

| field | value |
| --- | --- |
| evidence kind | `configured_rule_version_record` |
| verified rule-version records | 0 |
| lifecycle eligible downstream | 400 |
| lifecycle eligible without a verified rule version | 400 |
| study eligible downstream | 0 |
| `rule_vintage_gate` | unsatisfied |
| `source_semantics_gate` | unsatisfied |

The required evidence per contract is `contract_id`, `rule_hash`, `source_url`,
`verified_by`, `in_force_from`, `observed_at`, and `settlement_semantics`. None of
that is configured for this cohort.

Each candidate does carry a `rule_hash`, and the card prints a rule meaning such as
`YES pays 1 if the reference statistic is > 2.75 in greater terms` for
`FED-25DEC-T2.75`. What is absent is any binding between that hash and an interval
in force at the release. The gate detail states the reason plainly: a market's own
creation and open times date the market rather than the rule text a later fetch
returned, so with nothing binding a fetched rule hash to an interval in force at the
release, no lifecycle-eligible contract becomes verified semantics. Each candidate
records `rule_available_at: null` with basis
`unknown_no_verified_rule_version`, and `rule_version_verified: false`.

Two claims stay blocked by these gates. No payoff-equivalence claim between a
release contract and a policy contract is available, and no contract's payoff can be
read as the rule in force at the release.

## Candle gaps and resolution

Candle coverage is the second real limit, and the cards record it per contract.

Across the ten events the primary store holds 200 candle audits, 20 per event.
Requested intervals are 60 minutes and 1 minute, the two finest the venue documents
alongside 1440 minutes. Of the 200 audits, 87 contain interior holes and 177 do not
span the requested window.

For `cpi_2025_01` the card reports `candle_resolution_by_contract` covering the 20
queries. The unsatisfied gates are
`candle_series_on_grid_without_interior_holes` and
`candle_series_spans_requested_window`, each blocking quote reconstruction across
the affected window and any statement about quote activity outside the returned
range.

Two properties are stated by the artifacts and constrain interpretation. A candle is
a frequency observation and not order-book depth, so the store cannot support an
order-book reconstruction or an intra-candle quote timestamp. The public order-book
snapshot also carries no sequence number, so it cannot close a sequence gap. Trade
counts inside the window are near zero for the audited policy contracts; for
`cpi_2025_01` only one of the twenty contracts recorded any trade at all, at two
trades. A quote can update without a trade, so a zero trade count is not evidence
that nothing happened, and an unchanged candle is not evidence of no news.

The measurement window runs from 30 minutes before the scheduled instant to 60
minutes after it, which for `cpi_2025_01` is 13:00Z to 14:30Z. The card records
`window_seconds_before: 1800` and `window_seconds_after: 3600` for that reason.

## No independent expectations

No surprise is estimable from these artifacts, and the cards say so per event.

`consensus_available` is `false` for `cpi_2025_01`, and its `missing_expectations`
record reports status `unavailable`, `route_attempted: none`,
`midpoint_substituted_for_bucket: false`, `revised_series_substituted: false`, and
`vendor_consensus_assumed_free: false`. The recorded reason is that no licensed
point-in-time consensus is available to this project, no forecast archive was
prespecified, and no pre-release market-implied distribution was captured.

`configs/cohort.yaml` keeps the two consequences apart. An expectation source is
required only before a surprise slope is estimated,
`expectation_gate_scope: surprise_slopes_only`. Its absence leaves every event
eligible for the event-timing study, which is defined from the scheduled release
time and observed quotes and needs no expectation source. So the missing expectation
blocks a surprise slope and does not by itself block a timing study.

## No replication

There is no independent replication cohort and no verified cross-venue equivalent
pair. The reproduction's `G5` gate is blocked with evidence class `none`, and the
cohort configuration records `cross_venue_match_status:
unverified_polymarket_access_timed_out`. Under the specification, a failed rule
match blocks cross-venue pooling and a missing replication cohort leaves H5
unevaluated.

## What the counts do and do not mean

Three numbers invite the wrong reading, and the artifacts themselves distinguish
them.

The 52,895 attempted records are acquisition attempts across ten events, not
observations; 51,700 remain after deduplication, and 400 were deep audited. The 400
downstream candidates are a bounded selection from a capped walk, 40 per event,
covering 64 unique tickers, not 400 independent events; the audit's own
`candidate_selection_unbounded` gate is unsatisfied, so the selected contracts
cannot be called representative. The 400 lifecycle-eligible contracts are not
study-eligible contracts; that distinction is carried as
`lifecycle_eligible_is_not_study_eligible: true` and as a separate study-eligible
count of 0.

The audit's own summary field says the same thing about the whole exercise:
`acquisition_is_not_study_eligibility: true`. A successful HTTP response, or a
successful offline replay of one, is never study eligibility.

## Standing and what stays blocked

`data/public/final-audit/coverage.json` reports status `partial` with
`complete: false`. Its unsatisfied gates are 30 event-gate pairs, three gate names
over ten events, plus two unsatisfied scientific gates, `rule_vintage_gate` and
`source_semantics_gate`, which are cohort-wide rather than per event.

Every claim about a post-release quote response, a causal attribution of a price
movement to a release, an expectation-relative response, or an order-book
reconstruction stays blocked. What the artifacts do support is the candidate set
the venue listed at audit time with its attempted, acquired and eligible sizes
reported separately, the archived first-release values with the payload hash each
came from, and the requested versus observed candle resolution for the contracts
attempted.

## Where to read the evidence

| artifact | what it holds |
| --- | --- |
| `data/public/final-audit/verification.json` | The primary run's own summary |
| `data/public/final-audit/coverage.json` | Cohort sizes, gates, limitations, eligibility |
| `data/public/final-audit/release_source.json` | The sealed release dataset, its ten verified records and their provenance |
| `data/public/final-audit/raw_hashes.json` | The 427 named payload hashes |
| `data/public/final-audit/series_discovery.json` | Policy series admissions and exclusions |
| `data/public/final-audit/quality.json` | Receipt, blob and HTTP status counts for the raw store |
| `data/public/final-audit/event_card.json` | The audit's own per-event card |
| `data/public/delivery-audit/replay-verification.json` | The earlier offline replay summary |
| `data/public/bls-normalized/releases.parquet` | The ten normalized first releases, with its `.manifest.json` |
| `reports/event_card.json` | A card assembled for `cpi_2025_01` through the CLI |
| `reports/data_card.md` | The data card for the same inputs |
| `reports/reproduction_guide.md` | How to rebuild and re-verify these artifacts |

`reports/event_card.json` is a different artifact from the card the audit writes
into its own output directory. It is the command's own card, assembled for one event
through the `event-card` command. For `cpi_2025_01` it cites 187 payload hashes with
0 failures, reads its release from the named sealed dataset, and names
`data/public/final-audit/` as its source audit directory.
