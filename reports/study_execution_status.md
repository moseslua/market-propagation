# Study execution status

Durable checkpoint for the empirical information-diffusion study execution.
Updated at each stage boundary; the newest entry is at the top of the log.

## Authority and freeze

| Item | Value |
| --- | --- |
| Execution authority | `prediction_market_information_diffusion_study_execution_plan.md` |
| Plan SHA-256 at handoff | `0d26f0cdbcb3b52505cac44dd978980df4f01ca55932ab21d091a4ac5885f469` |
| Verified at execution start | yes, hash re-computed and matched |
| Continuation brief | `.audit/astra-repair/OMP_CONTINUE.md`, SHA-256 `6e407326285354cdeef511a93441ee483455b79b08ceb1c6b4a88c9a68d9891d` |
| Worktree | `/Users/moses/orca/projects/information-diffusion` |
| Spend | zero; public read-only access only |

The repository is untracked, so a Git commit hash is not a sufficient input
identity. Every artifact this execution writes records its own content hash and
the hash of the inputs it read, and the v2 run records a `source_hash` computed
over the actual bytes of the modules and configurations that produced it.

## Frozen files, and what v2 adds

The v1 files stay frozen and unchanged beside the v2 freeze; nothing below edits
them.

| File | Role |
| --- | --- |
| `configs/external_history_v1.yaml` | frozen v1 pipeline, clock and caps |
| `configs/cohort.yaml`, `configs/event_windows.yaml`, `configs/study_v1.yaml` | frozen v1 declarations |
| `configs/study_v2.yaml` | v2 clock, estimands, caps, feature map, ladder, splits, seeds |
| `configs/cohort_v2.yaml` | four declared policy series, declared candidate grid, receiver and donor rules, rule-vintage requirement |
| `configs/event_windows_v2.yaml` | v2 windows including the donor read and the own-lag window |
| `configs/neighbor_graph_v2.yaml` | declared decision calendar, predicate forms, rule-vintage record shape, calibration status |

## Corrections to the earlier status

Three conclusions in the previous revision of this file are withdrawn because the
evidence behind them does not support them.

| Withdrawn claim | Why it is withdrawn | Replacement finding |
| --- | --- | --- |
| "Kalshi lists one decision date at a time, so no matched pair is ever simultaneously listed" | The archive's `open_time` is the instant the *previous* meeting resolved, not the listing instant, and only one series was examined. The claim was a property of a metadata artefact, not of the venue. | 155 contracts carry a readable policy-rate predicate and a dated month; under the declared calendar 623 edges are structurally admissible, and the graph withholds them on rule vintage rather than on listing structure |
| "`FED-{YY}{MMM}-T{threshold}` level contracts fail empirically: 1 of 110" | The candidate set omitted the declared `FEDDECISION` series and the threshold list was ad hoc rather than declared. | Candidates are now the declared four-series grid: 689 contracts, 785 declared release-contract pairs |
| "`graph=None` … the local archive produces no admissible neighbour" | `graph=None` is a *missing input*, not a measured absence. Treating the two as the same fact reported an unbuilt graph as a result. | A `None` graph is now reported as `neighbor_graph_not_supplied`; the real graph is built over real contracts and its per-contract reasons are written to `graph_decisions.json` |

## Current measured state

### Candidate universe is declared, and the missing cells are kept

`scripts/build_study_panel.py` now passes the per-release declared grid into the
panel builder, so the grid is the denominator. `build_trade_panel` gained a
`candidates` parameter: a declared pair keeps its rows even when it never traded,
and a grid that omits a declared release is refused rather than silently reselected
from activity. The `FEDDECISION` series is a declared policy series and was
previously dropped by a `LIKE 'FED-%'` test; series selection now reuses
`ingest.audit.series_of`.

Measured over the frozen window:

| Quantity | Value |
| --- | --- |
| Declared policy series | 4 (`FED` 347, `KXFED` 142, `KXFEDDECISION` 130, `FEDDECISION` 70) |
| Candidate contracts | 689 |
| Declared release-contract pairs (the denominator) | 785 |
| Declared pairs that never traded in the window | 697 |
| Panel rows | 3,925 |
| Candidate universe flag | `candidate_universe_from_declared_listing_grid` |
| Extraction | 163,795 trades, 16 shards, `bounded: false` |
| Pre-event-observed pairs | 43 |
| Endpoint-observed pairs | 81 |
| `rule_verified_pairs` | 0 of 785 |
| `valid_rows` | 0 of 3,925 |

### The graph is built over real contracts and refuses them per contract

`neighbors.build_neighbor_graph` now requires a declared decision calendar
(`configs/neighbor_graph_v2.yaml`), refuses a calendar date that holds no contract
instead of reaching back a further meeting, requires each contract's rule version
to be verified in force across the whole measured window, and requires both
contracts to be listed from the origin through the window's end. Predicates are
read from the venue's own archived contract text by
`ingest.policy_predicates`, never from the shape of a ticker.

Measured over 10 releases:

| Quantity | Value |
| --- | --- |
| Contracts with a readable predicate | 155 |
| Contracts refused | 534 (533 month not dated by the declared calendar, 1 unreadable payout text) |
| Receiver decisions | 1,550 |
| `rule_vintage_unverified` | 785 (all receiver-side, before any donor is sought) |
| `resolved_before_forecast_origin` | 480 |
| `receiver_not_open_at_origin` | 285 |
| Edges | 0 |
| Graph digest (empty graph) | `4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945` |

The 785 is the receiver's own rule check, which runs before the donor search, so it
does not by itself say whether a donor exists. That question was measured
separately, with the rule gate temporarily satisfied by a **diagnostic sentinel** in
`.audit/study-v3/structural_probe.json`. The sentinel certifies every window and is
explicitly not evidence about any contract's rule vintage; it exists only to size
the gap.

| Structural probe (diagnostic, not evidence) | Value |
| --- | --- |
| Edges admitted | **623** of 155 predicates × 10 releases |
| `resolved_before_forecast_origin` | 628 |
| `no_matched_threshold` | 14 |
| `receiver_not_open_at_origin` | 285 |

So 623 edges are structurally admissible — about 62 per release — and every one of
them is withheld by the rule-vintage requirement alone. The binding constraint is
the rule-vintage record, which is a documentation gap, not the venue's listing
policy and not the trading session.

### The forecast panel is built on declared receivers

`historical_forecast.build_forecast_rows` now takes the declared per-release
receiver set, keeps a quiet receiver's masked row, records the recipient's own
pre-origin increment (`own_lag`) with its staleness envelope, and reports an
unsupplied graph as `neighbor_graph_not_supplied` rather than as an absent
neighbour.

| Quantity | Value |
| --- | --- |
| Forecast rows | 785 |
| Distinct receivers | 107 |
| Releases | 10 |
| Valid rows | 12 |
| Rows with a target | 29 |
| Rows with `own_lag` | 43 |
| Rows with `neighbor_lag` | 0 |
| Content hash | `e57417471b809878ec53a489fd40ffe1f9bd0b99dca551d7af14974d97d6eea0` |

### The ladder is fitted on real rows or blocked with the missing column named

`study.fit_forecast_ladder` maps the sealed forecast columns onto the ladder's
declared feature names and fits each rung on the real rows. Every rung is scored on
one identical sample, so a column one rung needs removes those rows from the others
too; that common gap is reported apart from each rung's own.

| Rung | Status | Reason |
| --- | --- | --- |
| `no_change` | blocked | `the_common_comparison_sample_is_incomplete_because_a_rung_column_is_absent` |
| `own` | blocked | same |
| `news` | blocked | `declared_ladder_columns_absent_from_the_forecast_table` |
| `network` | blocked | same |

Common missing columns: `delayed_shock`, `neighbor_lag`, `neighbor_lag_control`,
`shock`. Note that `neighbor_lag` exists as a column and is null on every row, so
it is reported as unsupplied rather than fitted as a constant.

### Expectations: validated, and absent

`ingest/expectations.py` implements the point-in-time expectation contract: a
forecast published at or after the release, a forecast scored against a revision, a
unit that disagrees with the release's own declared statistic, an unnamed consensus,
a market-implied value, an incomplete news vector and altered evidence bytes are
each refused with their own code. No expectation source exists on this checkout, so
`load_expectations` raises `expectation_source_is_absent` and the news and network
rungs are blocked. The absence is a reported input gap, not a zero-valued surprise.

### Rule vintages: not attested

`reports/contract_rule_registry.json` is a specification document (`status:
frozen_local_unregistered`) and carries no per-contract rule record, so no contract
on this archive has an attested rule version and the graph refuses every edge. The
earlier claim that the venue does not serve 2025 contracts was an artefact of using
the current-markets endpoint: `GET
/trade-api/v2/historical/markets?event_ticker=KXFEDDECISION-25JAN` returns HTTP 200
with all five January contracts and their `rules_primary`/`rules_secondary` text.
That is rule *text read now*, not an in-force interval, and this module is not
entitled to infer a vintage from it. The gate therefore stays blocked, for the
correct reason.

Two dated routes were then searched, and both were measured rather than assumed.

**The Internet Archive is reachable and holds nothing for these contracts.** The
earlier "HTTP 503 on every attempt" record is withdrawn. All 16 CDX queries across
four series, two API hosts and two page hosts returned, and **0 of 155** candidate
contracts has a capture. Six policy-series contracts do have captures and none is a
declared candidate; the only market-API capture naming any declared series is
`KXFEDDECISION-25DEC-T5.25` at 2026-03-26, archived with status **401**, a 585 byte
error page with no rule text. Market-page captures are client-rendered shells with no
contract text in them. Artifact: `.audit/study-v3/rule_archive_coverage.json`; probe:
`scripts/probe_rule_archive_coverage.py`.

**The regulatory-filing route is real, dated and free, but it is per product
template.** Kalshi self-certifies each product with the CFTC under Part 40, and the
CFTC publishes those filings as public PDFs. One was fetched in full
(`ptc09022529868.pdf`): a Regulation 40.2(a) notification dated 2 September 2025
carrying Appendix A, the Terms and Conditions, the Payout Criterion, the Expiration
time and the Settlement Value. It is authoritative, dated and free, which makes it
better shaped than anything else found. It does not close the gate, for three reasons
that are not search failures: it names no strike, event or individual market; the
Fed-decision product family was certified long before the January to May 2025
windows, so its filing date cannot bound a version in force inside them; and no
enumerable index was reachable (`cftc.gov/filings/ptc/YY/MM/` is 404,
`kalshi.com/regulatory/filings` is HTTP 429, rate limited rather than missing).

Whether a rulebook-level filing can attest a contract-level vintage is a question
about the declared evidence standard, not a search problem. Widening that standard is
a decision for the study owner.

### Calibration: run, and a pass

The declared calibration ran at 200 repetitions per primary scenario: simulated
transaction tapes passed through the same graph, observation, feature, fitting,
tuning, paired-uncertainty and promotion code as real data, with simultaneous null
bounds. Certificate `data/calibration/calibration_certificate.json`; registry record
`calibration-2856211b642b-bd7e5e807f5c` in `data/registry/rule_calibration.sqlite3`,
carrying a `source_hash` over the modules and configurations that produced it.

| Quantity | Value |
| --- | --- |
| Repetitions per scenario | 200 |
| Releases per repetition | 120 |
| Workers, seed, bootstrap draws | 9, 20260913, 200 |
| Declared null scenarios | 10 |
| Nulls estimable | 10, each promoted in 0 of 200 repetitions |
| Null one-sided upper bound, at simultaneous level 0.995 | 0.02614, against a 0.05 ceiling |
| Recovery `communication` | promoted in 196 of 200; rate 0.98; one-sided lower bound 0.9548 against a 0.80 target |
| Verdict | `pass` |

The first run reported two of the ten declared nulls as unestimable — `resolution_pause`
and `spread_only` — and both causes were defects in those scenarios' own declarations
rather than in the estimator, the bound or the promotion rule. `spread_only` declares
`news_active=False`, so every contract's declared sensitivity is zero;
`simulated_release_shocks` recovered the generator's per-release common shock as
`latent / (orientation * strength)` and skipped any event whose strength was falsy,
which returned an empty mapping and left the ladder's `shock` and `delayed_shock`
columns filled with nulls, so `nested_comparison` found no complete row. An event whose
roles all carry a declared zero sensitivity now receives an explicit `0.0` shock,
because a declared zero is an exact value and not a missing measurement.
`resolution_pause` declared `pause=(300.0, 900.0)` while the declared forecast settings
are `forecast_origin_seconds=300` and `future_horizon_seconds=300`, so every primary
row's window is `[event+300s, event+600s]` — entirely inside that halt. Every row was
therefore marked halted and its target was null. The declared halt is now
`(700.0, 1000.0)`, which opens after the primary window closes at +600s, so the halt
still invalidates every window that spans it without consuming all of them. Each
defect had blocked 200 of 200 repetitions on a not-run comparison; with both
declarations corrected the family's simultaneous bound is certified for all ten nulls.

The earlier 48-repeat `network_falsification` call stays recorded in the v2
configurations as superseded: it counts a gain-threshold event on the quote/simulator
path, which is not the declared transaction observation process, and 48 repetitions
cannot separate a 0.05 ceiling from a 0.80 target on either side.

This calibrates the decision rule on a synthetic process. It is not evidence about
any real contract, and it does not unblock the primary estimands: the graph over the
archive still admits no edge.

## Terminal findings

* **The primary propagation estimand is blocked on rule-vintage evidence.** 623
  edges are structurally admissible under the declared calendar, predicates,
  liveness and window rules — measured with a diagnostic sentinel that is explicitly
  not evidence — and every one is withheld because no contract has a verified
  in-force rule interval. This is a documentation gap that a per-contract rule record
  would close, and it is not closable from settlement outcomes or current rule text.
* **The primary absorption estimand is blocked on rule evidence too.** 0 of 785
  declared pairs carries verified rule evidence, so the masked panel has no valid
  row and the absorption ladder is fitted on masked rows only.
* **The candidate universe is now declared.** 785 pairs, 697 of which never traded,
  are in the denominator with their missing cells preserved, so a later run cannot
  report a cleaner observed fraction by letting untraded contracts fall out of the
  grid. The universe those pairs are drawn from is the union of the two declared
  observation paths — `kalshi_own_markets`, this repository's own live capture under
  `data/external/kalshi-own/markets/markets-*.parquet`, and `kalshi_markets`, the
  vendor archive under `data/external/kalshi-trades/markets/markets-*.parquet` —
  rather than the archive alone: measured on this checkout, 689 contracts, split 526
  `archived_only`, 0 `live_only` and 163 `archived_and_live`. That is the source the
  candidates are read from, not a change to who is eligible: the declared membership
  rule was already observation-source agnostic, and the 785 declared pairs are
  unchanged by this.

## Stage log

| Stage | State | Note |
| --- | --- | --- |
| S0 reconcile | complete | plan and continuation-brief hashes verified |
| S1 evidence feasibility | complete | ledger in `reports/source_feasibility.md` |
| S2 cohort extraction | complete | declared grid enforced; four series; `FEDDECISION` restored |
| S3 graph and forecast panel | complete | declared calendar, rule-interval and window liveness, contract-level refusals, declared receivers |
| S4 analysis integration | complete | ladder fitted on real forecast rows, blocked rungs named; `study-external` accepts `--forecast-panel` and `--registry` |
| S5 design lock | complete | v2 configs and `reports/preregistration_v2.md` frozen |
| S5 calibration | complete | 200-repetition transaction-tape calibration run; verdict `pass`, all ten declared nulls estimable at 0 of 200 repetitions each, upper bound 0.02614 at level 0.995; two declaration defects fixed before the rerun; superseded 48-repeat run recorded as insufficient |
| S6 empirical evaluation | not opened | no valid primary panel row, so no test release was reserved |
| S7 robustness | partial | exclusion accounting and missingness run; estimate-dependent analyses unrun and reported as unrun |
| S8 final package | complete | acceptance run below; reports corrected |

## Acceptance run (fresh directory)

`uv run --no-sync market-propagation build-trade-panel … --candidate-grid
.audit/acceptance/v2/candidate_grid.json` followed by
`uv run --no-sync market-propagation study-external --panel … --forecast-panel …
--registry …`, both into `.audit/acceptance/v2/`.

| Check | Result |
| --- | --- |
| Panel build exit code | 2 (ran, blocked: no row valid) |
| Panel content hash | `0df1db6a97af472a3a9679551cf4b086317cbb5941a79aec98ceb9d45d2abcae`, byte-identical to the script-built panel |
| Declared universe through the CLI | `declared_listing_grid`, 785 pairs, 697 without any window trade |
| Study exit code | 2 (ran, blocked: ladder rungs blocked) |
| Forecast panel rows | 785 |
| Registry recorded | yes, at the explicitly named path |
| Source hash inputs | 8 real file identities (six modules, the pipeline config, the forecast panel) |
| Altered sealed bytes | rejected: `content hash … does not match its manifest … the sealed dataset was modified`, exit 1 |
| Grid omitting a declared release | rejected, exit 1, release named |
| `uv run --no-sync pytest -q -p no:cacheprovider` | see the baseline table below |
| `uv run --no-sync ruff check src tests scripts` | clean |

## Baseline verification

| Command | Result |
| --- | --- |
| `uv run --no-sync market-propagation --help` | exit 0; all external-history commands plus `study-external` |
| `uv run --no-sync pytest -q -p no:cacheprovider` | **907 passed**; 899 passed before the admission audit, 872 before the expectations and ladder additions |
| `uv run --no-sync ruff check src tests scripts` | clean |
| `uv run --no-sync ruff format --check src tests scripts` | clean |

One intermittent failure was observed once in
`tests/test_reporting.py::test_rerunning_into_the_same_directory_respects_the_immutable_write_contract`
(registry identity differed across two identical reruns) and did not reproduce on
two subsequent full runs. It is in the reporting harness, which this execution did
not modify. It is recorded here rather than dismissed.

No typechecker is configured in `pyproject.toml`; that gap is reported rather than
claimed closed.
