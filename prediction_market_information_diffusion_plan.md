# Information Propagation in Prediction Markets
## End-to-end research and implementation plan

**Version:** 1.0  
**Prepared:** 12 September 2026  
**Status:** Research design complete; empirical hypotheses untested.  
**Operating scope:** Read-only public-data research and offline simulation. No funded accounts, real-money order placement, or market interventions are required.

## 1. Executive decision

Build a reproducible system that measures how a public information release changes prediction-market quotes, distributions, and order flow, then tests whether movements in other markets contain incremental predictive information after accounting for that shared release and observation delays.

The central question is:

> After a public information shock, which parts of the subsequent market response reflect direct updating, transmission between markets, and mechanical delays in observation or quoting?

The initial empirical domain is US CPI and Employment Situation releases. The primary outcomes are still-unresolved macro and policy contracts, conditional on verifying that sufficiently active contracts actually existed at each release. Directly resolved contracts form a separate settlement/assimilation study. Start with Kalshi historical data for a retrospective event study; capture public Polymarket data prospectively as a replication source where contract semantics match. Authenticated data access is an optional capability, not an assumption.

Do not start with a neural diffusion generator, a large scraped news corpus, or a trading bot. Here, diffusion means information propagation. Whether a diffusion-like dynamical model is useful is an empirical hypothesis, not a naming requirement.

The three outputs are a point-in-time event dataset, a falsification-tested response-estimation pipeline, and a research paper. A negative finding, such as apparent propagation disappearing after synchronization, is a valid outcome.

## 2. What the literature already covers

Diercks, Katz, and Wright's 2026 Federal Reserve working paper, *Kalshi and the Rise of Macro Markets*, already studies macroeconomic expectations, distributional forecasts, and responses to news. Merely showing that CPI changes policy-market prices is not a novel contribution. The authors discuss possible data/code distribution, but availability must be checked rather than inferred from an intention to release it. [1]

Goldstein, Li, and Wang's July 2026 working paper, *Learning from Prediction Markets: The Transmission of Information and Noise to Traditional Assets*, studies election-related cross-market information and noise transmission. It is a close predecessor for the spillover component, not proof that the same mechanism operates in macro contracts at short horizons. [2]

Bartlett and O'Hara's 2026 working paper studies adverse selection on Kalshi. It motivates treating order flow and liquidity as substantive mechanisms rather than nuisance columns. Wolfers and Zitzewitz explain why interpreting prices as probabilities requires assumptions about beliefs and preferences. Neither source licenses identifying an observed quote with an objective conditional probability. [3,4]

Kalshi's August 2026 calibration study is another useful comparator, but it is exchange-produced research. Treat its findings as results to interrogate, not as a blanket validation of every contract, horizon, or market condition. [5]

**Candidate contribution:** a measurement-aware, contract-aware, event-level test of how much apparent information propagation survives common-news controls, timestamp uncertainty, quote staleness, and payoff constraints, followed by genuinely out-of-sample prediction.

This is a proposed contribution, not a verified priority claim. Maintain a literature matrix recording event classes, sampling frequency, data access, identification strategy, baselines, and novelty overlap. Refresh it before submission. The specific earlier SSRN reference numbered 7021660 could not be verified in this review and is excluded from the evidence base.

## 3. Research contract and hypotheses

The target population is the set of macro prediction-market contracts and release dates passing prespecified eligibility and data-quality conditions. Conclusions apply to that selected population. Record the exclusion process so that conclusions are not generalized to every prediction market.

| Hypothesis | Mechanism being tested | Evidence required |
| --- | --- | --- |
| H1: Responses are heterogeneous in speed and shape | Different information-processing and quoting frictions | Event-level response curves with timing uncertainty and liquidity interactions |
| H2: Other contracts add predictive information | Cross-contract transmission or complementary information | Held-out improvement over a common-news plus own-market baseline |
| H3: Some apparent transmission is measurement-induced | Stale quotes, unequal feed delays, coarse buckets | Effects shrink or disappear under synchronization and synthetic-null tests |
| H4: Rule-related contracts regain consistency after news | Delayed joint updating across linked payoffs | Bid-ask-aware coherence trajectories, not midpoint discrepancies alone |
| H5: Results transfer across venues or later regimes | Mechanism robustness rather than one-sample fit | A separately evaluated replication cohort with exact contract matching |

H2 is a predictive hypothesis. Even a positive H2 does not establish causal transmission. H3 can be true at the same time as genuine propagation; the task is to distinguish components, not force a binary story.

For each hypothesis, preregister the primary outcome, eligible cohort, estimator, effect-size threshold, uncertainty procedure, and failure interpretation. Label every later change as exploratory.

## 4. Formal objects and admissible information

Work on a probability space $(\Omega,\mathcal F,\mathbb P)$. Let $\mathcal H_t$ represent the full relevant information history and $\mathcal G_t$ the information actually usable by the research system at time $t$, with usual augmentation where required.

For an ingested record $r$, store its source time, receipt time, and usable time. Define

\[
\mathcal G_t=\sigma\{r:t^{\mathrm{usable}}_r\leq t\}.
\]

Source time is not automatically usable time. Historical records without receipt timestamps cannot support the same latency claims as prospectively captured records. Assign them an availability interval or a documented coarse-time interpretation instead of inventing nanosecond precision.

Fix a finite union of $M$ contracts for a study cohort and an active/valid mask for each contract. A convenient observed state space is a product of bounded bid/ask prices, nonnegative depth and trade quantities, categorical lifecycle states, observation masks, and macro covariates, equipped with the product Borel sigma-algebra. Missing observations remain masked; they are not numeric zeroes.

For a standard binary contract, let

\[
Y_m=\mathbf 1_{A_m},\qquad A_m\in\mathcal H_{T_m},
\]

where $T_m$ is its resolution time. Let $b_m(t),a_m(t)$ denote valid executable-side quotes and $q_m(t)=(b_m(t)+a_m(t))/2$ the midpoint when both sides are observed.

The benchmark belief

\[
p^*_m(t)=\mathbb E_{\mathbb P}[Y_m\mid\mathcal H_t]
\]

is latent. Neither $q_m(t)=p^*_m(t)$ nor recovery of $p^*_m$ from quotes is assumed. Risk preferences, financing, inventory, settlement uncertainty, and observation noise can separate them. Even a risk-neutral pricing interpretation needs its own assumptions. [4]

Exceptional payouts, cancellations, and void contracts are recorded explicitly. They are excluded from the binary scoring cohort or modeled with their actual payout function. Never silently relabel a half payout as a binary resolution. Polymarket's rules describe both exceptional resolution outcomes and later clarifications, making versioned rules necessary. [6]

All predictive features and fitted parameters used at time $t$ must be $\mathcal G_t$-measurable. All training labels must already be available at the training cutoff.

## 5. Scope the experiment before collecting everything

### Core cohort

Use CPI and Employment Situation releases as separate event families. CPI's initial shock vector should distinguish headline and core measures; employment's should distinguish payrolls, unemployment, earnings, and disclosed revisions where data permit. Prespecify a parsimonious subset rather than fitting every component with a small event sample.

For each release, classify contracts into three groups:

**Direct-resolution contracts.** The release determines the contract's economic outcome. Study movement toward its known payout and the separation between information revelation, trading closure, and formal settlement. This is not the same problem as forecasting a still-uncertain event.

**Downstream unresolved contracts.** The release updates information about a later event, such as a policy decision. These are the primary propagation cohort.

**Matched controls.** Contracts with a plausible small exposure to the release but similar pre-event quote activity and lifecycle characteristics. Negative-control validity must be argued, not inferred from a different category label.

### Expansion order

First add PPI or another structured release with suitable data. Then add FOMC as a separate design with decision, statement, projections, and press-conference windows. Monetary policy announcements can reveal both policy choices and economic information, so a single hawkish/dovish label is inadequate. [7]

Only after the core study passes validation should the project add traditional assets or unscheduled news. Traditional-asset data require their own point-in-time timestamps and licensing. A low-frequency downloaded series cannot validate a seconds-scale spillover.

### Time horizons

Historical primary horizons: 1, 5, 15, 30, and 60 minutes, restricted to actual data resolution. Prospective secondary horizons: 1, 5, 15, and 30 seconds, added only after the clock audit justifies them. Longer intraday or daily responses are separate outcomes with separate contamination controls.

These are initial design choices. Select one or two primary horizons before the locked test, not whichever horizon produces the smallest p-value.

## 6. Data feasibility gate: documentation is not a dataset

Official Kalshi documentation distinguishes live and historical partitions, exposes a moving historical cutoff, and describes historical trades and market candles. The documented historical candle intervals are 1, 60, and 1,440 minutes. That is not evidence of complete historical order-book depth. [8,9]

Kalshi documents public REST market-data access, while its WebSocket handshake requires authentication. Order-book streaming uses a snapshot followed by deltas. Plan around the access actually available, without routing around account, eligibility, geographic, or API restrictions. [10,11]

Polymarket documents public market-data surfaces, a market WebSocket, and historical price endpoints. Its legacy price-history endpoint expresses fidelity in minutes; newer history documentation describes resolution metadata and age-dependent availability. Pin the actual endpoint and inspect returned spacing. Do not assume a current query parameter reconstructs a historical tick feed. [12,13,14]

| Data layer | Initial use | Gate before it enters an analysis |
| --- | --- | --- |
| Contract metadata and rules | Universe construction and payoff graph | Stable identifiers, historical eligibility, source/rule version, lifecycle times |
| Historical prices/trades | Minute-scale event study | Market-level coverage, bucket semantics, missingness, duplicates, partition reconciliation |
| Live quotes/books | Fine-grained response measurement | Legitimate access, snapshot recovery, timestamp and gap audit |
| Official releases | Shock time and initial actual values | Archived original payload, timezone, release-family mapping |
| Point-in-time expectations | Surprise construction | Forecast demonstrably recorded before the release |
| External assets | Later spillover replication | Compatible frequency, source/receipt clocks, market-session and contract-roll handling |

BLS publishes an official release calendar with Eastern Time timestamps. Use a timezone database rather than a fixed UTC offset. Federal Reserve calendars and release documents supply FOMC scheduling. Historical data revisions can be recovered through ALFRED's real-time period functionality, but date-level vintages do not establish intraday availability. [15,16,17]

A vendor consensus history is not assumed to be free, licensed, or accessible. Three admissible expectation routes are: a licensed point-in-time consensus; a forecast archived before the event; or a documented pre-release market-implied distribution. Report them separately. The market-implied route is endogenous to the market and cannot serve as an independent validation target. Open-ended outcome buckets require explicit tail assumptions or bounds; do not assign convenient midpoints silently.

**Gate G0:** select approximately ten historical release dates, retrieve all relevant eligible contracts including inactive ones, and produce a coverage report. Verify whether each endpoint actually supports ticker/time filters, pagination and complete historical coverage before budgeting a backfill; a global trade stream may be impractical to acquire for a narrow study. If only coarse data are usable, narrow the estimand. If consensus is unavailable, retain an event-timing study and explicitly limit the interpretation of surprise slopes.

## 7. Point-in-time data architecture

Use immutable raw storage, normalized event records, and derived research panels as separate layers. A normalized research record must always point back to its raw payload.

Required record families:

| Family | Minimum fields |
| --- | --- |
| `contracts` | venue, stable contract/token/event IDs, exact payoff definition, source, threshold operator, units, rounding, vintage rule, open/close/resolve times, rule hash, active mask |
| `releases` | event ID, release family, scheduled and observed publication times, reference period, first-release values, revisions, payload hash |
| `expectations` | event ID, expectation source, statistic definition, forecast value, source time, receipt/availability time, revision status |
| `book_messages` | venue, contract ID, event type, side, level, price, size/delta, sequence scope, source/receipt/usable times, connection ID, raw hash |
| `trades` | venue trade ID, contract, price, size, aggressor field if documented, all available times, block-trade flag if applicable |
| `quality` | window, gap flags, clock estimates, quote ages, missing sides, staleness, schema version, inclusion reason |
| `features` | prediction time, feature values, maximum input availability time, training cutoff, model/graph version |
| `resolutions` | realized payout, information-known time, venue resolution time, rule version used, exceptional outcome flags |

Store prices and quantities in documented fixed-point units or exact decimals during ingestion. Convert to floating point only in the research layer. Distinguish an absolute size replacement from a size increment. Do not infer cancellations from every depth decline, or trade aggressors from the word YES: those interpretations depend on the venue's documented fields.

For every record, preserve source time, receipt time, a monotonic local clock value, usable time, clock-quality information, and a stable deduplication key. A content hash alone must not collapse genuinely repeated identical events when the feed provides no unique ID.

Replay twice: source-time order for economic event studies, receipt/usable-time order for information-feasible prediction. Publish disagreements between the two rather than picking the more attractive ordering.

## 8. Contract semantics and the payoff graph

The graph should come from economic and logical relationships before it comes from price correlation.

Maintain three distinct relation types:

1. Exact or logical payoff relationships: complements, mutually exclusive/exhaustive buckets, nested thresholds, and genuinely identical cross-venue claims.
2. Economic exposure relationships: releases and contracts linked by a stated macro mechanism.
3. Empirical predictive relationships: training-only lag relationships that remain after controls.

Do not mix these edge types. An implication is not a causal transmission mechanism, and a correlation is not a payoff identity.

Matching must check the reference period, publication source, initial-versus-revised value, units, threshold strictness, rounding, deadline, timezone, settlement, currency, and exceptional-resolution policy. Similar titles are insufficient. [6]

### A useful mathematical diagnostic

Within a contract family with common settlement/numeraire assumptions, enumerate the $K$ admissible atomic outcomes. Let $A\in[0,1]^{M\times K}$ encode payouts. Binary-only cohorts have entries in $\{0,1\}$; exceptional payouts require the wider range.

After explicit discount normalization, the set of coherent normalized state prices is

\[
\mathcal C=\{A\pi:\pi\in\Delta^{K-1}\},\qquad
\Delta^{K-1}=\{\pi\geq0:\mathbf1^\top\pi=1\}.
\]

Let the quoted box be $\mathcal B_t=\prod_m[b_m(t),a_m(t)]$. Ask whether

\[
\mathcal C\cap\mathcal B_t\ne\varnothing.
\]

A midpoint can violate an identity while this intersection remains nonempty. Conversely, infeasibility is a quote-coherence finding under stated assumptions, not automatically an executable arbitrage. Fees, inventory constraints, finite size, differing cashflows, and non-simultaneous execution matter.

Define a finite-dimensional diagnostic

\[
d(t)=\min_{\pi\in\Delta^{K-1}}
\max_m\operatorname{dist}((A\pi)_m,[b_m(t),a_m(t)]).
\]

Study whether $d(t)$ changes after releases and how it returns toward baseline. Use only synchronized valid quotes. Report raw observations separately from any constrained projection: a projection enforces coherence and therefore cannot itself demonstrate market coherence.

Start with nested thresholds and simple outcome partitions, where atomic states are tractable. Do not enumerate an exponential joint state space across unrelated variables merely because the notation permits it.

## 9. Identification: separate direct news from cross-market transmission

The starting causal structure is

\[
S_k\to q_i,\quad S_k\to q_j,\quad
U_k\to(q_i,q_j),\quad
L_i\to\widetilde q_i,\quad L_j\to\widetilde q_j,
\]

where $S_k$ is observed news, $U_k$ unobserved shared information, and $L_i,L_j$ observation/quoting delays. The hypothesized additional mechanism is information from market $i$ influencing market $j$.

Known release time improves alignment. It does not make every associated price move causal, and it does not make the macro surprise a valid instrument for market $i$ when that surprise directly affects market $j$.

Maintain three evidence levels:

**Descriptive:** how quotes, flow, and dispersion change around releases.

**Predictive:** whether a neighbor's past information improves out-of-sample forecasts beyond own-market and shared-news information.

**Causal:** whether an intervention on access to a market signal would change another market's response. Real-market causal claims require separately defended variation and exclusion assumptions. The default dataset does not identify them automatically.

Before interpreting release coefficients causally, document conditional shock exogeneity, relevant simultaneous release components, no unmodeled coincident news in the chosen window, correct timing, and the adequacy of the specification. Post-event liquidity is potentially a mediator. Do not control for it while calling the result a total shock effect. Use pre-event liquidity for heterogeneity; analyze post-event flow in a separate mechanism model.

A carefully justified natural experiment can become an extension. An outage is not automatically exogenous, especially if caused by market load. Randomized communication delay in an offline simulator identifies the simulator's mechanism, not the real market's.

## 10. Shock construction and event-window rules

For release $k$, define a point-in-time vector

\[
S_k=D_{k^-}^{-1}(x_k^{\mathrm{first}}-\widehat x_{k^-}),
\]

where $D_{k^-}$ is a positive diagonal scaling matrix estimated only from earlier eligible releases. Keep raw-unit surprises alongside standardized ones. For dimensions with no defensible expectation, retain the released value as a control or omit that surprise dimension; do not manufacture a consensus.

Record revisions as newly released information. A contemporaneous payroll revision can matter even when the headline new-month payroll surprise is small. Overlapping scheduled releases should be jointly coded, explicitly excluded, or treated as a combined package.

Proposed event window: a 30-minute pre-event diagnostic interval and a 60-minute main post-event interval, with separate wider-window sensitivity analyses. Choose a documented baseline quote or pre-event summary and retain its age. Avoid carrying a stale last trade across the release and calling it a current quote.

For directly resolved contracts, distinguish economic outcome revelation from formal settlement. For downstream contracts, stop the window at closure, another contaminating event, or the quality boundary. Do not fill a closed market forward and infer instant assimilation.

Freeze contamination rules before outcomes are inspected. Keep an inclusion/exclusion ledger with reasons and summary statistics for excluded events.

## 11. Response measures with finite horizons

Define the observed midpoint response, when valid,

\[
R_{km}(h)=q_m(t_k+h)-q_m(t_k^-).
\]

Primary quantities are finite-horizon response magnitude, speed of within-window adjustment, overshoot, spread/depth adjustment, and payoff-coherence distance. Trade-price results are a separate robustness layer because transaction prices and quote midpoints measure different objects.

Let $\beta(h)$ denote an estimated response coefficient for a fixed shock dimension and target cohort. Report $\beta(1m),\beta(5m),\beta(15m),\beta(60m)$ with simultaneous uncertainty where possible.

One finite-window overshoot diagnostic is

\[
O_H=\max_{0\leq h\leq H}|\beta(h)|-|\beta(H)|.
\]

A signed version should be reported when direction matters. Ratios using $\beta(H)$ require a prespecified denominator threshold and are undefined or unstable near zero.

A settling-time diagnostic can use

\[
T_{\epsilon,H}=\inf\{h:|\beta(u)-\beta(H)|\leq\epsilon
\text{ for all }u\in[h,H]\}.
\]

This is convergence relative to a finite-window endpoint, not evidence of a permanent effect. It can be mechanically near $H$ when no stable plateau is observed. Flag such cases as unresolved within the window; do not publish a precise assimilation half-life from an unstable curve.

Do not integrate a persistent price displacement to infinity and call it finite information mass. Do not infer diffusion speed from the first trade alone. A quote can update without a transaction, and an unchanged quote can reflect either no news or missing observation.

For exact matched contracts, report quote disagreement together with spread and timing uncertainty. For related but nonidentical claims, use their payoff/exposure model rather than raw price differences.

## 12. Model ladder and promotion rules

### Level 0: diagnostics and baselines

Start with response plots, missingness and quote-age maps, no-change forecasts, own-market autoregressions, and release-surprise-only models. Include a shared-news, no-transmission model with heterogeneous delays. A network model must beat that model, not only a naive last-price baseline.

For future-price targets, use $q_{t+h}$ or $q_{t+h}-q_t$ explicitly. For resolution forecasts, use $Y_m$ and a proper probability score. These are different tasks and must not share an ambiguous accuracy number.

### Level 1: event-study local projections

For each horizon $h$, fit a parsimonious family-pooled model such as

\[
R_{km}(h)=\alpha_{f(m),h}
+\beta_{f(m),h}^{\top}S_k
+\gamma_h^{\top}X^-_{km}
+\delta_h^{\top}(S_k\otimes L^-_{km})+u_{kmh},
\]

where $X^-_{km}$ includes pre-release probability, time to outcome, quote activity, prior volatility, and other prespecified state variables; $L^-_{km}$ is a small subset of liquidity variables. Local projections are the initial response-estimation method, not a causal-identification shortcut. [18]

Do not pool opposite payoff orientations as if their signed responses were comparable. Either preregister a consistently oriented threshold cohort or let the shock response vary with a small set of payoff-shape variables, pre-event probability, and threshold location. Mutually exclusive bucket contracts may move in opposite directions after the same news.

Threshold contracts from the same release are dependent observations. Cluster uncertainty by release date and use calendar blocks where serial dependence matters. Allow class-specific effects for CPI versus employment. Avoid fitting a high-dimensional interaction surface from a few dozen independent releases.

Do not add unrestricted event fixed effects and then claim to identify a shock main effect that is constant within the event. Use a specification whose variation actually identifies the parameter of interest.

### Level 2: sparse conditional propagation

At a chosen data-supported interval, model valid probability changes or interior log-odds changes:

\[
\Delta z_t=\sum_{\ell=1}^{L}A_\ell\Delta z_{t-\ell}
+\sum_{j=0}^{J}B_j S_{t-j}+D X_{t^-}+\varepsilon_t.
\]

Here $S_t$ records release shocks at their usable time, the $B_j$ terms allow contract-specific delayed direct responses to those shocks, $X_{t^-}$ includes admissible pre-prediction covariates, and $A_\ell$ encodes lagged relationships. Include the same flexible direct-response and observation-delay structure in the no-cross-market baseline; otherwise another market can merely proxy an omitted delayed response to the original release. Exact-complement contracts should not create redundant unconstrained columns. Missingness and asynchronous observations need an explicit observation model or a justified coarser panel.

Use a payoff/exposure graph to define candidate relationships and sparsity penalties. Select lag length, regularization, and graph complexity on training/validation data. Conditional forecast impulse responses follow from the fitted recursion. A matrix coefficient does not become a structural intervention coefficient merely by being called a propagator.

Use raw probability-point responses as the baseline estimand. A linear response regression is a local approximation, not a globally bounded stochastic process. Probability forecasts must use a documented bounded link or constrained forecast mapping; report any clipping and fit all such choices within training data. If log-odds are used, document the interior cohort or clipping rule, report its sensitivity, and do not treat clipping as data. Stable dynamics for short-run increments do not require that event probabilities themselves revert to 0.5.

Do not impose a positive, mass-conserving graph Laplacian by default. Price responses may have opposite signs, and probability is not conserved across unrelated events. The graph is generally directed, signed, time-dependent, and only partially observed.

### Level 3: observation-aware latent-state model

Add a latent common-news state and venue-specific observation delays only if Level 2 leaves systematic residual structure. Compare nested models with shared shocks only, shared shocks plus heterogeneous delays, and shared shocks plus delays plus cross-market lags.

Latent efficient values, preference wedges, and microstructure noise are not generally separately identifiable from a single quote series. Report partial identification or model sensitivity rather than assigning each residual a convenient psychological label.

### Level 4: optional path-distribution model

A conditional generative diffusion or flow model may later estimate a distribution of future response trajectories. It is optional and needs sufficient independent variation, not merely many messages. Compare it against regularized linear/state-space and simpler probabilistic baselines using held-out proper scoring rules, coverage, and regime transfer.

A successful generator is a scenario model. Its denoising process does not prove that real information propagates by the same stochastic diffusion.

## 13. Experimental program

### Experiment A: historical release-response benchmark

Build a frozen historical panel with observed coverage and pre-event contract selection. Plot responses before fitting complex models. Estimate the core local projections and release-family interactions. Report data exclusions, event counts, endpoint uncertainty, and leave-one-event-out sensitivity.

Success means reliable measurement and interpretable uncertainty, not a predetermined statistically significant news effect.

### Experiment B: synchronized live observation

Capture raw public data prospectively with source and receipt clocks. Compare exact matched claims only when the rule audit passes. Assess whether observed lead-lag changes with quote refresh rate, feed delay, and resampling convention.

When two timing uncertainty intervals overlap, direction is unresolved at that scale. Record a tie or interval rather than forcing an ordering.

### Experiment C: common-news versus network information

Forecast a prespecified downstream response using nested feature sets: own-market history; own-market plus release surprise and shared state; then those features plus other contracts' admissible lagged information.

Keep the target market's contemporaneous price in the baseline. Otherwise the network may merely reconstruct information already visible in the target quote. Use a fixed prediction timestamp after each release and forecast subsequent movement, so the information cutoff is explicit.

Evaluate improvement at the release/event level. Strong prediction is still compatible with an omitted common signal, so report the interpretation accordingly.

### Experiment D: payoff-coherence recovery

Compute the bid-ask-aware coherence distance on prespecified logical families before and after releases. Compare it with a raw-midpoint diagnostic. Test whether apparent inconsistency is explained by asynchronous quotes and whether recovery speed varies with pre-event depth and quote activity.

The key comparison is between genuine joint-price inconsistency and a charting artifact. Do not substitute projected coherent prices for raw observations.

### Experiment E: external-asset replication

Only after the earlier experiments are stable, test whether prediction-market state improves future traditional-asset responses beyond the asset's own history, the full observed release vector, and appropriate conventional market controls.

Align actual sessions and quotes. Do not splice futures with a retrospectively chosen roll rule. Do not use information computed from the same target return window as a forecast input. Keep this experiment separate from the primary macro-contract paper when its data and identification burden would overwhelm the core contribution.

## 14. Adversarial simulator and falsification suite

Build the simulator before interpreting any learned graph. Start with the smallest process that exercises the estimator; a large agent-based economy is not necessary.

The first null has a common latent price $q^*(t)$ and observed quotes

\[
\widetilde q_1(t)=q^*(t),\qquad
\widetilde q_2(t)=q^*(t-\delta).
\]

There is no communication from market 1 to market 2. Nonetheless, market 1 appears to lead. Add distinct spreads, sparse updates, tick rounding, and dropped messages. A procedure that reports causal diffusion here fails its intended interpretation.

Further scenarios include genuine delayed communication, simultaneous direct news effects with different sensitivities, omitted shared shocks, opposing-sign responses, widening spreads with unchanged latent value, reversals caused by later news, incorrect rule matching, resolution pauses, and changes in sampling resolution.

Known communication edges in simulation should be recoverable when signal-to-noise permits; null simulations should control false discoveries. Report both power and false-positive behavior. Calibrate observational nuisance ranges from the development data, while also testing values outside that range.

Real-data falsifiers include pre-release leads, shifted release times matched on time of day, shuffled event labels within appropriate regimes, matched negative-control contracts, reversed candidate graph directions, quote-age stratification, endpoint changes, and leave-one-release-out analyses. Placebo exchangeability must be defended; blindly shuffling nonstationary time series is not a valid null.

## 15. Evaluation, uncertainty, and data leakage controls

Use chronological development, validation, and locked-test periods. All contracts for one economic release, including cross-venue equivalents, stay in the same split. Feature transforms, graphs, calibration maps, shock scaling, and hyperparameters are fitted only inside the training period.

For future-price forecasts, report probability-point MAE, squared error, directional accuracy only as a secondary metric, and probabilistic scores when a predictive distribution is produced. For resolution forecasts, report Brier score, log loss with an explicit boundary convention, and reliability conditional on horizon and regime. Millions of snapshots of the same eventual outcome do not create millions of independent binary trials.

For effect estimation, report uncertainty over the whole response curve and prespecified contrasts. Cluster by release/event; use appropriate calendar blocks when temporal dependence persists. With few clusters, present finite-sample sensitivity and broad intervals rather than treating asymptotic precision as settled.

For network models, report incremental held-out loss, edge stability, null false positives, and sensitivity to the conditioning set. Edge direction is predictive unless an additional identification design justifies a causal interpretation.

Specify one primary metric and comparison. Label the rest exploratory, account for multiple comparisons, and record all attempted configurations. The locked test is evaluated once per registered release of the research specification; changing the specification creates a new version and consumes a new test cohort.

Resolve labels only when known. A contract that settles after a training cutoff cannot contribute its terminal outcome to that training run. Purge overlapping forecast/label windows and embargo according to the actual horizon and label-availability structure, not a ceremonial fixed number of days.

No future graph construction, retrospective participant labels, interpolated future quotes, full-history scaling, revised macro data substituted for initial releases, or selection of markets based on post-event volume.

## 16. Sample size and power

The independent information unit is primarily the release date or economic event, not the number of messages. Closely related contracts provide useful cross-sectional structure but not independent macro shocks.

Aim to audit 24-36 months of retrospective coverage where available, rather than promising that such history exists. Maintain prospective capture for at least several additional months as a separate cohort. A 12-week engineering schedule may contain only a handful of each monthly release family; it is a pilot, not an assurance of adequate statistical power.

After the pilot, estimate event-level residual variance and dependence. Choose a smallest scientifically relevant effect, expressed in probability points or a prespecified reduction in forecast loss. Use cluster-aware simulation to estimate power and false-positive rates at the available event count. Derive the sample requirement from that exercise; do not invoke a universal thirty-event rule.

If the data cannot distinguish zero from the prespecified relevant effect, report an inconclusive result. If the interval excludes effects above the relevant threshold, report an informative bound rather than only a nonsignificant p-value. Collapse model complexity when independent events are scarce.

## 17. Engineering implementation

Use Python for orchestration, typed schema definitions, point-in-time joins, econometric estimation, and reporting. Use columnar files for immutable research datasets and a local analytical engine for reproducible queries. Put a Rust collector or replay core behind the same schema only if profiling shows Python cannot meet the actual message-rate and reliability requirements.

The proposed repository is:

```text
market-propagation/
  README.md
  pyproject.toml
  configs/
    cohort.yaml
    endpoints.yaml
    event_windows.yaml
    study_v1.yaml
  schemas/
    contracts.py
    releases.py
    market_events.py
  ingest/
    kalshi_rest.py
    polymarket_public.py
    macro_releases.py
  normalize/
    clocks.py
    prices.py
    contract_rules.py
  replay/
    books.py
    event_clock.py
    gaps.py
  features/
    point_in_time.py
    liquidity.py
    shocks.py
  models/
    baselines.py
    local_projections.py
    sparse_propagation.py
    coherence.py
  simulation/
    shared_news_null.py
    communication_network.py
  evaluation/
    splits.py
    bootstrap.py
    placebos.py
    scoring.py
  tests/
    fixtures/
    test_replay.py
    test_information_cutoffs.py
    test_contract_algebra.py
    test_null_behavior.py
  reports/
    data_audit.md
    preregistration.md
    experiment_registry.jsonl
    paper.md
```

This is a proposed layout, not an implemented repository. Public adapters must expose no order-submission route. Store secrets only for legitimately available read access; do not include keys in raw payload archives or releases.

Core interfaces should include `normalize(raw)`, `apply_book_event(state, event)`, `features_asof(time)`, `build_event_panel(spec)`, `fit(train, spec)`, and `evaluate(frozen_model, test)`.

When a sequence gap is detected, mark the reconstruction invalid until a fresh snapshot restores it. Do not assume sequence numbers are global across all connections or feeds. For feeds without a usable sequence, combine documented identifiers, reconnect boundaries, and periodic reconciliation; completeness may remain unprovable. [11,13]

Distinguish the time of the last price change, the time of the last verified live snapshot/message, and the time of the last trade. An unchanged standing quote is not automatically stale or invalid. Use gap detection and documented refresh information, not price-change age alone, to assess observational validity.

Use idempotent writes, bounded retries, backoff, pagination checkpoints, schema-change alarms, and graceful shutdown. Follow current documented request limits rather than embedding an outdated numerical quota. [19]

Each experiment records its source hashes, contract-universe version, event exclusions, input availability cutoff, git commit, environment lock, random seeds, fitted transforms, parameters, graph, metrics, and statistical unit count.

## 18. Acceptance tests

**Data and replay.** Exact-decimal price parsing; empty or one-sided books; snapshot-plus-delta correctness; duplicate handling without collapsing distinct repeated records; out-of-order input; disconnection and gap recovery; a daylight-saving transition; moving live/historical cutoffs; changed schemas; and consistency of trade counts across pagination boundaries.

**Information integrity.** Artificially inserting a future record must not alter earlier features. Training transformations must be unchanged when locked-test data are modified. Delaying a record's usable time must delay its influence. A label unresolved at the training cutoff must be unavailable to training code.

**Contract algebra.** Complement and threshold examples with known feasible state distributions; a feasible quoted box with inconsistent midpoints; a genuinely infeasible quoted box; strict versus non-strict thresholds; rounding; and exceptional payout handling. For small families, compare the optimization result with explicit enumeration.

**Statistics.** Recovery on known synthetic responses; false-positive behavior under common-news-plus-delay nulls; sensitivity to small event counts; complete release-cluster assignment; deterministic seeded resampling; and invariance of predictions to row ordering after the defined normalization/replay procedure.

**Reproducibility.** A clean environment rebuilds one sample report from immutable fixtures. Report numerical tolerance for model outputs rather than promising bitwise identity across all hardware. Raw normalization and discrete book reconstruction should be deterministic.

A passing software suite validates those tested properties. It does not establish economic identification or profitability.

## 19. Work plan and decision gates

| Phase | Approximate engineering window | Deliverable | Promotion gate |
| --- | --- | --- | --- |
| Scope and feasibility | Week 1 | Literature matrix, ten-event coverage audit, target/estimand registry | Valid cohort, known frequency, permissible data access |
| Recorder and normalization | Weeks 1-3 | Immutable capture, versioned rules, clock/gap audit | Replay and availability tests pass |
| Historical benchmark | Weeks 3-5 | Frozen event panel and baseline response report | Measurement passes visual and adversarial audit |
| Falsification and power | Weeks 4-6 | Synthetic-null report and sample-size assessment | Claimed mechanism distinguishable at chosen resolution |
| Conditional propagation | Weeks 6-8 | Nested predictive comparisons and coherence study | Held-out gain beyond common news and own-market state |
| Robustness and replication | Weeks 8-10 | Placebos, sensitivity, independent cohort results where available | No dependence on a single event, clock, or rule error |
| Paper and reproducibility | Weeks 10-12 | Draft, data documentation, code release package | Claims match evidence and remaining uncertainty |
| Prospective continuation | Beyond week 12 as needed | Additional untouched release cohorts | Event count and power justify stronger conclusions |

The schedule allocates engineering work. Data collection and statistical power follow the release calendar, not the schedule's optimism.

**Stop or pivot rules:** insufficient high-resolution history means a coarser study; absent point-in-time consensus means no independent consensus-surprise claim; poor matching means no cross-venue pooling; null failure blocks network interpretation; no held-out gain blocks model promotion; unidentified causal paths remain labeled unidentified.

## 20. Resources and operating budget

Start with a CPU-only research environment and a modest always-on recorder. As a planning configuration rather than a measured requirement, 8-16 CPU cores and 32-64 GB RAM should be evaluated against a small cohort before allocating more resources. The initial methods do not require a large GPU training run.

Measure storage instead of guessing from market popularity:

\[
\text{raw bytes/day}=\text{messages/day}\times\text{mean stored bytes/message}.
\]

For example, two million messages at 500 bytes each is approximately one decimal GB per day before indexes, replication, and derived datasets. This is an illustrative calculation, not a forecast of either venue's traffic. Estimate compression and feature-expansion ratios from a real pilot.

Use a fixed initial subscription/cloud spending cap chosen before paid data purchases. Paid historical depth, consensus archives, or cross-asset data are separate go/no-go decisions supported by the feasibility audit. Do not purchase compute to compensate for a missing estimand or unusable timestamps.

Run a daily collector-quality report and a periodic experiment-registry review through the eventual project scheduler. These are implementation requirements, not services already deployed.

## 21. Research integrity and interpretation limits

This project studies information aggregation. It should not require market manipulation, real-money interventions, access-restriction circumvention, or identifying the real people behind public addresses.

Wallet-level analysis is optional and secondary. A wallet is not necessarily one person, and a person may control multiple wallets. Participant labels based on later profitability cannot become historical real-time features. Any allowed historical labeling must use only earlier observations and still be described as a proxy, not proof of informed intent.

Do not equate calibration, efficiency, and commercial usefulness. A calibrated terminal forecast can update slowly; a rapid response can be poorly calibrated; a useful forecast improvement can be too small relative to spread and observational uncertainty. This plan's success criterion is research quality, not a profitable trading claim.

Any later execution or commercial feasibility project requires its own eligibility, legal, data-license, operational-risk, and transaction-cost review. It is not authorized or implemented by this research plan.

## 22. Deliverables and paper structure

Produce a data card, contract-rule registry, timestamp/coverage audit, preregistered analysis specification, deterministic replay fixtures, simulator with known ground truth, baseline report, conditional propagation report, and a reproduction guide. Release raw data only where licensing permits; otherwise release acquisition instructions, hashes, allowed aggregates, and synthetic fixtures.

The paper should have a narrow title such as *Measuring Information Propagation in Macro Prediction Markets under Asynchronous Observation*. The final title depends on the result rather than assuming a positive diffusion finding.

Its core narrative is: economic question; exact observable and estimand; data and contract construction; observation/identification problem; common-news and delay baselines; conditional propagation results; payoff-coherence diagnostic; falsification and replication; limitations.

Key figures are a timestamp/coverage map, event-response curves with uncertainty, model-comparison results across held-out events, a synthetic-null failure/success panel, and coherence distance versus quote age around news. A learned network diagram is optional and must distinguish logical relationships from empirical predictive edges.

An informative null paper is possible: for example, a documented reduction of apparent lead-lag after source/receipt alignment with a useful upper bound on remaining predictability. Insufficient data alone supports an infrastructure or feasibility report, not a strong economic conclusion.

## 23. First 72 hours

**Day 1:** freeze the primary question and claim hierarchy; review the closest papers; inspect the official macro calendar; create the contract-rule checklist; select ten historical release dates and a small candidate contract set.

**Day 2:** test accessible read-only endpoints, reconcile historical/live partitions, record actual history spacing, inspect point-in-time expectations, and produce the first coverage matrix. Start prospective raw recording where legitimately accessible.

**Day 3:** implement the first normalized event schema, one book-replay fixture, a future-data leakage test, and the common-news-plus-delay synthetic null. Build one end-to-end historical event card before any complex model is trained.

The first milestone is not a predicted return. It is one completely auditable event: what was released, which contracts were open, what each contract meant, what each feed showed, when the system could know it, and which claims those observations can support.

## 24. Completion criteria and present trust boundary

The empirical project is complete when the preregistered cohort, estimators, null tests, held-out evaluation, uncertainty analysis, and reproduction package support a classified result: evidence for conditional propagation, evidence excluding effects above the prespecified relevant size, or a precise inconclusive/identification-limited finding.

As of this plan, official documentation and adjacent literature have been reviewed. No historical market-level coverage audit, collector deployment, model fit, economic backtest, causal experiment, or power calculation has been executed. Endpoints and data availability remain implementation-time checks. Software and empirical validation steps described above are requirements, not reported successes.

The immediate priority is the data and timing audit. If that survives, estimate simple response curves. Only then ask whether a learned propagation operator earns its complexity.

## Sources

The sources below were checked on 12 September 2026. API documentation is mutable and must be pinned or archived during implementation. Published papers and working papers are distinguished in the text; none is treated as independent validation of this project's unrun experiments.

[1] Anthony M. Diercks, Jared Dean Katz, and Jonathan H. Wright. *Kalshi and the Rise of Macro Markets*. Federal Reserve FEDS working paper 2026-010. https://www.federalreserve.gov/econres/feds/kalshi-and-the-rise-of-macro-markets.htm ; paper: https://www.federalreserve.gov/econres/feds/files/2026010pap.pdf

[2] Itay Goldstein, Ye Li, and Chen Wang. *Learning from Prediction Markets: The Transmission of Information and Noise to Traditional Assets*. Working paper, 28 July 2026. https://chenwang.one/files/predictionmkt.pdf

[3] Robert Bartlett and Maureen O'Hara. *Adverse Selection in Prediction Markets: Evidence from Kalshi*. Working paper, summarized by Stanford Law, 21 April 2026. https://law.stanford.edu/2026/04/21/adverse-selection-in-prediction-markets-evidence-from-kalshi/

[4] Justin Wolfers and Eric Zitzewitz. *Interpreting Prediction Market Prices as Probabilities*. NBER working paper 12200, 2006. https://www.nber.org/system/files/working_papers/w12200/w12200.pdf

[5] Nicole Kagan and Rubens Baiocchi. *Calibration in Prediction Markets: Theory and Evidence*. Kalshi Research working paper, August 2026. https://kalshi.com/research/publications/calibration

[6] Polymarket. Resolution and resolution rules. https://docs.polymarket.com/concepts/resolution

[7] Marek Jarocinski and Peter Karadi. *Deconstructing Monetary Policy Surprises: The Role of Information Shocks*. American Economic Journal: Macroeconomics. https://www.aeaweb.org/articles?id=10.1257/mac.20180090

[8] Kalshi. Historical Data. https://docs.kalshi.com/getting_started/historical_data

[9] Kalshi. Historical Market Candlesticks and Historical Trades. https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks ; https://docs.kalshi.com/api-reference/historical/get-historical-trades

[10] Kalshi. Quick Start: Market Data; Quick Start: WebSockets. https://docs.kalshi.com/getting_started/quick_start_market_data ; https://docs.kalshi.com/getting_started/quick_start_websockets

[11] Kalshi. Orderbook Updates. https://docs.kalshi.com/websockets/orderbook-updates

[12] Polymarket. API integration surfaces. https://docs.polymarket.com/getting-started/api

[13] Polymarket. Market Channel. https://docs.polymarket.com/api-reference/wss/market

[14] Polymarket. Historical prices: legacy and newer documented surfaces. https://docs.polymarket.com/api-reference/markets/get-prices-history ; https://docs.polymarket.com/api-reference/markets/get-a-tokens-price-history

[15] Bureau of Labor Statistics. September 2026 release calendar; use the corresponding official calendar for each study month. https://www.bls.gov/schedule/2026/09_sched_list.htm

[16] Federal Reserve. FOMC calendars and information. https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm

[17] Federal Reserve Bank of St. Louis. FRED/ALFRED real-time periods. https://fred.stlouisfed.org/docs/api/fred/realtime_period.html

[18] Oscar Jorda. *Estimation and Inference of Impulse Responses by Local Projections*. American Economic Review 95(1), 161-182, 2005. https://www.aeaweb.org/articles?id=10.1257/0002828053828518

[19] Kalshi. Rate Limits and Tiers. https://docs.kalshi.com/getting_started/rate_limits
