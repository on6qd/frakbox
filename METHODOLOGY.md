# Research Methodology — Detailed Reference

This document contains the full methodology for the Stock Market Causal Research Project.
For the operational overview, see CLAUDE.md.

## The Research Loop

### Phase 1: Literature Review
Before researching any event category, first check what's already known:
- Search for academic finance papers on the topic (event studies, anomaly research)
- Check established quantitative findings (e.g., PEAD — Post-Earnings Announcement Drift is well-documented)
- Don't rediscover known effects — build on them or find gaps

### Phase 2: Historical Mining
Search the web for **event dates** — when did specific events occur? Then verify price impact with `measure_event_impact()`.
- **Web search is for finding dates, not for determining what happened.** News articles cherry-pick dramatic examples. Never form hypotheses from narratives alone.
- "When did major FDA rejections happen in biotech?" → get dates → `measure_event_impact()`
- **Minimum 12 historical instances** before considering a pattern real (5 for exploratory investigation only)
- **Use multi-symbol backtests**: pass event_dates as list of `{"symbol": "AAPL", "date": "2024-01-15"}` dicts to test across multiple stocks. A pattern on one stock is not a general effect.
- **Specify event timing**: pass `event_timing="after_hours"` etc. for correct reference price selection.

#### Survivorship Bias Protocol
For event types where companies could have failed (FDA rejections, earnings disasters, bankruptcies, dividend cuts):
- **Actively search for delisted/bankrupt companies** that experienced the event
- Include at least 2 delisted companies in the sample, OR explicitly document that none are relevant
- High-risk categories: `fda_decision`, `earnings_surprise`, `dividend_changes`, `regulatory_changes`

### Phase 3: Pattern Extraction (with rigor)
From historical data, identify reliable patterns:
- Does the effect repeat consistently across multiple instances?
- What's the typical magnitude and timeframe?
- **Use ABNORMAL returns, not raw returns.** `market_data.py` automatically computes stock return minus SPY return. A stock going up 3% when SPY went up 2.5% is a 0.5% abnormal return — barely an effect.
- **Use sector-adjusted returns** when available. Pass `sector_etf="XLV"` (or appropriate ETF) to `measure_event_impact()`. For large ETF constituents (>5% weight), the function automatically corrects for the stock's own contribution to the sector ETF return.
- **Check statistical significance**: look at `p_value_abnormal_*` and `significant_abnormal_*` fields. Uses scipy.stats.ttest_1samp for correct p-values.
- **Check multiple testing correction**: look at `passes_multiple_testing` field. With 5 horizons tested, a single p<0.05 hit has ~23% chance of being spurious. Need 2+ horizons significant at p<0.05, or 1 horizon at p<0.01.
- **Check power analysis**: look at `recommended_n_*` and `sample_sufficient_*` fields. If sample_sufficient is False, you need more historical instances before the result is reliable.
- **Check data quality**: look at `data_quality_warning` field. If >30% of attempted events failed to produce data, investigate.
- **Check cross-event contamination**: pass `known_events` to `measure_event_impact()` to detect overlapping events that could corrupt measurements.
- **Standard deviation matters**: if avg abnormal return is +2% but stdev is 8%, the effect is drowned in noise.

#### Out-of-Sample Validation (REQUIRED)
Before forming any hypothesis:
1. Use **temporal splits**: older events = discovery set, newer events = validation set
2. `validate_out_of_sample()` auto-splits by date if events have `date` fields
3. Or pass `discovery_cutoff_date` for explicit temporal cutoff
4. Minimum 3 validation instances
5. **DO NOT use random index splits** — they allow look-ahead bias
6. If the pattern doesn't hold out-of-sample, record as dead end

#### Regime Conditioning
When you have N≥15 historical instances:
- Subset by VIX regime: calm (<20), elevated (20-30), crisis (>30)
- Check if the effect is regime-dependent
- If it only works in calm markets, note that — and only test it in calm markets

### Phase 4: Hypothesis Formation
Structure every hypothesis as:
**"When [specific event type] occurs, [specific stock/sector] moves [direction] by [approximate magnitude] within [timeframe], because [causal mechanism]."**

**ALL of these gates must pass before a hypothesis is valid:**

1. **Statistical significance**: `passes_multiple_testing` is True from `measure_event_impact()` results.
2. **Effect size**: abnormal return > `min_abnormal_return_pct` from methodology.json (default 1.5%).
3. **Power**: `sample_sufficient` is True for the relevant horizon.
4. **Out-of-sample validation**: pattern holds in both discovery and validation sets using **temporal** split.
5. **Causal mechanism** satisfies at least 2 of 3 criteria from the rubric:
   - Identifies specific economic actors and their incentives
   - Explains the transmission channel
   - References an established economic principle or academic finding
6. **Confidence score** computed via `self_review.compute_confidence_score()` — NOT assigned by feel.
7. **Survivorship bias note** (REQUIRED): how was this addressed?
8. **Selection bias note** (REQUIRED): how was this addressed?
9. **Confounders**: all tracked confounders from methodology.json must be recorded.
10. **Dead end check**: `check_related_dead_ends()` is called automatically. Review warnings.
11. **Multi-symbol evidence**: `backtest_symbols` and `backtest_events` should be provided showing the pattern was tested across multiple stocks.
12. **Event timing** specified: pre_market/intraday/after_hours/unknown
13. **Regime note**: is the effect regime-dependent? (required when N≥15)

**Pre-registration**: when `create_hypothesis()` is called, the prediction is hashed and logged to results.jsonl before any trade is placed.

**Idempotency**: `create_hypothesis()` computes an idempotency key. On crash+rerun, duplicate hypotheses are prevented.

### Phase 5: Live Testing
When a matching event occurs in the real world:
- Place a paper trade via Alpaca
- Set clear entry price, expected move, and deadline
- **Position size is UNIFORM at ~5% of portfolio ($5,000)**
- Document the market context at entry (VIX, sector trends, recent news)

### Phase 6: Post-Mortem (most important phase)
When the experiment concludes:
- Record actual vs expected outcome using **abnormal returns** (pass `spy_return_pct` and `confounders_at_exit` to `complete_hypothesis()`)
- Was the direction correct? **Was the magnitude meaningful?** Check the `magnitude_ratio`.
- What confounding factors were present?
- Did the causal mechanism hold?
- **Check promotion/retirement**: run `research.check_promotion_or_retirement(event_type)`.

**ALL four structured post-mortem fields are REQUIRED** when calling `complete_hypothesis()`:
- `timing_accuracy`: Did the move happen in the expected window? "Move occurred in first 2 days of 5-day window" or "Move was delayed — didn't start until day 4"
- `mechanism_validated`: Did the theorized causal channel actually operate? "Yes — index fund buying visible in volume data" or "No — move was driven by unrelated earnings revision"
- `confounder_attribution`: What % of the observed move can be attributed to the event vs. other factors? "~70% event-driven, ~30% sector momentum (XLV up 2% same period)"
- `surprise_factor`: What was the most unexpected aspect? "Reversal happened faster than historical average" or "Effect was 3x larger than backtest suggested"

### Promotion and Retirement Criteria
**Promotion to known_effects**: ≥3 live tests, ≥60% accuracy, ≥0.3 magnitude ratio
**Retirement**: ≥5 live tests, ≤30% accuracy

### Self-Improvement
- `methodology.json` evolves based on results
- **Bootstrap review** at 3 completed experiments: checks pipeline health, data quality, post-mortem quality. Run `needs_bootstrap_review()` / `run_bootstrap_review()`.
- **Full self-review** triggers every 10 completed experiments
- Checks: magnitude accuracy, confidence calibration, per-category performance, timeframes, sample size impact, confounder analysis
- Weekly research diagnostic checks overall research progress (knowledge base growth, queue throughput, session health)
- Before placing any trade, check `self_review.get_category_settings(event_type)`

## Research Categories
- **Earnings surprises** — beat/miss by X% → stock moves by Y% within Z days
- **FDA decisions** — approval/rejection/delay → biotech/pharma price reactions
- **Fed rate decisions** — rate changes → sector rotation patterns
- **Commodity shocks** — oil/gas/metals spikes → downstream industry effects
- **Index rebalancing** — addition/removal announcements → forced buying/selling
- **Insider buying clusters** — unusual insider activity → subsequent price moves
- **Macro data surprises** — CPI, jobs, GDP vs consensus → market/sector reactions
- **Activist investor stakes** — 13D filings → target stock movement
- **Stock splits** — announcement vs execution date effects
- **Dividend changes** — cuts, initiations, special dividends → price reactions
- **Natural disasters / weather** — hurricanes, droughts → insurance, agriculture, construction
- **Regulatory changes** — new regulations → affected industry price patterns
- **M&A announcements** — acquirer vs target vs competitor reactions
- **Short squeeze setups** — high short interest + catalyst → squeeze probability
- **Cross-category interactions** — once 2+ categories have data, look for interaction effects

## Data Sources
- Web search for **event dates** (NOT for determining price impact)
- Yahoo Finance for historical price data (via `market_data.py`)
- SEC EDGAR for filings (13D, insider transactions)
- FRED for macro economic data
- Google Trends for sentiment/search patterns
- Academic finance papers for established patterns
- Prediction markets for probability-weighted scenarios
