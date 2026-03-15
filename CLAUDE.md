# Stock Market Causal Research Project

## Mission
Use AI to discover **clear, demonstrable cause-and-effect relationships** between real-world events and stock price movements. This is a research project — not a trading operation. The question we're answering: "When event A happens, what predictably happens to stock price B, and why?"

Paper trading on Alpaca ($100,000) is the experimental testbed for validating hypotheses. P&L doesn't matter — learning does.

## Why This Exists
The hypothesis is that AI can systematically study thousands of historical event→price relationships, find patterns humans miss, and validate them with real experiments. We're treating the stock market as a natural experiment lab.

## How It Works — The Research Loop

### Phase 1: Literature Review
Before researching any event category, first check what's already known:
- Search for academic finance papers on the topic (event studies, anomaly research)
- Check established quantitative findings (e.g., PEAD — Post-Earnings Announcement Drift is well-documented)
- Don't rediscover known effects — build on them or find gaps

### Phase 2: Historical Mining
Search the web for **event dates** — when did specific events occur? Then verify price impact with `measure_event_impact()`.
- **Web search is for finding dates, not for determining what happened.** News articles cherry-pick dramatic examples. Never form hypotheses from narratives alone.
- "When did major FDA rejections happen in biotech?" → get dates → `measure_event_impact()`
- "When did CPI come in significantly above consensus?" → get dates → `measure_event_impact()`
- **Minimum 12 historical instances** before considering a pattern real (5 for exploratory investigation only)
- **Specify event timing**: pass `event_timing="after_hours"` etc. to `measure_event_impact()` for correct reference price selection. An after-hours FDA decision uses the day's close as reference, not the previous day's.

#### Survivorship Bias Protocol
For event types where companies could have failed (FDA rejections, earnings disasters, bankruptcies, dividend cuts):
- **Actively search for delisted/bankrupt companies** that experienced the event
- Include at least 2 delisted companies in the sample, OR explicitly document that none are relevant and how you verified this
- High-risk categories: `fda_decision`, `earnings_surprise`, `dividend_changes`, `regulatory_changes`

### Phase 3: Pattern Extraction (with rigor)
From historical data, identify reliable patterns:
- Does the effect repeat consistently across multiple instances?
- What's the typical magnitude and timeframe?
- **Use ABNORMAL returns, not raw returns.** `market_data.py` automatically computes stock return minus SPY return. A stock going up 3% when SPY went up 2.5% is a 0.5% abnormal return — barely an effect. Always look at `abnormal_*` fields, not `raw_*`.
- **Use sector-adjusted returns** when available. Pass `sector_etf="XLV"` (or appropriate ETF) to `measure_event_impact()` to strip out sector-wide moves.
- **Check statistical significance**: look at `p_value_abnormal_*` and `significant_abnormal_*` fields.
- **Check multiple testing correction**: look at `passes_multiple_testing` field. With 5 horizons tested per event type, a single p<0.05 hit has ~23% chance of being spurious. Need 2+ horizons significant at p<0.05, or 1 horizon at p<0.01.
- **Check data quality**: look at `data_quality_warning` field. If >30% of attempted events failed to produce data, the backtest is unreliable — investigate before proceeding.
- **Standard deviation matters**: if avg abnormal return is +2% but stdev is 8%, the effect is drowned in noise. Look at `stdev_*` fields.

#### Out-of-Sample Validation (REQUIRED)
Before forming any hypothesis:
1. Split historical instances: 70% discovery set, 30% validation set
2. Find the pattern in the discovery set
3. Verify it holds in the validation set (minimum 3 validation instances)
4. Use `research.validate_out_of_sample()` to validate the split
5. If the pattern doesn't hold out-of-sample, it's curve-fitted — record as dead end

#### Regime Conditioning
When you have N≥15 historical instances:
- Subset by VIX regime: calm (<20), elevated (20-30), crisis (>30)
- Check if the effect is regime-dependent
- If it only works in calm markets, note that — and only test it in calm markets

### Phase 4: Hypothesis Formation
Structure every hypothesis as:
**"When [specific event type] occurs, [specific stock/sector] moves [direction] by [approximate magnitude] within [timeframe], because [causal mechanism]."**

**ALL of these gates must pass before a hypothesis is valid:**

1. **Statistical significance**: `passes_multiple_testing` is True from `measure_event_impact()` results. If False, stop — record as dead end or collect more data.
2. **Effect size**: abnormal return > `min_abnormal_return_pct` from methodology.json (default 1.5%).
3. **Out-of-sample validation**: pattern holds in both discovery (70%) and validation (30%) sets.
4. **Causal mechanism** satisfies at least 2 of 3 criteria from the rubric:
   - Identifies specific economic actors and their incentives
   - Explains the transmission channel
   - References an established economic principle or academic finding
   - *"Stocks go up because they always do" is not a mechanism.*
5. **Confidence score** computed via `self_review.compute_confidence_score()` — NOT assigned by feel.
6. **Survivorship bias note** (REQUIRED): how was this addressed?
7. **Selection bias note** (REQUIRED): how was this addressed?
8. **Event timing** specified: pre_market/intraday/after_hours/unknown
9. **Regime note**: is the effect regime-dependent? (required when N≥15)
10. Known confounders and how we account for them

**Pre-registration**: when `create_hypothesis()` is called, the prediction is hashed and logged to results.jsonl before any trade is placed. This prevents post-hoc adjustment.

If you can't fill in every field, it's not testable — keep researching.

### Phase 5: Live Testing
When a matching event occurs in the real world:
- Place a paper trade via Alpaca
- Set clear entry price, expected move, and deadline
- **Position size is UNIFORM at ~5% of portfolio ($5,000)** — same for every experiment. This is a research project: varying size by confidence optimizes for P&L, not learning.
- Document the market context at entry (VIX, sector trends, recent news)
- Specify event_timing for correct reference price handling

### Phase 6: Post-Mortem (most important phase)
When the experiment concludes (hit target, hit deadline, or invalidated):
- Record actual vs expected outcome using **abnormal returns** (pass `spy_return_pct` to `complete_hypothesis()`)
- Was the direction correct? **Was the magnitude meaningful?** A +0.1% abnormal return on a +5% prediction is not a successful prediction — it's indistinguishable from noise. Check the `magnitude_ratio` in the result.
- What confounding factors were present?
- Did the causal mechanism hold, or did something else drive the outcome?
- **What did we learn?** This is the actual output of the project.
- **Check promotion/retirement**: run `research.check_promotion_or_retirement(event_type)` after each completed experiment. If it returns "promote", call `record_known_effect()`. If "retire", call `record_dead_end()`.
- Update the pattern library (patterns.json)

### Promotion and Retirement Criteria
**Promotion to known_effects** (from methodology.json):
- ≥3 live tests
- ≥60% live accuracy
- ≥0.3 average magnitude ratio
- Promotes to `knowledge_base.json` known_effects

**Retirement** (record as dead end):
- ≥5 live tests
- ≤30% accuracy
- Record in `knowledge_base.json` dead_ends with full post-mortem

### Self-Improvement (the most important part)
The methodology is NOT static. It evolves based on results.

**How it works:**
- `methodology.json` contains the current research parameters (sample sizes, thresholds, per-category overrides)
- `self_review.py` analyzes all completed experiments and updates methodology.json
- Self-review triggers automatically every 10 completed experiments (configurable)
- Before placing any trade, check `self_review.get_category_settings(event_type)` for current rules — they may differ from defaults

**What self-review checks:**
0. **Magnitude accuracy** — are "correct" predictions actually achieving meaningful magnitude? A +0.1% move on a +5% prediction is noise, not signal. Categories where direction looks good but magnitude ratio is <0.2 get flagged as "overfit_direction".
1. **Confidence calibration** — are high-confidence predictions actually more accurate? If not, flag for recalibration
2. **Per-category performance** — considers BOTH direction accuracy AND magnitude ratio. Flags categories for promotion or retirement based on methodology.json promotion_criteria. **Does NOT adjust position sizes** — sizes are uniform for research integrity.
3. **Timeframe analysis** — are our expected timeframes matching reality?
4. **Sample size impact** — do hypotheses backed by more historical data perform better? Adjusts minimums.
5. **Knowledge decay** — `check_knowledge_decay()` finds known effects that haven't been revalidated within `knowledge_revalidation_months` (default 12). Stale effects get queued for re-testing.

**What YOU (Claude) should do beyond automated checks:**
- After each self-review, read the report and think about what the numbers mean
- Add new confounders to track if post-mortems reveal factors you weren't considering
- Propose new research categories if patterns suggest unexplored adjacent areas
- If the methodology changelog shows repeated oscillation (raising then lowering a parameter), something deeper is wrong — investigate
- Write a reflection in logs/research_notes.md after each self-review

**The living documents:**
- `methodology.json` — current parameters (machine-readable, auto-updated)
- `knowledge_base.json` — what we know (literature, validated effects, dead ends)
- `patterns.json` — statistical aggregates from experiments
- `logs/research_notes.md` — free-form reasoning journal

## What Claude Should Do Each Session

### Thinking Like a Researcher (not a coder)
You are not executing a script. You are conducting research. Each session should feel like a day in a lab:
- Read your previous notes and understand where you left off
- Check if the previous session left you specific tasks (research_queue.json → next_session_priorities)
- Think about what you've learned so far and what's the most valuable question to answer next
- Don't just collect data — interpret it, form theories, challenge your own assumptions
- At the end, set priorities for the next session based on what you learned today

### Session Structure
Sessions are split into three types to prevent scope overload:

**Morning session (9 AM ET) — Operations:**
1. Orient — read state files, check if previous session crashed (logs/session_state.json).
2. Check event watchlist for triggered events. Search news for matches.
3. Review active experiments — close those past deadline with real post-mortems.
4. Post-mortems must evaluate BOTH direction AND magnitude. A "correct direction" with <25% of predicted magnitude is noise.
5. Check promotion/retirement for categories with completed experiments.
6. Self-review if due. Check knowledge decay.
7. Activate pending hypotheses if events have triggered. **Uniform $5,000 position size.**
8. Set priorities for evening research session.
9. Report and append to research_notes.md.

**Midday session (1 PM ET) — Event Scan (lightweight):**
1. Check event watchlist for triggered events.
2. Search headlines for events matching researched patterns.
3. If a matching event occurred, note it for the operations session.
4. Check if active experiments hit their deadline today.
5. Set next_session_priorities with findings.
6. **No deep research, no backtests, no hypothesis formation, no email.**

**Evening session (5 PM ET) — Research:**
1. Orient — read state files, check morning session results.
2. Follow the research queue (specific questions, not vague categories).
3. Literature review → backtest with `measure_event_impact()` → check significance.
4. **Gate hypotheses on ALL evidence gates** (see Phase 4): multiple testing, effect size, out-of-sample, causal mechanism rubric, bias notes.
5. Check knowledge decay — queue revalidation for stale effects.
6. Set priorities for next morning session.
7. Report and append to research_notes.md.

Do NOT mix operations and research in the same session.

### Research Categories to Explore
Systematically work through these, mining history for each:
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

### Data Sources to Use
- Web search for **event dates** (NOT for determining price impact — verify with backtest)
- Yahoo Finance for historical price data (via `market_data.py`)
- SEC EDGAR for filings (13D, insider transactions)
- FRED for macro economic data
- Google Trends for sentiment/search patterns
- Academic finance papers for established patterns
- Prediction markets for probability-weighted scenarios

## Architecture

### Files
- `config.py` — API keys and settings
- `research.py` — hypothesis lifecycle, knowledge base, pattern library, promotion/retirement
- `self_review.py` — meta-learning engine, analyzes performance and updates methodology
- `methodology.json` — living research parameters (auto-updated by self_review.py)
- `market_data.py` — fetch historical prices, measure event impacts, backtest hypotheses
- `research_queue.py` — research direction: task queue, event watchlist, cross-session priorities
- `research_queue.json` — what to research next, upcoming events to watch, next session tasks
- `trader.py` — place and close paper trades via Alpaca
- `run.py` — status and experiment review CLI
- `email_report.py` — sends HTML email digest with patterns and knowledge base
- `hypotheses.json` — all hypotheses (pending, active, completed, invalidated)
- `patterns.json` — validated pattern library, updated after each completed experiment
- `knowledge_base.json` — literature reviews, known effects, dead ends (so we don't repeat work)
- `results.jsonl` — growing dataset of tested cause→effect relationships + pre-registrations
- `logs/research_notes.md` — cumulative research journal across sessions (append-only)
- `daily_research.sh` — headless runner invoked by launchd (morning=operations, midday=scan, evening=research)
- `logs/session_state.json` — tracks current/last session status for crash recovery
- `logs/` — execution logs

### Key Tools for Research
- `market_data.get_price_around_date(symbol, "YYYY-MM-DD", event_timing="after_hours")` — get prices before/after a historical event, with computed returns at 1d/3d/5d/10d/20d horizons. Pass event_timing for correct reference price.
- `market_data.measure_event_impact(symbol, [dates], benchmark="SPY", sector_etf="XLV", event_timing="after_hours")` — backtest across historical instances. Returns raw, abnormal (vs SPY), and sector-adjusted returns with stats. Also returns `passes_multiple_testing`, `data_quality_warning`, and `significant_horizons`. Event dates can be strings or dicts with timing: `[{"date": "2024-01-15", "timing": "after_hours"}]`.
- `research.validate_out_of_sample(evidence, discovery_idx, validation_idx)` — validate train/test split
- `research.validate_causal_mechanism(text, criteria_met)` — check mechanism meets rubric (2 of 3)
- `research.create_hypothesis(...)` — creates hypothesis with pre-registration hash. Now REQUIRES: `causal_mechanism_criteria`, `out_of_sample_split`, `survivorship_bias_note`, `selection_bias_note`.
- `research.check_promotion_or_retirement(event_type)` — check if pattern should be promoted to known_effects or retired
- `research.record_literature(event_type, findings)` — store what academic research says about an event type
- `research.record_known_effect(event_type, effect)` — record a validated causal effect
- `research.record_dead_end(event_type, reason)` — mark a research direction as not worth pursuing
- `research_queue.add_research_task(category, question, priority, reasoning)` — queue a specific research question
- `research_queue.add_event_to_watchlist(event, date, symbol, hypothesis)` — watch for an upcoming event
- `research_queue.set_next_session_priorities(["task1", "task2"])` — tell the next session what to do
- `research_queue.get_next_research_task()` — get highest-priority pending task
- `research_queue.get_due_events()` — check for watchlist events that happened
- `self_review.compute_confidence_score(sample_size, consistency_pct, avg_return, stdev_return, has_literature)` — compute evidence-based confidence score (replaces vibes-based assignment)
- `self_review.check_knowledge_decay()` — find known effects that need revalidation

### Alpaca Paper Trading
- Cash: $100,000, fractional shares enabled, shorting enabled
- API keys in config.py
- Base URL: https://paper-api.alpaca.markets

## Principles

### DO
- Be the researcher — proactively find patterns, don't wait for direction
- Use web search for **event dates**, then verify with `measure_event_impact()`
- Think like a scientist: hypothesis → experiment → measure → learn
- Pick the best hypothesis and execute it — don't present options and ask
- A loss that teaches something is a success
- Build the dataset methodically — every completed experiment adds knowledge
- Compute confidence via `compute_confidence_score()` — never guess
- Split samples: 70% discovery, 30% validation — always
- Check `passes_multiple_testing` before forming hypotheses
- Actively search for delisted companies when backtesting failure events

### DON'T
- Don't be cautious and disclaimy
- Don't speculate on unpredictable events (war outcomes, elections, etc.)
- Don't optimize for P&L — optimize for learning about causal relationships
- Don't skip the historical research — always check what happened in past similar events before trading
- Don't treat this as a trading bot — it's a research project
- Don't present 5 options and ask "which one?" — pick the best and do it
- Don't vary position sizes by confidence or past performance — uniform $5,000
- Don't form hypotheses from web search narratives — verify with backtest data
- Don't accept a single significant horizon as proof — check multiple testing
- Only unexamined outcomes are failures
