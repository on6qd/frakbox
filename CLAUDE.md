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
Search the web for historical instances of specific event types and measure what happened to prices:
- "What happened to airline stocks after past oil price spikes?"
- "How do pharma stocks react after FDA rejections? How long does the effect last?"
- "When CPI comes in hot, which sectors move and by how much?"
- Use financial news archives, historical price data, academic research
- **Minimum 5 historical instances** before considering a pattern real

### Phase 3: Pattern Extraction (with rigor)
From historical data, identify reliable patterns:
- Does the effect repeat consistently across multiple instances?
- What's the typical magnitude and timeframe?
- **Use ABNORMAL returns, not raw returns.** `market_data.py` automatically computes stock return minus SPY return. A stock going up 3% when SPY went up 2.5% is a 0.5% abnormal return — barely an effect. Always look at `abnormal_*` fields, not `raw_*`.
- **Use sector-adjusted returns** when available. Pass `sector_etf="XLV"` (or appropriate ETF) to `measure_event_impact()` to strip out sector-wide moves. A biotech going up 4% after FDA approval when all biotech went up 3% is only a 1% event effect.
- Is the sample size large enough to be meaningful?
- **Out-of-sample check**: did the pattern hold in different time periods, different market regimes?
- **Base rate**: how often does this event type occur? Rare events = small sample = less reliable.
- **Standard deviation matters**: if avg abnormal return is +2% but stdev is 8%, the effect is drowned in noise. Look at `stdev_*` fields.

### Phase 4: Hypothesis Formation
Structure every hypothesis as:
**"When [specific event type] occurs, [specific stock/sector] moves [direction] by [approximate magnitude] within [timeframe], because [causal mechanism]."**

Required fields before a hypothesis is valid:
- Causal mechanism (WHY, not just correlation)
- Sample size (N historical instances)
- Consistency rate (what % showed the expected effect)
- **Statistical significance**: p-value < 0.05 from `measure_event_impact()` t-test results. Check `p_value_abnormal_*` and `significant_abnormal_*` fields. If the effect isn't distinguishable from random, it's not a pattern — record it as a dead end.
- **Effect size above threshold**: abnormal return > `min_abnormal_return_pct` from methodology.json (default 1.5%). Trivially small effects waste experiments.
- Known confounders and how we account for them
- **Survivorship bias note** (required): could this pattern look different if delisted/failed companies were included? For FDA rejections, bankruptcies, etc. — actively search for companies that no longer exist.
- **Selection bias note** (required): are you finding this pattern because dramatic instances get news coverage? Did you search for instances where the event happened and the stock did NOT move notably?
- Current market regime and whether it matters
- **Confidence score (1-10) computed via `self_review.compute_confidence_score()`** — NOT assigned by feel. The function scores: sample size (0-3), consistency rate (0-3), signal-to-noise ratio (0-3), literature support (0-1).

If you can't fill in every field, it's not testable — keep researching.

### Phase 5: Live Testing
When a matching event occurs in the real world:
- Place a paper trade via Alpaca
- Set clear entry price, expected move, and deadline
- Size position at ~5% of portfolio ($5,000)
- Document the market context at entry (VIX, sector trends, recent news)

### Phase 6: Post-Mortem (most important phase)
When the experiment concludes (hit target, hit deadline, or invalidated):
- Record actual vs expected outcome using **abnormal returns** (pass `spy_return_pct` to `complete_hypothesis()`)
- Was the direction correct? **Was the magnitude meaningful?** A +0.1% abnormal return on a +5% prediction is not a successful prediction — it's indistinguishable from noise. Check the `magnitude_ratio` in the result.
- What confounding factors were present?
- Did the causal mechanism hold, or did something else drive the outcome?
- **What did we learn?** This is the actual output of the project.
- Update the pattern library (patterns.json)
- Should we increase or decrease confidence in this pattern?

### Self-Improvement (the most important part)
The methodology is NOT static. It evolves based on results.

**How it works:**
- `methodology.json` contains the current research parameters (sample sizes, thresholds, position sizes, per-category overrides)
- `self_review.py` analyzes all completed experiments and updates methodology.json
- Self-review triggers automatically every 10 completed experiments (configurable)
- Before placing any trade, check `self_review.get_category_settings(event_type)` for current rules — they may differ from defaults

**What self-review checks:**
0. **Magnitude accuracy** — are "correct" predictions actually achieving meaningful magnitude? A +0.1% move on a +5% prediction is noise, not signal. Categories where direction looks good but magnitude ratio is <0.2 get flagged as "overfit_direction".
1. **Confidence calibration** — are high-confidence predictions actually more accurate? If not, flag for recalibration
2. **Per-category performance** — considers BOTH direction accuracy AND magnitude ratio. High performers get increased position sizes, underperformers get reduced sizes, consistently bad categories get retired
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
Sessions are split into two types to prevent scope overload:

**Morning session (9 AM ET) — Operations:**
1. Orient — read state files, check if previous session crashed (logs/session_state.json).
2. Check event watchlist for triggered events. Search news for matches.
3. Review active experiments — close those past deadline with real post-mortems.
4. Post-mortems must evaluate BOTH direction AND magnitude. A "correct direction" with <25% of predicted magnitude is noise.
5. Self-review if due. Check knowledge decay.
6. Activate pending hypotheses if events have triggered.
7. Set priorities for evening research session.
8. Report and append to research_notes.md.

**Evening session (5 PM ET) — Research:**
1. Orient — read state files, check morning session results.
2. Follow the research queue (specific questions, not vague categories).
3. Literature review → backtest with `measure_event_impact()` → check significance.
4. **Gate hypotheses on evidence**: p-value < 0.05, abnormal return > 1.5%, confidence from `compute_confidence_score()`.
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
- Web search for financial news and historical events
- Yahoo Finance for historical price data
- SEC EDGAR for filings (13D, insider transactions)
- FRED for macro economic data
- Google Trends for sentiment/search patterns
- Academic finance papers for established patterns
- Prediction markets for probability-weighted scenarios

## Architecture

### Files
- `config.py` — API keys and settings
- `research.py` — hypothesis lifecycle, knowledge base, pattern library
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
- `results.jsonl` — growing dataset of tested cause→effect relationships
- `logs/research_notes.md` — cumulative research journal across sessions (append-only)
- `daily_research.sh` — headless runner invoked by launchd (morning=operations, evening=research)
- `logs/session_state.json` — tracks current/last session status for crash recovery
- `logs/` — execution logs

### Key Tools for Research
- `market_data.get_price_around_date(symbol, "YYYY-MM-DD")` — get prices before/after a historical event, with computed returns at 1d/3d/5d/10d/20d horizons
- `market_data.measure_event_impact(symbol, [dates], benchmark="SPY", sector_etf="XLV")` — backtest across historical instances. Returns raw, abnormal (vs SPY), and sector-adjusted returns with stats. Now also returns `t_stat_abnormal_*`, `p_value_abnormal_*`, and `significant_abnormal_*` fields for statistical significance testing. ALWAYS use abnormal returns for analysis.
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
- Use web search extensively — mine years of historical data
- Think like a scientist: hypothesis → experiment → measure → learn
- Pick the best hypothesis and execute it — don't present options and ask
- A loss that teaches something is a success
- Build the dataset methodically — every completed experiment adds knowledge
- Compute confidence via `compute_confidence_score()` — never guess

### DON'T
- Don't be cautious and disclaimy
- Don't speculate on unpredictable events (war outcomes, elections, etc.)
- Don't optimize for P&L — optimize for learning about causal relationships
- Don't skip the historical research — always check what happened in past similar events before trading
- Don't treat this as a trading bot — it's a research project
- Don't present 5 options and ask "which one?" — pick the best and do it
- Only unexamined outcomes are failures
