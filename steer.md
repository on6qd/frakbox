# Research Steering

Write directions here. The researcher reads this at the start of every session.
You can add topics to explore, questions to answer, or priorities to shift.
The researcher will note which items it picked up.

## Directions

## 2026-03-23 Politician Trading Signal (RESPONDED)
User asked: look into the relationship between the buy/sell orders of us politicians, or any legal entity they can be connected too, or relatives etc.

EXHAUSTIVELY TESTED (5 approaches, all dead ends):
1. 2023-2026 Capitol Trades (25,493 records, all large-cap purchases): +0.20% avg, direction=44%. Effect too small, below 0.5% threshold.
2. 2012-2020 Senate Stock Watcher data (8,350 records with SPOUSE flag): Tested self/spouse/joint/child separately. All p>0.10, direction 39-45%. No signal. STOCK Act disclosure lag (avg 30 days) eliminates any informational edge.
3. Reporting gap segmentation (fast/medium/slow disclosures): Medium(11-30d) p=0.0004 but direction=47% ONLY — below 50% threshold. Not tradeable.
4. Timothy Moore (R/House): Signal only in 2025 — regime-specific to Trump tariff volatility. Not generalizable.
5. Markwayne Mullin (Senate): n=83, direction=51%, too small.

Conclusion: DEAD END. US politician stock trading offers no consistently tradeable signal. Tested across:
- Date ranges: 2012-2020 and 2023-2026
- Owner types: politician self, spouse, joint accounts, children
- Trade sizes: all amounts, >$15K, >$50K
- Disclosure speed: fast (<10d), medium (11-30d), slow (>30d)
- Individual politicians with high activity

Root cause: STOCK Act 30-45 day disclosure lag means all information is priced in by the time it's public. Dead ends recorded in knowledge base.

## 2026-03-22 Disposition Effect / Cost Basis Signal (RESPONDED)
User suggested: When stock rises on low volume, lots of investors have unrealized gains -> produces selling pressure. When stock rises on high volume, less investors have unrealized gains -> produces less selling. Is it possible to calculate the average price at which investors are in a stock, and compare that to the actual price to predict direction?

TESTED (4 approaches, all dead ends):
1. VWAP overextension — signal exists but REVERSED from hypothesis (overextended stocks continued UP, not down — actually a LONG signal, not short)
2. Volume-disposition backtest — no signal at scale (100+ stocks, N=14,000+ observations); p-values 0.20-0.85 across all horizons
3. VWAP momentum — small universe finding (+0.64-1.19%) did not replicate with 100-stock universe; direction inconsistent across tests
4. Volume profile percent-in-profit — no signal above noise; positive_rate near 50% across all threshold levels

Conclusion: The disposition effect exists at the individual investor level but is overwhelmed by institutional momentum for large-cap stocks. VWAP deviation is a contrarian indicator pointing the wrong direction (overextended stocks keep going — momentum, not mean-reversion). This signal direction is a dead end. See knowledge_base.json for full recorded notes.
