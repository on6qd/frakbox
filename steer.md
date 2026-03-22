# Research Steering

Write directions here. The researcher reads this at the start of every session.
You can add topics to explore, questions to answer, or priorities to shift.
The researcher will note which items it picked up.

## Directions

<!-- Example:
- Look into how activist investor 13D filings affect stock prices
- Stop working on FDA stuff, focus on insider clusters
- Is there a signal around stock buyback announcements?
-->

## 2026-03-22 Disposition Effect / Cost Basis Signal (RESPONDED)
User suggested: When stock rises on low volume, lots of investors have unrealized gains -> produces selling pressure. When stock rises on high volume, less investors have unrealized gains -> produces less selling. Is it possible to calculate the average price at which investors are in a stock, and compare that to the actual price to predict direction?

TESTED (4 approaches, all dead ends):
1. VWAP overextension — signal exists but REVERSED from hypothesis (overextended stocks continued UP, not down — actually a LONG signal, not short)
2. Volume-disposition backtest — no signal at scale (100+ stocks, N=14,000+ observations); p-values 0.20-0.85 across all horizons
3. VWAP momentum — small universe finding (+0.64-1.19%) did not replicate with 100-stock universe; direction inconsistent across tests
4. Volume profile percent-in-profit — no signal above noise; positive_rate near 50% across all threshold levels

Conclusion: The disposition effect exists at the individual investor level but is overwhelmed by institutional momentum for large-cap stocks. VWAP deviation is a contrarian indicator pointing the wrong direction (overextended stocks keep going — momentum, not mean-reversion). This signal direction is a dead end. See knowledge_base.json for full recorded notes.
