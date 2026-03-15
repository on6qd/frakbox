#!/bin/bash
# Research cycle — invoked by launchd on the Mac Mini.
# Three session types based on time of day:
#   Morning (14:03 UTC / 9 AM ET): Operations — check events, manage experiments, post-mortems
#   Midday (18:03 UTC / 1 PM ET): Event scan — lightweight headline check only
#   Evening (22:03 UTC / 5 PM ET): Research — literature, backtesting, hypothesis formation
#
# The midday scan prevents missing time-sensitive events between sessions.

cd /Users/bartdelepeleer/trading_bot2

# Load secrets from .env
set -a
source .env
set +a

LOG_DIR="logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y-%m-%d_%H%M)
LOG_FILE="$LOG_DIR/${TIMESTAMP}.log"
SESSION_STATE="$LOG_DIR/session_state.json"

HOUR=$(date -u +%H)

# Mark session as started (for crash recovery)
if [ "$HOUR" -ge 17 ] && [ "$HOUR" -lt 19 ]; then
  SESSION_TYPE="event_scan"
elif [ "$HOUR" -lt 18 ]; then
  SESSION_TYPE="operations"
else
  SESSION_TYPE="research"
fi

cat > "$SESSION_STATE" <<STATEEOF
{
  "session_started": "$(date -Iseconds)",
  "session_type": "$SESSION_TYPE",
  "status": "running",
  "log_file": "$LOG_FILE"
}
STATEEOF

echo "=== Research cycle started $(date) — $SESSION_TYPE ===" | tee "$LOG_FILE"

if [ "$SESSION_TYPE" = "event_scan" ]; then
  # MIDDAY SESSION: Lightweight event scan — check headlines, no deep research
  SESSION_PROMPT="You are a researcher doing a quick event scan. Read CLAUDE.md for context.

Read research_queue.json to see the event watchlist and any pending hypotheses.
Read hypotheses.json for pending hypotheses ready to activate.

THIS IS A LIGHTWEIGHT EVENT SCAN. You have 5 minutes, not an hour. Do ONLY this:

1. Search news for events matching patterns on the event watchlist.
2. Search for breaking news in categories with pending hypotheses (earnings, FDA, FOMC, etc.).
3. If a matching event happened:
   - Update the event watchlist (mark_event_triggered)
   - If there's a pending hypothesis for it, note it for the operations session
   - Set next_session_priorities with what you found
4. Check if any active experiments hit their deadline today.

Do NOT: research new categories, form hypotheses, run backtests, write post-mortems, or send email.
This is a headline scan only."

elif [ "$SESSION_TYPE" = "operations" ]; then
  # MORNING SESSION: Operations — events, experiments, post-mortems
  SESSION_PROMPT="You are a researcher. Read CLAUDE.md for your full mission and methodology.

Start by reading these files — they ARE your memory:
- research_queue.json (what the previous session wants you to focus on)
- methodology.json (current research parameters, may have evolved)
- knowledge_base.json (what you know: literature, validated effects, dead ends)
- hypotheses.json (all hypotheses)
- patterns.json (statistical patterns from experiments)
- logs/research_notes.md (journal of all previous sessions)

Check logs/session_state.json — if the previous session status is 'running', it crashed. Note what it was doing and recover.

Then: source venv/bin/activate && python run.py --status

THIS IS AN OPERATIONS SESSION. Focus on:

1. Check next_session_priorities in research_queue.json — execute operational tasks.
2. Check the event watchlist for due/overdue events. Search news for events matching researched patterns.
3. Review active experiments. Close those past deadline with REAL post-mortems:
   - Was direction correct? Was MAGNITUDE correct? (a +0.1% move on a +5% prediction is not 'correct')
   - What confounders were present?
   - Did the causal mechanism hold?
   - Update the hypothesis result with abnormal returns, not raw returns.
4. Is a self-review due? (from self_review import needs_review, check_knowledge_decay)
5. Check promotion/retirement: run research.check_promotion_or_retirement() for categories with enough data.
   - If action='promote', call record_known_effect() to promote the pattern.
   - If action='retire', call record_dead_end() to retire the pattern.
6. If there are pending hypotheses ready to activate AND events have triggered, place trades.
   - Check max_concurrent_experiments before activating.
   - Position size is UNIFORM at 5% ($5,000) — do not vary by category.
7. Add newly discovered upcoming events to the watchlist.
8. Set next_session_priorities for the evening research session.
9. Send report: source venv/bin/activate && python email_report.py
10. Append to logs/research_notes.md.

Do NOT do deep research or literature reviews — that's for the evening session."
else
  # EVENING SESSION: Research — literature, backtesting, hypothesis formation
  SESSION_PROMPT="You are a researcher. Read CLAUDE.md for your full mission and methodology.

Start by reading these files — they ARE your memory:
- research_queue.json (what the previous session wants you to focus on)
- methodology.json (current research parameters, may have evolved)
- knowledge_base.json (what you know: literature, validated effects, dead ends)
- hypotheses.json (all hypotheses)
- patterns.json (statistical patterns from experiments)
- logs/research_notes.md (journal of all previous sessions)

Check logs/session_state.json — if the previous session status is 'running', it crashed. Note what it was doing and recover.

Then: source venv/bin/activate && python run.py --status

THIS IS A RESEARCH SESSION. Focus on:

1. Check next_session_priorities in research_queue.json — execute research tasks.
2. What's the most valuable research question right now? Check the queue. If empty, pick the most promising unexplored direction.
3. Do the research:
   - Literature review first: what's already known? Use record_literature().
   - Backtest with market_data.measure_event_impact() using real data.
   - CHECK STATISTICAL SIGNIFICANCE: look at p_value and t_stat fields in the results.
   - CHECK MULTIPLE TESTING: look at passes_multiple_testing field. If False, the pattern may be
     a false positive. Need 2+ horizons significant at p<0.05, or 1 horizon at p<0.01.
   - CHECK EFFECT SIZE: is the abnormal return above min_abnormal_return_pct (see methodology.json)?
   - CHECK DATA QUALITY: look at data_quality_warning field. If >30% of events failed, investigate.
   - EVENT TIMING: specify event_timing parameter ('pre_market', 'after_hours', etc.) in
     measure_event_impact() for accurate reference price selection.
   - Note survivorship bias concerns (are you only looking at surviving companies?).
     For high-risk categories (FDA, earnings, dividends, regulatory), actively search for
     delisted/failed companies to include in the sample.
   - Note selection bias concerns (are dramatic examples overrepresented?).
   - Record dead ends with record_dead_end() — don't waste future sessions.
4. OUT-OF-SAMPLE VALIDATION (required before forming hypotheses):
   - Split historical instances: 70% discovery, 30% validation.
   - Verify the pattern holds in BOTH sets.
   - Use validate_out_of_sample() from research.py.
   - Minimum 3 instances in the validation set.
5. REGIME CONDITIONING (when N>=15):
   - Subset backtests by VIX regime (calm <20, elevated 20-30, crisis >30).
   - Note if the effect is regime-dependent in the hypothesis.
6. Form hypotheses ONLY if ALL of these pass:
   - p-value < min_p_value from methodology.json (default 0.05)
   - passes_multiple_testing is True (from measure_event_impact)
   - Abnormal return > min_abnormal_return_pct (default 1.5%)
   - Out-of-sample validation holds (pattern in both discovery and validation sets)
   - Use compute_confidence_score() from self_review.py — do NOT assign confidence by feel.
   - Causal mechanism satisfies at least 2 of 3 rubric criteria (see methodology.json).
   - Fill in survivorship_bias_note and selection_bias_note (both REQUIRED).
7. Check knowledge decay: run check_knowledge_decay() from self_review. Queue revalidation tasks for stale effects.
8. Set priorities for the next (morning operations) session.
9. Send report: source venv/bin/activate && python email_report.py
10. Append to logs/research_notes.md: what you researched, what you found, what's next.

Do NOT place trades or manage positions — that's for the morning session."
fi

claude -p "$SESSION_PROMPT" 2>&1 | tee -a "$LOG_FILE"

# Mark session as completed
cat > "$SESSION_STATE" <<STATEEOF
{
  "session_started": "$(date -Iseconds)",
  "session_type": "$SESSION_TYPE",
  "status": "completed",
  "log_file": "$LOG_FILE",
  "session_ended": "$(date -Iseconds)"
}
STATEEOF

echo "=== Research cycle finished $(date) ===" | tee -a "$LOG_FILE"
