#!/bin/bash
# Research cycle — invoked by launchd on the Mac Mini.
# Two session types based on time of day:
#   Morning (14:03 UTC / 9 AM ET): Operations — check events, manage experiments, post-mortems
#   Evening (22:03 UTC / 5 PM ET): Research — literature, backtesting, hypothesis formation
#
# This split prevents a single session from trying to do everything and rushing.

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
cat > "$SESSION_STATE" <<STATEEOF
{
  "session_started": "$(date -Iseconds)",
  "session_type": "$([ "$HOUR" -lt 18 ] && echo 'operations' || echo 'research')",
  "status": "running",
  "log_file": "$LOG_FILE"
}
STATEEOF

echo "=== Research cycle started $(date) ===" | tee "$LOG_FILE"

if [ "$HOUR" -lt 18 ]; then
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
5. If there are pending hypotheses ready to activate AND events have triggered, place trades.
   - Check max_concurrent_experiments before activating.
6. Add newly discovered upcoming events to the watchlist.
7. Set next_session_priorities for the evening research session.
8. Send report: source venv/bin/activate && python email_report.py
9. Append to logs/research_notes.md.

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
   - CHECK EFFECT SIZE: is the abnormal return above min_abnormal_return_pct (see methodology.json)?
   - Note survivorship bias concerns (are you only looking at surviving companies?).
   - Note selection bias concerns (are dramatic examples overrepresented?).
   - Record dead ends with record_dead_end() — don't waste future sessions.
4. Form hypotheses ONLY if:
   - p-value < min_p_value from methodology.json (default 0.05)
   - Abnormal return > min_abnormal_return_pct (default 1.5%)
   - Use compute_confidence_score() from self_review.py — do NOT assign confidence by feel.
   - Fill in survivorship_bias_note and selection_bias_note.
5. Check knowledge decay: run check_knowledge_decay() from self_review. Queue revalidation tasks for stale effects.
6. Set priorities for the next (morning operations) session.
7. Send report: source venv/bin/activate && python email_report.py
8. Append to logs/research_notes.md: what you researched, what you found, what's next.

Do NOT place trades or manage positions — that's for the morning session."
fi

claude -p "$SESSION_PROMPT" 2>&1 | tee -a "$LOG_FILE"

# Mark session as completed
cat > "$SESSION_STATE" <<STATEEOF
{
  "session_started": "$(date -Iseconds)",
  "session_type": "$([ "$HOUR" -lt 18 ] && echo 'operations' || echo 'research')",
  "status": "completed",
  "log_file": "$LOG_FILE",
  "session_ended": "$(date -Iseconds)"
}
STATEEOF

echo "=== Research cycle finished $(date) ===" | tee -a "$LOG_FILE"
