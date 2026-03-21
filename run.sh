#!/bin/bash
# Daemon — runs a research session every hour.
# Usage: ./run.sh          (foreground)
#        nohup ./run.sh &  (background)

set -euo pipefail
cd "$(dirname "$0")"

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

LOCKFILE="${TMPDIR:-/tmp}/research_bot_$(id -u).lock"
INTERVAL=900
MAX_SESSIONS_PER_DAY=24

set -a; source .env; set +a
source venv/bin/activate 2>/dev/null || true

check_daily_limit() {
  local count_file="${TMPDIR:-/tmp}/research_sessions_$(date +%Y%m%d)_$(id -u).count"
  local count=0
  if [ -f "$count_file" ]; then
    count=$(cat "$count_file")
  fi
  if [ "$count" -ge "$MAX_SESSIONS_PER_DAY" ]; then
    echo "$(date): Daily session limit ($MAX_SESSIONS_PER_DAY) reached. Skipping."
    return 1
  fi
  echo $((count + 1)) > "$count_file"
  return 0
}

run_session() {
  local timestamp=$(date +%Y-%m-%d_%H%M)
  local logfile="logs/${timestamp}.log"
  mkdir -p logs

  # Skip if already running (don't consume daily limit for lock-skips)
  if ! mkdir "$LOCKFILE" 2>/dev/null; then
    echo "$(date): Session already running, skipping."
    return
  fi
  trap 'rmdir "$LOCKFILE" 2>/dev/null' RETURN

  # Check daily session limit (only after acquiring lock; trap ensures cleanup)
  if ! check_daily_limit; then
    return
  fi

  echo "=== Session started $(date) ===" | tee "$logfile"

  local prompt
  prompt=$(cat <<'PROMPT'
Your agent constitution (.claude/agents/financial-researcher.md) is loaded automatically — it defines your scientific standards and operational rules.

Read CLAUDE.md, then read your state files:
- research_queue.json (priorities and handoff from last session)
- knowledge_base.json (what you know)
- hypotheses.json (all hypotheses)
- methodology.json (research parameters)
- logs/research_journal.jsonl (session history)
- tools/ (custom analysis tools built by previous sessions)

Then run: python run.py --status (if it fails, read the JSON files directly)

You have approximately 50 minutes before this session times out. Commit to git after each significant finding.

Check logs/friction_log.jsonl for recurring friction patterns (3+ occurrences) — build tools to fix them.

Run research.verify_data_integrity() to check for data loss or dangling references.

Decide what is most valuable to work on right now. You might:
- Follow up on priorities from the last session
- Check news for events matching your watchlist
- Research a new question from the queue
- Run backtests and form hypotheses
- Review active experiments past their deadline
- Place or close paper trades

Do the work. When done:
1. Update research_queue.json with handoff for the next session
2. Append to logs/research_journal.jsonl
3. Commit your changes to git
PROMPT
  )

  if command -v gtimeout &>/dev/null; then
    TIMEOUT_CMD="gtimeout 50m"
  elif command -v timeout &>/dev/null; then
    TIMEOUT_CMD="timeout 50m"
  else
    TIMEOUT_CMD=""
  fi

  local exit_code=0
  $TIMEOUT_CMD claude \
    --agent financial-researcher \
    --dangerously-skip-permissions \
    --verbose \
    --output-format stream-json \
    -p "$prompt" < /dev/null >>"$logfile" 2>&1 || exit_code=$?

  echo "=== Session finished $(date) (exit code: $exit_code) ===" >> "$logfile"

  # Determine session status from exit code
  local status="completed"
  if [ $exit_code -eq 124 ]; then
    status="timed_out"
  elif [ $exit_code -ne 0 ]; then
    status="crashed"
  fi

  # Send email report with actual status
  python3 email_report.py --session research "$status" "$logfile" "" 2>>"$logfile" || true
}

echo "Research daemon started. Running every ${INTERVAL}s. Max ${MAX_SESSIONS_PER_DAY} sessions/day."
while true; do
  run_session
  echo "Next session in ~1h"
  sleep $INTERVAL
done
