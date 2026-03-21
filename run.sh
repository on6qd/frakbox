#!/bin/bash
# Daemon — runs a research session every hour.
# Usage: ./run.sh          (foreground)
#        nohup ./run.sh &  (background)

set -euo pipefail
cd "$(dirname "$0")"

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

LOCKFILE="/tmp/research_bot.lock"
INTERVAL=3600

set -a; source .env; set +a
source venv/bin/activate 2>/dev/null || true

run_session() {
  local timestamp=$(date +%Y-%m-%d_%H%M)
  local logfile="logs/${timestamp}.log"
  mkdir -p logs

  # Skip if already running
  if ! mkdir "$LOCKFILE" 2>/dev/null; then
    echo "$(date): Session already running, skipping." | tee -a "$logfile"
    return
  fi
  trap 'rmdir "$LOCKFILE" 2>/dev/null' RETURN

  echo "=== Session started $(date) ===" | tee "$logfile"

  local prompt
  prompt=$(cat <<'PROMPT'
You are a researcher. Read CLAUDE.md, then read your state files:
- research_queue.json (priorities and handoff from last session)
- knowledge_base.json (what you know)
- hypotheses.json (all hypotheses)
- methodology.json (research parameters)
- logs/research_journal.jsonl (session history)

Then: source venv/bin/activate && python run.py --status

Decide what's most valuable to work on right now. You might:
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

  $TIMEOUT_CMD claude \
    --agent financial-researcher \
    --permission-mode dontAsk \
    --verbose \
    --output-format stream-json \
    -p "$prompt" 2>>"$logfile" | tee -a "$logfile" || true

  echo "=== Session finished $(date) ===" | tee -a "$logfile"

  # Send email report
  python3 email_report.py --session research completed "$logfile" "" 2>>"$logfile" || true
}

echo "Research daemon started. Running every ${INTERVAL}s."
while true; do
  run_session
  echo "Next session at $(date -v+${INTERVAL}S +%H:%M 2>/dev/null || date -d "+${INTERVAL} seconds" +%H:%M 2>/dev/null || echo "~1h")"
  sleep $INTERVAL
done
