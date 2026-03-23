#!/bin/bash
# Unified daemon — research sessions, trade execution, and health monitoring.
# Usage: ./researcher.sh          (foreground)
#        nohup ./researcher.sh &  (background)
#
# Runs three tasks on different intervals from a single process:
#   - Trade loop:    every 2 min  (stop-losses, triggers, reconciliation)
#   - Health check:  every 10 min (watchdog, alerts, auto-restart)
#   - Research:      every 15 min (LLM sessions, 50 min each)

set -euo pipefail
cd "$(dirname "$0")"

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

LOCKFILE="${TMPDIR:-/tmp}/research_bot_$(id -u).lock"
SESSION_INTERVAL=900       # 15 min between research sessions
TRADE_INTERVAL=120         # 2 min between trade loop runs
HEALTH_INTERVAL=600        # 10 min between health checks
TICK=60                    # main loop tick (1 min)
MAX_SESSIONS_PER_DAY=64
RESEARCH_START_HOUR=21     # LLM sessions start at 9 PM local
RESEARCH_END_HOUR=7        # LLM sessions stop at 7 AM local

set -a; source .env; set +a
source venv/bin/activate 2>/dev/null || true

# Timestamps for interval tracking
last_trade=0
last_health=0
last_session=0
digest_sent=0

now() { date +%s; }

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

run_trade_loop() {
  python3 trade_loop.py >> logs/trade_loop.log 2>&1 || true
}

run_health_check() {
  python3 health_check.py >> logs/health_check.log 2>&1 || true
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

  echo "=== Session started $(date) ===" | tee -a logs/daemon.log | tee "$logfile"

  local prompt
  prompt=$(cat <<'PROMPT'
Your agent constitution (.claude/agents/financial-researcher.md) is loaded automatically — it defines your scientific standards and operational rules.

Read steer.md first — it contains directions from the human researcher. If there are active directions, prioritize them over your own queue. Note which ones you picked up.

Then read CLAUDE.md and your state files:
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
  echo "=== Session finished $(date) (exit code: $exit_code) ===" >> logs/daemon.log

  # Determine session status from exit code
  local status="completed"
  if [ $exit_code -eq 124 ]; then
    status="timed_out"
  elif [ $exit_code -ne 0 ]; then
    status="crashed"
  fi

  # Log status (daily digest sent at end of research window instead)
  echo "Session $status: $logfile" >> logs/daemon.log
}

# ---- Main loop ----

echo "Daemon started. Research every ${SESSION_INTERVAL}s, trades every ${TRADE_INTERVAL}s, health every ${HEALTH_INTERVAL}s." | tee -a logs/daemon.log

while true; do
  current=$(now)

  # Trade loop — every 2 min (fast, lightweight)
  if (( current - last_trade >= TRADE_INTERVAL )); then
    run_trade_loop
    last_trade=$(now)
  fi

  # Health check — every 10 min
  if (( current - last_health >= HEALTH_INTERVAL )); then
    run_health_check
    last_health=$(now)
  fi

  # Research session — every 15 min, only during night window (9 PM – 7 AM)
  local hour=$(date +%H)
  local in_window=0
  if (( hour >= RESEARCH_START_HOUR || hour < RESEARCH_END_HOUR )); then
    in_window=1
    if (( current - last_session >= SESSION_INTERVAL )); then
      run_session &
      last_session=$(now)
    fi
  fi

  # Daily digest — send once when research window closes
  if (( in_window == 0 && digest_sent == 0 )); then
    echo "Sending daily digest..." >> logs/daemon.log
    python3 -c "from email_report import send_report; send_report()" >> logs/daemon.log 2>&1 || true
    digest_sent=1
  fi
  # Reset digest flag when window opens again
  if (( in_window == 1 )); then
    digest_sent=0
  fi

  sleep $TICK
done
