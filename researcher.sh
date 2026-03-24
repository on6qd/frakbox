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

set -a; source .env; set +a
MAX_SESSIONS_PER_DAY="${MAX_SESSIONS_PER_DAY:-10}"  # from .env
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
    echo "=== Heartbeat $(date) ===" >> logs/daemon.log
    return
  fi

  echo "=== Session started $(date) ===" | tee -a logs/daemon.log | tee "$logfile"

  local prompt
  prompt=$(cat <<'PROMPT'
Run: python3 run.py --context
This is your complete state — account, trades, hypotheses, knowledge, queue, journal, friction, and data integrity. Steer.md (human directions) is included. Prioritize human directions over your own queue.

Do NOT dump full datasets (load_hypotheses(), load_knowledge(), load_queue()). Only query individual items (get_hypothesis_by_id, get_known_effect, db.get_recent_journal, etc.) when you need deep detail.

Read API_REFERENCE.md only when you need a function signature — not at session start.

You have ~50 minutes. Commit to git after each significant finding.

Do the work. When done:
1. Update research_queue with handoff for the next session
2. Log journal entry: db.append_journal_entry(date, type, investigated, findings, surprised_by, next_step)
3. Commit to git
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

  # Log token usage from the session to SQLite
  python3 -c "
from email_report import parse_token_usage
import db
db.init_db()
usage = parse_token_usage('$logfile')
if usage.get('total_tokens', 0) > 0:
    db.append_token_usage(
        input_tokens=usage.get('input_tokens', 0),
        output_tokens=usage.get('output_tokens', 0),
        cache_read_tokens=usage.get('cache_read_tokens', 0),
        cache_creation_tokens=usage.get('cache_creation_tokens', 0),
        total_tokens=usage.get('total_tokens', 0),
        api_calls=usage.get('api_calls', 0),
        session='$logfile',
        status='$status',
    )
" >> logs/daemon.log 2>&1 || true

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

  # Research session — every 15 min (daily limit enforced inside run_session)
  if (( current - last_session >= SESSION_INTERVAL )); then
    run_session &
    last_session=$(now)
  fi

  # Daily digest — send once at 7 AM
  hour=$((10#$(date +%H)))
  if (( hour >= 7 && hour < 8 && digest_sent == 0 )); then
    echo "Sending daily digest..." >> logs/daemon.log
    python3 -c "from email_report import send_report; send_report()" >> logs/daemon.log 2>&1 || true
    digest_sent=1
  fi
  # Reset digest flag after the window
  if (( hour >= 8 )); then
    digest_sent=0
  fi

  sleep $TICK
done
