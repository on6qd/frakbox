#!/bin/bash
# Quick health check — run manually to see how the research loop is doing.

cd "$(dirname "$0")"

echo "=== Research Bot Health ==="
echo ""

# Last session state
if [ -f logs/session_state.json ]; then
  echo "Last session:"
  python3 -c "
import json
with open('logs/session_state.json') as f:
    d = json.load(f)
for k, v in d.items():
    print(f'  {k}: {v}')
"
else
  echo "  No session state found — has the bot ever run?"
fi

echo ""

# Session history (last 7 days)
echo "Sessions (last 7 days):"
TOTAL=0
COMPLETED=0
CRASHED=0
TIMED_OUT=0
for log in logs/202*.log; do
  [ -f "$log" ] || continue
  # Only count logs from last 7 days
  LOG_DATE=$(basename "$log" | cut -d_ -f1)
  CUTOFF=$(date -v-7d +%Y-%m-%d 2>/dev/null || date -d '7 days ago' +%Y-%m-%d 2>/dev/null)
  if [[ "$LOG_DATE" > "$CUTOFF" ]] || [[ "$LOG_DATE" == "$CUTOFF" ]]; then
    TOTAL=$((TOTAL + 1))
    if grep -q "timed_out" "$log" 2>/dev/null; then
      TIMED_OUT=$((TIMED_OUT + 1))
    elif grep -q "finished" "$log" 2>/dev/null; then
      COMPLETED=$((COMPLETED + 1))
    fi
  fi
done
echo "  Total: $TOTAL | Completed: $COMPLETED | Timed out: $TIMED_OUT"
CRASH_COUNT=$(ls logs/crash_*.log 2>/dev/null | wc -l | tr -d ' ')
echo "  Crash logs: $CRASH_COUNT"

echo ""

# Research progress
echo "Research progress:"
python3 -c "
import json

rq = json.load(open('research_queue.json'))
hyp = json.load(open('hypotheses.json'))
kb = json.load(open('knowledge_base.json'))
pat = json.load(open('patterns.json'))

queue_pending = len([t for t in rq.get('queue', []) if t.get('status') == 'pending'])
queue_done = len([t for t in rq.get('queue', []) if t.get('status') == 'completed'])
watchlist = len(rq.get('event_watchlist', []))

print(f'  Research queue: {queue_pending} pending, {queue_done} completed')
print(f'  Event watchlist: {watchlist} events')
print(f'  Hypotheses: {len(hyp)}')
print(f'  Patterns: {len(pat)}')
print(f'  Literature entries: {len(kb.get(\"literature\", {}))}')
print(f'  Known effects: {len(kb.get(\"known_effects\", {}))}')
print(f'  Dead ends: {len(kb.get(\"dead_ends\", []))}')
" 2>/dev/null

echo ""

# Research notes size
if [ -f logs/research_notes.md ]; then
  LINES=$(wc -l < logs/research_notes.md | tr -d ' ')
  ENTRIES=$(grep -c '^---$' logs/research_notes.md)
  echo "Research journal: $LINES lines, ~$ENTRIES entries"
  if [ "$ENTRIES" -gt 30 ]; then
    echo "  WARNING: Consider running ./compact_notes.sh"
  fi
fi

# Launchd status
echo ""
echo "Scheduler:"
for PLIST_NAME in com.research.operations com.research.event_scan com.research.research; do
  if launchctl list | grep -q "$PLIST_NAME"; then
    echo "  $PLIST_NAME: LOADED"
  else
    echo "  $PLIST_NAME: NOT LOADED"
    echo "    Load with: launchctl load ${PLIST_NAME}.plist"
  fi
done
