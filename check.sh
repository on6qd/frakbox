#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate 2>/dev/null || true

# Running?
PIDS=$(pgrep -f "run.sh")
if [ -n "$PIDS" ]; then
  echo "RUNNING (pid $PIDS)"
else
  echo "NOT RUNNING"
fi

# Latest log — extract text from stream-json
LATEST=$(ls -t logs/*.log 2>/dev/null | head -1)
if [ -n "$LATEST" ]; then
  echo ""
  echo "Latest: $LATEST ($(du -h "$LATEST" | cut -f1))"
  echo ""
  echo "Recent activity:"
  python3 -c "
import json, sys
lines = open('$LATEST').readlines()
for line in lines[-50:]:
    try:
        d = json.loads(line)
        if d.get('type') == 'assistant':
            for c in d['message']['content']:
                if c['type'] == 'text' and c['text'].strip():
                    text = c['text'].strip()[:200]
                    print(f'  {text}')
        elif d.get('type') == 'result':
            print(f'  --- Done: {d[\"num_turns\"]} turns, \${d[\"total_cost_usd\"]:.2f} ---')
    except:
        pass
" 2>/dev/null | tail -10
fi

# Research status
echo ""
python3 run.py --status 2>/dev/null
