#!/bin/bash
cd "$(dirname "$0")"

# Running?
PIDS=$(pgrep -f "run.sh")
if [ -n "$PIDS" ]; then
  echo "Running (pid $PIDS)"
else
  echo "Not running"
fi

# Latest log
LATEST=$(ls -t logs/*.log 2>/dev/null | head -1)
if [ -n "$LATEST" ]; then
  echo ""
  echo "Latest: $LATEST"
  echo "Last lines:"
  tail -5 "$LATEST"
fi

# Research status
echo ""
source venv/bin/activate 2>/dev/null
python3 run.py --status 2>/dev/null
