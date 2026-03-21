#!/bin/bash
cd "$(dirname "$0")"

if pgrep -f "run.sh" >/dev/null; then
  echo "Already running (pid $(pgrep -f run.sh))"
  exit 1
fi

nohup ./run.sh > /dev/null 2>&1 &
echo "Started (pid $!)"
