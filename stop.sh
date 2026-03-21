#!/bin/bash
PIDS=$(pgrep -f "run.sh")

if [ -z "$PIDS" ]; then
  echo "Not running"
  exit 0
fi

kill $PIDS 2>/dev/null
rmdir /tmp/research_bot.lock 2>/dev/null
echo "Stopped"
