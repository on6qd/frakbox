#!/bin/bash
PIDS=$(pgrep -f "run.sh" 2>/dev/null)
CLAUDE_PIDS=$(pgrep -f "claude --agent financial-researcher" 2>/dev/null)

if [ -z "$PIDS" ] && [ -z "$CLAUDE_PIDS" ]; then
  echo "Not running"
  exit 0
fi

[ -n "$PIDS" ] && kill $PIDS 2>/dev/null
[ -n "$CLAUDE_PIDS" ] && kill $CLAUDE_PIDS 2>/dev/null
rmdir /tmp/research_bot.lock 2>/dev/null
echo "Stopped"
