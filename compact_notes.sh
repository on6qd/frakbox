#!/bin/bash
# Compact research_notes.md when it gets too long.
# Keeps the last 20 entries verbatim and summarizes older ones.
# Run manually or add to a weekly cron.
#
# Strategy: entries are separated by "---". When there are >30 entries,
# the oldest (all except last 20) get moved to research_notes_archive.md
# and a one-line summary of each is left in place.

cd /Users/bartdelepeleer/trading_bot2

NOTES="logs/research_notes.md"
ARCHIVE="logs/research_notes_archive.md"

if [ ! -f "$NOTES" ]; then
  echo "No research notes to compact."
  exit 0
fi

# Count entries (separated by ---)
ENTRY_COUNT=$(grep -c '^---$' "$NOTES")

if [ "$ENTRY_COUNT" -le 30 ]; then
  echo "Only $ENTRY_COUNT entries — no compaction needed (threshold: 30)."
  exit 0
fi

echo "Found $ENTRY_COUNT entries. Compacting..."

# Use Claude to summarize old entries and keep recent ones
source .env
source venv/bin/activate

claude --max-turns 5 -p "Read logs/research_notes.md. It has $ENTRY_COUNT dated entries separated by '---'.

Your task:
1. Identify the 20 most recent entries (by date).
2. For every OLDER entry, write a one-line summary: '- YYYY-MM-DD: <what was researched and key finding>'
3. Append the full text of older entries to logs/research_notes_archive.md (create if needed).
4. Rewrite logs/research_notes.md with this structure:
   - The header (# Research Notes + description)
   - A section '## Archived sessions' with the one-line summaries
   - '---'
   - The 20 most recent entries, verbatim (do not modify them)

Preserve all content — this is archival, not deletion."
