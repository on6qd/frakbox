#!/bin/bash
# Compact research_journal.jsonl when it gets too long.
# Keeps the last 50 entries and archives older ones.
# No Claude invocation needed — just tail and move.

cd "$(dirname "$0")"

JOURNAL="logs/research_journal.jsonl"
ARCHIVE="logs/research_journal_archive.jsonl"
KEEP=50
THRESHOLD=100

if [ ! -f "$JOURNAL" ]; then
  echo "No research journal to compact."
  exit 0
fi

ENTRY_COUNT=$(wc -l < "$JOURNAL" | tr -d ' ')

if [ "$ENTRY_COUNT" -le "$THRESHOLD" ]; then
  echo "Only $ENTRY_COUNT entries — no compaction needed (threshold: $THRESHOLD)."
  exit 0
fi

ARCHIVE_COUNT=$((ENTRY_COUNT - KEEP))
echo "Found $ENTRY_COUNT entries. Archiving oldest $ARCHIVE_COUNT, keeping $KEEP."

# Append older entries to archive
head -n "$ARCHIVE_COUNT" "$JOURNAL" >> "$ARCHIVE"

# Keep only the recent entries
tail -n "$KEEP" "$JOURNAL" > "${JOURNAL}.tmp" && mv "${JOURNAL}.tmp" "$JOURNAL"

echo "Done. Archive now has $(wc -l < "$ARCHIVE" | tr -d ' ') entries. Journal has $(wc -l < "$JOURNAL" | tr -d ' ') entries."
