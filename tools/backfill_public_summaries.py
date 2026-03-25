#!/usr/bin/env python3
"""Backfill public_summary for all journal entries missing one.

Uses claude CLI to convert technical findings into readable summaries.
"""

import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

PROMPT_TEMPLATE = """Convert this technical research finding into a clear, readable 1-2 sentence public summary for a dashboard journal.

Rules:
- Write in past tense, third person ("Tested...", "Found...", "Confirmed...")
- No jargon — a smart non-quant should understand it
- Mention the key result or conclusion
- If multiple topics, pick the 1-2 most important
- Keep it under 200 characters
- No markdown, no bullet points, just plain text
- Output ONLY the summary, nothing else

Session type: {session_type}
What was investigated: {investigated}
Technical findings:
{findings}"""


def generate_summary(session_type, investigated, findings):
    """Call claude CLI to generate a readable summary."""
    prompt = PROMPT_TEMPLATE.format(
        session_type=session_type or "research",
        investigated=investigated or "",
        findings=(findings or "")[:2000],
    )
    result = subprocess.run(
        ["claude", "-p", prompt, "--model", "haiku"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def main():
    db.init_db()
    conn = db.get_db()

    rows = conn.execute(
        "SELECT id, session_type, investigated, findings FROM research_journal "
        "WHERE public_summary IS NULL OR public_summary = '' "
        "ORDER BY id"
    ).fetchall()

    print(f"Processing {len(rows)} journal entries...")

    for i, row in enumerate(rows):
        rid, session_type, investigated, findings = row
        if not findings:
            continue

        try:
            summary = generate_summary(session_type, investigated, findings)
            conn.execute(
                "UPDATE research_journal SET public_summary = ? WHERE id = ?",
                (summary, rid),
            )
            conn.commit()
            print(f"  [{i+1}/{len(rows)}] id={rid}: {summary[:80]}...")
        except Exception as e:
            print(f"  [{i+1}/{len(rows)}] id={rid}: ERROR - {e}")

    print("Done!")


if __name__ == "__main__":
    main()
