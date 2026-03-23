#!/usr/bin/env python3
"""
One-time migration from JSON files to SQLite.

Usage:
    python migrate.py              # migrate and verify
    python migrate.py --verify     # verify only (no migration)
    python migrate.py --force      # delete existing db and re-migrate

Reads:
    hypotheses.json, knowledge_base.json, research_queue.json

Writes:
    research.db (SQLite database)

The JSON files are NOT deleted — they remain as backup.
After verifying the migration, you can safely delete them.
"""

import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)


def verify_migration():
    """Verify that the SQLite database matches the JSON files."""
    import db

    issues = []

    # 1. Hypotheses
    hyp_path = os.path.join(BASE_DIR, "hypotheses.json")
    if os.path.exists(hyp_path):
        with open(hyp_path) as f:
            json_hyps = json.load(f)
        db_hyps = db.load_hypotheses()
        if len(json_hyps) != len(db_hyps):
            issues.append(f"Hypothesis count mismatch: JSON={len(json_hyps)}, DB={len(db_hyps)}")
        else:
            json_ids = sorted(h["id"] for h in json_hyps)
            db_ids = sorted(h["id"] for h in db_hyps)
            if json_ids != db_ids:
                issues.append(f"Hypothesis IDs differ: JSON={json_ids}, DB={db_ids}")
            else:
                for jh in json_hyps:
                    dh = db.get_hypothesis_by_id(jh["id"])
                    if dh is None:
                        issues.append(f"Hypothesis {jh['id']} missing from DB")
                    elif dh.get("status") != jh.get("status"):
                        issues.append(f"Hypothesis {jh['id']} status mismatch: JSON={jh['status']}, DB={dh['status']}")
                    elif dh.get("event_type") != jh.get("event_type"):
                        issues.append(f"Hypothesis {jh['id']} event_type mismatch")

    # 2. Knowledge base
    kb_path = os.path.join(BASE_DIR, "knowledge_base.json")
    if os.path.exists(kb_path):
        with open(kb_path) as f:
            json_kb = json.load(f)
        db_kb = db.load_knowledge()
        for section in ["literature", "known_effects"]:
            jcount = len(json_kb.get(section, {}))
            dcount = len(db_kb.get(section, {}))
            if jcount != dcount:
                issues.append(f"Knowledge {section} count mismatch: JSON={jcount}, DB={dcount}")
        jde = len(json_kb.get("dead_ends", []))
        dde = len(db_kb.get("dead_ends", []))
        if jde != dde:
            issues.append(f"Dead ends count mismatch: JSON={jde}, DB={dde}")

    # 3. Research queue
    rq_path = os.path.join(BASE_DIR, "research_queue.json")
    if os.path.exists(rq_path):
        with open(rq_path) as f:
            json_rq = json.load(f)
        db_rq = db.load_queue()
        jq = len(json_rq.get("queue", []))
        dq = len(db_rq.get("queue", []))
        if jq != dq:
            issues.append(f"Queue task count mismatch: JSON={jq}, DB={dq}")
        jw = len(json_rq.get("event_watchlist", []))
        dw = len(db_rq.get("event_watchlist", []))
        if jw != dw:
            issues.append(f"Watchlist count mismatch: JSON={jw}, DB={dw}")

    return issues


def main():
    import db

    force = "--force" in sys.argv
    verify_only = "--verify" in sys.argv

    db_path = os.path.join(BASE_DIR, "research.db")

    if verify_only:
        if not os.path.exists(db_path):
            print("No database found. Run 'python migrate.py' first.")
            sys.exit(1)
        issues = verify_migration()
        if issues:
            print("Verification FAILED:")
            for i in issues:
                print(f"  - {i}")
            sys.exit(1)
        else:
            print("Verification PASSED. Database matches JSON files.")
            sys.exit(0)

    if force and os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed existing {db_path}")

    print("Migrating from JSON to SQLite...")
    result = db.migrate_from_json(BASE_DIR)

    print(f"  Hypotheses:    {result['hypotheses']}")
    print(f"  Literature:    {result['literature']}")
    print(f"  Known effects: {result['known_effects']}")
    print(f"  Dead ends:     {result['dead_ends']}")
    print(f"  Queue tasks:   {result['queue_tasks']}")
    print(f"  Watchlist:     {result['watchlist']}")

    print("\nVerifying migration...")
    issues = verify_migration()
    if issues:
        print("Verification FAILED:")
        for i in issues:
            print(f"  - {i}")
        sys.exit(1)
    else:
        print("Verification PASSED.")
        print(f"\nDatabase created at: {db_path}")
        print("JSON files are preserved as backup. Safe to delete once confirmed.")


if __name__ == "__main__":
    main()
