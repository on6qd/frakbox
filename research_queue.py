"""
Research queue — gives the researcher direction instead of random exploration.

Three components:
1. Research queue: what categories to investigate next, ranked by priority
2. Event watchlist: specific upcoming events to monitor for hypothesis testing
3. Session priorities: what the PREVIOUS session thinks the NEXT session should do

This is how the researcher develops intentionality across sessions.
"""

import json
import os
import tempfile
import uuid
from datetime import datetime


QUEUE_FILE = os.path.join(os.path.dirname(__file__), "research_queue.json")


def load_queue():
    if not os.path.exists(QUEUE_FILE):
        return {"queue": [], "event_watchlist": [], "next_session_priorities": []}
    with open(QUEUE_FILE) as f:
        q = json.load(f)
    # Migrate: add IDs to any tasks missing them
    changed = False
    for task in q.get("queue", []):
        if "id" not in task:
            task["id"] = uuid.uuid4().hex[:8]
            changed = True
    if changed:
        save_queue(q)
    return q


def save_queue(q):
    dir_name = os.path.dirname(QUEUE_FILE)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(q, f, indent=2)
        os.replace(tmp_path, QUEUE_FILE)
    except Exception:
        os.unlink(tmp_path)
        raise


def add_research_task(category, question, priority, reasoning, depends_on=None):
    """
    Add a research task to the queue.

    Args:
        category: Event type to research
        question: The specific question to answer (not just "research X" but
                  "does the effect persist beyond 5 days?" or
                  "is the effect stronger for small-cap vs large-cap?")
        priority: 1-5 (1 = highest)
        reasoning: Why this is worth investigating now
        depends_on: Optional task ID that must be completed before this task starts.
                    Use this for chained research questions (e.g., "does PEAD exist?"
                    must be completed before "is PEAD stronger for small-cap?").

    Returns:
        The created task dict (with its assigned ID), or None if a duplicate was skipped.
    """
    q = load_queue()

    # Deduplication: skip if a pending task with the same category and question exists
    for existing in q["queue"]:
        if (existing.get("status") == "pending"
                and existing["category"] == category
                and existing["question"] == question):
            return None

    task = {
        "id": uuid.uuid4().hex[:8],
        "category": category,
        "question": question,
        "priority": priority,
        "status": "pending",
        "reasoning": reasoning,
        "added": datetime.now().isoformat(),
    }
    if depends_on:
        task["depends_on"] = depends_on

    q["queue"].append(task)
    q["queue"].sort(key=lambda x: x["priority"])
    save_queue(q)
    return task


def add_event_to_watchlist(event_description, expected_date, symbol, hypothesis_template):
    """
    Add an upcoming event to watch for. When this event occurs,
    the researcher should immediately test the hypothesis.

    Args:
        event_description: "FOMC rate decision", "AAPL Q2 earnings", etc.
        expected_date: "YYYY-MM-DD" when the event should occur
        symbol: Stock/ETF to trade when event occurs
        hypothesis_template: Pre-formed hypothesis to activate when event triggers
    """
    q = load_queue()

    # Deduplication: skip if same event/date/symbol already on watchlist
    for existing in q["event_watchlist"]:
        if (existing.get("status") == "watching"
                and existing["event"] == event_description
                and existing["expected_date"] == expected_date
                and existing["symbol"] == symbol):
            return None

    entry = {
        "event": event_description,
        "expected_date": expected_date,
        "symbol": symbol,
        "hypothesis_template": hypothesis_template,
        "added": datetime.now().isoformat(),
        "status": "watching",  # watching, triggered, expired
    }
    q["event_watchlist"].append(entry)
    save_queue(q)
    return entry


def set_next_session_priorities(priorities, handoff=None):
    """
    Set what the next session should focus on.
    Called at the end of each session based on what was learned.

    Args:
        priorities: List of strings describing what to do next and why
        handoff: Optional dict with structured session hand-off context:
            {
                "attempted": "what this session tried to do",
                "accomplished": "what was actually completed",
                "partial_results": "any intermediate findings not yet in knowledge_base",
                "blocked_by": "what prevented further progress (if anything)",
                "key_insight": "the most important thing the next session should know",
            }
    """
    q = load_queue()
    q["next_session_priorities"] = [{
        "task": p,
        "set_by_session": datetime.now().isoformat(),
    } for p in priorities]
    if handoff:
        q["session_handoff"] = {
            **handoff,
            "written_at": datetime.now().isoformat(),
        }
    save_queue(q)


def get_next_research_task():
    """Get the highest-priority pending research task whose dependencies are met."""
    q = load_queue()
    completed_ids = {t["id"] for t in q["queue"] if t.get("status") == "completed"}
    for task in q["queue"]:
        if task["status"] != "pending":
            continue
        dep = task.get("depends_on")
        if dep and dep not in completed_ids:
            continue  # Dependency not yet completed — skip
        return task
    return None


def get_due_events(today=None):
    """Get watchlist events that are due today or overdue."""
    if today is None:
        today = datetime.now().strftime("%Y-%m-%d")
    q = load_queue()
    return [e for e in q["event_watchlist"]
            if e["status"] == "watching" and e["expected_date"] <= today]


def complete_research_task(task_id, findings_summary):
    """
    Mark a research task as completed.

    Args:
        task_id: The task ID to complete. Falls back to matching by category
                 if no ID match is found (backward compatibility).
        findings_summary: What was learned.
    """
    q = load_queue()
    # Try matching by ID first
    matched = False
    for task in q["queue"]:
        if task.get("id") == task_id and task["status"] in ("pending", "in_progress"):
            task["status"] = "completed"
            task["completed"] = datetime.now().isoformat()
            task["findings"] = findings_summary
            matched = True
            break

    # Fallback: match by category (backward compatibility for tasks without IDs)
    if not matched:
        for task in q["queue"]:
            if task["category"] == task_id and task["status"] in ("pending", "in_progress"):
                task["status"] = "completed"
                task["completed"] = datetime.now().isoformat()
                task["findings"] = findings_summary
                matched = True
                break

    if matched:
        save_queue(q)
    return matched


def mark_event_triggered(event_description):
    """Mark a watchlist event as triggered (it happened)."""
    q = load_queue()
    for event in q["event_watchlist"]:
        if event["event"] == event_description and event["status"] == "watching":
            event["status"] = "triggered"
            event["triggered_date"] = datetime.now().isoformat()
            break
    save_queue(q)


def expire_old_events():
    """Mark overdue watchlist events as expired."""
    today = datetime.now().strftime("%Y-%m-%d")
    q = load_queue()
    changed = False
    for event in q["event_watchlist"]:
        if event["status"] == "watching" and event["expected_date"] < today:
            # Give 2 days grace for date estimates being slightly off
            expected = datetime.strptime(event["expected_date"], "%Y-%m-%d")
            if (datetime.now() - expected).days > 2:
                event["status"] = "expired"
                changed = True
    if changed:
        save_queue(q)
