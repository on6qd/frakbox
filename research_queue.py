"""
Research queue — gives the researcher direction instead of random exploration.

Three components:
1. Research queue: what categories to investigate next, ranked by priority
2. Event watchlist: specific upcoming events to monitor for hypothesis testing
3. Session priorities: what the PREVIOUS session thinks the NEXT session should do

This is how the researcher develops intentionality across sessions.

All storage is now in SQLite via db.py. This module provides the same public API
so existing callers (the LLM agent, run.sh, etc.) continue to work unchanged.
"""

import db as _db


def load_queue():
    return _db.load_queue()


def save_queue(q):
    _db.save_queue(q)


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

    Returns:
        The created task dict (with its assigned ID), or None if a duplicate was skipped.
    """
    return _db.add_research_task(category, question, priority, reasoning, depends_on=depends_on)


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
    return _db.add_event_to_watchlist(event_description, expected_date, symbol, hypothesis_template)


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
    _db.set_next_session_priorities(priorities, handoff=handoff)


def get_next_research_task():
    """Get the highest-priority pending research task whose dependencies are met."""
    return _db.get_next_research_task()


def get_due_events(today=None):
    """Get watchlist events that are due today or overdue."""
    return _db.get_due_events(today=today)


def complete_research_task(task_id, findings_summary):
    """
    Mark a research task as completed.

    Args:
        task_id: The task ID to complete. Falls back to matching by category
                 if no ID match is found (backward compatibility).
        findings_summary: What was learned.
    """
    return _db.complete_research_task(task_id, findings_summary)


def mark_event_triggered(event_description):
    """Mark a watchlist event as triggered (it happened)."""
    _db.mark_event_triggered(event_description)


def expire_old_events():
    """Mark overdue watchlist events as expired."""
    _db.expire_old_events()
