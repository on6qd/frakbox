"""
Database layer — SQLite storage replacing JSON files.

Provides CRUD functions for all research data:
- hypotheses (from hypotheses.json)
- known_effects, dead_ends, literature (from knowledge_base.json)
- research_queue, event_watchlist, session_priorities, session_handoff (from research_queue.json)

All functions operate on a singleton connection per process with WAL mode
for safe concurrent access from research, trade_loop, and health_check processes.
"""

import json
import os
import sqlite3
import threading
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "research.db")

_local = threading.local()


def get_db():
    """Return a singleton SQLite connection for the current thread."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, timeout=10)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
        _local.conn.execute("PRAGMA busy_timeout=5000")
    return _local.conn


def close_db():
    """Close the thread-local connection if open."""
    if hasattr(_local, "conn") and _local.conn is not None:
        _local.conn.close()
        _local.conn = None


def init_db():
    """Create all tables if they don't exist."""
    conn = get_db()
    conn.executescript(_SCHEMA)
    conn.commit()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS hypotheses (
    id TEXT PRIMARY KEY,
    created TEXT NOT NULL,
    prediction_hash TEXT,
    idempotency_key TEXT,
    status TEXT NOT NULL DEFAULT 'pending',

    -- The thesis
    event_type TEXT NOT NULL,
    event_description TEXT,
    causal_mechanism TEXT,
    causal_mechanism_criteria TEXT,  -- JSON list
    expected_symbol TEXT,
    expected_direction TEXT,
    expected_magnitude_pct REAL,
    expected_timeframe_days INTEGER,
    event_timing TEXT DEFAULT 'unknown',

    -- Multi-symbol backtest evidence
    backtest_symbols TEXT,  -- JSON list
    backtest_events TEXT,   -- JSON list of dicts

    -- Research backing
    historical_evidence TEXT,  -- JSON list
    sample_size INTEGER,
    consistency_pct REAL,
    out_of_sample_split TEXT,  -- JSON dict
    confounders TEXT,          -- JSON dict
    market_regime_note TEXT,
    regime_note TEXT,
    confidence INTEGER,
    literature_reference TEXT,
    survivorship_bias_note TEXT,
    selection_bias_note TEXT,
    passes_multiple_testing INTEGER,  -- boolean
    multiple_testing_warning TEXT,

    -- Warnings
    confounder_warnings TEXT,
    symbol_warnings TEXT,   -- JSON list
    dead_end_warnings TEXT, -- JSON list

    -- Trade fields
    trade TEXT,   -- JSON dict (entry_price, position_size, deadline, stop_loss, etc.)
    result TEXT,  -- JSON dict (exit_price, return_pct, post_mortem, etc.)

    -- Trigger fields (for trade_loop.py)
    trigger TEXT,
    trigger_position_size REAL,
    trigger_stop_loss_pct REAL,
    trigger_take_profit_pct REAL,

    -- Extra data (any additional fields as JSON)
    extra TEXT   -- JSON dict for fields like live_validation_march_2026, etc.
);

CREATE INDEX IF NOT EXISTS idx_hypotheses_status ON hypotheses(status);
CREATE INDEX IF NOT EXISTS idx_hypotheses_idempotency ON hypotheses(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_hypotheses_event_type ON hypotheses(event_type);

CREATE TABLE IF NOT EXISTS known_effects (
    event_type TEXT PRIMARY KEY,
    data TEXT NOT NULL,  -- JSON dict with all effect fields
    last_updated TEXT
);

CREATE TABLE IF NOT EXISTS dead_ends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL UNIQUE,
    reason TEXT NOT NULL,
    recorded TEXT,
    updated TEXT
);

CREATE TABLE IF NOT EXISTS literature (
    event_type TEXT PRIMARY KEY,
    data TEXT NOT NULL,  -- JSON dict with all literature fields
    recorded TEXT
);

CREATE TABLE IF NOT EXISTS research_queue (
    id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    question TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 3,
    status TEXT NOT NULL DEFAULT 'pending',
    reasoning TEXT,
    added TEXT,
    completed TEXT,
    findings TEXT,
    depends_on TEXT,
    implementation_notes TEXT,
    extra TEXT  -- JSON dict for any additional fields
);

CREATE INDEX IF NOT EXISTS idx_research_queue_status ON research_queue(status);

CREATE TABLE IF NOT EXISTS event_watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT NOT NULL,
    expected_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    hypothesis_template TEXT,  -- JSON
    added TEXT,
    status TEXT NOT NULL DEFAULT 'watching',
    triggered_date TEXT,
    UNIQUE(event, expected_date, symbol)
);

CREATE TABLE IF NOT EXISTS session_priorities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task TEXT NOT NULL,
    set_by_session TEXT
);

CREATE TABLE IF NOT EXISTS session_handoff (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- singleton row
    data TEXT NOT NULL,  -- JSON dict
    written_at TEXT
);
"""

# ---------------------------------------------------------------------------
# Hypothesis CRUD
# ---------------------------------------------------------------------------

# Fields stored as JSON in the hypotheses table
_HYPOTHESIS_JSON_FIELDS = {
    "causal_mechanism_criteria", "backtest_symbols", "backtest_events",
    "historical_evidence", "out_of_sample_split", "confounders",
    "symbol_warnings", "dead_end_warnings", "trade", "result",
}

# Known top-level column names in hypotheses table
_HYPOTHESIS_COLUMNS = {
    "id", "created", "prediction_hash", "idempotency_key", "status",
    "event_type", "event_description", "causal_mechanism",
    "causal_mechanism_criteria", "expected_symbol", "expected_direction",
    "expected_magnitude_pct", "expected_timeframe_days", "event_timing",
    "backtest_symbols", "backtest_events", "historical_evidence",
    "sample_size", "consistency_pct", "out_of_sample_split", "confounders",
    "market_regime_note", "regime_note", "confidence", "literature_reference",
    "survivorship_bias_note", "selection_bias_note", "passes_multiple_testing",
    "multiple_testing_warning", "confounder_warnings", "symbol_warnings",
    "dead_end_warnings", "trade", "result",
    "trigger", "trigger_position_size", "trigger_stop_loss_pct",
    "trigger_take_profit_pct", "extra",
}


def _hypothesis_to_dict(row):
    """Convert a sqlite3.Row to a hypothesis dict, deserializing JSON fields."""
    d = dict(row)
    # Try to deserialize any string that looks like JSON (starts with [ or {)
    for field in list(d.keys()):
        val = d.get(field)
        if isinstance(val, str) and val and val[0] in ("{", "["):
            try:
                d[field] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass
    # Merge extra fields back into the top-level dict
    extra = d.pop("extra", None)
    if extra:
        try:
            extra_dict = json.loads(extra) if isinstance(extra, str) else extra
            if isinstance(extra_dict, dict):
                d.update(extra_dict)
        except (json.JSONDecodeError, TypeError):
            pass
    # Convert passes_multiple_testing from int to bool
    if d.get("passes_multiple_testing") is not None:
        d["passes_multiple_testing"] = bool(d["passes_multiple_testing"])
    return d


def _hypothesis_from_dict(d):
    """Convert a hypothesis dict to column values for INSERT/UPDATE."""
    row = {}
    extra = {}
    for key, val in d.items():
        if key in _HYPOTHESIS_COLUMNS:
            if key == "passes_multiple_testing" and val is not None:
                row[key] = 1 if val else 0
            elif isinstance(val, (list, dict)):
                # Always serialize complex types to JSON
                row[key] = json.dumps(val)
            else:
                row[key] = val
        else:
            # Store unknown fields in extra
            extra[key] = val
    if extra:
        row["extra"] = json.dumps(extra)
    return row


def load_hypotheses():
    """Load all hypotheses as a list of dicts (same format as old JSON file)."""
    conn = get_db()
    init_db()
    rows = conn.execute("SELECT * FROM hypotheses ORDER BY created").fetchall()
    return [_hypothesis_to_dict(row) for row in rows]


def save_hypotheses(hypotheses):
    """Replace all hypotheses (bulk save, used for backward compatibility)."""
    conn = get_db()
    init_db()
    conn.execute("DELETE FROM hypotheses")
    for h in hypotheses:
        _upsert_hypothesis(h, conn)
    conn.commit()


def save_hypothesis(hypothesis):
    """Upsert a single hypothesis."""
    conn = get_db()
    init_db()
    _upsert_hypothesis(hypothesis, conn)
    conn.commit()


def _upsert_hypothesis(h, conn):
    """Insert or replace a hypothesis row."""
    row = _hypothesis_from_dict(h)
    columns = list(row.keys())
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(columns)
    conn.execute(
        f"INSERT OR REPLACE INTO hypotheses ({col_names}) VALUES ({placeholders})",
        [row[c] for c in columns],
    )


def get_hypotheses_by_status(status):
    """Load hypotheses filtered by status."""
    conn = get_db()
    init_db()
    rows = conn.execute(
        "SELECT * FROM hypotheses WHERE status = ? ORDER BY created", (status,)
    ).fetchall()
    return [_hypothesis_to_dict(row) for row in rows]


def get_hypothesis_by_id(hypothesis_id):
    """Load a single hypothesis by ID."""
    conn = get_db()
    init_db()
    row = conn.execute(
        "SELECT * FROM hypotheses WHERE id = ?", (hypothesis_id,)
    ).fetchone()
    return _hypothesis_to_dict(row) if row else None


def find_hypothesis_by_idempotency_key(key):
    """Find a hypothesis by idempotency key (for duplicate detection)."""
    conn = get_db()
    init_db()
    row = conn.execute(
        "SELECT * FROM hypotheses WHERE idempotency_key = ?", (key,)
    ).fetchone()
    return _hypothesis_to_dict(row) if row else None


def update_hypothesis_fields(hypothesis_id, **fields):
    """Update specific fields on a hypothesis."""
    conn = get_db()
    # Load existing to merge extra fields correctly
    existing = get_hypothesis_by_id(hypothesis_id)
    if not existing:
        raise ValueError(f"Hypothesis {hypothesis_id} not found")
    existing.update(fields)
    _upsert_hypothesis(existing, conn)
    conn.commit()


def count_hypotheses_by_status(status):
    """Count hypotheses with a given status (without loading full data)."""
    conn = get_db()
    init_db()
    row = conn.execute(
        "SELECT COUNT(*) FROM hypotheses WHERE status = ?", (status,)
    ).fetchone()
    return row[0]


# ---------------------------------------------------------------------------
# Knowledge Base CRUD
# ---------------------------------------------------------------------------

def load_knowledge():
    """Load the full knowledge base as a dict (backward-compatible format)."""
    conn = get_db()
    init_db()
    kb = {"literature": {}, "known_effects": {}, "dead_ends": []}

    for row in conn.execute("SELECT * FROM literature").fetchall():
        data = json.loads(row["data"])
        data["recorded"] = row["recorded"]
        kb["literature"][row["event_type"]] = data

    for row in conn.execute("SELECT * FROM known_effects").fetchall():
        data = json.loads(row["data"])
        data["last_updated"] = row["last_updated"]
        kb["known_effects"][row["event_type"]] = data

    for row in conn.execute("SELECT * FROM dead_ends ORDER BY id").fetchall():
        entry = {
            "event_type": row["event_type"],
            "reason": row["reason"],
            "recorded": row["recorded"],
        }
        if row["updated"]:
            entry["updated"] = row["updated"]
        kb["dead_ends"].append(entry)

    return kb


def save_knowledge(kb):
    """Save the full knowledge base (bulk replacement, backward compatibility)."""
    conn = get_db()
    init_db()

    conn.execute("DELETE FROM literature")
    for event_type, data in kb.get("literature", {}).items():
        recorded = data.pop("recorded", datetime.now().isoformat())
        conn.execute(
            "INSERT INTO literature (event_type, data, recorded) VALUES (?, ?, ?)",
            (event_type, json.dumps(data), recorded),
        )
        data["recorded"] = recorded  # restore the dict

    conn.execute("DELETE FROM known_effects")
    for event_type, data in kb.get("known_effects", {}).items():
        last_updated = data.pop("last_updated", datetime.now().isoformat())
        conn.execute(
            "INSERT INTO known_effects (event_type, data, last_updated) VALUES (?, ?, ?)",
            (event_type, json.dumps(data), last_updated),
        )
        data["last_updated"] = last_updated

    conn.execute("DELETE FROM dead_ends")
    for de in kb.get("dead_ends", []):
        conn.execute(
            "INSERT INTO dead_ends (event_type, reason, recorded, updated) VALUES (?, ?, ?, ?)",
            (de["event_type"], de["reason"], de.get("recorded"), de.get("updated")),
        )

    conn.commit()


def record_literature(event_type, findings):
    """Store literature review findings for an event type."""
    conn = get_db()
    init_db()
    recorded = datetime.now().isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO literature (event_type, data, recorded) VALUES (?, ?, ?)",
        (event_type, json.dumps(findings), recorded),
    )
    conn.commit()


def record_known_effect(event_type, effect):
    """Record a validated causal effect."""
    conn = get_db()
    init_db()
    last_updated = datetime.now().isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO known_effects (event_type, data, last_updated) VALUES (?, ?, ?)",
        (event_type, json.dumps(effect), last_updated),
    )
    conn.commit()


def record_dead_end(event_type, reason):
    """Record a research direction that didn't pan out."""
    conn = get_db()
    init_db()
    # Check if already exists — update if so
    existing = conn.execute(
        "SELECT id FROM dead_ends WHERE event_type = ?", (event_type,)
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE dead_ends SET reason = ?, updated = ? WHERE event_type = ?",
            (reason, datetime.now().isoformat(), event_type),
        )
    else:
        conn.execute(
            "INSERT INTO dead_ends (event_type, reason, recorded) VALUES (?, ?, ?)",
            (event_type, reason, datetime.now().isoformat()),
        )
    conn.commit()


def get_dead_ends():
    """Get all dead ends."""
    conn = get_db()
    init_db()
    rows = conn.execute("SELECT * FROM dead_ends ORDER BY id").fetchall()
    return [dict(row) for row in rows]


def get_known_effect(event_type):
    """Get a single known effect by event type."""
    conn = get_db()
    init_db()
    row = conn.execute(
        "SELECT * FROM known_effects WHERE event_type = ?", (event_type,)
    ).fetchone()
    if row:
        data = json.loads(row["data"])
        data["last_updated"] = row["last_updated"]
        return data
    return None


# ---------------------------------------------------------------------------
# Research Queue CRUD
# ---------------------------------------------------------------------------

def load_queue():
    """Load the full research queue state (backward-compatible dict format)."""
    conn = get_db()
    init_db()
    q = {"queue": [], "event_watchlist": [], "next_session_priorities": []}

    for row in conn.execute("SELECT * FROM research_queue ORDER BY priority, added").fetchall():
        task = {
            "id": row["id"],
            "category": row["category"],
            "question": row["question"],
            "priority": row["priority"],
            "status": row["status"],
            "reasoning": row["reasoning"],
            "added": row["added"],
        }
        if row["completed"]:
            task["completed"] = row["completed"]
        if row["findings"]:
            task["findings"] = row["findings"]
        if row["depends_on"]:
            task["depends_on"] = row["depends_on"]
        if row["implementation_notes"]:
            task["implementation_notes"] = row["implementation_notes"]
        if row["extra"]:
            try:
                task.update(json.loads(row["extra"]))
            except (json.JSONDecodeError, TypeError):
                pass
        q["queue"].append(task)

    for row in conn.execute("SELECT * FROM event_watchlist ORDER BY id").fetchall():
        entry = {
            "event": row["event"],
            "expected_date": row["expected_date"],
            "symbol": row["symbol"],
            "added": row["added"],
            "status": row["status"],
        }
        if row["hypothesis_template"]:
            try:
                entry["hypothesis_template"] = json.loads(row["hypothesis_template"])
            except (json.JSONDecodeError, TypeError):
                entry["hypothesis_template"] = row["hypothesis_template"]
        if row["triggered_date"]:
            entry["triggered_date"] = row["triggered_date"]
        q["event_watchlist"].append(entry)

    for row in conn.execute("SELECT * FROM session_priorities ORDER BY id").fetchall():
        q["next_session_priorities"].append({
            "task": row["task"],
            "set_by_session": row["set_by_session"],
        })

    handoff_row = conn.execute(
        "SELECT * FROM session_handoff WHERE id = 1"
    ).fetchone()
    if handoff_row:
        try:
            q["session_handoff"] = json.loads(handoff_row["data"])
            q["session_handoff"]["written_at"] = handoff_row["written_at"]
        except (json.JSONDecodeError, TypeError):
            pass

    return q


def save_queue(q):
    """Save the full research queue state (bulk replacement, backward compatibility)."""
    conn = get_db()
    init_db()

    conn.execute("DELETE FROM research_queue")
    for task in q.get("queue", []):
        _insert_research_task(task, conn)

    conn.execute("DELETE FROM event_watchlist")
    for entry in q.get("event_watchlist", []):
        _insert_watchlist_entry(entry, conn)

    conn.execute("DELETE FROM session_priorities")
    for p in q.get("next_session_priorities", []):
        if isinstance(p, dict):
            conn.execute(
                "INSERT INTO session_priorities (task, set_by_session) VALUES (?, ?)",
                (p.get("task", str(p)), p.get("set_by_session")),
            )
        else:
            conn.execute(
                "INSERT INTO session_priorities (task) VALUES (?)", (str(p),)
            )

    handoff = q.get("session_handoff")
    if handoff:
        written_at = handoff.pop("written_at", datetime.now().isoformat())
        conn.execute(
            "INSERT OR REPLACE INTO session_handoff (id, data, written_at) VALUES (1, ?, ?)",
            (json.dumps(handoff), written_at),
        )
        handoff["written_at"] = written_at

    conn.commit()


def _insert_research_task(task, conn):
    """Insert a single research task row."""
    known_cols = {"id", "category", "question", "priority", "status",
                  "reasoning", "added", "completed", "findings", "depends_on",
                  "implementation_notes"}
    extra = {k: v for k, v in task.items() if k not in known_cols}
    conn.execute(
        """INSERT OR REPLACE INTO research_queue
           (id, category, question, priority, status, reasoning, added,
            completed, findings, depends_on, implementation_notes, extra)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            task.get("id"), task["category"], task["question"],
            task.get("priority", 3), task.get("status", "pending"),
            task.get("reasoning"), task.get("added"),
            task.get("completed"), task.get("findings"),
            task.get("depends_on"), task.get("implementation_notes"),
            json.dumps(extra) if extra else None,
        ),
    )


def _insert_watchlist_entry(entry, conn):
    """Insert a single event watchlist entry."""
    template = entry.get("hypothesis_template")
    if template is not None and not isinstance(template, str):
        template = json.dumps(template)
    conn.execute(
        """INSERT OR IGNORE INTO event_watchlist
           (event, expected_date, symbol, hypothesis_template, added, status, triggered_date)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            entry["event"], entry["expected_date"], entry["symbol"],
            template, entry.get("added"),
            entry.get("status", "watching"), entry.get("triggered_date"),
        ),
    )


def add_research_task(category, question, priority, reasoning, depends_on=None):
    """Add a research task. Returns task dict or None if duplicate."""
    conn = get_db()
    init_db()
    import uuid

    # Deduplication
    existing = conn.execute(
        "SELECT id FROM research_queue WHERE status = 'pending' AND category = ? AND question = ?",
        (category, question),
    ).fetchone()
    if existing:
        return None

    task_id = uuid.uuid4().hex[:8]
    task = {
        "id": task_id,
        "category": category,
        "question": question,
        "priority": priority,
        "status": "pending",
        "reasoning": reasoning,
        "added": datetime.now().isoformat(),
    }
    if depends_on:
        task["depends_on"] = depends_on

    _insert_research_task(task, conn)
    conn.commit()
    return task


def complete_research_task(task_id, findings_summary):
    """Mark a research task as completed. Returns True if found and updated."""
    conn = get_db()
    init_db()
    now = datetime.now().isoformat()

    # Try by ID first
    result = conn.execute(
        "UPDATE research_queue SET status = 'completed', completed = ?, findings = ? "
        "WHERE id = ? AND status IN ('pending', 'in_progress')",
        (now, findings_summary, task_id),
    )
    if result.rowcount > 0:
        conn.commit()
        return True

    # Fallback: match by category
    result = conn.execute(
        "UPDATE research_queue SET status = 'completed', completed = ?, findings = ? "
        "WHERE category = ? AND status IN ('pending', 'in_progress') "
        "ORDER BY priority LIMIT 1",
        (now, findings_summary, task_id),
    )
    if result.rowcount > 0:
        conn.commit()
        return True

    return False


def add_event_to_watchlist(event_description, expected_date, symbol, hypothesis_template):
    """Add an event to the watchlist. Returns entry dict or None if duplicate."""
    conn = get_db()
    init_db()

    template = hypothesis_template
    if template is not None and not isinstance(template, str):
        template = json.dumps(template)

    try:
        conn.execute(
            """INSERT INTO event_watchlist
               (event, expected_date, symbol, hypothesis_template, added, status)
               VALUES (?, ?, ?, ?, ?, 'watching')""",
            (event_description, expected_date, symbol, template,
             datetime.now().isoformat()),
        )
        conn.commit()
        return {
            "event": event_description,
            "expected_date": expected_date,
            "symbol": symbol,
            "hypothesis_template": hypothesis_template,
            "added": datetime.now().isoformat(),
            "status": "watching",
        }
    except sqlite3.IntegrityError:
        return None  # Duplicate


def set_next_session_priorities(priorities, handoff=None):
    """Set what the next session should focus on."""
    conn = get_db()
    init_db()
    now = datetime.now().isoformat()

    conn.execute("DELETE FROM session_priorities")
    for p in priorities:
        conn.execute(
            "INSERT INTO session_priorities (task, set_by_session) VALUES (?, ?)",
            (p, now),
        )

    if handoff:
        conn.execute(
            "INSERT OR REPLACE INTO session_handoff (id, data, written_at) VALUES (1, ?, ?)",
            (json.dumps(handoff), now),
        )

    conn.commit()


def get_next_research_task():
    """Get the highest-priority pending research task whose dependencies are met."""
    conn = get_db()
    init_db()
    completed_ids_rows = conn.execute(
        "SELECT id FROM research_queue WHERE status = 'completed'"
    ).fetchall()
    completed_ids = {row["id"] for row in completed_ids_rows}

    tasks = conn.execute(
        "SELECT * FROM research_queue WHERE status = 'pending' ORDER BY priority, added"
    ).fetchall()

    for task in tasks:
        dep = task["depends_on"]
        if dep and dep not in completed_ids:
            continue
        return dict(task)
    return None


def get_due_events(today=None):
    """Get watchlist events that are due today or overdue."""
    if today is None:
        today = datetime.now().strftime("%Y-%m-%d")
    conn = get_db()
    init_db()
    rows = conn.execute(
        "SELECT * FROM event_watchlist WHERE status = 'watching' AND expected_date <= ?",
        (today,),
    ).fetchall()
    return [dict(row) for row in rows]


def mark_event_triggered(event_description):
    """Mark a watchlist event as triggered."""
    conn = get_db()
    init_db()
    conn.execute(
        "UPDATE event_watchlist SET status = 'triggered', triggered_date = ? "
        "WHERE event = ? AND status = 'watching'",
        (datetime.now().isoformat(), event_description),
    )
    conn.commit()


def expire_old_events():
    """Mark overdue watchlist events as expired (2-day grace period)."""
    conn = get_db()
    init_db()
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    conn.execute(
        "UPDATE event_watchlist SET status = 'expired' "
        "WHERE status = 'watching' AND expected_date < ?",
        (cutoff,),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Migration from JSON files
# ---------------------------------------------------------------------------

def migrate_from_json(base_dir=None):
    """
    One-time migration from JSON files to SQLite.

    Reads hypotheses.json, knowledge_base.json, and research_queue.json,
    then inserts all data into the SQLite database. Idempotent — safe to
    run multiple times (uses INSERT OR REPLACE).

    Returns a summary dict.
    """
    if base_dir is None:
        base_dir = os.path.dirname(__file__)

    init_db()
    summary = {"hypotheses": 0, "literature": 0, "known_effects": 0,
               "dead_ends": 0, "queue_tasks": 0, "watchlist": 0}

    # 1. Hypotheses
    hyp_path = os.path.join(base_dir, "hypotheses.json")
    if os.path.exists(hyp_path):
        with open(hyp_path) as f:
            hypotheses = json.load(f)
        save_hypotheses(hypotheses)
        summary["hypotheses"] = len(hypotheses)

    # 2. Knowledge base
    kb_path = os.path.join(base_dir, "knowledge_base.json")
    if os.path.exists(kb_path):
        with open(kb_path) as f:
            kb = json.load(f)
        save_knowledge(kb)
        summary["literature"] = len(kb.get("literature", {}))
        summary["known_effects"] = len(kb.get("known_effects", {}))
        summary["dead_ends"] = len(kb.get("dead_ends", []))

    # 3. Research queue
    rq_path = os.path.join(base_dir, "research_queue.json")
    if os.path.exists(rq_path):
        with open(rq_path) as f:
            rq = json.load(f)
        save_queue(rq)
        summary["queue_tasks"] = len(rq.get("queue", []))
        summary["watchlist"] = len(rq.get("event_watchlist", []))

    return summary
