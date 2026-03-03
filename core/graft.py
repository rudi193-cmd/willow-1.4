"""
GRAFT — Trunk
=============
G — Govern
R — Route
A — Arbitrate
F — Flow
T — Tasks

Task management. The work queue.
Governs what gets done, routes tasks to the right executor,
arbitrates conflicts by priority, manages flow control via
dependency chains, and tracks every state change in an audit log.

State machine:
  pending → in_progress → completed | failed | cancelled
  (blocked: pending task with unresolved dependencies — skipped by next_task)

Priority: 0–10 (higher = sooner). FIFO within same priority (created_at ASC).

Governance tier stored for auditability — enforcement is the caller's job:
  T1 = human approval required before execution
  T2 = log + auto-approve
  T3 = proceed immediately

DB: artifacts/{username}/graft.db (caller provides path)

AUTHOR: Shiva (Claude Code) + Sean Campbell
GOVERNANCE: graft-initial-2026-03-03.commit
VERSION: 1.0.0
"""

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("graft")

_VALID_STATUSES = {"pending", "in_progress", "completed", "failed", "cancelled"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_tables(db_path: str) -> None:
    """Create all GRAFT tables. Idempotent."""
    conn = _connect(db_path)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id      TEXT PRIMARY KEY,
                username     TEXT NOT NULL,
                subject      TEXT NOT NULL,
                description  TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                agent        TEXT NOT NULL,
                priority     INTEGER NOT NULL DEFAULT 5,
                tier         INTEGER NOT NULL DEFAULT 2,
                created_at   TEXT NOT NULL,
                updated_at   TEXT NOT NULL,
                completed_at TEXT,
                metadata     TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_username ON tasks(username);
            CREATE INDEX IF NOT EXISTS idx_tasks_status   ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_agent    ON tasks(agent);
            CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority DESC, created_at ASC);

            CREATE TABLE IF NOT EXISTS task_deps (
                task_id       TEXT NOT NULL REFERENCES tasks(task_id),
                blocked_by_id TEXT NOT NULL REFERENCES tasks(task_id),
                PRIMARY KEY (task_id, blocked_by_id)
            );

            CREATE TABLE IF NOT EXISTS task_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id   TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                action    TEXT NOT NULL,
                agent     TEXT NOT NULL,
                notes     TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_log_task ON task_log(task_id);
        """)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Task CRUD
# ---------------------------------------------------------------------------

def create(db_path: str, username: str, subject: str, description: str,
           agent: str, priority: int = 5, tier: int = 2,
           metadata: Optional[dict] = None) -> str:
    """
    Create a new task. Returns task_id.
    priority: 0–10, default 5. Higher = processed sooner.
    tier: governance tier (1/2/3), stored for caller to enforce.
    """
    task_id = str(uuid.uuid4())[:12]
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO tasks
               (task_id, username, subject, description, status, agent,
                priority, tier, created_at, updated_at, metadata)
               VALUES (?,?,?,?,'pending',?,?,?,?,?,?)""",
            (task_id, username, subject, description, agent,
             max(0, min(10, priority)), tier, now, now,
             json.dumps(metadata) if metadata else None)
        )
        conn.execute(
            "INSERT INTO task_log (task_id, timestamp, action, agent, notes) "
            "VALUES (?,?,'created',?,?)",
            (task_id, now, agent, subject)
        )
        conn.commit()
        log.debug(f"GRAFT: created task {task_id} {subject!r} agent={agent} pri={priority}")
        return task_id
    finally:
        conn.close()


def get(db_path: str, task_id: str) -> Optional[dict]:
    """Fetch task by ID. Returns None if not found."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if not row:
            return None
        t = dict(row)
        if t.get("metadata"):
            t["metadata"] = json.loads(t["metadata"])
        # Include dependency IDs
        t["blocked_by"] = [
            r["blocked_by_id"] for r in conn.execute(
                "SELECT blocked_by_id FROM task_deps WHERE task_id=?", (task_id,)
            ).fetchall()
        ]
        return t
    finally:
        conn.close()


def list_tasks(db_path: str, username: str,
               agent: Optional[str] = None,
               status: Optional[str] = None,
               limit: int = 50) -> list:
    """List tasks with optional filters. Ordered by priority DESC, created_at ASC."""
    conn = _connect(db_path)
    try:
        clauses = ["username=?"]
        params: list = [username]
        if agent:
            clauses.append("agent=?")
            params.append(agent)
        if status:
            clauses.append("status=?")
            params.append(status)
        params.append(limit)
        rows = conn.execute(
            f"SELECT * FROM tasks WHERE {' AND '.join(clauses)} "
            f"ORDER BY priority DESC, created_at ASC LIMIT ?",
            params
        ).fetchall()
        result = []
        for row in rows:
            t = dict(row)
            if t.get("metadata"):
                t["metadata"] = json.loads(t["metadata"])
            result.append(t)
        return result
    finally:
        conn.close()


def update(db_path: str, task_id: str, status: str,
           agent: str, notes: Optional[str] = None) -> bool:
    """
    Update task status. Returns False if task not found or invalid status.
    Automatically sets completed_at when status is completed/failed/cancelled.
    """
    if status not in _VALID_STATUSES:
        log.warning(f"GRAFT: invalid status {status!r}")
        return False

    now = _now()
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT task_id FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if not row:
            return False

        terminal = status in ("completed", "failed", "cancelled")
        if terminal:
            conn.execute(
                "UPDATE tasks SET status=?, updated_at=?, completed_at=? WHERE task_id=?",
                (status, now, now, task_id)
            )
        else:
            conn.execute(
                "UPDATE tasks SET status=?, updated_at=? WHERE task_id=?",
                (status, now, task_id)
            )
        conn.execute(
            "INSERT INTO task_log (task_id, timestamp, action, agent, notes) "
            "VALUES (?,?,?,?,?)",
            (task_id, now, f"status:{status}", agent, notes)
        )
        conn.commit()
        log.debug(f"GRAFT: task {task_id} → {status} by {agent}")
        return True
    finally:
        conn.close()


def cancel(db_path: str, task_id: str, agent: str,
           reason: Optional[str] = None) -> bool:
    """Cancel a task. Returns False if not found or already terminal."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT status FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if not row:
            return False
        if row["status"] in ("completed", "failed", "cancelled"):
            return False
    finally:
        conn.close()
    return update(db_path, task_id, "cancelled", agent, notes=reason)


def delete(db_path: str, task_id: str) -> bool:
    """Delete a task and its log. Returns False if not found."""
    conn = _connect(db_path)
    try:
        cur = conn.execute("DELETE FROM tasks WHERE task_id=?", (task_id,))
        if cur.rowcount == 0:
            return False
        conn.execute("DELETE FROM task_deps WHERE task_id=? OR blocked_by_id=?",
                     (task_id, task_id))
        conn.execute("DELETE FROM task_log WHERE task_id=?", (task_id,))
        conn.commit()
        return True
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Flow control — worker pop + dependency arbitration
# ---------------------------------------------------------------------------

def next_task(db_path: str, username: str, agent: str) -> Optional[dict]:
    """
    Pop the next pending task for an agent: highest priority, FIFO tiebreak,
    no unresolved dependencies. Marks it in_progress atomically.
    Returns the task dict or None if queue is empty.
    """
    conn = _connect(db_path)
    try:
        # Find highest-priority pending task with all deps completed
        row = conn.execute("""
            SELECT t.task_id FROM tasks t
            WHERE t.username=? AND t.agent=? AND t.status='pending'
              AND NOT EXISTS (
                  SELECT 1 FROM task_deps d
                  JOIN tasks dep ON dep.task_id = d.blocked_by_id
                  WHERE d.task_id = t.task_id
                    AND dep.status NOT IN ('completed', 'cancelled')
              )
            ORDER BY t.priority DESC, t.created_at ASC
            LIMIT 1
        """, (username, agent)).fetchone()

        if not row:
            return None

        task_id = row["task_id"]
        now = _now()
        conn.execute(
            "UPDATE tasks SET status='in_progress', updated_at=? WHERE task_id=?",
            (now, task_id)
        )
        conn.execute(
            "INSERT INTO task_log (task_id, timestamp, action, agent, notes) "
            "VALUES (?,?,'claimed',?,?)",
            (task_id, now, agent, "Worker claimed task")
        )
        conn.commit()
    finally:
        conn.close()

    return get(db_path, task_id)


def add_dependency(db_path: str, task_id: str, blocked_by_id: str) -> None:
    """Block task_id until blocked_by_id is completed/cancelled."""
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO task_deps (task_id, blocked_by_id) VALUES (?,?)",
            (task_id, blocked_by_id)
        )
        conn.commit()
        log.debug(f"GRAFT: {task_id} blocked by {blocked_by_id}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def get_log(db_path: str, task_id: str) -> list:
    """Full audit log for a task, newest first."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT timestamp, action, agent, notes FROM task_log "
            "WHERE task_id=? ORDER BY timestamp DESC",
            (task_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def stats(db_path: str, username: str,
          agent: Optional[str] = None) -> dict:
    """Task counts by status. Optionally filtered by agent."""
    conn = _connect(db_path)
    try:
        clauses = ["username=?"]
        params: list = [username]
        if agent:
            clauses.append("agent=?")
            params.append(agent)
        where = " AND ".join(clauses)

        total = conn.execute(
            f"SELECT COUNT(*) FROM tasks WHERE {where}", params
        ).fetchone()[0]

        by_status: dict = {}
        for s in ("pending", "in_progress", "completed", "failed", "cancelled"):
            by_status[s] = conn.execute(
                f"SELECT COUNT(*) FROM tasks WHERE {where} AND status=?",
                params + [s]
            ).fetchone()[0]

        by_priority: dict = {}
        for row in conn.execute(
            f"SELECT priority, COUNT(*) as cnt FROM tasks WHERE {where} "
            f"GROUP BY priority ORDER BY priority DESC",
            params
        ).fetchall():
            by_priority[row["priority"]] = row["cnt"]

        return {
            "total":       total,
            "by_status":   by_status,
            "by_priority": by_priority,
        }
    finally:
        conn.close()
