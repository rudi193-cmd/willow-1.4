"""
PULSE — Trunk
=============
P — Process
U — Unify
L — Loop
S — Schedule
E — Execute

Daemon worker. The heartbeat.
Runs the background processing loop — picks up tasks from graft,
executes them, loops back, schedules recurring work.
30-second poll. 3-failure backoff. Archives stale tasks on startup.

tick() is the testable unit. The daemon loop calls tick() every poll_interval.
Handlers dict: {handler_name: callable(username, metadata) → dict}.

Built-in handlers are registered automatically if their modules import:
  soil_drain               → soil.drain()
  loam_backfill_summaries  → loam.backfill_summaries()
  loam_backfill_embeddings → loam.backfill_embeddings()

DB: artifacts/{username}/pulse.db (caller provides path)

AUTHOR: Shiva (Claude Code) + Sean Campbell
GOVERNANCE: pulse-initial-2026-03-03.commit
VERSION: 1.0.0
"""

import json
import logging
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional

log = logging.getLogger("pulse")

# Module-level daemon state
_stop_event: Optional[threading.Event] = None
_daemon_thread: Optional[threading.Thread] = None
_session_stats: dict = {"tasks_processed": 0, "schedules_ran": 0, "errors": 0,
                         "started_at": None}

# Per-handler failure tracking for backoff
_failure_counts: dict[str, int] = {}
_backoff_until: dict[str, float] = {}
_BACKOFF_BASE = 30.0
_BACKOFF_MAX = 300.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts() -> float:
    return time.monotonic()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_tables(db_path: str) -> None:
    """Create all PULSE tables. Idempotent."""
    conn = _connect(db_path)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS schedules (
                schedule_id          TEXT PRIMARY KEY,
                username             TEXT NOT NULL,
                name                 TEXT NOT NULL,
                handler_name         TEXT NOT NULL,
                interval_seconds     INTEGER NOT NULL,
                last_run             TEXT,
                next_run             TEXT NOT NULL,
                enabled              INTEGER NOT NULL DEFAULT 1,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                metadata             TEXT,
                UNIQUE(username, name)
            );

            CREATE INDEX IF NOT EXISTS idx_sched_user    ON schedules(username, enabled);
            CREATE INDEX IF NOT EXISTS idx_sched_nextrun ON schedules(next_run);

            CREATE TABLE IF NOT EXISTS pulse_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                username         TEXT NOT NULL,
                tick_at          TEXT NOT NULL,
                tasks_processed  INTEGER NOT NULL DEFAULT 0,
                schedules_ran    INTEGER NOT NULL DEFAULT 0,
                errors           INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_plog_user ON pulse_log(username, tick_at);
        """)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Built-in handlers
# ---------------------------------------------------------------------------

def _builtin_handlers(username: str) -> dict:
    """Register built-in handlers for available modules."""
    handlers: dict[str, Callable] = {}

    try:
        from core import soil as _soil
        def _soil_drain(u, meta):
            graft_db = meta.get("graft_db_path", "")
            loam_db = meta.get("loam_db_path", "")
            vine_db = meta.get("vine_db_path")
            n = _soil.drain(meta.get("soil_db_path", ""), u, loam_db,
                            vine_db_path=vine_db, batch_size=10)
            return {"success": True, "result": f"drained {n} signals"}
        handlers["soil_drain"] = _soil_drain
    except ImportError:
        pass

    try:
        from core import loam as _loam
        def _backfill_summaries(u, meta):
            n = _loam.backfill_summaries(meta.get("loam_db_path", ""), u,
                                         batch_size=5)
            return {"success": True, "result": f"backfilled {n} summaries"}
        handlers["loam_backfill_summaries"] = _backfill_summaries

        def _backfill_embeddings(u, meta):
            n = _loam.backfill_embeddings(meta.get("loam_db_path", ""), u,
                                           batch_size=20)
            return {"success": True, "result": f"backfilled {n} embeddings"}
        handlers["loam_backfill_embeddings"] = _backfill_embeddings
    except ImportError:
        pass

    return handlers


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def add_schedule(db_path: str, username: str, name: str,
                 handler_name: str, interval_seconds: int,
                 metadata: Optional[dict] = None) -> str:
    """
    Register a recurring job. Idempotent on (username, name) — updates
    handler and interval if already exists. Returns schedule_id.
    """
    schedule_id = str(uuid.uuid4())[:12]
    now = _now()
    next_run = (datetime.now(timezone.utc)
                + timedelta(seconds=interval_seconds)).isoformat()

    conn = _connect(db_path)
    try:
        existing = conn.execute(
            "SELECT schedule_id FROM schedules WHERE username=? AND name=?",
            (username, name)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE schedules SET handler_name=?, interval_seconds=?, "
                "next_run=?, metadata=? WHERE schedule_id=?",
                (handler_name, interval_seconds, next_run,
                 json.dumps(metadata) if metadata else None,
                 existing["schedule_id"])
            )
            conn.commit()
            return existing["schedule_id"]

        conn.execute(
            """INSERT INTO schedules
               (schedule_id, username, name, handler_name, interval_seconds,
                next_run, metadata)
               VALUES (?,?,?,?,?,?,?)""",
            (schedule_id, username, name, handler_name, interval_seconds,
             next_run, json.dumps(metadata) if metadata else None)
        )
        conn.commit()
        log.debug(f"PULSE: scheduled {name!r} every {interval_seconds}s")
        return schedule_id
    finally:
        conn.close()


def list_schedules(db_path: str, username: str) -> list:
    """Return all schedules for a user."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM schedules WHERE username=? ORDER BY name",
            (username,)
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("metadata"):
                d["metadata"] = json.loads(d["metadata"])
            result.append(d)
        return result
    finally:
        conn.close()


def remove_schedule(db_path: str, schedule_id: str) -> bool:
    """Remove a schedule. Returns False if not found."""
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "DELETE FROM schedules WHERE schedule_id=?", (schedule_id,)
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def due_schedules(db_path: str, username: str) -> list:
    """Return enabled schedules where next_run <= now."""
    now = _now()
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM schedules WHERE username=? AND enabled=1 "
            "AND next_run <= ? ORDER BY next_run",
            (username, now)
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("metadata"):
                d["metadata"] = json.loads(d["metadata"])
            result.append(d)
        return result
    finally:
        conn.close()


def _mark_ran(db_path: str, schedule_id: str, success: bool) -> None:
    """Update last_run, compute next_run, track failures."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT interval_seconds, consecutive_failures FROM schedules "
            "WHERE schedule_id=?", (schedule_id,)
        ).fetchone()
        if not row:
            return
        now_dt = datetime.now(timezone.utc)
        last_run = now_dt.isoformat()
        failures = 0 if success else (row["consecutive_failures"] + 1)
        # Backoff: double interval after 3 failures, cap at BACKOFF_MAX
        if failures >= 3:
            delay = min(_BACKOFF_MAX,
                        row["interval_seconds"] * (2 ** (failures - 2)))
        else:
            delay = row["interval_seconds"]
        next_run = (now_dt + timedelta(seconds=delay)).isoformat()
        conn.execute(
            "UPDATE schedules SET last_run=?, next_run=?, consecutive_failures=? "
            "WHERE schedule_id=?",
            (last_run, next_run, failures, schedule_id)
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tick — the testable unit
# ---------------------------------------------------------------------------

def tick(graft_db_path: str, pulse_db_path: str, username: str,
         handlers: Optional[dict] = None, max_tasks: int = 5) -> dict:
    """
    Single processing cycle:
      1. Run due schedules via handlers.
      2. Pop up to max_tasks pending tasks from GRAFT and execute them.

    handlers: {handler_name: callable(username, metadata) → dict}
    Returns {tasks_processed, schedules_ran, errors}.
    """
    from core import graft as _graft

    all_handlers = _builtin_handlers(username)
    if handlers:
        all_handlers.update(handlers)

    tasks_done = 0
    scheds_done = 0
    error_count = 0
    tick_at = _now()

    # 1. Run due schedules
    for sched in due_schedules(pulse_db_path, username):
        handler_name = sched["handler_name"]
        sid = sched["schedule_id"]

        # Backoff check
        if _backoff_until.get(sid, 0) > time.monotonic():
            log.debug(f"PULSE: {sched['name']!r} in backoff, skipping")
            continue

        handler = all_handlers.get(handler_name)
        if not handler:
            log.warning(f"PULSE: no handler for {handler_name!r}")
            _mark_ran(pulse_db_path, sid, success=False)
            error_count += 1
            continue

        try:
            meta = sched.get("metadata") or {}
            result = handler(username, meta)
            success = result.get("success", True) if isinstance(result, dict) else True
            _mark_ran(pulse_db_path, sid, success=success)
            if success:
                _failure_counts[sid] = 0
                _backoff_until.pop(sid, None)
                scheds_done += 1
                log.debug(f"PULSE: schedule {sched['name']!r} ran — "
                          f"{result.get('result', '') if isinstance(result, dict) else ''}")
            else:
                _failure_counts[sid] = _failure_counts.get(sid, 0) + 1
                if _failure_counts[sid] >= 3:
                    delay = min(_BACKOFF_MAX,
                                _BACKOFF_BASE * (2 ** (_failure_counts[sid] - 2)))
                    _backoff_until[sid] = time.monotonic() + delay
                    log.warning(f"PULSE: {sched['name']!r} failed "
                                f"{_failure_counts[sid]}x — backoff {delay:.0f}s")
                error_count += 1
        except Exception as e:
            _failure_counts[sid] = _failure_counts.get(sid, 0) + 1
            _mark_ran(pulse_db_path, sid, success=False)
            log.error(f"PULSE: schedule {sched['name']!r} raised: {e}")
            error_count += 1

    # 2. Process GRAFT tasks
    seen_agents: set = set()
    for _ in range(max_tasks):
        # Find any agent with a pending task
        try:
            from core import graft as _graft
            pending = _graft.list_tasks(graft_db_path, username,
                                        status="pending", limit=20)
            if not pending:
                break

            # Try each agent until we pop one
            popped = None
            for t in pending:
                agent = t["agent"]
                if agent in seen_agents:
                    continue
                task = _graft.next_task(graft_db_path, username, agent)
                if task:
                    popped = task
                    seen_agents.add(agent)
                    break

            if not popped:
                break

            handler_name = popped.get("metadata", {}).get("handler") if popped.get("metadata") else None
            if not handler_name:
                handler_name = popped["agent"]

            handler = all_handlers.get(handler_name)
            if not handler:
                _graft.update(graft_db_path, popped["task_id"], "failed",
                              "pulse", notes=f"No handler for {handler_name!r}")
                error_count += 1
                continue

            try:
                meta = popped.get("metadata") or {}
                result = handler(username, meta)
                success = result.get("success", True) if isinstance(result, dict) else True
                final_status = "completed" if success else "failed"
                notes = result.get("result", "") if isinstance(result, dict) else str(result)
                _graft.update(graft_db_path, popped["task_id"], final_status,
                              "pulse", notes=notes[:200])
                tasks_done += 1 if success else 0
                if not success:
                    error_count += 1
            except Exception as e:
                _graft.update(graft_db_path, popped["task_id"], "failed",
                              "pulse", notes=str(e)[:200])
                log.error(f"PULSE: task {popped['task_id']} raised: {e}")
                error_count += 1

        except Exception as e:
            log.error(f"PULSE: tick task loop error: {e}")
            error_count += 1
            break

    # Log tick
    conn = _connect(pulse_db_path)
    try:
        conn.execute(
            "INSERT INTO pulse_log (username, tick_at, tasks_processed, "
            "schedules_ran, errors) VALUES (?,?,?,?,?)",
            (username, tick_at, tasks_done, scheds_done, error_count)
        )
        conn.commit()
    finally:
        conn.close()

    _session_stats["tasks_processed"] += tasks_done
    _session_stats["schedules_ran"] = _session_stats.get("schedules_ran", 0) + scheds_done
    _session_stats["errors"] = _session_stats.get("errors", 0) + error_count

    return {
        "tasks_processed": tasks_done,
        "schedules_ran":   scheds_done,
        "errors":          error_count,
        "tick_at":         tick_at,
    }


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------

def start(graft_db_path: str, pulse_db_path: str, username: str,
          handlers: Optional[dict] = None,
          poll_interval: float = 30.0) -> threading.Thread:
    """
    Start the PULSE daemon in a background thread.
    Archives stale tasks on startup. Calls tick() every poll_interval seconds.
    Returns the Thread (daemon=True, stops when main process exits).
    """
    global _stop_event, _daemon_thread, _session_stats

    if _daemon_thread and _daemon_thread.is_alive():
        log.warning("PULSE: daemon already running")
        return _daemon_thread

    archive_stale(graft_db_path, username)

    _stop_event = threading.Event()
    _session_stats = {"tasks_processed": 0, "schedules_ran": 0, "errors": 0,
                      "started_at": _now()}

    def _loop():
        log.info(f"PULSE: daemon started — poll={poll_interval}s user={username}")
        while not _stop_event.is_set():
            try:
                result = tick(graft_db_path, pulse_db_path, username, handlers)
                if result["tasks_processed"] or result["schedules_ran"]:
                    log.debug(f"PULSE: tick — tasks={result['tasks_processed']} "
                              f"scheds={result['schedules_ran']} err={result['errors']}")
            except Exception as e:
                log.error(f"PULSE: daemon tick error: {e}")
            _stop_event.wait(timeout=poll_interval)
        log.info("PULSE: daemon stopped")

    _daemon_thread = threading.Thread(target=_loop, daemon=True, name="pulse")
    _daemon_thread.start()
    return _daemon_thread


def stop() -> None:
    """Signal the daemon loop to stop."""
    global _stop_event
    if _stop_event:
        _stop_event.set()
        log.info("PULSE: stop signalled")


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------

def archive_stale(graft_db_path: str, username: str,
                  older_than_days: int = 7) -> int:
    """
    Move terminal tasks (completed/failed/cancelled) older than N days
    to graft_archive table. Keeps graft.db lean.
    Returns count archived.
    """
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=older_than_days)).isoformat()

    conn = sqlite3.connect(graft_db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        # Ensure archive table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS graft_archive (
                task_id TEXT PRIMARY KEY,
                username TEXT, subject TEXT, description TEXT,
                status TEXT, agent TEXT, priority INTEGER, tier INTEGER,
                created_at TEXT, updated_at TEXT, completed_at TEXT,
                metadata TEXT, archived_at TEXT
            )
        """)

        rows = conn.execute(
            """SELECT * FROM tasks WHERE username=?
               AND status IN ('completed','failed','cancelled')
               AND updated_at < ?""",
            (username, cutoff)
        ).fetchall()

        if not rows:
            conn.commit()
            return 0

        archived_at = _now()
        conn.executemany(
            """INSERT OR IGNORE INTO graft_archive
               (task_id, username, subject, description, status, agent,
                priority, tier, created_at, updated_at, completed_at,
                metadata, archived_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(r["task_id"], r["username"], r["subject"], r["description"],
              r["status"], r["agent"], r["priority"], r["tier"],
              r["created_at"], r["updated_at"], r["completed_at"],
              r["metadata"], archived_at)
             for r in rows]
        )
        ids = tuple(r["task_id"] for r in rows)
        conn.execute(
            f"DELETE FROM tasks WHERE task_id IN ({','.join('?'*len(ids))})",
            ids
        )
        conn.commit()
        log.info(f"PULSE: archived {len(rows)} stale tasks for {username}")
        return len(rows)
    finally:
        conn.close()


def health(pulse_db_path: str, username: str) -> dict:
    """Return daemon health: last tick, session totals, schedule count."""
    conn = _connect(pulse_db_path)
    try:
        last = conn.execute(
            "SELECT tick_at, tasks_processed, schedules_ran, errors "
            "FROM pulse_log WHERE username=? ORDER BY tick_at DESC LIMIT 1",
            (username,)
        ).fetchone()
        sched_count = conn.execute(
            "SELECT COUNT(*) FROM schedules WHERE username=? AND enabled=1",
            (username,)
        ).fetchone()[0]
    finally:
        conn.close()

    alive = _daemon_thread is not None and _daemon_thread.is_alive()
    started = _session_stats.get("started_at")
    uptime = 0.0
    if started and alive:
        try:
            dt = datetime.fromisoformat(started.rstrip("Z"))
            uptime = (datetime.now(timezone.utc) - dt.replace(tzinfo=timezone.utc)).total_seconds()
        except Exception:
            pass

    return {
        "alive":            alive,
        "uptime_seconds":   round(uptime),
        "last_tick":        dict(last) if last else None,
        "session_totals":   {k: v for k, v in _session_stats.items()
                             if k != "started_at"},
        "schedule_count":   sched_count,
    }
