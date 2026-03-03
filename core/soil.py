"""
SOIL — Root Zone
================
S — Sense
O — Observe
I — Intake
L — Listen

Inbound signal processing. The first contact layer.
Receives raw input from the environment (files, voice, text, events),
normalizes it, and passes it downstream for interpretation.

Signal lifecycle:
  raw input → normalize() → Signal dict → queue() → soil.db
  soil.db   → drain()     → loam.ingest_full() → knowledge atom

SOIL has no opinions about meaning. It receives, normalizes, passes.
LOAM decides what to store. VINE decides who's involved.

Watchers use polling (not inotify) for WSL compatibility.
Content > 8KB is written to a sidecar file; queue table stays light.

DB: artifacts/{username}/soil.db (caller provides path)

AUTHOR: Shiva (Claude Code) + Sean Campbell
GOVERNANCE: soil-initial-2026-03-03.commit
VERSION: 1.0.0
"""

import hashlib
import json
import logging
import mimetypes
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("soil")

INLINE_LIMIT = 8 * 1024  # 8KB — larger content goes to sidecar

# Extension → category inference
_EXT_CATEGORY: dict[str, str] = {
    ".md":   "narrative",  ".txt": "narrative",  ".rst": "narrative",
    ".pdf":  "document",   ".doc": "document",   ".docx": "document",
    ".py":   "code",       ".js":  "code",       ".ts":   "code",
    ".json": "data",       ".csv": "data",       ".yaml": "data",
    ".yml":  "data",       ".toml": "data",
    ".jpg":  "image",      ".jpeg": "image",     ".png":  "image",
    ".mp3":  "audio",      ".wav": "audio",      ".m4a":  "audio",
    ".mp4":  "video",      ".mov": "video",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_tables(db_path: str) -> None:
    """Create SOIL tables. Idempotent."""
    conn = _connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                signal_id    TEXT PRIMARY KEY,
                signal_type  TEXT NOT NULL,
                source       TEXT NOT NULL,
                content_text TEXT,
                content_path TEXT,
                category     TEXT,
                username     TEXT NOT NULL,
                received_at  TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                error        TEXT,
                metadata     TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sig_user_status ON signals(username, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sig_received   ON signals(received_at)")
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def normalize(source: str, content: str, signal_type: str, username: str,
              category: Optional[str] = None,
              metadata: Optional[dict] = None) -> dict:
    """
    Convert raw input into a Signal dict. Stateless — does not write to DB.

    signal_type: "file" | "text" | "event" | "drop"
    category: inferred from file extension if not provided.
    """
    if not category:
        ext = Path(source).suffix.lower() if source else ""
        category = _EXT_CATEGORY.get(ext, "narrative")

    signal_id = str(uuid.uuid4())

    return {
        "signal_id":   signal_id,
        "signal_type": signal_type,
        "source":      source,
        "content":     content,
        "category":    category,
        "username":    username,
        "received_at": _now(),
        "status":      "pending",
        "metadata":    metadata or {},
    }


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------

def queue(db_path: str, signal: dict) -> str:
    """
    Persist a Signal to the queue. Returns signal_id.
    Content > INLINE_LIMIT is written to a sidecar file beside soil.db.
    """
    content = signal.get("content", "") or ""
    content_text = None
    content_path = None

    if len(content.encode("utf-8", errors="replace")) <= INLINE_LIMIT:
        content_text = content
    else:
        # Write sidecar next to soil.db
        sidecar_dir = Path(db_path).parent / "soil_sidecars"
        sidecar_dir.mkdir(parents=True, exist_ok=True)
        sidecar = sidecar_dir / f"{signal['signal_id']}.txt"
        sidecar.write_text(content, encoding="utf-8", errors="replace")
        content_path = str(sidecar)

    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT OR IGNORE INTO signals
               (signal_id, signal_type, source, content_text, content_path,
                category, username, received_at, status, metadata)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                signal["signal_id"],
                signal["signal_type"],
                signal["source"],
                content_text,
                content_path,
                signal.get("category"),
                signal["username"],
                signal["received_at"],
                "pending",
                json.dumps(signal.get("metadata") or {}),
            )
        )
        conn.commit()
    finally:
        conn.close()

    log.debug(f"SOIL: queued {signal['signal_type']} {signal['source']!r} id={signal['signal_id'][:8]}")
    return signal["signal_id"]


def _read_content(row: sqlite3.Row) -> str:
    """Retrieve content from inline text or sidecar file."""
    if row["content_text"] is not None:
        return row["content_text"]
    if row["content_path"]:
        try:
            return Path(row["content_path"]).read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            log.warning(f"SOIL: sidecar read failed {row['content_path']}: {e}")
    return ""


# ---------------------------------------------------------------------------
# Drain
# ---------------------------------------------------------------------------

def drain(db_path: str, username: str, loam_db_path: str,
          vine_db_path: Optional[str] = None,
          batch_size: int = 10,
          llm_router=None) -> int:
    """
    Process pending signals → loam.ingest_full() for each.
    Updates signal status to 'ingested' or 'failed'.
    Returns count successfully ingested.
    """
    try:
        from core import loam as _loam
    except ImportError:
        log.error("SOIL: drain requires LOAM — not available")
        return 0

    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM signals WHERE username=? AND status='pending' "
            "ORDER BY received_at LIMIT ?",
            (username, batch_size)
        ).fetchall()
        rows = list(rows)
    finally:
        conn.close()

    if not rows:
        return 0

    ingested = 0
    for row in rows:
        signal_id = row["signal_id"]
        content = _read_content(row)
        source_id = hashlib.sha256(
            f"{row['signal_type']}:{row['source']}".encode()
        ).hexdigest()[:40]

        try:
            kid = _loam.ingest_full(
                loam_db_path,
                source_type=row["signal_type"],
                source_id=source_id,
                title=Path(row["source"]).name if row["source"] else "Signal",
                content=content,
                category=row["category"] or "narrative",
                username=username,
                vine_db_path=vine_db_path,
                llm_router=llm_router,
            )
            status = "ingested" if kid > 0 else "failed"
            error = None if kid > 0 else "loam.ingest_full returned -1"
            if kid > 0:
                ingested += 1
        except Exception as e:
            status = "failed"
            error = str(e)[:300]
            log.warning(f"SOIL: drain failed for {signal_id[:8]}: {e}")

        conn = _connect(db_path)
        try:
            conn.execute(
                "UPDATE signals SET status=?, error=? WHERE signal_id=?",
                (status, error, signal_id)
            )
            conn.commit()
        finally:
            conn.close()

    log.info(f"SOIL: drained {ingested}/{len(rows)} signals for {username}")
    return ingested


# ---------------------------------------------------------------------------
# Drop watcher
# ---------------------------------------------------------------------------

def watch_drop(db_path: str, username: str, drop_dir: str,
               extensions: Optional[list] = None) -> int:
    """
    Scan drop_dir for files not yet queued. Queue each as a 'drop' signal.
    Returns count of new files queued.

    Polling-based (not inotify) — WSL compatible.
    extensions: list of lowercase extensions to accept (e.g. ['.md', '.txt']).
                None means accept all non-hidden files.
    """
    drop_path = Path(drop_dir)
    if not drop_path.is_dir():
        log.debug(f"SOIL: watch_drop dir not found: {drop_dir}")
        return 0

    # Load known sources to avoid re-queuing
    conn = _connect(db_path)
    try:
        known = {
            r["source"]
            for r in conn.execute(
                "SELECT source FROM signals WHERE username=? AND signal_type='drop'",
                (username,)
            ).fetchall()
        }
    finally:
        conn.close()

    queued = 0
    for path in sorted(drop_path.iterdir()):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        if extensions and path.suffix.lower() not in extensions:
            continue
        if str(path) in known:
            continue

        try:
            content = _read_file(path)
        except Exception as e:
            log.warning(f"SOIL: could not read drop file {path}: {e}")
            continue

        sig = normalize(
            source=str(path),
            content=content,
            signal_type="drop",
            username=username,
            metadata={"filename": path.name, "size": path.stat().st_size},
        )
        queue(db_path, sig)
        queued += 1
        log.info(f"SOIL: queued drop file {path.name!r}")

    return queued


def _read_file(path: Path) -> str:
    """Read a file as text. Binary files get a placeholder."""
    mime, _ = mimetypes.guess_type(str(path))
    if mime and not (mime.startswith("text") or mime in ("application/json",
                                                          "application/yaml",
                                                          "application/toml")):
        return f"[BINARY: {path.name} ({path.stat().st_size} bytes)]"
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return path.read_bytes().decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------

def pending(db_path: str, username: str, limit: int = 20) -> list:
    """Return pending signals for a user."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT signal_id, signal_type, source, category, received_at "
            "FROM signals WHERE username=? AND status='pending' "
            "ORDER BY received_at LIMIT ?",
            (username, limit)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def stats(db_path: str, username: str) -> dict:
    """Signal counts by status and type."""
    conn = _connect(db_path)
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE username=?", (username,)
        ).fetchone()[0]

        by_status = {}
        for s in ("pending", "ingested", "failed"):
            by_status[s] = conn.execute(
                "SELECT COUNT(*) FROM signals WHERE username=? AND status=?",
                (username, s)
            ).fetchone()[0]

        by_type = {}
        for row in conn.execute(
            "SELECT signal_type, COUNT(*) as cnt FROM signals "
            "WHERE username=? GROUP BY signal_type ORDER BY cnt DESC",
            (username,)
        ).fetchall():
            by_type[row["signal_type"]] = row["cnt"]

        return {"total": total, "by_status": by_status, "by_type": by_type}
    finally:
        conn.close()
