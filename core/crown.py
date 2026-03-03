"""
CROWN — Canopy
==============
C — Compose
R — Release
O — Output
W — Witness
N — Nurture

Output layer. The launch benchmark.
Composes final responses, releases them to the user or SAFE,
outputs structured artifacts, witnesses what was produced (formal record),
and nurtures ongoing relationships (UTETY community layer).

Benchmark status:
  Compose  ✅ — format_response()
  Release  ✅ — release()
  Output   ✅ — artifact() — ingests output into LOAM (closes the loop)
  Witness  ✅ — record() — SHA256 tamper-evident log
  Nurture  ⚠️ — stub — UTETY hooks pending Campus integration

Witness DB: artifacts/{username}/crown_witness.db (caller provides path)

AUTHOR: Shiva (Claude Code) + Sean Campbell
GOVERNANCE: canopy-initial-2026-03-03.commit
VERSION: 1.0.0
"""

import hashlib
import json
import logging
import sqlite3
import textwrap
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("crown")

_FORMATS = ("markdown", "plain", "json")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Witness schema
# ---------------------------------------------------------------------------

def init_witness(db_path: str) -> None:
    """Create witness log table. Idempotent."""
    conn = _connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS witness_log (
                witness_id  TEXT PRIMARY KEY,
                username    TEXT NOT NULL,
                agent       TEXT NOT NULL,
                title       TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                content_len INTEGER NOT NULL,
                produced_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wlog_user ON witness_log(username, produced_at)"
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Compose
# ---------------------------------------------------------------------------

def format_response(content: str, fmt: str = "markdown") -> str:
    """
    Format content for output.
    fmt: "markdown" (default) | "plain" | "json"
    """
    if fmt not in _FORMATS:
        fmt = "markdown"

    if fmt == "plain":
        # Strip markdown formatting
        import re
        text = re.sub(r'\*{1,3}(.+?)\*{1,3}', r'\1', content)
        text = re.sub(r'#{1,6}\s+', '', text)
        text = re.sub(r'`{1,3}[^`]*`{1,3}', lambda m: m.group(0).strip('`'), text)
        text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)
        return text.strip()

    if fmt == "json":
        return json.dumps({"content": content, "produced_at": _now()}, indent=2)

    # markdown — pass through (it's already markdown)
    return content.strip()


# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------

def release(content: str, target: str = "user",
            metadata: Optional[dict] = None) -> dict:
    """
    Release output to a target.

    target: "user" | "safe" | "pigeon"
      user   — formatted for direct display
      safe   — packaged for SAFE federation (with seed_packet stub)
      pigeon — wrapped for Pigeon transport

    Returns release manifest dict.
    """
    if target not in ("user", "safe", "pigeon"):
        target = "user"

    manifest: dict = {
        "target":      target,
        "content_len": len(content),
        "released_at": _now(),
        "metadata":    metadata or {},
    }

    if target == "user":
        manifest["payload"] = format_response(content, "markdown")

    elif target == "safe":
        manifest["payload"] = {
            "content":     content,
            "seed_packet": {"version": "1.4", "status": "PENDING_FEDERATION"},
            "format":      "markdown",
        }
        log.debug("CROWN: released to SAFE — seed_packet stub, federation pending")

    elif target == "pigeon":
        try:
            from core import rings as _rings
            manifest["payload"] = _rings.make_pigeon(
                content={"body": content},
                gate_conditions={"min_trust": "GUEST"},
                sender="crown",
            )
        except ImportError:
            manifest["payload"] = {"body": content, "transport": "pigeon"}

    return manifest


# ---------------------------------------------------------------------------
# Output — closes the loop back into LOAM
# ---------------------------------------------------------------------------

def artifact(content: str, title: str, source_type: str,
             username: str, loam_db_path: str,
             vine_db_path: Optional[str] = None) -> int:
    """
    Ingest this output as a knowledge atom in LOAM.
    Closes the loop: what CROWN produces becomes what LOAM knows.
    Returns knowledge_id or -1 on failure.
    """
    try:
        from core import loam as _loam
    except ImportError:
        log.error("CROWN: artifact() requires LOAM — not available")
        return -1

    try:
        kid = _loam.ingest(
            loam_db_path,
            source_type=source_type,
            source_id=f"crown:{hashlib.sha256(content.encode()).hexdigest()[:16]}",
            title=title,
            content=content,
            category="output",
            username=username,
            vine_db_path=vine_db_path,
        )
        log.debug(f"CROWN: artifact ingested → loam id={kid}")
        return kid
    except Exception as e:
        log.error(f"CROWN: artifact failed: {e}")
        return -1


# ---------------------------------------------------------------------------
# Witness — formal tamper-evident record
# ---------------------------------------------------------------------------

def record(title: str, content: str, agent: str,
           username: str, witness_db_path: str) -> str:
    """
    Write a tamper-evident witness record of what was produced.
    Stores SHA256 hash of content + metadata. Content itself not stored —
    only its hash, length, and identity.

    Returns witness_id (hex string).
    """
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    witness_id = hashlib.sha256(
        f"{username}:{agent}:{title}:{content_hash}:{_now()}".encode()
    ).hexdigest()[:32]

    conn = _connect(witness_db_path)
    try:
        conn.execute(
            """INSERT OR IGNORE INTO witness_log
               (witness_id, username, agent, title, content_hash,
                content_len, produced_at)
               VALUES (?,?,?,?,?,?,?)""",
            (witness_id, username, agent, title, content_hash,
             len(content), _now())
        )
        conn.commit()
        log.debug(f"CROWN: witnessed {title!r} id={witness_id[:12]}")
    finally:
        conn.close()

    return witness_id


def verify_witness(witness_id: str, content: str,
                   witness_db_path: str) -> bool:
    """
    Verify that content matches the witness record.
    Returns True if SHA256 matches stored hash.
    """
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    conn = _connect(witness_db_path)
    try:
        row = conn.execute(
            "SELECT content_hash FROM witness_log WHERE witness_id=?",
            (witness_id,)
        ).fetchone()
        return bool(row and row["content_hash"] == content_hash)
    finally:
        conn.close()


def get_witness_log(witness_db_path: str, username: str,
                    limit: int = 20) -> list:
    """Return recent witness records for a user."""
    conn = _connect(witness_db_path)
    try:
        rows = conn.execute(
            "SELECT witness_id, agent, title, content_hash, content_len, produced_at "
            "FROM witness_log WHERE username=? ORDER BY produced_at DESC LIMIT ?",
            (username, limit)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Nurture — stub (UTETY Campus integration pending)
# ---------------------------------------------------------------------------

def nurture(username: str, community: Optional[str] = None) -> dict:
    """
    Nurture ongoing relationships via UTETY community hooks.
    STATUS: stub — UTETY Campus integration not yet wired.

    When implemented:
      - Surface relevant UTETY community threads
      - Log engagement signals back to LOAM
      - Route to UTETY-aware agents
    """
    log.debug(f"CROWN: nurture called for {username!r} — not yet implemented")
    return {
        "status":    "not_implemented",
        "message":   "Nurture requires UTETY Campus integration (pending)",
        "username":  username,
        "community": community,
    }
