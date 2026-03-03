"""
VINE — Root Zone
================
V — Vector
I — Identify
N — Network
E — Entity

Relationship tracking. The connection layer.
Maps how entities relate to each other — people, concepts, events, places.

Three-layer privacy model:
  L1 — Anonymous: type known, identity unknown. No name stored.
  L2 — Reference: named or aliased, not fully verified. (most personal knowledge)
  L3 — Named: fully identified, verifiable or explicitly consented.

Promotion: L1 → L2 → L3 as context accumulates. Never demoted automatically.
BLACK_BOX: any entity can be flagged to suppress retrieval. Irreversible without human action.

DB: artifacts/{username}/vine.db (caller provides path)

AUTHOR: Shiva (Claude Code) + Sean Campbell
GOVERNANCE: vine-initial-2026-03-03.commit
VERSION: 1.0.0
"""

import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("vine")

# ---------------------------------------------------------------------------
# Known entity seeds (Tier 1 extraction)
# ---------------------------------------------------------------------------

_KNOWN: dict[str, list[str]] = {
    "person":   ["Sean Campbell", "Sean", "Christoph", "Kartikeya"],
    "project":  ["Die-Namic", "UTETY", "SAFE", "ECCR", "Willow", "willow-1.4"],
    "concept":  ["governance", "dual commit", "delta E", "ΔE", "coherence",
                 "three-ring", "source ring", "bridge ring", "continuity ring"],
    "tool":     ["Ollama", "Gemini", "Claude", "llm_router", "Pigeon"],
    "legal":    ["bankruptcy", "Chapter 13", "26-10177-j13"],
}

_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(re.escape(name), re.IGNORECASE), etype)
    for etype, names in _KNOWN.items()
    for name in names
]

_CAP_PHRASE = re.compile(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b')


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
    """Create all VINE tables. Idempotent."""
    conn = _connect(db_path)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS entities (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                layer         INTEGER NOT NULL CHECK(layer IN (1, 2, 3)),
                entity_type   TEXT    NOT NULL,
                name          TEXT,
                confidence    REAL    DEFAULT 0.5,
                mention_count INTEGER DEFAULT 1,
                first_seen    TEXT    NOT NULL,
                last_seen     TEXT    NOT NULL,
                username      TEXT    NOT NULL,
                promoted_from INTEGER REFERENCES entities(id),
                is_blackbox   INTEGER DEFAULT 0,
                notes         TEXT
            );

            CREATE TABLE IF NOT EXISTS entity_aliases (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id INTEGER NOT NULL REFERENCES entities(id),
                alias     TEXT    NOT NULL,
                source    TEXT,
                UNIQUE(entity_id, alias)
            );

            CREATE TABLE IF NOT EXISTS entity_relations (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_a       INTEGER NOT NULL REFERENCES entities(id),
                entity_b       INTEGER NOT NULL REFERENCES entities(id),
                relation_type  TEXT    NOT NULL,
                confidence     REAL    DEFAULT 0.5,
                evidence_count INTEGER DEFAULT 1,
                first_seen     TEXT    NOT NULL,
                last_seen      TEXT    NOT NULL,
                username       TEXT    NOT NULL,
                UNIQUE(entity_a, entity_b, relation_type)
            );

            CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(username, name);
            CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(username, entity_type);
            CREATE INDEX IF NOT EXISTS idx_aliases_alias ON entity_aliases(alias);
            CREATE INDEX IF NOT EXISTS idx_relations_a   ON entity_relations(entity_a);
            CREATE INDEX IF NOT EXISTS idx_relations_b   ON entity_relations(entity_b);
        """)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------

def upsert(db_path: str, name: str, entity_type: str, username: str,
           layer: int = 2, notes: Optional[str] = None) -> int:
    """
    Insert or update an entity. Returns entity_id.
    On conflict (same name+username): increment mention_count, update last_seen.
    """
    now = _now()
    conn = _connect(db_path)
    try:
        existing = conn.execute(
            "SELECT id, mention_count FROM entities WHERE username=? AND name=? COLLATE NOCASE",
            (username, name)
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE entities SET mention_count=?, last_seen=? WHERE id=?",
                (existing["mention_count"] + 1, now, existing["id"])
            )
            if notes:
                conn.execute("UPDATE entities SET notes=? WHERE id=?", (notes, existing["id"]))
            conn.commit()
            return existing["id"]

        cur = conn.execute(
            """INSERT INTO entities
               (layer, entity_type, name, first_seen, last_seen, username, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (layer, entity_type, name, now, now, username, notes)
        )
        conn.commit()
        eid = cur.lastrowid
        log.debug(f"VINE: upserted entity id={eid} name={name!r} layer={layer}")
        return eid
    finally:
        conn.close()


def link(db_path: str, name_a: str, name_b: str, relation_type: str,
         username: str, confidence: float = 0.5) -> int:
    """
    Create or strengthen a relationship between two named entities.
    On conflict: increment evidence_count, raise confidence by 0.05 (cap 1.0).
    Returns relation_id, or -1 if either entity not found.
    """
    now = _now()
    conn = _connect(db_path)
    try:
        def _resolve(name: str) -> Optional[int]:
            row = conn.execute(
                "SELECT id FROM entities WHERE username=? AND name=? COLLATE NOCASE",
                (username, name)
            ).fetchone()
            if row:
                return row["id"]
            row = conn.execute(
                """SELECT e.id FROM entities e
                   JOIN entity_aliases a ON a.entity_id = e.id
                   WHERE e.username=? AND a.alias=? COLLATE NOCASE""",
                (username, name)
            ).fetchone()
            return row["id"] if row else None

        id_a = _resolve(name_a)
        id_b = _resolve(name_b)
        if id_a is None or id_b is None:
            log.warning(f"VINE link: entity not found — {name_a!r} or {name_b!r}")
            return -1

        existing = conn.execute(
            "SELECT id, evidence_count, confidence FROM entity_relations "
            "WHERE entity_a=? AND entity_b=? AND relation_type=?",
            (id_a, id_b, relation_type)
        ).fetchone()

        if existing:
            new_conf = min(1.0, existing["confidence"] + 0.05)
            conn.execute(
                "UPDATE entity_relations SET evidence_count=?, confidence=?, last_seen=? WHERE id=?",
                (existing["evidence_count"] + 1, new_conf, now, existing["id"])
            )
            conn.commit()
            return existing["id"]

        cur = conn.execute(
            """INSERT INTO entity_relations
               (entity_a, entity_b, relation_type, confidence, first_seen, last_seen, username)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (id_a, id_b, relation_type, confidence, now, now, username)
        )
        conn.commit()
        rid = cur.lastrowid
        log.debug(f"VINE: linked {name_a!r} -{relation_type}-> {name_b!r} rel_id={rid}")
        return rid
    finally:
        conn.close()


def get(db_path: str, name: str, username: str) -> Optional[dict]:
    """
    Fetch entity by name or alias. Returns None if not found or is_blackbox=1.
    """
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM entities WHERE username=? AND name=? COLLATE NOCASE",
            (username, name)
        ).fetchone()

        if not row:
            row = conn.execute(
                """SELECT e.* FROM entities e
                   JOIN entity_aliases a ON a.entity_id = e.id
                   WHERE e.username=? AND a.alias=? COLLATE NOCASE""",
                (username, name)
            ).fetchone()

        if not row or row["is_blackbox"]:
            return None

        aliases = [r["alias"] for r in conn.execute(
            "SELECT alias FROM entity_aliases WHERE entity_id=?", (row["id"],)
        ).fetchall()]

        return {
            "id":            row["id"],
            "layer":         row["layer"],
            "entity_type":   row["entity_type"],
            "name":          row["name"],
            "confidence":    row["confidence"],
            "mention_count": row["mention_count"],
            "first_seen":    row["first_seen"],
            "last_seen":     row["last_seen"],
            "notes":         row["notes"],
            "aliases":       aliases,
        }
    finally:
        conn.close()


def related(db_path: str, entity_id: int,
            relation_type: Optional[str] = None,
            min_confidence: float = 0.3) -> list[dict]:
    """
    Get connected entities. Traverses both directions. Skips blackboxed entities.
    Returns list of {entity, relation_type, confidence, evidence_count, direction}.
    """
    conn = _connect(db_path)
    try:
        results = []
        type_clause = "AND r.relation_type=?" if relation_type else ""

        for direction, col_self, col_other in [
            ("outbound", "entity_a", "entity_b"),
            ("inbound",  "entity_b", "entity_a"),
        ]:
            params = [entity_id, min_confidence]
            if relation_type:
                params.append(relation_type)

            rows = conn.execute(
                f"""SELECT e.*, r.relation_type as rel_type,
                           r.confidence as rel_conf, r.evidence_count
                    FROM entity_relations r
                    JOIN entities e ON e.id = r.{col_other}
                    WHERE r.{col_self}=? AND r.confidence>=?
                      AND e.is_blackbox=0 {type_clause}
                    ORDER BY r.confidence DESC""",
                params
            ).fetchall()

            for row in rows:
                aliases = [a["alias"] for a in conn.execute(
                    "SELECT alias FROM entity_aliases WHERE entity_id=?", (row["id"],)
                ).fetchall()]
                results.append({
                    "entity": {
                        "id":          row["id"],
                        "layer":       row["layer"],
                        "entity_type": row["entity_type"],
                        "name":        row["name"],
                        "confidence":  row["confidence"],
                        "aliases":     aliases,
                    },
                    "relation_type":  row["rel_type"],
                    "confidence":     row["rel_conf"],
                    "evidence_count": row["evidence_count"],
                    "direction":      direction,
                })
        return results
    finally:
        conn.close()


def extract(text: str, username: str) -> list[dict]:
    """
    Extract entities from text.
    Tier 1: regex against known patterns.
    Tier 2: heuristic — capitalized multi-word phrases not already caught.
    Returns list of {name, entity_type, layer}.
    """
    found = []
    seen: set[str] = set()

    for pattern, etype in _PATTERNS:
        for m in pattern.finditer(text):
            name = m.group(0)
            if name.lower() not in seen:
                seen.add(name.lower())
                found.append({"name": name, "entity_type": etype, "layer": 2})

    for m in _CAP_PHRASE.finditer(text):
        name = m.group(1)
        if name.lower() not in seen:
            seen.add(name.lower())
            found.append({"name": name, "entity_type": "concept", "layer": 2})

    return found


def promote(db_path: str, entity_id: int, new_layer: int,
            name: Optional[str] = None) -> bool:
    """
    Promote entity to higher layer. name required when promoting from L1 to L2+.
    Returns False if new_layer <= current layer or entity not found.
    """
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT layer, name FROM entities WHERE id=?", (entity_id,)
        ).fetchone()
        if not row:
            return False
        if new_layer <= row["layer"]:
            return False
        if new_layer >= 2 and not row["name"] and not name:
            log.warning(f"VINE promote: name required for L1→L{new_layer}")
            return False

        conn.execute("UPDATE entities SET layer=? WHERE id=?", (new_layer, entity_id))
        if name:
            conn.execute("UPDATE entities SET name=? WHERE id=?", (name, entity_id))
        conn.commit()
        log.info(f"VINE: promoted entity id={entity_id} to L{new_layer}")
        return True
    finally:
        conn.close()


def search(db_path: str, query: str, username: str,
           layer: Optional[int] = None) -> list[dict]:
    """Case-insensitive LIKE search on name and aliases. Respects is_blackbox."""
    conn = _connect(db_path)
    try:
        like = f"%{query}%"
        layer_clause = "AND e.layer=?" if layer else ""
        params = [username, like, like]
        if layer:
            params.append(layer)

        rows = conn.execute(
            f"""SELECT DISTINCT e.* FROM entities e
                LEFT JOIN entity_aliases a ON a.entity_id = e.id
                WHERE e.username=? AND e.is_blackbox=0
                  AND (e.name LIKE ? OR a.alias LIKE ?)
                  {layer_clause}
                ORDER BY e.mention_count DESC""",
            params
        ).fetchall()

        results = []
        for row in rows:
            aliases = [r["alias"] for r in conn.execute(
                "SELECT alias FROM entity_aliases WHERE entity_id=?", (row["id"],)
            ).fetchall()]
            results.append({
                "id":            row["id"],
                "layer":         row["layer"],
                "entity_type":   row["entity_type"],
                "name":          row["name"],
                "confidence":    row["confidence"],
                "mention_count": row["mention_count"],
                "first_seen":    row["first_seen"],
                "last_seen":     row["last_seen"],
                "notes":         row["notes"],
                "aliases":       aliases,
            })
        return results
    finally:
        conn.close()


def blackbox(db_path: str, entity_id: int) -> bool:
    """Flag entity to suppress from retrieval. Irreversible without human action."""
    conn = _connect(db_path)
    try:
        conn.execute("UPDATE entities SET is_blackbox=1 WHERE id=?", (entity_id,))
        conn.commit()
        log.info(f"VINE: blackboxed entity id={entity_id}")
        return True
    finally:
        conn.close()
