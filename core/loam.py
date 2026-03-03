"""
LOAM — Root Zone
================
L — Ledger
O — Organic
A — Archive
M — Memory

Knowledge storage. The soil memory layer.
Stores what Willow knows: knowledge atoms, conversation memory,
ring topology, and gaps (the loss function).

Entity management lives in VINE. LOAM references vine entities via
a bridge table (knowledge_entities). vine_db_path is optional —
LOAM degrades gracefully without it.

Ring topology (Möbius):
  source     — governance, charter, seed, architecture
  continuity — handoff, summary, memory, journal
  bridge     — everything else

Gap table is the loss function: every zero-result query is recorded.
Old knowledge compacts (summaries backfilled), new knowledge roots.

DB: artifacts/{username}/loam.db (caller provides path)

AUTHOR: Shiva (Claude Code) + Sean Campbell
GOVERNANCE: loam-initial-2026-03-03.commit
VERSION: 1.0.0
"""

import json
import logging
import re
import sqlite3
import struct
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("loam")

# ---------------------------------------------------------------------------
# Ring classification
# ---------------------------------------------------------------------------

_SOURCE_CATS = {"governance", "charter", "hard_stop", "seed", "architecture"}
_CONTINUITY_CATS = {"handoff", "summary", "memory", "journal"}
_SOURCE_TITLE_KW = ("GOVERNANCE", "CHARTER", "HARD_STOP", "SEED_PACKET", "SEED")
_CONTINUITY_TITLE_KW = ("HANDOFF", "JOURNAL", "ENTRY_", "SUMMARY")


def ring(category: str, source_type: str, title: str,
         ring_override: Optional[str] = None) -> str:
    """
    Resolve ring position for a knowledge atom.
    Human ring_override takes precedence (Aios Addendum §4).
    """
    if ring_override:
        return ring_override
    if source_type == "conversation":
        return "continuity"
    cat = (category or "").lower()
    if cat in _SOURCE_CATS:
        return "source"
    t = (title or "").upper()
    if any(kw in t for kw in _SOURCE_TITLE_KW):
        return "source"
    if cat in _CONTINUITY_CATS:
        return "continuity"
    if any(kw in t for kw in _CONTINUITY_TITLE_KW):
        return "continuity"
    return "bridge"


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_tables(db_path: str) -> None:
    """Create all LOAM tables. Idempotent."""
    conn = _connect(db_path)
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS knowledge (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type     TEXT    NOT NULL,
                source_id       TEXT    NOT NULL,
                title           TEXT    NOT NULL,
                summary         TEXT,
                content_snippet TEXT,
                category        TEXT,
                ring            TEXT    NOT NULL DEFAULT 'bridge',
                ring_override   TEXT,
                embedding       BLOB,
                username        TEXT    NOT NULL,
                created_at      TEXT    NOT NULL,
                UNIQUE(source_type, source_id)
            )
        """)

        # FTS5 — porter+unicode61, synced via triggers
        fts_exists = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='knowledge_fts'"
        ).fetchone()
        if not fts_exists:
            cur.execute("""
                CREATE VIRTUAL TABLE knowledge_fts USING fts5(
                    title, summary, content_snippet, category,
                    content='knowledge', content_rowid='id',
                    tokenize='porter unicode61'
                )
            """)
            cur.execute("""
                CREATE TRIGGER knowledge_ai AFTER INSERT ON knowledge BEGIN
                    INSERT INTO knowledge_fts(rowid, title, summary, content_snippet, category)
                    VALUES (new.id, new.title, new.summary, new.content_snippet, new.category);
                END
            """)
            cur.execute("""
                CREATE TRIGGER knowledge_au AFTER UPDATE ON knowledge BEGIN
                    INSERT INTO knowledge_fts(knowledge_fts, rowid, title, summary, content_snippet, category)
                    VALUES ('delete', old.id, old.title, old.summary, old.content_snippet, old.category);
                    INSERT INTO knowledge_fts(rowid, title, summary, content_snippet, category)
                    VALUES (new.id, new.title, new.summary, new.content_snippet, new.category);
                END
            """)
            cur.execute("""
                CREATE TRIGGER knowledge_ad AFTER DELETE ON knowledge BEGIN
                    INSERT INTO knowledge_fts(knowledge_fts, rowid, title, summary, content_snippet, category)
                    VALUES ('delete', old.id, old.title, old.summary, old.content_snippet, old.category);
                END
            """)

        # Bridge to VINE entity IDs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_entities (
                knowledge_id INTEGER NOT NULL REFERENCES knowledge(id),
                entity_id    INTEGER NOT NULL,
                PRIMARY KEY (knowledge_id, entity_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS conversation_memory (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                knowledge_id       INTEGER NOT NULL REFERENCES knowledge(id),
                persona            TEXT,
                user_input         TEXT,
                assistant_response TEXT,
                coherence_index    REAL    DEFAULT 0.0,
                delta_e            REAL    DEFAULT 0.0,
                topics             TEXT,
                created_at         TEXT    NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_gaps (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                query                    TEXT    NOT NULL,
                source                   TEXT    NOT NULL,
                gap_type                 TEXT    NOT NULL,
                entity_name              TEXT,
                username                 TEXT    NOT NULL,
                times_hit                INTEGER DEFAULT 1,
                first_seen               TEXT    NOT NULL,
                last_seen                TEXT    NOT NULL,
                resolved                 INTEGER DEFAULT 0,
                resolved_by_knowledge_id INTEGER,
                UNIQUE(query, source, username)
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_k_username  ON knowledge(username)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_k_ring      ON knowledge(ring)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_k_category  ON knowledge(category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_k_created   ON knowledge(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ke_kid      ON knowledge_entities(knowledge_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gaps_user   ON knowledge_gaps(username, resolved)")

        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def ingest(db_path: str, source_type: str, source_id: str,
           title: str, content: str, category: str, username: str,
           vine_db_path: Optional[str] = None,
           ring_override: Optional[str] = None) -> int:
    """
    Ingest a knowledge atom. Idempotent on (source_type, source_id).
    Returns knowledge_id (existing or new). Returns -1 on error.

    All fleet calls (summary, entity extraction, embedding) must be done
    BEFORE calling this function. Pass results via keyword args if needed.
    This function is a fast DB write only — no slow I/O inside.

    For convenience, summary and embedding can be passed directly:
      ingest(..., _summary="...", _embedding=b"...")
    """
    return _ingest_raw(
        db_path, source_type, source_id, title, content, category,
        username, vine_db_path, ring_override,
        summary=None, embedding=None,
    )


def ingest_full(db_path: str, source_type: str, source_id: str,
                title: str, content: str, category: str, username: str,
                vine_db_path: Optional[str] = None,
                ring_override: Optional[str] = None,
                llm_router=None) -> int:
    """
    High-level ingest with fleet summary + entity extraction + embedding.
    All slow I/O happens BEFORE the DB write.
    Returns knowledge_id or -1 on error.
    """
    snippet = content[:1000]

    # --- Fleet: summary ---
    summary = None
    if llm_router:
        try:
            prompt = (
                f"Summarize this document in 2-3 sentences. "
                f"Focus on what it IS and what it's about.\n\n"
                f"Title: {title}\nCategory: {category}\n\n"
                f"Content:\n{snippet}\n\nSummary:"
            )
            resp = llm_router.ask(prompt, preferred_tier="free")
            if resp and resp.content:
                summary = resp.content.strip()[:500]
        except Exception as e:
            log.debug(f"LOAM: summary failed for {title!r}: {e}")

    # --- Fleet: entity extraction (LLM tier) ---
    llm_entities = []
    if llm_router:
        try:
            prompt = (
                "Extract named entities from this text. Return ONLY a JSON array "
                "of objects with 'name' and 'type' fields. "
                "Types: person, project, concept, tool, organization.\n"
                "If no entities found, return [].\n\n"
                f"Text: {snippet[:1500]}\n\nJSON:"
            )
            resp = llm_router.ask(prompt, preferred_tier="free")
            if resp and resp.content:
                raw = resp.content.strip()
                if raw.startswith("```"):
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                    raw = raw.strip()
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    llm_entities = [
                        e for e in parsed
                        if isinstance(e, dict) and "name" in e and "type" in e
                    ]
        except Exception as e:
            log.debug(f"LOAM: LLM entity extraction failed: {e}")

    # --- Embedding ---
    embedding = None
    try:
        from core import embeddings as _emb
        if _emb.is_available():
            embedding = _emb.embed(f"{title} {snippet}"[:512])
    except Exception:
        pass

    kid = _ingest_raw(
        db_path, source_type, source_id, title, content, category,
        username, vine_db_path, ring_override,
        summary=summary, embedding=embedding,
    )

    # --- Link entities to VINE ---
    if kid > 0 and vine_db_path and llm_entities:
        try:
            from core import vine as _vine
            conn = _connect(db_path)
            try:
                for ent in llm_entities:
                    eid = _vine.upsert(vine_db_path, ent["name"], ent["type"], username)
                    conn.execute(
                        "INSERT OR IGNORE INTO knowledge_entities (knowledge_id, entity_id) VALUES (?,?)",
                        (kid, eid)
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            log.debug(f"LOAM: VINE entity link failed: {e}")

    return kid


def ingest_conversation(db_path: str, username: str, persona: str,
                        user_input: str, assistant_response: str,
                        coherence_index: float = 0.0, delta_e: float = 0.0,
                        vine_db_path: Optional[str] = None) -> int:
    """
    Ingest a conversation turn. Returns knowledge_id.
    Creates both a knowledge atom and a conversation_memory row.
    """
    now = _now()
    source_id = f"conv_{now.replace(':', '').replace('-', '').replace('+', '').replace('.', '')}"
    title = user_input[:60].strip() + ("..." if len(user_input) > 60 else "")
    snippet = f"User: {user_input[:400]}\n{persona}: {assistant_response[:600]}"
    topics = _extract_topics(user_input)

    kid = _ingest_raw(
        db_path, "conversation", source_id, title, snippet, "conversation",
        username, vine_db_path, ring_override=None,
        summary=None, embedding=None,
    )

    if kid > 0:
        conn = _connect(db_path)
        try:
            conn.execute(
                """INSERT INTO conversation_memory
                   (knowledge_id, persona, user_input, assistant_response,
                    coherence_index, delta_e, topics, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (kid, persona, user_input, assistant_response,
                 coherence_index, delta_e, json.dumps(topics), now)
            )
            conn.commit()
        finally:
            conn.close()

    return kid


def _ingest_raw(db_path: str, source_type: str, source_id: str,
                title: str, content: str, category: str, username: str,
                vine_db_path: Optional[str], ring_override: Optional[str],
                summary: Optional[str], embedding: Optional[bytes]) -> int:
    """Internal: fast DB write. No fleet calls inside."""
    snippet = content[:1000]
    r = ring(category, source_type, title, ring_override)
    now = _now()

    conn = _connect(db_path)
    try:
        existing = conn.execute(
            "SELECT id FROM knowledge WHERE source_type=? AND source_id=?",
            (source_type, source_id)
        ).fetchone()
        if existing:
            return existing["id"]

        cur = conn.execute(
            """INSERT OR IGNORE INTO knowledge
               (source_type, source_id, title, summary, content_snippet,
                category, ring, ring_override, embedding, username, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (source_type, source_id, title, summary, snippet,
             category, r, ring_override, embedding, username, now)
        )
        conn.commit()
        kid = cur.lastrowid
        log.debug(f"LOAM: ingested {source_type}/{source_id!r} id={kid} ring={r}")
        return kid or -1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------

def get(db_path: str, knowledge_id: int) -> Optional[dict]:
    """Fetch a single knowledge atom by ID."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM knowledge WHERE id=?", (knowledge_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def search(db_path: str, query: str, username: str,
           max_results: int = 10) -> list:
    """
    FTS5 BM25-ranked search. Falls back to LIKE on parse error.
    Returns list of dicts with id, title, summary, snippet, category, ring, rank.
    Records a gap if zero results.
    """
    conn = _connect(db_path)
    try:
        fts_query = re.sub(r'[^\w\s]', '', query).strip()
        if not fts_query:
            return []

        fts_expr = " OR ".join(fts_query.split())
        try:
            rows = conn.execute(
                """SELECT k.id, k.source_type, k.title, k.summary,
                          k.content_snippet, k.category, k.ring, k.created_at,
                          rank
                   FROM knowledge_fts
                   JOIN knowledge k ON k.id = knowledge_fts.rowid
                   WHERE knowledge_fts MATCH ? AND k.username=?
                   ORDER BY rank
                   LIMIT ?""",
                (fts_expr, username, max_results)
            ).fetchall()
        except sqlite3.OperationalError:
            rows = conn.execute(
                """SELECT id, source_type, title, summary, content_snippet,
                          category, ring, created_at, 0 as rank
                   FROM knowledge
                   WHERE username=?
                     AND (title LIKE ? OR summary LIKE ? OR content_snippet LIKE ?)
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (username, f"%{query}%", f"%{query}%", f"%{query}%", max_results)
            ).fetchall()

        results = [_row_to_dict(conn, row) for row in rows]
    finally:
        conn.close()

    if not results:
        record_gap(db_path, username, query, "search", "zero_results")

    return results


def semantic_search(db_path: str, username: str, query: str,
                    max_results: int = 5) -> list:
    """
    Cosine similarity search over embedding blobs.
    Falls back to FTS5 if embeddings unavailable.
    """
    try:
        from core import embeddings as _emb
        if not _emb.is_available():
            return search(db_path, query, username, max_results)
    except ImportError:
        return search(db_path, query, username, max_results)

    query_vec = _emb.embed(query)
    if not query_vec:
        return search(db_path, query, username, max_results)

    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, source_type, title, summary, content_snippet, "
            "category, ring, created_at, embedding "
            "FROM knowledge WHERE username=? AND embedding IS NOT NULL",
            (username,)
        ).fetchall()

        scored = []
        for row in rows:
            sim = _cosine(query_vec, row["embedding"])
            scored.append((sim, row))
        scored.sort(key=lambda x: x[0], reverse=True)

        results = []
        for sim, row in scored[:max_results]:
            d = _row_to_dict(conn, row)
            d["similarity"] = round(sim, 4)
            d["rank"] = -sim
            results.append(d)
        return results
    finally:
        conn.close()


def build_context(db_path: str, username: str, query: str,
                  max_chars: int = 3000) -> str:
    """
    Build a formatted knowledge context block for prompt injection.
    Combines FTS5 results, semantic fallback, and recent conversation memory.
    Returns empty string if nothing found.
    """
    parts = []
    total = 0

    results = search(db_path, query, username, max_results=5)
    if len(results) < 2:
        sem = semantic_search(db_path, username, query, max_results=5)
        if sem:
            results = sem

    if results:
        parts.append("## RETRIEVED CONTEXT")
        for r in results:
            if total >= max_chars:
                break
            lines = [f"\n**{r['title']}** ({r['source_type']}, {r['category']})"]
            if r.get("summary"):
                lines.append(f"Summary: {r['summary']}")
            elif r.get("content_snippet"):
                lines.append(r["content_snippet"][:300])
            entry = "\n".join(lines)
            parts.append(entry)
            total += len(entry)

    if total < max_chars:
        conn = _connect(db_path)
        try:
            convos = conn.execute(
                """SELECT persona, user_input, assistant_response, created_at
                   FROM conversation_memory cm
                   JOIN knowledge k ON k.id = cm.knowledge_id
                   WHERE k.username=?
                   ORDER BY cm.created_at DESC LIMIT 3""",
                (username,)
            ).fetchall()
            if convos:
                parts.append("\n## RECENT CONVERSATIONS")
                for c in convos:
                    if total >= max_chars:
                        break
                    entry = (
                        f"\n[{c['created_at']}] {c['persona']}\n"
                        f"User: {c['user_input'][:150]}\n"
                        f"Response: {c['assistant_response'][:150]}"
                    )
                    parts.append(entry)
                    total += len(entry)
        except Exception:
            pass
        finally:
            conn.close()

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Gap detection (loss function)
# ---------------------------------------------------------------------------

def record_gap(db_path: str, username: str, query: str, source: str,
               gap_type: str, entity_name: Optional[str] = None) -> None:
    """Record something the system doesn't know. Idempotent — increments times_hit."""
    norm = query.strip().lower()[:200]
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO knowledge_gaps
               (query, source, gap_type, entity_name, username, first_seen, last_seen)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(query, source, username) DO UPDATE SET
                   times_hit = times_hit + 1,
                   last_seen = ?""",
            (norm, source, gap_type, entity_name, username, now, now, now)
        )
        conn.commit()
    except Exception as e:
        log.debug(f"LOAM: record_gap failed: {e}")
    finally:
        conn.close()


def get_top_gaps(db_path: str, username: str, limit: int = 10) -> list:
    """Return most frequently hit unresolved gaps."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT query, source, gap_type, entity_name, times_hit,
                      first_seen, last_seen
               FROM knowledge_gaps
               WHERE username=? AND resolved=0
               ORDER BY times_hit DESC LIMIT ?""",
            (username, limit)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def resolve_gap(db_path: str, username: str, query: str, source: str,
                knowledge_id: int) -> None:
    """Mark gap resolved when the knowledge is later ingested."""
    norm = query.strip().lower()[:200]
    conn = _connect(db_path)
    try:
        conn.execute(
            """UPDATE knowledge_gaps SET resolved=1, resolved_by_knowledge_id=?
               WHERE query=? AND source=? AND username=?""",
            (knowledge_id, norm, source, username)
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def backfill_summaries(db_path: str, username: str,
                       batch_size: int = 5, llm_router=None) -> int:
    """
    Fill NULL summaries via fleet. Fleet calls happen before DB writes.
    Returns count of summaries filled.
    """
    if not llm_router:
        return 0

    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, title, content_snippet, category FROM knowledge "
            "WHERE username=? AND summary IS NULL LIMIT ?",
            (username, batch_size)
        ).fetchall()
        rows = list(rows)
    finally:
        conn.close()

    if not rows:
        return 0

    updates = []
    for row in rows:
        if not row["content_snippet"]:
            continue
        try:
            prompt = (
                f"Summarize this document in 2-3 sentences.\n\n"
                f"Title: {row['title']}\nCategory: {row['category']}\n\n"
                f"Content:\n{row['content_snippet']}\n\nSummary:"
            )
            resp = llm_router.ask(prompt, preferred_tier="free")
            if resp and resp.content:
                updates.append((resp.content.strip()[:500], row["id"]))
        except Exception as e:
            log.debug(f"LOAM: backfill_summaries failed id={row['id']}: {e}")
            break

    if updates:
        conn = _connect(db_path)
        try:
            conn.executemany("UPDATE knowledge SET summary=? WHERE id=?", updates)
            conn.commit()
        finally:
            conn.close()
        log.info(f"LOAM: backfilled {len(updates)}/{len(rows)} summaries for {username}")

    return len(updates)


def backfill_embeddings(db_path: str, username: str,
                        batch_size: int = 20) -> int:
    """
    Compute embeddings for atoms without them. Returns count filled.
    """
    try:
        from core import embeddings as _emb
        if not _emb.is_available():
            return 0
    except ImportError:
        return 0

    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, title, content_snippet FROM knowledge "
            "WHERE username=? AND embedding IS NULL LIMIT ?",
            (username, batch_size)
        ).fetchall()
        rows = list(rows)
    finally:
        conn.close()

    if not rows:
        return 0

    updates = []
    for row in rows:
        text = f"{row['title'] or ''} {row['content_snippet'] or ''}"[:512]
        vec = _emb.embed(text)
        if vec:
            updates.append((vec, row["id"]))

    if updates:
        conn = _connect(db_path)
        try:
            conn.executemany("UPDATE knowledge SET embedding=? WHERE id=?", updates)
            conn.commit()
        finally:
            conn.close()
        log.info(f"LOAM: backfilled {len(updates)}/{len(rows)} embeddings for {username}")

    return len(updates)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def stats(db_path: str, username: str) -> dict:
    """Atom counts by ring and category. Gap summary."""
    conn = _connect(db_path)
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM knowledge WHERE username=?", (username,)
        ).fetchone()[0]

        by_ring = {}
        for r in ("source", "bridge", "continuity"):
            by_ring[r] = conn.execute(
                "SELECT COUNT(*) FROM knowledge WHERE username=? AND ring=?",
                (username, r)
            ).fetchone()[0]

        by_cat = {}
        for row in conn.execute(
            "SELECT category, COUNT(*) as cnt FROM knowledge "
            "WHERE username=? GROUP BY category ORDER BY cnt DESC LIMIT 10",
            (username,)
        ).fetchall():
            by_cat[row["category"] or "uncategorized"] = row["cnt"]

        gaps_open = conn.execute(
            "SELECT COUNT(*) FROM knowledge_gaps WHERE username=? AND resolved=0",
            (username,)
        ).fetchone()[0]

        no_summary = conn.execute(
            "SELECT COUNT(*) FROM knowledge WHERE username=? AND summary IS NULL",
            (username,)
        ).fetchone()[0]

        no_embedding = conn.execute(
            "SELECT COUNT(*) FROM knowledge WHERE username=? AND embedding IS NULL",
            (username,)
        ).fetchone()[0]

        return {
            "total":        total,
            "by_ring":      by_ring,
            "by_category":  by_cat,
            "gaps_open":    gaps_open,
            "no_summary":   no_summary,
            "no_embedding": no_embedding,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_dict(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    d = dict(row)
    d.pop("embedding", None)
    return d


def _extract_topics(text: str, max_topics: int = 5) -> list:
    STOP = {
        "a", "an", "the", "is", "are", "was", "were", "be", "been",
        "have", "has", "had", "do", "does", "did", "will", "would",
        "could", "should", "may", "might", "can", "to", "of", "in",
        "for", "on", "with", "at", "by", "from", "as", "and", "but",
        "if", "or", "not", "so", "just", "that", "this", "what",
        "who", "how", "when", "where", "i", "me", "my", "we", "you",
        "your", "he", "she", "it", "they", "them", "their",
    }
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
    seen: set = set()
    topics = []
    for w in words:
        if w not in STOP and w not in seen:
            seen.add(w)
            topics.append(w)
            if len(topics) >= max_topics:
                break
    return topics


def _cosine(vec_bytes_a: bytes, vec_bytes_b: bytes) -> float:
    """Cosine similarity between two packed float32 blobs."""
    try:
        dim_a = len(vec_bytes_a) // 4
        dim_b = len(vec_bytes_b) // 4
        if dim_a != dim_b or dim_a == 0:
            return 0.0
        a = struct.unpack(f"{dim_a}f", vec_bytes_a)
        b = struct.unpack(f"{dim_b}f", vec_bytes_b)
        dot = sum(x * y for x, y in zip(a, b))
        mag_a = sum(x * x for x in a) ** 0.5
        mag_b = sum(x * x for x in b) ** 0.5
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)
    except Exception:
        return 0.0
