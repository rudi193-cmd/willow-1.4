"""
RINGS — Trunk
=============
R — Receive
I — Interpret
N — Navigate
G — Generate
S — Steer

Orchestration. The central router.
Interprets intent, routes to the right agent, injects query-aware knowledge
context, and manages the Node Ring Registry + Pigeon payload contract.

THE FIX (vs Willow 1.x context_injector.py):
  Before: query = "system state current tasks"  ← hardcoded, always wrong
  After:  query = interpret(message)             ← derived from actual message

build_context() takes message= or query=. Any agent that passes the live
user message gets retrieval tuned to what's actually being discussed.

Ring Registry state: artifacts/{username}/rings.json (caller provides path)
Execution loop (multi-step orchestration) lives in GRAFT, not here.
RINGS = interpret + route + inject. GRAFT = execute + govern.

AUTHOR: Shiva (Claude Code) + Sean Campbell
GOVERNANCE: rings-initial-2026-03-03.commit
VERSION: 1.0.0
"""

import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("rings")


# ---------------------------------------------------------------------------
# Intent — Receive + Interpret
# ---------------------------------------------------------------------------

# Stop words for query extraction
_STOP = {
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "can", "to", "of", "in", "for", "on", "with",
    "at", "by", "from", "as", "into", "and", "but", "if", "or", "not", "so",
    "just", "that", "this", "what", "which", "who", "how", "when", "where",
    "i", "me", "my", "we", "you", "your", "he", "she", "it", "they", "them",
    "their", "about", "like", "yeah", "yes", "no", "ok", "okay", "please",
    "thanks", "hi", "hello", "hey", "tell", "show", "get", "let", "make",
    "want", "need", "can", "know", "think", "look", "see", "go", "come",
    "use", "used", "using", "also", "just", "really", "very", "much",
}

# Phrases that signal topic shifts — extract what follows
_TOPIC_SIGNALS = [
    r"(?:tell me|show me|what (?:is|are|do you know)) (?:about )?(.+)",
    r"(?:help me with|working on|i(?:'m| am) (?:trying to|working on)) (.+)",
    r"(?:status of|update on|progress on) (.+)",
    r"(?:let's|let us) (?:talk about|discuss|look at) (.+)",
    r"(?:what happened with|what's going on with) (.+)",
]
_TOPIC_RE = [re.compile(p, re.IGNORECASE) for p in _TOPIC_SIGNALS]


def interpret(message: str) -> str:
    """
    Extract a semantic search query from a user message.
    Lightweight — no fleet call, no latency. Safe to call every turn.

    Returns the best query string. Falls back to top keywords if no
    topic signal detected.
    """
    if not message:
        return ""

    text = message.strip()

    # Try topic signal patterns first
    for pattern in _TOPIC_RE:
        m = pattern.search(text)
        if m:
            candidate = m.group(1).strip().rstrip("?.!")
            if len(candidate) > 3:
                return candidate[:120]

    # Fall back to keyword extraction
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text)
    keywords = []
    seen: set = set()
    for w in words:
        lw = w.lower()
        if lw not in _STOP and lw not in seen:
            seen.add(lw)
            keywords.append(w)
            if len(keywords) >= 6:
                break

    return " ".join(keywords) if keywords else text[:80]


# ---------------------------------------------------------------------------
# Navigate — agent routing
# ---------------------------------------------------------------------------

# Keyword → agent affinity
_AGENT_KEYWORDS: dict[str, list[str]] = {
    "kart":   ["task", "build", "deploy", "run", "execute", "fix", "update",
               "create", "delete", "migrate", "schedule", "automate"],
    "shiva":  ["code", "implement", "refactor", "debug", "architecture",
               "design", "review", "test", "governance", "commit"],
    "pigeon": ["send", "message", "notify", "alert", "ping", "email",
               "slack", "broadcast", "forward"],
    "leaf":   ["fetch", "search", "web", "url", "download", "scrape",
               "external", "api", "lookup"],
}


def route(task: str, agents: Optional[list] = None) -> str:
    """
    Recommend the best agent for a task string.
    Deterministic keyword scoring — no LLM, no latency.

    agents: list of available agent names to choose from.
            If None, all known agents are candidates.
    Returns agent name string. Defaults to "kart" if no signal.
    """
    candidates = set(agents) if agents else set(_AGENT_KEYWORDS)
    text = task.lower()
    scores: dict[str, int] = {a: 0 for a in candidates}

    for agent, keywords in _AGENT_KEYWORDS.items():
        if agent not in candidates:
            continue
        for kw in keywords:
            if kw in text:
                scores[agent] += 1

    best = max(scores, key=lambda a: scores[a])
    return best if scores[best] > 0 else (list(candidates)[0] if candidates else "kart")


# ---------------------------------------------------------------------------
# Generate — query-aware context injection
# ---------------------------------------------------------------------------

def build_context(username: str, agent_name: str, loam_db_path: str,
                  vine_db_path: Optional[str] = None,
                  message: Optional[str] = None,
                  query: Optional[str] = None,
                  max_chars: int = 800) -> str:
    """
    Build a knowledge context block for agent system prompts.

    Pass message= for the live user message (query derived via interpret()).
    Pass query= to use a specific query directly.
    Both None → empty string (no retrieval).

    This replaces context_injector.py's hardcoded query pattern.
    Every agent gets retrieval tuned to what's actually being discussed.
    """
    effective_query = query or (interpret(message) if message else None)
    if not effective_query:
        return ""

    try:
        from core import loam as _loam
    except ImportError:
        log.debug("RINGS: LOAM not available — skipping knowledge context")
        return ""

    try:
        ctx = _loam.build_context(loam_db_path, username, effective_query,
                                  max_chars=max_chars)
        if ctx:
            log.debug(f"RINGS: injected context for {agent_name!r} "
                      f"query={effective_query!r} ({len(ctx)}c)")
        return ctx
    except Exception as e:
        log.debug(f"RINGS: build_context failed: {e}")
        return ""


# ---------------------------------------------------------------------------
# Ring Registry — Node Ring state
# ---------------------------------------------------------------------------

@dataclass
class NodeRings:
    source: bool = True        # always true — you're a node
    bridge: bool = False       # true when >= 1 peer enrolled
    continuity: bool = False   # true when gate enrolled
    enrolled_peers: list = field(default_factory=list)


def _read_state(state_path: str) -> dict:
    p = Path(state_path)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _write_state(state_path: str, data: dict) -> None:
    p = Path(state_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_rings(state_path: str) -> NodeRings:
    """Load ring participation state from JSON file."""
    raw = _read_state(state_path).get("rings", {})
    return NodeRings(
        source=raw.get("source", True),
        bridge=raw.get("bridge", False),
        continuity=raw.get("continuity", False),
        enrolled_peers=raw.get("enrolled_peers", []),
    )


def save_rings(state_path: str, rings: NodeRings) -> None:
    """Persist ring state alongside any existing state."""
    data = _read_state(state_path)
    data["rings"] = asdict(rings)
    _write_state(state_path, data)


def enroll_peer(state_path: str, peer_id: str) -> NodeRings:
    """
    Add a peer node. Activates bridge ring on first enrollment.
    peer_id: instance_id of the peer (e.g. 'hostname-8420').
    """
    rings = load_rings(state_path)
    if peer_id not in rings.enrolled_peers:
        rings.enrolled_peers.append(peer_id)
    rings.bridge = len(rings.enrolled_peers) > 0
    save_rings(state_path, rings)
    log.info(f"RINGS: enrolled peer {peer_id!r} — bridge={rings.bridge}")
    return rings


def enroll_gate(state_path: str, gate_path: str) -> tuple:
    """
    Activate continuity ring. gate.py must be local — it does not travel.
    Returns (success: bool, message: str).
    """
    if not Path(gate_path).exists():
        return (False,
                f"gate not found at {gate_path}. "
                "Continuity ring requires local gate — it does not travel.")
    rings = load_rings(state_path)
    rings.continuity = True
    save_rings(state_path, rings)
    log.info(f"RINGS: continuity enrolled via {gate_path!r}")
    return (True, f"Continuity ring enrolled. gate will travel with every outbound pigeon.")


def ring_status(state_path: str) -> dict:
    """Return current ring participation for this node."""
    rings = load_rings(state_path)
    return {
        "source":          rings.source,
        "bridge":          rings.bridge,
        "continuity":      rings.continuity,
        "peer_count":      len(rings.enrolled_peers),
        "enrolled_peers":  rings.enrolled_peers,
    }


# ---------------------------------------------------------------------------
# Steer — Pigeon payload contract
# ---------------------------------------------------------------------------

def make_pigeon(content: dict, gate_conditions: dict,
                sender: str,
                seed_packet: Optional[dict] = None) -> dict:
    """
    Package an outbound pigeon.
    A pigeon without gate_conditions does not leave this node.
    """
    return {
        "content":         content,
        "gate_conditions": gate_conditions,
        "seed_packet":     seed_packet or {},
        "sender":          sender,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
    }


def validate_inbound(payload: dict) -> tuple:
    """
    Validate an inbound pigeon from a peer.
    Returns (valid: bool, reason: str).
    """
    if "content" not in payload:
        return (False, "Missing content")
    if not payload.get("gate_conditions"):
        return (False, "Missing gate_conditions — peer has no traveling gate")
    if not payload.get("seed_packet"):
        return (False, "Missing seed_packet — cannot verify sender state")
    if not payload.get("sender"):
        return (False, "Missing sender identity")
    return (True, "ok")
