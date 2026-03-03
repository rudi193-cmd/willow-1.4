"""
SAFE Framework Integration Template
====================================
Copy this file to your app root as safe_integration.py and set APP_ID.

Drop point: POST /api/pigeon/drop
Topics: ask, query, contribute, connect, status

Usage:
    import safe_integration as willow
    reply = willow.ask("What is the capital of France?")
    atoms = willow.query("France geography", limit=3)
    willow.contribute("Paris is the capital of France.", category="reference")
"""

import uuid
import requests
from typing import Optional

import os
WILLOW_URL = os.environ.get("WILLOW_URL", "http://localhost:8420")
PIGEON_URL = f"{WILLOW_URL}/api/pigeon/drop"
APP_ID = "safe-app-{name}"  # Replace with your app's ID from safe-app-manifest.json

_session_id = str(uuid.uuid4())


def ask(prompt: str, persona: Optional[str] = None, tier: str = "free") -> str:
    """Ask Willow a question. Returns the LLM response as a string."""
    result = _drop("ask", {"prompt": prompt, "persona": persona, "tier": tier})
    if result.get("ok"):
        return result.get("result", "")
    return f"[Error: {result.get('error', 'unknown')}]"


def query(q: str, limit: int = 5) -> list:
    """Query Willow's knowledge graph. Returns a list of matching atoms."""
    result = _drop("query", {"q": q, "limit": limit})
    if result.get("ok"):
        return result.get("result", [])
    return []


def contribute(content: str, category: str = "note", metadata: Optional[dict] = None) -> dict:
    """Contribute content to Willow's knowledge graph."""
    return _drop("contribute", {
        "content": content,
        "category": category,
        "metadata": metadata or {},
    })


def connect(entity_a: str, entity_b: str, relation: str = "related_to") -> dict:
    """Propose an entity connection for Willow review."""
    return _drop("connect", {
        "entity_a": entity_a,
        "entity_b": entity_b,
        "relation": relation,
    })


def status() -> dict:
    """Check if Willow bus is reachable."""
    return _drop("status", {})


def _drop(topic: str, payload: dict) -> dict:
    """Internal: drop a message onto the Pigeon bus."""
    try:
        r = requests.post(PIGEON_URL, json={
            "topic": topic,
            "app_id": APP_ID,
            "session_id": _session_id,
            "payload": payload,
        }, timeout=30)
        return r.json() if r.ok else {"ok": False, "error": r.text}
    except requests.ConnectionError:
        return {
            "ok": False,
            "guest_mode": True,
            "error": f"Willow not reachable at {WILLOW_URL}. "
                     "Set WILLOW_URL env var or run Willow locally."
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
