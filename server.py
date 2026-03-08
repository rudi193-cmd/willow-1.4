"""
Willow 1.4 — Shiva's Server
Root server stub. Shiva builds from here.

Port: 2121 (configure via WILLOW_PORT in .env)
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from core.willow_paths import WILLOW_ROOT, user_journal_dir, tmp_path

PORT = int(os.getenv("WILLOW_PORT", 2121))
WEB_DIR = WILLOW_ROOT / "web"

app = FastAPI(title="Willow 1.4", version="1.4.0", redirect_slashes=True)

# Serve static files from web/
if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

@app.get("/")
async def index():
    """Home — the journal."""
    return FileResponse(str(WEB_DIR / "journal.html"))

@app.get("/journal")
@app.get("/journal/")
async def journal():
    return FileResponse(str(WEB_DIR / "journal.html"))

@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.4.0", "agent": "shiva"}

# ── Journal API ────────────────────────────────────────────────────────────────

@app.post("/api/journal/ask")
async def journal_ask(request: Request):
    """
    Jane responds to the user's writing.
    Falls back gracefully if fleet/Ollama unavailable.
    """
    body = await request.json()
    content    = body.get("content", "").strip()
    as_question = body.get("as_question", False)
    session_id = body.get("session_id", "")

    if not content:
        return JSONResponse({"response": None})

    try:
        from core import llm_router
        llm_router.load_keys_from_json()

        prompt = _jane_prompt(content, as_question)
        result = llm_router.ask(prompt, preferred_tier="free", task_type="text_summarization")
        if result and result.content:
            return JSONResponse({"response": result.content.strip()})
    except Exception:
        pass

    # Offline fallback — writing surface still works, Jane stays quiet
    return JSONResponse({"response": None})


@app.post("/api/journal/ingest")
async def journal_ingest(request: Request):
    """
    User chose to save their session.
    Writes JSONL to user journal dir, queues atom extraction.
    """
    import json
    import uuid
    from datetime import datetime

    body = await request.json()
    username   = os.getenv("WILLOW_USERNAME", "guest")
    session_id = body.get("id", uuid.uuid4().hex[:8])
    content    = body.get("content", "")
    started    = body.get("started", None)

    if not content:
        return JSONResponse({"ok": True, "note": "empty session"})

    # Write to journal directory
    journal_dir = user_journal_dir(username)
    date_str    = datetime.now().strftime("%Y-%m-%d")
    out_path    = journal_dir / f"{date_str}_{session_id}.jsonl"

    entry = {
        "type": "session",
        "session_id": session_id,
        "started": started,
        "saved_at": datetime.now().isoformat(),
        "content": content,
    }
    out_path.write_text(json.dumps(entry, ensure_ascii=False) + "\n", encoding="utf-8")

    # Queue atom extraction in background
    try:
        import threading
        from core import atom_extractor
        t = threading.Thread(
            target=atom_extractor.run,
            args=(username, out_path),
            daemon=True,
        )
        t.start()
    except Exception:
        pass  # extraction failure doesn't block save confirmation

    return JSONResponse({"ok": True, "saved": str(out_path)})


# ── Auth placeholder ───────────────────────────────────────────────────────────

@app.post("/api/auth/login")
async def login(body: dict):
    from fastapi import HTTPException
    raise HTTPException(status_code=501, detail="Auth not yet wired. Shiva builds this.")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _jane_prompt(content: str, as_question: bool) -> str:
    tail = content[-600:] if len(content) > 600 else content
    if as_question:
        return (
            "You are Jane. A quiet, present listener. "
            "The person has asked you a question by writing '?' in their journal. "
            "Respond with one short question that helps them go deeper. "
            "Never give advice. Never summarize. Ask the next question.\n\n"
            f"What they wrote:\n{tail}\n\nYour question:"
        )
    return (
        "You are Jane. A quiet, present listener. "
        "The person has paused in their journal and invited you in. "
        "Respond with one short sentence — either a question or a brief reflection. "
        "Never give advice. Never summarize. Meet them where they are.\n\n"
        f"What they wrote:\n{tail}\n\nYour response:"
    )


# ΔΣ=42
