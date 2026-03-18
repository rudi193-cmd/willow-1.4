"""
Willow 1.4 — Shiva's Server
Root server stub. Shiva builds from here.

Port: 2121 (configure via WILLOW_PORT in .env)
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

load_dotenv()

PORT = int(os.getenv("WILLOW_PORT", 2121))
WEB_DIR = Path(__file__).parent / "web"

app = FastAPI(title="Willow 1.4", version="1.4.0")

# Serve static files from web/
if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

@app.get("/")
async def index():
    """Login screen."""
    return FileResponse(str(WEB_DIR / "index.html"))

@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.4.0", "agent": "shiva"}

# Auth placeholder — Shiva wires real auth when ready
@app.post("/api/auth/login")
async def login(body: dict):
    from fastapi import HTTPException
    # TODO: Shiva implements real auth against Willow prime
    raise HTTPException(status_code=501, detail="Auth not yet wired. Shiva builds this.")

# ΔΣ=42
