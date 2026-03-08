"""
willow_paths.py — Runtime path resolution for Willow.

Willow discovers where it is. Nothing is hardcoded.

Usage:
    from core.willow_paths import WILLOW_ROOT, user_data, tmp_path

All modules import from here instead of hardcoding paths.
.env overrides are always respected.
"""

import os
import tempfile
from pathlib import Path
from dotenv import load_dotenv

# Load .env from wherever we find it — before anything else
def _load_env():
    candidate = Path(__file__).resolve()
    for parent in [candidate, *candidate.parents]:
        env_file = parent / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=False)
            return
_load_env()


# ── Root discovery ─────────────────────────────────────────────────────────────

def find_willow_root() -> Path:
    """
    Walk up from this file until we find server.py.
    That directory is WILLOW_ROOT.

    Override with WILLOW_ROOT env var if set.
    """
    override = os.getenv("WILLOW_ROOT", "").strip()
    if override:
        p = Path(override)
        if p.exists():
            return p.resolve()

    candidate = Path(__file__).resolve()
    for parent in [candidate, *candidate.parents]:
        if (parent / "server.py").exists():
            return parent

    raise RuntimeError(
        "Cannot locate Willow root. Is willow_paths.py inside the Willow repo?\n"
        "Set WILLOW_ROOT in your .env to override."
    )


# ── Module-level root — resolved once at import ────────────────────────────────

WILLOW_ROOT: Path = find_willow_root()


# ── User paths ─────────────────────────────────────────────────────────────────

def user_data(username: str) -> Path:
    """
    Persistent user data directory.
    WILLOW_ROOT/artifacts/{username}/
    Created if it doesn't exist.
    """
    p = WILLOW_ROOT / "artifacts" / username
    p.mkdir(parents=True, exist_ok=True)
    return p


def user_db(username: str, name: str = "willow_knowledge.db") -> Path:
    """
    Path to a user's SQLite database.
    WILLOW_ROOT/artifacts/{username}/{name}
    """
    return user_data(username) / name


def user_journal_dir(username: str) -> Path:
    """
    Journal session files.
    WILLOW_ROOT/artifacts/{username}/journal/
    """
    p = user_data(username) / "journal"
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── Temp paths ─────────────────────────────────────────────────────────────────

def tmp_dir(username: str = "") -> Path:
    """
    Temp directory for this user.
    {system_tmp}/willow/{username}/
    Created if it doesn't exist.
    """
    base = Path(tempfile.gettempdir()) / "willow"
    if username:
        base = base / username
    base.mkdir(parents=True, exist_ok=True)
    return base


def tmp_path(filename: str, username: str = "") -> Path:
    """
    Path to a named temp file.
    {system_tmp}/willow/{username}/{filename}
    """
    return tmp_dir(username) / filename


def journal_session_tmp(username: str, session_id: str) -> Path:
    """
    Live JSONL path for an active journal session.
    Written to during the session, resolved on close.
    {system_tmp}/willow/{username}/journal_{session_id}.jsonl
    """
    return tmp_path(f"journal_{session_id}.jsonl", username)


def orphaned_journal_sessions(username: str) -> list[Path]:
    """
    Find JSONL files in tmp that were never closed (crash/disconnect recovery).
    Returns list of Path objects, newest first.
    """
    d = tmp_dir(username)
    files = sorted(d.glob("journal_*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)
    return files


# ── Core paths ─────────────────────────────────────────────────────────────────

def core_path(*parts) -> Path:
    """Path inside WILLOW_ROOT/core/"""
    return WILLOW_ROOT / "core" / Path(*parts)


def web_path(*parts) -> Path:
    """Path inside WILLOW_ROOT/web/"""
    return WILLOW_ROOT / "web" / Path(*parts)


def governance_path(*parts) -> Path:
    """Path inside WILLOW_ROOT/governance/"""
    return WILLOW_ROOT / "governance" / Path(*parts)


# ── Shiva DB ───────────────────────────────────────────────────────────────────

def shiva_db() -> Path:
    """
    Shiva's memory database.
    Override with SHIVA_DB env var.
    """
    override = os.getenv("SHIVA_DB", "").strip()
    if override:
        return Path(override)
    return WILLOW_ROOT / "shiva_memory" / "shiva.db"


# ── Diagnostics ────────────────────────────────────────────────────────────────

def report() -> dict:
    """Return all resolved paths for diagnostics."""
    return {
        "willow_root":  str(WILLOW_ROOT),
        "core":         str(core_path()),
        "web":          str(web_path()),
        "governance":   str(governance_path()),
        "shiva_db":     str(shiva_db()),
        "tmp_dir":      str(tmp_dir()),
        "env_override": os.getenv("WILLOW_ROOT", "(none)"),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(report(), indent=2))
