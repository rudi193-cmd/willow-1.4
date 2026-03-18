# Willow Seed — Quickstart

## What You're Starting From

You cloned willow-seed. This is the minimal bootstrap for a SAFE-compliant app in the Willow ecosystem.

## Setup

1. **Rename the manifest**
   ```
   cp safe-app-manifest.template.json safe-app-manifest.json
   ```
   Edit `safe-app-manifest.json` with your app's ID, name, and entry point.

2. **Copy the integration layer**
   ```
   cp safe_integration.template.py safe_integration.py
   ```
   Set `APP_ID` to match your `app_id` in the manifest.

3. **Install deps**
   ```
   pip install -r requirements.txt
   ```

4. **Build your app**
   Your app code goes in a module matching the `entry_point` in your manifest.

## Connecting to Willow

Your app talks to Willow through one drop point: `POST /api/pigeon/drop`.

```python
import safe_integration as willow

# Ask Willow a question
reply = willow.ask("What is the capital of France?")

# Query the knowledge graph
atoms = willow.query("France geography", limit=3)

# Contribute knowledge
willow.contribute("Paris is the capital.", category="reference")
```

That's it. You never call fleet APIs, import llm_router, or know what model answered.
Willow routes your drop to the right agent. You just drop and receive.

## Drop Topics

| Topic | What it does |
|-------|-------------|
| `ask` | LLM response via Willow fleet |
| `query` | Knowledge graph search → atom list |
| `contribute` | Ingest content into knowledge graph |
| `connect` | Propose an entity connection for Willow review |
| `status` | Health check |

## SAFE Principles

- **Session consent**: Ask permission every time. It expires when the session ends.
- **Local-first**: Data lives on the user's machine.
- **Minimal permissions**: Only request what you actually use.
- **No engagement optimization**: Your app is a tool, not a trap.

## ΔΣ=42
