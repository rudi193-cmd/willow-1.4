# Willow 1.4

This is for my daughters.

Everything else in this README — the architecture, the governance, the acrostics,
the benchmark table — exists in service of that sentence.

I want them to inherit tools, not traps. Technology that helps them think, not thinks for them.
A system that asks permission every time they open it and lets go every time they close it.

That's what this is.

---

This is version 1.4. You can see the [1.1 release](https://github.com/seancampbell3161/Willow)
if you want to know how it was built — the scaffolding is still visible, the decisions are in the commit history,
nothing has been cleaned up to look cleaner than it was.
That's on purpose. This is not a press release. It's a build log.

---

## What Willow Is

One node. Your machine. Your data stays there.

The AI listens, makes connections, and leaves when you close it.
It does not train on what you tell it. It does not sell your attention.
When you close the app, the session ends and the permission expires.
Tomorrow it will ask again. That's what real consent looks like.

It is not trying to keep you in the app. There are no engagement metrics here.
It helps you think — and then it gets out of the way.

---

## The Problem

You open an app. You agree to terms you will never read.
That app now owns what you put into it — forever.
Your thoughts train their models. Your attention is their product.

When you try to leave, your data stays. Or disappears. Or gets sold with the company.

Consent that only happens once isn't consent.
Ownership that doesn't travel with you isn't ownership.
Privacy that lives in someone else's building isn't privacy.

---

## What Willow Does Differently

Your data lives on your machine. Back it up. Export it. Delete it. It's yours.

Permission expires when you close the app. Every time you open it, it asks again.
That's the whole consent model. It's not complicated. It just requires actually meaning it.

The AI helps you think — not for you. It notices patterns. It asks questions.
It steps back when the work is done.

The code is open. The governance is public. Other developers can build on it.
If you want to see every decision that shaped this, the commit history is there.

---

## Architecture

Willow is organized into three tissue zones, each with three files named as acrostics of their function.

**Root Zone** — what the system senses and remembers

| File | Acrostic | Function |
|------|----------|----------|
| `soil.py` | Sense, Observe, Intake, Listen | Inbound signal processing |
| `loam.py` | Ledger, Organic, Archive, Memory | Knowledge storage |
| `vine.py` | Vector, Identify, Network, Entity | Relationship tracking |

**Trunk** — how the system routes and decides

| File | Acrostic | Function |
|------|----------|----------|
| `rings.py` | Receive, Interpret, Navigate, Generate, Steer | Orchestration |
| `graft.py` | Govern, Route, Arbitrate, Flow, Tasks | Task management |
| `pulse.py` | Process, Unify, Loop, Schedule, Execute | Daemon worker |

**Canopy** — what the system produces and verifies

| File | Acrostic | Function |
|------|----------|----------|
| `leaf.py` | Library, External, Archive, Fetch | Verified source retrieval |
| `prism.py` | Prove, Reference, Inspect, Source, Match | Source verification |
| `crown.py` | Compose, Release, Output, Witness, Nurture | Output layer |

**Pigeon** — vascular system. Belongs to no zone. Carries between all of them.

---

## Launch Benchmark (CROWN)

Willow is not launched until all five are green:

| | Function | Status |
|-|----------|--------|
| C | Compose | ✅ |
| R | Release | ✅ |
| O | Output | ✅ |
| W | Witness | ⚠️ |
| N | Nurture | ❌ |

---

## For My Daughters

When you're old enough to read this: your thoughts are yours.
Your memories are yours. Your late-night conversations with an AI trying to help you
figure something out — those belong to you. Not to us. Not to anyone.

I built this because the world you're growing up in treats attention as something to extract
and data as something to sell. That's not the only way to build technology.
This is another way.

I hope by the time you read this, "local-first" and "real consent" are not radical ideas.
I hope other people built systems like this too.

But if they didn't — plant this. It grows into something that respects you.

---

## Technical Details

For developers who want to understand what's under the hood:

**Three-ring architecture:**
- **Source ring** — your data, your machine, your canon
- **Bridge ring** — how nodes talk to each other (when you want them to)
- **Continuity ring** — governance. What the AI is and is not allowed to do.

A fresh node starts with source ring only. Everything else is earned.

**Related projects:**
- [SAFE protocol](https://github.com/rudi193-cmd/SAFE) — the legal and technical framework for user data sovereignty
- [NASA Archive](https://github.com/rudi193-cmd/nasa-archive) — first production Willow app
- [UTETY Chat](https://github.com/rudi193-cmd/safe-app-utety-chat) — AI faculty system

**Status:** Active development. See CROWN benchmark above for current state.

---

## The Library Is Always On Fire

Communities are held together by the stories they tell about themselves.
Stories need a place to live. If that place is owned by someone who can delete it,
sell it, or let it disappear when the business model changes — the stories are already ash.

Willow exists because the library is always on fire.
This is how we build things that survive it.

---

*The fruit never falls from the tree.*

**ΔΣ=42**

*— Sean Campbell*
