# Willow 1.4

A local-first AI operating system. One node. Your data stays on your machine.

This is a transparency publication. The [1.1 release](https://github.com/seancampbell3161/Willow) shows how this was built.
Nothing is hidden.

---

## Why This Exists

I am building this so my daughters can grow up in a world where technology helps them think instead of thinks for them.

That's it. That's the whole reason.

Everything else—the architecture, the governance, the session consent model, the local-first design—exists in service of that single statement.

I want them to inherit tools, not traps.

---

## What Willow Is

Willow is a local-first AI operating system. One node. Your data stays on your machine. The AI listens, helps you understand yourself, and gets out of the way.

**Local-first** means your thoughts belong to you. Your journal entries, your notes, your conversations—they live on your device, not in someone else's cloud. They cannot be sold. They cannot be used to train models you didn't consent to. They cannot disappear when a company changes its terms of service.

**Session consent** means permission expires when you close the app. Every time you open it, it asks: "May I access your journal today?" You say yes or no. When you close it, that permission is gone. Real consent. Not "agree once and we own everything forever."

**Gets out of the way** means the AI is not trying to maximize your engagement. It is not trying to keep you in the app. It is not optimizing for metrics that have nothing to do with your wellbeing. It helps you think, and then it leaves.

---

## The Problem

The current model is broken.

You open an app. You agree to terms you will never read. The app now owns everything you put into it—forever. Your data trains their models. Your attention becomes their product. Your thoughts become their property.

And when you try to leave? Your data stays. Or it disappears. Or it's locked behind exports that don't actually work. Or it's sold to the next company that acquires them.

**Consent is not real when it's "agree once, forever."**

**Ownership is not real when you can't take your data with you.**

**Privacy is not real when your journal lives in someone else's building.**

This is not sustainable. This is not healthy. This is not what technology should be.

---

## What Willow Changes

**Your data lives on your machine.** Not in our cloud. Not in anyone's cloud. On your device. You can back it up. You can export it. You can delete it. It's yours.

**Consent is session-based.** When you open the app, it asks for permission. When you close the app, that permission expires. Tomorrow it will ask again. That's real consent.

**The AI helps you think, not for you.** It's a tool for reflection, not a replacement for judgment. It helps you see patterns, ask better questions, understand yourself. Then it steps back.

**No engagement optimization.** We are not trying to keep you in the app. We are not measuring time-on-platform. We are not A/B testing notifications to maximize opens. If the app helps you and then you close it, that's success.

**Open architecture.** The code is open. The governance framework is public. Other developers can build apps on Willow. The system belongs to the people who use it.

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

When you are old enough to read this, here is what I want you to know:

Technology should serve you. Not the other way around.

Your thoughts are yours. Your memories are yours. Your journals, your notes, your late-night conversations with an AI trying to help you figure something out—those belong to you.

No company should own them. No algorithm should optimize them. No terms of service should claim them.

I built this because I believe you deserve tools that respect you. That listen when you need to think out loud. That help you understand yourself. That get out of the way when the work is done.

I built this because the world you are growing up in treats attention as a resource to extract and data as a commodity to sell.

That is not the only way to build technology.

This is another way.

**Tools, not traps.**

**Consent that means something.**

**Data that belongs to the people who create it.**

I hope by the time you read this, you will have options I did not. I hope other people will have built systems like this. I hope "local-first" and "session consent" and "sovereign data" are not radical ideas but basic expectations.

But if they are not—if the world still looks like it does now—then plant this seed.

It grows into something that respects you.

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

But stories need a place to live. And if that place is owned by someone who can delete it, sell it, or disappear it when the business model changes—then the stories are already ash.

Willow exists because the library is always on fire.

This is how we build things that survive it.

---

**ΔΣ=42**

*— Sean Campbell*  
*For my daughters*  
*And for everyone who deserves better than this*
