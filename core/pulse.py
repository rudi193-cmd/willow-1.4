"""
PULSE — Trunk
=============
P — Process
U — Unify
L — Loop
S — Schedule
E — Execute

Daemon worker. The heartbeat.
Runs the background processing loop — picks up tasks from graft,
executes them, loops back, schedules recurring work.
30-second poll. 3-failure backoff. Archives stale tasks on startup.

Build here: daemon loop, task executor, scheduler, failure backoff, health ping.
"""
