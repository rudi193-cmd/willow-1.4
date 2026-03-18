@echo off
REM Willow 1.4 — Full Boot
REM Starts server + all daemons, then opens the login screen.

echo ========================================
echo  Willow 1.4 // Shiva's Ground
echo ========================================
echo.

REM Change to willow-1.4 directory
cd /d "%~dp0"

REM Check for Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found in PATH
    pause
    exit /b 1
)

REM --- SERVER ---
echo [*] Starting Willow 1.4 server on port 2121...
start "Willow14-Server" /MIN cmd /c "uvicorn server:app --host 0.0.0.0 --port 2121 --reload"

REM Give server a moment to bind
timeout /t 3 >nul

REM --- BROWSER ---
echo [*] Opening login screen...
start "" "http://localhost:2121/"

echo.
echo Starting daemons...
echo.

REM Start Governance Monitor (every 60s, checks pending commits)
echo [1/7] Governance Monitor...
start "Willow-GovernanceMonitor" /MIN python governance/monitor.py --interval 60 --daemon

REM Start Coherence Scanner (every 1h, scans knowledge for drift)
echo [2/7] Coherence Scanner...
start "Willow-CoherenceScanner" /MIN python core/coherence_scanner.py --interval 3600 --daemon

REM Start Topology Builder (every 1h, builds knowledge graph edges)
echo [3/7] Topology Builder...
start "Willow-TopologyBuilder" /MIN python core/topology_builder.py --interval 3600 --daemon

REM Start Compost (every 24h, archives old data — was knowledge_compactor.py)
echo [4/7] Compost...
start "Willow-Compost" /MIN python core/compost.py --interval 86400 --daemon

REM Start SAFE Sync (every 5m, syncs to SAFE repo)
echo [5/7] SAFE Sync...
start "Willow-SAFESync" /MIN python core/safe_sync.py --interval 300 --daemon

REM Start Persona Scheduler (every 60s, runs scheduled persona tasks)
echo [6/7] Persona Scheduler...
start "Willow-PersonaScheduler" /MIN python core/persona_scheduler.py --interval 60 --daemon

REM Start Pulse — Kart Bridge Ring daemon (30s poll, processes task queue)
echo [7/7] Pulse (Kart daemon)...
start "Willow-Pulse" /MIN python core/pulse.py --daemon

echo.
echo ========================================
echo  Willow 1.4 is running.
echo  Server:  http://localhost:2121/
echo  Daemons: 7 started
echo ========================================
echo.
echo To stop: close this window or run stop_daemons.bat
echo Logs: core/*.log, governance/violations.log
echo.
pause
