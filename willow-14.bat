@echo off
REM Willow 1.4 — Full Boot
REM Starts Willow (8420) + Shiva journal server (2121) + core daemons

set WILLOW=%~dp0..\Willow
set W14=%~dp0
set VENV=%USERPROFILE%\.willow-venv\Scripts\python.exe

echo ========================================
echo  Willow 1.4 // Shiva's Ground
echo ========================================
echo.

REM ── Kill any stale instances first ──
echo [*] Clearing stale processes...
taskkill /F /FI "WINDOWTITLE eq Willow-*" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Willow14-*" >nul 2>&1
timeout /t 1 >nul

REM ── Main Willow server (8420) ──
echo [*] Starting Willow server on port 8420...
start "Willow-Server" /MIN cmd /c "cd /d "%WILLOW%" && "%VENV%" -m uvicorn server:app --host 0.0.0.0 --port 8420"
timeout /t 4 >nul

REM ── Willow 1.4 / Journal server (2121) ──
echo [*] Starting Shiva journal server on port 2121...
start "Willow14-Server" /MIN cmd /c "cd /d "%W14%" && "%VENV%" -m uvicorn server:app --host 0.0.0.0 --port 2121 --reload"
timeout /t 3 >nul

REM ── Core daemons (one each) ──
echo.
echo Starting daemons...
echo.

echo [1/4] Pigeon daemon...
start "Willow-Pigeon" /MIN cmd /c "cd /d "%WILLOW%" && "%VENV%" core/pigeon_daemon.py"

echo [2/4] OCR consumer...
start "Willow-OCR" /MIN cmd /c "cd /d "%WILLOW%" && "%VENV%" core/ocr_consumer_daemon.py"

echo [3/4] Inbox watcher...
start "Willow-Inbox" /MIN cmd /c "cd /d "%WILLOW%" && "%VENV%" tools/inbox_watcher.py"

echo [4/4] MCP server...
start "Willow-MCP" /MIN cmd /c "cd /d "%WILLOW%" && "%VENV%" mcp/willow_server.py"

REM ── Open journal ──
timeout /t 2 >nul
echo.
echo [*] Opening journal...
start "" "http://localhost:2121/journal/"

echo.
echo ========================================
echo  Willow is running.
echo  Willow:  http://localhost:8420/
echo  Journal: http://localhost:2121/journal/
echo  Daemons: pigeon, ocr, inbox, mcp
echo ========================================
echo.
echo To stop: close this window or run stop_daemons.bat
echo.
pause
