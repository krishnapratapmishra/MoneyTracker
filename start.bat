@echo off
title Money Magnet - Starting...
color 0A

echo.
echo  ============================================
echo    💎 Money Magnet - Starting Server...
echo  ============================================
echo.

:: Change to the directory where this bat file lives
cd /d "%~dp0"

:: ── Locate Python ────────────────────────────────────────────────────────────
:: Try the real Python install first, then the py launcher, then generic python
set PYTHON=

if exist "%LOCALAPPDATA%\Python\bin\python.exe" (
    set PYTHON="%LOCALAPPDATA%\Python\bin\python.exe"
    goto :found
)

where py >nul 2>&1 && set PYTHON=py && goto :found
where python3 >nul 2>&1 && set PYTHON=python3 && goto :found
where python  >nul 2>&1 && set PYTHON=python  && goto :found

echo  [ERROR] Python not found. Install Python from https://python.org and try again.
pause
exit /b 1

:found
echo  Using Python: %PYTHON%
echo.

:: Open browser after 3 seconds (gives Flask time to boot)
start "" cmd /c "timeout /t 3 /nobreak >nul && start http://127.0.0.1:5000"

:: Start Flask (keeps this window open as the server console)
echo  Server starting at http://127.0.0.1:5000
echo  Press Ctrl+C to stop the server.
echo.
%PYTHON% app.py

echo.
echo  Server stopped.
pause
