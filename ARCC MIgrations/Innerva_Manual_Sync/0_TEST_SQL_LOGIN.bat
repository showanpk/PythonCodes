@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_innerva.ps1" -Mode test-connection
if errorlevel 1 (
    echo.
    echo SQL LOGIN TEST FAILED - Check the message above.
)
pause
