@echo off
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_innerva.ps1" -Mode commit-safe
if errorlevel 1 echo.
if errorlevel 1 echo Import stopped or failed. See the message above.
pause
