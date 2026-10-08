@echo off
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_innerva.ps1" -Mode preview
if errorlevel 1 echo.
if errorlevel 1 echo PREVIEW FAILED - see the message above.
echo.
pause
