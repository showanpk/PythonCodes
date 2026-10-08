@echo off
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_innerva.ps1" -Mode commit
if errorlevel 1 echo.
if errorlevel 1 echo COMMIT FAILED - see the message above.
echo.
pause
