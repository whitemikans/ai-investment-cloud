@echo off
setlocal

cd /d "%~dp0"

set "PY_EXE=%LocalAppData%\Programs\Python\Python314\python.exe"
if not exist "%PY_EXE%" set "PY_EXE=%LocalAppData%\Programs\Python\Python313\python.exe"
if not exist "%PY_EXE%" set "PY_EXE=%LocalAppData%\Programs\Python\Python312\python.exe"
if not exist "%PY_EXE%" set "PY_EXE=python"

echo Running Gemini connection test...
"%PY_EXE%" "%~dp0scripts\test_gemini_connection.py"

echo.
echo Press any key to close.
pause >nul
endlocal
