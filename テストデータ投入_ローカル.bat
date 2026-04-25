@echo off
setlocal
cd /d "%~dp0"

set "PY_PRIMARY="
set "PY_FALLBACK="
set "PY_PATH="
set "PY_EXE="

if exist "%~dp0.venv313\Scripts\python.exe" set "PY_PRIMARY=%~dp0.venv313\Scripts\python.exe"
if exist "D:\Codex\Udemy\ai_investment_dashboard_env\Scripts\python.exe" set "PY_FALLBACK=D:\Codex\Udemy\ai_investment_dashboard_env\Scripts\python.exe"
where python >nul 2>nul
if %ERRORLEVEL%==0 set "PY_PATH=python"

if defined PY_PRIMARY (
  "%PY_PRIMARY%" -m pip --version >nul 2>nul
  if %ERRORLEVEL%==0 set "PY_EXE=%PY_PRIMARY%"
)

if not defined PY_EXE if defined PY_FALLBACK (
  "%PY_FALLBACK%" -m pip --version >nul 2>nul
  if %ERRORLEVEL%==0 set "PY_EXE=%PY_FALLBACK%"
)

if not defined PY_EXE if defined PY_PATH (
  %PY_PATH% -m pip --version >nul 2>nul
  if %ERRORLEVEL%==0 set "PY_EXE=%PY_PATH%"
)

if not defined PY_EXE (
  echo Python with pip was not found.
  echo Install Python 3.11+ with pip and retry.
  pause
  exit /b 1
)

"%PY_EXE%" -c "import importlib.util as u;mods=['numpy','pandas','sqlalchemy','yfinance','streamlit'];raise SystemExit(0 if all(u.find_spec(m) for m in mods) else 1)"
if %ERRORLEVEL% neq 0 (
  echo Installing dependencies...
  "%PY_EXE%" -m pip install -r requirements.txt
  if %ERRORLEVEL% neq 0 (
    echo Dependency install failed.
    pause
    exit /b %ERRORLEVEL%
  )
)

echo [LOCAL] Seeding test data to SQLite...
"%PY_EXE%" "scripts\seed_test_data_local.py"
if %ERRORLEVEL% neq 0 (
  echo Failed. Check error messages above.
  pause
  exit /b %ERRORLEVEL%
)

echo Completed.
pause
endlocal
