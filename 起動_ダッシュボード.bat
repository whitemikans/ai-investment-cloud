@echo off
setlocal

cd /d "%~dp0"

set "PY_EXE="

REM Prefer real Python executable under LocalAppData
for /d %%D in ("%LocalAppData%\Programs\Python\Python*") do (
  if exist "%%~fD\python.exe" (
    set "PY_EXE=%%~fD\python.exe"
  )
)

REM Fallback to python from PATH
if not defined PY_EXE (
  where python >nul 2>nul
  if %ERRORLEVEL%==0 set "PY_EXE=python"
)

REM Fallback to py launcher only if callable
if not defined PY_EXE (
  where py >nul 2>nul
  if %ERRORLEVEL%==0 (
    py -3 -V >nul 2>nul
    if %ERRORLEVEL%==0 set "PY_EXE=py -3"
  )
)

if not defined PY_EXE (
  echo Python was not found.
  echo Install Python 3.10+ and run this file again.
  pause
  exit /b 1
)

set "MISS_FILE=%TEMP%\ai_dashboard_missing_modules.txt"
if exist "%MISS_FILE%" del /q "%MISS_FILE%" >nul 2>nul

%PY_EXE% -c "import importlib.util as u;mods=['streamlit','plotly','yfinance','pandas','numpy','sqlalchemy','feedparser','requests'];miss=[m for m in mods if u.find_spec(m) is None];print(','.join(miss)) if miss else None;raise SystemExit(1 if miss else 0)" > "%MISS_FILE%"

if %ERRORLEVEL% neq 0 (
  set /p MISSING=<"%MISS_FILE%"
  echo Missing modules: %MISSING%
  echo Installing dependencies for current user...
  %PY_EXE% -m pip install --user -r "%~dp0requirements.txt"
  if %ERRORLEVEL% neq 0 (
    echo Failed to install dependencies.
    echo If your network is restricted, run this once on a network that can access PyPI.
    pause
    exit /b 1
  )
)

echo Starting dashboard...
%PY_EXE% -m streamlit run "%~dp0app.py"

endlocal
