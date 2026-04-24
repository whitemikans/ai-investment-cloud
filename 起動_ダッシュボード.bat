@echo off
setlocal EnableDelayedExpansion

cd /d "%~dp0"

set "PY_EXE="
set "PORT="
set "USE_VENV313=0"

REM Prefer project-local Python venv for #09
if exist "%~dp0.venv313\Scripts\python.exe" (
  set "PY_EXE=%~dp0.venv313\Scripts\python.exe"
  set "USE_VENV313=1"
)

REM Prefer real Python executable under LocalAppData
if not defined PY_EXE (
  for /d %%D in ("%LocalAppData%\Programs\Python\Python*") do (
    if exist "%%~fD\python.exe" (
      set "PY_EXE=%%~fD\python.exe"
    )
  )
)

REM Fallback to python from PATH
if not defined PY_EXE (
  where python >nul 2>nul
  if %ERRORLEVEL%==0 set "PY_EXE=python"
)

if not defined PY_EXE (
  echo Python was not found.
  echo Install Python 3.10+ and run this file again.
  pause
  exit /b 1
)

REM If selected python has no pip, fallback to project shared venv when available
%PY_EXE% -m pip --version >nul 2>nul
if %ERRORLEVEL% neq 0 (
  if exist "D:\Codex\Udemy\ai_investment_dashboard_env\Scripts\python.exe" (
    "D:\Codex\Udemy\ai_investment_dashboard_env\Scripts\python.exe" -m pip --version >nul 2>nul
    if %ERRORLEVEL%==0 set "PY_EXE=D:\Codex\Udemy\ai_investment_dashboard_env\Scripts\python.exe"
  )
)

set "MISS_FILE=%TEMP%\ai_dashboard_missing_modules.txt"
if exist "%MISS_FILE%" del /q "%MISS_FILE%" >nul 2>nul

%PY_EXE% -c "import importlib.util as u;mods=['streamlit','plotly','yfinance','pandas','numpy','sqlalchemy','feedparser','requests','scipy','google.genai','crewai','crewai_tools'];miss=[m for m in mods if u.find_spec(m) is None];print(','.join(miss)) if miss else None;raise SystemExit(1 if miss else 0)" > "%MISS_FILE%"

if %ERRORLEVEL% neq 0 (
  set /p MISSING=<"%MISS_FILE%"
  echo Missing modules: %MISSING%
  echo Installing dependencies...
  if "%USE_VENV313%"=="1" (
    %PY_EXE% -m pip install -r "%~dp0requirements.txt"
    %PY_EXE% -m pip install crewai crewai-tools google-genai
  ) else (
    %PY_EXE% -m pip install --user -r "%~dp0requirements.txt"
    %PY_EXE% -m pip install --user crewai crewai-tools google-genai
  )
  if %ERRORLEVEL% neq 0 (
    echo Failed to install dependencies.
    echo If your network is restricted, run this once on a network that can access PyPI.
    pause
    exit /b 1
  )
)

REM Find first free port in 8501-8520
for /l %%P in (8501,1,8520) do (
  netstat -ano | findstr /r /c:":%%P .*LISTENING" >nul 2>nul
  if errorlevel 1 (
    set "PORT=%%P"
    goto :port_found
  )
)

:port_found
if not defined PORT (
  echo No available port in 8501-8520.
  echo Close existing Streamlit apps and try again.
  pause
  exit /b 1
)

echo Starting dashboard...
echo URL: http://localhost:%PORT%
echo Python: %PY_EXE%
%PY_EXE% -m streamlit run "%~dp0app.py" --server.port=%PORT% --server.headless=false --browser.serverAddress=localhost
if %ERRORLEVEL% neq 0 (
  echo Streamlit failed to start. See error messages above.
  pause
  exit /b %ERRORLEVEL%
)

endlocal
