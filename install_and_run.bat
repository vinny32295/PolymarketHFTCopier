@echo off
REM =============================================================================
REM Polymarket Martingale Bot — Windows Installer & Launcher
REM =============================================================================
REM Double-click this file or run from Command Prompt.
REM Requires Python 3.8+ installed and on PATH.
REM =============================================================================

echo =============================================
echo   Polymarket Martingale Bot — Setup
echo =============================================
echo.

REM --- Check Python ---
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found on PATH.
    echo         Install from https://python.org/downloads/
    echo         Make sure to check "Add Python to PATH" during install.
    pause
    exit /b 1
)

python -c "import sys; exit(0 if sys.version_info >= (3,8) else 1)" 2>nul
if errorlevel 1 (
    echo [ERROR] Python 3.8+ is required.
    pause
    exit /b 1
)

for /f "tokens=*" %%i in ('python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"') do set PYVER=%%i
echo [OK] Found Python %PYVER%

REM --- Create virtual environment ---
if not exist ".venv" (
    echo.
    echo Creating virtual environment...
    python -m venv .venv
    echo [OK] Virtual environment created
) else (
    echo [OK] Virtual environment already exists
)

REM --- Activate and install ---
echo.
echo Installing dependencies...
call .venv\Scripts\activate.bat
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo [OK] Dependencies installed

REM --- Run ---
echo.
echo =============================================
echo   Setup Complete! Launching bot...
echo =============================================
echo.
python polymarket_martingale.py

pause
