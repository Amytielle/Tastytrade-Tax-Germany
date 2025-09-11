@echo off
REM TastyTrade Transaction Tracker - Windows Launcher
REM This batch file launches the TastyTrade application on Windows

echo TastyTrade Transaction Tracker - Windows Launcher
echo ================================================

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python 3.7 or higher
    pause
    exit /b 1
)

REM Change to the script directory
cd /d "%~dp0"

REM Check if required files exist
if not exist "app.py" (
    echo Error: app.py not found
    pause
    exit /b 1
)

if not exist "requirements.txt" (
    echo Error: requirements.txt not found
    pause
    exit /b 1
)

echo Installing dependencies...
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo Error: Failed to install dependencies
    pause
    exit /b 1
)

REM Initialize database if it doesn't exist
if not exist "transactions.db" (
    echo Database not found. Initializing...
    python create_database.py
    if errorlevel 1 (
        echo Error: Failed to initialize database
        pause
        exit /b 1
    )
    echo Database initialized successfully.
) else (
    echo Database found.
)

echo.
echo Starting TastyTrade Transaction Tracker...
echo Application will be available at: http://localhost:5000
echo Press Ctrl+C to stop the application.
echo --------------------------------------------------
echo.

REM Start the Flask application
python app.py

echo.
echo Application stopped.
pause