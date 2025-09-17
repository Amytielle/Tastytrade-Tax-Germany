TODO
- Options
- ROC (Return of Capital)
- Export documents

# Latest fixes
- fixed USD to EUR conversion using historical rates
- added basic options calculation (still WIP)
- added EUR conversion transactions

# TastyTrade Transaction Tracker

A Flask web application for tracking and analyzing TastyTrade transactions with tax reporting capabilities.

## Prerequisites

- Python 3.7 or higher
- pip (Python package installer)

## Quick Start

### Option 1: Windows Batch File (Recommended for Windows)
Double-click `start_app.bat` or run in Command Prompt:
```cmd
start_app.bat
```
This will automatically:
- Install required dependencies
- Initialize the database if needed
- Start the application
- Open your web browser

### Option 2: Manual Start (Cross-Platform)
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Initialize database (if needed):
   ```bash
   python create_database.py
   ```
3. Start application:
   ```bash
   python app.py
   ```

## Access the Application
Once started, open your web browser and go to:
- http://localhost:5000

## Features
- Import TastyTrade transaction CSV files
- Track realized and unrealized gains/losses
- Generate tax summaries with EUR conversions
- View detailed transaction history
- Calculate dividends and fees

## Requirements
- Python 3.7 or higher
- Flask and other dependencies (see requirements.txt)

## Deployment & Distribution

### For Recipients
1. Extract all files to a folder
2. Ensure Python 3.7+ is installed
3. Run `start_app.bat` (Windows) or follow manual start instructions
4. The application will automatically set up dependencies and database

## Project Structure
- `app.py` - Main Flask application
- `create_database.py` - Database initialization script
- `start_app.bat` - Windows batch launcher
- `templates/` - HTML templates
- `services/` - Business logic layer
- `utils/` - Validation and utility functions
- `config/` - Configuration management
- `transactions.db` - SQLite database (created automatically)
- `requirements.txt` - Python dependencies
