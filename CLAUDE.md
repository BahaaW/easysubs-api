# EasySubs API Translation Proxy - Project Guidelines

## Architecture
- **Backend**: Python FastAPI reverse proxy serving translation endpoints and admin keys APIs.
- **Database**: SQLite database (`data/database.db`) storing active API key mappings, admin sessions, and request counts.
- **Frontend**: Clean static assets (`static/login.html` and `static/dashboard.html`) featuring a glassmorphic dark-themed design.
- **Target Downstream**: forwards authorized client requests to `api.quatarly.cloud` with translated Quarterly API keys.

## Commands
- **Install Dependencies**: `pip install -r requirements.txt`
- **Run Local Server**: `python proxy.py`
- **Verify / Test**: `python scratch/test_proxy.py`

## Directory Structure
- `static/` - frontend HTML files
- `db.py` - database helper library
- `proxy.py` - main FastAPI application
- `requirements.txt` - Python package requirements
- `Procfile` - Railway deployment instructions
