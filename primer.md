# EasySubs API - Project Primer
**Status**: Admin dashboard, translation proxy, 3-min timeouts, logging silence, and SQLite WAL concurrency complete.
**Completed this session**:
- Created SQLite database layer (`db.py`) and enabled WAL mode with a 30s timeout.
- Created glassmorphic admin pages (`login.html` and `dashboard.html`) in `static/`.
- Updated `proxy.py` with session cookies, silent warning/error logging, and translated proxy bearer keys.
- Shared global `httpx.AsyncClient` with a 3-minute timeout to handle concurrent API requests.
- Added `Procfile` and `.gitignore` for Railway deployment.
- Wrote integration tests verifying 100% database, auth, and concurrent proxy translation success.
**Next step**: Deploy the project on Railway, configure admin credentials env vars, and attach a `/data` volume.
**Blockers**: None.
