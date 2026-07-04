# EasySubs API - Project Primer
**Status**: Admin dashboard, translation proxy, HTTP/2 multiplexing, sliding cache, Gzip compression, last-used timestamp, and GPT models cleanup complete.
**Completed this session**:
- Removed the broken `gpt-5.4` and `gpt-5.5` models from the static fallback list in `proxy.py` and `IMPPP.txt` to sync with Quatarly model mappings.
- Added `last_used_at` timestamp tracking in SQLite and in-memory cache sync.
- Displayed formatting and live update of "Last Used" date column directly on dashboard panel.
- Implemented sliding 1-hour in-memory cache for API keys with in-place request counter updates to avoid cache invalidation.
- Added Gzip compression middleware to shrink response sizes for large JSON payloads.
- Fixed 13 codebase-wide vulnerabilities and bugs including XSS and socket/resource leaks.
**Next step**: Run Claude Code queries and verify real-time performance metrics via the admin dashboard.
**Blockers**: None.
