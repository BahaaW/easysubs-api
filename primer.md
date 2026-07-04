# EasySubs API - Project Primer
**Status**: Admin dashboard, translation proxy, HTTP/2 multiplexing, sliding cache, real-time streaming, thread offloads, and batch flusher complete.
**Completed this session**:
- Offloaded all blocking SQLite file I/O operations to worker threads via `asyncio.to_thread` to prevent freezing the FastAPI event loop.
- Implemented write buffering for request counts in RAM, writing to disk in a single batch every 10 seconds (saving up to 99% I/O overhead).
- Tuned HTTPX connection limits to `max_keepalive_connections=100` and `max_connections=500` for high concurrency.
- Resolved real-time word-by-word streaming bugs by creating a custom `SafeGZipMiddleware` that completely bypasses LLM API routes (eliminating event buffering).
- Injected explicit `Content-Type: text/event-stream` and anti-buffering headers (`X-Accel-Buffering`, `Cache-Control`) into downstream streams.
- Removed `gpt-5.4` and `gpt-5.5` entirely from the static fallback list in `proxy.py` and `IMPPP.txt` (retaining all Claude and Gemini fallback models).
- Added `last_used_at` timestamp tracking in SQLite and in-memory cache sync.
**Next step**: Run Claude Code queries and verify real-time performance metrics via the admin dashboard.
**Blockers**: None.
