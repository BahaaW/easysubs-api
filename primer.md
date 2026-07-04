# EasySubs API - Project Primer
**Status**: Admin dashboard, translation proxy, 3-min timeouts, HTTP/2 support, logging silence, and SQLite WAL concurrency complete.
**Completed this session**:
- Enabled HTTP/2 multiplexing on global `httpx.AsyncClient` for high-concurrency request pipelining.
- Supported Anthropic-style `x-api-key` authentication headers for compatibility with Claude Code startup and prompt hooks.
- Integrated sliding window IP-based rate limiting on admin login, models, and proxy routes.
- Wrote integration tests verifying 100% database, auth, rate limiting, and HTTP/2 proxy translation success.
**Next step**: Run Claude Code queries and verify real-time performance metrics via the admin dashboard.
**Blockers**: None.
