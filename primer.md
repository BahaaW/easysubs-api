# EasySubs API - Project Primer
**Status**: Admin dashboard, translation proxy, HTTP/2 multiplexing, sliding cache, real-time streaming, thread offloads, batch flusher, XML-to-JSON parser, and multi-app compat layer complete.
**Completed this session**:
- Implemented tool block history translation to plain text (protecting against Bedrock 400 errors).
- Added multi-app compatibility: wired model aliases, rate_limit_rpm limiter, models caching, and path normalization.
- Solved double-close response bug, verified Cursor 400 error pass-through, and migrated database sessions table to add missing expires_at column.
**Next steps**:
- Run VS Code or Cursor client through the proxy and verify RPM limiting and format mapping on live runs.
**Blockers**: None.
