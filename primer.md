# EasySubs API - Project Primer
**Status**: Full security audit (007) completed, performance optimizations applied, dashboard metrics fixed.
**Completed this session**:
- 007 security audit: 3 critical, 2 medium, 1 low finding — all fixed.
- Added CSRF protection, Fernet key encryption, CSP headers, `__Host-` cookie prefix.
- Fixed dashboard not showing request_count/last_used_at (flush wasn't writing them).
- Fixed double-counting bug in DB recovery (baseline + new increments).
- Fixed daily_reset_date migration type (INTEGER → TEXT).
- Added rate_limit_rpm column migration.
- Performance: non-streaming reqs skip upstream SSE, stream generator fast-path for clean responses.
- Updated wiki: Security Hardening, Performance Optimization, Fernet Encryption pages.
**Next steps**: None.
**Blockers**: None.
