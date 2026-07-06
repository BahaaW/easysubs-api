# EasySubs API - Project Primer
**Status**: Admin dashboard, translation proxy, HTTP/2 multiplexing, sliding cache, real-time streaming, thread offloads, batch flusher, and XML-to-JSON tool parser complete.
**Completed this session**:
- Implemented plain text conversion for toolUse/toolResult (Anthropic) and tool_calls/tool (OpenAI) blocks in request history.
- Stripped tools/tool_choice parameters from thinking model requests to bypass Bedrock 400 errors.
- Added comprehensive unit tests in test_proxy.py covering all formats, and verified all tests pass successfully.
**Next step**: Run Claude Code queries and verify real-time performance metrics via the admin dashboard.
**Blockers**: None.
