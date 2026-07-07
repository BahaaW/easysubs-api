# EasySubs API - Project Primer
**Status**: Bedrock 400 fixed, editing bugs fixed, stream generator tool call detection fixed, 502 timeout handling improved.
**Completed this session**:
- Fixed Bedrock 400: thinking models keep tools, only history converted to text.
- Fixed "doesn't edit" bug: stream fast-path skipping sanitize/mapper on tool call chunks.
- Fixed Anthropic input_json_delta tool call detection.
- Fixed 502 after timeout: body parsing failure now defaults to streaming mode.
- Wiki: added Bedrock 400, editing bugs, and stream fix log entries.
**Next steps**: None.
**Blockers**: None.
