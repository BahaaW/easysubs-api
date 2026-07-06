# Research: Elevating the Proxy to Native Anthropic API Standards

This document details the feature gaps, architecture enhancements, and compatibility additions required to make the reverse proxy a 100% compliant translation layer for all Anthropic clients (including Claude Code, VS Code extensions, and standard SDKs).

---

## 1. Stream Event & Index Mapping (Crucial for Parallel Tool Calls)

### Current Gap
Our `ToolCallIndexMapper` and `map_chunk_tool_calls` only map indices inside the OpenAI schema structure (`choices[0].delta.tool_calls[i].index`).
For Anthropic streams, if Quatarly sends parallel tool calls with non-sequential indices (e.g. `index: 3` and `index: 5`), they are passed through verbatim. Some SDKs will throw an index error or drop the block because they expect sequential indexes starting from `0`.

### Proposed Fix
Extend the index mapper to intercept Anthropic's top-level `index` values inside `content_block_start` and `content_block_delta` events, remapping them to sequential integers:

```python
def map_anthropic_chunk_indices(chunk: dict, mapper: ToolCallIndexMapper) -> dict:
    if not isinstance(chunk, dict):
        return chunk
        
    c_type = chunk.get("type")
    if c_type in ("content_block_start", "content_block_delta", "content_block_stop"):
        if "index" in chunk:
            incoming = chunk["index"]
            # Detect if it's a tool_use block start to increment mapping
            has_id = False
            tool_call_id = None
            if c_type == "content_block_start":
                block = chunk.get("content_block", {})
                if block.get("type") == "tool_use":
                    has_id = True
                    tool_call_id = block.get("id")
                    
            chunk["index"] = mapper.map_index(incoming, has_id, tool_call_id)
            
    return chunk
```

---

## 2. Bidirectional Format Translation (Universal SDK Interoperability)

### Current Gap
The proxy assumes the client is using the format native to the endpoint they hit (OpenAI for `/v1/chat/completions`, Anthropic for `/v1/messages`). 
If a client configured for OpenAI-compatible base URLs selects an Anthropic model (e.g., `claude-3-5-sonnet-latest`), the proxy forwards the request as OpenAI format. If Quatarly only accepts Anthropic format on `/v1/messages`, the request fails.

### Proposed Fix
Implement a bidirectional request/response translator middleware inside the proxy:
- **OpenAI Request → Anthropic Request:**
  - Map `messages` array (translate roles: `assistant`, `user`, `system`).
  - Translate `role: "system"` messages into the top-level `system` prompt parameter.
  - Map `tools` and `tool_choice` format.
  - Map `max_tokens` / `temperature` / `stream`.
- **Anthropic Response/Stream → OpenAI Response/Stream:**
  - Translate `message_start` / `content_block_delta` events back to standard `choices[0].delta` structures.

This allows *any* OpenAI client to call *any* Anthropic model, and vice versa.

---

## 3. Strict Error Schema Mapping

### Current Gap
If Quatarly or Bedrock returns a 400 or 500 error, we return the raw body verbatim. However, if the client is expecting an Anthropic format but receives an OpenAI error format, the client SDK will crash during error parsing, masking the actual error.

### Proposed Fix
Intercept and normalize error payloads:
- **Anthropic clients expect:**
  ```json
  {
    "type": "error",
    "error": {
      "type": "invalid_request_error",
      "message": "Error details here"
    }
  }
  ```
- **OpenAI clients expect:**
  ```json
  {
    "error": {
      "message": "Error details here",
      "type": "invalid_request_error",
      "param": null,
      "code": null
    }
  }
  ```

Add an error translator before returning responses to the client.

---

## 4. Prompt Caching Metrics & Token Usage Analytics

### Current Gap
The proxy does not log input/output tokens or cache performance. This makes it impossible to build billing systems, analyze token consumption, or track prompt caching savings.

### Proposed Fix
Extend the DB schema to track token usage per key:
- Add columns: `input_tokens`, `output_tokens`, `cache_creation_input_tokens`, `cache_read_input_tokens`.
- In `_stream_generator`, parse the `message_start` and `message_delta` events to extract `usage` statistics.
- Flush these statistics asynchronously to the database.

---

## 5. Web-Socket Stream Keeping (Heartbeats)

### Current Gap
Some firewalls or reverse-proxies (like Cloudflare or Railway's gateway) terminate SSE streams if no data is sent for 30–60 seconds (common during long thinking/reasoning blocks).

### Proposed Fix
Inject keepalive/ping comments (`: ping\n\n`) into the SSE stream if the model goes silent for more than 15 seconds:
- Run a background timer task during the generator loop.
- If no chunk arrives, yield a silent comment line to keep the TCP socket open.
