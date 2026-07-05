import os
import json
import logging
from contextlib import asynccontextmanager
from typing import Any
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
import httpx
import uvicorn

import db

import asyncio
import time
import re
import html
import secrets

# Global in-memory list to store the last 100 stream debug logs for troubleshooting
debug_logs = []

# Configure logging (only show warnings and errors)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("QuartarlyProxy")

# Rate limiting settings (default: 120 requests per 60 seconds per IP)
RATE_LIMIT_REQUESTS = int(os.environ.get("RATE_LIMIT_REQUESTS", 120))
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", 60))
ip_request_history = {}
_rate_limit_cleanup_counter = 0

def get_client_ip(request: Request) -> str:
    """Extracts the real client IP address, handling reverse proxies."""
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def is_rate_limited(ip: str) -> bool:
    """Checks if the given IP address has exceeded the rate limit."""
    global _rate_limit_cleanup_counter
    now = time.time()
    if ip not in ip_request_history:
        ip_request_history[ip] = []
    
    # Filter out timestamps older than the sliding window
    cutoff = now - RATE_LIMIT_WINDOW
    ip_request_history[ip] = [t for t in ip_request_history[ip] if t > cutoff]
    
    if len(ip_request_history[ip]) >= RATE_LIMIT_REQUESTS:
        return True
        
    ip_request_history[ip].append(now)
    
    # Periodically sweep inactive IP entries to prevent unbounded dict growth.
    # Every 1000 requests, remove IPs with no activity in the last window.
    _rate_limit_cleanup_counter += 1
    if _rate_limit_cleanup_counter >= 1000:
        _rate_limit_cleanup_counter = 0
        stale = [k for k, v in ip_request_history.items() if not v]
        for k in stale:
            del ip_request_history[k]
    
    return False

# Increased keepalive pool limits for high concurrency
limits = httpx.Limits(max_keepalive_connections=100, max_connections=500)
http_client = httpx.AsyncClient(timeout=180.0, http2=True, limits=limits)

async def flush_increments_periodically():
    """Background task that flushes pending database metrics every 10 seconds."""
    while True:
        try:
            await asyncio.sleep(10)
            await asyncio.to_thread(db.flush_pending_increments)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in background metrics flusher: {e}")

# Use lifespan context manager (replaces deprecated @app.on_event)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    db.init_db()
    admin_user = os.environ.get("ADMIN_USERNAME", "admin")
    admin_pass = os.environ.get("ADMIN_PASSWORD", "admin_secure_pass")
    if admin_user == "admin" and admin_pass == "admin_secure_pass":
        logger.warning("WARNING: Running with default admin credentials. Please set ADMIN_USERNAME and ADMIN_PASSWORD in environment variables.")
    
    # Start background metrics flusher
    flusher_task = asyncio.create_task(flush_increments_periodically())
    yield
    # --- Shutdown ---
    flusher_task.cancel()
    try:
        await flusher_task
    except asyncio.CancelledError:
        pass
    # Flush any remaining increments to disk before shutting down
    db.flush_pending_increments()
    await http_client.aclose()

app = FastAPI(title="EasySubs API Translation Proxy", lifespan=lifespan)

# Custom Gzip Middleware that automatically bypasses LLM API routes.
# Standard Gzip middleware buffers streaming responses (like SSE text/event-stream),
# which stops real-time token rendering. This subclass ensures static site pages
# still get compressed, but all API routes stream immediately.
class SafeGZipMiddleware(GZipMiddleware):
    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            path = scope.get("path", "")
            if "/v1/" in path or "/models" in path or "/api/" in path:
                # Bypass compression entirely
                await self.app(scope, receive, send)
                return
        await super().__call__(scope, receive, send)

# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(SafeGZipMiddleware, minimum_size=1000)

TARGET_HOST = "api.quatarly.cloud"

# Request models
class LoginRequest(BaseModel):
    username: str
    password: str

class KeyCreateRequest(BaseModel):
    label: str
    quarterly_key: str

# Helper to verify admin session (async to avoid thread blocks)
async def is_authenticated(request: Request) -> bool:
    session_id = request.cookies.get("admin_session")
    return await asyncio.to_thread(db.validate_session, session_id)

# ----------------- ADMIN UI ROUTERS -----------------

@app.get("/", response_class=HTMLResponse)
async def root_redirect(request: Request):
    if await is_authenticated(request):
        return RedirectResponse(url="/dashboard")
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
async def get_login_page(request: Request):
    if await is_authenticated(request):
        return RedirectResponse(url="/dashboard")
    
    file_path = os.path.join(os.path.dirname(__file__), "static", "login.html")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h2>Login template not found.</h2>", status_code=404)

@app.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard_page(request: Request):
    if not await is_authenticated(request):
        return RedirectResponse(url="/login")
    
    file_path = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h2>Dashboard template not found.</h2>", status_code=404)

# ----------------- ADMIN API ENDPOINTS -----------------

@app.post("/api/admin/login")
async def admin_login(payload: LoginRequest, request: Request, response: Response):
    client_ip = get_client_ip(request)
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too many login attempts. Please try again later.")
        
    expected_user = os.environ.get("ADMIN_USERNAME", "admin")
    expected_pass = os.environ.get("ADMIN_PASSWORD", "admin_secure_pass")
    
    if payload.username == expected_user and payload.password == expected_pass:
        session_id = await asyncio.to_thread(db.create_session)
        # Set session cookie (HttpOnly for security)
        is_secure = request.headers.get("x-forwarded-proto", "http") == "https"
        response.set_cookie(
            key="admin_session",
            value=session_id,
            httponly=True,
            samesite="lax",
            secure=is_secure,
            max_age=86400 * 7 # 7 days
        )
        return {"success": True, "message": "Authenticated successfully"}
    else:
        raise HTTPException(status_code=401, detail="Invalid username or password")

@app.post("/api/admin/logout")
async def admin_logout(request: Request, response: Response):
    session_id = request.cookies.get("admin_session")
    if session_id:
        await asyncio.to_thread(db.delete_session, session_id)
    response.delete_cookie("admin_session")
    return {"success": True, "message": "Logged out successfully"}

@app.get("/api/admin/keys")
async def get_keys(request: Request):
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized admin session")
    
    keys = await asyncio.to_thread(db.get_all_keys)
    # Mask Quarterly key for security on the UI
    for k in keys:
        if len(k["quarterly_key"]) > 10:
            k["quarterly_key"] = k["quarterly_key"][:7] + "..." + k["quarterly_key"][-4:]
    return keys

@app.post("/api/admin/keys")
async def create_key(request: Request, payload: KeyCreateRequest):
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized admin session")
    
    if not payload.label.strip() or not payload.quarterly_key.strip():
        raise HTTPException(status_code=400, detail="Label and Quarterly key are required")
        
    try:
        new_key = await asyncio.to_thread(db.add_api_key, payload.label, payload.quarterly_key)
        return new_key
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/api/admin/keys/{key_id}/toggle")
async def toggle_key(key_id: int, request: Request):
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized admin session")
        
    updated = await asyncio.to_thread(db.toggle_key_status, key_id)
    if not updated:
        raise HTTPException(status_code=404, detail="API key not found")
    return updated

@app.delete("/api/admin/keys/{key_id}")
async def delete_key(key_id: int, request: Request):
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized admin session")
        
    success = await asyncio.to_thread(db.delete_key, key_id)
    if not success:
        raise HTTPException(status_code=404, detail="API key not found")
    return {"success": True, "message": "API key deleted"}

@app.get("/api/admin/debug_stream")
async def get_debug_stream():
    """Endpoint to fetch the last 100 stream chunk arrival timestamps."""
    return {"logs": debug_logs}

# ----------------- PROXY CORE LOGIC -----------------

def sanitize_json(obj: Any) -> Any:
    """Recursively converts any numeric 'id' field in a JSON object to a string."""
    if obj is None:
        return None
    if isinstance(obj, (int, float, str, bool)):
        return obj
    if isinstance(obj, list):
        return [sanitize_json(item) for item in obj]
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            if k == 'id' and isinstance(v, (int, float)):
                new_dict[k] = str(v)
            else:
                new_dict[k] = sanitize_json(v)
        return new_dict
    return obj

class ToolCallIndexMapper:
    """Fixes the Quatarly streaming index mismatch bug by mapping incoming tool call indices to sequential ones."""
    def __init__(self):
        self.incoming_to_mapped = {}
        self.last_mapped = 0

    def map_index(self, incoming_index: int, has_id: bool) -> int:
        if incoming_index in self.incoming_to_mapped:
            return self.incoming_to_mapped[incoming_index]
        
        if has_id:
            # It's a new tool call block
            mapped = len(self.incoming_to_mapped)
            self.incoming_to_mapped[incoming_index] = mapped
            self.last_mapped = mapped
            return mapped
        else:
            # Continuation of the last active tool call block
            self.incoming_to_mapped[incoming_index] = self.last_mapped
            return self.last_mapped

def map_chunk_tool_calls(chunk: dict, mapper: ToolCallIndexMapper) -> dict:
    """Updates index on tool calls inside choice deltas using the mapper."""
    if not isinstance(chunk, dict):
        return chunk
    
    if "choices" in chunk and isinstance(chunk["choices"], list):
        for choice in chunk["choices"]:
            if "delta" in choice and isinstance(choice["delta"], dict):
                delta = choice["delta"]
                if "tool_calls" in delta and isinstance(delta["tool_calls"], list):
                    for tool_call in delta["tool_calls"]:
                        if isinstance(tool_call, dict) and "index" in tool_call:
                            incoming_idx = tool_call["index"]
                            has_id = "id" in tool_call and tool_call["id"] is not None
                            mapped_idx = mapper.map_index(incoming_idx, has_id)
                            tool_call["index"] = mapped_idx
    return chunk

def parse_xml_tool_calls(xml_str: str) -> list[dict]:
    """Helper to robustly parse Claude's thinking-mode XML tool calls."""
    # Match all <invoke name="...">...</invoke> blocks
    invokes = re.findall(r'<invoke name="([^"]+)"\s*>(.*?)</invoke>', xml_str, re.DOTALL)
    results = []
    for name, content in invokes:
        # Match all <parameter name="...">...</parameter> blocks inside this invoke
        params = re.findall(r'<parameter name="([^"]+)"\s*>(.*?)</parameter>', content, re.DOTALL)
        arguments = {}
        for param_name, param_value in params:
            val = html.unescape(param_value.strip())
            try:
                # If value is stringified JSON (array/object), load it
                arguments[param_name] = json.loads(val)
            except Exception:
                arguments[param_name] = val
        results.append({
            "name": name,
            "arguments": arguments
        })
    return results

class XMLToJSONConverter:
    """Detects, buffers, and translates Claude's XML function calls into standard JSON chunks."""
    def __init__(self, is_openai_format: bool = True):
        self.is_openai_format = is_openai_format
        self.in_xml = False
        self.text_buffer = ""
        self.xml_buffer = ""
        self.tool_call_index = 0

    def process_chunk_text(self, text: str) -> tuple[str, list[dict]]:
        """Processes incoming text chunk. Returns tuple of (clean_text_to_yield, list_of_tool_call_chunks)."""
        if not self.in_xml:
            self.text_buffer += text
            
            # Find the first '<' in the buffer
            idx = self.text_buffer.find("<")
            if idx == -1:
                # No '<' found, flush the entire text buffer immediately (zero latency)
                to_yield = self.text_buffer
                self.text_buffer = ""
                return to_yield, []
            else:
                # Found a '<'. Check if the text matches the prefix of "<function_calls>"
                slice_to_check = self.text_buffer[idx:]
                target = "<function_calls>"
                
                if target.startswith(slice_to_check):
                    # We are potentially building the tag, yield the prefix and hold the rest
                    to_yield = self.text_buffer[:idx]
                    self.text_buffer = slice_to_check
                    return to_yield, []
                elif "<function_calls" in self.text_buffer:
                    # Tag completed, split it
                    parts = self.text_buffer.split("<function_calls", 1)
                    text_to_yield = parts[0]
                    self.in_xml = True
                    self.xml_buffer = ""
                    rest = parts[1]
                    if ">" in rest:
                        self.xml_buffer = rest.split(">", 1)[1]
                    else:
                        self.xml_buffer = rest
                    self.text_buffer = ""
                    
                    # Immediately process the xml_buffer in case the tag closing is already present
                    xml_text, xml_chunks = self.process_chunk_text("")
                    return text_to_yield + xml_text, xml_chunks
                else:
                    # Not a tag match, flush the buffer
                    to_yield = self.text_buffer
                    self.text_buffer = ""
                    return to_yield, []
        else:
            self.xml_buffer += text
            if "</function_calls>" in self.xml_buffer:
                self.in_xml = False
                parts = self.xml_buffer.split("</function_calls>", 1)
                xml_to_parse = parts[0]
                remaining_text = parts[1]
                
                # Parse the complete XML block and generate output JSON chunks
                tool_calls = parse_xml_tool_calls(xml_to_parse)
                chunks = self.generate_tool_call_chunks(tool_calls)
                
                self.xml_buffer = ""
                self.text_buffer = ""
                
                # Recursively process any remaining text
                rem_text, rem_chunks = self.process_chunk_text(remaining_text)
                return rem_text, chunks + rem_chunks
            else:
                return "", []

    def flush(self, message_id: str = None) -> tuple[str, list[dict]]:
        """Flushes any remaining text when the stream ends."""
        if not self.in_xml:
            to_yield = self.text_buffer
            self.text_buffer = ""
            return to_yield, []
        else:
            # Stream ended inside XML mode (malformed tag). Return it as raw text.
            raw_text = "<function_calls" + self.xml_buffer
            self.in_xml = False
            self.xml_buffer = ""
            return raw_text, []

    def generate_tool_call_chunks(self, tool_calls: list[dict], message_id: str = None) -> list[dict]:
        chunks = []
        if not tool_calls:
            return chunks

        if self.is_openai_format:
            openai_tool_calls = []
            for i, tc in enumerate(tool_calls):
                openai_tool_calls.append({
                    "index": self.tool_call_index + i,
                    "id": f"call_{secrets.token_hex(12)}",
                    "type": "function",
                    "function": {
                        "name": tc["name"],
                        "arguments": json.dumps(tc["arguments"])
                    }
                })
            self.tool_call_index += len(tool_calls)
            
            chunks.append({
                "id": message_id or f"chatcmpl-{secrets.token_hex(12)}",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "choices": [{
                    "index": 0,
                    "delta": {
                        "tool_calls": openai_tool_calls
                    },
                    "finish_reason": None
                }]
            })
        else:
            # Anthropic tool use chunks sequence
            for tc in tool_calls:
                call_id = f"toolu_{secrets.token_hex(12)}"
                block_index = self.tool_call_index
                self.tool_call_index += 1
                
                chunks.append({
                    "type": "content_block_start",
                    "index": block_index,
                    "content_block": {
                        "type": "tool_use",
                        "id": call_id,
                        "name": tc["name"],
                        "input": {}
                    }
                })
                chunks.append({
                    "type": "content_block_delta",
                    "index": block_index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": json.dumps(tc["arguments"])
                    }
                })
                chunks.append({
                    "type": "content_block_stop",
                    "index": block_index
                })
        return chunks

def convert_xml_to_json_non_streaming(response_json: dict) -> dict:
    """Intercepts and parses XML tool calls for non-streaming completions."""
    if not isinstance(response_json, dict):
        return response_json
        
    # 1. OpenAI Format
    if "choices" in response_json and isinstance(response_json["choices"], list) and len(response_json["choices"]) > 0:
        choice = response_json["choices"][0]
        if "message" in choice and isinstance(choice["message"], dict):
            message = choice["message"]
            content = message.get("content") or ""
            if "<function_calls" in content and "</function_calls>" in content:
                parts = content.split("<function_calls", 1)
                text_before = parts[0]
                rest = parts[1].split("</function_calls>", 1)
                xml_to_parse = rest[0].split(">", 1)[1] if ">" in rest[0] else rest[0]
                text_after = rest[1] if len(rest) > 1 else ""
                
                tool_calls = parse_xml_tool_calls(xml_to_parse)
                if tool_calls:
                    message["content"] = (text_before + text_after).strip() or None
                    openai_tool_calls = []
                    for i, tc in enumerate(tool_calls):
                        openai_tool_calls.append({
                            "id": f"call_{secrets.token_hex(12)}",
                            "type": "function",
                            "function": {
                                "name": tc["name"],
                                "arguments": json.dumps(tc["arguments"])
                            }
                        })
                    message["tool_calls"] = openai_tool_calls
                    
    # 2. Anthropic Format
    elif "content" in response_json and isinstance(response_json["content"], list):
        new_content = []
        for item in response_json["content"]:
            if isinstance(item, dict) and item.get("type") == "text":
                content = item.get("text") or ""
                if "<function_calls" in content and "</function_calls>" in content:
                    parts = content.split("<function_calls", 1)
                    text_before = parts[0]
                    rest = parts[1].split("</function_calls>", 1)
                    xml_to_parse = rest[0].split(">", 1)[1] if ">" in rest[0] else rest[0]
                    text_after = rest[1] if len(rest) > 1 else ""
                    
                    if text_before.strip():
                        new_content.append({"type": "text", "text": text_before.strip()})
                        
                    tool_calls = parse_xml_tool_calls(xml_to_parse)
                    for tc in tool_calls:
                        new_content.append({
                            "type": "tool_use",
                            "id": f"toolu_{secrets.token_hex(12)}",
                            "name": tc["name"],
                            "input": tc["arguments"]
                        })
                        
                    if text_after.strip():
                        new_content.append({"type": "text", "text": text_after.strip()})
                else:
                    new_content.append(item)
            else:
                new_content.append(item)
        response_json["content"] = new_content
        
    return response_json

# Models endpoints (OpenAI-compatible)
@app.get("/v1/models")
@app.get("/models")
async def get_models(request: Request):
    client_ip = get_client_ip(request)
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too Many Requests: Rate limit exceeded.")
        
    # 1. Authenticate proxy key via Bearer token or x-api-key
    # Starlette normalizes all header names to lowercase, so .get() is already case-insensitive.
    proxy_key = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        proxy_key = auth_header[7:].strip()
    else:
        x_api_key = request.headers.get("x-api-key")
        if x_api_key:
            proxy_key = x_api_key.strip()
                    
    if not proxy_key:
        raise HTTPException(status_code=401, detail="Unauthorized: Missing or invalid Bearer token or X-API-Key.")

    
    key_mapping = await asyncio.to_thread(db.get_key_by_proxy_key, proxy_key)
    if not key_mapping:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid, inactive, or revoked API key.")
        
    db.increment_request_count(proxy_key)
    
    # 2. Try to fetch dynamically from Quarterly
    target_url = f"https://{TARGET_HOST}/v1/models"
    headers = {
        "authorization": f"Bearer {key_mapping['quarterly_key']}",
        "x-api-key": key_mapping['quarterly_key']
    }
    
    try:
        response = await http_client.get(target_url, headers=headers)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.warning(f"Failed to fetch models from Quarterly: {e}. Falling back to static list.")
        
    # 3. Fallback static list of models from IMPPP.txt (OpenAI-compatible)
    fallback_models = [
        {"id": "claude-haiku-4-5-20251001", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-opus-4-6", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-opus-4-6-thinking", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-opus-4-7", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-opus-4-7-thinking", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-opus-4-8", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-opus-4-8-thinking", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-sonnet-4-6", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-sonnet-4-6-20250929", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "claude-sonnet-4-6-thinking", "object": "model", "created": 1700000000, "owned_by": "anthropic"},
        {"id": "gemini-3.1-pro", "object": "model", "created": 1700000000, "owned_by": "google"},
        {"id": "gemini-3.1-pro-low", "object": "model", "created": 1700000000, "owned_by": "google"},
    ]
    return {"object": "list", "data": fallback_models}

# Catch-all Route: translates proxy API keys to real Quarterly keys and forwards the requests
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy_request(request: Request, path: str):
    client_ip = get_client_ip(request)
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too Many Requests: Rate limit exceeded.")
        
    # 1. Authenticate proxy key via Bearer token or x-api-key
    # Starlette normalizes all header names to lowercase, so .get() is already case-insensitive.
    proxy_key = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        proxy_key = auth_header[7:].strip()
    else:
        x_api_key = request.headers.get("x-api-key")
        if x_api_key:
            proxy_key = x_api_key.strip()
                    
    if not proxy_key:
        raise HTTPException(status_code=401, detail="Unauthorized: Missing or invalid Bearer token or X-API-Key.")
    
    # 2. Lookup Quarterly key mapping
    key_mapping = await asyncio.to_thread(db.get_key_by_proxy_key, proxy_key)
    if not key_mapping:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid, inactive, or revoked API key.")
    
    # 3. Log traffic metrics (in-memory update)
    db.increment_request_count(proxy_key)
    
    # Construct target URL
    target_url = f"https://{TARGET_HOST}/{path}"
    
    # Forward headers safely (filter out hop-by-hop, auto-managed, and HTTP/2 pseudo-headers)
    headers = {}
    for k, v in request.headers.items():
        k_lower = k.lower()
        # HTTP/2 pseudo-headers (starting with ':'), auth keys, and hop-by-hop headers must not be forwarded
        if k_lower.startswith(":") or k_lower in [
            "host",
            "connection",
            "content-length",
            "accept-encoding",
            "content-encoding",
            "transfer-encoding",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailers",
            "upgrade",
            "proxy-connection",
            "x-api-key",       # Filter out client's x-api-key
            "authorization"    # Filter out client's bearer token
        ]:
            continue
        headers[k] = v

    # 4. Inject translated real Quarterly API Key in both formats (OpenAI & Anthropic)
    headers["authorization"] = f"Bearer {key_mapping['quarterly_key']}"
    headers["x-api-key"] = key_mapping['quarterly_key']

    # Read body
    body = await request.body()
    
    # Forward method
    method = request.method
    
    logger.info(f"Key '{key_mapping['label']}' forwarding {method} to: {target_url}")
    
    async def stream_generator(response: httpx.Response) -> Any:
        """Yields SSE lines to the client and ensures the upstream response is always closed."""
        mapper = ToolCallIndexMapper()
        converter = None
        is_openai = True
        last_message_id = None
        
        try:
            async for line in response.aiter_lines():
                if not line:
                    continue
                
                # Debug log chunk arrival times to isolate buffering
                log_msg = f"STREAM_DEBUG: Received line of length {len(line)} at {time.time()}"
                logger.warning(log_msg)
                global debug_logs
                debug_logs.append(log_msg)
                if len(debug_logs) > 100:
                    debug_logs.pop(0)
                
                if line.startswith("data:"):
                    data_str = line[5:].strip()
                    if data_str == "[DONE]":
                        # Flush the converter before ending the stream
                        if converter:
                            rem_text, rem_chunks = converter.flush(message_id=last_message_id)
                            if rem_text:
                                if is_openai:
                                    flush_chunk = {
                                        "id": last_message_id or f"chatcmpl-{secrets.token_hex(12)}",
                                        "object": "chat.completion.chunk",
                                        "created": int(time.time()),
                                        "choices": [{
                                            "index": 0,
                                            "delta": {"content": rem_text},
                                            "finish_reason": None
                                        }]
                                    }
                                else:
                                    flush_chunk = {
                                        "type": "content_block_delta",
                                        "index": 0,
                                        "delta": {
                                            "type": "text_delta",
                                            "text": rem_text
                                        }
                                    }
                                yield f"data: {json.dumps(flush_chunk)}\n\n".encode("utf-8")
                            
                            for c in rem_chunks:
                                yield f"data: {json.dumps(c)}\n\n".encode("utf-8")
                        
                        yield f"{line}\n\n".encode("utf-8")
                        continue
                        
                    try:
                        chunk = json.loads(data_str)
                        sanitized = sanitize_json(chunk)
                        sanitized = map_chunk_tool_calls(sanitized, mapper)
                        
                        # Initialize converter on first chunk
                        if converter is None:
                            is_openai = "choices" in sanitized
                            converter = XMLToJSONConverter(is_openai_format=is_openai)
                        
                        if is_openai:
                            last_message_id = sanitized.get("id") or last_message_id
                            
                        # Extract and process text delta
                        text = ""
                        if is_openai:
                            if "choices" in sanitized and len(sanitized["choices"]) > 0:
                                choice = sanitized["choices"][0]
                                if "delta" in choice and "content" in choice["delta"]:
                                    text = choice["delta"]["content"] or ""
                        else:
                            if sanitized.get("type") == "content_block_delta":
                                delta = sanitized.get("delta", {})
                                if delta.get("type") == "text_delta":
                                    text = delta.get("text") or ""
                                    
                        if text:
                            text_to_yield, chunks_to_yield = converter.process_chunk_text(text)
                            
                            # Update current chunk
                            if is_openai:
                                sanitized["choices"][0]["delta"]["content"] = text_to_yield
                            else:
                                sanitized["delta"]["text"] = text_to_yield
                                
                            # Yield modified chunk first
                            yield f"data: {json.dumps(sanitized)}\n\n".encode("utf-8")
                            
                            # Then yield any generated tool call chunks
                            for c in chunks_to_yield:
                                yield f"data: {json.dumps(c)}\n\n".encode("utf-8")
                        else:
                            yield f"data: {json.dumps(sanitized)}\n\n".encode("utf-8")
                            
                    except Exception as e:
                        logger.warning(f"Failed to parse chunk JSON: {data_str}. Error: {e}")
                        yield f"{line}\n\n".encode("utf-8")
                else:
                    yield f"{line}\n".encode("utf-8")
        finally:
            # Always close the upstream response — prevents HTTP/2 stream leaks on client disconnect
            await response.aclose()
 
    try:
        # Build request to forward using the global connection pool
        proxied_request = http_client.build_request(
            method=method,
            url=target_url,
            headers=headers,
            content=body
        )
        
        response = await http_client.send(proxied_request, stream=True)
        
        content_type = response.headers.get("content-type", "")
        if "text/event-stream" in content_type:
            # stream_generator owns the response lifecycle and closes it in its finally block
            return StreamingResponse(
                stream_generator(response),
                status_code=response.status_code,
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no"
                }
            )
        else:
            try:
                await response.aread()
                try:
                    # Attempt to parse as JSON, sanitize and convert XML to JSON
                    response_json = response.json()
                    response_json = sanitize_json(response_json)
                    response_json = convert_xml_to_json_non_streaming(response_json)
                    return JSONResponse(
                        content=response_json,
                        status_code=response.status_code
                    )
                except Exception:
                    # If not JSON, return as raw content
                    return Response(
                        content=response.content,
                        status_code=response.status_code
                    )
            finally:
                # Always close non-streaming responses to release the HTTP/2 stream
                await response.aclose()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Proxy request failed with exception:")
        raise HTTPException(status_code=502, detail=f"Proxy Error: {type(e).__name__} - {str(e)}")

if __name__ == "__main__":
    # Fetch port from environment (Railway standard)
    port = int(os.environ.get("PORT", 8005))
    logger.warning(f"Starting proxy on 0.0.0.0:{port}")
    # Silence uvicorn info logging to keep stdout clean
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
