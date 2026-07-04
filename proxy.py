import os
import json
import logging
from typing import Any
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
import httpx
import uvicorn

import db

import time

# Configure logging (only show warnings and errors)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("QuartarlyProxy")

# Rate limiting settings (default: 120 requests per 60 seconds per IP)
RATE_LIMIT_REQUESTS = int(os.environ.get("RATE_LIMIT_REQUESTS", 120))
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", 60))
ip_request_history = {}

def get_client_ip(request: Request) -> str:
    """Extracts the real client IP address, handling reverse proxies."""
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def is_rate_limited(ip: str) -> bool:
    """Checks if the given IP address has exceeded the rate limit."""
    now = time.time()
    if ip not in ip_request_history:
        ip_request_history[ip] = []
    
    # Filter out timestamps older than the sliding window
    cutoff = now - RATE_LIMIT_WINDOW
    ip_request_history[ip] = [t for t in ip_request_history[ip] if t > cutoff]
    
    if len(ip_request_history[ip]) >= RATE_LIMIT_REQUESTS:
        return True
        
    ip_request_history[ip].append(now)
    return False

app = FastAPI(title="EasySubs API Translation Proxy")

# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Compress responses larger than 1KB (JSON, HTML, etc.).
# Starlette automatically skips SSE (text/event-stream) so streaming completions are unaffected.
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Initialize a global async HTTP client with a 3-minute timeout and HTTP/2 multiplexing support
http_client = httpx.AsyncClient(timeout=180.0, http2=True)

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

TARGET_HOST = "api.quatarly.cloud"

# Initialize database on application startup
@app.on_event("startup")
def startup_event():
    db.init_db()
    # Log admin credentials warning if using defaults
    admin_user = os.environ.get("ADMIN_USERNAME", "admin")
    admin_pass = os.environ.get("ADMIN_PASSWORD", "admin_secure_pass")
    if admin_user == "admin" and admin_pass == "admin_secure_pass":
        logger.warning("WARNING: Running with default admin credentials. Please set ADMIN_USERNAME and ADMIN_PASSWORD in environment variables.")

# Request models
class LoginRequest(BaseModel):
    username: str
    password: str

class KeyCreateRequest(BaseModel):
    label: str
    quarterly_key: str

# Helper to verify admin session
def is_authenticated(request: Request) -> bool:
    session_id = request.cookies.get("admin_session")
    return db.validate_session(session_id)

# ----------------- ADMIN UI ROUTERS -----------------

@app.get("/", response_class=HTMLResponse)
async def root_redirect(request: Request):
    if is_authenticated(request):
        return RedirectResponse(url="/dashboard")
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
async def get_login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse(url="/dashboard")
    
    file_path = os.path.join(os.path.dirname(__file__), "static", "login.html")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h2>Login template not found.</h2>", status_code=404)

@app.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard_page(request: Request):
    if not is_authenticated(request):
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
        session_id = db.create_session()
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
        db.delete_session(session_id)
    response.delete_cookie("admin_session")
    return {"success": True, "message": "Logged out successfully"}

@app.get("/api/admin/keys")
async def get_keys(request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized admin session")
    
    keys = db.get_all_keys()
    # Mask Quarterly key for security on the UI
    for k in keys:
        if len(k["quarterly_key"]) > 10:
            k["quarterly_key"] = k["quarterly_key"][:7] + "..." + k["quarterly_key"][-4:]
    return keys

@app.post("/api/admin/keys")
async def create_key(request: Request, payload: KeyCreateRequest):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized admin session")
    
    if not payload.label.strip() or not payload.quarterly_key.strip():
        raise HTTPException(status_code=400, detail="Label and Quarterly key are required")
        
    try:
        new_key = db.add_api_key(payload.label, payload.quarterly_key)
        return new_key
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/api/admin/keys/{key_id}/toggle")
async def toggle_key(key_id: int, request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized admin session")
        
    updated = db.toggle_key_status(key_id)
    if not updated:
        raise HTTPException(status_code=404, detail="API key not found")
    return updated

@app.delete("/api/admin/keys/{key_id}")
async def delete_key(key_id: int, request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized admin session")
        
    success = db.delete_key(key_id)
    if not success:
        raise HTTPException(status_code=404, detail="API key not found")
    return {"success": True, "message": "API key deleted"}

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

# Models endpoints (OpenAI-compatible)
@app.get("/v1/models")
@app.get("/models")
async def get_models(request: Request):
    client_ip = get_client_ip(request)
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too Many Requests: Rate limit exceeded.")
        
    # 1. Authenticate proxy key via Bearer token or x-api-key
    proxy_key = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        proxy_key = auth_header[7:].strip()
    else:
        x_api_key = request.headers.get("x-api-key")
        if x_api_key:
            proxy_key = x_api_key.strip()
        else:
            # Case insensitive check
            for k, v in request.headers.items():
                if k.lower() == "x-api-key":
                    proxy_key = v.strip()
                    break
                    
    if not proxy_key:
        raise HTTPException(status_code=401, detail="Unauthorized: Missing or invalid Bearer token or X-API-Key.")
    
    key_mapping = db.get_key_by_proxy_key(proxy_key)
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
        {"id": "gpt-5.4", "object": "model", "created": 1700000000, "owned_by": "openai"},
        {"id": "gpt-5.5", "object": "model", "created": 1700000000, "owned_by": "openai"},
    ]
    return {"object": "list", "data": fallback_models}

# Catch-all Route: translates proxy API keys to real Quarterly keys and forwards the requests
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy_request(request: Request, path: str):
    client_ip = get_client_ip(request)
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too Many Requests: Rate limit exceeded.")
        
    # 1. Authenticate proxy key via Bearer token or x-api-key
    proxy_key = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        proxy_key = auth_header[7:].strip()
    else:
        x_api_key = request.headers.get("x-api-key")
        if x_api_key:
            proxy_key = x_api_key.strip()
        else:
            # Case insensitive check
            for k, v in request.headers.items():
                if k.lower() == "x-api-key":
                    proxy_key = v.strip()
                    break
                    
    if not proxy_key:
        raise HTTPException(status_code=401, detail="Unauthorized: Missing or invalid Bearer token or X-API-Key.")
    
    # 2. Lookup Quarterly key mapping
    key_mapping = db.get_key_by_proxy_key(proxy_key)
    if not key_mapping:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid, inactive, or revoked API key.")
    
    # 3. Log traffic metrics
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
        mapper = ToolCallIndexMapper()
        async for line in response.aiter_lines():
            if not line:
                continue
            
            if line.startswith("data:"):
                data_str = line[5:].strip()
                if data_str == "[DONE]":
                    yield f"{line}\n\n".encode("utf-8")
                    continue
                try:
                    chunk = json.loads(data_str)
                    sanitized = sanitize_json(chunk)
                    sanitized = map_chunk_tool_calls(sanitized, mapper)
                    yield f"data: {json.dumps(sanitized)}\n\n".encode("utf-8")
                except Exception as e:
                    logger.warning(f"Failed to parse chunk JSON: {data_str}. Error: {e}")
                    yield f"{line}\n\n".encode("utf-8")
            else:
                yield f"{line}\n".encode("utf-8")
 
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
            return StreamingResponse(
                stream_generator(response),
                status_code=response.status_code
            )
        else:
            await response.aread()
            try:
                # Attempt to parse as JSON and sanitize
                response_json = response.json()
                response_json = sanitize_json(response_json)
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
    except Exception as e:
        logger.exception("Proxy request failed with exception:")
        raise HTTPException(status_code=502, detail=f"Proxy Error: {type(e).__name__} - {str(e)}")

if __name__ == "__main__":
    # Fetch port from environment (Railway standard)
    port = int(os.environ.get("PORT", 8005))
    logger.warning(f"Starting proxy on 0.0.0.0:{port}")
    # Silence uvicorn info logging to keep stdout clean
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
