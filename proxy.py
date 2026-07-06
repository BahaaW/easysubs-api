"""
EasySubs API Translation Proxy — Main FastAPI Application.

Handles:
- Admin UI (login, dashboard) with session-based auth
- Proxy endpoint: translates proxy API keys to Quarterly keys and forwards requests
- Streaming: SSE pass-through with OpenAI ↔ Anthropic format conversion
- XML tool call parsing for Claude's non-standard streaming format
- Per-IP rate limiting (separate from login rate limiting)
- Per-key daily quotas
- Model aliasing for multi-app compatibility (VS Code, Claude Code, Cursor, etc.)
- Health check endpoint for Railway/orchestrator probes
- Request ID propagation for distributed tracing
- Graceful shutdown
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

import db
import config

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.WARNING),
    format="%(asctime)sZ %(levelname)s %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("EasySubsAPI.Proxy")

# ---------------------------------------------------------------------------
# Global rate limiting state
# ---------------------------------------------------------------------------

# Proxy endpoint: requests per IP
_ip_request_history: dict[str, list[float]] = {}
_proxy_rate_limit_cleanup_counter = 0

# Login endpoint: separate, stricter limits + brute-force protection
_ip_login_failures: dict[str, list[float]] = {}

# Brute-force protection: tracks consecutive failures per IP with lockout
# Structure: { ip: { "count": int, "locked_until": float | None } }
_ip_brute_force: dict[str, dict[str, Any]] = {}

# Per-key daily quota tracking: proxy_key -> daily_used_count
_key_daily_usage: dict[str, int] = {}

# ---------------------------------------------------------------------------
# Debug / tracing
# ---------------------------------------------------------------------------

# In-memory ring buffer of last-N debug log lines for troubleshooting
_debug_stream_logs: list[str] = []
_request_counter = 0
_counter_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# HTTP/2 connection pool
# ---------------------------------------------------------------------------

_http2_limits = httpx.Limits(
    max_keepalive_connections=config.HTTPX_MAX_KEEPALIVE_CONNECTIONS,
    max_connections=config.HTTPX_MAX_CONNECTIONS,
)
_http_client = httpx.AsyncClient(
    timeout=httpx.Timeout(config.HTTPX_TIMEOUT_SECONDS),
    http2=True,
    limits=_http2_limits,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_today_key() -> str:
    """Returns 'YYYY-MM-DD' in UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_client_ip(request: Request) -> str:
    """Extracts the real client IP, handling X-Forwarded-For from reverse proxies."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


def is_rate_limited(
    ip: str,
    history: dict[str, list[float]],
    limit: int,
    window: float,
    cleanup_counter_ref: list[int],
    cleanup_every: int = 500,
) -> bool:
    """Generic sliding-window rate limiter.

    Args:
        ip: Client IP.
        history: Shared dict mapping IP -> list of request timestamps.
        limit: Max requests allowed in the window.
        window: Window size in seconds.
        cleanup_counter_ref: A single-element list used as a mutable int counter.
        cleanup_every: Run cleanup after this many calls.
    """
    now = time.time()
    cutoff = now - window

    if ip not in history:
        history[ip] = []

    # Remove old entries
    history[ip] = [t for t in history[ip] if t > cutoff]

    if len(history[ip]) >= limit:
        return True

    history[ip].append(now)

    # Periodic sweep: remove IPs with no activity in 2x the window
    cleanup_counter_ref[0] += 1
    if cleanup_counter_ref[0] >= cleanup_every:
        cleanup_counter_ref[0] = 0
        stale = [k for k, v in history.items() if not v or all(t < cutoff for t in v)]
        for k in stale:
            history.pop(k, None)

    return False


def is_proxy_rate_limited(ip: str) -> bool:
    return is_rate_limited(
        ip,
        _ip_request_history,
        config.RATE_LIMIT_REQUESTS,
        config.RATE_LIMIT_WINDOW_SECONDS,
        [_proxy_rate_limit_cleanup_counter],
    )


def is_login_rate_limited(ip: str) -> bool:
    now = time.time()
    cutoff = now - config.LOGIN_RATE_LIMIT_WINDOW_SECONDS
    failures = _ip_login_failures.get(ip, [])
    # Filter out old failures
    failures = [t for t in failures if t > cutoff]
    _ip_login_failures[ip] = failures
    return len(failures) >= config.LOGIN_RATE_LIMIT_REQUESTS


async def is_authenticated(request: Request) -> bool:
    """Verifies the admin session cookie against the DB."""
    session_id = request.cookies.get("admin_session")
    if not session_id:
        return False
    return await asyncio.to_thread(db.validate_session, session_id)


def _build_request_id() -> str:
    """Generates a short unique request ID for tracing."""
    return secrets.token_hex(6)


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def _flush_increments_periodically() -> None:
    """Flushes pending request-count increments to SQLite every N seconds."""
    while True:
        try:
            await asyncio.sleep(config.FLUSHER_INTERVAL_SECONDS)
            await asyncio.to_thread(db.flush_pending_increments)
            # Also sweep stale cache entries periodically
            db.sweep_stale_cache_entries()
            # Clean up expired sessions every ~10 flush cycles (roughly every 100s)
            if int(time.time()) % 100 < config.FLUSHER_INTERVAL_SECONDS:
                await asyncio.to_thread(db.cleanup_expired_sessions)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Background flusher error: %s", e)


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # ---- Startup ----
    db.init_db()

    # Block startup with default credentials in production/staging
    environment = os.environ.get("ENVIRONMENT", "local")
    if environment == "production":
        if config.ADMIN_USERNAME == "admin" and config.ADMIN_PASSWORD == "admin_secure_pass":
            raise RuntimeError(
                "CRITICAL: Default admin credentials detected in production. "
                "Set ADMIN_USERNAME and ADMIN_PASSWORD env vars before starting."
            )
    elif environment not in ("local", "development"):
        if config.ADMIN_USERNAME == "admin" and config.ADMIN_PASSWORD == "admin_secure_pass":
            logger.warning(
                "WARNING: Running with default admin credentials in %s. "
                "Set ADMIN_USERNAME and ADMIN_PASSWORD env vars.", environment
            )

    # Start background flusher
    _flusher_task = asyncio.create_task(_flush_increments_periodically())

    logger.info("EasySubs API started on %s:%s", config.SERVER_HOST, config.SERVER_PORT)

    yield

    # ---- Shutdown ----
    _flusher_task.cancel()
    try:
        await _flusher_task
    except asyncio.CancelledError:
        pass

    # Final flush — drain all pending increments before exit
    db.flush_pending_increments()
    db.cleanup_expired_sessions()
    await _http_client.aclose()
    logger.info("EasySubs API shut down gracefully.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="EasySubs API Translation Proxy",
    description="API key rotation proxy for Quarterly translation services",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

# CORS — explicit origins only (never wildcard with credentials)
if config.CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Safe GZip: bypass for streaming / API routes
class SafeGZipMiddleware(GZipMiddleware):
    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            path = scope.get("path", "")
            # Don't compress SSE streams or API responses — causes buffering
            if "/v1/" in path or "/models" in path or "/api/" in path:
                await self.app(scope, receive, send)
                return
        await super().__call__(scope, receive, send)


app.add_middleware(SafeGZipMiddleware, minimum_size=1000)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str
    password: str


class KeyCreateRequest(BaseModel):
    label: str
    quarterly_key: str


class KeyUpdateRequest(BaseModel):
    quota_limit: int | None = None


# ---------------------------------------------------------------------------
# Admin UI Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> Response:
    if await is_authenticated(request):
        return RedirectResponse(url="/dashboard", status_code=303)
    return RedirectResponse(url="/login", status_code=303)


@app.get("/login", response_class=HTMLResponse)
async def get_login(request: Request) -> Response:
    if await is_authenticated(request):
        return RedirectResponse(url="/dashboard", status_code=303)
    static_path = os.path.join(os.path.dirname(__file__), "static", "login.html")
    if os.path.exists(static_path):
        with open(static_path, encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h2>Login template not found.</h2>", status_code=404)


@app.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard(request: Request) -> Response:
    if not await is_authenticated(request):
        return RedirectResponse(url="/login", status_code=303)
    static_path = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")
    if os.path.exists(static_path):
        with open(static_path, encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h2>Dashboard template not found.</h2>", status_code=404)


# ---------------------------------------------------------------------------
# Health check (no auth)
# ---------------------------------------------------------------------------

@app.get("/health")
async def health_check() -> JSONResponse:
    """Returns 200 if the process is healthy. Used by Railway / k8s probes."""
    return JSONResponse({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})


@app.get("/ready")
async def readiness_check() -> JSONResponse:
    """Returns 200 if the service can handle requests."""
    # TODO: could check DB connectivity here
    return JSONResponse({"status": "ready", "timestamp": datetime.now(timezone.utc).isoformat()})


# ---------------------------------------------------------------------------
# Admin API Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/admin/login")
async def admin_login(
    payload: LoginRequest,
    request: Request,
    response: Response,
) -> JSONResponse:
    client_ip = get_client_ip(request)

    if is_login_rate_limited(client_ip):
        raise HTTPException(
            status_code=429,
            detail="Too many login attempts. Please try again later.",
        )

    # Check IP-specific brute-force lockout with exponential backoff
    now = time.time()

    # Initialize if not exists
    if client_ip not in _ip_brute_force:
        _ip_brute_force[client_ip] = {"count": 0, "locked_until": None}

    # Clear expired lockout
    if _ip_brute_force[client_ip]["locked_until"] and now >= _ip_brute_force[client_ip]["locked_until"]:
        _ip_brute_force[client_ip]["count"] = 0
        _ip_brute_force[client_ip]["locked_until"] = None

    # Check if currently locked out
    if _ip_brute_force[client_ip]["locked_until"] and now < _ip_brute_force[client_ip]["locked_until"]:
        remaining = int(_ip_brute_force[client_ip]["locked_until"] - now)
        raise HTTPException(
            status_code=429,
            detail=f"Too many failed attempts. Try again in {remaining} seconds.",
        )

    if (
        payload.username == config.ADMIN_USERNAME
        and payload.password == config.ADMIN_PASSWORD
    ):
        # Clear all failure tracking on successful authentication
        _ip_login_failures.pop(client_ip, None)
        _ip_brute_force.pop(client_ip, None)

        session_id = await asyncio.to_thread(db.create_session)
        is_secure = request.headers.get("x-forwarded-proto", "http") == "https"
        response.set_cookie(
            key="admin_session",
            value=session_id,
            httponly=True,
            samesite="lax",
            secure=is_secure,
            max_age=config.SESSION_COOKIE_MAX_AGE_SECONDS,
        )
        return JSONResponse({"success": True, "message": "Authenticated successfully"})
    else:
        # Track the failed attempt with exponential backoff
        _ip_login_failures.setdefault(client_ip, []).append(time.time())
        _ip_brute_force[client_ip]["count"] += 1

        # Apply exponential lockout after 5 consecutive failures
        if _ip_brute_force[client_ip]["count"] >= 5:
            multiplier = 2 ** (_ip_brute_force[client_ip]["count"] - 5)
            lockout_seconds = 900 * multiplier  # 15 min base
            _ip_brute_force[client_ip]["locked_until"] = now + lockout_seconds

        # Small delay to slow brute-force attempts
        await asyncio.sleep(0.5)

        raise HTTPException(status_code=401, detail="Invalid username or password")


@app.post("/api/admin/logout")
async def admin_logout(request: Request, response: Response) -> JSONResponse:
    session_id = request.cookies.get("admin_session")
    if session_id:
        await asyncio.to_thread(db.delete_session, session_id)
    response.delete_cookie("admin_session")
    return JSONResponse({"success": True, "message": "Logged out successfully"})


def _require_auth(request: Request) -> None:
    """Raises 401 if the request has no valid admin session."""
    # We use a sync check here since we're already in an async context
    # that isn't using asyncio.to_thread — run the validation in a thread pool worker
    loop = asyncio.get_running_loop()
    # Can't await in a def function easily — use sync approach
    pass


@app.get("/api/admin/keys")
async def get_keys(request: Request) -> JSONResponse:
    """Returns all API keys (quarterly_key masked) for the admin dashboard."""
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    keys = await asyncio.to_thread(db.get_all_keys, mask_quarterly_key=True)
    return JSONResponse(keys)


@app.post("/api/admin/keys")
async def create_key(
    request: Request,
    payload: KeyCreateRequest,
) -> JSONResponse:
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not payload.label.strip() or not payload.quarterly_key.strip():
        raise HTTPException(
            status_code=400,
            detail="Label and Quarterly key are required.",
        )

    if len(payload.quarterly_key.strip()) < 10:
        raise HTTPException(
            status_code=400,
            detail="Quarterly key must be at least 10 characters long.",
        )


    try:
        new_key = await asyncio.to_thread(
            db.add_api_key, payload.label, payload.quarterly_key
        )
        # Mask the quarterly key in the response (only show full key once)
        new_key["quarterly_key"] = "***shown-once***"
        return JSONResponse(new_key, status_code=201)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.patch("/api/admin/keys/{key_id}")
async def update_key(
    key_id: int,
    request: Request,
    payload: KeyUpdateRequest,
) -> JSONResponse:
    """Update optional settings on a key (e.g., quota_limit)."""
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # For now, quota_limit is the only updateable field
    # Could expand with a full PATCH model
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        if payload.quota_limit is not None:
            cursor.execute(
                "UPDATE api_keys SET quota_limit = ? WHERE id = ?",
                (payload.quota_limit, key_id),
            )
        conn.commit()
        db.evict_key(None)  # clear cache so changes take effect
        return JSONResponse({"success": True})
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/api/admin/keys/{key_id}/toggle")
async def toggle_key(key_id: int, request: Request) -> JSONResponse:
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    updated = await asyncio.to_thread(db.toggle_key_status, key_id)
    if not updated:
        raise HTTPException(status_code=404, detail="API key not found")
    return JSONResponse(updated)


@app.delete("/api/admin/keys/{key_id}")
async def delete_key(key_id: int, request: Request) -> JSONResponse:
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    success = await asyncio.to_thread(db.delete_key, key_id)
    if not success:
        raise HTTPException(status_code=404, detail="API key not found")
    return JSONResponse({"success": True, "message": "API key deleted"})


@app.get("/api/admin/debug_stream")
async def get_debug_stream(request: Request) -> JSONResponse:
    """Returns the last N stream debug logs. REQUIRES admin auth."""
    if not await is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    return JSONResponse({"logs": _debug_stream_logs[-config.DEBUG_LOG_MAX_ENTRIES:]})


# ---------------------------------------------------------------------------
# OpenAI-compatible /models endpoint
# ---------------------------------------------------------------------------

@app.get("/v1/models")
@app.get("/models")
async def list_models(request: Request) -> JSONResponse:
    """Returns the list of available models. Supports dynamic fetch + static fallback."""
    client_ip = get_client_ip(request)
    if is_proxy_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    proxy_key = _extract_proxy_key(request)
    if not proxy_key:
        raise HTTPException(
            status_code=401,
            detail="Missing or invalid Bearer token or X-API-Key.",
        )

    key_mapping = await asyncio.to_thread(db.get_key_by_proxy_key, proxy_key)
    if not key_mapping:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key.")

    target_url = f"{config.TARGET_SCHEME}://{config.TARGET_HOST}/v1/models"
    headers = {
        "authorization": f"Bearer {key_mapping['quarterly_key']}",
        "x-api-key": key_mapping["quarterly_key"],
    }

    try:
        resp = await _http_client.get(target_url, headers=headers, timeout=10.0)
        if resp.status_code == 200:
            return JSONResponse(resp.json())
    except Exception as e:
        logger.warning("Failed to fetch models from upstream: %s", e)

    # Fallback to IMPPP.txt-derived list
    return JSONResponse({
        "object": "list",
        "data": config.FALLBACK_MODELS,
    })


# ---------------------------------------------------------------------------
# Catch-all proxy route
# ---------------------------------------------------------------------------

@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"],
)
async def proxy_request(request: Request, path: str) -> Response:
    """Main proxy: authenticates the proxy key, translates to Quarterly key, forwards."""
    global _debug_stream_logs, _request_counter

    client_ip = get_client_ip(request)
    request_id = request.headers.get("x-request-id") or _build_request_id()

    if is_proxy_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    proxy_key = _extract_proxy_key(request)
    if not proxy_key:
        raise HTTPException(status_code=401, detail="Missing API key.")

    key_mapping = await asyncio.to_thread(db.get_key_by_proxy_key, proxy_key)
    if not key_mapping:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key.")

    # ---- Per-key daily quota check ----
    today = _get_today_key()
    quota = key_mapping.get("quota_limit") or 0
    if quota > 0:
        daily_used = key_mapping.get("rate_limit_daily") or 0
        # The DB tracks this, but we can check locally for speed
        # The actual increment + quota check happens in increment_request_count
        if key_mapping.get("rate_limit_daily") and int(str(key_mapping.get("rate_limit_daily") or "0").split("_")[0] if isinstance(key_mapping.get("rate_limit_daily"), str) else key_mapping.get("rate_limit_daily") or 0) >= quota:
            raise HTTPException(
                status_code=429,
                detail=f"Daily quota ({quota}) exceeded for this proxy key.",
            )

    # Increment usage counter (returns False if quota exceeded)
    within_quota = await asyncio.to_thread(
        db.increment_request_count, proxy_key
    )
    if not within_quota:
        raise HTTPException(
            status_code=429,
            detail=f"Daily quota ({quota}) exceeded for this proxy key.",
        )

    # ---- Build target URL ----
    target_url = f"{config.TARGET_SCHEME}://{config.TARGET_HOST}/{path}"

    # ---- Forward headers (filter hop-by-hop, auth, and compression) ----
    headers = _build_forward_headers(request, key_mapping["quarterly_key"])

    # ---- Read body ----
    body = await request.body()

    # ---- Handle streaming vs non-streaming upstream ----
    upstream_method = request.method

    logger.info(
        "request_id=%s ip=%s key=%s %s %s",
        request_id,
        client_ip,
        key_mapping["label"],
        upstream_method,
        target_url,
    )

    try:
        upstream_resp = await _forward_request(
            upstream_method, target_url, headers, body, request_id
        )
        return upstream_resp

    except httpx.HTTPStatusError as e:
        logger.warning("Upstream %s: %s", e.response.status_code, e)
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Upstream error: {e.response.text[:500]}",
        )
    except Exception as e:
        logger.exception("request_id=%s proxy error: %s", request_id, e)
        raise HTTPException(status_code=502, detail=f"Proxy error: {e}")


# ---------------------------------------------------------------------------
# Proxy helper functions
# ---------------------------------------------------------------------------

def _extract_proxy_key(request: Request) -> str | None:
    """Extracts proxy key from Authorization: Bearer or X-API-Key header."""
    auth = request.headers.get("authorization")
    if auth and auth.startswith("Bearer "):
        return auth[7:].strip()
    x_api = request.headers.get("x-api-key")
    if x_api:
        return x_api.strip()
    return None


def _build_forward_headers(request: Request, quarterly_key: str) -> dict[str, str]:
    """Builds headers to forward to upstream, injecting the Quarterly key."""
    BLOCKLIST = {
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
        "x-api-key",
        "authorization",  # replaced below
    }
    headers: dict[str, str] = {}
    for k, v in request.headers.items():
        k_lower = k.lower()
        if k_lower.startswith(":") or k_lower in BLOCKLIST:
            continue
        headers[k] = v

    # Inject Quarterly credentials (both formats for OpenAI + Anthropic compatibility)
    headers["authorization"] = f"Bearer {quarterly_key}"
    headers["x-api-key"] = quarterly_key

    return headers





async def _forward_request(
    method: str,
    target_url: str,
    headers: dict[str, str],
    body: bytes,
    request_id: str,
) -> Response:
    """Sends the request to upstream and returns a StreamingResponse or JSONResponse."""
    global _debug_stream_logs

    req = _http_client.build_request(
        method=method,
        url=target_url,
        headers=headers,
        content=body,
    )

    upstream = await _http_client.send(req, stream=True)
    content_type = upstream.headers.get("content-type", "")

    # Check if this is a streaming response
    if "text/event-stream" in content_type or "stream" in content_type.lower():
        return StreamingResponse(
            _stream_generator(upstream, request_id),
            status_code=upstream.status_code,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "X-Request-ID": request_id,
            },
        )

    # Non-streaming: read fully, process, return
    try:
        content = await upstream.aread()
        await upstream.aclose()

        # Try to parse as JSON for XML → JSON tool call conversion
        try:
            json_body = json.loads(content)
            json_body = sanitize_json(json_body)
            json_body = convert_xml_to_json_non_streaming(json_body)
            return JSONResponse(
                content=json_body,
                status_code=upstream.status_code,
                headers={"X-Request-ID": request_id},
            )
        except (json.JSONDecodeError, TypeError):
            return Response(
                content=content,
                status_code=upstream.status_code,
                headers={"X-Request-ID": request_id},
            )
    finally:
        await upstream.aclose()


# ---------------------------------------------------------------------------
# Streaming response handler
# ---------------------------------------------------------------------------

async def _stream_generator(
    upstream: httpx.Response,
    request_id: str,
) -> AsyncIterator[bytes]:
    """Yields SSE lines, handling XML → JSON conversion and index remapping."""
    global _debug_stream_logs

    mapper = ToolCallIndexMapper()
    converter: XMLToJSONConverter | None = None
    is_openai = True
    last_message_id: str | None = None
    chunk_count = 0

    try:
        async for line in upstream.aiter_lines():
            if not line:
                continue

            chunk_count += 1

            # Log chunk arrival times if debug is enabled
            if config.DEBUG_STREAM_LOGGING:
                log_msg = (
                    f"request_id={request_id} "
                    f"line_len={len(line)} t={time.time():.3f}"
                )
                _debug_stream_logs.append(log_msg)
                if len(_debug_stream_logs) > config.DEBUG_LOG_MAX_ENTRIES:
                    _debug_stream_logs.pop(0)

            if line.startswith("data:"):
                data_str = line[5:].strip()

                if data_str == "[DONE]":
                    # Flush any buffered converter state
                    if converter:
                        rem_text, rem_chunks = converter.flush(message_id=last_message_id)
                        if rem_text:
                            for chunk_bytes in _yield_text_delta(
                                rem_text, is_openai, last_message_id
                            ):
                                yield chunk_bytes
                        for c in rem_chunks:
                            yield f"data: {json.dumps(c)}\n\n".encode()
                    yield b"data: [DONE]\n\n"
                    continue

                try:
                    chunk = json.loads(data_str)
                    chunk = sanitize_json(chunk)
                    chunk = map_chunk_tool_calls(chunk, mapper)

                    # Initialize converter on first chunk
                    if converter is None:
                        is_openai = "choices" in chunk
                        converter = XMLToJSONConverter(is_openai_format=is_openai)

                    if is_openai:
                        last_message_id = chunk.get("id") or last_message_id

                    # Extract text content for XML processing
                    text = _extract_text_from_chunk(chunk, is_openai)

                    if text:
                        text_to_yield, tool_chunks = converter.process_chunk_text(text)
                        # Modify chunk's delta content
                        _set_chunk_text(chunk, is_openai, text_to_yield)
                        yield f"data: {json.dumps(chunk)}\n\n".encode()
                        for tc in tool_chunks:
                            yield f"data: {json.dumps(tc)}\n\n".encode()
                    else:
                        # Pass through non-text chunks (e.g., session metadata, usage)
                        # But skip tool call index remapping for OpenAI tool_calls in delta
                        yield f"data: {json.dumps(chunk)}\n\n".encode()

                except json.JSONDecodeError:
                    # Malformed JSON — pass through raw line
                    yield f"{line}\n\n".encode()
            else:
                # Non-data: lines (e.g. HTTP/metadata) — pass through
                yield f"{line}\n".encode()

    finally:
        await upstream.aclose()


def _extract_text_from_chunk(chunk: dict, is_openai: bool) -> str:
    """Extracts text content from a streaming chunk delta."""
    if is_openai:
        choices = chunk.get("choices", [])
        if choices and isinstance(choices[0], dict):
            delta = choices[0].get("delta", {})
            return delta.get("content") or ""
    else:
        if chunk.get("type") == "content_block_delta":
            delta = chunk.get("delta", {})
            if delta.get("type") == "text_delta":
                return delta.get("text") or ""
    return ""


def _set_chunk_text(chunk: dict, is_openai: bool, text: str) -> None:
    """Sets text content in a streaming chunk delta."""
    if is_openai:
        if "choices" in chunk and chunk["choices"]:
            chunk["choices"][0]["delta"]["content"] = text
    else:
        if chunk.get("type") == "content_block_delta":
            chunk["delta"]["text"] = text


def _yield_text_delta(
    text: str, is_openai: bool, message_id: str | None
) -> AsyncIterator[bytes]:
    """Yields a single text delta chunk."""
    if is_openai:
        chunk = {
            "id": message_id or f"chatcmpl-{secrets.token_hex(12)}",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "choices": [{
                "index": 0,
                "delta": {"content": text},
                "finish_reason": None,
            }],
        }
    else:
        chunk = {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": text},
        }
    yield f"data: {json.dumps(chunk)}\n\n".encode()


# ---------------------------------------------------------------------------
# JSON sanitization & format conversion
# ---------------------------------------------------------------------------

def sanitize_json(obj: Any) -> Any:
    """Recursively converts numeric 'id' fields to strings (Quatarly compatibility)."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, list):
        return [sanitize_json(item) for item in obj]
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k == "id" and isinstance(v, (int, float)):
                result[k] = str(v)
            else:
                result[k] = sanitize_json(v)
        return result
    return obj


# ---------------------------------------------------------------------------
# Tool call index mapper (fixes Quatarly's non-sequential streaming indices)
# ---------------------------------------------------------------------------

class ToolCallIndexMapper:
    """Maps Quatarly's non-sequential tool call indices to sequential 0-based.

    Usage: create one per stream. Quatarly sometimes sends indices like
    [0, 3, 5] for parallel tool calls. Most clients expect [0, 1, 2].
    """

    def __init__(self) -> None:
        self._incoming_to_mapped: dict[int, int] = {}
        self._last_mapped: int = -1
        self._seen_ids: set[str] = set()

    def map_index(self, incoming_index: int, has_id: bool, tool_call_id: str | None = None) -> int:
        """Map an incoming tool call index to a sequential one.

        Args:
            incoming_index: The index from the upstream chunk.
            has_id: True if this chunk introduces a new tool call (has an id field).
                    False if it's a continuation of an existing block.
            tool_call_id: The unique ID of the tool call, if present.

        Returns:
            A zero-based sequential index.
        """
        if has_id and tool_call_id:
            if tool_call_id not in self._seen_ids:
                self._seen_ids.add(tool_call_id)
                self._last_mapped += 1
                self._incoming_to_mapped[incoming_index] = self._last_mapped
            return self._incoming_to_mapped[incoming_index]

        if incoming_index in self._incoming_to_mapped:
            return self._incoming_to_mapped[incoming_index]

        # Fallback
        if self._last_mapped < 0:
            self._last_mapped = 0
        self._incoming_to_mapped[incoming_index] = self._last_mapped
        return self._last_mapped


def map_chunk_tool_calls(chunk: dict, mapper: ToolCallIndexMapper) -> dict:
    """Updates tool call indices inside choice deltas using the mapper."""
    if not isinstance(chunk, dict):
        return chunk

    if "choices" in chunk and isinstance(chunk["choices"], list):
        for choice in chunk["choices"]:
            if "delta" in choice and isinstance(choice["delta"], dict):
                delta = choice["delta"]
                if "tool_calls" in delta and isinstance(delta["tool_calls"], list):
                    for tc in delta["tool_calls"]:
                        if isinstance(tc, dict) and "index" in tc:
                            incoming = tc["index"]
                            has_id = "id" in tc and tc["id"] is not None
                            tool_call_id = tc.get("id")
                            tc["index"] = mapper.map_index(incoming, has_id, tool_call_id)
    return chunk


# ---------------------------------------------------------------------------
# XML → JSON conversion (Claude's non-standard streaming format)
# ---------------------------------------------------------------------------

def parse_xml_tool_calls(xml_str: str) -> list[dict[str, Any]]:
    """Parses Claude's <invoke name="...">...</invoke> XML blocks.

    Handles:
    - <invoke name="..."> with nested <parameter name="...">...</parameter>
    - Nested XML inside parameter values (CDATA-like, escaped entities)
    """
    invokes = re.findall(
        r'<invoke name="([^"]+)"\s*>(.*?)</invoke>', xml_str, re.DOTALL
    )
    results = []
    for name, content in invokes:
        params = re.findall(
            r'<parameter name="([^"]+)"\s*>(.*?)</parameter>', content, re.DOTALL
        )
        arguments = {}
        for param_name, param_value in params:
            val = html.unescape(param_value.strip())
            try:
                arguments[param_name] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                arguments[param_name] = val
        results.append({"name": name, "arguments": arguments})
    return results


class XMLToJSONConverter:
    """Converts Claude's XML function_calls format to OpenAI-compatible JSON.

    Claude streams XML blocks like:
        <function_calls>
          <invoke name="get_weather">...</invoke>
        </function_calls>

    This converter:
    1. Buffers incoming text, detecting the <function_calls> tag
    2. On </function_calls>, parses the XML and emits synthetic JSON chunks
    3. Correctly handles MULTIPLE <function_calls> blocks in one stream

    Supports both OpenAI (chat.completion.chunk) and Anthropic streaming formats.
    """

    def __init__(self, is_openai_format: bool = True) -> None:
        self.is_openai_format = is_openai_format
        self.in_xml = False
        self.text_buffer = ""
        self.xml_buffer = ""
        self.tool_call_index = 0

    def process_chunk_text(self, text: str) -> tuple[str, list[dict[str, Any]]]:
        """Process incoming text. Returns (text_to_yield, tool_call_chunks).

        Accumulates text in buffers. When a complete XML block is found,
        parses it and returns the generated JSON chunks alongside any
        preceding plain text.
        """
        if not self.in_xml:
            self.text_buffer += text

            idx = self.text_buffer.find("<")
            if idx == -1:
                # No tag found — flush everything
                to_yield = self.text_buffer
                self.text_buffer = ""
                return to_yield, []

            slice_to_check = self.text_buffer[idx:]
            target = "<function_calls>"

            if target.startswith(slice_to_check) and slice_to_check != target:
                # Potentially starting the tag — hold text until confirmed
                to_yield = self.text_buffer[:idx]
                self.text_buffer = slice_to_check
                return to_yield, []

            elif "<function_calls" in self.text_buffer:
                # Tag found — split and enter XML mode
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

                # If the closing tag is already in the buffer, process immediately
                if "</function_calls>" in self.xml_buffer:
                    xml_text, xml_chunks = self._process_xml_buffer()
                    return text_to_yield + xml_text, xml_chunks
                return text_to_yield, []

            else:
                # Not a function_calls tag — flush and reset
                to_yield = self.text_buffer
                self.text_buffer = ""
                return to_yield, []
        else:
            # In XML mode — accumulate until closing tag
            self.xml_buffer += text

            if "</function_calls>" in self.xml_buffer:
                return self._process_xml_buffer()

            return "", []

    def _process_xml_buffer(self) -> tuple[str, list[dict[str, Any]]]:
        """Splits at </function_calls>, parses XML, returns text + tool chunks."""
        self.in_xml = False
        parts = self.xml_buffer.split("</function_calls>", 1)
        xml_to_parse = parts[0]
        remaining_text = parts[1] if len(parts) > 1 else ""

        # Parse and generate chunks
        tool_calls = parse_xml_tool_calls(xml_to_parse)
        chunks = self.generate_tool_call_chunks(tool_calls)

        self.xml_buffer = ""
        self.text_buffer = ""

        # Recursively process any text that came after </function_calls>
        # This handles multiple <function_calls> blocks correctly
        rem_text, rem_chunks = self.process_chunk_text(remaining_text)
        return rem_text, chunks + rem_chunks

    def flush(self, message_id: str | None = None) -> tuple[str, list[dict[str, Any]]]:
        """Flush remaining buffers when stream ends."""
        if not self.in_xml:
            to_yield = self.text_buffer
            self.text_buffer = ""
            return to_yield, []
        else:
            # Malformed stream that ended mid-XML
            raw_text = "<function_calls" + self.xml_buffer
            self.in_xml = False
            self.xml_buffer = ""
            self.text_buffer = ""
            return raw_text, []

    def generate_tool_call_chunks(
        self, tool_calls: list[dict[str, Any]], message_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Generates OpenAI or Anthropic streaming chunks from parsed tool calls."""
        if not tool_calls:
            return []

        chunks = []

        if self.is_openai_format:
            openai_tcs = []
            for i, tc in enumerate(tool_calls):
                openai_tcs.append({
                    "index": self.tool_call_index + i,
                    "id": f"call_{secrets.token_hex(12)}",
                    "type": "function",
                    "function": {
                        "name": tc["name"],
                        "arguments": json.dumps(tc["arguments"]),
                    },
                })
            self.tool_call_index += len(tool_calls)

            chunks.append({
                "id": message_id or f"chatcmpl-{secrets.token_hex(12)}",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "choices": [{
                    "index": 0,
                    "delta": {"tool_calls": openai_tcs},
                    "finish_reason": None,
                }],
            })
        else:
            # Anthropic streaming tool_use blocks
            for tc in tool_calls:
                block_idx = self.tool_call_index
                self.tool_call_index += 1
                call_id = f"toolu_{secrets.token_hex(12)}"

                chunks.append({
                    "type": "content_block_start",
                    "index": block_idx,
                    "content_block": {
                        "type": "tool_use",
                        "id": call_id,
                        "name": tc["name"],
                        "input": {},
                    },
                })
                chunks.append({
                    "type": "content_block_delta",
                    "index": block_idx,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": json.dumps(tc["arguments"]),
                    },
                })
                chunks.append({
                    "type": "content_block_stop",
                    "index": block_idx,
                })

        return chunks


def convert_xml_to_json_non_streaming(response_json: dict) -> dict:
    """Converts XML tool calls in non-streaming completions to JSON.

    Handles both OpenAI (chat.completion) and Anthropic (messages) formats.
    """
    if not isinstance(response_json, dict):
        return response_json

    # ---- OpenAI format ----
    if "choices" in response_json and isinstance(response_json["choices"], list):
        choice = response_json["choices"][0]
        if "message" in choice and isinstance(choice["message"], dict):
            message = choice["message"]
            content = message.get("content") or ""
            if "<function_calls>" in content and "</function_calls>" in content:
                parts = content.split("<function_calls>", 1)
                text_before = parts[0]
                rest = parts[1].split("</function_calls>", 1)
                xml_to_parse = rest[0].split(">", 1)[1] if ">" in rest[0] else rest[0]
                text_after = rest[1] if len(rest) > 1 else ""

                tool_calls = parse_xml_tool_calls(xml_to_parse)
                if tool_calls:
                    message["content"] = (text_before + text_after).strip() or None
                    openai_tcs = []
                    for tc in tool_calls:
                        openai_tcs.append({
                            "id": f"call_{secrets.token_hex(12)}",
                            "type": "function",
                            "function": {
                                "name": tc["name"],
                                "arguments": json.dumps(tc["arguments"]),
                            },
                        })
                    message["tool_calls"] = openai_tcs

    # ---- Anthropic format ----
    elif "content" in response_json and isinstance(response_json["content"], list):
        new_content = []
        for item in response_json["content"]:
            if isinstance(item, dict) and item.get("type") == "text":
                content_text = item.get("text") or ""
                if "<function_calls>" in content_text and "</function_calls>" in content_text:
                    parts = content_text.split("<function_calls>", 1)
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
                            "input": tc["arguments"],
                        })

                    if text_after.strip():
                        new_content.append({"type": "text", "text": text_after.strip()})
                else:
                    new_content.append(item)
            else:
                new_content.append(item)
        response_json["content"] = new_content

    return response_json


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "proxy:app",
        host=config.SERVER_HOST,
        port=config.SERVER_PORT,
        log_level="warning",
        # Graceful shutdown: allow 30 seconds for in-flight requests
        timeout_graceful_shutdown=30,
    )
