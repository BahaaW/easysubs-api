"""
Centralized configuration for EasySubs API.

All settings are loaded from environment variables with safe defaults.
This module is imported at module level by proxy.py and db.py.
"""

import os
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_BASE_DIR = Path(__file__).parent.resolve()


# ---------------------------------------------------------------------------
# Target upstream
# ---------------------------------------------------------------------------
TARGET_HOST: str = os.environ.get("TARGET_HOST", "api.quatarly.cloud")
TARGET_SCHEME: str = os.environ.get("TARGET_SCHEME", "https")


# ---------------------------------------------------------------------------
# Admin credentials
# ---------------------------------------------------------------------------
ADMIN_USERNAME: str = os.environ.get("ADMIN_USERNAME", "")
ADMIN_PASSWORD: str = os.environ.get("ADMIN_PASSWORD", "")

# Raise at startup if running with default credentials
_is_local = os.environ.get("ENVIRONMENT", "local") == "local"
if not _is_local and (not ADMIN_USERNAME or not ADMIN_PASSWORD):
    raise ValueError(
        "ADMIN_USERNAME and ADMIN_PASSWORD must be set in non-local environments. "
        "Set ENVIRONMENT=local to allow defaults (development only)."
    )

# Defaults used only in local/dev mode
ADMIN_USERNAME = ADMIN_USERNAME or "admin"
ADMIN_PASSWORD = ADMIN_PASSWORD or "admin_secure_pass"


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_PATH: str | None = os.environ.get("DATABASE_PATH") or None


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------
# Proxy endpoint rate limit per IP (requests per window)
RATE_LIMIT_REQUESTS: int = int(os.environ.get("RATE_LIMIT_REQUESTS", 120))
RATE_LIMIT_WINDOW_SECONDS: int = int(os.environ.get("RATE_LIMIT_WINDOW", 60))

# Login endpoint rate limit per IP — stricter than proxy limits
LOGIN_RATE_LIMIT_REQUESTS: int = int(os.environ.get("LOGIN_RATE_LIMIT_REQUESTS", 10))
LOGIN_RATE_LIMIT_WINDOW_SECONDS: int = int(
    os.environ.get("LOGIN_RATE_LIMIT_WINDOW", 300)
)  # 5-minute window for login


# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
# Explicit list — never use wildcard with credentials
_cors_origins_raw = os.environ.get("CORS_ORIGINS", "")
if _cors_origins_raw:
    CORS_ORIGINS: list[str] = [o.strip() for o in _cors_origins_raw.split(",") if o.strip()]
else:
    # Safe default: same-origin only in production, permissive in local
    CORS_ORIGINS = ["http://localhost:3000", "http://127.0.0.1:3000"] if _is_local else []


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------
SERVER_PORT: int = int(os.environ.get("PORT", 8005))
SERVER_HOST: str = os.environ.get("HOST", "0.0.0.0")


# ---------------------------------------------------------------------------
# Proxy connection pool
# ---------------------------------------------------------------------------
HTTPX_MAX_KEEPALIVE_CONNECTIONS: int = int(
    os.environ.get("HTTPX_MAX_KEEPALIVE_CONNECTIONS", 100)
)
HTTPX_MAX_CONNECTIONS: int = int(os.environ.get("HTTPX_MAX_CONNECTIONS", 500))
HTTPX_TIMEOUT_SECONDS: float = float(os.environ.get("HTTPX_TIMEOUT_SECONDS", 180.0))


# ---------------------------------------------------------------------------
# Background flusher interval
# ---------------------------------------------------------------------------
FLUSHER_INTERVAL_SECONDS: float = float(os.environ.get("FLUSHER_INTERVAL_SECONDS", 10))


# ---------------------------------------------------------------------------
# Model aliasing / routing
# ---------------------------------------------------------------------------
# Maps client-sent model names to the names Quatarly expects.
# This enables compatibility across VS Code, Claude Code, Cursor, etc.
# Values of None mean "pass through unchanged".
_MODEL_ALIASES: dict[str, str | None] = {
    # Anthropic-style "latest" aliases
    "claude-3-5-sonnet-latest": "claude-sonnet-4-6",
    "claude-3-5-sonnet-20241022": "claude-sonnet-4-6",
    "claude-3-opus-latest": "claude-opus-4-6",
    "claude-3-haiku-latest": "claude-haiku-4-5-20251001",
    "claude-3.5-sonnet-latest": "claude-sonnet-4-6",
    "claude-3.5-haiku-latest": "claude-haiku-4-5-20251001",
    # Claude Code commonly sends these versioned names
    "claude-opus-4-5": "claude-opus-4-6",
    "claude-sonnet-4-5": "claude-sonnet-4-6",
    "claude-3-opus-20240229": "claude-opus-4-6",
    "claude-3-haiku-20240307": "claude-haiku-4-5-20251001",
    "claude-3-sonnet-20240229": "claude-sonnet-4-6",
    # OpenAI-compatible aliases (pass through — Quatarly handles or ignores)
    "gpt-4o": None,
    "gpt-4o-mini": None,
    "gpt-4-turbo": None,
    "gpt-4": None,
    # Gemini
    "gemini-3.1-pro-latest": "gemini-3.1-pro",
    "gemini-3.1-pro-low-latest": "gemini-3.1-pro-low",
    # Thinking variants (Quatarly-specific) — pass through unchanged
    "claude-sonnet-4-6-thinking": "claude-sonnet-4-6-thinking",
    "claude-opus-4-7-thinking": "claude-opus-4-7-thinking",
    "claude-opus-4-8-thinking": "claude-opus-4-8-thinking",
    "claude-opus-4-6-thinking": "claude-opus-4-6-thinking",
}


def resolve_model_alias(model: str) -> str:
    """Translate a client-sent model name to the Quatarly-side name.

    Unknown models are passed through unchanged.
    """
    return _MODEL_ALIASES.get(model, model)


# ---------------------------------------------------------------------------
# Dynamic fallback model list (loaded from IMPPP.txt at startup)
# ---------------------------------------------------------------------------

def _load_fallback_models() -> list[dict[str, Any]]:
    """Load fallback model list from IMPPP.txt if it exists."""
    imppp_path = _BASE_DIR / "IMPPP.txt"
    if not imppp_path.exists():
        # Hardcoded fallback — matches IMPPP.txt contents
        return [
            {"id": "claude-haiku-4-5-20251001", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-opus-4-6", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-opus-4-6-thinking", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-opus-4-7", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-opus-4-7-thinking", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-opus-4-8", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-opus-4-8-thinking", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-sonnet-4-6", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-sonnet-4-6-20250929", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "claude-sonnet-4-6-thinking", "object": "model",
             "created": 1700000000, "owned_by": "anthropic"},
            {"id": "gemini-3.1-pro", "object": "model",
             "created": 1700000000, "owned_by": "google"},
            {"id": "gemini-3.1-pro-low", "object": "model",
             "created": 1700000000, "owned_by": "google"},
        ]

    try:
        import json

        raw = imppp_path.read_text(encoding="utf-8").strip()
        parsed = json.loads(raw)
        if "models" in parsed and isinstance(parsed["models"], dict):
            models = []
            for model_id, meta in parsed["models"].items():
                name = meta.get("name", model_id) if isinstance(meta, dict) else str(meta)
                models.append({
                    "id": model_id,
                    "object": "model",
                    "created": 1700000000,
                    "owned_by": "anthropic",
                    "description": name,
                })
            return models
        return []
    except Exception:
        return []


FALLBACK_MODELS: list[dict[str, Any]] = _load_fallback_models()


# ---------------------------------------------------------------------------
# Session / cookie settings
# ---------------------------------------------------------------------------
SESSION_COOKIE_MAX_AGE_SECONDS: int = int(
    os.environ.get("SESSION_COOKIE_MAX_AGE_SECONDS", 86400 * 7)
)  # 7 days


# ---------------------------------------------------------------------------
# Debug / logging
# ---------------------------------------------------------------------------
LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "WARNING")
DEBUG_STREAM_LOGGING: bool = os.environ.get("DEBUG_STREAM_LOGGING", "0") == "1"
DEBUG_LOG_MAX_ENTRIES: int = int(os.environ.get("DEBUG_LOG_MAX_ENTRIES", 100))


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
SHUTDOWN_TIMEOUT_SECONDS: float = float(os.environ.get("SHUTDOWN_TIMEOUT_SECONDS", 30.0))


