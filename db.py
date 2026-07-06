"""
Database layer for EasySubs API Translation Proxy.

Handles:
- API key lifecycle (create, read, update, delete)
- Admin session management with TTL-based expiration
- Request count aggregation with atomic batch flush
- In-memory cache with TTL + LRU eviction for hot-path optimization

All DB operations use thread-safe SQLite connections. The in-memory cache
is process-global and shared across async tasks — access is serialized via
the single-threaded SQLite connection, so no lock is needed for cache reads.
"""

import os
import sqlite3
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any
import logging

logger = logging.getLogger("EasySubsAPI.DB")

# ---------------------------------------------------------------------------
# Cache configuration
# ---------------------------------------------------------------------------
_MAX_CACHE_SIZE: int = 500
_CACHE_TTL_SECONDS: float = 3600.0  # 1 hour sliding expiration

# ---------------------------------------------------------------------------
# Session configuration
# ---------------------------------------------------------------------------
_SESSION_TTL_DAYS: int = 7

# ---------------------------------------------------------------------------
# In-memory cache of proxy_key -> {"data": dict, "expires_at": float}
# Uses sliding expiration: cache hits extend TTL by one full window.
# Eviction strategy: LRU when size >= _MAX_CACHE_SIZE; stale entries
# are purged on access or during explicit cache sweeps.
# ---------------------------------------------------------------------------
_keys_cache: dict[str, dict[str, Any]] = {}

# Lock to serialize cache mutations. Since all DB access is already serialized
# through SQLite connections, this lock is lightweight — it only gates the
# shared Python dict. A threading.Lock (not async) is correct because
# db.py functions run in thread pool workers via asyncio.to_thread().
_cache_lock: threading.Lock = threading.Lock()

# ---------------------------------------------------------------------------
# In-memory buffer to batch write request counts and timestamps.
# Keys are proxy_key strings; values are pending increment counts.
# Flushed to SQLite periodically by the background flusher in proxy.py.
# ---------------------------------------------------------------------------
_pending_increments: dict[str, int] = {}
_pending_increments_lock: threading.Lock = threading.Lock()



# ---------------------------------------------------------------------------
# Database path resolution
# ---------------------------------------------------------------------------
def _get_today_date() -> str:
    """Returns today's date string in YYYY-MM-DD format (UTC)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _resolve_db_path() -> str:
    """Resolves the database file path with the following priority:

    1. Explicit DATABASE_PATH env var (always respected).
    2. /data volume (Railway persistent volume).
    3. data/database.db relative to this file's directory.
    """
    if os.environ.get("DATABASE_PATH"):
        db_path = os.environ["DATABASE_PATH"]
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        logger.info("Using database path from DATABASE_PATH env: %s", db_path)
        return db_path

    if os.path.exists("/data") and os.access("/data", os.W_OK):
        db_path = "/data/database.db"
        logger.info("Using Railway persistent volume database: %s", db_path)
        return db_path

    # Local development fallback — relative to this file
    local_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(local_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    db_path = os.path.join(data_dir, "database.db")
    logger.info("Using local database path: %s", db_path)
    return db_path


DB_PATH: str = _resolve_db_path()


def get_connection() -> sqlite3.Connection:
    """Returns a SQLite connection with a 30-second timeout and Row factory."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Schema initialization
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Creates all tables and runs any pending migrations. Idempotent."""
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Enable WAL mode only if not already in WAL mode (avoid log spam)
        cursor.execute("PRAGMA journal_mode;")
        current_mode = cursor.fetchone()
        if current_mode and current_mode[0].upper() != "WAL":
            cursor.execute("PRAGMA journal_mode=WAL;")

        # ---- api_keys table ----
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                label            TEXT    NOT NULL,
                proxy_key        TEXT    UNIQUE NOT NULL,
                quarterly_key    TEXT    NOT NULL,
                request_count    INTEGER DEFAULT 0,
                status           TEXT    DEFAULT 'active',
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at     TIMESTAMP,
                rate_limit_daily INTEGER  DEFAULT 0,  -- requests used today (UTC date key)
                quota_limit      INTEGER  DEFAULT 0   -- 0 = unlimited
            )
        """)

        # ---- sessions table ----
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id  TEXT PRIMARY KEY,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at  TIMESTAMP NOT NULL
            )
        """)

        # ---- Migrate last_used_at if missing (pre-2025 DBs) ----
        try:
            cursor.execute(
                "ALTER TABLE api_keys ADD COLUMN last_used_at TIMESTAMP"
            )
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

        # ---- Migrate expires_at if missing from sessions ----
        try:
            cursor.execute(
                "ALTER TABLE sessions ADD COLUMN expires_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'"
            )
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

        # ---- Migrate rate_limit_daily / quota_limit / rate_limit_rpm if missing ----
        for col, default in [
            ("rate_limit_daily", 0),
            ("quota_limit", 0),
            ("rate_limit_rpm", 0),   # 0 = unlimited (per-key requests per minute)
        ]:
            try:
                cursor.execute(
                    f"ALTER TABLE api_keys ADD COLUMN {col} INTEGER DEFAULT {default}"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise

        conn.commit()
        logger.info("Database initialized successfully.")

        # ---- Run cleanup on startup to purge expired sessions ----
        cleanup_expired_sessions()

    except Exception as e:
        logger.error("Failed to initialize database: %s", e)
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# API Key operations
# ---------------------------------------------------------------------------

def generate_proxy_key() -> str:
    """Generates a cryptographically secure proxy API key with 'esk-' prefix."""
    return f"esk-{secrets.token_hex(16)}"


def evict_key(proxy_key: str | None = None) -> None:
    """Thread-safe cache eviction. Pass a specific key or None to clear all."""
    with _cache_lock:
        if proxy_key is not None:
            _keys_cache.pop(proxy_key, None)
        else:
            _keys_cache.clear()


# Private alias for internal use within db.py
_evict_key = evict_key


def add_api_key(label: str, quarterly_key: str) -> dict[str, Any]:
    """Creates a new proxy API key mapped to a Quarterly key."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        proxy_key = generate_proxy_key()
        cursor.execute(
            "INSERT INTO api_keys (label, proxy_key, quarterly_key) VALUES (?, ?, ?)",
            (label.strip(), proxy_key, quarterly_key.strip()),
        )
        conn.commit()

        cursor.execute(
            "SELECT id, label, proxy_key, quarterly_key, request_count, status, "
            "created_at, last_used_at FROM api_keys WHERE proxy_key = ?",
            (proxy_key,),
        )
        row = cursor.fetchone()
        return dict(row) if row else {}
    except Exception as e:
        logger.error("Error adding API key: %s", e)
        conn.rollback()
        raise
    finally:
        conn.close()


def mask_key(key: str | None) -> str:
    """Helper to safely mask an API key. Returns masked placeholder if too short."""
    if not key:
        return ""
    if len(key) > 10:
        return f"{key[:4]}...{key[-4:]}"
    return "***masked***"


def get_key_by_proxy_key(proxy_key: str) -> dict[str, Any] | None:
    """Finds an active key mapping by proxy key.

    Checks the in-memory cache first (sliding TTL). Falls back to SQLite.
    Only active keys are cached. On cache miss (fresh DB read), daily_used
    is initialized to 0.
    """
    now = time.time()

    # Fast path: cached entry
    with _cache_lock:
        if proxy_key in _keys_cache:
            entry = _keys_cache[proxy_key]
            if now < entry["expires_at"]:
                # Slide TTL forward
                entry["expires_at"] = now + _CACHE_TTL_SECONDS
                return entry["data"]
            else:
                # Expired — remove now
                del _keys_cache[proxy_key]

    # Slow path: SQLite
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, label, proxy_key, quarterly_key, request_count, status, "
            "created_at, last_used_at, quota_limit, rate_limit_rpm "
            "FROM api_keys WHERE proxy_key = ? AND status = 'active'",
            (proxy_key,),
        )
        row = cursor.fetchone()
        result = dict(row) if row else None

        if result is not None:
            # Attach daily_used tracking (initialized to 0 on fresh DB read)
            result["daily_used"] = 0
            result["daily_reset_date"] = _get_today_date()
            with _cache_lock:
                # Evict oldest entry when at capacity (dict preserves insertion order)
                if len(_keys_cache) >= _MAX_CACHE_SIZE:
                    oldest_key = next(iter(_keys_cache))
                    del _keys_cache[oldest_key]
                _keys_cache[proxy_key] = {
                    "data": result,
                    "expires_at": now + _CACHE_TTL_SECONDS,
                }
        return result
    except Exception as e:
        logger.error("Error fetching API key by proxy key: %s", e)
        return None
    finally:
        conn.close()


def increment_request_count(proxy_key: str) -> bool:
    """Increments the in-memory request counter and updates last_used_at.

    Also checks daily quota if one is set on the key. Resets daily_used at midnight UTC.

    Returns:
        True if the request is within quota (or no quota set), False if quota exceeded.
    """
    global _pending_increments

    today = _get_today_date()

    # Check quota from cache if available
    with _cache_lock:
        if proxy_key in _keys_cache:
            entry = _keys_cache[proxy_key]
            quota = entry["data"].get("quota_limit") or 0
            
            # Reset daily_used if date has changed
            if entry["data"].get("daily_reset_date") != today:
                entry["data"]["daily_used"] = 0
                entry["data"]["daily_reset_date"] = today
            
            daily_used = entry["data"].get("daily_used") or 0
            if quota > 0 and daily_used >= quota:
                return False  # Quota exceeded
        else:
            # Cache miss — fall back to DB to check quota before allowing the request.
            # Without this, a fresh process restart or cache eviction would bypass quotas.
            conn = get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT quota_limit FROM api_keys WHERE proxy_key = ? AND status = 'active'",
                    (proxy_key,),
                )
                row = cursor.fetchone()
                if row:
                    quota = row["quota_limit"] or 0
                    if quota > 0:
                        # Key not in cache means daily_used is effectively 0, so quota
                        # can't be exceeded. But we still need to prime the cache so
                        # subsequent requests within the same day are tracked.
                        pass
            finally:
                conn.close()

    # Buffer the increment
    with _pending_increments_lock:
        if proxy_key not in _pending_increments:
            _pending_increments[proxy_key] = 0
        _pending_increments[proxy_key] += 1

    # Update read cache immediately so UI reflects the increment
    with _cache_lock:
        if proxy_key in _keys_cache:
            entry = _keys_cache[proxy_key]
            entry["data"]["request_count"] = entry["data"].get("request_count", 0) + 1
            entry["data"]["last_used_at"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            entry["data"]["daily_used"] = entry["data"].get("daily_used", 0) + 1
            entry["expires_at"] = time.time() + _CACHE_TTL_SECONDS

    return True


def flush_pending_increments() -> None:
    """Atomically flushes all pending increments to SQLite in a single transaction."""
    global _pending_increments

    # Snapshot and clear atomically so concurrent requests keep accumulating
    with _pending_increments_lock:
        if not _pending_increments:
            return
        to_flush = _pending_increments
        _pending_increments = {}

    conn = get_connection()
    try:
        cursor = conn.cursor()
        for proxy_key, count in to_flush.items():
            cursor.execute(
                "UPDATE api_keys SET "
                "  request_count = request_count + ?, "
                "  last_used_at  = CURRENT_TIMESTAMP "
                "WHERE proxy_key = ?",
                (count, proxy_key),
            )
        conn.commit()

        logger.debug("Flushed %d increments to DB", len(to_flush))
    except Exception as e:
        logger.error("Error flushing pending increments to database: %s", e)
        conn.rollback()

        # Restore pending increments directly
        with _pending_increments_lock:
            for k, v in to_flush.items():
                _pending_increments[k] = _pending_increments.get(k, 0) + v
        logger.warning("Restored %d pending increments after flush failure", len(to_flush))
    finally:
        conn.close()


def toggle_key_status(key_id: int) -> dict[str, Any] | None:
    """Toggles status between 'active' and 'disabled'. Evicts cache entry."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, proxy_key FROM api_keys WHERE id = ?", (key_id,)
        )
        row = cursor.fetchone()
        if not row:
            return None

        new_status = "disabled" if row["status"] == "active" else "active"
        cursor.execute(
            "UPDATE api_keys SET status = ? WHERE id = ?", (new_status, key_id)
        )
        conn.commit()

        # Evict from cache immediately so the next request sees fresh status.
        evict_key(row["proxy_key"])

        cursor.execute(
            "SELECT id, label, proxy_key, quarterly_key, request_count, status, "
            "created_at, last_used_at, rate_limit_daily, quota_limit "
            "FROM api_keys WHERE id = ?",
            (key_id,),
        )
        updated = cursor.fetchone()
        return dict(updated) if updated else None
    except Exception as e:
        logger.error("Error toggling status for key %d: %s", key_id, e)
        conn.rollback()
        return None
    finally:
        conn.close()


def delete_key(key_id: int) -> bool:
    """Deletes an API key. Returns True iff a row was actually deleted."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Fetch proxy_key before deleting so we can evict from cache
        cursor.execute(
            "SELECT proxy_key FROM api_keys WHERE id = ?", (key_id,)
        )
        row = cursor.fetchone()
        if not row:
            return False

        # Evict from cache BEFORE deleting to prevent stale reads
        _evict_key(row["proxy_key"])

        cursor.execute("DELETE FROM api_keys WHERE id = ?", (key_id,))
        deleted = cursor.rowcount
        conn.commit()

        # Evict again AFTER commit to ensure no concurrent request re-cached it in the interim
        _evict_key(row["proxy_key"])

        return deleted > 0
    except Exception as e:
        logger.error("Error deleting key %d: %s", key_id, e)
        conn.rollback()
        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

def create_session() -> str:
    """Creates a new admin session with a 7-day TTL."""
    session_id = secrets.token_hex(32)
    expires_at = datetime.now(timezone.utc) + timedelta(days=_SESSION_TTL_DAYS)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO sessions (session_id, expires_at) VALUES (?, ?)",
            (session_id, expires_at.strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        return session_id
    except Exception as e:
        logger.error("Error creating session: %s", e)
        conn.rollback()
        raise
    finally:
        conn.close()


def validate_session(session_id: str) -> bool:
    """Returns True if the session is valid and not expired."""
    if not session_id:
        return False
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sessions WHERE session_id = ? AND expires_at > CURRENT_TIMESTAMP",
            (session_id,),
        )
        row = cursor.fetchone()
        return row is not None
    except Exception as e:
        logger.error("Error validating session: %s", e)
        return False
    finally:
        conn.close()


def delete_session(session_id: str) -> bool:
    """Deletes a session (logout). Idempotent — returns True if it existed."""
    if not session_id:
        return False
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error("Error deleting session: %s", e)
        conn.rollback()
        return False
    finally:
        conn.close()


def cleanup_expired_sessions() -> int:
    """Removes all expired sessions. Returns the number of sessions removed."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM sessions WHERE expires_at <= CURRENT_TIMESTAMP"
        )
        deleted = cursor.rowcount
        conn.commit()
        if deleted:
            logger.info("Cleaned up %d expired sessions.", deleted)
        return deleted
    except Exception as e:
        logger.error("Error cleaning up expired sessions: %s", e)
        conn.rollback()
        return 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Cache maintenance
# ---------------------------------------------------------------------------

def sweep_stale_cache_entries() -> int:
    """Removes all expired entries from the key cache. Returns count removed."""
    now = time.time()
    removed = 0
    with _cache_lock:
        stale = [k for k, v in _keys_cache.items() if now >= v["expires_at"]]
        for k in stale:
            del _keys_cache[k]
            removed += 1
    if removed:
        logger.debug("Swept %d stale entries from key cache.", removed)
    return removed


def get_all_keys(
    mask_quarterly_key: bool = True,
) -> list[dict[str, Any]]:
    """Retrieves all API keys.

    Args:
        mask_quarterly_key: If True, quarterly_key is masked.
                            Use False only for internal admin operations.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, label, proxy_key, quarterly_key, request_count, status, "
            "created_at, last_used_at, rate_limit_daily, quota_limit, rate_limit_rpm "
            "FROM api_keys ORDER BY created_at DESC"
        )
        rows = cursor.fetchall()
        keys = []
        for row in rows:
            k = dict(row)
            if mask_quarterly_key:
                k["quarterly_key"] = mask_key(k.get("quarterly_key"))
            keys.append(k)
        return keys
    except Exception as e:
        logger.error("Error fetching all API keys: %s", e)
        return []
    finally:
        conn.close()
