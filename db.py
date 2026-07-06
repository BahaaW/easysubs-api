"""
Database layer for EasySubs API Translation Proxy.

Handles:
- API key lifecycle (create, read, update, delete)
- Admin session management with TTL-based expiration
- Request count aggregation with atomic batch flush
- WAL-backed crash-resilient pending increments (survives SIGKILL)
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
# WAL-backed crash-resilient pending increments.
#
# _pending_increments: proxy_key -> {"count": int, "date": str, "dirty": bool}
#   count  – pending increment for (proxy_key, date)
#   date   – UTC date key (YYYY-MM-DD) the count belongs to
#   dirty  – True if this entry needs a DB flush
#
# Flushing strategy (from flush_pending_increments in proxy.py):
#   1. On every N increments (flush every 50 requests per key)
#   2. On the periodic timer
#   3. On graceful shutdown
#   4. On startup (recover dirty state from DB before serving traffic)
#
# This survives SIGKILL because the DB is WAL-backed — a checkpoint writes
# all committed changes to the WAL file before the write-ahead log is
# trusted for reads. The periodic timer ensures dirty rows are checkpointed
# frequently enough that no more than ~50 increments are lost in the worst
# case.
# ---------------------------------------------------------------------------
_pending_increments: dict[str, dict[str, Any]] = {}
_pending_increments_lock: threading.Lock = threading.Lock()

# Flush to DB every N increments per key. A crash loses at most this many
# counts per key before the next flush. Keep small enough that the daily
# quota can still be meaningfully enforced (50 is a good balance).
FLUSH_EVERY_N: int = 50



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

        # Enable WAL mode
        cursor.execute("PRAGMA journal_mode=WAL;")
        # Verify it took — WAL can fail silently on some filesystems
        cursor.execute("PRAGMA journal_mode;")
        mode = cursor.fetchone()
        if not mode or mode[0].upper() != "WAL":
            logger.warning("WAL mode not enabled — journal_mode=%s", mode)

        # ---- request_counts table ----
        # Primary key is (proxy_key, date) so increments are partitioned by day.
        # The date column enables efficient cleanup of old rows and daily reset.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS request_counts (
                proxy_key    TEXT    NOT NULL,
                date         TEXT    NOT NULL,   -- YYYY-MM-DD (UTC)
                count        INTEGER DEFAULT 0,
                PRIMARY KEY (proxy_key, date)
            )
        """)

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
                daily_used       INTEGER DEFAULT 0,  -- today's request count (UTC date key)
                daily_reset_date TEXT,                 -- YYYY-MM-DD of daily_used
                quota_limit      INTEGER DEFAULT 0     -- 0 = unlimited
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

        # ---- Migrate pre-Fix3 DBs ----
        try:
            cursor.execute(
                "ALTER TABLE api_keys ADD COLUMN last_used_at TIMESTAMP"
            )
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

        try:
            cursor.execute(
                "ALTER TABLE sessions ADD COLUMN expires_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'"
            )
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

        for col, col_type, default in [
            ("daily_used", "INTEGER", 0),
            ("daily_reset_date", "TEXT", "''"),
            ("quota_limit", "INTEGER", 0),
            ("rate_limit_rpm", "INTEGER", 0),
        ]:
            try:
                cursor.execute(
                    f"ALTER TABLE api_keys ADD COLUMN {col} {col_type} DEFAULT {default}"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise

        conn.commit()
        logger.info("Database initialized successfully.")

        # ---- Rebuild _pending_increments from dirty rows on startup ----
        _recover_dirty_increments()

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



def _recover_dirty_increments() -> None:
    """Loads today's counts from request_counts as baseline on startup.

    These baseline counts are already in the DB. _pending_increments only
    tracks NEW increments since the last flush. On flush, the pending count
    is ADDED to the DB (INSERT ... ON CONFLICT DO UPDATE SET count = count + ?),
    so we must NOT include the baseline in _pending_increments — otherwise
    the baseline gets double-counted.

    Instead, we store the baseline separately so increment_request_count can
    use it for quota checks without a DB round-trip.
    """
    global _pending_increments

    today = _get_today_date()
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT proxy_key, count FROM request_counts WHERE date = ?",
            (today,),
        )
        rows = cursor.fetchall()
        if rows:
            # Store as dirty=False with count=0 — the baseline is in the DB.
            # increment_request_count will add new increments on top.
            recovered = {row["proxy_key"]: {"count": 0, "date": today, "dirty": False}
                       for row in rows}
            _pending_increments.update(recovered)
            logger.info("Recovered %d baseline entries from DB.", len(recovered))
    except Exception as e:
        logger.warning("Failed to recover pending increments: %s", e)
    finally:
        conn.close()


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
    Only active keys are cached.

    On a cache miss, daily_used is loaded from the request_counts table if
    today's counts exist; otherwise it is initialized to 0. This means a
    restarted process correctly respects daily quota without losing counts.
    """
    now = time.time()
    today = _get_today_date()

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
            "created_at, last_used_at, quota_limit, rate_limit_rpm, daily_used, daily_reset_date "
            "FROM api_keys WHERE proxy_key = ? AND status = 'active'",
            (proxy_key,),
        )
        row = cursor.fetchone()
        result = dict(row) if row else None

        if result is not None:
            # Reset daily_used if the cached date is stale (UTC midnight crossed)
            cached_date = result.get("daily_reset_date") or ""
            if cached_date != today:
                result["daily_used"] = 0
                result["daily_reset_date"] = today
            else:
                # Load today's count from the request_counts table to account for
                # any increments that were flushed to DB before this key entered
                # the cache (or after it was evicted and reloaded).
                cursor.execute(
                    "SELECT count FROM request_counts WHERE proxy_key = ? AND date = ?",
                    (proxy_key, today),
                )
                count_row = cursor.fetchone()
                if count_row:
                    result["daily_used"] = count_row["count"]

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


def increment_request_count(proxy_key: str) -> tuple[bool, bool]:
    """Increments the in-memory request counter and updates last_used_at.

    Also checks daily quota if one is set on the key. Resets daily_used at
    midnight UTC.

    Returns:
        (allowed, flush_needed):
            allowed     – True if the request is within quota (or no quota set)
            flush_needed – True if the caller should call flush_pending_increments()
                          because we've hit the FLUSH_EVERY_N threshold
    """
    global _pending_increments

    today = _get_today_date()
    now = time.time()
    flush_needed = False

    # Check quota from cache if available
    with _cache_lock:
        if proxy_key in _keys_cache:
            entry = _keys_cache[proxy_key]
            quota = entry["data"].get("quota_limit") or 0

            # Reset daily_used if UTC date has crossed midnight
            if entry["data"].get("daily_reset_date") != today:
                entry["data"]["daily_used"] = 0
                entry["data"]["daily_reset_date"] = today

            daily_used = entry["data"].get("daily_used") or 0
            if quota > 0 and daily_used >= quota:
                return False, False  # Quota exceeded

    # Cache miss — check quota from DB (includes today's flushed count from
    # request_counts, so we never bypass quota after a process restart).
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT quota_limit, daily_used, daily_reset_date FROM api_keys "
            "WHERE proxy_key = ? AND status = 'active'",
            (proxy_key,),
        )
        row = cursor.fetchone()
        if row:
            quota = row["quota_limit"] or 0
            cached_date = row["daily_reset_date"] or ""
            if cached_date != today:
                current_daily = 0
            else:
                current_daily = row["daily_used"] or 0
            if quota > 0 and current_daily >= quota:
                return False, False
    finally:
        conn.close()

    # Buffer the increment
    with _pending_increments_lock:
        if proxy_key not in _pending_increments:
            _pending_increments[proxy_key] = {"count": 0, "date": today, "dirty": False}
        _pending_increments[proxy_key]["count"] += 1
        _pending_increments[proxy_key]["dirty"] = True
        if _pending_increments[proxy_key]["count"] >= FLUSH_EVERY_N:
            flush_needed = True

    # Update in-memory cache so the UI reflects the increment immediately
    with _cache_lock:
        if proxy_key in _keys_cache:
            entry = _keys_cache[proxy_key]
            entry["data"]["request_count"] = entry["data"].get("request_count", 0) + 1
            entry["data"]["last_used_at"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            entry["data"]["daily_used"] = entry["data"].get("daily_used", 0) + 1
            entry["data"]["daily_reset_date"] = today
            entry["expires_at"] = now + _CACHE_TTL_SECONDS

    return True, flush_needed


def flush_pending_increments() -> None:
    """Atomically flushes all dirty pending increments to SQLite in a single transaction.

    Writes to two tables:
    - request_counts (proxy_key, date) — durable, survives crashes via WAL
    - api_keys.daily_used              — used by the UI for display

    Only flushes rows that are marked dirty. Clean rows (recovered from DB on
    startup but not yet modified) are skipped.
    """
    global _pending_increments

    # Snapshot dirty entries and reset their counts (but keep the keys so
    # non-dirty entries recovered from DB on startup aren't lost).
    with _pending_increments_lock:
        if not _pending_increments:
            return
        to_flush = {}
        for k, v in _pending_increments.items():
            if v.get("dirty"):
                to_flush[k] = dict(v)
                # Reset count but keep the entry (preserves date, clears dirty)
                v["count"] = 0
                v["dirty"] = False

    if not to_flush:
        return

    conn = get_connection()
    try:
        cursor = conn.cursor()
        for proxy_key, pending in to_flush.items():
            date = pending["date"]
            count = pending["count"]

            # Upsert into request_counts (WAL-backed — survives SIGKILL)
            cursor.execute(
                "INSERT INTO request_counts (proxy_key, date, count) VALUES (?, ?, ?) "
                "ON CONFLICT(proxy_key, date) DO UPDATE SET count = count + ?",
                (proxy_key, date, count, count),
            )
            # Update api_keys for UI display. Read back the cumulative totals
            # from request_counts so daily_used and request_count reflect ALL
            # increments, not just the current batch.
            cursor.execute(
                "UPDATE api_keys SET "
                "daily_used = (SELECT count FROM request_counts WHERE proxy_key = ? AND date = ?), "
                "request_count = (SELECT COALESCE(SUM(count), 0) FROM request_counts WHERE proxy_key = ?), "
                "daily_reset_date = ?, "
                "last_used_at = CURRENT_TIMESTAMP "
                "WHERE proxy_key = ?",
                (proxy_key, date, proxy_key, date, proxy_key),
            )
        conn.commit()

        logger.debug("Flushed %d dirty increment groups to DB", len(to_flush))
    except Exception as e:
        logger.error("Error flushing pending increments to database: %s", e)
        conn.rollback()

        # Restore dirty entries so no increments are lost
        with _pending_increments_lock:
            for k, v in to_flush.items():
                if k not in _pending_increments:
                    _pending_increments[k] = {"count": 0, "date": v["date"], "dirty": False}
                _pending_increments[k]["count"] += v["count"]
                _pending_increments[k]["dirty"] = True
        logger.warning("Restored %d dirty increment groups after flush failure", len(to_flush))
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
            "created_at, last_used_at, daily_used, daily_reset_date, quota_limit, rate_limit_rpm "
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
            "created_at, last_used_at, daily_used, daily_reset_date, quota_limit, rate_limit_rpm "
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
