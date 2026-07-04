import os
import sqlite3
import secrets
from datetime import datetime
import logging
import time

logger = logging.getLogger("QuartarlyProxy.DB")

# In-memory cache of active proxy key → {"data": dict, "expires_at": float}
# Caches key configurations to eliminate SQLite reads on the hot path.
# Uses sliding expiration: cache hits extend the lifetime by 1 hour.
_keys_cache: dict = {}
_MAX_CACHE_SIZE = 500
_CACHE_TTL = 3600  # 1 hour sliding expiration

# Determine DB location.
# 1. Explicit environment variable check
if os.environ.get("DATABASE_PATH"):
    DB_PATH = os.environ.get("DATABASE_PATH")
    parent_dir = os.path.dirname(DB_PATH)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    logger.info(f"Using database path from env: {DB_PATH}")
# 2. If /data volume exists and is writable, use it.
elif os.path.exists("/data") and os.access("/data", os.W_OK):
    DB_PATH = "/data/database.db"
    logger.info(f"Using Railway persistent volume database path: {DB_PATH}")
# 3. Local fallback
else:
    os.makedirs("data", exist_ok=True)
    DB_PATH = "data/database.db"
    logger.info(f"Using local database path: {DB_PATH}")

def get_connection():
    # Set a 30-second timeout to handle concurrent writes gracefully
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes tables if they do not exist."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Enable Write-Ahead Logging (WAL) for high concurrency.
        # WAL mode is persistent on the database file, so this is a no-op after the first run — still safe to call.
        cursor.execute("PRAGMA journal_mode=WAL;")
        
        # Create api_keys table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                label TEXT NOT NULL,
                proxy_key TEXT UNIQUE NOT NULL,
                quarterly_key TEXT NOT NULL,
                request_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Create sessions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def generate_proxy_key() -> str:
    """Generates a secure proxy API key."""
    return f"esk-{secrets.token_hex(16)}"

def _evict_key(proxy_key: str | None = None):
    """Remove a specific key from the cache, or clear everything."""
    if proxy_key and proxy_key in _keys_cache:
        del _keys_cache[proxy_key]
    elif proxy_key is None:
        _keys_cache.clear()

def add_api_key(label: str, quarterly_key: str) -> dict:
    """Creates a new proxy API key mapped to a Quarterly key."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        proxy_key = generate_proxy_key()
        cursor.execute(
            "INSERT INTO api_keys (label, proxy_key, quarterly_key) VALUES (?, ?, ?)",
            (label.strip(), proxy_key, quarterly_key.strip())
        )
        conn.commit()
        
        # Get the inserted row
        cursor.execute("SELECT * FROM api_keys WHERE proxy_key = ?", (proxy_key,))
        row = cursor.fetchone()
        return dict(row)
    except Exception as e:
        logger.error(f"Error adding API key: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_all_keys() -> list:
    """Retrieves all API keys."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM api_keys ORDER BY created_at DESC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error fetching all API keys: {e}")
        return []
    finally:
        conn.close()

def get_key_by_proxy_key(proxy_key: str) -> dict:
    """Finds an active key mapping by proxy key.
    
    Checks the in-memory cache first to avoid disk I/O on every request.
    Applies a sliding 1-hour expiration on hits.
    """
    now = time.time()
    if proxy_key in _keys_cache:
        entry = _keys_cache[proxy_key]
        if now < entry["expires_at"]:
            # Slide expiration forward by another hour on hit
            entry["expires_at"] = now + _CACHE_TTL
            return entry["data"]
        else:
            # Expired
            del _keys_cache[proxy_key]
    
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM api_keys WHERE proxy_key = ? AND status = 'active'", (proxy_key,))
        row = cursor.fetchone()
        result = dict(row) if row else None
        # Only cache valid active keys
        if result is not None:
            if len(_keys_cache) >= _MAX_CACHE_SIZE:
                # Evict the oldest entry (dict insertion order guaranteed Python 3.7+)
                _keys_cache.pop(next(iter(_keys_cache)))
            _keys_cache[proxy_key] = {
                "data": result,
                "expires_at": now + _CACHE_TTL
            }
        return result
    except Exception as e:
        logger.error(f"Error fetching API key by proxy key: {e}")
        return None
    finally:
        conn.close()

def increment_request_count(proxy_key: str):
    """Increments request counter for a proxy key."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE api_keys SET request_count = request_count + 1 WHERE proxy_key = ?", (proxy_key,))
        conn.commit()
        # Update cache in-place instead of evicting to keep the cache hot!
        if proxy_key in _keys_cache:
            _keys_cache[proxy_key]["data"]["request_count"] += 1
            # Also slide the expiration forward on write hits
            _keys_cache[proxy_key]["expires_at"] = time.time() + _CACHE_TTL
    except Exception as e:
        logger.error(f"Error incrementing request count: {e}")
        conn.rollback()
    finally:
        conn.close()

def toggle_key_status(key_id: int) -> dict:
    """Toggles status between active and disabled."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT status, proxy_key FROM api_keys WHERE id = ?", (key_id,))
        row = cursor.fetchone()
        if not row:
            return None
        
        new_status = 'disabled' if row['status'] == 'active' else 'active'
        cursor.execute("UPDATE api_keys SET status = ? WHERE id = ?", (new_status, key_id))
        conn.commit()
        
        # Evict just this key from cache so the next request re-reads the new status.
        # This prevents a disabled key from being served from stale cache.
        _evict_key(row['proxy_key'])
        
        cursor.execute("SELECT * FROM api_keys WHERE id = ?", (key_id,))
        updated_row = cursor.fetchone()
        return dict(updated_row)
    except Exception as e:
        logger.error(f"Error toggling status for key {key_id}: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

def delete_key(key_id: int) -> bool:
    """Deletes an API key from the system."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Fetch proxy_key before deleting so we can evict it from cache
        cursor.execute("SELECT proxy_key FROM api_keys WHERE id = ?", (key_id,))
        row = cursor.fetchone()
        cursor.execute("DELETE FROM api_keys WHERE id = ?", (key_id,))
        # Capture rowcount BEFORE commit — some SQLite drivers reset it to -1 after commit
        deleted = cursor.rowcount
        conn.commit()
        if row:
            _evict_key(row['proxy_key'])
        return deleted > 0
    except Exception as e:
        logger.error(f"Error deleting key {key_id}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

# Session Management
def create_session() -> str:
    """Creates a new admin session and stores it."""
    session_id = secrets.token_hex(32)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO sessions (session_id) VALUES (?)", (session_id,))
        conn.commit()
        return session_id
    except Exception as e:
        logger.error(f"Error creating session: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def validate_session(session_id: str) -> bool:
    """Checks if a session ID is valid."""
    if not session_id:
        return False
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM sessions WHERE session_id = ?", (session_id,))
        row = cursor.fetchone()
        return row is not None
    except Exception as e:
        logger.error(f"Error validating session: {e}")
        return False
    finally:
        conn.close()

def delete_session(session_id: str) -> bool:
    """Deletes a session (logout)."""
    if not session_id:
        return False
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Error deleting session: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


