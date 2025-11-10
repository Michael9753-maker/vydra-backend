"""
Optimized app.py for VYDRA backend
- Robust DB switching: PostgreSQL (Supabase) via psycopg2 pool OR local sqlite3 (WAL + busy timeout)
- Integrates download_manager (proxy-first) as a singleton
- Safer DB writes with retries to avoid `database is locked`
- Lightweight background tasks: cleanup scheduler, midnight maintenance
- Clear startup diagnostics for Render vs local

HOW TO USE:
- Replace your current app.py with this file (make a backup first).
- Ensure required env vars are set in Render or your local .env: SUPABASE_URL, SUPABASE_KEY, OPENAI_API_KEY (optional), SUPABASE_DB_URL (optional) etc.
- `pip install -r requirements.txt` should include psycopg2-binary, flask, flask_cors, yt-dlp, supabase, openai, requests

This file intentionally keeps the app-layer logic concise and delegates download work to download_manager.py (your existing module).
"""

from __future__ import annotations

import os
import sys
import time
import json
import logging
import traceback
import atexit
import signal
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

from flask import Flask, request, jsonify, send_from_directory, redirect
from flask_cors import CORS

# optional dotenv for local dev (you can keep or remove)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- imports for DB clients ---
try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
    PSYCOPG2_AVAILABLE = True
except Exception:
    psycopg2 = None
    ThreadedConnectionPool = None
    PSYCOPG2_AVAILABLE = False

import sqlite3

# download manager (the patched one you already added proxy support to)
try:
    from download_manager import get_default_manager
except Exception:
    # fallback: try to import from src (Render layout) if needed
    try:
        from src.download_manager import get_default_manager
    except Exception:
        get_default_manager = None

# --- Config & logging ---
LOG = logging.getLogger("vydra")
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s")

# Environment configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL")  # postgres connection string (optional)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
PAYSTACK_SECRET = os.environ.get("PAYSTACK_SECRET")

# fallback DB path for sqlite
SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", os.path.join(os.path.dirname(__file__), "vydra_local.db"))

# threaded pg pool (initialized if SUPABASE_DB_URL present and psycopg2 available)
_pg_pool: Optional[ThreadedConnectionPool] = None

# sqlite connection pool / global
_sqlite_conn: Optional[sqlite3.Connection] = None
_sqlite_lock = threading.Lock()

# app
app = Flask(__name__)
CORS(app)

# Download manager singleton
if get_default_manager:
    download_mgr = get_default_manager()
else:
    download_mgr = None

# utilities
def _now_iso():
    return datetime.now(timezone.utc).isoformat()

# ---------------- Database helpers ----------------

def init_postgres_pool(dsn: str, minconn: int = 1, maxconn: int = 10):
    global _pg_pool
    if not PSYCOPG2_AVAILABLE:
        LOG.warning("psycopg2 not available; cannot init pg pool")
        return
    if _pg_pool:
        return
    try:
        _pg_pool = ThreadedConnectionPool(minconn, maxconn, dsn)
        LOG.info("Postgres connection pool initialized (min=%s,max=%s)", minconn, maxconn)
    except Exception:
        LOG.exception("Failed to initialize Postgres pool")
        _pg_pool = None


def get_pg_conn():
    """Get a psycopg2 connection from the pool. Caller must close/putconn()."""
    if not _pg_pool:
        raise RuntimeError("Postgres pool not initialized")
    return _pg_pool.getconn()


def release_pg_conn(conn):
    if _pg_pool and conn:
        try:
            _pg_pool.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

# sqlite helpers

def init_sqlite(path: str):
    global _sqlite_conn
    if _sqlite_conn:
        return
    LOG.info("Initializing SQLite DB at %s", path)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
    # WAL mode reduces "database is locked" issues when there are concurrent readers/writers
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        LOG.exception("failed to set sqlite pragmas")
    conn.row_factory = sqlite3.Row
    _sqlite_conn = conn


def get_sqlite_conn():
    global _sqlite_conn
    if not _sqlite_conn:
        init_sqlite(SQLITE_DB_PATH)
    return _sqlite_conn

# unified get_db_conn (yields a connection and a type flag)

def get_db_conn():
    """Return (conn, 'pg'|'sqlite') where conn is a DB connection. For PG you must release via release_pg_conn()."""
    if SUPABASE_DB_URL and PSYCOPG2_AVAILABLE:
        # prefer PG if available
        if not _pg_pool:
            init_postgres_pool(SUPABASE_DB_URL, minconn=1, maxconn=int(os.environ.get("PG_POOL_MAX", "10")))
        if _pg_pool:
            conn = get_pg_conn()
            return conn, "pg"
        else:
            LOG.warning("pg pool not available, falling back to sqlite")
    # fallback to sqlite
    conn = get_sqlite_conn()
    return conn, "sqlite"

# safe execute with retries (avoid sqlite "database is locked")

def db_execute(query: str, params: tuple = (), commit: bool = True, retries: int = 5, retry_sleep: float = 0.1):
    """Executes a write query with retries. Automatically commits for sqlite; for pg commits by default.
    Returns rows for selects.
    """
    for attempt in range(retries):
        try:
            conn, kind = get_db_conn()
            if kind == "pg":
                cur = conn.cursor()
                cur.execute(query, params)
                if commit:
                    conn.commit()
                # fetch if select
                try:
                    rows = cur.fetchall()
                    cur.close()
                    release_pg_conn(conn)
                    return rows
                except Exception:
                    cur.close()
                    release_pg_conn(conn)
                    return None
            else:
                # sqlite: single shared connection, use lock for thread-safety
                with _sqlite_lock:
                    cur = conn.cursor()
                    cur.execute(query, params)
                    if commit:
                        conn.commit()
                    try:
                        rows = cur.fetchall()
                        return rows
                    except Exception:
                        return None
        except Exception as e:
            # detect locked-like errors and retry
            msg = str(e).lower()
            if "locked" in msg or "database is locked" in msg or "could not serialize access" in msg or "deadlock" in msg:
                LOG.warning("DB locked/serialization error on attempt %s/%s: %s", attempt + 1, retries, e)
                time.sleep(retry_sleep * (attempt + 1))
                continue
            else:
                LOG.exception("DB execute exception (not retrying): %s", e)
                raise
    LOG.error("DB execute failed after %s retries: %s", retries, query)
    raise RuntimeError("DB execute failed: max retries")

# convenience insert for downloads history

def record_download_history(jid: str, url: str, user_id: Optional[str], status: str, file_path: Optional[str], created_at: Optional[str], finished_at: Optional[str], error: Optional[str]):
    try:
        if SUPABASE_DB_URL and PSYCOPG2_AVAILABLE and _pg_pool:
            # postgres table schema expected (create manually or via migrations)
            q = "INSERT INTO downloads_history (id, url, user_id, status, file_path, created_at, finished_at, error) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, file_path=EXCLUDED.file_path, finished_at=EXCLUDED.finished_at, error=EXCLUDED.error"
            params = (jid, url, user_id, status, file_path, created_at, finished_at, error)
            # quick attempt, but tolerate failures
            try:
                conn = get_pg_conn()
                cur = conn.cursor()
                cur.execute(q, params)
                conn.commit()
                cur.close()
                release_pg_conn(conn)
            except Exception:
                LOG.exception("Failed to write download history to postgres; ignoring")
        else:
            # sqlite table creation if missing
            q = "INSERT OR REPLACE INTO downloads_history (id, url, user_id, status, file_path, created_at, finished_at, error) VALUES (?,?,?,?,?,?,?,?)"
            db_execute(q, (jid, url, user_id, status, file_path, created_at, finished_at, error), commit=True)
    except Exception:
        LOG.exception("record_download_history failed (final) for %s", jid)

# ----------------- Init DB schema for sqlite fallback -----------------
def _init_sqlite_schema_if_needed():
    conn = get_sqlite_conn()
    with _sqlite_lock:
        cur = conn.cursor()
        # lightweight schema: downloads_history and users and premium_subscriptions
        cur.execute("""
        CREATE TABLE IF NOT EXISTS downloads_history (
            id TEXT PRIMARY KEY,
            url TEXT,
            user_id TEXT,
            status TEXT,
            file_path TEXT,
            created_at TEXT,
            finished_at TEXT,
            error TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            created_at INTEGER DEFAULT (strftime('%s','now')),
            total_visits INTEGER DEFAULT 0,
            visits_today INTEGER DEFAULT 0,
            visits_month INTEGER DEFAULT 0,
            total_invites INTEGER DEFAULT 0
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS premium_subscriptions (
            user_id TEXT PRIMARY KEY,
            expires_at INTEGER
        )
        """)
        conn.commit()

# ----------------- Download endpoints -----------------

@app.route("/", methods=["GET"])
def home():
    return "✅ VYDRA Backend is live and running!"


@app.route("/api/download", methods=["POST"]) 
def api_download():
    payload = request.get_json(force=True)
    url = payload.get("url")
    user_id = payload.get("user_id")
    mode = payload.get("mode")
    if not url:
        return jsonify({"error": "missing url"}), 400

    if not download_mgr:
        return jsonify({"error": "download manager unavailable"}), 500

    job_id, err = download_mgr.start_download_job(url=url, user_id=user_id, mode=mode)
    if err:
        return jsonify({"error": err}), 500

    # record initial history (best-effort)
    try:
        record_download_history(job_id, url, user_id, "queued", None, _now_iso(), None, None)
    except Exception:
        LOG.exception("failed to record initial history for %s", job_id)

    return jsonify({"job_id": job_id}), 202


@app.route("/api/progress/<job_id>", methods=["GET"])
def api_progress(job_id):
    if not download_mgr:
        return jsonify({"error": "download manager unavailable"}), 500
    status = download_mgr.get_job_status(job_id)
    if not status:
        return jsonify({"error": "job not found"}), 404
    return jsonify(status)


@app.route("/api/file/<path:filename>", methods=["GET"])
def api_file(filename):
    # serve file from download_dir
    try:
        mgr = get_default_manager()
        download_dir = mgr.download_dir
    except Exception:
        # fallback to local downloads
        download_dir = os.path.join(os.path.dirname(__file__), 'downloads')

    safe_path = os.path.abspath(os.path.join(download_dir, filename))
    if not safe_path.startswith(os.path.abspath(download_dir)):
        return jsonify({"error": "invalid filename"}), 400
    if not os.path.exists(safe_path):
        return jsonify({"error": "file not found"}), 404
    return send_from_directory(download_dir, filename, as_attachment=True)

# ----------------- Background tasks -----------------

def _cleanup_scheduler(interval_seconds: int = 60 * 60):
    """Schedule periodic cleanup using the download manager's _cleanup_old_files method."""
    def job():
        try:
            mgr = get_default_manager()
            try:
                mgr._cleanup_old_files()
            except Exception:
                LOG.exception("cleanup routine failed")
        except Exception:
            LOG.exception("cleanup scheduler error")
        # reschedule
        t = threading.Timer(interval_seconds, job)
        t.daemon = True
        t.start()
    # start first run after small delay
    t0 = threading.Timer(5, job)
    t0.daemon = True
    t0.start()


def _midnight_maintenance_loop():
    def job():
        while True:
            try:
                # compute seconds until next midnight UTC (or configurable RESET_TIME)
                now = datetime.utcnow()
                tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
                secs = (tomorrow - now).total_seconds()
                LOG.info("midnight maintenance sleeping for %.0f seconds", secs)
                time.sleep(max(1, secs))
                # perform maintenance: reset visits_today, cleanup old premium, etc.
                try:
                    # example: reset visits_today
                    if SUPABASE_DB_URL and PSYCOPG2_AVAILABLE and _pg_pool:
                        conn = get_pg_conn()
                        cur = conn.cursor()
                        cur.execute("UPDATE users SET visits_today = 0")
                        conn.commit()
                        cur.close()
                        release_pg_conn(conn)
                    else:
                        # sqlite
                        db_execute("UPDATE users SET visits_today = 0", (), commit=True)
                    LOG.info("✅ Midnight maintenance: reset visits_today for all users.")
                except Exception:
                    LOG.exception("Midnight maintenance error")
            except Exception:
                LOG.exception("midnight maintenance outer loop error")
                time.sleep(60)

    t = threading.Thread(target=job, daemon=True)
    t.start()

# ----------------- Graceful shutdown -----------------

def _shutdown(signum=None, frame=None):
    LOG.info("Shutdown signal received: %s", signum)
    try:
        mgr = get_default_manager()
        try:
            mgr.shutdown(wait_seconds=2.0)
        except Exception:
            LOG.exception("download manager shutdown failed")
    except Exception:
        pass
    # close pg pool
    try:
        if _pg_pool:
            _pg_pool.closeall()
    except Exception:
        pass
    try:
        if _sqlite_conn:
            _sqlite_conn.close()
    except Exception:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)
atexit.register(_shutdown)

# ----------------- App startup diagnostics & bootstrap -----------------

def startup_checks():
    LOG.info("Starting VYDRA backend — startup checks")
    LOG.info("Environment: SUPABASE_DB_URL=%s SUPABASE_URL=%s", bool(SUPABASE_DB_URL), bool(SUPABASE_URL))

    # init DB
    if SUPABASE_DB_URL and PSYCOPG2_AVAILABLE:
        try:
            init_postgres_pool(SUPABASE_DB_URL, minconn=1, maxconn=int(os.environ.get('PG_POOL_MAX', '10')))
            # do a quick ping
            conn = get_pg_conn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            release_pg_conn(conn)
            LOG.info("✅ Postgres reachable")
        except Exception:
            LOG.exception("❌ Database connection failed: falling back to sqlite")
            init_sqlite(SQLITE_DB_PATH)
            _init_sqlite_schema_if_needed()
    else:
        # sqlite fallback (local dev)
        init_sqlite(SQLITE_DB_PATH)
        _init_sqlite_schema_if_needed()

    # download manager
    if download_mgr:
        LOG.info("INFO: download_manager loaded: %s", getattr(download_mgr, 'download_dir', 'unknown'))
    else:
        LOG.warning("Download manager not found; /api/download will be disabled")

    # start background tasks
    try:
        _cleanup_scheduler(interval_seconds=int(os.environ.get('CLEANUP_INTERVAL', 60*60)))
        _midnight_maintenance_loop()
    except Exception:
        LOG.exception("failed to start background tasks")


# when launched directly
if __name__ == "__main__":
    try:
        startup_checks()
        # prefer port defined by env or default 8000 for local
        port = int(os.environ.get('PORT', '8000'))
        LOG.info("Starting VYDRA backend (local/dev) on 0.0.0.0:%s", port)
        app.run(host='0.0.0.0', port=port, threaded=True)
    except Exception:
        LOG.exception("Fatal startup error")
        raise

# when imported by gunicorn/Render, run startup checks once
try:
    startup_checks()
except Exception:
    LOG.exception("startup checks failed during import")

