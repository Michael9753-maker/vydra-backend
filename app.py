#!/usr/bin/env python3
"""
app.py — VYDRA main backend (fully merged & upgraded)

This file implements:
 - Smart Unlimited download strategy: visible "unlimited" downloads for guests/free users with no hard daily quota.
 - Guest tracking using cookie-based guest IDs.
 - Premium expiry auto-downgrade (DB-backed + in-memory); midnight maintenance cleans expired subs.
 - Robust Paystack initialize/verify/webhook + session sync + safer recording.
 - AI spend tracking + AI_LIMITED when monthly spend >= threshold (env VYDRA_AI_SPEND_THRESHOLD).
 - History cleanup (per-user keep N, TTL 3h, orphan cleanup) with optimized background worker.
 - Alerts to ALERT_EMAIL_TO when AI threshold reached or monthly summary (SMTP optional).
 - Compatibility wrappers for download_manager and ai_manager with safe fallbacks.
 - Defensive DB handling and epoch-based timestamps to avoid SQLite timestamp conversion issues.

Notes:
 - Daily quota enforcement has been removed and replaced with the Smart Unlimited approach: downloads are recorded for analytics but are not blocked by daily counts.
 - Soft protective measures (cleanup, file size limits) remain to protect server resources.
"""

# ---------- START: Render-ready top of app.py ----------
import os
import json
import time
import uuid
import threading
import traceback
import secrets
import requests
import hmac
import hashlib
import importlib
import signal
import atexit
import shutil
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, send_file, redirect, make_response, Response, stream_with_context
from flask_cors import CORS
import logging
from typing import Optional, Dict, Any
import sqlite3  # use plain sqlite3 (avoid detect_types auto-conversion pitfalls)

# Third-party clients
from supabase import create_client, Client
import openai
import os
import logging
from flask import Flask

# ===================== Fix DOWNLOAD_DIR =====================
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")  # Path for downloaded files
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)  # Create folder if it doesn't exist

# ===================== Root route for testing =====================
app = Flask(__name__)

@app.route("/")
def home():
    return "✅ VYDRA Backend is live and running!"

# -------- Base directory (used for DB path, file storage, etc.) --------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# -------- Logging setup --------
log = logging.getLogger("vydra-backend")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# ===================== Cleanup function =====================
import shutil  # for removing non-empty directories

def do_cleanup_once():
    try:
        if not os.path.exists(DOWNLOAD_DIR):
            logging.warning(f"DOWNLOAD_DIR '{DOWNLOAD_DIR}' does not exist. Skipping cleanup.")
            return

        for fn in os.listdir(DOWNLOAD_DIR):
            file_path = os.path.join(DOWNLOAD_DIR, fn)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logging.warning(f"Failed to remove '{file_path}': {e}")

        logging.info("✅ Cleanup worker completed successfully.")

    except Exception as e:
        logging.error(f"Cleanup worker error: {e}")

# -------- Flask app --------
app = Flask(__name__)
CORS(app)

# -------- Environment variables (Render provides these) --------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
PAYSTACK_SECRET = os.environ.get("PAYSTACK_SECRET")  # keep this name consistent
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Helpful debugging message if anything is missing (fail fast so Render logs show the issue)
missing = [name for name, val in (
    ("SUPABASE_URL", SUPABASE_URL),
    ("SUPABASE_KEY", SUPABASE_KEY),
    ("PAYSTACK_SECRET", PAYSTACK_SECRET),
    ("OPENAI_API_KEY", OPENAI_API_KEY),
) if not val]

if missing:
    # This will cause Render to surface a clear error in the deploy logs
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    # ===================== Schedule cleanup =====================
from threading import Timer

def schedule_cleanup():
    do_cleanup_once()  # Run it immediately
    # Schedule it to run every 24 hours (86400 seconds)
    Timer(86400, schedule_cleanup).start()

# Start the schedule when the app runs
schedule_cleanup()

# -------- Initialize external clients --------
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    log.info("Supabase client initialized.")
except Exception:
    log.exception("Failed to initialize Supabase client.")
    raise

# OpenAI setup
openai.api_key = OPENAI_API_KEY
log.info("OpenAI client configured.")

# -------- PORT setup (Render sets PORT; default for local = 8000) --------
PORT = int(os.environ.get("PORT", 8000))

# ---------- END: Render-ready top of app.py ----------

# ------------- Application config and thresholds -------------
CLEANUP_SECONDS = int(os.environ.get("CLEANUP_SECONDS", 1800))  # legacy worker expiry (unused for history TTL below)
HISTORY_MAX = int(os.environ.get("HISTORY_MAX", 200))
MAX_RECENT_DOWNLOADS = int(os.environ.get("MAX_RECENT_DOWNLOADS", 5))  # per-user file keep
HISTORY_FILE_TTL_SECS = int(os.environ.get("HISTORY_FILE_TTL_SECS", 3 * 3600))  # 3 hours default
CLEANUP_INTERVAL_SECS = int(os.environ.get("VYDRA_CLEANUP_INTERVAL_SECS", 300))  # default 5 minutes
ADMIN_TOKEN = os.environ.get("VYDRA_ADMIN_TOKEN", "")

MAX_DURATION_SECONDS = int(os.environ.get("VYDRA_MAX_DURATION_SECONDS", 20 * 60))
MAX_FILESIZE_BYTES = int(os.environ.get("VYDRA_MAX_FILESIZE_BYTES", 300 * 1024 * 1024))

# Use DB_PATH inside repo by default
DB_PATH = os.path.join(BASE_DIR, "vydra_referrals.db")
# Ensure directory exists (will create if missing)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

WA_LINK = os.environ.get("WA_LINK", "https://wa.link/rcptsq")

# Frontend origins — keep string for env var, but also provide parsed list for CORS checks
FRONTEND_ORIGINS = os.environ.get("FRONTEND_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
FRONTEND_ORIGINS_LIST = [o.strip() for o in FRONTEND_ORIGINS.split(",") if o.strip()]

# Alerting address (locked as requested)
ALERT_EMAIL_TO = os.environ.get("VYDRA_ALERT_EMAIL", "vydra.contact@gmail.com")
# System identity (locked)
SYSTEM_IDENTITY = os.environ.get("VYDRA_SYSTEM_IDENTITY", "VYDRA AI Infrastructure")

# AI spend threshold (currency units). When reached, AI will be limited.
AI_SPEND_THRESHOLD = float(os.environ.get("VYDRA_AI_SPEND_THRESHOLD", "25.0"))
AI_LIMITED = False

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger("vydra")

# ---------------- Runtime capability checks ----------------
HAVE_FFMPEG = shutil.which("ffmpeg") is not None
try:
    import yt_dlp  # type: ignore
    HAVE_YTDLP = True
except Exception:
    HAVE_YTDLP = False

log.info("Startup: ffmpeg=%s, yt-dlp=%s", HAVE_FFMPEG, HAVE_YTDLP)
log.info("Alerts will be sent to: %s (if SMTP configured)", ALERT_EMAIL_TO)

# ---------------- Flask ----------------
app = Flask(__name__, static_folder=os.path.join(BASE_DIR, "static"))
origins = [o.strip() for o in FRONTEND_ORIGINS.split(",") if o.strip()]
CORS(app, resources={r"/api/*": {"origins": origins}, r"/download*": {"origins": origins}, r"/*": {"origins": origins}})

# ---------------- SQLite helpers ----------------
def get_db_conn():
    # Use plain connection without detect_types to avoid automatic timestamp conversion problems.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        created_at INTEGER DEFAULT (strftime('%s','now')),
        total_visits INTEGER DEFAULT 0,
        visits_today INTEGER DEFAULT 0,
        visits_month INTEGER DEFAULT 0,
        total_invites INTEGER DEFAULT 0
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS visit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id TEXT,
        visitor_ip TEXT,
        user_agent TEXT,
        ts INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reward_tokens (
        token TEXT PRIMARY KEY,
        user_id TEXT,
        milestone TEXT,
        created_at INTEGER DEFAULT (strftime('%s','now')),
        expires_at INTEGER,
        claimed INTEGER DEFAULT 0,
        claimed_at INTEGER
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS claimed_invite_rewards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT,
        milestone TEXT,
        claimed_at INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS premium_subscriptions (
        user_id TEXT PRIMARY KEY,
        expires_at INTEGER
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trial_claims (
        user_id TEXT PRIMARY KEY,
        claimed_at INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        reference TEXT PRIMARY KEY,
        user_id TEXT,
        email TEXT,
        plan TEXT,
        amount INTEGER,
        status TEXT,
        paystack_response TEXT,
        created_at INTEGER DEFAULT (strftime('%s','now')),
        processed INTEGER DEFAULT 0,
        processed_at INTEGER
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ai_hashtag_learning (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tag TEXT,
        platform TEXT,
        times_used INTEGER DEFAULT 0,
        engagement_score REAL DEFAULT 0.0,
        last_used INTEGER DEFAULT (strftime('%s','now')),
        UNIQUE(tag, platform)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS downloads_history (
        id TEXT PRIMARY KEY,
        url TEXT,
        user_id TEXT,
        status TEXT,
        file_path TEXT,
        created_at INTEGER,
        finished_at INTEGER,
        error TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ai_spend (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT,
        amount REAL,
        ts INTEGER
    );
    """)
    conn.commit()
    conn.close()

init_db()

# ---------------- Plan System (in-memory store & DB sync helpers) ----------------
USER_PLANS: Dict[str, Dict[str, Any]] = {}

def _epoch_to_dt(ts: Optional[int]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        return None

def _dt_to_epoch(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None

def get_user_plan(user_id: str):
    # Returns {"plan": "free"|"premium", "expiry": epoch or None}
    if not user_id:
        return {"plan": "free", "expiry": None}
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        conn.close()
        if row and row["expires_at"]:
            expires_epoch = int(row["expires_at"])
            expires_dt = _epoch_to_dt(expires_epoch)
            if expires_dt and datetime.now(timezone.utc) < expires_dt:
                return {"plan": "premium", "expiry": expires_dt.isoformat()}
            return {"plan": "free", "expiry": None}
    except Exception:
        log.exception("get_user_plan DB check failed")

    ud = USER_PLANS.get(user_id)
    if not ud:
        return {"plan": "free", "expiry": None}
    expiry = ud.get("expiry")
    try:
        if isinstance(expiry, datetime):
            exp_dt = expiry
        else:
            exp_dt = datetime.fromisoformat(expiry)
        if exp_dt.tzinfo is None:
            exp_dt = exp_dt.replace(tzinfo=timezone.utc)
        if exp_dt > datetime.now(timezone.utc):
            return {"plan": ud.get("plan", "premium"), "expiry": exp_dt.isoformat()}
    except Exception:
        pass
    return {"plan": "free", "expiry": None}

def activate_plan(user_id: str, duration_days: int):
    expiry_dt = datetime.now(timezone.utc) + timedelta(days=duration_days)
    USER_PLANS[user_id] = {"plan": "premium", "expiry": expiry_dt}
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO premium_subscriptions (user_id, expires_at) VALUES (?, ?)", (user_id, int(expiry_dt.timestamp())))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("Failed to persist premium subscription to DB for %s", user_id)
    return {"plan": "premium", "expiry": expiry_dt.isoformat()}

def check_premium(user_id: Optional[str]) -> bool:
    if not user_id:
        return False
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        conn.close()
        if row and row["expires_at"]:
            expires_epoch = int(row["expires_at"])
            expires_dt = _epoch_to_dt(expires_epoch)
            if expires_dt and datetime.now(timezone.utc) < expires_dt:
                return True
            return False
    except Exception:
        log.exception("DB check_premium failed; falling back to in-memory")

    ud = USER_PLANS.get(user_id)
    if not ud:
        return False
    expiry = ud.get("expiry")
    try:
        if isinstance(expiry, datetime):
            exp_dt = expiry
        else:
            exp_dt = datetime.fromisoformat(expiry)
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < exp_dt
    except Exception:
        return False

def premium_expires_at(user_id: Optional[str]) -> Optional[str]:
    if not user_id:
        return None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        conn.close()
        if row and row["expires_at"]:
            return _epoch_to_dt(int(row["expires_at"])) .isoformat()
    except Exception:
        pass
    ud = USER_PLANS.get(user_id)
    if ud and ud.get("expiry"):
        try:
            if isinstance(ud["expiry"], datetime):
                return ud["expiry"].isoformat()
            return str(ud["expiry"])
        except Exception:
            return None
    return None

# ---------------- small user helpers ----------------
def ensure_user_row(user_id):
    if not user_id:
        return
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
        if not cur.fetchone():
            cur.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
            conn.commit()
        conn.close()
    except Exception:
        log.exception("ensure_user_row failed for %s", user_id)

def increment_visit(referrer_id, visitor_ip, user_agent):
    if not referrer_id:
        return
    ensure_user_row(referrer_id)
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO visit_log (referrer_id, visitor_ip, user_agent) VALUES (?,?,?)", (referrer_id, visitor_ip, user_agent))
        cur.execute("UPDATE users SET total_visits = total_visits + 1, visits_today = visits_today + 1, visits_month = visits_month + 1 WHERE user_id=?", (referrer_id,))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("increment_visit failed for %s", referrer_id)

# ---------------- Payments helpers (Paystack integration) ----------------
def record_payment(reference, user_id=None, email=None, plan=None, amount=None, status=None, paystack_response=None):
    """
    Robust recording: ensure created_at preserved and processed flag kept.
    created_at and processed are stored as integers (epoch) / ints.
    """
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT created_at, processed, processed_at FROM payments WHERE reference=?", (reference,))
        existing = cur.fetchone()
        created_at = int(existing["created_at"]) if existing and existing["created_at"] else None
        processed = int(existing["processed"]) if existing and "processed" in existing.keys() and existing["processed"] else 0
        processed_at = int(existing["processed_at"]) if existing and "processed_at" in existing.keys() and existing["processed_at"] else None
        if created_at:
            cur.execute("""UPDATE payments SET user_id=?, email=?, plan=?, amount=?, status=?, paystack_response=?, processed=?, processed_at=? WHERE reference=?""",
                        (user_id, email, plan, amount, status, json.dumps(paystack_response if paystack_response is not None else {}), processed, processed_at, reference))
        else:
            cur.execute("""INSERT INTO payments (reference, user_id, email, plan, amount, status, paystack_response, processed, processed_at) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL)""",
                        (reference, user_id, email, plan, amount, status, json.dumps(paystack_response if paystack_response is not None else {})))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("Failed to record payment %s", reference)

def get_payment_by_reference(reference):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM payments WHERE reference=?", (reference,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        log.exception("get_payment_by_reference failed for %s", reference)
        return None

def mark_payment_processed(reference):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE payments SET processed=1, processed_at=? WHERE reference=?", (int(time.time()), reference))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("Failed to mark payment processed: %s", reference)

# ---------------- Try to import managers & adapt ----------------
dm = None
ai = None

JOB_CACHE: Dict[str, Dict[str, Any]] = {}
JOB_CACHE_LOCK = threading.Lock()

def _iso_to_epoch(iso: Optional[str]) -> Optional[int]:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None

def record_download_history(job: Dict[str, Any]):
    """
    Writes a stable downloads_history row. Ensures created_at and finished_at are integer epoch seconds or NULL.
    """
    try:
        if not job or not isinstance(job, dict):
            return
        conn = get_db_conn()
        cur = conn.cursor()
        jid = job.get("job_id") or job.get("id") or str(uuid.uuid4().hex[:12])
        url = job.get("url")
        user_id = job.get("user_id")
        status = job.get("status")
        local_path = job.get("local_path") or job.get("file_path") or job.get("file") or None

        created_raw = job.get("created_at") or job.get("created")
        finished_raw = job.get("finished_at") or job.get("finished")

        # normalize to epoch ints
        created_at = None
        finished_at = None
        if isinstance(created_raw, int):
            created_at = created_raw
        else:
            created_at = _iso_to_epoch(created_raw) or int(time.time())
        if isinstance(finished_raw, int):
            finished_at = finished_raw
        else:
            if status in ("finished", "error", "cancelled", "expired", "deleted"):
                finished_at = _iso_to_epoch(finished_raw) or int(time.time())
            else:
                finished_at = None

        error = job.get("error")
        cur.execute("INSERT OR REPLACE INTO downloads_history (id, url, user_id, status, file_path, created_at, finished_at, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (jid, url, user_id, status, local_path, created_at, finished_at, error))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("record_download_history failed for job %s", job.get("job_id"))

def _normalize_and_merge_into_cache(job_id: str, payload: Dict[str, Any]):
    # keep using existing logic but ensure user_id preserved and record history on terminal states
    if not isinstance(payload, dict):
        return
    norm = dict(payload)
    file_path = norm.pop("file_path", None) or norm.pop("file", None)
    if file_path:
        try:
            fname = os.path.basename(file_path)
            webpath = f"/api/file/{fname}"
            norm["file"] = webpath
            local_candidate = os.path.join(DOWNLOAD_DIR, fname)
            norm["local_path"] = local_candidate if os.path.exists(local_candidate) else file_path
        except Exception:
            norm["file"] = file_path
    if "progress_percent" in norm and "progress" not in norm:
        try:
            norm["progress"] = {"percent": norm.pop("progress_percent")}
        except Exception:
            pass
    if "progress" in norm and not isinstance(norm["progress"], dict):
        norm["progress"] = {"percent": norm["progress"]}
    norm["last_update"] = datetime.now(timezone.utc).isoformat()
    with JOB_CACHE_LOCK:
        existing = JOB_CACHE.get(job_id, {}) or {}
        # preserve existing user_id if present
        if "user_id" not in norm and "user_id" in existing:
            norm["user_id"] = existing.get("user_id")
        existing.update(norm)
        existing.setdefault("job_id", job_id)
        existing["last_update"] = norm["last_update"]
        JOB_CACHE[job_id] = existing
        # If terminal state, record to DB
        if existing.get("status") in ("finished", "error", "cancelled", "expired", "deleted"):
            try:
                record_download_history(existing)
            except Exception:
                log.exception("record history failed for %s", job_id)

def download_progress_cb(job_id: str, payload: Dict[str, Any]):
    try:
        _normalize_and_merge_into_cache(job_id, payload or {})
    except Exception:
        log.exception("download_progress_cb error for %s", job_id)

# adapter function (keeps compatibility with multiple shapes)
def _adapt_download_manager_module(mod):
    if hasattr(mod, "start_download_job") and callable(getattr(mod, "start_download_job")):
        class Wrapper:
            def __init__(self, mod):
                self.mod = mod
            def start_download_job(self, **kwargs):
                try:
                    return self.mod.start_download_job(**kwargs)
                except TypeError:
                    try:
                        return self.mod.start_download_job(kwargs.get("url"), kwargs.get("user_id"), kwargs.get("mode", "video"), kwargs.get("quality", "best"))
                    except Exception as e:
                        log.exception("start_download_job wrapper failed: %s", e)
                        raise
                except Exception:
                    log.exception("start_download_job wrapper unexpected error")
                    raise
            def get_job_status(self, job_id):
                try:
                    s = self.mod.get_job_status(job_id)
                    if isinstance(s, dict):
                        _normalize_and_merge_into_cache(job_id, s)
                    return s
                except Exception:
                    log.exception("mod.get_job_status failed")
                    with JOB_CACHE_LOCK:
                        return JOB_CACHE.get(job_id)
            def cancel_job(self, job_id):
                try:
                    return self.mod.cancel_job(job_id)
                except Exception:
                    log.exception("mod.cancel_job failed")
                    return False
            def list_history(self):
                try:
                    return self.mod.list_history()
                except Exception:
                    log.exception("mod.list_history failed")
                    return []
        return Wrapper(mod)

    if hasattr(mod, "get_default_manager") and callable(getattr(mod, "get_default_manager")):
        try:
            instance = mod.get_default_manager(progress_callback=download_progress_cb, download_dir=DOWNLOAD_DIR, db_path=DB_PATH)
            class InstWrapper:
                def __init__(self, inst):
                    self.inst = inst
                def start_download_job(self, **kwargs):
                    if hasattr(self.inst, "start_download_job") and callable(getattr(self.inst, "start_download_job")):
                        call_kwargs = {}
                        for k in ("url", "user_id", "mode", "quality", "requested_quality", "is_premium", "enhance_video", "enhance_audio"):
                            if k in kwargs:
                                call_kwargs[k] = kwargs[k]
                        return self.inst.start_download_job(**call_kwargs)
                    raise AttributeError("manager instance has no start_download_job")
                def get_job_status(self, job_id):
                    try:
                        s = self.inst.get_job_status(job_id)
                        if isinstance(s, dict):
                            _normalize_and_merge_into_cache(job_id, s)
                        return s
                    except Exception:
                        log.exception("inst.get_job_status failed")
                        with JOB_CACHE_LOCK:
                            return JOB_CACHE.get(job_id)
                def cancel_job(self, job_id):
                    try:
                        return self.inst.cancel_job(job_id)
                    except Exception:
                        log.exception("inst.cancel_job failed")
                        return False
                def list_history(self):
                    try:
                        return self.inst.list_history()
                    except Exception:
                        log.exception("inst.list_history failed")
                        return []
            return InstWrapper(instance)
        except Exception:
            log.exception("Failed to instantiate get_default_manager from module")

    for cls_name in ("DownloadManager", "SubprocessDownloadManager"):
        if hasattr(mod, cls_name):
            try:
                cls = getattr(mod, cls_name)
                inst = cls(download_dir=DOWNLOAD_DIR, progress_callback=download_progress_cb, db_path=DB_PATH)
                class ClassWrapper:
                    def __init__(self, inst):
                        self.inst = inst
                    def start_download_job(self, **kwargs):
                        if hasattr(self.inst, "start_download_job"):
                            return self.inst.start_download_job(url=kwargs.get("url"), user_id=kwargs.get("user_id"), mode=kwargs.get("mode", "video"), requested_quality=kwargs.get("quality", kwargs.get("requested_quality", "best")), is_premium=kwargs.get("is_premium", False), enhance_video=kwargs.get("enhance_video", False), enhance_audio=kwargs.get("enhance_audio", False))
                        raise AttributeError("download manager instance has no start_download_job")
                    def get_job_status(self, job_id):
                        try:
                            s = self.inst.get_job_status(job_id)
                            if isinstance(s, dict):
                                _normalize_and_merge_into_cache(job_id, s)
                            return s
                        except Exception:
                            log.exception("inst.get_job_status failed")
                            with JOB_CACHE_LOCK:
                                return JOB_CACHE.get(job_id)
                    def cancel_job(self, job_id):
                        try:
                            return self.inst.cancel_job(job_id)
                        except Exception:
                            log.exception("inst.cancel_job failed")
                            return False
                    def list_history(self):
                        try:
                            return self.inst.list_history()
                        except Exception:
                            log.exception("inst.list_history failed")
                            return []
                return ClassWrapper(inst)
            except Exception:
                log.exception("Failed to instantiate class %s from module", cls_name)

    log.warning("download_manager module present but not adaptible; falling back to JOB_CACHE-only behavior")
    class _Fallback:
        def start_download_job(self, **kwargs):
            job_id = uuid.uuid4().hex[:12]
            now_iso = datetime.now(timezone.utc).isoformat()
            with JOB_CACHE_LOCK:
                JOB_CACHE[job_id] = {
                    "job_id": job_id,
                    "url": kwargs.get("url"),
                    "status": "queued",
                    "options": {"enhance_video": kwargs.get("enhance_video"), "enhance_audio": kwargs.get("enhance_audio")},
                    "user_id": kwargs.get("user_id"),
                    "created_at": now_iso,
                    "last_update": now_iso
                }
            return job_id, None
        def get_job_status(self, job_id):
            with JOB_CACHE_LOCK:
                return JOB_CACHE.get(job_id)
        def cancel_job(self, job_id):
            with JOB_CACHE_LOCK:
                j = JOB_CACHE.get(job_id)
                if not j:
                    return False
                j["status"] = "cancelled"
                j["last_update"] = datetime.now(timezone.utc).isoformat()
                JOB_CACHE[job_id] = j
                return True
        def list_history(self):
            with JOB_CACHE_LOCK:
                return list(JOB_CACHE.values())[:HISTORY_MAX]
    return _Fallback()

class FallbackManager:
    def __init__(self):
        self._lock = JOB_CACHE_LOCK
    def start_download_job(self, **kwargs):
        job_id = uuid.uuid4().hex[:12]
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            JOB_CACHE[job_id] = {
                "job_id": job_id,
                "url": kwargs.get("url"),
                "status": "queued",
                "options": {"enhance_video": kwargs.get("enhance_video"), "enhance_audio": kwargs.get("enhance_audio")},
                "user_id": kwargs.get("user_id"),
                "created_at": now_iso,
                "last_update": now_iso
            }
        return job_id, None
    def get_job_status(self, job_id):
        with self._lock:
            return JOB_CACHE.get(job_id)
    def cancel_job(self, job_id):
        with self._lock:
            j = JOB_CACHE.get(job_id)
            if not j:
                return False
            j["status"] = "cancelled"
            j["last_update"] = datetime.now(timezone.utc).isoformat()
            JOB_CACHE[job_id] = j
            return True
    def list_history(self):
        with self._lock:
            return list(JOB_CACHE.values())[:HISTORY_MAX]

# Attempt import of download_manager and ai_manager
try:
    dm_module = importlib.import_module("download_manager")
    log.info("Imported download_manager module: %s", getattr(dm_module, "__file__", "unknown"))
    try:
        dm = _adapt_download_manager_module(dm_module)
        log.info("download_manager adapted successfully")
    except Exception:
        log.exception("Failed to adapt download_manager module")
        dm = FallbackManager()
        log.info("Falling back to in-memory fallback manager")
except Exception as e:
    log.warning("download_manager not available yet: %s — using in-memory fallback manager", e)
    dm = FallbackManager()

try:
    ai_module = importlib.import_module("ai_manager")
    ai = ai_module
    log.info("Imported ai_manager module: %s", getattr(ai_module, "__file__", "unknown"))
except Exception as e:
    log.warning("ai_manager not available yet: %s", e)
    ai = None

# ---------------- Helper wrappers ----------------
def require_premium_or_400(user_id):
    if check_premium(user_id):
        return True, None
    return False, (jsonify({"error": "Upgrade to Premium to access this feature"}), 403)

def _normalize_and_merge_into_cache_for_start(job_id: str, url: str, user_id: Optional[str], enhance_video: bool, enhance_audio: bool):
    now_iso = datetime.now(timezone.utc).isoformat()
    with JOB_CACHE_LOCK:
        JOB_CACHE.setdefault(job_id, {"job_id": job_id, "url": url, "status": "queued", "options": {"enhance_video": enhance_video, "enhance_audio": enhance_audio}, "user_id": user_id, "created_at": now_iso, "last_update": now_iso})

def safe_dm_start(url, mode, quality, enhance_video, enhance_audio, user_id):
    if dm is None:
        return None, "server_not_ready:download_manager_missing"
    try:
        job_id, err = dm.start_download_job(url=url, user_id=user_id, mode=mode, quality=quality, enhance_video=enhance_video, enhance_audio=enhance_audio, is_premium=check_premium(user_id))
        if job_id:
            _normalize_and_merge_into_cache_for_start(job_id, url, user_id, enhance_video, enhance_audio)
        return job_id, err
    except Exception as e:
        log.exception("dm.start_download_job error: %s", e)
        return None, str(e)

def safe_dm_get_status(job_id):
    if dm is None:
        with JOB_CACHE_LOCK:
            return JOB_CACHE.get(job_id)
    try:
        s = dm.get_job_status(job_id)
        with JOB_CACHE_LOCK:
            cached = JOB_CACHE.get(job_id, {})
        if not s:
            return cached or None
        if isinstance(s, dict):
            merged = dict(cached)
            merged.update(s)
            if "file_path" in merged and "file" not in merged:
                try:
                    fname = os.path.basename(merged.get("file_path"))
                    merged["file"] = f"/api/file/{fname}"
                except Exception:
                    pass
            with JOB_CACHE_LOCK:
                JOB_CACHE[job_id] = merged
            return merged
        return s
    except Exception:
        log.exception("dm.get_job_status error")
        with JOB_CACHE_LOCK:
            return JOB_CACHE.get(job_id)

def safe_dm_cancel(job_id):
    if dm is None:
        with JOB_CACHE_LOCK:
            j = JOB_CACHE.get(job_id)
            if not j:
                return False
            j["status"] = "cancelled"
            j["last_update"] = datetime.now(timezone.utc).isoformat()
            JOB_CACHE[job_id] = j
            return True
    try:
        return dm.cancel_job(job_id)
    except Exception:
        log.exception("dm.cancel_job error")
        return False

# ---------------- Smart download message helper ----------------
def smart_download_message(job_info):
    if not job_info:
        return "No job info available (download manager missing or job not found)."
    try:
        options = job_info.get("options", {}) or {}
        mode = job_info.get("mode", "video")
        status = (job_info.get("status") or "").lower()
        enhance_video = bool(options.get("enhance_video") or options.get("enhanceVideo"))
        enhance_audio = bool(options.get("enhance_audio") or options.get("enhanceAudio"))
        user_id = job_info.get("user_id") or job_info.get("owner") or None

        parts = []
        if mode == "audio":
            parts.append("Audio extraction job — ffmpeg is used to ensure quality and format compatibility.")
        else:
            if enhance_video:
                parts.append("Quality-first: server will prefer FFmpeg workflows to deliver the chosen resolution. This is slower but more reliable.")
            else:
                parts.append("Speed-first: yt-dlp direct download is used for fastest results; if the file is faulty we will attempt an FFmpeg repair automatically.")
        if enhance_audio:
            parts.append("Audio enhancement requested.")
        if user_id and check_premium(user_id):
            parts.append("User is premium — higher-quality paths enabled.")
        else:
            parts.append("Free/trial user — limited enhancements / quotas apply.")
        if status == "downloading":
            parts.append("Download in progress.")
        elif status == "processing":
            parts.append("Post-processing underway.")
        elif status == "finished":
            parts.append("Download complete.")
        elif status in ("error", "failed"):
            parts.append("Download encountered an issue; a repair attempt may run.")
        return " ".join(parts)
    except Exception:
        log.exception("smart_download_message error")
        return "Unable to compute smart download message."

# ---------------- Small reconciliation helpers ----------------
def _mark_jobs_finished_for_filename(filename: str):
    if not filename:
        return
    safe_name = os.path.basename(filename)
    webpath = f"/api/file/{safe_name}"
    now_iso = datetime.now(timezone.utc).isoformat()
    updated_jobs = []
    with JOB_CACHE_LOCK:
        for job_id, job in list(JOB_CACHE.items()):
            try:
                file_field = job.get("file") or ""
                local_field = job.get("local_path") or job.get("file_path") or ""
                match = False
                if file_field:
                    if os.path.basename(file_field) == safe_name or file_field == webpath:
                        match = True
                if not match and local_field:
                    if os.path.basename(local_field) == safe_name:
                        match = True
                if match:
                    st = (job.get("status") or "").lower()
                    if st not in ("finished", "error", "cancelled"):
                        job["status"] = "finished"
                        prog = job.get("progress")
                        if isinstance(prog, dict):
                            prog["percent"] = 100
                            job["progress"] = prog
                        else:
                            job["progress"] = {"percent": 100}
                        job["file"] = webpath
                        job["local_path"] = os.path.join(DOWNLOAD_DIR, safe_name)
                        job["finished_at"] = now_iso
                        job["last_update"] = now_iso
                        JOB_CACHE[job_id] = job
                        updated_jobs.append(job_id)
            except Exception:
                log.exception("Error marking job finished for filename %s", filename)
    if updated_jobs:
        log.info("Marked jobs finished for %s -> %s", filename, ", ".join(updated_jobs))
        for jid in updated_jobs:
            try:
                record_download_history(JOB_CACHE.get(jid))
            except Exception:
                log.exception("failed record history after mark finished %s", jid)

def _mark_jobs_deleted_for_filename(filename: str):
    if not filename:
        return
    safe_name = os.path.basename(filename)
    now_iso = datetime.now(timezone.utc).isoformat()
    updated_jobs = []
    with JOB_CACHE_LOCK:
        for job_id, job in list(JOB_CACHE.items()):
            try:
                file_field = job.get("file") or ""
                local_field = job.get("local_path") or ""
                match = False
                if file_field:
                    if os.path.basename(file_field) == safe_name or file_field.endswith(f"/{safe_name}"):
                        match = True
                if not match and local_field:
                    if os.path.basename(local_field) == safe_name:
                        match = True
                if match:
                    st = (job.get("status") or "").lower()
                    if st not in ("deleted", "expired"):
                        job["status"] = "expired"
                        job["deleted_at"] = now_iso
                        job["last_update"] = now_iso
                        JOB_CACHE[job_id] = job
                        updated_jobs.append(job_id)
            except Exception:
                log.exception("Error marking job deleted for filename %s", filename)
    if updated_jobs:
        log.info("Marked jobs deleted for %s -> %s", filename, ", ".join(updated_jobs))
        for jid in updated_jobs:
            try:
                record_download_history(JOB_CACHE.get(jid))
            except Exception:
                log.exception("failed record history after mark deleted %s", jid)

def _find_file_for_job(job_id: str) -> Optional[str]:
    try:
        if not job_id:
            return None
        candidates = []
        for entry in os.listdir(DOWNLOAD_DIR):
            if not entry.startswith(job_id + "."):
                continue
            if entry.endswith(".part") or entry.endswith(".part.tmp") or entry.endswith(".incomplete") or entry.endswith(".temp"):
                continue
            candidates.append(entry)
        if not candidates:
            return None
        for ext in (".mp4", ".m4a", ".mp3", ".webm", ".mkv"):
            for c in candidates:
                if c.lower().endswith(ext):
                    return c
        return candidates[0]
    except Exception:
        log.exception("_find_file_for_job error")
        return None

# ---------------- Utility: midnight / date helpers ----------------
def server_midnight_epoch() -> int:
    # server-local midnight (00:00) epoch seconds
    now = datetime.now()
    midnight = datetime(year=now.year, month=now.month, day=now.day)
    return int(midnight.replace(tzinfo=None).timestamp())

def start_of_today_epoch() -> int:
    return server_midnight_epoch()

def start_of_month_epoch() -> int:
    now = datetime.now()
    mstart = datetime(year=now.year, month=now.month, day=1)
    return int(mstart.replace(tzinfo=None).timestamp())

# ---------------- AI-spend helpers & alerting ----------------
def add_ai_spend(user_id: Optional[str], amount: float):
    try:
        if not user_id:
            user_id = "anonymous"
        ts = int(time.time())
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO ai_spend (user_id, amount, ts) VALUES (?, ?, ?)", (user_id, float(amount), ts))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("Failed to add ai_spend record")

def get_ai_spend_this_month() -> float:
    try:
        start = start_of_month_epoch()
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(SUM(amount),0) as s FROM ai_spend WHERE ts >= ?", (start,))
        row = cur.fetchone()
        conn.close()
        return float(row["s"] or 0.0)
    except Exception:
        log.exception("get_ai_spend_this_month failed")
        return 0.0

def evaluate_ai_limit_and_alert():
    global AI_LIMITED
    try:
        total = get_ai_spend_this_month()
        log.info("AI spend this month: %s (threshold=%s)", total, AI_SPEND_THRESHOLD)
        if total >= AI_SPEND_THRESHOLD:
            if not AI_LIMITED:
                AI_LIMITED = True
                msg = f"AI spend threshold reached: {total} >= {AI_SPEND_THRESHOLD}. AI usage will be limited."
                log.warning(msg)
                try:
                    send_alert_email("VYDRA AI spend threshold reached", msg)
                except Exception:
                    log.exception("Failed to send AI spend alert email")
        else:
            if AI_LIMITED:
                AI_LIMITED = False
                log.info("AI spend back under threshold; lifting limit.")
    except Exception:
        log.exception("evaluate_ai_limit_and_alert failed")

def send_alert_email(subject: str, body: str):
    smtp_host = os.environ.get("VYDRA_SMTP_HOST")
    smtp_port = int(os.environ.get("VYDRA_SMTP_PORT", "587"))
    smtp_user = os.environ.get("VYDRA_SMTP_USER")
    smtp_pass = os.environ.get("VYDRA_SMTP_PASS")
    from_addr = smtp_user or f"{SYSTEM_IDENTITY} <no-reply@vydra.local>"
    to_addr = ALERT_EMAIL_TO
    log.info("Attempting to send alert email to %s (SMTP configured=%s)", to_addr, bool(smtp_host and smtp_user and smtp_pass))
    if not smtp_host or not smtp_user or not smtp_pass:
        log.warning("SMTP not configured; cannot send email. Subject: %s Body: %s", subject, body)
        return False
    try:
        msg = EmailMessage()
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.set_content(body)
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
        log.info("Alert email sent to %s", to_addr)
        return True
    except Exception:
        log.exception("Failed to send alert email")
        return False

# initial evaluation at startup
try:
    evaluate_ai_limit_and_alert()
except Exception:
    log.exception("Initial AI evaluation failed")

# ---------------- Quota helpers ----------------
def check_daily_quota_for_identity(identity: str, limit: int) -> bool:
    """
    DEPRECATED: Smart Unlimited in effect.
    Historically returned whether identity had remaining downloads for the day.
    Under Smart Unlimited we keep this for analytics but always allow downloads.
    """
    try:
        # Keep ability to compute the count for metrics, but never block.
        since = start_of_today_epoch()
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) as cnt FROM downloads_history WHERE user_id=? AND created_at >= ?", (identity, since))
        row = cur.fetchone()
        conn.close()
        cnt = int(row["cnt"]) if row and row["cnt"] is not None else 0
        log.debug("SmartUnlimited: identity=%s downloads_today=%d (limit param=%s)", identity, cnt, limit)
        return True
    except Exception:
        log.exception("quota DB check failed (SmartUnlimited fallback); allowing by default")
        return True

# ---------------- Routes: Download ----------------
@app.route("/api/download", methods=["POST"])
def api_download():
    data = request.json or {}
    user_id = data.get("user_id")
    url = data.get("url")
    batch = data.get("batch")
    mode = data.get("mode", "video")
    quality = data.get("quality", "best")
    enhance_video = bool(data.get("enhanceVideo") or data.get("enhance_video"))
    enhance_audio = bool(data.get("enhanceAudio") or data.get("enhance_audio"))

    # Identity & smart-unlimited handling
    guest_cookie = request.cookies.get("vydra_guest_id")
    set_guest_cookie = False
    if not user_id and not guest_cookie:
        guest_cookie = f"guest_{uuid.uuid4().hex[:12]}"
        set_guest_cookie = True

    identity = user_id or guest_cookie

    # Smart Unlimited: do NOT enforce a daily quota. Keep recording for analytics.
    # Premium users still get premium-only features elsewhere; here all identities may start downloads.

    if batch:
        if not isinstance(batch, list):
            return jsonify({"error": "batch must be array"}), 400
        results = {"job_ids": [], "errors": []}
        for u in batch:
            jid, err = safe_dm_start(u, mode, quality, enhance_video, enhance_audio, identity)
            if jid:
                results["job_ids"].append(jid)
            else:
                results["errors"].append({"url": u, "error": err})
        if not results["job_ids"]:
            return jsonify({"error": "no jobs started", "details": results["errors"]}), 400
        resp = make_response(jsonify(results))
        if not user_id and set_guest_cookie:
            resp.set_cookie("vydra_guest_id", guest_cookie, max_age=30*24*3600, httponly=True)
        return resp

    if not url:
        return jsonify({"error": "missing url"}), 400

    job_id, err = safe_dm_start(url, mode, quality, enhance_video, enhance_audio, identity)
    if not job_id:
        return jsonify({"error": err}), 400

    resp = make_response(jsonify({"job_id": job_id, "status_url": f"/api/status/{job_id}", "progress_url": f"/api/progress/{job_id}"}), 202)
    if not user_id and set_guest_cookie:
        resp.set_cookie("vydra_guest_id", guest_cookie, max_age=30*24*3600, httponly=True)
    return resp

@app.route("/api/status/<job_id>", methods=["GET"])
def api_status(job_id):
    s = safe_dm_get_status(job_id)
    if s is None:
        return jsonify({"error": "job not found or manager missing"}), 404
    try:
        s = dict(s) if not isinstance(s, dict) else s
        s["smart_message"] = smart_download_message(s)
    except Exception:
        log.exception("Failed to attach smart_message to status")
    return jsonify(s)

@app.route("/api/progress/<job_id>", methods=["GET"])
def api_progress(job_id):
    s = safe_dm_get_status(job_id)
    if s is None:
        return jsonify({"error": "job not found or manager missing"}), 404
    try:
        file_field = s.get("file")
        if file_field:
            fname = os.path.basename(file_field)
            path = os.path.join(DOWNLOAD_DIR, fname)
            if os.path.exists(path):
                _mark_jobs_finished_for_filename(fname)
                s = safe_dm_get_status(job_id) or s

        st = (s.get("status") or "").lower()
        if st not in ("finished", "cancelled"):
            found = _find_file_for_job(job_id)
            if found:
                _mark_jobs_finished_for_filename(found)
                s = safe_dm_get_status(job_id) or s

        compact = {
            "job_id": job_id,
            "status": s.get("status"),
            "progress": s.get("progress") or s.get("progress_percent") or None,
            "file": s.get("file"),
            "title": s.get("title"),
            "last_update": s.get("last_update") or s.get("created_at")
        }
        prog = s.get("progress")
        if isinstance(prog, dict):
            compact["progress"] = prog
        return jsonify(compact)
    except Exception:
        log.exception("api_progress error for %s", job_id)
        return jsonify(s)

@app.route("/progress/<job_id>")
def web_progress(job_id):
    return redirect(f"/api/progress/{job_id}")

@app.route("/api/cancel/<job_id>", methods=["POST"])
def api_cancel(job_id):
    ok = safe_dm_cancel(job_id)
    if not ok:
        return jsonify({"error": "cancel failed or manager missing"}), 400
    return jsonify({"ok": True})

@app.route("/api/file/<filename>", methods=["GET"])
def api_file(filename):
    safe = os.path.basename(filename)
    path = os.path.join(DOWNLOAD_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "file not found"}), 404
    try:
        _mark_jobs_finished_for_filename(safe)
    except Exception:
        log.exception("Failed to mark job finished before serving file: %s", safe)
    return send_file(path, as_attachment=True)

@app.route("/api/history", methods=["GET"])
def api_history():
    try:
        if dm and hasattr(dm, "list_history"):
            try:
                hist = dm.list_history()
                if isinstance(hist, list):
                    return jsonify(hist)
            except Exception:
                log.exception("dm.list_history failed")
        with JOB_CACHE_LOCK:
            items = list(JOB_CACHE.values())[-HISTORY_MAX:]
        return jsonify(items)
    except Exception:
        log.exception("api_history error")
        return jsonify([])

@app.route("/history")
def web_history():
    return redirect("/api/history")

# ---------------- Smart message & SSE ----------------
@app.route("/api/smart_message/<job_id>", methods=["GET"])
def api_smart_message(job_id):
    s = safe_dm_get_status(job_id)
    msg = smart_download_message(s)
    return jsonify({"job_id": job_id, "smart_message": msg})

def _sse_format(ev_type, data):
    return f"event: {ev_type}\ndata: {json.dumps(data)}\n\n"

@app.route("/api/stream/<job_id>")
def api_stream(job_id):
    @stream_with_context
    def event_stream():
        last_sent = None
        idle_counter = 0
        while True:
            s = safe_dm_get_status(job_id)
            if s is None:
                yield _sse_format("error", {"message": "job not found or manager missing"})
                break
            try:
                s_local = dict(s) if not isinstance(s, dict) else s
                s_local["smart_message"] = smart_download_message(s_local)
            except Exception:
                s_local = s
            cur_hash = json.dumps(s_local, sort_keys=True)
            if cur_hash != last_sent:
                yield _sse_format("status", s_local)
                last_sent = cur_hash
                idle_counter = 0
            else:
                idle_counter += 1
            st = (s_local.get("status") or "").lower()
            if st in ("finished", "error", "cancelled", "expired"):
                break
            time.sleep(0.8)
            if idle_counter > 300:
                yield _sse_format("warning", {"message": "No progress updates for a while"})
                idle_counter = 0
        s_final = safe_dm_get_status(job_id) or {"status": "unknown"}
        try:
            s_final["smart_message"] = smart_download_message(s_final)
        except Exception:
            pass
        yield _sse_format("status", s_final)
    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return Response(event_stream(), mimetype="text/event-stream", headers=headers)

# ---------------- AI helpers: auto-detect last file ----------------
def _get_identity_from_request(json_data: dict) -> Optional[str]:
    user_id = json_data.get("user_id")
    if user_id:
        return user_id
    guest_cookie = request.cookies.get("vydra_guest_id")
    return guest_cookie

def _get_last_finished_download_for_identity(identity: str) -> Optional[Dict[str, Any]]:
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM downloads_history WHERE user_id=? AND status='finished' AND file_path IS NOT NULL ORDER BY finished_at DESC LIMIT 1", (identity,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        fp = d.get("file_path")
        if fp:
            fname = os.path.basename(fp)
            web = f"/api/file/{fname}"
            if os.path.exists(os.path.join(DOWNLOAD_DIR, fname)):
                d["local_path"] = os.path.join(DOWNLOAD_DIR, fname)
                d["file"] = web
            else:
                d["file"] = web
        return d
    except Exception:
        log.exception("_get_last_finished_download_for_identity failed")
        return None

def _inject_file_into_ai_payload(data: dict, identity: Optional[str]) -> dict:
    if not identity:
        return data
    if data.get("job_id") or data.get("file") or data.get("file_path"):
        return data
    last = _get_last_finished_download_for_identity(identity)
    if not last:
        return data
    data = dict(data)
    if last.get("file"):
        data["file"] = last.get("file")
    if last.get("file_path"):
        data["file_path"] = last.get("file_path")
    if last.get("id"):
        data["job_id"] = last.get("id")
    return data

# ---------------- AI endpoints (delegated to ai_manager) ----------------
@app.route("/api/ai/thumbnail/phase1", methods=["POST"])
def route_ai_thumb_p1():
    data = request.json or {}
    identity = _get_identity_from_request(data)
    ok, resp = require_premium_or_400(identity)
    if not ok:
        return resp
    if AI_LIMITED:
        return jsonify({"error": "AI usage currently limited due to spend limits; try later or contact support."}), 429
    if ai is None:
        return jsonify({"error": "ai_manager not available"}), 500
    try:
        data = _inject_file_into_ai_payload(data, identity)
        return jsonify(ai.generate_thumbnail_phase1(data))
    except Exception as e:
        log.exception("ai.generate_thumbnail_phase1 error: %s", e)
        return jsonify({"error": "ai error", "details": str(e)}), 500

@app.route("/api/ai/thumbnail/phase2", methods=["POST"])
def route_ai_thumb_p2():
    data = request.json or {}
    identity = _get_identity_from_request(data)
    ok, resp = require_premium_or_400(identity)
    if not ok:
        return resp
    if AI_LIMITED:
        return jsonify({"error": "AI usage currently limited due to spend limits; try later or contact support."}), 429
    if ai is None:
        return jsonify({"error": "ai_manager not available"}), 500
    try:
        data = _inject_file_into_ai_payload(data, identity)
        return jsonify(ai.generate_thumbnail_phase2(data))
    except Exception as e:
        log.exception("ai.generate_thumbnail_phase2 error: %s", e)
        return jsonify({"error": "ai error", "details": str(e)}), 500

@app.route("/api/ai/thumbnail/phase3", methods=["POST"])
def route_ai_thumb_p3():
    data = request.json or {}
    identity = _get_identity_from_request(data)
    ok, resp = require_premium_or_400(identity)
    if not ok:
        return resp
    if AI_LIMITED:
        return jsonify({"error": "AI usage currently limited due to spend limits; try later or contact support."}), 429
    if ai is None:
        return jsonify({"error": "ai_manager not available"}), 500
    try:
        data = _inject_file_into_ai_payload(data, identity)
        return jsonify(ai.generate_thumbnail_phase3(data))
    except Exception as e:
        log.exception("ai.generate_thumbnail_phase3 error: %s", e)
        return jsonify({"error": "ai error", "details": str(e)}), 500

@app.route("/api/ai/thumbnail/phase4", methods=["POST"])
def route_ai_thumb_p4():
    data = request.json or {}
    identity = _get_identity_from_request(data)
    ok, resp = require_premium_or_400(identity)
    if not ok:
        return resp
    if AI_LIMITED:
        return jsonify({"error": "AI usage currently limited due to spend limits; try later or contact support."}), 429
    if ai is None:
        return jsonify({"error": "ai_manager not available"}), 500
    try:
        data = _inject_file_into_ai_payload(data, identity)
        return jsonify(ai.generate_thumbnail_phase4(data))
    except Exception as e:
        log.exception("ai.generate_thumbnail_phase4 error: %s", e)
        return jsonify({"error": "ai error", "details": str(e)}), 500

@app.route("/api/ai/hashtags", methods=["POST"])
def route_ai_hashtags():
    data = request.json or {}
    identity = _get_identity_from_request(data)
    ok, resp = require_premium_or_400(identity)
    if not ok:
        return resp
    if AI_LIMITED:
        return jsonify({"error": "AI usage currently limited due to spend limits; try later or contact support."}), 429
    if ai is None:
        return jsonify({"error": "ai_manager not available"}), 500
    try:
        data = _inject_file_into_ai_payload(data, identity)
        add_ai_spend(identity, 0.05)
        evaluate_ai_limit_and_alert()
        return jsonify(ai.generate_hashtags(data))
    except Exception as e:
        log.exception("ai.generate_hashtags error: %s", e)
        return jsonify({"error": "ai error", "details": str(e)}), 500

@app.route("/api/ai/audio/enhance", methods=["POST"])
def route_ai_audio_enhance():
    data = request.json or {}
    identity = _get_identity_from_request(data)
    ok, resp = require_premium_or_400(identity)
    if not ok:
        return resp
    if AI_LIMITED:
        return jsonify({"error": "AI usage currently limited due to spend limits; try later or contact support."}), 429
    if ai is None:
        return jsonify({"error": "ai_manager not available"}), 500
    try:
        data = _inject_file_into_ai_payload(data, identity)
        add_ai_spend(identity, 0.2)
        evaluate_ai_limit_and_alert()
        return jsonify(ai.enhance_audio_ai(data))
    except Exception as e:
        log.exception("ai.enhance_audio_ai error: %s", e)
        return jsonify({"error": "ai error", "details": str(e)}), 500

# ---------------- Referral & Rewards ----------------
INVITE_REWARDS = {
    "invite_5": {"threshold": 5, "reward": "1 week Premium", "days": 7},
    "invite_10": {"threshold": 10, "reward": "2 weeks Premium", "days": 14},
    "invite_20": {"threshold": 20, "reward": "3 weeks Premium", "days": 21},
    "invite_60": {"threshold": 60, "reward": "3 months Premium", "days": 90},
}

@app.route("/r/<referrer_id>")
def ref_redirect(referrer_id):
    visitor_ip = request.remote_addr or ""
    user_agent = request.headers.get("User-Agent", "")
    try:
        cookie_name = f"v_ref_{referrer_id}"
        if not request.cookies.get(cookie_name):
            increment_visit(referrer_id, visitor_ip, user_agent)
            resp = make_response(redirect("/"))
            resp.set_cookie(cookie_name, "1", max_age=24*3600, httponly=True)
            return resp
        else:
            return redirect("/")
    except Exception:
        log.exception("ref_redirect error")
        return redirect("/")

@app.route("/api/generate_reward/<user_id>", methods=["POST"])
def api_generate_reward(user_id):
    data = request.json or {}
    milestone = data.get("milestone")
    token = secrets.token_urlsafe(24)
    expires = int(time.time()) + 7 * 24 * 3600
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO reward_tokens (token, user_id, milestone, expires_at, claimed) VALUES (?,?,?,?,0)", (token, user_id, milestone, expires))
    conn.commit()
    conn.close()
    claim_url = f"{request.host_url.rstrip('/')}" + f"/reward/{token}"
    return jsonify({"claim_url": claim_url})

@app.route("/api/claim_invite_reward/<user_id>", methods=["POST"])
def api_claim_invite_reward(user_id):
    data = request.get_json(force=True) or {}
    milestone = data.get("milestone")
    if milestone not in INVITE_REWARDS:
        return jsonify({"error": "Invalid milestone"}), 400
    ensure_user_row(user_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT total_invites FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    total_invites = int(row["total_invites"]) if row and row["total_invites"] is not None else 0
    threshold = INVITE_REWARDS[milestone]["threshold"]
    if total_invites < threshold:
        conn.close()
        return jsonify({"error": "Not enough invites yet"}), 400
    cur.execute("SELECT COUNT(1) as cnt FROM claimed_invite_rewards WHERE user_id=? AND milestone=?", (user_id, milestone))
    already = cur.fetchone()
    already_cnt = int(already["cnt"]) if already and already["cnt"] is not None else 0
    if already_cnt:
        conn.close()
        return jsonify({"error": "Reward already claimed"}), 400
    cur.execute("INSERT INTO claimed_invite_rewards (user_id, milestone) VALUES (?,?)", (user_id, milestone))
    conn.commit()
    conn.close()
    days = INVITE_REWARDS[milestone]["days"]
    new_expires = activate_plan(user_id, days)
    return jsonify({"ok": True, "message": f"Invite reward claimed: {INVITE_REWARDS[milestone]['reward']}", "premium_expires_at": new_expires})

@app.route("/api/start_trial", methods=["POST"])
def api_start_trial():
    data = request.json or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400
    ensure_user_row(user_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM trial_claims WHERE user_id=?", (user_id,))
    if cur.fetchone():
        conn.close()
        return jsonify({"ok": False, "message": "Trial already claimed for this user."}), 400
    cur.execute("INSERT INTO trial_claims (user_id) VALUES (?)", (user_id,))
    conn.commit()
    conn.close()
    expires = activate_plan(user_id, 21)
    return jsonify({"ok": True, "message": "Trial started", "expires_at": expires})

# ---------------- Paystack integration ----------------
@app.route('/api/paystack/initialize', methods=['POST'])
def paystack_initialize():
    data = request.json or {}
    email = data.get("email")
    amount = data.get("amount")
    plan = data.get("plan", "weekly")
    metadata = data.get("metadata", {})
    user_id = data.get("user_id")
    if not email or amount is None:
        return jsonify({"error": "email and amount required"}), 400
    secret = PAYSTACK_SECRET_KEY
    if not secret:
        log.error("PAYSTACK_SECRET_KEY not set")
        return jsonify({"error": "PAYSTACK_SECRET_KEY not set on server"}), 500
    try:
        amount_int = int(amount)
    except Exception:
        return jsonify({"error": "Invalid amount"}), 400
    body = {
        "email": email,
        "amount": amount_int,
        "metadata": {"plan": plan, **(metadata or {}), "user_id": user_id},
        "callback_url": f"{request.host_url.rstrip('/')}" + "/api/paystack/callback"
    }
    headers = {"Authorization": f"Bearer {secret}", "Content-Type": "application/json"}
    try:
        r = requests.post("https://api.paystack.co/transaction/initialize", json=body, headers=headers, timeout=15)
        payload = r.json()
        data_field = payload.get("data") or {}
        reference = data_field.get("reference")
        if reference:
            record_payment(reference, user_id=user_id, email=email, plan=plan, amount=amount_int, status=data_field.get("status") or "initialized", paystack_response=payload)
        return jsonify(payload), r.status_code
    except Exception as e:
        log.exception("paystack initialize failed: %s", e)
        return jsonify({"error": "Failed to contact Paystack", "details": str(e)}), 500

@app.route("/api/paystack/verify/<reference>", methods=["GET"])
def paystack_verify(reference):
    secret = PAYSTACK_SECRET_KEY
    if not secret:
        return jsonify({"error": "PAYSTACK_SECRET_KEY not set on server"}), 500
    try:
        r = requests.get(f"https://api.paystack.co/transaction/verify/{reference}", headers={"Authorization": f"Bearer {secret}"}, timeout=15)
        data = r.json()
    except Exception as e:
        log.exception("paystack verify request failed: %s", e)
        return jsonify({"error": "Paystack verification request failed", "details": str(e)}), 500
    if not r.ok:
        try:
            record_payment(reference, paystack_response=data, status="verify_failed")
        except Exception:
            pass
        return jsonify({"ok": False, "data": data}), 400
    txn = data.get("data") or {}
    status = txn.get("status")
    metadata = txn.get("metadata") or {}
    plan = metadata.get("plan") or "weekly"
    user_id = metadata.get("user_id") or (txn.get("customer") or {}).get("email")
    amount = txn.get("amount")
    try:
        record_payment(reference, user_id=user_id, email=(txn.get("customer") or {}).get("email"), plan=plan, amount=amount, status=status, paystack_response=data)
    except Exception:
        log.exception("Failed to record payment after verify")
    result = {"ok": True, "verified": status == "success", "paystack": txn}
    if status == "success" and user_id:
        pay = get_payment_by_reference(reference)
        if pay and pay.get("processed"):
            result["premium_granted"] = False
            result["note"] = "Already processed"
        else:
            days_map = {"weekly": 7, "monthly": 30, "quarter": 90, "yearly": 365}
            days = days_map.get(plan, 7)
            try:
                new_expires = activate_plan(user_id, days)
                result["premium_granted"] = True
                result["premium_expires_at"] = new_expires
                mark_payment_processed(reference)
            except Exception as e:
                result["premium_granted"] = False
                result["premium_error"] = str(e)
    return jsonify(result)

@app.route('/webhook/paystack', methods=['POST'])
def paystack_webhook():
    raw = request.get_data()
    sig = request.headers.get('x-paystack-signature')
    if not PAYSTACK_SECRET_KEY:
        return jsonify({'error': 'missing PAYSTACK_SECRET_KEY'}), 500
    computed = hmac.new(PAYSTACK_SECRET_KEY.encode(), raw, hashlib.sha512).hexdigest()
    if not hmac.compare_digest(computed, (sig or '')):
        log.warning("Invalid Paystack signature on webhook")
        return jsonify({'error': 'invalid signature'}), 400
    payload = request.json or {}
    event = payload.get('event')
    data = payload.get('data', {}) or {}
    try:
        reference = data.get('reference')
        status = data.get('status') or payload.get('event')
        if reference:
            record_payment(reference, user_id=(data.get('metadata') or {}).get('user_id') or (data.get('customer') or {}).get('email'),
                           email=(data.get('customer') or {}).get('email'),
                           plan=(data.get('metadata') or {}).get('plan'),
                           amount=data.get('amount'),
                           status=status,
                           paystack_response=payload)
    except Exception:
        log.exception("Error recording webhook payload")
    try:
        if event == 'charge.success' or (data.get('status') == 'success'):
            reference = data.get('reference')
            metadata = data.get('metadata') or {}
            user_id = metadata.get('user_id') or (data.get('customer') or {}).get('email')
            plan = metadata.get('plan')
            if not reference:
                return jsonify({'status': 'ok'}), 200
            pay = get_payment_by_reference(reference)
            if pay and pay.get("processed"):
                return jsonify({'status': 'ok'}), 200
            days_map = {"weekly": 7, "monthly": 30, "quarter": 90, "yearly": 365}
            days = days_map.get(plan, 7)
            if user_id and plan in days_map:
                try:
                    activate_plan(user_id, days)
                    mark_payment_processed(reference)
                    log.info("Webhook: granted premium for %s (plan=%s) ref=%s", user_id, plan, reference)
                except Exception:
                    log.exception("Webhook: failed to grant premium")
            # Optional Supabase sync (best-effort)
            if SUPABASE_URL and SUPABASE_KEY:
                try:
                    url = SUPABASE_URL.rstrip('/') + '/rest/v1/subscriptions'
                    headers = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json', 'Prefer': 'return=representation'}
                    payload2 = {'user_id': user_id, 'email': (data.get('customer') or {}).get('email'), 'plan': plan, 'start_date': datetime.now(timezone.utc).isoformat(), 'expiry_date': (datetime.now(timezone.utc)+timedelta(days=days)).isoformat(), 'paystack_reference': reference}
                    requests.post(url, headers=headers, json=payload2, timeout=10)
                except Exception:
                    log.exception("supabase webhook insert error")
    except Exception:
        log.exception("webhook handling error")
    return jsonify({'status': 'ok'})

@app.route("/api/paystack/callback")
def paystack_callback():
    ref = request.args.get("reference") or request.args.get("trxref")
    frontend = os.environ.get("FRONTEND_ORIGIN", "http://localhost:3000")
    if ref:
        return redirect(f"{frontend.rstrip('/')}" + f"/payment-success?reference={ref}")
    return redirect(frontend)

@app.route("/api/payments/<reference>")
def api_get_payment(reference):
    pay = get_payment_by_reference(reference)
    if not pay:
        return jsonify({"error": "Not found"}), 404
    pr = pay.get("paystack_response")
    try:
        pay["paystack_response"] = json.loads(pr) if pr else {}
    except Exception:
        pay["paystack_response"] = pr
    return jsonify(pay)

# ---------------- Health / Admin / cleanup ----------------
@app.route("/_health")
def health():
    return jsonify({"ok": True, "ts": int(time.time()), "ffmpeg": HAVE_FFMPEG, "yt_dlp": HAVE_YTDLP, "ai_limited": AI_LIMITED})

def do_cleanup_once():
    """
    History cleanup policy (per-user):
      - Keep most recent MAX_RECENT_DOWNLOADS finished files per user (by finished_at)
      - Remove files older than HISTORY_FILE_TTL_SECS (3 hours) regardless of count
      - Also remove orphan files not referenced in DB if older than HISTORY_FILE_TTL_SECS
    """
    removed = []
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        # 1) For every user (including guest ids stored as strings), prune older finished downloads beyond MAX_RECENT_DOWNLOADS
        cur.execute("SELECT DISTINCT user_id FROM downloads_history WHERE user_id IS NOT NULL")
        rows = cur.fetchall() or []
        users = [r["user_id"] for r in rows if r and r["user_id"]]
        now_ts = int(time.time())
        for uid in users:
            cur.execute("SELECT id, file_path, finished_at FROM downloads_history WHERE user_id=? AND status='finished' ORDER BY finished_at DESC", (uid,))
            items = cur.fetchall() or []
            keep = items[:MAX_RECENT_DOWNLOADS]
            to_remove = items[MAX_RECENT_DOWNLOADS:]
            for rem in to_remove:
                fp = rem["file_path"]
                if not fp:
                    try:
                        cur.execute("DELETE FROM downloads_history WHERE id=?", (rem["id"],))
                        conn.commit()
                    except Exception:
                        log.exception("Failed to delete downloads_history row id=%s", rem["id"])
                    continue
                # normalize file path: if it's a web path, translate to local file
                fname = os.path.basename(fp)
                full = os.path.join(DOWNLOAD_DIR, fname)
                try:
                    _mark_jobs_deleted_for_filename(fname)
                except Exception:
                    log.exception("mark deleted failed for %s", fname)
                try:
                    if os.path.exists(full):
                        os.remove(full)
                        removed.append(full)
                        log.info("Removed old file (per-user limit): %s", full)
                except Exception:
                    log.exception("Failed to remove %s", full)
                try:
                    cur.execute("DELETE FROM downloads_history WHERE id=?", (rem["id"],))
                    conn.commit()
                except Exception:
                    log.exception("Failed to delete downloads_history row id=%s", rem["id"])
        # 2) Remove files older than HISTORY_FILE_TTL_SECS (3 hours) regardless of user
        cutoff = int(time.time()) - HISTORY_FILE_TTL_SECS
        cur.execute("SELECT id, file_path, finished_at FROM downloads_history WHERE finished_at IS NOT NULL")
        all_finished = cur.fetchall() or []

        delete_ids = []
        for row in all_finished:
            try:
                fin = int(row["finished_at"]) if row["finished_at"] else 0
                if fin and fin < cutoff:
                    fp = row["file_path"]
                    # try to remove local file
                    if fp:
                        fname = os.path.basename(fp)
                        fullpath = os.path.join(DOWNLOAD_DIR, fname)
                        try:
                            if os.path.exists(fullpath):
                                os.remove(fullpath)
                                log.info("Auto-deleted expired file: %s", fullpath)
                        except Exception:
                            log.exception("Failed to remove expired file %s", fullpath)
                    delete_ids.append(row["id"])
            except Exception as e:
                log.warning("Cleanup skipped row due to error: %s", e)
                continue

        if delete_ids:
            try:
                cur.executemany("DELETE FROM downloads_history WHERE id = ?", [(i,) for i in delete_ids])
                conn.commit()
                log.info("Batch-cleaned %d old history records (age > %ds)", len(delete_ids), HISTORY_FILE_TTL_SECS)
            except Exception:
                log.exception("Batch cleanup failed")

        # 3) Orphan cleanup: any file in DOWNLOAD_DIR older than HISTORY_FILE_TTL_SECS and not referenced in DB
        for fn in os.listdir(DOWNLOAD_DIR):
            if fn.startswith("."):
                continue
            full = os.path.join(DOWNLOAD_DIR, fn)
            try:
                mtime = os.path.getmtime(full)
            except Exception:
                continue
            if (time.time() - mtime) > HISTORY_FILE_TTL_SECS:
                try:
                    cur.execute("SELECT COUNT(1) as cnt FROM downloads_history WHERE file_path LIKE ?", (f"%{fn}%",))
                    row2 = cur.fetchone()
                    cnt = int(row2["cnt"]) if row2 and row2["cnt"] is not None else 0
                except Exception:
                    cnt = 0
                if cnt == 0:
                    try:
                        os.remove(full)
                        removed.append(full)
                        log.info("Removed orphan old file: %s", full)
                    except Exception:
                        log.exception("Failed to remove orphan file %s", full)
        conn.close()
    except Exception:
        log.exception("Cleanup worker error")
    return removed

def cleanup_worker():
    while True:
        try:
            do_cleanup_once()
        except Exception:
            log.exception("cleanup_worker encountered an error")
        time.sleep(CLEANUP_INTERVAL_SECS)

if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    threading.Thread(target=cleanup_worker, daemon=True).start()

@app.route('/api/admin/cleanup_now', methods=['POST'])
def api_admin_cleanup():
    token = request.headers.get('X-ADMIN-TOKEN') or request.args.get('token')
    if not ADMIN_TOKEN or (token != ADMIN_TOKEN):
        return jsonify({'error': 'unauthorized'}), 403
    removed = do_cleanup_once()
    return jsonify({'removed': removed})

# Admin dashboard metrics endpoint (requires ADMIN_TOKEN header; optional check of ADMIN_EMAIL)
@app.route("/api/admin-dashboard", methods=["GET"])
def api_admin_dashboard():
    token = request.headers.get("X-ADMIN-TOKEN") or request.args.get("token")
    server_token = os.environ.get("VYDRA_ADMIN_TOKEN") or ADMIN_TOKEN or ""
    allowed_admin_email = os.environ.get("VYDRA_ADMIN_EMAIL")
    if not server_token or token != server_token:
        return jsonify({"error": "unauthorized"}), 403
    client_email = request.headers.get("X-ADMIN-EMAIL") or request.args.get("admin_email")
    if allowed_admin_email and client_email:
        if client_email.lower() != allowed_admin_email.lower():
            return jsonify({"error": "forbidden"}), 403
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) as cnt FROM users")
        total_users = int(cur.fetchone()["cnt"] or 0)
        try:
            cur.execute("SELECT COUNT(1) as cnt FROM premium_subscriptions WHERE expires_at > ?", (int(time.time()),))
            total_premium_active = int(cur.fetchone()["cnt"] or 0)
        except Exception:
            cur.execute("SELECT COUNT(1) as cnt FROM premium_subscriptions WHERE expires_at IS NOT NULL")
            total_premium_active = int(cur.fetchone()["cnt"] or 0)
        cur.execute("SELECT COUNT(1) as cnt FROM downloads_history")
        total_downloads = int(cur.fetchone()["cnt"] or 0)
        cur.execute("SELECT COALESCE(SUM(total_invites),0) as s FROM users")
        total_referrals = int(cur.fetchone()["s"] or 0)
        try:
            cur.execute("SELECT COALESCE(SUM(amount),0) as s FROM payments WHERE created_at >= (strftime('%s','now','-30 days'))")
            revenue_sum = cur.fetchone()["s"] or 0
            estimated_monthly_revenue = float(revenue_sum)
        except Exception:
            estimated_monthly_revenue = 0.0
        ai_spend_month = get_ai_spend_this_month()
        resp = {
            "total_users": total_users,
            "total_premium": total_premium_active,
            "total_downloads": total_downloads,
            "total_referrals": total_referrals,
            "estimated_monthly_revenue": estimated_monthly_revenue,
            "ai_spend_this_month": ai_spend_month,
            "ai_limited": AI_LIMITED,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
        conn.close()
        return jsonify(resp)
    except Exception:
        log.exception("admin-dashboard failed")
        return jsonify({"error": "internal_error"}), 500

# ---------------- Some admin helpers ----------------
@app.route("/api/admin/add_visits/<user_id>/<int:n>", methods=["POST"])
def admin_add_visits(user_id, n):
    ensure_user_row(user_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET total_visits = total_visits + ?, visits_today = visits_today + ?, visits_month = visits_month + ? WHERE user_id=?", (n, n, n, user_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/admin/add_invites/<user_id>/<int:n>", methods=["POST"])
def admin_add_invites(user_id, n):
    ensure_user_row(user_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET total_invites = total_invites + ? WHERE user_id=?", (n, user_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/admin/grant_premium/<user_id>/<int:days>", methods=["POST"])
def admin_grant_premium(user_id, days):
    new_expires = activate_plan(user_id, days)
    return jsonify({"ok": True, "new_expires": new_expires})

# ---------------- Plan route & premium status ----------------
@app.route("/api/my-plan/<user_id>", methods=["GET"])
def get_my_plan(user_id):
    plan_data = get_user_plan(user_id)
    return jsonify(plan_data), 200

@app.route("/api/premium_status/<user_id>")
def api_premium_status(user_id):
    ok = check_premium(user_id)
    return jsonify({"premium": bool(ok), "expires_at": premium_expires_at(user_id)})

# ---------------- Midnight maintenance (resets visits_today, evaluates AI spend monthly rollover) ----------------
def midnight_maintenance_loop():
    """
    Runs continuously and performs daily and monthly system maintenance:
     - At midnight (server local): reset users.visits_today to 0
     - Clean up expired premium_subscriptions (epoch or ISO format supported)
     - On the first day of each month: evaluate AI spend and send summary email
    """
    last_midnight_day = None
    last_month = None

    while True:
        try:
            now = datetime.now()
            today_day = now.day
            this_month = now.month

            # ---------------- DAILY RESET ----------------
            if last_midnight_day != today_day:
                try:
                    conn = get_db_conn()
                    cur = conn.cursor()
                    cur.execute("UPDATE users SET visits_today = 0")
                    conn.commit()
                    conn.close()
                    log.info("✅ Midnight maintenance: reset visits_today for all users.")
                except Exception:
                    log.exception("❌ Midnight maintenance failed to reset visits_today")

                # ---------------- PREMIUM CLEANUP ----------------
                try:
                    conn = get_db_conn()
                    cur = conn.cursor()
                    cur.execute("SELECT user_id, expires_at FROM premium_subscriptions WHERE expires_at IS NOT NULL")
                    rows = cur.fetchall() or []

                    for r in rows:
                        uid = r["user_id"]
                        ex_epoch = r["expires_at"]
                        dt = None

                        if ex_epoch:
                            # Try to interpret both epoch and ISO datetime strings
                            try:
                                # If stored as numeric epoch (e.g. 1736054400)
                                dt = _epoch_to_dt(int(ex_epoch))
                            except (ValueError, TypeError):
                                try:
                                    # If ISO string (e.g. "2026-10-08T08:11:38.322708+00:00")
                                    dt = datetime.fromisoformat(str(ex_epoch).replace("Z", "+00:00"))
                                except Exception:
                                    dt = None

                        # If a valid datetime and expired
                        if dt and dt < datetime.now(timezone.utc):
                            try:
                                cur.execute("DELETE FROM premium_subscriptions WHERE user_id=?", (uid,))
                                conn.commit()
                                if uid in USER_PLANS:
                                    USER_PLANS.pop(uid, None)
                                log.info(f"💡 Removed expired premium for user {uid}")
                            except Exception:
                                log.exception(f"Failed to remove expired premium for user {uid}")

                    conn.close()
                except Exception:
                    log.exception("❌ Midnight maintenance premium cleanup failed")

                last_midnight_day = today_day

            # ---------------- MONTHLY AI SPEND CHECK ----------------
            if last_month != this_month:
                try:
                    evaluate_ai_limit_and_alert()
                    summary = f"Monthly maintenance: AI spend this month so far: {get_ai_spend_this_month()}"
                    send_alert_email(f"VYDRA monthly summary - {now.strftime('%Y-%m-%d')}", summary)
                    log.info(f"📊 Monthly AI spend check completed for {now.strftime('%B %Y')}")
                except Exception:
                    log.exception("❌ Monthly maintenance error")

                last_month = this_month

        except Exception:
            log.exception("midnight_maintenance_loop general error")

        # Sleep 60 seconds before re-check
        time.sleep(60)


# Launch maintenance thread (only once in production mode)
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    threading.Thread(target=midnight_maintenance_loop, daemon=True).start()

# ---------------- Graceful shutdown helpers ----------------
def _shutdown_manager():
    global dm
    try:
        if dm and hasattr(dm, "shutdown"):
            try:
                dm.shutdown()
                log.info("Called dm.shutdown()")
            except Exception:
                log.exception("Error while calling dm.shutdown()")
        try:
            mod = importlib.import_module("download_manager")
            if hasattr(mod, "shutdown"):
                try:
                    mod.shutdown()
                    log.info("Called download_manager.shutdown()")
                except Exception:
                    log.exception("download_manager.shutdown() failed")
        except Exception:
            pass
    except Exception:
        log.exception("Error in _shutdown_manager")

def _signal_handler(signum, frame):
    log.info("Signal received (%s) — shutting down manager and exiting.", signum)
    _shutdown_manager()
    try:
        os._exit(0)
    except Exception:
        pass

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)
atexit.register(_shutdown_manager)

# ---------- START: Render-ready bottom of app.py ----------
if __name__ == "__main__":
    log.info("Starting VYDRA backend (local/dev) on 0.0.0.0:%s", PORT)
    # debug=False to mimic production. Set to True while actively developing.
    app.run(host="0.0.0.0", port=PORT, debug=False)
# ---------- END: Render-ready bottom of app.py ----------