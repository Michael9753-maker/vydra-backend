#!/usr/bin/env python3
"""
app.py - VYDRA backend (production-minded)

Features:
 - yt-dlp download runner (single + batch)
 - size / duration checks
 - background cleanup worker
 - SQLite referral / invite / reward / subscription tables
 - Paystack initialize, verify, webhook, callback (reads PAYSTACK_SECRET_KEY from .env)
 - lightweight AI endpoints (thumbnail, audio/video enhance, caption generator)
"""

import os
import time
import uuid
import threading
import traceback
import sqlite3
import secrets
import requests
import hmac
import hashlib
import subprocess
from datetime import datetime, timedelta, timezone
from collections import deque
from flask import Flask, request, jsonify, send_file, redirect, make_response
from flask_cors import CORS
import yt_dlp
from PIL import Image, ImageDraw, ImageFont
import logging
from dotenv import load_dotenv

# Load .env
load_dotenv()

# ---------------- Config ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

PORT = int(os.environ.get("VYDRA_PORT", 5000))
CLEANUP_SECONDS = int(os.environ.get("VYDRA_CLEANUP_SECONDS", 1800))
HISTORY_MAX = int(os.environ.get("VYDRA_HISTORY_MAX", 200))
MAX_RECENT_DOWNLOADS = int(os.environ.get("VYDRA_MAX_RECENT_DOWNLOADS", 5))

MAX_DURATION_SECONDS = int(os.environ.get("VYDRA_MAX_DURATION_SECONDS", 20 * 60))
MAX_FILESIZE_BYTES = int(os.environ.get("VYDRA_MAX_FILESIZE_BYTES", 300 * 1024 * 1024))

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
PAYSTACK_SECRET_KEY = os.environ.get("PAYSTACK_SECRET_KEY")
WA_LINK = os.environ.get("WA_LINK", "https://wa.link/rcptsq")

DB_PATH = os.path.join(BASE_DIR, "vydra_referrals.db")
FRONTEND_ORIGINS = os.environ.get("FRONTEND_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger("vydra")

# ---------------- Flask ----------------
app = Flask(__name__)
origins = [o.strip() for o in FRONTEND_ORIGINS.split(",") if o.strip()]
CORS(app, resources={r"/*": {"origins": origins}})

# ---------------- SQLite helpers ----------------
def get_db_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reward_tokens (
        token TEXT PRIMARY KEY,
        user_id TEXT,
        milestone TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP,
        claimed INTEGER DEFAULT 0,
        claimed_at TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS claimed_invite_rewards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT,
        milestone TEXT,
        claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS premium_subscriptions (
        user_id TEXT PRIMARY KEY,
        expires_at TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trial_claims (
        user_id TEXT PRIMARY KEY,
        claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    conn.close()

init_db()

# ---------------- Job state ----------------
jobs = {}
job_history = deque(maxlen=HISTORY_MAX)

# ---------------- Utilities ----------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def safe_filename(name):
    return os.path.basename(name or "")

def ensure_user_row(user_id):
    if not user_id:
        return
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if not cur.fetchone():
        cur.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
    conn.close()

def increment_visit(referrer_id, visitor_ip, user_agent):
    if not referrer_id:
        return
    ensure_user_row(referrer_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO visit_log (referrer_id, visitor_ip, user_agent) VALUES (?,?,?)",
                (referrer_id, visitor_ip, user_agent))
    cur.execute("UPDATE users SET total_visits = total_visits + 1, visits_today = visits_today + 1, visits_month = visits_month + 1 WHERE user_id=?",
                (referrer_id,))
    conn.commit()
    conn.close()

def get_stats_for_user(user_id):
    if not user_id:
        return {"totalVisits": 0, "visitsToday": 0, "visitsThisMonth": 0, "totalInvites": 0}
    ensure_user_row(user_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT total_visits, visits_today, visits_month, total_invites FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return {
            "totalVisits": int(row["total_visits"] or 0),
            "visitsToday": int(row["visits_today"] or 0),
            "visitsThisMonth": int(row["visits_month"] or 0),
            "totalInvites": int(row["total_invites"] or 0)
        }
    return {"totalVisits": 0, "visitsToday": 0, "visitsThisMonth": 0, "totalInvites": 0}

def add_invite_count(referrer_id, amount=1):
    if not referrer_id:
        return
    ensure_user_row(referrer_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET total_invites = total_invites + ? WHERE user_id=?", (amount, referrer_id))
    conn.commit()
    conn.close()

def grant_premium(user_id, days):
    if not user_id:
        raise ValueError("user_id required to grant premium")
    ensure_user_row(user_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    now = datetime.now(timezone.utc)
    if row and row["expires_at"]:
        try:
            existing = datetime.fromisoformat(row["expires_at"])
            if existing.tzinfo is None:
                existing = existing.replace(tzinfo=timezone.utc)
        except Exception:
            existing = now
    else:
        existing = now
    start = existing if existing > now else now
    new_expires = start + timedelta(days=days)
    cur.execute("INSERT OR REPLACE INTO premium_subscriptions (user_id, expires_at) VALUES (?, ?)",
                (user_id, new_expires.isoformat()))
    conn.commit()
    conn.close()
    log.info("Granted premium: %s for %d days -> expires %s", user_id, days, new_expires.isoformat())
    return new_expires.isoformat()

def check_premium(user_id):
    if not user_id:
        return False
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if not row or not row["expires_at"]:
        return False
    try:
        expires = datetime.fromisoformat(row["expires_at"])
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return datetime.now(timezone.utc) < expires

def premium_expires_at(user_id):
    if not user_id:
        return None
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row["expires_at"] if row and row["expires_at"] else None

# ---------------- yt-dlp & downloads ----------------
def parse_percent(percent_str):
    if percent_str is None:
        return None
    try:
        if isinstance(percent_str, (int, float)):
            return int(round(float(percent_str)))
        if isinstance(percent_str, str):
            p = percent_str.strip().replace("%", "")
            return int(round(float(p)))
    except Exception:
        pass
    return None

def ytdlp_hook(d, job_id):
    try:
        if job_id not in jobs:
            return
        status = d.get("status")
        if status == "downloading":
            percent = parse_percent(d.get("_percent_str") or d.get("percent"))
            jobs[job_id]["status"] = "downloading"
            jobs[job_id]["progress"] = {
                "percent": percent if percent is not None else jobs[job_id].get("progress", {}).get("percent", 0),
                "speed": d.get("_speed_str") or d.get("speed"),
                "eta": d.get("eta"),
                "stage": "downloading",
            }
        elif status == "finished":
            jobs[job_id]["status"] = "processing"
            jobs[job_id]["progress"] = {"percent": 100, "stage": "postprocessing"}
    except Exception:
        traceback.print_exc()

def estimate_size_bytes(info, quality, mode):
    try:
        size = int(info.get("filesize") or info.get("filesize_approx") or 0)
    except Exception:
        size = 0
    formats = info.get("formats") or []
    if not formats:
        return size
    height_map = {"360p": 360, "480p": 480, "720p": 720, "1080p": 1080, "1440p": 1440, "2160p": 2160, "best": 999999}
    max_height = height_map.get(quality, 999999)
    best_candidate = 0
    for f in formats:
        try:
            fsize = int(f.get("filesize") or f.get("filesize_approx") or 0)
        except Exception:
            fsize = 0
        if mode == "audio":
            if f.get("vcodec") in (None, "none") or (f.get("acodec") and not f.get("vcodec")):
                if fsize > best_candidate:
                    best_candidate = fsize
        else:
            height = f.get("height") or 0
            if height and height <= max_height:
                if fsize > best_candidate:
                    best_candidate = fsize
            if not height and fsize > best_candidate:
                best_candidate = fsize
    if best_candidate:
        return best_candidate
    return size

def cleanup_downloads(max_files=MAX_RECENT_DOWNLOADS):
    try:
        files = [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR) if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]
        files.sort(key=os.path.getmtime, reverse=True)
        deleted = []
        for f in files[max_files:]:
            try:
                os.remove(f)
                deleted.append(os.path.basename(f))
                log.info("Deleted old file %s", f)
            except Exception as e:
                log.warning("Failed to delete %s: %s", f, e)
        if deleted:
            for job in list(job_history):
                file_rel = job.get("file", "")
                if file_rel and any(fn in file_rel for fn in deleted):
                    try:
                        job_history.remove(job)
                    except ValueError:
                        pass
    except Exception as e:
        log.exception("cleanup_downloads error: %s", e)

def run_download(job_id, url, mode, quality, enhance_video, enhance_audio, preferred_audio_codec="mp3", preferred_bitrate="192"):
    try:
        ydl_opts = {
            "outtmpl": os.path.join(DOWNLOAD_DIR, f"{job_id}.%(ext)s"),
            "merge_output_format": "mp4",
            "progress_hooks": [lambda d: ytdlp_hook(d, job_id)],
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
        }

        # format / postprocessing
        if mode == "audio":
            codec = (preferred_audio_codec or "mp3").lower()
            if codec not in ("mp3", "m4a", "aac", "opus"):
                codec = "mp3"
            ydl_opts.update({
                "format": "bestaudio/best",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": codec,
                    "preferredquality": str(preferred_bitrate or "192"),
                }],
            })
        else:
            if quality == "360p":
                ydl_opts["format"] = "bestvideo[height<=360]+bestaudio/best"
            elif quality == "480p":
                ydl_opts["format"] = "bestvideo[height<=480]+bestaudio/best"
            elif quality == "720p":
                ydl_opts["format"] = "bestvideo[height<=720]+bestaudio/best"
            elif quality == "1080p":
                ydl_opts["format"] = "bestvideo[height<=1080]+bestaudio/best"
            else:
                ydl_opts["format"] = "bestvideo+bestaudio/best"

        info = None
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                log.info("Starting download job %s for %s (mode=%s, quality=%s)", job_id, url, mode, quality)
                info = ydl.extract_info(url, download=True)
        except Exception as e_inner:
            log.warning("Primary download attempt failed: %s", e_inner)
            if mode != "audio" and ydl_opts.get("format") != "bestvideo+bestaudio/best":
                try:
                    ydl_opts["format"] = "bestvideo+bestaudio/best"
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                except Exception as e2:
                    log.exception("Fallback download failed: %s", e2)
                    raise
            else:
                raise

        final_file = None
        for ext_try in ["mp3", "m4a", "mp4", "mkv", "webm"]:
            candidate = os.path.join(DOWNLOAD_DIR, f"{job_id}.{ext_try}")
            if os.path.exists(candidate):
                final_file = candidate
                break

        if not final_file and info:
            title = info.get("title") or ""
            safe_base = "".join(c for c in title if c.isalnum() or c in (" ", "-", "_")).strip().replace(" ", "_")
            for f in os.listdir(DOWNLOAD_DIR):
                if safe_base and safe_base in f:
                    final_file = os.path.join(DOWNLOAD_DIR, f)
                    break

        if not final_file:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = "Downloaded file not found after yt-dlp run"
            log.error("Job %s finished but file not found", job_id)
            return

        title = (info.get("title") or os.path.basename(url))
        tags = info.get("tags") or []
        hashtags = []
        if tags:
            for t in tags[:6]:
                hashtags.append("#" + "".join(str(t).strip().split()))
        else:
            words = [w for w in title.split() if len(w) > 2]
            for w in words[:4]:
                hashtags.append("#" + "".join(w.strip().split()))

        jobs[job_id].update({
            "id": job_id,
            "url": url,
            "mode": mode,
            "options": {"quality": quality, "enhance_video": bool(enhance_video), "enhance_audio": bool(enhance_audio)},
            "status": "finished",
            "file": f"/file/{os.path.basename(final_file)}",
            "history": {"title": title, "hashtags": hashtags},
            "time": datetime.now(timezone.utc).isoformat()
        })
        job_history.appendleft(jobs[job_id].copy())
        log.info("Job %s finished; file: %s", job_id, final_file)
        cleanup_downloads(MAX_RECENT_DOWNLOADS)
    except Exception as e:
        if job_id in jobs:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)
        log.exception("run_download error for job %s: %s", job_id, e)

# ---------------- Routes ----------------
@app.route("/download", methods=["POST"])
def download():
    data = request.json or {}
    url = data.get("url")
    batch = data.get("batch")
    mode = data.get("mode", "video")
    quality = data.get("quality", "best")
    enhance_video = data.get("enhanceVideo", False) or data.get("enhance_video", False)
    enhance_audio = data.get("enhanceAudio", False) or data.get("enhance_audio", False)

    MAX_BATCH = 12

    def start_single_job(target_url):
        if not target_url:
            return (None, "Empty URL")
        try:
            with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
                info = ydl.extract_info(target_url, download=False)
        except Exception as e:
            return (None, f"Could not extract metadata: {str(e)}")

        duration = info.get("duration")
        if not duration:
            return (None, "Could not determine video duration")
        if duration > MAX_DURATION_SECONDS:
            return (None, f"Video duration ({int(duration)}s) exceeds limit of {MAX_DURATION_SECONDS}s")

        est_size = estimate_size_bytes(info, quality, mode)
        if not est_size:
            return (None, "Could not estimate download size from metadata")
        if est_size > MAX_FILESIZE_BYTES:
            return (None, f"Estimated size {(est_size/1024/1024):.1f}MB exceeds limit of {(MAX_FILESIZE_BYTES/1024/1024):.0f}MB")

        job_id = str(uuid.uuid4())
        jobs[job_id] = {"status": "downloading", "progress": {"percent": 0, "stage": "queued"}, "type": mode}
        t = threading.Thread(target=run_download, args=(job_id, target_url, mode, quality, enhance_video, enhance_audio), daemon=True)
        t.start()
        return (job_id, None)

    if batch:
        if not isinstance(batch, list):
            return jsonify({"error": "Batch must be an array of URLs"}), 400
        if len(batch) == 0:
            return jsonify({"error": "Batch array is empty"}), 400
        if len(batch) > MAX_BATCH:
            return jsonify({"error": f"Batch too large (max {MAX_BATCH} links)"}), 400

        results = {"job_ids": [], "errors": []}
        for entry in batch:
            if not entry or not isinstance(entry, str):
                results["errors"].append({"url": entry, "error": "Invalid url entry"})
                continue
            entry = entry.strip()
            if not entry:
                results["errors"].append({"url": entry, "error": "Empty url"})
                continue
            job_id, err = start_single_job(entry)
            if job_id:
                results["job_ids"].append(job_id)
            else:
                results["errors"].append({"url": entry, "error": err})
        if not results["job_ids"]:
            return jsonify({"error": "No jobs started", "details": results["errors"]}), 400
        return jsonify(results)

    if not url:
        return jsonify({"error": "Missing url"}), 400

    job_id, err = start_single_job(url)
    if not job_id:
        return jsonify({"error": err}), 400
    return jsonify({"job_id": job_id})

@app.route("/progress/<job_id>")
def route_progress(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "No such job"}), 404
    resp = dict(job)
    resp["job_id"] = job_id
    if "progress" in resp:
        try:
            resp["progress"]["percent"] = int(resp["progress"].get("percent") or 0)
        except Exception:
            resp["progress"]["percent"] = 0
    return jsonify(resp)

@app.route("/cancel/<job_id>", methods=["POST"])
def route_cancel(job_id):
    if job_id not in jobs:
        return jsonify({"error": "No such job"}), 404
    jobs[job_id]["status"] = "cancelled"
    jobs[job_id]["progress"] = {"percent": jobs[job_id].get("progress", {}).get("percent", 0), "stage": "cancelled"}
    return jsonify({"ok": True})

@app.route("/file/<filename>")
def route_file(filename):
    safe = safe_filename(filename)
    path = os.path.join(DOWNLOAD_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path, as_attachment=True)

@app.route("/history")
def route_history():
    history_list = []
    for job in list(job_history):
        file_rel = job.get("file")
        if file_rel:
            filename = file_rel.replace("/file/", "")
            path = os.path.join(DOWNLOAD_DIR, filename)
            if not os.path.exists(path):
                try:
                    job_history.remove(job)
                except ValueError:
                    pass
                continue
        history_list.append({
            "id": job.get("id"),
            "title": job.get("history", {}).get("title", "Unknown"),
            "hashtags": job.get("history", {}).get("hashtags", []),
            "file": job.get("file"),
            "url": job.get("url"),
            "mode": job.get("mode"),
            "time": job.get("time"),
            "status": job.get("status"),
        })
    return jsonify(history_list)

# ---------------- Referral & Rewards ----------------
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
    except Exception as e:
        log.exception("ref_redirect error: %s", e)
        return redirect("/")

@app.route("/api/stats/<user_id>")
def api_get_stats(user_id):
    stats = get_stats_for_user(user_id)
    stats["isPremium"] = check_premium(user_id)
    stats["premiumExpiresAt"] = premium_expires_at(user_id)
    return jsonify(stats)

@app.route("/api/premium_status/<user_id>")
def api_premium_status(user_id):
    return jsonify({"isPremium": check_premium(user_id), "expiresAt": premium_expires_at(user_id)})

@app.route("/api/generate_reward/<user_id>", methods=["POST"])
def api_generate_reward(user_id):
    data = request.json or {}
    milestone = data.get("milestone")
    stats = get_stats_for_user(user_id)
    milestone_thresholds = {"20k_month": 20000, "50k_month": 50000, "100k_month": 100000, "500k_month": 500000}
    if milestone not in milestone_thresholds:
        return jsonify({"error": "Unknown milestone"}), 400
    needed = milestone_thresholds[milestone]
    if stats["visitsThisMonth"] < needed:
        return jsonify({"error": "Milestone not reached"}), 400
    token = secrets.token_urlsafe(24)
    expires = datetime.now(timezone.utc) + timedelta(days=7)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO reward_tokens (token, user_id, milestone, expires_at, claimed) VALUES (?,?,?,?,0)",
                (token, user_id, milestone, expires.isoformat()))
    conn.commit()
    conn.close()
    claim_url = f"{request.host_url.rstrip('/')}/reward/{token}"
    return jsonify({"claim_url": claim_url})

@app.route("/reward/<token>")
def claim_reward(token):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, milestone, expires_at, claimed FROM reward_tokens WHERE token=?", (token,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return "Invalid or expired token", 404
    user_id, milestone, expires_at, claimed = row["user_id"], row["milestone"], row["expires_at"], row["claimed"]
    try:
        expires_dt = datetime.fromisoformat(expires_at)
        if expires_dt.tzinfo is None:
            expires_dt = expires_dt.replace(tzinfo=timezone.utc)
    except Exception:
        expires_dt = datetime.now(timezone.utc) - timedelta(days=1)
    if claimed:
        conn.close()
        return "This reward link has already been used.", 410
    if datetime.now(timezone.utc) > expires_dt:
        conn.close()
        return "This reward link has expired.", 410
    cur.execute("UPDATE reward_tokens SET claimed=1, claimed_at=CURRENT_TIMESTAMP WHERE token=?", (token,))
    conn.commit()
    conn.close()
    return redirect(WA_LINK)

INVITE_REWARDS = {
    "invite_5": {"threshold": 5, "reward": "1 week Premium", "days": 7},
    "invite_10": {"threshold": 10, "reward": "2 weeks Premium", "days": 14},
    "invite_20": {"threshold": 20, "reward": "3 weeks Premium", "days": 21},
    "invite_60": {"threshold": 60, "reward": "3 months Premium", "days": 90},
}

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
    total_invites = row["total_invites"] if row else 0
    threshold = INVITE_REWARDS[milestone]["threshold"]
    if total_invites < threshold:
        conn.close()
        return jsonify({"error": "Not enough invites yet"}), 400
    cur.execute("SELECT COUNT(1) as cnt FROM claimed_invite_rewards WHERE user_id=? AND milestone=?", (user_id, milestone))
    already = cur.fetchone()["cnt"]
    if already:
        conn.close()
        return jsonify({"error": "Reward already claimed"}), 400
    cur.execute("INSERT INTO claimed_invite_rewards (user_id, milestone) VALUES (?,?)", (user_id, milestone))
    conn.commit()
    conn.close()
    days = INVITE_REWARDS[milestone]["days"]
    new_expires = grant_premium(user_id, days)
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
    expires = grant_premium(user_id, 21)
    return jsonify({"ok": True, "message": "Trial started", "expires_at": expires})

# ---------------- Paystack integration ----------------
@app.route('/api/paystack/initialize', methods=['POST'])
def paystack_initialize():
    data = request.json or {}
    email = data.get("email")
    amount = data.get("amount")
    plan = data.get("plan", "weekly")
    metadata = data.get("metadata", {})
    if not email or amount is None:
        return jsonify({"error": "email and amount required"}), 400
    secret = PAYSTACK_SECRET_KEY
    if not secret:
        log.error("PAYSTACK_SECRET_KEY not set")
        return jsonify({"error": "PAYSTACK_SECRET_KEY not set on server"}), 500

    # Paystack expects amount in kobo (smallest currency unit)
    try:
        amount_int = int(amount)
    except Exception:
        return jsonify({"error": "Invalid amount"}), 400

    body = {
        "email": email,
        "amount": amount_int,
        "metadata": {"plan": plan, **(metadata or {})},
        "callback_url": f"{request.host_url.rstrip('/')}/api/paystack/callback"
    }
    headers = {"Authorization": f"Bearer {secret}", "Content-Type": "application/json"}
    try:
        r = requests.post("https://api.paystack.co/transaction/initialize", json=body, headers=headers, timeout=15)
        return jsonify(r.json()), r.status_code
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
        return jsonify({"ok": False, "data": data}), 400
    txn = data.get("data") or {}
    status = txn.get("status")
    metadata = txn.get("metadata") or {}
    plan = metadata.get("plan") or "weekly"
    user_id = metadata.get("user_id") or (txn.get("customer") or {}).get("email")
    result = {"ok": True, "verified": status == "success", "paystack": txn}
    if status == "success" and user_id:
        days_map = {"weekly": 7, "monthly": 30, "quarter": 90, "yearly": 365}
        days = days_map.get(plan, 7)
        try:
            new_expires = grant_premium(user_id, days)
            result["premium_granted"] = True
            result["premium_expires_at"] = new_expires
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
        return jsonify({'error': 'invalid signature'}), 400
    payload = request.json or {}
    event = payload.get('event')
    data = payload.get('data', {})
    if event == 'charge.success':
        metadata = data.get('metadata') or {}
        reference = data.get('reference')
        user_id = metadata.get('user_id') or metadata.get('email')
        plan = metadata.get('plan')
        if user_id and plan in ('weekly', 'monthly', 'quarter', 'yearly'):
            try:
                days = 7 if plan == 'weekly' else (30 if plan == 'monthly' else (90 if plan == 'quarter' else 365))
                grant_premium(user_id, days)
                if SUPABASE_URL and SUPABASE_KEY:
                    try:
                        url = SUPABASE_URL.rstrip('/') + '/rest/v1/subscriptions'
                        headers = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json', 'Prefer': 'return=representation'}
                        payload2 = {'user_id': user_id, 'email': metadata.get('email'), 'plan': plan, 'start_date': datetime.now(timezone.utc).isoformat(), 'expiry_date': (datetime.now(timezone.utc)+timedelta(days=days)).isoformat(), 'paystack_reference': reference}
                        requests.post(url, headers=headers, json=payload2, timeout=10)
                    except Exception as e:
                        log.exception("supabase webhook insert error: %s", e)
            except Exception:
                log.exception("webhook handling error")
    return jsonify({'status': 'ok'})

@app.route("/api/paystack/callback")
def paystack_callback():
    # Paystack will redirect here with ?reference
    ref = request.args.get("reference") or request.args.get("trxref")
    frontend = os.environ.get("FRONTEND_ORIGIN", "http://localhost:3000")
    if ref:
        return redirect(f"{frontend.rstrip('/')}/payment-success?reference={ref}")
    return redirect(frontend)
# ---------------- AI phases (Thumbnail / Hashtags / Noise) ----------------
# Requires: ffmpeg, ffprobe installed on server for frame extraction; PIL/Pillow imported earlier.

import sqlite3
from datetime import datetime

# helper: record hashtag learning
def record_hashtag_usage(tag, platform=None, engagement=0.0):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, times_used, engagement_score FROM ai_hashtag_learning WHERE tag=? AND platform=?",
                    (tag, platform or "generic"))
        row = cur.fetchone()
        if row:
            _id, times_used, score = row["id"], (row["times_used"] or 0), (row["engagement_score"] or 0.0)
            cur.execute("UPDATE ai_hashtag_learning SET times_used = ?, engagement_score = ?, last_used = CURRENT_TIMESTAMP WHERE id = ?",
                        (times_used + 1, (score + float(engagement))/2.0, _id))
        else:
            cur.execute("INSERT INTO ai_hashtag_learning (tag, platform, times_used, engagement_score) VALUES (?,?,1,?)",
                        (tag, platform or "generic", float(engagement)))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("record_hashtag_usage error")

def fetch_hashtag_stats(tag, platform=None):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT times_used, engagement_score FROM ai_hashtag_learning WHERE tag=? AND platform=?", (tag, platform or "generic"))
        row = cur.fetchone()
        conn.close()
        if row:
            return {"times_used": row["times_used"] or 0, "engagement_score": float(row["engagement_score"] or 0.0)}
    except Exception:
        pass
    return {"times_used": 0, "engagement_score": 0.0}

# ---------- THUMBNAIL PHASES (1..4) ----------
# core util (re-uses your existing helpers where possible)
def _extract_frame(in_path, out_path, ts=None, target_w=1280):
    # uses the extract_frame_with_ffmpeg logic from your existing code if present, else fallback
    try:
        # compute ts using ffprobe if None
        if ts is None:
            try:
                cmd_info = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "default=noprint_wrappers=1:nokey=1", in_path]
                out = subprocess.check_output(cmd_info, stderr=subprocess.STDOUT).decode().strip()
                dur = float(out) if out else None
            except Exception:
                dur = None
            ts = max(2.0, dur/2.0) if dur and dur > 4.0 else 2.0
        scale_arg = f"scale={target_w}:-2"
        cmd = ["ffmpeg", "-y", "-ss", str(float(ts)), "-i", in_path, "-frames:v", "1", "-q:v", "2", "-vf", scale_arg, out_path]
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True, None
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr.decode(errors="ignore") if e.stderr else str(e))
        log.warning("frame extraction failed: %s", stderr)
        return False, stderr
    except Exception as e:
        log.exception("frame extraction error: %s", e)
        return False, str(e)

@app.route("/api/ai/thumbnail/phase1", methods=["POST"])
def ai_thumbnail_phase1():
    """
    Phase 1: basic thumbnail generation
      - If file provided and available locally -> extract single middle frame and overlay simple title.
      - Else -> PIL fallback title image.
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    title = (data.get("title") or "VYDRA").strip()
    file_param = data.get("file")
    token = secrets.token_urlsafe(8)
    out_name = f"thumbnail-p1-{token}.png"
    out_path = os.path.join(DOWNLOAD_DIR, out_name)

    # attempt frame extract
    in_path = None
    if file_param:
        fname = file_param.replace("/file/", "").lstrip("/")
        candidate = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
        if os.path.exists(candidate):
            in_path = candidate

    if in_path:
        ok_e, err = _extract_frame(in_path, out_path, ts=None, target_w=1280)
        if not ok_e:
            log.info("phase1: extraction failed (%s) - PIL fallback", err)
            # fallback to PIL simple generator
            _ = fallback_pil_thumbnail_helper(out_path, title)
    else:
        # PIL fallback
        _ = fallback_pil_thumbnail_helper(out_path, title)

    if not os.path.exists(out_path):
        return jsonify({"error": "thumbnail failed"}), 500
    return jsonify({"thumbnail_file": f"/file/{out_name}", "phase": 1})

@app.route("/api/ai/thumbnail/phase2", methods=["POST"])
def ai_thumbnail_phase2():
    """
    Phase 2: multi-frame selection (3 candidate frames)
     - Extract 3 frames at different points (25%, 50%, 75%) where possible
     - Return list of candidate thumbnail file paths: /file/...
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    title = (data.get("title") or "VYDRA").strip()
    token_base = secrets.token_urlsafe(6)
    candidates = []
    if not file_param:
        # no video available -> return 3 PIL variants with slight color tweaks
        for i in range(3):
            out_name = f"thumbnail-p2-{token_base}-{i}.png"
            out_path = os.path.join(DOWNLOAD_DIR, out_name)
            fallback_pil_thumbnail_helper(out_path, f"{title} • variant {i+1}", accent_shift=i)
            candidates.append(f"/file/{out_name}")
        return jsonify({"candidates": candidates, "phase": 2})

    # locate local file
    fname = file_param.replace("/file/", "").lstrip("/")
    in_path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(in_path):
        # fallback to PIL
        for i in range(3):
            out_name = f"thumbnail-p2-{token_base}-{i}.png"
            out_path = os.path.join(DOWNLOAD_DIR, out_name)
            fallback_pil_thumbnail_helper(out_path, f"{title} • variant {i+1}", accent_shift=i)
            candidates.append(f"/file/{out_name}")
        return jsonify({"candidates": candidates, "phase": 2})

    # attempt to find duration and extract at 25%,50%,75%
    try:
        cmd_info = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1", in_path]
        out = subprocess.check_output(cmd_info, stderr=subprocess.STDOUT).decode().strip()
        dur = float(out) if out else None
    except Exception:
        dur = None
    points = [2.0, 4.0, 6.0]
    if dur and dur > 10:
        points = [max(1.0, dur*0.25), max(2.0, dur*0.5), max(3.0, dur*0.75)]

    for idx, p in enumerate(points):
        out_name = f"thumbnail-p2-{token_base}-{idx}.png"
        out_path = os.path.join(DOWNLOAD_DIR, out_name)
        ok_e, err = _extract_frame(in_path, out_path, ts=p, target_w=1280)
        if not ok_e or not os.path.exists(out_path):
            # fallback PIL for this candidate
            fallback_pil_thumbnail_helper(out_path, f"{title} • variant {idx+1}", accent_shift=idx)
        else:
            # overlay simple title
            try:
                img = Image.open(out_path).convert("RGBA")
                draw = ImageDraw.Draw(img)
                font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                try:
                    font = ImageFont.truetype(font_path, 40) if os.path.exists(font_path) else ImageFont.load_default()
                except Exception:
                    font = ImageFont.load_default()
                W, H = img.size
                label = title
                padding = 10
                tw, th = draw.textsize(label, font=font)
                od = Image.new("RGBA", img.size)
                od_draw = ImageDraw.Draw(od)
                od_draw.rectangle([50, H - th - 60, 50 + tw + padding*2, H - 40], fill=(0,0,0,160))
                img = Image.alpha_composite(img, od)
                draw = ImageDraw.Draw(img)
                draw.text((50 + padding, H - th - 55), label, font=font, fill=(255,255,255,255))
                img = img.convert("RGB")
                img.save(out_path, "PNG")
            except Exception:
                log.exception("phase2 overlay failed")
        candidates.append(f"/file/{out_name}")

    return jsonify({"candidates": candidates, "phase": 2})

@app.route("/api/ai/thumbnail/phase3", methods=["POST"])
def ai_thumbnail_phase3():
    """
    Phase 3: AI enhancement step (optional external AI service).
    - Accepts `use_ai` boolean and will attempt to POST the extracted image to THUMBNAIL_AI_URL env var.
    - Falls back to local improvement (PIL contrast / sharpen) when AI unavailable.
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")  # expected to be local extracted thumbnail path (e.g. /file/thumbnail-p2-..)
    use_ai = bool(data.get("use_ai"))
    result = {"ai_attempted": False, "ai_ok": False, "phase": 3}

    if not file_param:
        return jsonify({"error": "Missing file param to enhance"}), 400

    fname = file_param.replace("/file/", "").lstrip("/")
    path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404

    AI_URL = os.environ.get("THUMBNAIL_AI_URL")
    AI_KEY = os.environ.get("THUMBNAIL_AI_KEY")

    if use_ai and AI_URL:
        # attempt external AI call
        try:
            with open(path, "rb") as fh:
                files = {"image": ("thumb.png", fh, "image/png")}
                headers = {}
                if AI_KEY:
                    headers["Authorization"] = f"Bearer {AI_KEY}"
                r = requests.post(AI_URL, files=files, headers=headers, timeout=60)
            result["ai_attempted"] = True
            if r.ok and "image" in (r.headers.get("Content-Type","") or ""):
                with open(path, "wb") as outfh:
                    outfh.write(r.content)
                result["ai_ok"] = True
            else:
                result["ai_ok"] = False
                result["ai_msg"] = f"bad_response:{r.status_code}"
        except Exception as e:
            log.exception("phase3 AI request failed: %s", e)
            result["ai_ok"] = False
            result["ai_msg"] = str(e)

    if not result["ai_ok"]:
        # local enhancement: simple unsharp mask + auto-contrast
        try:
            img = Image.open(path).convert("RGB")
            from PIL import ImageFilter, ImageOps
            img = ImageOps.autocontrast(img, cutoff=1)
            img = img.filter(ImageFilter.UnsharpMask(radius=1, percent=120, threshold=3))
            img.save(path, "PNG")
            result["note"] = "local_enhanced"
        except Exception as e:
            log.exception("local enhance failed: %s", e)
            result["note"] = "local_enhance_failed"

    return jsonify(result)

@app.route("/api/ai/thumbnail/phase4", methods=["POST"])
def ai_thumbnail_phase4():
    """
    Phase 4: Emotion-aware composition + mobile/desktop variants
      - Tries to detect bright/dark average and applies simple grading.
      - Produces two output files: desktop (1280x720) and vertical mobile (720x1280).
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    title = (data.get("title") or "").strip()
    if not file_param:
        return jsonify({"error": "Missing base thumbnail file"}), 400

    fname = file_param.replace("/file/", "").lstrip("/")
    path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404

    token = secrets.token_urlsafe(6)
    desktop_name = f"thumbnail-p4-desktop-{token}.png"
    mobile_name = f"thumbnail-p4-mobile-{token}.png"
    desktop_path = os.path.join(DOWNLOAD_DIR, desktop_name)
    mobile_path = os.path.join(DOWNLOAD_DIR, mobile_name)

    try:
        img = Image.open(path).convert("RGB")
        W, H = img.size
        # compute average brightness
        stat = ImageStat.Stat(img.convert("L"))
        avg = stat.mean[0] if stat.mean else 128
        # simple grade: if dark -> lift shadows; if bright -> increase contrast
        from PIL import ImageEnhance
        if avg < 100:
            enhancer = ImageEnhance.Brightness(img)
            img2 = enhancer.enhance(1.15)
        else:
            enhancer = ImageEnhance.Contrast(img)
            img2 = enhancer.enhance(1.08)
        # desktop crop/resize
        img_desktop = img2.resize((1280, int(1280 * H / W))) if W != 1280 else img2
        img_desktop = ImageOps.fit(img_desktop, (1280, 720), method=Image.Resampling.LANCZOS)
        img_desktop.save(desktop_path, "PNG")
        # mobile vertical
        img_mobile = ImageOps.fit(img2, (720, 1280), method=Image.Resampling.LANCZOS)
        img_mobile.save(mobile_path, "PNG")
        return jsonify({"desktop": f"/file/{desktop_name}", "mobile": f"/file/{mobile_name}", "phase": 4})
    except Exception as e:
        log.exception("phase4 composition failed: %s", e)
        return jsonify({"error": "phase4 failed", "details": str(e)}), 500

# ---------- HASHTAG PHASES (1..5) ----------
@app.route("/api/ai/hashtags/phase1", methods=["POST"])
def ai_hashtags_phase1():
    """
    Phase1: simple hashtag extraction from title / description (no learning)
    """
    data = request.json or {}
    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()
    if not title and not description:
        return jsonify({"hashtags": []})
    text = (title + " " + description).strip()
    words = [w.strip().lower().strip("#,.!?()[]{}") for w in text.split() if len(w) > 3][:12]
    tags = []
    seen = set()
    for w in words:
        tag = "#" + "".join(ch for ch in w if ch.isalnum())
        if tag not in seen:
            tags.append(tag)
            seen.add(tag)
        if len(tags) >= 8:
            break
    return jsonify({"hashtags": tags, "phase": 1})

@app.route("/api/ai/hashtags/phase2", methods=["POST"])
def ai_hashtags_phase2():
    """
    Phase2: platform-optimized tags. Accept 'platform' param: 'tiktok','youtube','instagram'
    Applies simple heuristics to adjust tag count and verbosity.
    """
    data = request.json or {}
    platform = (data.get("platform") or "generic").lower()
    res = ai_hashtags_phase1().get_json()
    tags = res.get("hashtags", [])
    # heuristics
    if platform == "tiktok":
        # tiktok: shorter, trendy single-word tags, pick top 6
        tags = [t for t in tags][:6]
    elif platform in ("instagram", "ig"):
        # IG: allow more descriptive tags, up to 12
        tags = [t for t in tags] + ["#instagood"]  # gentle hint
        tags = tags[:12]
    else:
        tags = tags[:8]
    return jsonify({"hashtags": tags, "platform": platform, "phase": 2})

@app.route("/api/ai/hashtags/phase3", methods=["POST"])
def ai_hashtags_phase3():
    """
    Phase3: ranking + scoring using simple statistics (length, historical usage)
    """
    data = request.json or {}
    platform = (data.get("platform") or "generic").lower()
    base = ai_hashtags_phase2().get_json()
    tags = base.get("hashtags", [])
    scored = []
    for t in tags:
        stats = fetch_hashtag_stats(t, platform)
        score = (stats["times_used"] * 0.6) + (stats["engagement_score"] * 10) - (len(t) * 0.1)
        scored.append({"tag": t, "score": score})
    scored.sort(key=lambda x: x["score"], reverse=True)
    ranked = [s["tag"] for s in scored]
    return jsonify({"hashtags": ranked, "ranked": True, "phase": 3})

@app.route("/api/ai/hashtags/phase4", methods=["POST"])
def ai_hashtags_phase4():
    """
    Phase4: adaptive learning — records the generated hashtags to the local learning DB (no heavy ML).
    Returns top tags + stores usages.
    """
    data = request.json or {}
    tags = data.get("tags")
    platform = (data.get("platform") or "generic").lower()
    if not tags:
        # generate first then record
        gen = ai_hashtags_phase3().get_json()
        tags = gen.get("hashtags", [])
    # record every tag with a default engagement 0.0
    for t in tags:
        try:
            record_hashtag_usage(t, platform, engagement=0.0)
        except Exception:
            pass
    return jsonify({"hashtags": tags, "learned": True, "phase": 4})

@app.route("/api/ai/hashtags/phase5", methods=["POST"])
def ai_hashtags_phase5():
    """
    Phase5: feedback endpoint: frontend should POST actual engagement data here to improve the learner
    payload: { tag: "#tag", platform: "tiktok", engagement: <float 0.0-1.0> }
    """
    data = request.json or {}
    tag = data.get("tag")
    platform = (data.get("platform") or "generic").lower()
    engagement = float(data.get("engagement") or 0.0)
    if not tag:
        return jsonify({"error": "Missing tag"}), 400
    try:
        record_hashtag_usage(tag, platform, engagement=engagement)
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("phase5 feedback failed: %s", e)
        return jsonify({"error": "write failed", "details": str(e)}), 500

# ---------- NOISE REDUCTION PHASES (1..3) ----------
@app.route("/api/ai/noise/phase1", methods=["POST"])
def ai_noise_phase1():
    """
    Phase1: Basic ffmpeg filters for audio/video denoise
      - For audio files: loudnorm + afftdn (if available)
      - For videos: apply hqdn3d filter
    Request: { user_id, file }
    Returns: enhanced file path
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    if not file_param:
        return jsonify({"error": "Missing file"}), 400
    fname = file_param.replace("/file/", "").lstrip("/")
    in_path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(in_path):
        return jsonify({"error": "file not found"}), 404

    token = secrets.token_urlsafe(8)
    if fname.lower().endswith((".mp3", ".m4a", ".wav", ".aac", ".ogg")) or data.get("mode") == "audio":
        out_name = f"denoised-audio-{token}.mp3"
        out_path = os.path.join(DOWNLOAD_DIR, out_name)
        cmd = ["ffmpeg", "-y", "-i", in_path, "-af", "afftdn=nf=-25,aresample=44100,loudnorm=I=-16:TP=-1.5:LRA=11", out_path]
    else:
        out_name = f"denoised-video-{token}.mp4"
        out_path = os.path.join(DOWNLOAD_DIR, out_name)
        # keep quality; apply hqdn3d
        cmd = ["ffmpeg", "-y", "-i", in_path, "-vf", "hqdn3d=1.5:1.5:6:6", "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "copy", out_path]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return jsonify({"enhanced": f"/file/{out_name}", "phase": 1})
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors="ignore") if e.stderr else str(e)
        log.exception("noise phase1 ffmpeg failed: %s", stderr)
        return jsonify({"error": "ffmpeg failed", "details": stderr}), 500

@app.route("/api/ai/noise/phase2", methods=["POST"])
def ai_noise_phase2():
    """
    Phase2: neural denoise (RNNoise) if available on server, else fallback to phase1.
    Expects 'use_neural' boolean.
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    use_neural = bool(data.get("use_neural"))
    file_param = data.get("file")
    if not file_param:
        return jsonify({"error": "Missing file"}), 400
    fname = file_param.replace("/file/", "").lstrip("/")
    in_path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(in_path):
        return jsonify({"error": "file not found"}), 404

    token = secrets.token_urlsafe(8)
    out_name = f"denoised-nn-{token}.wav"
    out_path = os.path.join(DOWNLOAD_DIR, out_name)

    # RNNoise binary expected (rnnoise_demo) or so; try, otherwise fallback
    if use_neural:
        try:
            # attempt a simple ffmpeg->pipe->rnnoise flow if rnnoise available
            # We'll try to call an external 'rnnoise' command if present; otherwise fallback
            rnnoise_cmd = shutil.which("rnnoise") or shutil.which("rnnoise_demo")
            if rnnoise_cmd:
                # convert to wav, run rnnoise, save out_path
                tmp_wav = out_path + ".tmp.wav"
                subprocess.run(["ffmpeg", "-y", "-i", in_path, "-ar", "48000", "-ac", "1", tmp_wav], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # run rnnoise binary (expects in/out wav)
                subprocess.run([rnnoise_cmd, tmp_wav, out_path], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                os.remove(tmp_wav)
                return jsonify({"enhanced": f"/file/{os.path.basename(out_path)}", "phase": 2, "method": "rnnoise"})
            else:
                # no rnnoise -> fallback to phase1
                log.info("rnnoise binary not found; falling back to phase1")
                return ai_noise_phase1()
        except Exception as e:
            log.exception("rnnoise processing failed: %s", e)
            return ai_noise_phase1()
    else:
        # fallback to phase1
        return ai_noise_phase1()

@app.route("/api/ai/noise/phase3", methods=["POST"])
def ai_noise_phase3():
    """
    Phase3: GPU-accelerated denoising hint. If env USE_GPU is set and ffmpeg build supports gpu filters,
    attempt to use 'nlmeans' (CPU) or GPU filter if available. Otherwise fall back to phase2.
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    prefer_gpu = bool(os.environ.get("VYDRA_USE_GPU", "0")) or bool(data.get("use_gpu"))
    fname = (file_param or "").replace("/file/", "").lstrip("/")
    if not fname:
        return jsonify({"error": "Missing file"}), 400
    in_path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(in_path):
        return jsonify({"error": "file not found"}), 404

    token = secrets.token_urlsafe(6)
    out_name = f"denoised-gpu-{token}.mp4"
    out_path = os.path.join(DOWNLOAD_DIR, out_name)

    try:
        if prefer_gpu:
            # Try a GPU-capable ffmpeg path (this is optional and will likely fail on vanilla setups)
            # This command is conservative and will be attempted; if it fails we fallback.
            try:
                cmd = ["ffmpeg", "-y", "-i", in_path, "-vf", "hqdn3d=1.5:1.5:6:6", "-c:v", "h264_nvenc", "-preset", "p5", "-c:a", "copy", out_path]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return jsonify({"enhanced": f"/file/{out_name}", "phase": 3, "gpu": True})
            except Exception:
                log.info("GPU ffmpeg path failed; trying CPU-based denoise")
                return ai_noise_phase2()
        else:
            # CPU path: use nlmeans if available, else hqdn3d
            try:
                cmd = ["ffmpeg", "-y", "-i", in_path, "-vf", "nlmeans=s=7:p=7", "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "copy", out_path]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return jsonify({"enhanced": f"/file/{out_name}", "phase": 3, "gpu": False, "method": "nlmeans"})
            except subprocess.CalledProcessError:
                # nlmeans might not be available -> fallback to hqdn3d
                cmd = ["ffmpeg", "-y", "-i", in_path, "-vf", "hqdn3d=1.2:1.2:4:4", "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "copy", out_path]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return jsonify({"enhanced": f"/file/{out_name}", "phase": 3, "gpu": False, "method": "hqdn3d"})
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors="ignore") if e.stderr else str(e)
        log.exception("phase3 denoise ffmpeg failed: %s", stderr)
        return jsonify({"error": "denoise failed", "details": stderr}), 500
    except Exception as e:
        log.exception("phase3 denoise error: %s", e)
        return jsonify({"error": "denoise failed", "details": str(e)}), 500

# ---------------- small helpers used above ----------------
def fallback_pil_thumbnail_helper(out_path, title_text, accent_shift=0):
    """
    Small helper to generate a PIL thumbnail. accent_shift gives slight color variation.
    """
    try:
        W, H = 1280, 720
        # slight accent variation by accent_shift
        accents = [(110,231,183), (110,180,231), (200,150,255)]
        accent = accents[accent_shift % len(accents)]
        bg_color = (15, 23, 42)
        img = Image.new("RGB", (W, H), bg_color)
        draw = ImageDraw.Draw(img)
        draw.rounded_rectangle([40, 40, W-40, H-40], radius=20, fill=(17,24,39))
        font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        try:
            font = ImageFont.truetype(font_path, 64) if os.path.exists(font_path) else ImageFont.load_default()
        except Exception:
            font = ImageFont.load_default()
        max_w = W - 200
        lines = []
        cur = ""
        for w in title_text.split():
            test = (cur + " " + w).strip()
            tw, th = draw.textsize(test, font=font)
            if tw > max_w and cur:
                lines.append(cur)
                cur = w
            else:
                cur = test
        if cur:
            lines.append(cur)
        y = H//2 - (len(lines) * 36)
        for line in lines:
            tw, th = draw.textsize(line, font=font)
            draw.text(((W - tw) / 2, y), line, font=font, fill=(255,255,255))
            y += th + 8
        caption = "Generated by VYDRA"
        cw, ch = draw.textsize(caption, font=ImageFont.load_default())
        draw.text((W - cw - 60, H - ch - 60), caption, font=ImageFont.load_default(), fill=accent)
        img.save(out_path, "PNG")
        return True
    except Exception:
        log.exception("fallback_pil_thumbnail_helper failed")
        return False

# ---------------- Admin / Debug ----------------
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
    new_expires = grant_premium(user_id, days)
    return jsonify({"ok": True, "new_expires": new_expires})

@app.route("/_health")
def health():
    return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()})

# ---------------- Cleanup worker ----------------
def cleanup_worker():
    while True:
        try:
            files = sorted([os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR)], key=os.path.getmtime)
            while len(files) > MAX_RECENT_DOWNLOADS:
                oldest = files.pop(0)
                try:
                    os.remove(oldest)
                    log.info("Removed old file: %s", oldest)
                except Exception:
                    log.exception("Failed to remove %s", oldest)
            now_ts = time.time()
            for f in list(files):
                try:
                    if now_ts - os.path.getmtime(f) > CLEANUP_SECONDS:
                        os.remove(f)
                        log.info("Auto-deleted expired file: %s", f)
                except Exception:
                    log.exception("Failed cleanup for %s", f)
        except Exception:
            log.exception("Cleanup worker error")
        time.sleep(30)

if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    threading.Thread(target=cleanup_worker, daemon=True).start()

# ---------------- Main ----------------
if __name__ == "__main__":
    log.info("Starting VYDRA backend on 0.0.0.0:%s", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=True)
#!/usr/bin/env python3
"""
app.py - VYDRA backend (production-minded)

Features:
 - yt-dlp download runner (single + batch)
 - size / duration checks
 - background cleanup worker
 - SQLite referral / invite / reward / subscription tables
 - Paystack initialize, verify, webhook, callback (reads PAYSTACK_SECRET_KEY from .env)
 - lightweight AI endpoints (thumbnail, audio/video enhance, caption generator)
"""

import os
import time
import uuid
import threading
import traceback
import sqlite3
import secrets
import requests
import hmac
import hashlib
import subprocess
import json
from datetime import datetime, timedelta, timezone
from collections import deque
from flask import Flask, request, jsonify, send_file, redirect, make_response
from flask_cors import CORS
import yt_dlp
from PIL import Image, ImageDraw, ImageFont
import logging
from dotenv import load_dotenv

# Load .env
load_dotenv()

# ---------------- Config ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

PORT = int(os.environ.get("VYDRA_PORT", 5000))
CLEANUP_SECONDS = int(os.environ.get("VYDRA_CLEANUP_SECONDS", 1800))
HISTORY_MAX = int(os.environ.get("VYDRA_HISTORY_MAX", 200))
MAX_RECENT_DOWNLOADS = int(os.environ.get("VYDRA_MAX_RECENT_DOWNLOADS", 5))

MAX_DURATION_SECONDS = int(os.environ.get("VYDRA_MAX_DURATION_SECONDS", 20 * 60))
MAX_FILESIZE_BYTES = int(os.environ.get("VYDRA_MAX_FILESIZE_BYTES", 300 * 1024 * 1024))

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
PAYSTACK_SECRET_KEY = os.environ.get("PAYSTACK_SECRET_KEY")
WA_LINK = os.environ.get("WA_LINK", "https://wa.link/rcptsq")

DB_PATH = os.path.join(BASE_DIR, "vydra_referrals.db")
FRONTEND_ORIGINS = os.environ.get("FRONTEND_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger("vydra")

# ---------------- Flask ----------------
app = Flask(__name__)
origins = [o.strip() for o in FRONTEND_ORIGINS.split(",") if o.strip()]
CORS(app, resources={r"/*": {"origins": origins}})

# ---------------- SQLite helpers ----------------
def get_db_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_conn()
    cur = conn.cursor()

    # ---------------- USERS ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_visits INTEGER DEFAULT 0,
        visits_today INTEGER DEFAULT 0,
        visits_month INTEGER DEFAULT 0,
        total_invites INTEGER DEFAULT 0
    );
    """)

    # ---------------- VISIT LOG ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS visit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id TEXT,
        visitor_ip TEXT,
        user_agent TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ---------------- REWARD TOKENS ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reward_tokens (
        token TEXT PRIMARY KEY,
        user_id TEXT,
        milestone TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP,
        claimed INTEGER DEFAULT 0,
        claimed_at TIMESTAMP
    );
    """)

    # ---------------- CLAIMED INVITE REWARDS ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS claimed_invite_rewards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT,
        milestone TEXT,
        claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ---------------- PREMIUM SUBSCRIPTIONS ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS premium_subscriptions (
        user_id TEXT PRIMARY KEY,
        expires_at TIMESTAMP
    );
    """)

    # ---------------- TRIAL CLAIMS ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trial_claims (
        user_id TEXT PRIMARY KEY,
        claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ---------------- PAYMENTS (Paystack integration) ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        reference TEXT PRIMARY KEY,
        user_id TEXT,
        email TEXT,
        plan TEXT,
        amount INTEGER,
        status TEXT,
        paystack_response TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed INTEGER DEFAULT 0,
        processed_at TIMESTAMP
    );
    """)

    # ---------------- AI HASHTAG LEARNING TABLE ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ai_hashtag_learning (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tag TEXT,
        platform TEXT,
        times_used INTEGER DEFAULT 0,
        engagement_score REAL DEFAULT 0.0,
        last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    conn.close()

# ✅ Initialize database tables on startup
init_db()

# ---------------- Job state ----------------
jobs = {}
job_history = deque(maxlen=HISTORY_MAX)

# ---------------- Utilities ----------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def safe_filename(name):
    return os.path.basename(name or "")

def ensure_user_row(user_id):
    if not user_id:
        return
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if not cur.fetchone():
        cur.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
    conn.close()

def increment_visit(referrer_id, visitor_ip, user_agent):
    if not referrer_id:
        return
    ensure_user_row(referrer_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO visit_log (referrer_id, visitor_ip, user_agent) VALUES (?,?,?)",
                (referrer_id, visitor_ip, user_agent))
    cur.execute("UPDATE users SET total_visits = total_visits + 1, visits_today = visits_today + 1, visits_month = visits_month + 1 WHERE user_id=?",
                (referrer_id,))
    conn.commit()
    conn.close()

def get_stats_for_user(user_id):
    if not user_id:
        return {"totalVisits": 0, "visitsToday": 0, "visitsThisMonth": 0, "totalInvites": 0}
    ensure_user_row(user_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT total_visits, visits_today, visits_month, total_invites FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return {
            "totalVisits": int(row["total_visits"] or 0),
            "visitsToday": int(row["visits_today"] or 0),
            "visitsThisMonth": int(row["visits_month"] or 0),
            "totalInvites": int(row["total_invites"] or 0)
        }
    return {"totalVisits": 0, "visitsToday": 0, "visitsThisMonth": 0, "totalInvites": 0}

def add_invite_count(referrer_id, amount=1):
    if not referrer_id:
        return
    ensure_user_row(referrer_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET total_invites = total_invites + ? WHERE user_id=?", (amount, referrer_id))
    conn.commit()
    conn.close()

def grant_premium(user_id, days):
    if not user_id:
        raise ValueError("user_id required to grant premium")
    ensure_user_row(user_id)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    now = datetime.now(timezone.utc)
    if row and row["expires_at"]:
        try:
            existing = datetime.fromisoformat(row["expires_at"])
            if existing.tzinfo is None:
                existing = existing.replace(tzinfo=timezone.utc)
        except Exception:
            existing = now
    else:
        existing = now
    start = existing if existing > now else now
    new_expires = start + timedelta(days=days)
    cur.execute("INSERT OR REPLACE INTO premium_subscriptions (user_id, expires_at) VALUES (?, ?)",
                (user_id, new_expires.isoformat()))
    conn.commit()
    conn.close()
    log.info("Granted premium: %s for %d days -> expires %s", user_id, days, new_expires.isoformat())
    return new_expires.isoformat()

def check_premium(user_id):
    if not user_id:
        return False
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if not row or not row["expires_at"]:
        return False
    try:
        expires = datetime.fromisoformat(row["expires_at"])
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return datetime.now(timezone.utc) < expires

def premium_expires_at(user_id):
    if not user_id:
        return None
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT expires_at FROM premium_subscriptions WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row["expires_at"] if row and row["expires_at"] else None

# ---------------- Payment helpers ----------------
def record_payment(reference, user_id=None, email=None, plan=None, amount=None, status=None, paystack_response=None):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO payments (reference, user_id, email, plan, amount, status, paystack_response, created_at, processed, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM payments WHERE reference=?), CURRENT_TIMESTAMP), COALESCE((SELECT processed FROM payments WHERE reference?), 0), COALESCE((SELECT processed_at FROM payments WHERE reference?), NULL))
        """, (reference, user_id, email, plan, amount, status, json.dumps(paystack_response if paystack_response is not None else {}), reference, reference))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("Failed to record payment %s", reference)

def get_payment_by_reference(reference):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM payments WHERE reference=?", (reference,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def mark_payment_processed(reference):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE payments SET processed=1, processed_at=CURRENT_TIMESTAMP WHERE reference=?", (reference,))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("Failed to mark payment processed: %s", reference)

# ---------------- yt-dlp & downloads ----------------
def parse_percent(percent_str):
    if percent_str is None:
        return None
    try:
        if isinstance(percent_str, (int, float)):
            return int(round(float(percent_str)))
        if isinstance(percent_str, str):
            p = percent_str.strip().replace("%", "")
            return int(round(float(p)))
    except Exception:
        pass
    return None

def ytdlp_hook(d, job_id):
    try:
        if job_id not in jobs:
            return
        status = d.get("status")
        if status == "downloading":
            percent = parse_percent(d.get("_percent_str") or d.get("percent"))
            jobs[job_id]["status"] = "downloading"
            jobs[job_id]["progress"] = {
                "percent": percent if percent is not None else jobs[job_id].get("progress", {}).get("percent", 0),
                "speed": d.get("_speed_str") or d.get("speed"),
                "eta": d.get("eta"),
                "stage": "downloading",
            }
        elif status == "finished":
            jobs[job_id]["status"] = "processing"
            jobs[job_id]["progress"] = {"percent": 100, "stage": "postprocessing"}
    except Exception:
        traceback.print_exc()

def estimate_size_bytes(info, quality, mode):
    try:
        size = int(info.get("filesize") or info.get("filesize_approx") or 0)
    except Exception:
        size = 0
    formats = info.get("formats") or []
    if not formats:
        return size
    height_map = {"360p": 360, "480p": 480, "720p": 720, "1080p": 1080, "1440p": 1440, "2160p": 2160, "best": 999999}
    max_height = height_map.get(quality, 999999)
    best_candidate = 0
    for f in formats:
        try:
            fsize = int(f.get("filesize") or f.get("filesize_approx") or 0)
        except Exception:
            fsize = 0
        if mode == "audio":
            if f.get("vcodec") in (None, "none") or (f.get("acodec") and not f.get("vcodec")):
                if fsize > best_candidate:
                    best_candidate = fsize
        else:
            height = f.get("height") or 0
            if height and height <= max_height:
                if fsize > best_candidate:
                    best_candidate = fsize
            if not height and fsize > best_candidate:
                best_candidate = fsize
    if best_candidate:
        return best_candidate
    return size

def cleanup_downloads(max_files=MAX_RECENT_DOWNLOADS):
    try:
        files = [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR) if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]
        files.sort(key=os.path.getmtime, reverse=True)
        deleted = []
        for f in files[max_files:]:
            try:
                os.remove(f)
                deleted.append(os.path.basename(f))
                log.info("Deleted old file %s", f)
            except Exception as e:
                log.warning("Failed to delete %s: %s", f, e)
        if deleted:
            for job in list(job_history):
                file_rel = job.get("file", "")
                if file_rel and any(fn in file_rel for fn in deleted):
                    try:
                        job_history.remove(job)
                    except ValueError:
                        pass
    except Exception as e:
        log.exception("cleanup_downloads error: %s", e)

def run_download(job_id, url, mode, quality, enhance_video, enhance_audio, preferred_audio_codec="mp3", preferred_bitrate="192"):
    try:
        ydl_opts = {
            "outtmpl": os.path.join(DOWNLOAD_DIR, f"{job_id}.%(ext)s"),
            "merge_output_format": "mp4",
            "progress_hooks": [lambda d: ytdlp_hook(d, job_id)],
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
        }

        # format / postprocessing
        if mode == "audio":
            codec = (preferred_audio_codec or "mp3").lower()
            if codec not in ("mp3", "m4a", "aac", "opus"):
                codec = "mp3"
            ydl_opts.update({
                "format": "bestaudio/best",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": codec,
                    "preferredquality": str(preferred_bitrate or "192"),
                }],
            })
        else:
            if quality == "360p":
                ydl_opts["format"] = "bestvideo[height<=360]+bestaudio/best"
            elif quality == "480p":
                ydl_opts["format"] = "bestvideo[height<=480]+bestaudio/best"
            elif quality == "720p":
                ydl_opts["format"] = "bestvideo[height<=720]+bestaudio/best"
            elif quality == "1080p":
                ydl_opts["format"] = "bestvideo[height<=1080]+bestaudio/best"
            else:
                ydl_opts["format"] = "bestvideo+bestaudio/best"

        info = None
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                log.info("Starting download job %s for %s (mode=%s, quality=%s)", job_id, url, mode, quality)
                info = ydl.extract_info(url, download=True)
        except Exception as e_inner:
            log.warning("Primary download attempt failed: %s", e_inner)
            if mode != "audio" and ydl_opts.get("format") != "bestvideo+bestaudio/best":
                try:
                    ydl_opts["format"] = "bestvideo+bestaudio/best"
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                except Exception as e2:
                    log.exception("Fallback download failed: %s", e2)
                    raise
            else:
                raise

        final_file = None
        for ext_try in ["mp3", "m4a", "mp4", "mkv", "webm"]:
            candidate = os.path.join(DOWNLOAD_DIR, f"{job_id}.{ext_try}")
            if os.path.exists(candidate):
                final_file = candidate
                break

        if not final_file and info:
            title = info.get("title") or os.path.basename(url)
            safe_base = "".join(c for c in title if c.isalnum() or c in (" ", "-", "_")).strip().replace(" ", "_")
            for f in os.listdir(DOWNLOAD_DIR):
                if safe_base and safe_base in f:
                    final_file = os.path.join(DOWNLOAD_DIR, f)
                    break

        if not final_file:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = "Downloaded file not found after yt-dlp run"
            log.error("Job %s finished but file not found", job_id)
            return

        title = (info.get("title") or os.path.basename(url))
        tags = info.get("tags") or []
        hashtags = []
        if tags:
            for t in tags[:6]:
                hashtags.append("#" + "".join(str(t).strip().split()))
        else:
            words = [w for w in title.split() if len(w) > 2]
            for w in words[:4]:
                hashtags.append("#" + "".join(w.strip().split()))

        jobs[job_id].update({
            "id": job_id,
            "url": url,
            "mode": mode,
            "options": {"quality": quality, "enhance_video": bool(enhance_video), "enhance_audio": bool(enhance_audio)},
            "status": "finished",
            "file": f"/file/{os.path.basename(final_file)}",
            "history": {"title": title, "hashtags": hashtags},
            "time": datetime.now(timezone.utc).isoformat()
        })
        job_history.appendleft(jobs[job_id].copy())
        log.info("Job %s finished; file: %s", job_id, final_file)
        cleanup_downloads(MAX_RECENT_DOWNLOADS)
    except Exception as e:
        if job_id in jobs:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)
        log.exception("run_download error for job %s: %s", job_id, e)

# ---------------- Routes ----------------
@app.route("/download", methods=["POST"])
def download():
    data = request.json or {}
    url = data.get("url")
    batch = data.get("batch")
    mode = data.get("mode", "video")
    quality = data.get("quality", "best")
    # accept both camelCase & snake_case payloads
    enhance_video = data.get("enhanceVideo", False) or data.get("enhance_video", False)
    enhance_audio = data.get("enhanceAudio", False) or data.get("enhance_audio", False)

    MAX_BATCH = 12

    def start_single_job(target_url):
        if not target_url:
            return (None, "Empty URL")
        try:
            with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
                info = ydl.extract_info(target_url, download=False)
        except Exception as e:
            return (None, f"Could not extract metadata: {str(e)}")

        duration = info.get("duration")
        if not duration:
            return (None, "Could not determine video duration")
        if duration > MAX_DURATION_SECONDS:
            return (None, f"Video duration ({int(duration)}s) exceeds limit of {MAX_DURATION_SECONDS}s")

        est_size = estimate_size_bytes(info, quality, mode)
        if not est_size:
            return (None, "Could not estimate download size from metadata")
        if est_size > MAX_FILESIZE_BYTES:
            return (None, f"Estimated size {(est_size/1024/1024):.1f}MB exceeds limit of {(MAX_FILESIZE_BYTES/1024/1024):.0f}MB")

        job_id = str(uuid.uuid4())
        jobs[job_id] = {"status": "downloading", "progress": {"percent": 0, "stage": "queued"}, "type": mode}
        t = threading.Thread(target=run_download, args=(job_id, target_url, mode, quality, enhance_video, enhance_audio), daemon=True)
        t.start()
        return (job_id, None)

    if batch:
        if not isinstance(batch, list):
            return jsonify({"error": "Batch must be an array of URLs"}), 400
        if len(batch) == 0:
            return jsonify({"error": "Batch array is empty"}), 400
        if len(batch) > MAX_BATCH:
            return jsonify({"error": f"Batch too large (max {MAX_BATCH} links)"}), 400

        results = {"job_ids": [], "errors": []}
        for entry in batch:
            if not entry or not isinstance(entry, str):
                results["errors"].append({"url": entry, "error": "Invalid url entry"})
                continue
            entry = entry.strip()
            if not entry:
                results["errors"].append({"url": entry, "error": "Empty url"})
                continue
            job_id, err = start_single_job(entry)
            if job_id:
                results["job_ids"].append(job_id)
            else:
                results["errors"].append({"url": entry, "error": err})
        if not results["job_ids"]:
            return jsonify({"error": "No jobs started", "details": results["errors"]}), 400
        return jsonify(results)

    if not url:
        return jsonify({"error": "Missing url"}), 400

    job_id, err = start_single_job(url)
    if not job_id:
        return jsonify({"error": err}), 400
    return jsonify({"job_id": job_id})

@app.route("/progress/<job_id>")
def route_progress(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "No such job"}), 404
    resp = dict(job)
    resp["job_id"] = job_id
    if "progress" in resp:
        try:
            resp["progress"]["percent"] = int(resp["progress"].get("percent") or 0)
        except Exception:
            resp["progress"]["percent"] = 0
    return jsonify(resp)

@app.route("/cancel/<job_id>", methods=["POST"])
def route_cancel(job_id):
    if job_id not in jobs:
        return jsonify({"error": "No such job"}), 404
    jobs[job_id]["status"] = "cancelled"
    jobs[job_id]["progress"] = {"percent": jobs[job_id].get("progress", {}).get("percent", 0), "stage": "cancelled"}
    return jsonify({"ok": True})

@app.route("/file/<filename>")
def route_file(filename):
    safe = safe_filename(filename)
    path = os.path.join(DOWNLOAD_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path, as_attachment=True)

@app.route("/history")
def route_history():
    history_list = []
    for job in list(job_history):
        file_rel = job.get("file")
        if file_rel:
            filename = file_rel.replace("/file/", "")
            path = os.path.join(DOWNLOAD_DIR, filename)
            if not os.path.exists(path):
                try:
                    job_history.remove(job)
                except ValueError:
                    pass
                continue
        history_list.append({
            "id": job.get("id"),
            "title": job.get("history", {}).get("title", "Unknown"),
            "hashtags": job.get("history", {}).get("hashtags", []),
            "file": job.get("file"),
            "url": job.get("url"),
            "mode": job.get("mode"),
            "time": job.get("time"),
            "status": job.get("status"),
        })
    return jsonify(history_list)

# ---------------- Referral & Rewards ----------------
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
    except Exception as e:
        log.exception("ref_redirect error: %s", e)
        return redirect("/")

@app.route("/api/stats/<user_id>")
def api_get_stats(user_id):
    stats = get_stats_for_user(user_id)
    stats["isPremium"] = check_premium(user_id)
    stats["premiumExpiresAt"] = premium_expires_at(user_id)
    return jsonify(stats)

@app.route("/api/premium_status/<user_id>")
def api_premium_status(user_id):
    return jsonify({"isPremium": check_premium(user_id), "expiresAt": premium_expires_at(user_id)})

@app.route("/api/generate_reward/<user_id>", methods=["POST"])
def api_generate_reward(user_id):
    data = request.json or {}
    milestone = data.get("milestone")
    stats = get_stats_for_user(user_id)
    milestone_thresholds = {"20k_month": 20000, "50k_month": 50000, "100k_month": 100000, "500k_month": 500000}
    if milestone not in milestone_thresholds:
        return jsonify({"error": "Unknown milestone"}), 400
    needed = milestone_thresholds[milestone]
    if stats["visitsThisMonth"] < needed:
        return jsonify({"error": "Milestone not reached"}), 400
    token = secrets.token_urlsafe(24)
    expires = datetime.now(timezone.utc) + timedelta(days=7)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO reward_tokens (token, user_id, milestone, expires_at, claimed) VALUES (?,?,?,?,0)",
                (token, user_id, milestone, expires.isoformat()))
    conn.commit()
    conn.close()
    claim_url = f"{request.host_url.rstrip('/')}/reward/{token}"
    return jsonify({"claim_url": claim_url})

@app.route("/reward/<token>")
def claim_reward(token):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, milestone, expires_at, claimed FROM reward_tokens WHERE token=?", (token,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return "Invalid or expired token", 404
    user_id, milestone, expires_at, claimed = row["user_id"], row["milestone"], row["expires_at"], row["claimed"]
    try:
        expires_dt = datetime.fromisoformat(expires_at)
        if expires_dt.tzinfo is None:
            expires_dt = expires_dt.replace(tzinfo=timezone.utc)
    except Exception:
        expires_dt = datetime.now(timezone.utc) - timedelta(days=1)
    if claimed:
        conn.close()
        return "This reward link has already been used.", 410
    if datetime.now(timezone.utc) > expires_dt:
        conn.close()
        return "This reward link has expired.", 410
    cur.execute("UPDATE reward_tokens SET claimed=1, claimed_at=CURRENT_TIMESTAMP WHERE token=?", (token,))
    conn.commit()
    conn.close()
    return redirect(WA_LINK)

INVITE_REWARDS = {
    "invite_5": {"threshold": 5, "reward": "1 week Premium", "days": 7},
    "invite_10": {"threshold": 10, "reward": "2 weeks Premium", "days": 14},
    "invite_20": {"threshold": 20, "reward": "3 weeks Premium", "days": 21},
    "invite_60": {"threshold": 60, "reward": "3 months Premium", "days": 90},
}

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
    total_invites = row["total_invites"] if row else 0
    threshold = INVITE_REWARDS[milestone]["threshold"]
    if total_invites < threshold:
        conn.close()
        return jsonify({"error": "Not enough invites yet"}), 400
    cur.execute("SELECT COUNT(1) as cnt FROM claimed_invite_rewards WHERE user_id=? AND milestone=?", (user_id, milestone))
    already = cur.fetchone()["cnt"]
    if already:
        conn.close()
        return jsonify({"error": "Reward already claimed"}), 400
    cur.execute("INSERT INTO claimed_invite_rewards (user_id, milestone) VALUES (?,?)", (user_id, milestone))
    conn.commit()
    conn.close()
    days = INVITE_REWARDS[milestone]["days"]
    new_expires = grant_premium(user_id, days)
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
    expires = grant_premium(user_id, 21)
    return jsonify({"ok": True, "message": "Trial started", "expires_at": expires})

# ---------------- Paystack integration (improved + idempotent) ----------------
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

    # Paystack expects amount in kobo (smallest currency unit)
    try:
        amount_int = int(amount)
    except Exception:
        return jsonify({"error": "Invalid amount"}), 400

    body = {
        "email": email,
        "amount": amount_int,
        "metadata": {"plan": plan, **(metadata or {}), "user_id": user_id},
        "callback_url": f"{request.host_url.rstrip('/')}/api/paystack/callback"
    }
    headers = {"Authorization": f"Bearer {secret}", "Content-Type": "application/json"}
    try:
        r = requests.post("https://api.paystack.co/transaction/initialize", json=body, headers=headers, timeout=15)
        payload = r.json()
        # If Paystack returned a reference, persist the initialized payment record
        data_field = payload.get("data") or {}
        reference = data_field.get("reference")
        if reference:
            # record minimal info now (status 'initialized')
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
        # persist the response for troubleshooting
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
    # persist the result (create or update)
    try:
        record_payment(reference, user_id=user_id, email=(txn.get("customer") or {}).get("email"), plan=plan, amount=amount, status=status, paystack_response=data)
    except Exception:
        log.exception("Failed to record payment after verify")

    result = {"ok": True, "verified": status == "success", "paystack": txn}
    if status == "success" and user_id:
        # idempotent grant: check payments table processed flag
        pay = get_payment_by_reference(reference)
        if pay and pay.get("processed"):
            result["premium_granted"] = False
            result["note"] = "Already processed"
        else:
            days_map = {"weekly": 7, "monthly": 30, "quarter": 90, "yearly": 365}
            days = days_map.get(plan, 7)
            try:
                new_expires = grant_premium(user_id, days)
                result["premium_granted"] = True
                result["premium_expires_at"] = new_expires
                # mark as processed
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
    # persist incoming webhook for audit (keep paystack_response small)
    try:
        reference = data.get('reference')
        status = data.get('status') or payload.get('event')
        # record payment (insert or update)
        if reference:
            record_payment(reference, user_id=(data.get('metadata') or {}).get('user_id') or (data.get('customer') or {}).get('email'),
                           email=(data.get('customer') or {}).get('email'),
                           plan=(data.get('metadata') or {}).get('plan'),
                           amount=data.get('amount'),
                           status=status,
                           paystack_response=payload)
    except Exception:
        log.exception("Error recording webhook payload")

    # handle successful charges idempotently
    try:
        if event == 'charge.success' or (data.get('status') == 'success'):
            reference = data.get('reference')
            metadata = data.get('metadata') or {}
            user_id = metadata.get('user_id') or (data.get('customer') or {}).get('email')
            plan = metadata.get('plan')
            if not reference:
                log.warning("Webhook charge.success with no reference")
                return jsonify({'status': 'ok'}), 200
            pay = get_payment_by_reference(reference)
            if pay and pay.get("processed"):
                log.info("Webhook: payment %s already processed, skipping", reference)
                return jsonify({'status': 'ok'}), 200
            # mark processed and grant premium
            days_map = {"weekly": 7, "monthly": 30, "quarter": 90, "yearly": 365}
            days = days_map.get(plan, 7)
            if user_id and plan in days_map:
                try:
                    grant_premium(user_id, days)
                    mark_payment_processed(reference)
                    log.info("Webhook: granted premium for %s (plan=%s) ref=%s", user_id, plan, reference)
                except Exception:
                    log.exception("Webhook: failed to grant premium for %s ref=%s", user_id, reference)
            # insert into supabase if configured (best-effort)
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
    # Paystack will redirect here with ?reference
    ref = request.args.get("reference") or request.args.get("trxref")
    frontend = os.environ.get("FRONTEND_ORIGIN", "http://localhost:3000")
    if ref:
        # Let frontend verify via /api/paystack/verify/<ref>
        return redirect(f"{frontend.rstrip('/')}/payment-success?reference={ref}")
    return redirect(frontend)

@app.route("/api/payments/<reference>")
def api_get_payment(reference):
    pay = get_payment_by_reference(reference)
    if not pay:
        return jsonify({"error": "Not found"}), 404
    # safely parse paystack_response back into object
    pr = pay.get("paystack_response")
    try:
        pay["paystack_response"] = json.loads(pr) if pr else {}
    except Exception:
        pay["paystack_response"] = pr
    return jsonify(pay)

# ---------------- AI endpoints ----------------
# Upgraded AI integration block (Thumbnail, Hashtags, Noise) with robust fallbacks,
# optional external AI service hooks (THUMBNAIL_AI_URL + THUMBNAIL_AI_KEY), and simple hashtag learning DB.

import shutil
from PIL import ImageStat, ImageOps, ImageFilter, ImageEnhance
import io
import math
import tempfile

# Ensure learning table exists (safe to call repeatedly)
def ensure_ai_learning_table():
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ai_hashtag_learning (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT NOT NULL,
                platform TEXT DEFAULT 'generic',
                times_used INTEGER DEFAULT 0,
                engagement_score REAL DEFAULT 0.0,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tag, platform)
            );
        """)
        conn.commit()
        conn.close()
    except Exception:
        log.exception("ensure_ai_learning_table failed")

# record hashtag usage for simple adaptive learning
def record_hashtag_usage(tag, platform=None, engagement=0.0):
    if not tag:
        return
    try:
        ensure_ai_learning_table()
        conn = get_db_conn()
        cur = conn.cursor()
        plat = (platform or "generic")
        cur.execute("SELECT id, times_used, engagement_score FROM ai_hashtag_learning WHERE tag=? AND platform=?",
                    (tag, plat))
        row = cur.fetchone()
        if row:
            _id, times_used, score = row["id"], (row["times_used"] or 0), (row["engagement_score"] or 0.0)
            new_times = times_used + 1
            new_score = ((score * times_used) + float(engagement)) / (new_times if new_times > 0 else 1)
            cur.execute("UPDATE ai_hashtag_learning SET times_used=?, engagement_score=?, last_used=CURRENT_TIMESTAMP WHERE id=?",
                        (new_times, new_score, _id))
        else:
            cur.execute("INSERT INTO ai_hashtag_learning (tag, platform, times_used, engagement_score) VALUES (?,?,1,?)",
                        (tag, plat, float(engagement)))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("record_hashtag_usage error")

def fetch_hashtag_stats(tag, platform=None):
    try:
        ensure_ai_learning_table()
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT times_used, engagement_score FROM ai_hashtag_learning WHERE tag=? AND platform=?", (tag, platform or "generic"))
        row = cur.fetchone()
        conn.close()
        if row:
            return {"times_used": row["times_used"] or 0, "engagement_score": float(row["engagement_score"] or 0.0)}
    except Exception:
        pass
    return {"times_used": 0, "engagement_score": 0.0}

# small helper: safe PIL fallback thumbnail (guaranteed)
def fallback_pil_thumbnail_helper(out_path, title_text, accent_shift=0):
    try:
        W, H = 1280, 720
        accents = [(110,231,183), (110,180,231), (200,150,255)]
        accent = accents[accent_shift % len(accents)]
        bg_color = (15, 23, 42)
        img = Image.new("RGB", (W, H), bg_color)
        draw = ImageDraw.Draw(img)
        # rounded rectangle (works with modern Pillow)
        try:
            draw.rounded_rectangle([40, 40, W-40, H-40], radius=20, fill=(17,24,39))
        except Exception:
            draw.rectangle([40, 40, W-40, H-40], fill=(17,24,39))
        font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        try:
            font = ImageFont.truetype(font_path, 64) if os.path.exists(font_path) else ImageFont.load_default()
        except Exception:
            font = ImageFont.load_default()
        max_w = W - 200
        lines = []
        cur = ""
        for w in title_text.split():
            test = (cur + " " + w).strip()
            tw, th = draw.textsize(test, font=font)
            if tw > max_w and cur:
                lines.append(cur)
                cur = w
            else:
                cur = test
        if cur:
            lines.append(cur)
        y = H//2 - (len(lines) * 36)
        for line in lines:
            tw, th = draw.textsize(line, font=font)
            draw.text(((W - tw) / 2, y), line, font=font, fill=(255,255,255))
            y += th + 8
        caption = "Generated by VYDRA"
        cw, ch = draw.textsize(caption, font=ImageFont.load_default())
        draw.text((W - cw - 60, H - ch - 60), caption, font=ImageFont.load_default(), fill=accent)
        img.save(out_path, "PNG")
        return True
    except Exception:
        log.exception("fallback_pil_thumbnail_helper failed")
        return False

# unified frame extraction util
def _extract_frame(in_path, out_path, ts=None, target_w=1280):
    try:
        if ts is None:
            try:
                cmd_info = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "default=noprint_wrappers=1:nokey=1", in_path]
                out = subprocess.check_output(cmd_info, stderr=subprocess.STDOUT).decode().strip()
                dur = float(out) if out else None
            except Exception:
                dur = None
            ts = max(2.0, dur/2.0) if dur and dur > 4.0 else 2.0
        scale_arg = f"scale={target_w}:-2"
        cmd = ["ffmpeg", "-y", "-ss", str(float(ts)), "-i", in_path, "-frames:v", "1", "-q:v", "2", "-vf", scale_arg, out_path]
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True, None
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr.decode(errors="ignore") if e.stderr else str(e))
        log.warning("frame extraction failed: %s", stderr)
        return False, stderr
    except Exception as e:
        log.exception("frame extraction error: %s", e)
        return False, str(e)

# Optional external AI enhancement helper
def try_ai_enhance(image_path):
    AI_URL = os.environ.get("THUMBNAIL_AI_URL")
    AI_KEY = os.environ.get("THUMBNAIL_AI_KEY")
    if not AI_URL:
        return False, "no-ai-config"
    try:
        with open(image_path, "rb") as fh:
            files = {"image": ("thumb.png", fh, "image/png")}
            headers = {}
            if AI_KEY:
                headers["Authorization"] = f"Bearer {AI_KEY}"
            r = requests.post(AI_URL, files=files, headers=headers, timeout=60)
        if r.ok and "image" in (r.headers.get("Content-Type","") or ""):
            with open(image_path, "wb") as outfh:
                outfh.write(r.content)
            return True, "ai_ok"
        return False, f"ai_http:{r.status_code}"
    except Exception as e:
        log.exception("AI enhance request failed: %s", e)
        return False, str(e)

# ---------------- Thumbnail endpoints (phased) ----------------

@app.route("/api/ai/thumbnail/phase1", methods=["POST"])
def ai_thumbnail_phase1():
    """
    Phase1 - single-frame thumbnail (local or PIL fallback)
    payload: { user_id, file (optional), title (optional), timestamp (optional) }
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    title = (data.get("title") or "VYDRA").strip()
    file_param = data.get("file")
    token = secrets.token_urlsafe(8)
    out_name = f"thumbnail-p1-{token}.png"
    out_path = os.path.join(DOWNLOAD_DIR, out_name)

    in_path = None
    if file_param:
        fname = file_param.replace("/file/", "").lstrip("/")
        candidate = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
        if os.path.exists(candidate):
            in_path = candidate

    if in_path:
        ts = None
        if data.get("timestamp") is not None:
            try:
                ts = float(data.get("timestamp"))
            except Exception:
                ts = None
        ok_e, err = _extract_frame(in_path, out_path, ts=ts, target_w=1280)
        if not ok_e:
            log.info("phase1: extraction failed (%s) - PIL fallback", err)
            fallback_pil_thumbnail_helper(out_path, title)
        else:
            # overlay small label
            try:
                img = Image.open(out_path).convert("RGBA")
                draw = ImageDraw.Draw(img)
                font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                try:
                    font = ImageFont.truetype(font_path, 36) if os.path.exists(font_path) else ImageFont.load_default()
                except Exception:
                    font = ImageFont.load_default()
                W, H = img.size
                label = title
                padding = 10
                tw, th = draw.textsize(label, font=font)
                overlay = Image.new("RGBA", img.size)
                od = ImageDraw.Draw(overlay)
                od.rectangle([40, H - th - 60, 40 + tw + padding*2, H - 40], fill=(0,0,0,160))
                img = Image.alpha_composite(img, overlay)
                draw = ImageDraw.Draw(img)
                draw.text((40 + padding, H - th - 55), label, font=font, fill=(255,255,255,255))
                img = img.convert("RGB")
                img.save(out_path, "PNG")
            except Exception:
                log.exception("phase1 overlay failed")
    else:
        fallback_pil_thumbnail_helper(out_path, title)

    if not os.path.exists(out_path):
        return jsonify({"error": "thumbnail failed"}), 500
    return jsonify({"thumbnail_file": f"/file/{out_name}", "phase": 1})

@app.route("/api/ai/thumbnail/phase2", methods=["POST"])
def ai_thumbnail_phase2():
    """
    Phase2 - produce 3 candidate thumbnails (25%,50%,75% points) or 3 PIL variants when no file.
    payload: { user_id, file (optional), title (optional) }
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    title = (data.get("title") or "VYDRA").strip()
    token_base = secrets.token_urlsafe(6)
    candidates = []

    if not file_param:
        for i in range(3):
            out_name = f"thumbnail-p2-{token_base}-{i}.png"
            out_path = os.path.join(DOWNLOAD_DIR, out_name)
            fallback_pil_thumbnail_helper(out_path, f"{title} • variant {i+1}", accent_shift=i)
            candidates.append(f"/file/{out_name}")
        return jsonify({"candidates": candidates, "phase": 2})

    fname = file_param.replace("/file/", "").lstrip("/")
    in_path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(in_path):
        for i in range(3):
            out_name = f"thumbnail-p2-{token_base}-{i}.png"
            out_path = os.path.join(DOWNLOAD_DIR, out_name)
            fallback_pil_thumbnail_helper(out_path, f"{title} • variant {i+1}", accent_shift=i)
            candidates.append(f"/file/{out_name}")
        return jsonify({"candidates": candidates, "phase": 2})

    # find duration
    try:
        cmd_info = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1", in_path]
        out = subprocess.check_output(cmd_info, stderr=subprocess.STDOUT).decode().strip()
        dur = float(out) if out else None
    except Exception:
        dur = None

    points = [2.0, 4.0, 6.0]
    if dur and dur > 10:
        points = [max(1.0, dur*0.25), max(2.0, dur*0.5), max(3.0, dur*0.75)]

    for idx, p in enumerate(points):
        out_name = f"thumbnail-p2-{token_base}-{idx}.png"
        out_path = os.path.join(DOWNLOAD_DIR, out_name)
        ok_e, err = _extract_frame(in_path, out_path, ts=p, target_w=1280)
        if not ok_e or not os.path.exists(out_path):
            fallback_pil_thumbnail_helper(out_path, f"{title} • variant {idx+1}", accent_shift=idx)
        else:
            try:
                img = Image.open(out_path).convert("RGBA")
                draw = ImageDraw.Draw(img)
                font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                try:
                    font = ImageFont.truetype(font_path, 40) if os.path.exists(font_path) else ImageFont.load_default()
                except Exception:
                    font = ImageFont.load_default()
                W, H = img.size
                label = title
                padding = 10
                tw, th = draw.textsize(label, font=font)
                od = Image.new("RGBA", img.size)
                od_draw = ImageDraw.Draw(od)
                od_draw.rectangle([50, H - th - 60, 50 + tw + padding*2, H - 40], fill=(0,0,0,160))
                img = Image.alpha_composite(img, od)
                draw = ImageDraw.Draw(img)
                draw.text((50 + padding, H - th - 55), label, font=font, fill=(255,255,255,255))
                img = img.convert("RGB")
                img.save(out_path, "PNG")
            except Exception:
                log.exception("phase2 overlay failed")
        candidates.append(f"/file/{out_name}")

    return jsonify({"candidates": candidates, "phase": 2})

@app.route("/api/ai/thumbnail/phase3", methods=["POST"])
def ai_thumbnail_phase3():
    """
    Phase3 - optional AI enhancement of a chosen thumbnail file (use_ai + THUMBNAIL_AI_URL)
    payload: { user_id, file, use_ai (bool) }
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    use_ai = bool(data.get("use_ai"))
    if not file_param:
        return jsonify({"error": "Missing file param to enhance"}), 400

    fname = file_param.replace("/file/", "").lstrip("/")
    path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404

    if use_ai and os.environ.get("THUMBNAIL_AI_URL"):
        ok_ai, msg = try_ai_enhance(path)
        return jsonify({"ai_attempted": True, "ai_ok": ok_ai, "ai_msg": msg, "phase": 3})
    # local enhance fallback
    try:
        img = Image.open(path).convert("RGB")
        img = ImageOps.autocontrast(img, cutoff=1)
        img = img.filter(ImageFilter.UnsharpMask(radius=1, percent=120, threshold=3))
        img.save(path, "PNG")
        return jsonify({"ai_attempted": False, "ai_ok": True, "note": "local_enhanced", "phase": 3})
    except Exception as e:
        log.exception("phase3 local enhance failed: %s", e)
        return jsonify({"ai_attempted": False, "ai_ok": False, "note": "local_failed", "phase": 3, "details": str(e)}), 500

@app.route("/api/ai/thumbnail/phase4", methods=["POST"])
def ai_thumbnail_phase4():
    """
    Phase4 - emotion-aware composition + produce desktop & mobile variants
    payload: { user_id, file, title (optional) }
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    title = (data.get("title") or "").strip()
    if not file_param:
        return jsonify({"error": "Missing base thumbnail file"}), 400

    fname = file_param.replace("/file/", "").lstrip("/")
    path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404

    token = secrets.token_urlsafe(6)
    desktop_name = f"thumbnail-p4-desktop-{token}.png"
    mobile_name = f"thumbnail-p4-mobile-{token}.png"
    desktop_path = os.path.join(DOWNLOAD_DIR, desktop_name)
    mobile_path = os.path.join(DOWNLOAD_DIR, mobile_name)

    try:
        img = Image.open(path).convert("RGB")
        W, H = img.size
        stat = ImageStat.Stat(img.convert("L"))
        avg = stat.mean[0] if stat.mean else 128
        if avg < 100:
            enhancer = ImageEnhance.Brightness(img)
            img2 = enhancer.enhance(1.15)
        else:
            enhancer = ImageEnhance.Contrast(img)
            img2 = enhancer.enhance(1.08)
        # produce desktop (1280x720) and mobile vertical (720x1280)
        img_desktop = ImageOps.fit(img2, (1280, 720), method=Image.Resampling.LANCZOS)
        img_desktop.save(desktop_path, "PNG")
        img_mobile = ImageOps.fit(img2, (720, 1280), method=Image.Resampling.LANCZOS)
        img_mobile.save(mobile_path, "PNG")
        return jsonify({"desktop": f"/file/{desktop_name}", "mobile": f"/file/{mobile_name}", "phase": 4})
    except Exception as e:
        log.exception("phase4 composition failed: %s", e)
        return jsonify({"error": "phase4 failed", "details": str(e)}), 500

# ---------------- Hashtag endpoints (phased) ----------------

@app.route("/api/ai/hashtags/phase1", methods=["POST"])
def ai_hashtags_phase1():
    """
    Phase1: simple hashtag extraction from title/description
    payload: { title, description }
    """
    data = request.json or {}
    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()
    if not title and not description:
        return jsonify({"hashtags": [], "phase": 1})
    text = (title + " " + description).strip()
    words = [w.strip().lower().strip("#,.!?()[]{}:;\"'") for w in text.split() if len(w) > 3][:24]
    tags = []
    seen = set()
    for w in words:
        tag = "#" + "".join(ch for ch in w if ch.isalnum())
        if tag and tag not in seen:
            tags.append(tag)
            seen.add(tag)
        if len(tags) >= 8:
            break
    return jsonify({"hashtags": tags, "phase": 1})

@app.route("/api/ai/hashtags/phase2", methods=["POST"])
def ai_hashtags_phase2():
    """
    Phase2: platform-optimized tags. payload: { title, description, platform }
    """
    data = request.json or {}
    platform = (data.get("platform") or "generic").lower()
    res = ai_hashtags_phase1().get_json()
    tags = res.get("hashtags", [])
    if platform == "tiktok":
        tags = tags[:6]
    elif platform in ("instagram", "ig"):
        tags = (tags + ["#instagood"])[:12]
    else:
        tags = tags[:8]
    return jsonify({"hashtags": tags, "platform": platform, "phase": 2})

@app.route("/api/ai/hashtags/phase3", methods=["POST"])
def ai_hashtags_phase3():
    """
    Phase3: ranking + scoring using simple local stats
    payload: { title, description, platform }
    """
    data = request.json or {}
    platform = (data.get("platform") or "generic").lower()
    base = ai_hashtags_phase2().get_json()
    tags = base.get("hashtags", [])
    scored = []
    for t in tags:
        stats = fetch_hashtag_stats(t, platform)
        score = (stats["times_used"] * 0.6) + (stats["engagement_score"] * 10) - (len(t) * 0.1)
        scored.append({"tag": t, "score": score})
    scored.sort(key=lambda x: x["score"], reverse=True)
    ranked = [s["tag"] for s in scored]
    return jsonify({"hashtags": ranked, "ranked": True, "phase": 3})

@app.route("/api/ai/hashtags/phase4", methods=["POST"])
def ai_hashtags_phase4():
    """
    Phase4: record generated tags into local learning DB
    payload: { tags (optional), platform (optional) }
    """
    data = request.json or {}
    tags = data.get("tags")
    platform = (data.get("platform") or "generic").lower()
    if not tags:
        gen = ai_hashtags_phase3().get_json()
        tags = gen.get("hashtags", [])
    for t in tags:
        try:
            record_hashtag_usage(t, platform, engagement=0.0)
        except Exception:
            pass
    return jsonify({"hashtags": tags, "learned": True, "phase": 4})

@app.route("/api/ai/hashtags/phase5", methods=["POST"])
def ai_hashtags_phase5():
    """
    Phase5: feedback endpoint for frontend to POST real engagement numbers
    payload: { tag: "#tag", platform: "tiktok", engagement: 0.0-1.0 }
    """
    data = request.json or {}
    tag = data.get("tag")
    platform = (data.get("platform") or "generic").lower()
    try:
        engagement = float(data.get("engagement") or 0.0)
    except Exception:
        engagement = 0.0
    if not tag:
        return jsonify({"error": "Missing tag"}), 400
    try:
        record_hashtag_usage(tag, platform, engagement=engagement)
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("phase5 feedback failed: %s", e)
        return jsonify({"error": "write failed", "details": str(e)}), 500

# ---------------- Noise reduction endpoints (phased) ----------------

@app.route("/api/ai/noise/phase1", methods=["POST"])
def ai_noise_phase1():
    """
    Phase1: basic ffmpeg denoise:
      - audio: afftdn + loudnorm
      - video: hqdn3d filter
    payload: { user_id, file, mode (optional: audio/video) }
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    if not file_param:
        return jsonify({"error": "Missing file"}), 400
    fname = file_param.replace("/file/", "").lstrip("/")
    in_path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(in_path):
        return jsonify({"error": "file not found"}), 404

    token = secrets.token_urlsafe(8)
    mode = (data.get("mode") or "").lower()
    if fname.lower().endswith((".mp3", ".m4a", ".wav", ".aac", ".ogg")) or mode == "audio":
        out_name = f"denoised-audio-{token}.mp3"
        out_path = os.path.join(DOWNLOAD_DIR, out_name)
        cmd = ["ffmpeg", "-y", "-i", in_path, "-af", "afftdn=nf=-25,aresample=44100,loudnorm=I=-16:TP=-1.5:LRA=11", out_path]
    else:
        out_name = f"denoised-video-{token}.mp4"
        out_path = os.path.join(DOWNLOAD_DIR, out_name)
        cmd = ["ffmpeg", "-y", "-i", in_path, "-vf", "hqdn3d=1.5:1.5:6:6", "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "copy", out_path]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return jsonify({"enhanced": f"/file/{out_name}", "phase": 1})
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors="ignore") if e.stderr else str(e)
        log.exception("noise phase1 ffmpeg failed: %s", stderr)
        return jsonify({"error": "ffmpeg failed", "details": stderr}), 500

@app.route("/api/ai/noise/phase2", methods=["POST"])
def ai_noise_phase2():
    """
    Phase2: neural denoise (RNNoise) if present, otherwise fallback to phase1.
    payload: { user_id, file, use_neural (bool) }
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    use_neural = bool(data.get("use_neural"))
    file_param = data.get("file")
    if not file_param:
        return jsonify({"error": "Missing file"}), 400
    fname = file_param.replace("/file/", "").lstrip("/")
    in_path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(in_path):
        return jsonify({"error": "file not found"}), 404

    if use_neural:
        try:
            rnnoise_cmd = shutil.which("rnnoise") or shutil.which("rnnoise_demo")
            if rnnoise_cmd:
                token = secrets.token_urlsafe(8)
                out_name = f"denoised-nn-{token}.wav"
                out_path = os.path.join(DOWNLOAD_DIR, out_name)
                tmp_wav = out_path + ".tmp.wav"
                subprocess.run(["ffmpeg", "-y", "-i", in_path, "-ar", "48000", "-ac", "1", tmp_wav], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run([rnnoise_cmd, tmp_wav, out_path], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    os.remove(tmp_wav)
                except Exception:
                    pass
                return jsonify({"enhanced": f"/file/{os.path.basename(out_path)}", "phase": 2, "method": "rnnoise"})
            else:
                log.info("rnnoise binary not found; falling back to phase1")
                return ai_noise_phase1()
        except Exception as e:
            log.exception("rnnoise processing failed: %s", e)
            return ai_noise_phase1()
    else:
        return ai_noise_phase1()

@app.route("/api/ai/noise/phase3", methods=["POST"])
def ai_noise_phase3():
    """
    Phase3: GPU-accelerated / advanced denoise hint. Attempts GPU ffmpeg if VYDRA_USE_GPU set; falls back sensibly.
    payload: { user_id, file, use_gpu (optional) }
    """
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code

    file_param = data.get("file")
    prefer_gpu = bool(os.environ.get("VYDRA_USE_GPU", "0")) or bool(data.get("use_gpu"))
    if not file_param:
        return jsonify({"error": "Missing file"}), 400
    fname = file_param.replace("/file/", "").lstrip("/")
    in_path = os.path.join(DOWNLOAD_DIR, safe_filename(fname))
    if not os.path.exists(in_path):
        return jsonify({"error": "file not found"}), 404

    token = secrets.token_urlsafe(6)
    out_name = f"denoised-gpu-{token}.mp4"
    out_path = os.path.join(DOWNLOAD_DIR, out_name)

    try:
        if prefer_gpu:
            try:
                cmd = ["ffmpeg", "-y", "-i", in_path, "-vf", "hqdn3d=1.5:1.5:6:6", "-c:v", "h264_nvenc", "-preset", "p5", "-c:a", "copy", out_path]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return jsonify({"enhanced": f"/file/{out_name}", "phase": 3, "gpu": True})
            except Exception:
                log.info("GPU ffmpeg path failed; trying CPU-based denoise")
                return ai_noise_phase2()
        else:
            try:
                cmd = ["ffmpeg", "-y", "-i", in_path, "-vf", "nlmeans=s=7:p=7", "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "copy", out_path]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return jsonify({"enhanced": f"/file/{out_name}", "phase": 3, "gpu": False, "method": "nlmeans"})
            except subprocess.CalledProcessError:
                cmd = ["ffmpeg", "-y", "-i", in_path, "-vf", "hqdn3d=1.2:1.2:4:4", "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "copy", out_path]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return jsonify({"enhanced": f"/file/{out_name}", "phase": 3, "gpu": False, "method": "hqdn3d"})
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors="ignore") if e.stderr else str(e)
        log.exception("phase3 denoise ffmpeg failed: %s", stderr)
        return jsonify({"error": "denoise failed", "details": stderr}), 500
    except Exception as e:
        log.exception("phase3 denoise error: %s", e)
        return jsonify({"error": "denoise failed", "details": str(e)}), 500


# ---------------- lightweight caption & enhancers (compat) ----------------

@app.route("/api/generate_caption", methods=["POST"])
def api_generate_caption():
    data = request.json or {}
    user_id = data.get("user_id")
    ok, resp, code = require_premium_or_400(user_id)
    if not ok:
        return resp, code
    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()
    tags = data.get("tags") or []
    short_desc = (description.split(".")[0][:120].strip()) if description else ""
    caption = title if title else "Check this out!"
    if short_desc:
        caption = f"{title} — {short_desc}" if title else short_desc
    suggestions = []
    if tags and isinstance(tags, list):
        for t in tags[:6]:
            ttext = str(t).strip().lstrip('#')
            if ttext:
                suggestions.append('#' + ''.join(ttext.split()))
    else:
        words = [w for w in (title + ' ' + description).split() if len(w) > 3]
        seen = set()
        for w in words:
            tag = '#' + ''.join(w.strip().split()).lower()
            if tag not in seen:
                suggestions.append(tag)
                seen.add(tag)
            if len(suggestions) >= 6:
                break
    short_variant = caption if len(caption) <= 100 else caption[:97] + '...'
    medium_variant = caption if len(caption) <= 200 else caption[:197] + '...'
    long_variant = (caption + '\n\n' + ' '.join(suggestions[:6]))[:800]
    return jsonify({"caption": caption, "variants": {"short": short_variant, "medium": medium_variant, "long": long_variant}, "hashtags": suggestions})

# End of AI block - leave Admin / Debug section below unchanged.

# ---------------- Admin / Debug ----------------
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
    new_expires = grant_premium(user_id, days)
    return jsonify({"ok": True, "new_expires": new_expires})

@app.route("/_health")
def health():
    return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()})

# ---------------- Cleanup worker ----------------
def cleanup_worker():
    while True:
        try:
            files = sorted([os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR)], key=os.path.getmtime)
            while len(files) > MAX_RECENT_DOWNLOADS:
                oldest = files.pop(0)
                try:
                    os.remove(oldest)
                    log.info("Removed old file: %s", oldest)
                except Exception:
                    log.exception("Failed to remove %s", oldest)
            now_ts = time.time()
            for f in list(files):
                try:
                    if now_ts - os.path.getmtime(f) > CLEANUP_SECONDS:
                        os.remove(f)
                        log.info("Auto-deleted expired file: %s", f)
                except Exception:
                    log.exception("Failed cleanup for %s", f)
        except Exception:
            log.exception("Cleanup worker error")
        time.sleep(30)

if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    threading.Thread(target=cleanup_worker, daemon=True).start()

    from flask import Flask, request, jsonify
import os
import subprocess
from PIL import Image
import requests
from io import BytesIO
import random

app = Flask(__name__)

# === Generate Thumbnail ===
@app.route("/api/thumbnail", methods=["POST"])
def generate_thumbnail():
    try:
        data = request.json
        video_url = data.get("video_url")

        # Mock thumbnail generation (in real use, download video or extract frame)
        random_color = (random.randint(0,255), random.randint(0,255), random.randint(0,255))
        thumbnail_path = f"static/thumbnail_{random.randint(1000,9999)}.jpg"

        # Create a simple thumbnail image
        img = Image.new("RGB", (480, 270), random_color)
        img.save(thumbnail_path)

        return jsonify({
            "status": "success",
            "thumbnail_url": f"/{thumbnail_path}"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# === Generate Hashtags ===
@app.route("/api/ai/hashtags", methods=["POST"])
def generate_hashtags():
    try:
        data = request.json
        caption = data.get("caption", "")
        keywords = caption.split(" ")
        hashtags = [f"#{word.strip().lower()}" for word in keywords if len(word) > 3][:10]
        return jsonify({"hashtags": hashtags}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)


# ---------------- Main ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)