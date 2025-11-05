#!/usr/bin/env python3
"""
ai_manager.py  —  Pro V2 for VYDRA

Improvements vs. V1:
 - retains all public functions expected by app.py
 - optional Supabase-backed usage & event logging (best-effort)
 - multi-feature bundle generator for AI Studio (thumbnail + hashtags + title)
 - conservative budget accounting (monthly cap) with Supabase-first write,
   local SQLite fallback for resilience
 - robust PIL/ffmpeg fallbacks for thumbnail generation and audio processing
 - careful input validation and defensive behavior so app.py can call functions
   and receive small serializable dicts on error
"""

import os
import io
import json
import time
import uuid
import sqlite3
import threading
import logging
import shutil
import subprocess
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

import requests
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps, ImageStat, ImageEnhance

# ---------------- Logging ----------------
log = logging.getLogger("ai_manager")
log.setLevel(logging.INFO)
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    log.addHandler(handler)

# ---------------- Config (env-driven) ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DB = os.path.join(BASE_DIR, "ai_manager.db")
os.makedirs(os.path.dirname(LOCAL_DB), exist_ok=True)

# External AI endpoints / keys (optional)
REPLICATE_API_KEY = os.environ.get("REPLICATE_API_KEY")
REPLICATE_API_URL = os.environ.get("REPLICATE_API_URL")  # optional generic external endpoint
THUMBNAIL_AI_URL = os.environ.get("THUMBNAIL_AI_URL")  # optional external thumbnail endpoint
THUMBNAIL_AI_KEY = os.environ.get("THUMBNAIL_AI_KEY")

# Supabase (optional)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Cost control (USD)
AI_MONTHLY_CAP = float(os.environ.get("AI_MONTHLY_CAP", "25.0"))
AI_THUMBNAIL_COST = float(os.environ.get("AI_THUMBNAIL_COST", "0.20"))  # per thumbnail candidate (external)
AI_HASHTAG_COST = float(os.environ.get("AI_HASHTAG_COST", "0.03"))     # per candidate tag list (external)
AI_AUDIO_COST = float(os.environ.get("AI_AUDIO_COST", "0.80"))         # per audio enhance request (external/neural)

# Hashtag cache settings
HASHTAG_MAX_KEEP = int(os.environ.get("HASHTAG_MAX_KEEP", "500"))
HASHTAG_EXPIRE_DAYS = int(os.environ.get("HASHTAG_EXPIRE_DAYS", "60"))

# Downloads directory
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR") or os.path.join(BASE_DIR, "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Font detection for thumbnails (cross-platform)
_possible_fonts = [
    os.environ.get("AI_FALLBACK_FONT"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "C:\\Windows\\Fonts\\arial.ttf",
    "C:\\Windows\\Fonts\\segoeui.ttf"
]
FONT_PATH = None
for p in _possible_fonts:
    if p and os.path.exists(p):
        FONT_PATH = p
        break

# ---------------- Local DB (sqlite) ----------------
def _get_conn():
    conn = sqlite3.connect(LOCAL_DB, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_local_db():
    conn = _get_conn()
    cur = conn.cursor()
    # hashtag learning
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_hashtag_learning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT NOT NULL,
            platform TEXT DEFAULT 'generic',
            times_used INTEGER DEFAULT 0,
            engagement_score REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tag, platform)
        );
    """)
    # usage accounting (month granularity)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL,
            amount REAL NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # event log (local fallback)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            event_type TEXT,
            payload TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()

init_local_db()

# ---------------- Supabase helpers (best-effort) ----------------
def _current_month_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")

def supabase_insert_usage(amount: float, note: str = "") -> bool:
    """Try to insert usage into Supabase REST table 'ai_usage' (best-effort)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        url = SUPABASE_URL.rstrip('/') + '/rest/v1/ai_usage'
        payload = {"month": _current_month_key(), "amount": amount, "note": note}
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json", "Prefer": "return=representation"}
        r = requests.post(url, headers=headers, json=payload, timeout=8)
        return r.ok
    except Exception:
        log.exception("supabase_insert_usage failed")
        return False

def supabase_get_month_sum() -> float:
    """Get sum of ai_usage.amount for current month from Supabase (best-effort)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return 0.0
    try:
        url = SUPABASE_URL.rstrip('/') + '/rest/v1/ai_usage'
        params = {"month": f"eq.{_current_month_key()}", "select": "amount"}
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        r = requests.get(url, headers=headers, params=params, timeout=8)
        if not r.ok:
            return 0.0
        rows = r.json() or []
        total = sum(float(r.get("amount", 0.0) or 0.0) for r in rows)
        return total
    except Exception:
        log.exception("supabase_get_month_sum failed")
        return 0.0

def supabase_insert_event(user_id: Optional[str], event_type: str, payload: Dict[str, Any]) -> bool:
    """Insert event to Supabase 'ai_events' table (best-effort)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        url = SUPABASE_URL.rstrip('/') + '/rest/v1/ai_events'
        body = {"user_id": user_id, "event_type": event_type, "payload": json.dumps(payload)}
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json", "Prefer": "return=representation"}
        r = requests.post(url, headers=headers, json=body, timeout=8)
        return r.ok
    except Exception:
        log.exception("supabase_insert_event failed")
        return False

# ---------------- Local usage helpers ----------------
def local_record_usage(amount: float, note: str = "") -> None:
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO ai_usage (month, amount, note) VALUES (?,?,?)", (_current_month_key(), float(amount), note))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("local_record_usage failed")

def local_get_month_sum() -> float:
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT SUM(amount) as s FROM ai_usage WHERE month=?", (_current_month_key(),))
        row = cur.fetchone()
        conn.close()
        return float(row["s"] or 0.0)
    except Exception:
        log.exception("local_get_month_sum failed")
        return 0.0

def local_insert_event(user_id: Optional[str], event_type: str, payload: Dict[str, Any]) -> None:
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO ai_events (user_id, event_type, payload) VALUES (?, ?, ?)", (user_id, event_type, json.dumps(payload)))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("local_insert_event failed")

# ---------------- Budget control ----------------
_budget_lock = threading.Lock()

def get_month_spend() -> float:
    """Return current month spend (supabase-first, fallback to local)."""
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            return supabase_get_month_sum()
        except Exception:
            pass
    return local_get_month_sum()

def reserve_ai_quota(amount: float, note: str = "") -> bool:
    """Reserve quota: if would exceed cap -> False. Otherwise record usage and return True."""
    with _budget_lock:
        current = get_month_spend()
        projected = current + float(amount)
        if projected > AI_MONTHLY_CAP:
            log.info("AI monthly cap exceeded: current=%s, requested=%s, cap=%s", current, amount, AI_MONTHLY_CAP)
            return False
        # record to supabase (best-effort) and local always
        try:
            supabase_insert_usage(amount, note)
        except Exception:
            pass
        try:
            local_record_usage(amount, note)
        except Exception:
            log.exception("local_record_usage failed after reserve")
        return True

# ---------------- Hashtag learning helpers ----------------
def record_hashtag_usage(tag: str, platform: str = "generic", engagement: float = 0.0):
    if not tag:
        return
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, times_used, engagement_score FROM ai_hashtag_learning WHERE tag=? AND platform=?", (tag, platform))
        row = cur.fetchone()
        if row:
            _id, times_used, score = row["id"], (row["times_used"] or 0), (row["engagement_score"] or 0.0)
            new_times = times_used + 1
            new_score = ((score * times_used) + float(engagement)) / (new_times if new_times > 0 else 1)
            cur.execute("UPDATE ai_hashtag_learning SET times_used=?, engagement_score=?, last_used=CURRENT_TIMESTAMP WHERE id=?", (new_times, new_score, _id))
        else:
            cur.execute("INSERT INTO ai_hashtag_learning (tag, platform, times_used, engagement_score) VALUES (?,?,1,?)", (tag, platform, float(engagement)))
        conn.commit()
        conn.close()
    except Exception:
        log.exception("record_hashtag_usage error")

def fetch_hashtag_stats(tag: str, platform: str = "generic") -> Dict[str, float]:
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT times_used, engagement_score FROM ai_hashtag_learning WHERE tag=? AND platform=?", (tag, platform))
        row = cur.fetchone()
        conn.close()
        if row:
            return {"times_used": int(row["times_used"] or 0), "engagement_score": float(row["engagement_score"] or 0.0)}
    except Exception:
        pass
    return {"times_used": 0, "engagement_score": 0.0}

# ---------------- Cleanup: hashtags & DB compaction ----------------
def cleanup_hashtag_cache() -> Dict[str, Any]:
    """Delete old hashtag rows and trim to top N by engagement. Returns summary."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM ai_hashtag_learning WHERE DATE(last_used) < DATE('now', ?)", (f"-{HASHTAG_EXPIRE_DAYS} day",))
        cur.execute("SELECT id FROM ai_hashtag_learning ORDER BY engagement_score DESC, times_used DESC LIMIT ?", (HASHTAG_MAX_KEEP,))
        keep_ids = [r[0] for r in cur.fetchall()]
        if keep_ids:
            placeholders = ",".join(["?" for _ in keep_ids])
            cur.execute(f"DELETE FROM ai_hashtag_learning WHERE id NOT IN ({placeholders})", keep_ids)
        else:
            cur.execute("DELETE FROM ai_hashtag_learning")
        conn.commit()
        cur.execute("VACUUM")
        conn.close()
        return {"ok": True}
    except Exception:
        log.exception("cleanup_hashtag_cache failed")
        return {"ok": False}

# schedule weekly cleanup thread
def _start_cleanup_thread():
    def _job():
        while True:
            try:
                cleanup_hashtag_cache()
            except Exception:
                log.exception("cleanup thread error")
            time.sleep(60 * 60 * 24 * 7)  # weekly
    t = threading.Thread(target=_job, daemon=True)
    t.start()

_start_cleanup_thread()

# ---------------- Utility: PIL fallback thumbnail ----------------
def _fallback_pil_thumbnail(out_path: str, title_text: str = "VYDRA", accent_shift: int = 0) -> bool:
    try:
        W, H = 1280, 720
        accents = [(110,231,183), (110,180,231), (200,150,255)]
        accent = accents[accent_shift % len(accents)]
        bg_color = (15, 23, 42)
        img = Image.new("RGB", (W, H), bg_color)
        draw = ImageDraw.Draw(img)
        try:
            draw.rounded_rectangle([40, 40, W-40, H-40], radius=20, fill=(17,24,39))
        except Exception:
            draw.rectangle([40, 40, W-40, H-40], fill=(17,24,39))
        try:
            font = ImageFont.truetype(FONT_PATH, 64) if FONT_PATH and os.path.exists(FONT_PATH) else ImageFont.load_default()
        except Exception:
            font = ImageFont.load_default()
        max_w = W - 200
        words = title_text.split()
        lines = []
        cur = ""
        for w in words:
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
        log.exception("_fallback_pil_thumbnail failed")
        return False

# ---------------- Optional external AI helper (generic) ----------------
def _call_external_ai(file_path: str, endpoint: Optional[str], api_key: Optional[str], extra: Optional[dict] = None) -> Tuple[bool, Optional[bytes], str]:
    """POST file to external AI endpoint. Returns (ok, content_bytes_or_none, message)"""
    if not endpoint:
        return False, None, "no_endpoint"
    try:
        headers = {}
        files = {"image": (os.path.basename(file_path), open(file_path, "rb"), "image/png")}
        params = extra or {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        r = requests.post(endpoint, files=files, data={"meta": json.dumps(params)} if params else None, headers=headers, timeout=60)
        if r.ok and r.content:
            return True, r.content, "ok"
        return False, None, f"http:{r.status_code}"
    except Exception as e:
        log.exception("_call_external_ai failed: %s", e)
        return False, None, str(e)

# ---------------- Thumbnail APIs (phases) ----------------
def generate_thumbnail_phase1(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase1: extract a single frame or fallback to PIL generated thumbnail.
    data: { user_id, file (optional '/file/name'), title (optional), timestamp (optional) }
    Returns: { thumbnail_file: '/file/xxx', phase: 1 } or error-like dict
    """
    try:
        title = (data.get("title") or "VYDRA").strip()
        file_param = data.get("file")
        token = uuid.uuid4().hex[:10]
        out_name = f"thumbnail-p1-{token}.png"
        out_path = os.path.join(DOWNLOAD_DIR, out_name)

        # if file present, try ffmpeg extraction (local)
        if file_param:
            fname = file_param.replace('/file/', '').lstrip('/')
            in_path = os.path.join(DOWNLOAD_DIR, fname)
            if os.path.exists(in_path):
                # reserve cost only when we plan to attempt external/expensive AI;
                # frame extraction itself is local, but we optionally charge a small cost for server-side processing heuristics.
                cost = AI_THUMBNAIL_COST * 0.0  # kept zero here to avoid charging for simple extraction; change if desired
                # compute timestamp
                ts = None
                if data.get("timestamp") is not None:
                    try:
                        ts = float(data.get("timestamp"))
                    except Exception:
                        ts = None
                try:
                    target_w = 1280
                    if ts is None:
                        try:
                            out = subprocess.check_output(
                                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                                 "-of", "default=noprint_wrappers=1:nokey=1", in_path],
                                stderr=subprocess.STDOUT, timeout=6
                            ).decode().strip()
                            dur = float(out) if out else None
                        except Exception:
                            dur = None
                        ts = max(1.0, dur/2.0) if dur and dur > 4.0 else 2.0
                    cmd = ["ffmpeg", "-y", "-ss", str(float(ts)), "-i", in_path, "-frames:v", "1", "-q:v", "2", "-vf", f"scale={target_w}:-2", out_path]
                    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
                    # overlay title
                    try:
                        img = Image.open(out_path).convert("RGBA")
                        draw = ImageDraw.Draw(img)
                        try:
                            font = ImageFont.truetype(FONT_PATH, 36) if FONT_PATH and os.path.exists(FONT_PATH) else ImageFont.load_default()
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
                    # log event (best-effort)
                    try:
                        user_id = data.get("user_id")
                        payload = {"file": file_param, "method": "ffmpeg_frame", "title": title}
                        supabase_insert_event(user_id, "thumbnail.p1.generated", payload)
                    except Exception:
                        pass
                    return {"thumbnail_file": f"/file/{out_name}", "phase": 1}
                except subprocess.CalledProcessError as e:
                    log.warning("phase1 ffmpeg extraction failed: %s", e)
                except Exception:
                    log.exception("phase1 ffmpeg extraction error")
        # fallback to PIL
        ok = _fallback_pil_thumbnail(out_path, (data.get("title") or "VYDRA"), accent_shift=0)
        if ok:
            return {"thumbnail_file": f"/file/{out_name}", "phase": 1}
        return {"error": "thumbnail_failed"}
    except Exception as e:
        log.exception("generate_thumbnail_phase1 unexpected error: %s", e)
        return {"error": "thumbnail_exception", "details": str(e)}

def generate_thumbnail_phase2(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase2: produce 3 candidate thumbnails.
    data: { user_id, file (optional), title (optional) }
    Returns: { candidates: ['/file/a','...'], phase: 2 }
    """
    try:
        title = (data.get("title") or "VYDRA").strip()
        file_param = data.get("file")
        token_base = uuid.uuid4().hex[:8]
        candidates: List[str] = []

        if not file_param:
            # purely local variants — no AI cost
            for i in range(3):
                out_name = f"thumbnail-p2-{token_base}-{i}.png"
                out_path = os.path.join(DOWNLOAD_DIR, out_name)
                _fallback_pil_thumbnail(out_path, f"{title} • variant {i+1}", accent_shift=i)
                candidates.append(f"/file/{out_name}")
            return {"candidates": candidates, "phase": 2}

        fname = file_param.replace('/file/', '').lstrip('/')
        in_path = os.path.join(DOWNLOAD_DIR, fname)
        if not os.path.exists(in_path):
            for i in range(3):
                out_name = f"thumbnail-p2-{token_base}-{i}.png"
                out_path = os.path.join(DOWNLOAD_DIR, out_name)
                _fallback_pil_thumbnail(out_path, f"{title} • variant {i+1}", accent_shift=i)
                candidates.append(f"/file/{out_name}")
            return {"candidates": candidates, "phase": 2}

        # extract frames at heuristic points
        try:
            out = subprocess.check_output(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", in_path],
                stderr=subprocess.STDOUT, timeout=6
            ).decode().strip()
            dur = float(out) if out else None
        except Exception:
            dur = None

        points = [2.0, 4.0, 6.0]
        if dur and dur > 10:
            points = [max(1.0, dur*0.25), max(2.0, dur*0.5), max(3.0, dur*0.75)]

        for idx, p in enumerate(points):
            out_name = f"thumbnail-p2-{token_base}-{idx}.png"
            out_path = os.path.join(DOWNLOAD_DIR, out_name)
            try:
                cmd = ["ffmpeg", "-y", "-ss", str(float(p)), "-i", in_path, "-frames:v", "1", "-q:v", "2", "-vf", "scale=1280:-2", out_path]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
                # overlay
                try:
                    img = Image.open(out_path).convert("RGBA")
                    draw = ImageDraw.Draw(img)
                    try:
                        font = ImageFont.truetype(FONT_PATH, 40) if FONT_PATH and os.path.exists(FONT_PATH) else ImageFont.load_default()
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
            except Exception:
                log.exception("phase2 extraction failed for point %s, using fallback", p)
                _fallback_pil_thumbnail(out_path, f"{title} • variant {idx+1}", accent_shift=idx)
            candidates.append(f"/file/{out_name}")

        # log event
        try:
            user_id = data.get("user_id")
            supabase_insert_event(user_id, "thumbnail.p2.generated", {"candidates": candidates})
        except Exception:
            pass

        return {"candidates": candidates, "phase": 2}
    except Exception as e:
        log.exception("generate_thumbnail_phase2 unexpected error: %s", e)
        return {"error": "thumbnail_p2_exception", "details": str(e)}

def generate_thumbnail_phase3(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase3: enhance a chosen thumbnail either via external AI or local processing.
    data: { user_id, file, use_ai (bool) }
    """
    try:
        file_param = data.get("file")
        use_ai = bool(data.get("use_ai"))
        if not file_param:
            return {"error": "missing file"}

        fname = file_param.replace('/file/', '').lstrip('/')
        path = os.path.join(DOWNLOAD_DIR, fname)
        if not os.path.exists(path):
            return {"error": "file not found"}

        # reserve cost only if using external AI
        if use_ai and (THUMBNAIL_AI_URL or REPLICATE_API_URL):
            if not reserve_ai_quota(AI_THUMBNAIL_COST, note="thumbnail.p3"):
                return {"error": "ai_budget_exceeded", "allowed": False}

        if use_ai and (THUMBNAIL_AI_URL or REPLICATE_API_URL):
            endpoint = THUMBNAIL_AI_URL or REPLICATE_API_URL
            api_key = THUMBNAIL_AI_KEY or REPLICATE_API_KEY
            ok, content, msg = _call_external_ai(path, endpoint, api_key, extra={"purpose": "thumbnail_enhance"})
            if ok and content:
                try:
                    with open(path, "wb") as fh:
                        fh.write(content)
                    # log
                    try:
                        supabase_insert_event(data.get("user_id"), "thumbnail.p3.ai_success", {"file": file_param})
                    except Exception:
                        pass
                    return {"ai_attempted": True, "ai_ok": True, "ai_msg": msg, "phase": 3}
                except Exception:
                    log.exception("Failed writing AI-enhanced thumbnail to disk")
                    return {"ai_attempted": True, "ai_ok": False, "ai_msg": "write_failed", "phase": 3}
            # AI attempted but failed
            return {"ai_attempted": True, "ai_ok": False, "ai_msg": msg, "phase": 3}

        # local enhance fallback (no AI budget consumed)
        try:
            img = Image.open(path).convert("RGB")
            img = ImageOps.autocontrast(img, cutoff=1)
            img = img.filter(ImageFilter.UnsharpMask(radius=1, percent=120, threshold=3))
            img.save(path, "PNG")
            return {"ai_attempted": False, "ai_ok": True, "note": "local_enhanced", "phase": 3}
        except Exception as e:
            log.exception("phase3 local enhance failed: %s", e)
            return {"ai_attempted": False, "ai_ok": False, "note": "local_failed", "phase": 3, "details": str(e)}
    except Exception as e:
        log.exception("generate_thumbnail_phase3 unexpected error: %s", e)
        return {"error": "thumbnail_p3_exception", "details": str(e)}

def generate_thumbnail_phase4(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase4: compose desktop & mobile variants from a base thumbnail.
    data: { user_id, file, title (optional) }
    """
    try:
        file_param = data.get("file")
        title = (data.get("title") or "").strip()
        if not file_param:
            return {"error": "Missing base thumbnail file"}

        fname = file_param.replace('/file/', '').lstrip('/')
        path = os.path.join(DOWNLOAD_DIR, fname)
        if not os.path.exists(path):
            return {"error": "File not found"}

        token = uuid.uuid4().hex[:8]
        desktop_name = f"thumbnail-p4-desktop-{token}.png"
        mobile_name = f"thumbnail-p4-mobile-{token}.png"
        desktop_path = os.path.join(DOWNLOAD_DIR, desktop_name)
        mobile_path = os.path.join(DOWNLOAD_DIR, mobile_name)

        try:
            img = Image.open(path).convert("RGB")
            stat = ImageStat.Stat(img.convert("L")) if img else None
            avg = stat.mean[0] if (stat and stat.mean) else 128
            if avg < 100:
                img2 = ImageEnhance.Brightness(img).enhance(1.15) if hasattr(ImageEnhance, 'Brightness') else img
            else:
                img2 = ImageEnhance.Contrast(img).enhance(1.08) if hasattr(ImageEnhance, 'Contrast') else img
            if hasattr(Image, 'Resampling'):
                img_desktop = ImageOps.fit(img2, (1280, 720), method=Image.Resampling.LANCZOS)
                img_mobile = ImageOps.fit(img2, (720, 1280), method=Image.Resampling.LANCZOS)
            else:
                img_desktop = ImageOps.fit(img2, (1280,720))
                img_mobile = ImageOps.fit(img2, (720,1280))
            img_desktop.save(desktop_path, "PNG")
            img_mobile.save(mobile_path, "PNG")
            return {"desktop": f"/file/{desktop_name}", "mobile": f"/file/{mobile_name}", "phase": 4}
        except Exception as e:
            log.exception("phase4 composition failed: %s", e)
            return {"error": "phase4_failed", "details": str(e)}
    except Exception as e:
        log.exception("generate_thumbnail_phase4 unexpected error: %s", e)
        return {"error": "thumbnail_p4_exception", "details": str(e)}

# ---------------- Hashtag generator ----------------
def generate_hashtags(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates hashtags (local heuristics). If external AI wired, consider using reserve_ai_quota.
    data: { title, description, platform, candidates (int, default 1) }
    """
    try:
        title = (data.get("title") or "").strip()
        description = (data.get("description") or "").strip()
        platform = (data.get("platform") or "generic").lower()
        candidates = int(data.get("candidates") or 1)
        candidates = max(1, min(4, candidates))

        base_text = (title + " " + description).strip()
        if not base_text:
            return {"hashtags": [], "phase": 1}

        words = [w.strip().lower().strip("#,.!?()[]{}:;\"'~") for w in base_text.split() if len(w) > 3][:48]
        tags = []
        seen = set()
        for w in words:
            tag = "#" + "".join(ch for ch in w if ch.isalnum())
            if tag and tag not in seen:
                tags.append(tag)
                seen.add(tag)
            if len(tags) >= 12:
                break

        variants: List[List[str]] = []
        for i in range(candidates):
            v = list(tags)
            if platform == "tiktok":
                v = v[:6]
            elif platform in ("instagram", "ig"):
                v = (v + ["#instagood"])[:12]
            else:
                v = v[:8]
            if i > 0:
                v = sorted(v, key=lambda x: (len(x), x)) if i % 2 == 0 else list(reversed(v))
            variants.append(v)

        # persist usage stats for first variant
        try:
            for t in (variants[0] if variants else []):
                record_hashtag_usage(t, platform, engagement=0.0)
        except Exception:
            log.exception("record_hashtag_usage in generate_hashtags failed")

        # log event (best-effort)
        try:
            supabase_insert_event(data.get("user_id"), "hashtags.generated", {"title": title, "variants_count": len(variants)})
        except Exception:
            pass

        return {"hashtags": variants[0] if variants else [], "variants": variants, "phase": 1}
    except Exception as e:
        log.exception("generate_hashtags unexpected error: %s", e)
        return {"error": "hashtags_exception", "details": str(e)}

# ---------------- Audio enhancement ----------------
def enhance_audio_ai(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhance audio: neural (rnnoise) or ffmpeg-based fallback.
    data: { user_id, file, use_neural (optional) }
    """
    try:
        file_param = data.get("file")
        use_neural = bool(data.get("use_neural"))
        if not file_param:
            return {"error": "missing file"}

        # conservative: charge AI_AUDIO_COST for each enhancement attempt (helps limit abuse)
        if not reserve_ai_quota(AI_AUDIO_COST, note="audio.enhance"):
            return {"error": "ai_budget_exceeded", "allowed": False}

        fname = file_param.replace('/file/', '').lstrip('/')
        in_path = os.path.join(DOWNLOAD_DIR, fname)
        if not os.path.exists(in_path):
            return {"error": "file not found"}

        token = uuid.uuid4().hex[:8]
        out_name = f"denoised-audio-{token}.mp3"
        out_path = os.path.join(DOWNLOAD_DIR, out_name)

        # try rnnoise if requested and binary present
        if use_neural:
            rnnoise_bin = shutil.which("rnnoise") or shutil.which("rnnoise_demo")
            if rnnoise_bin:
                try:
                    tmp_wav = os.path.join(DOWNLOAD_DIR, f"{token}.tmp.wav")
                    tmp_out_wav = os.path.join(DOWNLOAD_DIR, f"{token}.denoised.wav")
                    subprocess.run(["ffmpeg", "-y", "-i", in_path, "-ar", "48000", "-ac", "1", tmp_wav],
                                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    subprocess.run([rnnoise_bin, tmp_wav, tmp_out_wav], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    subprocess.run(["ffmpeg", "-y", "-i", tmp_out_wav, "-vn", "-c:a", "libmp3lame", "-q:a", "4", out_path],
                                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    for f in (tmp_wav, tmp_out_wav):
                        try:
                            os.remove(f)
                        except Exception:
                            pass
                    # log event
                    try:
                        supabase_insert_event(data.get("user_id"), "audio.enhance", {"method": "rnnoise"})
                    except Exception:
                        pass
                    return {"enhanced": f"/file/{out_name}", "method": "rnnoise"}
                except Exception:
                    log.exception("rnnoise flow failed, falling back")

        # fallback ffmpeg chain -> afftdn + loudnorm -> mp3
        try:
            cmd = [
                "ffmpeg", "-y", "-i", in_path, "-vn",
                "-af", "afftdn=nf=-25,aresample=44100,loudnorm=I=-16:TP=-1.5:LRA=11",
                "-c:a", "libmp3lame", "-q:a", "4", out_path
            ]
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                supabase_insert_event(data.get("user_id"), "audio.enhance", {"method": "ffmpeg_loudnorm"})
            except Exception:
                pass
            return {"enhanced": f"/file/{out_name}", "method": "ffmpeg_loudnorm"}
        except Exception as e:
            log.exception("audio enhancement failed: %s", e)
            return {"error": "audio_enhance_failed", "details": str(e)}
    except Exception as e:
        log.exception("enhance_audio_ai unexpected error: %s", e)
        return {"error": "audio_exception", "details": str(e)}

# ---------------- New convenience: multi-feature bundle ----------------
def generate_ai_bundle(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produce a small bundle with:
     - thumbnail candidates (phase2)
     - hashtags (generate_hashtags)
     - suggested title (very simple heuristic)
    Returns: { thumbnails: [...], hashtags: [...], title: "...", analytics: {...} }
    This is a convenience for AI Studio frontends to call one function.
    """
    try:
        # Keep everything defensive: call existing helpers
        user_id = data.get("user_id")
        title_hint = (data.get("title") or "").strip() or ""
        file_param = data.get("file") or data.get("video_file") or data.get("video")
        description = data.get("description") or data.get("caption") or ""

        thumb_req = {"user_id": user_id, "file": file_param, "title": title_hint}
        thumbs = generate_thumbnail_phase2(thumb_req)
        tags_req = {"title": title_hint, "description": description, "platform": (data.get("platform") or "generic"), "candidates": int(data.get("tag_candidates") or 1)}
        hashtags = generate_hashtags(tags_req)

        # naive title suggestion: keep existing or shorten description
        if title_hint:
            suggested_title = title_hint
        else:
            # pick first 8 words of description or "VYDRA Video"
            w = [t for t in (description or "").split() if t]
            if not w:
                suggested_title = "VYDRA Video"
            else:
                suggested_title = " ".join(w[:8]).strip()
                if len(suggested_title) < 3:
                    suggested_title = (w[0] if w else "VYDRA Video")

        # log bundle event
        try:
            supabase_insert_event(user_id, "bundle.generated", {"file": file_param, "title": suggested_title})
        except Exception:
            local_insert_event(user_id, "bundle.generated", {"file": file_param, "title": suggested_title})

        return {
            "thumbnails": thumbs.get("candidates") or ([thumbs.get("thumbnail_file")] if thumbs.get("thumbnail_file") else []),
            "hashtags": hashtags.get("hashtags") or [],
            "variants": hashtags.get("variants") or [],
            "title": suggested_title,
            "phase": "bundle"
        }
    except Exception as e:
        log.exception("generate_ai_bundle failed: %s", e)
        return {"error": "bundle_failed", "details": str(e)}

# ---------------- Expose public functions ----------------
__all__ = [
    "generate_thumbnail_phase1",
    "generate_thumbnail_phase2",
    "generate_thumbnail_phase3",
    "generate_thumbnail_phase4",
    "generate_hashtags",
    "enhance_audio_ai",
    "cleanup_hashtag_cache",
    "record_hashtag_usage",
    "fetch_hashtag_stats",
    "generate_ai_bundle",
    "get_month_spend",
]

# ---------------- Quick sanity log ----------------
log.info("ai_manager (Pro V2) loaded; monthly cap=$%s, thumbnail_cost=$%s, hashtag_cost=$%s, audio_cost=$%s",
         AI_MONTHLY_CAP, AI_THUMBNAIL_COST, AI_HASHTAG_COST, AI_AUDIO_COST)
