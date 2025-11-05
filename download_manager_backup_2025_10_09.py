# download_manager.py
"""
VYDRA - Full-featured backend helper
- Threaded, crash-proof downloads (yt-dlp)
- Cache folder + 7-day auto cleanup
- Thumbnail generation (yt-dlp thumbnail or ffmpeg frame extraction)
- Hashtag generator (n-grams)
- Endpoints:
  POST /api/download            -> enqueue download
  GET  /api/status/<task_id>    -> check download status
  GET  /api/file/<task_id>      -> download ready file
  POST /api/thumbnail           -> enqueue thumbnail generation
  GET  /api/thumb_status/<id>   -> poll thumbnail status
  GET  /api/cache/<filename>    -> serve cached file
  POST /api/ai/hashtags         -> generate hashtags from text or url
  GET  /api/health              -> healthcheck
"""

import os
import time
import uuid
import hashlib
import threading
import logging
import subprocess
from pathlib import Path
from collections import Counter
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS

import yt_dlp
import requests
from PIL import Image

# ---------- Configuration ----------
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL_SECONDS = 60 * 60 * 24 * 7  # 7 days TTL, adjust if needed
MAX_WORKERS = 6  # concurrent workers (tune for your machine)
DOWNLOAD_RETRIES = 3

# ---------- App & logging ----------
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})  # restrict origins in production
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Executors & in-memory stores ----------
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Download tasks
tasks_lock = threading.Lock()
tasks = {}  # task_id -> {status, progress, filename, filepath, error, meta, created_at}

# Thumbnail tasks
thumb_lock = threading.Lock()
thumbnail_tasks = {}  # tid -> {status, progress, filename, error, created_at}

# ---------- Utilities ----------
def sha_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

def set_task(task_id, **kwargs):
    with tasks_lock:
        tasks.setdefault(task_id, {
            "status": "queued", "progress": 0.0, "filename": None,
            "filepath": None, "error": None, "meta": {}, "created_at": datetime.utcnow()
        })
        tasks[task_id].update(kwargs)

def get_task(task_id):
    with tasks_lock:
        return dict(tasks.get(task_id)) if task_id in tasks else None

def set_thumb_task(tid, **kwargs):
    with thumb_lock:
        thumbnail_tasks.setdefault(tid, {
            "status": "queued", "progress": 0.0, "filename": None,
            "error": None, "created_at": time.time()
        })
        thumbnail_tasks[tid].update(kwargs)

def get_thumb_task(tid):
    with thumb_lock:
        return dict(thumbnail_tasks.get(tid)) if tid in thumbnail_tasks else None

# ---------- Background cache cleaner ----------
def cache_cleaner():
    while True:
        now = time.time()
        removed = 0
        try:
            for fp in list(CACHE_DIR.iterdir()):
                try:
                    if now - fp.stat().st_mtime > CACHE_TTL_SECONDS:
                        fp.unlink(missing_ok=True)
                        removed += 1
                except Exception as e:
                    logging.exception("Error cleaning cache file %s: %s", fp, e)
            if removed:
                logging.info("Cache cleaner removed %d file(s).", removed)
        except Exception:
            logging.exception("Cache cleaner top-level error")
        time.sleep(60 * 60)  # run hourly

threading.Thread(target=cache_cleaner, daemon=True).start()

# ---------- yt-dlp download progress hook ----------
def yt_progress_hook(task_id, d):
    try:
        status = d.get("status")
        if status == "downloading":
            downloaded = d.get("downloaded_bytes") or 0
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            percent = (downloaded / total * 100.0) if total else 0.0
            set_task(task_id, status="downloading", progress=min(max(percent, 0.0), 99.0))
        elif status == "finished":
            set_task(task_id, status="processing", progress=98.0)
        elif status == "error":
            set_task(task_id, status="error", progress=0.0, error=d.get("msg", "yt-dlp error"))
    except Exception:
        logging.exception("Error in yt_progress_hook for task %s", task_id)

# ---------- Download worker ----------
def download_worker(task_id, url, ytdlp_opts=None):
    set_task(task_id, status="starting", progress=1.0)
    key = sha_key(url)

    # cache check
    for cand in CACHE_DIR.glob(f"{key}.*"):
        set_task(task_id, status="ready", progress=100.0, filename=cand.name, filepath=str(cand))
        logging.info("Cache hit for %s -> %s", url, cand)
        return

    out_template = str(CACHE_DIR / f"{key}.%(ext)s")
    opts = {
        "outtmpl": out_template,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "retries": DOWNLOAD_RETRIES,
        "progress_hooks": [lambda d: yt_progress_hook(task_id, d)],
    }
    if ytdlp_opts:
        opts.update(ytdlp_opts)

    try:
        set_task(task_id, status="fetching", progress=5.0)
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            ext = info.get("ext") or "mp4"
            expected_fp = CACHE_DIR / f"{key}.{ext}"
            if not expected_fp.exists():
                # try to find any candidate matching key
                found = None
                for c in CACHE_DIR.glob(f"{key}.*"):
                    found = c
                    break
                if found:
                    expected_fp = found
            if not expected_fp.exists():
                raise FileNotFoundError("downloaded file not found after yt-dlp finished")
            set_task(task_id, status="ready", progress=100.0,
                     filename=expected_fp.name, filepath=str(expected_fp),
                     meta={"title": info.get("title"), "uploader": info.get("uploader")})
            logging.info("Download task %s finished -> %s", task_id, expected_fp)
    except Exception as e:
        logging.exception("Download failed for task %s: %s", task_id, e)
        set_task(task_id, status="error", progress=0.0, error=str(e))

# ---------- Flask endpoints: downloads ----------
@app.route("/api/download", methods=["POST", "OPTIONS"])
def api_start_download():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json(force=True) or {}
    url = data.get("url")
    if not url:
        return jsonify({"error": "missing 'url'"}), 400
    task_id = str(uuid.uuid4())
    set_task(task_id, status="queued", progress=0.0)
    executor.submit(download_worker, task_id, url, data.get("ytdlp_opts"))
    return jsonify({"task_id": task_id, "status_url": f"/api/status/{task_id}"}), 202

@app.route("/api/status/<task_id>", methods=["GET"])
def api_status(task_id):
    t = get_task(task_id)
    if not t:
        return jsonify({"error": "task not found"}), 404
    payload = dict(t)
    if isinstance(payload.get("created_at"), datetime):
        payload["created_at"] = payload["created_at"].isoformat() + "Z"
    return jsonify(payload)

@app.route("/api/file/<task_id>", methods=["GET"])
def api_file(task_id):
    t = get_task(task_id)
    if not t:
        return jsonify({"error": "task not found"}), 404
    if t.get("status") != "ready":
        return jsonify({"error": "file not ready", "status": t.get("status")}), 409
    fp = t.get("filepath")
    if not fp or not Path(fp).exists():
        return jsonify({"error": "file missing on server"}), 404
    return send_file(fp, as_attachment=True, download_name=t.get("filename"))

# ---------- Serve cached files ----------
@app.route("/api/cache/<path:filename>", methods=["GET"])
def serve_cache_file(filename):
    fp = CACHE_DIR / filename
    if not fp.exists():
        return jsonify({"error": "file not found"}), 404
    # Let send_file set appropriate headers
    return send_file(str(fp), as_attachment=False)

# ---------- Thumbnail worker (cache-URL only) ----------
def download_url_to_path(url, dest_path, timeout=30):
    resp = requests.get(url, stream=True, timeout=timeout)
    resp.raise_for_status()
    with open(dest_path, "wb") as fh:
        for chunk in resp.iter_content(1024 * 32):
            if chunk:
                fh.write(chunk)
    return dest_path

def ffmpeg_extract_frame(video_path, time_seconds, out_path):
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(time_seconds),
        "-i", str(video_path),
        "-frames:v", "1",
        "-q:v", "2",
        str(out_path)
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode("utf-8", errors="ignore"))
    return out_path

def thumbnail_worker(tid, url, time_sec=5, prefer_yt_thumbnail=True):
    set_thumb_task(tid, status="starting", progress=5.0)
    key = sha_key(url)
    out_name = f"{key}_thumb.jpg"
    out_path = CACHE_DIR / out_name

    if out_path.exists():
        set_thumb_task(tid, status="ready", progress=100.0, filename=out_name)
        return

    try:
        set_thumb_task(tid, status="fetching_meta", progress=10.0)
        thumb_url = None
        try:
            with yt_dlp.YoutubeDL({"skip_download": True, "quiet": True}) as ydl:
                info = ydl.extract_info(url, download=False)
            thumb_url = info.get("thumbnail")
        except Exception:
            thumb_url = None

        if prefer_yt_thumbnail and thumb_url:
            try:
                set_thumb_task(tid, status="downloading_image", progress=35.0)
                download_url_to_path(thumb_url, out_path)
                try:
                    img = Image.open(out_path)
                    img = img.convert("RGB")
                    img.thumbnail((1280, 720))
                    img.save(out_path, format="JPEG", quality=85)
                except Exception:
                    pass
                set_thumb_task(tid, status="ready", progress=100.0, filename=out_name)
                return
            except Exception as e:
                logging.exception("Thumbnail direct download failed: %s", e)
                set_thumb_task(tid, status="thumb_download_failed", progress=40.0, error=str(e))

        # fallback: download small video and extract frame
        set_thumb_task(tid, status="downloading_video", progress=45.0)
        tmp_template = str(CACHE_DIR / f"{key}_tmp.%(ext)s")
        ytdl_opts = {
            "outtmpl": tmp_template,
            "noplaylist": True,
            "quiet": True,
            "format": "best[height<=480]/best"
        }

        downloaded_path = None
        with yt_dlp.YoutubeDL(ytdl_opts) as ydl:
            info2 = ydl.extract_info(url, download=True)
            ext = info2.get("ext", "mp4")
            candidate = CACHE_DIR / f"{key}_tmp.{ext}"
            if candidate.exists():
                downloaded_path = candidate

        if not downloaded_path:
            raise RuntimeError("Could not download fallback video for thumbnail extraction")

        set_thumb_task(tid, status="extracting_frame", progress=75.0)
        try:
            ffmpeg_extract_frame(downloaded_path, time_sec, out_path)
        finally:
            try:
                downloaded_path.unlink(missing_ok=True)
            except Exception:
                pass

        # normalize & resize
        try:
            img = Image.open(out_path)
            img = img.convert("RGB")
            img.thumbnail((1280, 720))
            img.save(out_path, format="JPEG", quality=85)
        except Exception:
            pass

        set_thumb_task(tid, status="ready", progress=100.0, filename=out_name)
    except Exception as e:
        logging.exception("Thumbnail worker error for %s: %s", tid, e)
        set_thumb_task(tid, status="error", progress=0.0, error=str(e))

@app.route("/api/thumbnail", methods=["POST", "OPTIONS"])
def api_enqueue_thumbnail():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json(force=True) or {}
    url = data.get("url")
    time_sec = int(data.get("time", 5))
    if not url:
        return jsonify({"error": "missing url"}), 400
    tid = str(uuid.uuid4())
    set_thumb_task(tid, status="queued", progress=0.0)
    executor.submit(thumbnail_worker, tid, url, time_sec)
    return jsonify({"task_id": tid, "status_url": f"/api/thumb_status/{tid}"}), 202

@app.route("/api/thumb_status/<tid>", methods=["GET"])
def api_thumb_status(tid):
    t = get_thumb_task(tid)
    if not t:
        return jsonify({"error": "task not found"}), 404
    payload = {
        "status": t.get("status"),
        "progress": t.get("progress"),
        "filename": t.get("filename"),
        "thumbnail_url": (f"/api/cache/{t.get('filename')}") if t.get("filename") else None,
        "error": t.get("error"),
    }
    return jsonify(payload)

# ---------- Hashtags generator ----------
STOPWORDS = {
    "the","and","for","with","this","that","from","your","have","you","not","are","was","but","they",
    "their","what","when","where","which","will","about","would","there","here","like","can","all",
    "it's","its","our","service","video","watch","download","https","http","com","www","also",
    "get","use","using","new","one","two","three","just","clip","time","day","days","week",
    "month","years","year","make","people","see","show","many","how","more","most","via","amp"
}

def generate_hashtags_from_text(text, n=8):
    s = text.lower()
    s = re.sub(r"http\S+", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    tokens = [w for w in s.split() if len(w) > 2 and not w.isdigit()]
    tokens = [w for w in tokens if w not in STOPWORDS]

    uni = Counter(tokens)
    bi = Counter()
    tri = Counter()
    for i in range(len(tokens)-1):
        bi[f"{tokens[i]} {tokens[i+1]}"] += 1
    for i in range(len(tokens)-2):
        tri[f"{tokens[i]} {tokens[i+1]} {tokens[i+2]}"] += 1

    candidates = Counter()
    for k,v in uni.items():
        candidates[k] += v * 1.0
    for k,v in bi.items():
        candidates[k] += v * 2.0
    for k,v in tri.items():
        candidates[k] += v * 3.0

    sorted_items = [p for p,_ in candidates.most_common((n*3))]
    hashtags = []
    seen = set()
    def format_tag(phrase):
        parts = phrase.split()
        if len(parts) == 1:
            return "#" + parts[0]
        return "#" + "".join(p.capitalize() for p in parts)

    for phrase in sorted_items:
        tag = format_tag(phrase)
        if tag.lower() in seen:
            continue
        hashtags.append(tag)
        seen.add(tag.lower())
        if len(hashtags) >= n:
            break

    if "#VYDRA".lower() not in seen:
        hashtags.append("#VYDRA")
    return hashtags[:n]

@app.route("/api/ai/hashtags", methods=["POST", "OPTIONS"])
def api_hashtags():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json(force=True) or {}
    text = (data.get("text") or "").strip()
    url = data.get("url")
    n = int(data.get("n", 8))
    try:
        if url and not text:
            with yt_dlp.YoutubeDL({"skip_download": True, "quiet": True}) as ydl:
                info = ydl.extract_info(url, download=False)
            title = info.get("title") or ""
            desc = info.get("description") or ""
            text = f"{title} {desc}"
        if not text:
            return jsonify({"error": "provide 'text' or 'url' in request body"}), 400
        tags = generate_hashtags_from_text(text, n=n)
        return jsonify({"hashtags": tags})
    except Exception as e:
        logging.exception("Hashtag generation error")
        return jsonify({"error": str(e)}), 500

# ---------- Healthcheck ----------
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "workers": MAX_WORKERS})

# ---------- Run ----------
if __name__ == "__main__":
    logging.info("Starting download_manager server on port 5000")
    app.run(host="0.0.0.0", port=5000, threaded=True)
