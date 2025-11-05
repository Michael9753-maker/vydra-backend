# download_manager.py
"""
Download manager compatible with the VYDRA app adapter.

Exposes:
 - get_default_manager(progress_callback=..., download_dir=..., db_path=...)
 - start_download_job(...), get_job_status(job_id), cancel_job(job_id), list_history(limit=...)

Behavior highlights:
 - Asynchronous downloads using yt-dlp in worker threads
 - Progress posted to progress_callback(job_id, payload)
 - Robust final-file discovery and short polling for post-processing
 - Cleanup routine after successful/finished runs to keep N newest files and
   remove files older than CLEANUP_SECONDS (both configurable via env)
 - Provides shutdown() to cancel running jobs and wait briefly for threads
"""
from __future__ import annotations

import os
import threading
import uuid
import time
import traceback
import glob
import logging
import shutil
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

# try to import yt-dlp
try:
    from yt_dlp import YoutubeDL
    YTDLP_AVAILABLE = True
except Exception:
    YoutubeDL = None
    YTDLP_AVAILABLE = False

log = logging.getLogger("download_manager")
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

# in-memory job records
_DOWNLOADS: Dict[str, Dict[str, Any]] = {}
_LOCK = threading.Lock()

# cleanup configuration (override via env)
CLEANUP_SECONDS = int(os.environ.get("VYDRA_CLEANUP_SECONDS", 1800))        # 30 minutes default
MAX_RECENT_DOWNLOADS = int(os.environ.get("VYDRA_MAX_RECENT_DOWNLOADS", 5))  # keep last 5 by default

# helper
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class DownloadManager:
    def __init__(self, progress_callback: Optional[callable] = None, download_dir: Optional[str] = None, db_path: Optional[str] = None):
        """
        progress_callback(job_id: str, payload: dict) will be called on updates.
        download_dir: folder to store files. default: ./downloads next to this file.
        db_path: reserved for compatibility; not used here except passed through.
        """
        self.progress_callback = progress_callback
        base_dir = os.path.dirname(__file__)
        self.download_dir = download_dir or os.path.join(base_dir, "downloads")
        os.makedirs(self.download_dir, exist_ok=True)
        self._cancel_flags: Dict[str, threading.Event] = {}
        self._threads: Dict[str, threading.Thread] = {}
        self._shutdown_lock = threading.Lock()
        self._is_shutting_down = False

    # ---- internal helpers ----
    def _emit(self, job_id: str, payload: Dict[str, Any]):
        """Update internal state and call progress callback (defensively)."""
        if not isinstance(payload, dict):
            return
        with _LOCK:
            j = _DOWNLOADS.get(job_id, {})
            j.update(payload)
            j.setdefault("job_id", job_id)
            j["last_update"] = _now_iso()
            _DOWNLOADS[job_id] = j
        # call callback but never allow it to crash manager
        try:
            if self.progress_callback:
                try:
                    self.progress_callback(job_id, payload)
                except Exception:
                    log.debug("progress_callback raised", exc_info=True)
        except Exception:
            log.exception("Error invoking progress callback")

    def _progress_hook(self, job_id: str):
        """Return a callable for yt-dlp progress_hooks that captures job_id and cancel flag."""
        cancel_ev = self._cancel_flags.get(job_id)
        def hook(d: Dict[str, Any]):
            # This will be called frequently by yt-dlp
            try:
                status = d.get("status")
                if status == "downloading":
                    downloaded = d.get("downloaded_bytes")
                    total = d.get("total_bytes") or d.get("total_bytes_estimate")
                    percent = None
                    if downloaded is not None and total:
                        try:
                            percent = int(round((downloaded / float(total)) * 100))
                        except Exception:
                            percent = None
                    elif d.get("progress"):
                        try:
                            p = d.get("progress")
                            if isinstance(p, dict) and "percent" in p:
                                percent = int(round(float(p.get("percent", 0))))
                        except Exception:
                            percent = None

                    # fallback: elapsed clamped under 95
                    if percent is None:
                        try:
                            elapsed = int(d.get("elapsed") or 0)
                            percent = min(95, elapsed)
                        except Exception:
                            percent = 0

                    payload = {"status": "downloading", "progress": {"percent": int(percent)}}
                    if d.get("filename"):
                        payload["file_path"] = d.get("filename")
                        payload["file"] = f"/api/file/{os.path.basename(d.get('filename'))}"
                    if d.get("info_dict") and d["info_dict"].get("title"):
                        payload["title"] = d["info_dict"]["title"]
                    self._emit(job_id, payload)

                elif status == "finished":
                    filename = d.get("filename")
                    payload = {"status": "processing", "message": "download finished, post-processing", "progress": {"percent": 95}}
                    if filename:
                        payload["file_path"] = filename
                        payload["file"] = f"/api/file/{os.path.basename(filename)}"
                    self._emit(job_id, payload)

                elif status == "error":
                    self._emit(job_id, {"status": "error", "error": d.get("error", "download error")})

            except Exception:
                log.exception("progress_hook exception for job %s", job_id)
                # attempt to signal error to cache
                try:
                    self._emit(job_id, {"status": "error", "error": "progress_hook_exception"})
                except Exception:
                    pass

            # cancellation: break out of yt-dlp by raising KeyboardInterrupt
            if cancel_ev and cancel_ev.is_set():
                raise KeyboardInterrupt("cancelled by user")

        return hook

    def _find_final_file(self, job_id: str, info: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        Try multiple heuristics to find final file output by yt-dlp.
        Returns absolute path or None.
        """
        try:
            # 1) files in download_dir that contain job_id or start with job_id.
            candidates = []
            for f in os.listdir(self.download_dir):
                if f.startswith(job_id + ".") or f.startswith(job_id + "_") or job_id in f:
                    candidates.append(os.path.join(self.download_dir, f))
            if candidates:
                # prefer mp4 and then largest size
                def keyfn(p):
                    try:
                        size = os.path.getsize(p) if os.path.exists(p) else 0
                    except Exception:
                        size = 0
                    pref = 0 if p.lower().endswith(".mp4") else 1
                    return (pref, -size)
                candidates_sorted = sorted(candidates, key=keyfn)
                return candidates_sorted[0]

            # 2) try yt-dlp prepare_filename if info provided
            try:
                if info and YoutubeDL and hasattr(YoutubeDL, "prepare_filename"):
                    with YoutubeDL({}) as tmp:
                        try:
                            fname = tmp.prepare_filename(info)
                            base = os.path.splitext(os.path.basename(fname))[0]
                            for ext in (".mp4", ".mkv", ".webm", ".mp3", ".m4a"):
                                p = os.path.join(self.download_dir, base + ext)
                                if os.path.exists(p):
                                    return p
                            pfull = os.path.join(self.download_dir, os.path.basename(fname))
                            if os.path.exists(pfull):
                                return pfull
                        except Exception:
                            pass
            except Exception:
                pass

            # 3) glob fallback: any file that contains job_id
            try:
                g = glob.glob(os.path.join(self.download_dir, f"*{job_id}*"))
                if g:
                    g_sorted = sorted(g, key=lambda x: -os.path.getsize(x) if os.path.exists(x) else 0)
                    return g_sorted[0]
            except Exception:
                pass

        except Exception:
            log.exception("_find_final_file error for job %s", job_id)
        return None

    def _cleanup_old_files(self):
        """Keep only MAX_RECENT_DOWNLOADS newest files; remove those older than CLEANUP_SECONDS."""
        try:
            all_files = [f for f in os.listdir(self.download_dir) if os.path.isfile(os.path.join(self.download_dir, f))]
            files_with_mtime = [(f, os.path.getmtime(os.path.join(self.download_dir, f))) for f in all_files]
            files_with_mtime.sort(key=lambda x: x[1], reverse=True)  # newest first

            # remove beyond max recent
            if len(files_with_mtime) > MAX_RECENT_DOWNLOADS:
                for fname, _ in files_with_mtime[MAX_RECENT_DOWNLOADS:]:
                    path = os.path.join(self.download_dir, fname)
                    try:
                        os.remove(path)
                        log.info("Auto-deleted old file (excess): %s", path)
                    except Exception:
                        log.exception("Failed to delete old file: %s", path)

            # remove files older than CLEANUP_SECONDS
            now_ts = time.time()
            for fname, mtime in files_with_mtime:
                try:
                    if now_ts - mtime > CLEANUP_SECONDS:
                        p = os.path.join(self.download_dir, fname)
                        if os.path.exists(p):
                            os.remove(p)
                            log.info("Auto-deleted expired file: %s", p)
                except Exception:
                    log.exception("Failed to delete expired file: %s", fname)

        except Exception:
            log.exception("Cleanup routine failed")

    # ---- worker ----
    def _worker(self, job_id: str, url: str, opts: Dict[str, Any], cancel_ev: threading.Event):
        """
        Worker thread to perform a download via yt-dlp and emit progress via _emit.
        opts: passed kwargs (mode, quality/requested_quality, user_id, is_premium, etc.)
        """
        try:
            self._emit(job_id, {"status": "starting", "progress": {"percent": 0}, "message": "Starting download"})

            # prepare yt-dlp options
            format_sel = "bestvideo+bestaudio/best"
            outtmpl = os.path.join(self.download_dir, f"{job_id}.%(ext)s")
            ydl_opts = {
                "format": format_sel,
                "outtmpl": outtmpl,
                "merge_output_format": "mp4",
                "noplaylist": True,
                "quiet": True,
                "no_warnings": True,
                "progress_hooks": [self._progress_hook(job_id)],
                # postprocessors may sometimes trigger KeyError in some yt-dlp versions when info is unexpected.
                # We'll still request a merger, but be prepared to fallback.
                "postprocessors": [{"key": "FFmpegMerger"}],
            }

            mode = opts.get("mode") or opts.get("requested_mode") or "video"
            if mode == "audio" or opts.get("mode") == "audio":
                ydl_opts["format"] = "bestaudio"
                ydl_opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}]
                ydl_opts["outtmpl"] = outtmpl

            info = None

            # Primary attempt
            try:
                with YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
            except Exception as primary_exc:
                # Defensive fallback: log original and attempt a conservative run (no merger)
                log.exception("yt-dlp primary run failed for job %s; attempting conservative fallback", job_id)
                try:
                    fallback_opts = dict(ydl_opts)
                    # remove potentially-problematic postprocessor and merge flag
                    fallback_opts.pop("postprocessors", None)
                    fallback_opts.pop("merge_output_format", None)
                    # prefer 'best' (single file) to avoid requested_formats postprocessor issues
                    fallback_opts["format"] = "best"
                    fallback_opts["progress_hooks"] = [self._progress_hook(job_id)]
                    with YoutubeDL(fallback_opts) as ydl2:
                        info = ydl2.extract_info(url, download=True)
                except Exception as fallback_exc:
                    # if fallback also fails, raise the original exception to be recorded below
                    log.exception("yt-dlp fallback also failed for job %s", job_id)
                    raise primary_exc

            # After extract_info returns, try to locate final file
            final_file = self._find_final_file(job_id, info)
            if final_file and os.path.exists(final_file):
                webpath = f"/api/file/{os.path.basename(final_file)}"
                self._emit(job_id, {"status": "finished", "progress": {"percent": 100}, "file_path": final_file, "file": webpath, "title": (info.get("title") if isinstance(info, dict) else None), "finished_at": _now_iso()})
                # cleanup
                try:
                    self._cleanup_old_files()
                except Exception:
                    log.exception("cleanup after finish failed")
                return

            # short polling period while postprocessing might still be running
            wait_secs = 15
            poll_interval = 0.8
            steps = max(1, int(wait_secs / poll_interval))
            for i in range(steps):
                if cancel_ev and cancel_ev.is_set():
                    raise KeyboardInterrupt("cancelled by user")
                time.sleep(poll_interval)
                pct = 96 + min(3, int((i / steps) * 3))
                self._emit(job_id, {"status": "processing", "progress": {"percent": int(pct)}, "message": "post-processing (merging / converting)"})
                final_file = self._find_final_file(job_id, info)
                if final_file and os.path.exists(final_file):
                    webpath = f"/api/file/{os.path.basename(final_file)}"
                    self._emit(job_id, {"status": "finished", "progress": {"percent": 100}, "file_path": final_file, "file": webpath, "title": (info.get("title") if isinstance(info, dict) else None), "finished_at": _now_iso()})
                    try:
                        self._cleanup_old_files()
                    except Exception:
                        log.exception("cleanup after finish failed")
                    return

            # last resort: check info-dict hints
            try:
                possible_names: List[str] = []
                if isinstance(info, dict):
                    if info.get("_filename"):
                        possible_names.append(os.path.join(self.download_dir, os.path.basename(info.get("_filename"))))
                    if info.get("requested_downloads"):
                        for rd in info.get("requested_downloads", []):
                            fn = rd.get("path") or rd.get("filename")
                            if fn:
                                possible_names.append(os.path.join(self.download_dir, os.path.basename(fn)))
                    if info.get("filename"):
                        possible_names.append(os.path.join(self.download_dir, os.path.basename(info.get("filename"))))
                for p in possible_names:
                    if p and os.path.exists(p):
                        webpath = f"/api/file/{os.path.basename(p)}"
                        self._emit(job_id, {"status": "finished", "progress": {"percent": 100}, "file_path": p, "file": webpath, "title": (info.get("title") if isinstance(info, dict) else None), "finished_at": _now_iso()})
                        try:
                            self._cleanup_old_files()
                        except Exception:
                            log.exception("cleanup after finish failed")
                        return
            except Exception:
                log.exception("final filename check failed for job %s", job_id)

            # nothing found — emit finished but warn
            self._emit(job_id, {"status": "finished", "progress": {"percent": 100}, "message": "finished but output file not found (check server logs)", "title": (info.get("title") if isinstance(info, dict) else None), "finished_at": _now_iso()})
            try:
                self._cleanup_old_files()
            except Exception:
                log.exception("cleanup after finish failed")

        except KeyboardInterrupt:
            # cancellation requested
            self._emit(job_id, {"status": "cancelled", "message": "cancelled by user"})
        except Exception as exc:
            tb = traceback.format_exc()
            log.exception("Worker exception for job %s", job_id)
            self._emit(job_id, {"status": "error", "error": str(exc), "trace": tb})
        finally:
            # clean up internal structures
            try:
                self._cancel_flags.pop(job_id, None)
                self._threads.pop(job_id, None)
            except Exception:
                pass

    # ---- public API ----
    def start_download_job(self, **kwargs) -> (Optional[str], Optional[str]):
        """
        Start a job asynchronously.

        Accepts typical kwargs:
            url (required), user_id, mode, quality/requested_quality, is_premium, enhance_video, enhance_audio

        Returns (job_id, error). error is None on success.
        """
        if self._is_shutting_down:
            return None, "shutting_down"
        url = kwargs.get("url")
        if not url:
            return None, "missing_url"
        if not YTDLP_AVAILABLE:
            return None, "yt-dlp-not-installed"

        job_id = uuid.uuid4().hex[:12]
        with _LOCK:
            _DOWNLOADS[job_id] = {
                "job_id": job_id,
                "url": url,
                "user_id": kwargs.get("user_id"),
                "status": "queued",
                "progress": {"percent": 0},
                "created_at": _now_iso(),
                "last_update": _now_iso()
            }

        cancel_ev = threading.Event()
        self._cancel_flags[job_id] = cancel_ev
        t = threading.Thread(target=self._worker, args=(job_id, url, kwargs, cancel_ev), daemon=True)
        self._threads[job_id] = t
        t.start()
        return job_id, None

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        with _LOCK:
            return dict(_DOWNLOADS.get(job_id)) if job_id in _DOWNLOADS else None

    def cancel_job(self, job_id: str) -> bool:
        ev = self._cancel_flags.get(job_id)
        if ev:
            ev.set()
            return True
        with _LOCK:
            j = _DOWNLOADS.get(job_id)
            if not j:
                return False
            j["status"] = "cancelled"
            j["last_update"] = _now_iso()
            _DOWNLOADS[job_id] = j
            return True

    def list_history(self, limit: int = 200) -> List[Dict[str, Any]]:
        with _LOCK:
            items = list(_DOWNLOADS.values())[-limit:]
            return [dict(i) for i in items]

    # allow app to request graceful shutdown
    def shutdown(self, wait_seconds: float = 2.0):
        """
        Signal all running jobs to cancel and wait briefly for threads to exit.
        Called by app.py when it wants to stop the manager.
        """
        with self._shutdown_lock:
            self._is_shutting_down = True
            # set all cancel flags
            for ev in list(self._cancel_flags.values()):
                try:
                    ev.set()
                except Exception:
                    pass
            # join threads briefly
            end = time.time() + float(wait_seconds)
            for tid, t in list(self._threads.items()):
                try:
                    remaining = max(0.0, end - time.time())
                    t.join(timeout=remaining)
                except Exception:
                    pass
            log.info("DownloadManager.shutdown complete (wait_seconds=%s)", wait_seconds)

# ---- singleton factory and module-level convenience ----
_MANAGER_SINGLETON: Optional[DownloadManager] = None

def get_default_manager(progress_callback: Optional[callable] = None, download_dir: Optional[str] = None, db_path: Optional[str] = None) -> DownloadManager:
    global _MANAGER_SINGLETON
    if _MANAGER_SINGLETON is None:
        _MANAGER_SINGLETON = DownloadManager(progress_callback=progress_callback, download_dir=download_dir, db_path=db_path)
    else:
        # update callback if provided
        if progress_callback:
            _MANAGER_SINGLETON.progress_callback = progress_callback
    return _MANAGER_SINGLETON

# module-level wrappers that app.py's adapter expects
def start_download_job(**kwargs):
    mgr = get_default_manager()
    return mgr.start_download_job(**kwargs)

def get_job_status(job_id: str):
    mgr = get_default_manager()
    return mgr.get_job_status(job_id)

def cancel_job(job_id: str):
    mgr = get_default_manager()
    return mgr.cancel_job(job_id)

def list_history(limit: int = 200):
    mgr = get_default_manager()
    return mgr.list_history(limit=limit)

# optional module-level shutdown (app.py will attempt to call download_manager.shutdown())
def shutdown():
    global _MANAGER_SINGLETON
    if _MANAGER_SINGLETON:
        try:
            _MANAGER_SINGLETON.shutdown()
        except Exception:
            log.exception("download_manager.shutdown failed")
