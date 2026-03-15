"""
============================================================
🎬 FULLY AUTOMATED MULTI-MOVIE INSTAGRAM REEL UPLOADER
============================================================
Just upload movies to Google Drive folder → Everything else
is 100% automated: detect → download → split → thumbnail
(Gemini AI) → upload → track → next movie → repeat

Author: Auto Reel Bot
============================================================
"""

import os
import sys
import json
import math
import time
import random
import shutil
import requests
import traceback
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta

# Video processing
from moviepy.editor import VideoFileClip

# Image / Thumbnail
from PIL import Image, ImageDraw, ImageFont

# Instagram
from instagrapi import Client
from instagrapi.exceptions import (
    LoginRequired,
    ChallengeRequired,
    FeedbackRequired,
    PleaseWaitFewMinutes,
    ClientThrottledError,
)

# Google Drive download
import gdown

# Gemini AI for thumbnails
GEMINI_AVAILABLE = False
try:
    from google import genai
    from google.genai import types as genai_types
    GEMINI_AVAILABLE = True
except ImportError:
    print("⚠️ google-genai not installed → using video-frame thumbnails")


# ============================================================
#                    CONFIGURATION
# ============================================================
class Config:
    # --- Credentials (from GitHub Secrets) ---
    IG_USERNAME      = os.environ.get("IG_USERNAME", "")
    IG_PASSWORD      = os.environ.get("IG_PASSWORD", "")
    GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "")
    GDRIVE_API_KEY   = os.environ.get("GDRIVE_API_KEY", "")
    GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")

    # --- Video ---
    CLIP_LENGTH = 60            # seconds per reel

    # --- Upload timing ---
    # ~2 hours between uploads with 2-3 min random variation
    MAX_UPLOADS_PER_RUN = 3     # 3 uploads × ~2hr gap ≈ 5hr (within 6hr GitHub limit)
    DELAY_MIN = 7080            # 1 hour 58 minutes (seconds)
    DELAY_MAX = 7380            # 2 hours 3 minutes  (seconds)

    # --- File paths ---
    REELS_DIR      = "reels"
    THUMBS_DIR     = "thumbnails"
    MOVIE_FILE     = "current_movie.mp4"
    SESSION_FILE   = "session.json"
    LOG_FILE       = "movies_log.json"
    PROGRESS_FILE  = "progress.json"
    DETAIL_LOG     = "detailed_log.txt"
    GEMINI_BG      = "gemini_background.png"

    VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".mov", ".webm")

    # --- Caption templates (rotated randomly) ---
    CAPTIONS = [
        "🎬 {name} | Part {p}/{t}\n\n#movie #reels #viral #trending #fyp #cinema",
        "🔥 {name} — Part {p}/{t}\n\nFollow for next part! 🍿\n\n#movie #viral #reels",
        "🎥 {name} [{p}/{t}]\n\n⬇️ Follow for more parts!\n\n#movies #cinema #viral #fyp",
        "🍿 {name} | Part {p} of {t}\n\nLike & Follow for more ❤️\n\n#movie #trending #reels",
        "📽️ {name} • Part {p}/{t}\n\nStay tuned! 🔔\n\n#film #reels #viral #trending #fyp",
    ]

    # --- Gemini models to try (in order) ---
    GEMINI_MODELS = [
    "models/gemini-2.5-flash",
    "models/gemini-flash-latest"
    ]


# ============================================================
#                      LOGGER
# ============================================================
class Logger:
    """Dual logger → console + file with timestamps"""

    def __init__(self, filepath):
        self.filepath = filepath

    def _ts(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _short(self):
        return datetime.now().strftime("%H:%M:%S")

    def _write(self, line):
        try:
            with open(self.filepath, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def info(self, msg):
        print(f"[{self._short()}] ✅ {msg}")
        self._write(f"[{self._ts()}] INFO  | {msg}")

    def warn(self, msg):
        print(f"[{self._short()}] ⚠️  {msg}")
        self._write(f"[{self._ts()}] WARN  | {msg}")

    def error(self, msg):
        print(f"[{self._short()}] ❌ {msg}")
        self._write(f"[{self._ts()}] ERROR | {msg}")

    def upload(self, movie, part, total, status):
        line = f"[{self._ts()}] UPLOAD | {movie} | Part {part}/{total} | {status}"
        print(f"  📤 {movie} Part {part}/{total} → {status}")
        self._write(line)

    def separator(self, char="=", length=60):
        sep = char * length
        print(sep)
        self._write(sep)


log = Logger(Config.DETAIL_LOG)


# ============================================================
#                   HELPER FUNCTIONS
# ============================================================
def load_json(filepath, default=None):
    """Safely load JSON file"""
    if default is None:
        default = {}
    try:
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log.error(f"Failed to load {filepath}: {e}")
    return default


def save_json(filepath, data):
    """Safely save JSON file"""
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except IOError as e:
        log.error(f"Failed to save {filepath}: {e}")


def movie_display_name(filename):
    """Extract clean movie name from filename
    'Inception (2010).mp4' → 'Inception (2010)'
    """
    return Path(filename).stem


def cleanup_temp():
    """Remove temporary files to free disk space"""
    for path in [Config.MOVIE_FILE, Config.GEMINI_BG]:
        if os.path.exists(path):
            os.remove(path)
    for folder in [Config.REELS_DIR, Config.THUMBS_DIR]:
        if os.path.exists(folder):
            shutil.rmtree(folder)
    log.info("🧹 Temporary files cleaned up")


def git_push():
    """Commit and push progress files to GitHub"""
    try:
        os.system('git config user.name "Reel Bot"')
        os.system('git config user.email "bot@reelbot.com"')

        # Stage only tracking files
        for f in [Config.LOG_FILE, Config.PROGRESS_FILE,
                  Config.SESSION_FILE, Config.DETAIL_LOG]:
            if os.path.exists(f):
                os.system(f'git add "{f}"')

        os.system('git diff --staged --quiet || git commit -m "🤖 Auto: progress update"')
        os.system('git push')
        log.info("📁 Progress pushed to GitHub")
    except Exception as e:
        log.error(f"Git push failed: {e}")


# ============================================================
#              SETUP VERIFICATION
# ============================================================
def verify_setup():
    """Check all required secrets / credentials are set"""
    critical_missing = []
    warnings = []

    if not Config.IG_USERNAME:
        critical_missing.append("IG_USERNAME")
    if not Config.IG_PASSWORD:
        critical_missing.append("IG_PASSWORD")
    if not Config.GDRIVE_FOLDER_ID:
        critical_missing.append("GDRIVE_FOLDER_ID")
    if not Config.GDRIVE_API_KEY:
        critical_missing.append("GDRIVE_API_KEY")
    if not Config.GEMINI_API_KEY:
        warnings.append("GEMINI_API_KEY not set → using video-frame thumbnails")

    for w in warnings:
        log.warn(w)

    if critical_missing:
        for m in critical_missing:
            log.error(f"Missing required secret: {m}")
        return False

    log.info("✅ All required credentials verified")
    return True


# ============================================================
#                 GOOGLE DRIVE MANAGER
# ============================================================
def list_drive_movies():
    """
    List all video files in the Google Drive folder.
    Uses Drive API v3 with API key (folder must be shared publicly).
    Handles pagination for large folders.
    """
    folder_id = Config.GDRIVE_FOLDER_ID
    api_key = Config.GDRIVE_API_KEY
    url = "https://www.googleapis.com/drive/v3/files"
    all_files = []
    page_token = None

    log.info(f"📂 Scanning Google Drive folder: {folder_id}")

    while True:
        params = {
            "q": f"'{folder_id}' in parents and trashed=false",
            "key": api_key,
            "fields": "nextPageToken,files(id,name,size,mimeType,createdTime)",
            "pageSize": 100,
            "orderBy": "name",
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            r = requests.get(url, params=params, timeout=30)

            if r.status_code == 403:
                log.error("Drive API: Access denied. Check API key and folder sharing.")
                return []
            if r.status_code == 404:
                log.error("Drive API: Folder not found. Check GDRIVE_FOLDER_ID.")
                return []
            if r.status_code != 200:
                log.error(f"Drive API error {r.status_code}: {r.text[:200]}")
                return []

            data = r.json()

            for f in data.get("files", []):
                if any(f["name"].lower().endswith(ext) for ext in Config.VIDEO_EXTS):
                    all_files.append({
                        "id": f["id"],
                        "name": f["name"],
                        "size": int(f.get("size", 0)),
                        "created": f.get("createdTime", ""),
                    })

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        except requests.exceptions.RequestException as e:
            log.error(f"Drive API request failed: {e}")
            return []

    log.info(f"📂 Found {len(all_files)} video files in Drive")
    return all_files


def download_movie(file_id, output_path):
    """Download a movie from Google Drive using gdown"""
    try:
        if os.path.exists(output_path):
            os.remove(output_path)

        log.info("📥 Downloading movie from Google Drive...")
        download_url = f"https://drive.google.com/uc?id={file_id}"
        gdown.download(download_url, output_path, quiet=False)

        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            log.info(f"📥 Download complete! Size: {size_mb:.1f} MB")
            return True
        else:
            log.error("Download produced empty or missing file")
            return False

    except Exception as e:
        log.error(f"Download failed: {e}")
        log.error(traceback.format_exc())
        return False


# ============================================================
#                  VIDEO PROCESSOR
# ============================================================
def get_video_info(video_path):
    """Get video duration and calculate total parts"""
    try:
        video = VideoFileClip(video_path)
        duration = video.duration
        # Skip very short final clips (< 5 seconds)
        total_parts = 0
        for start in range(0, int(duration), Config.CLIP_LENGTH):
            end = min(start + Config.CLIP_LENGTH, duration)
            if end - start >= 5:
                total_parts += 1
        video.close()
        return duration, total_parts
    except Exception as e:
        log.error(f"Failed to read video: {e}")
        return 0, 0


def extract_clip(video_path, part_num, output_path):
    """Extract a single clip from the movie"""
    video = None
    clip = None
    try:
        video = VideoFileClip(video_path)
        start = (part_num - 1) * Config.CLIP_LENGTH
        end = min(start + Config.CLIP_LENGTH, video.duration)

        if end - start < 5:
            log.warn(f"Part {part_num}: too short ({end-start:.1f}s), skipping")
            return False

        clip = video.subclip(start, end)
        clip.write_videofile(
            output_path,
            codec="libx264",
            audio_codec="aac",
            threads=2,
            logger=None,
        )
        log.info(f"✂️ Part {part_num} extracted ({start}s → {end:.0f}s)")
        return True

    except Exception as e:
        log.error(f"Failed to extract part {part_num}: {e}")
        return False
    finally:
        if clip:
            try: clip.close()
            except: pass
        if video:
            try: video.close()
            except: pass


def extract_frame(video_path, time_sec):
    """Extract a single frame from video as PIL Image"""
    video = None
    try:
        video = VideoFileClip(video_path)
        time_sec = min(time_sec, video.duration - 0.5)
        time_sec = max(0, time_sec)
        frame = video.get_frame(time_sec)
        image = Image.fromarray(frame)
        return image
    except Exception as e:
        log.error(f"Frame extraction failed: {e}")
        # Return a plain dark background as ultimate fallback
        return Image.new("RGB", (1920, 1080), (20, 20, 40))
    finally:
        if video:
            try: video.close()
            except: pass

def extract_frames_for_grid(video_path, frame_count=9):
    video = VideoFileClip(video_path)
    duration = video.duration

    frames = []

    for i in range(frame_count):
        t = duration * (0.2 + i * 0.06)
        frame = video.get_frame(t)
        img = Image.fromarray(frame)
        frames.append(img)

    video.close()
    return frames


def create_frame_grid(frames):
    size = 320

    resized = [f.resize((size, size)) for f in frames]

    grid = Image.new("RGB", (size * 3, size * 3))

    index = 0
    for y in range(3):
        for x in range(3):
            grid.paste(resized[index], (x * size, y * size))
            index += 1

    return grid


def choose_best_frame_with_gemini(grid_image, frames):

    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        log.warn("Gemini not available → using middle frame")
        return frames[4]

    client = genai.Client(api_key=Config.GEMINI_API_KEY)

    prompt = """
You are selecting the best movie thumbnail.

The image shows a 3x3 grid of movie frames.

Choose the frame that:
- looks interesting
- has characters or action
- is bright and clear
- would attract viewers

Numbering:

1 2 3
4 5 6
7 8 9

Reply ONLY with the number.
"""

    buf = BytesIO()
    grid_image.save(buf, format="JPEG")

    response = client.models.generate_content(
        model="models/gemini-2.5-flash",
        contents=[
            prompt,
            {
                "mime_type": "image/jpeg",
                "data": buf.getvalue()
            }
        ]
    )

    try:
        index = int(response.text.strip())
        return frames[index - 1]
    except:
        log.warn("Gemini returned invalid result → using middle frame")
        return frames[4]


def get_font(size):
    """Load font with fallback"""
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                continue
    return ImageFont.load_default()


def create_thumbnail(bg_image, movie_name, part_num, total_parts,
                     movie_num, total_movies, output_path):
    """
    Create a professional thumbnail with:
    - Background (Gemini or video frame)
    - Dark gradient overlay
    - Movie name (top)
    - Part number (bottom, gold)
    - Movie counter (bottom)
    """
    try:
        # Resize to Instagram Reel cover: 1080×1920 (9:16)
        thumb = bg_image.copy()
        thumb = thumb.resize((1080, 1920), Image.LANCZOS)
        thumb = thumb.convert("RGBA")

        # --- Dark gradient overlay (top + bottom) ---
        overlay = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
        odraw = ImageDraw.Draw(overlay)

        # Top gradient
        for y in range(500):
            alpha = int(220 * (1 - y / 500))
            odraw.rectangle([(0, y), (1080, y + 1)], fill=(0, 0, 0, alpha))

        # Bottom gradient
        for y in range(1420, 1920):
            alpha = int(220 * ((y - 1420) / 500))
            odraw.rectangle([(0, y), (1080, y + 1)], fill=(0, 0, 0, alpha))

        thumb = Image.alpha_composite(thumb, overlay)
        thumb = thumb.convert("RGB")
        draw = ImageDraw.Draw(thumb)

        # --- Fonts ---
        font_title = get_font(68)
        font_part = get_font(56)
        font_info = get_font(36)

        # --- Movie name (top center) ---
        title = movie_name.upper()
        # Word wrap if title is too long
        max_chars = 18
        if len(title) > max_chars:
            words = title.split()
            lines = []
            line = ""
            for w in words:
                if len(line + " " + w) > max_chars and line:
                    lines.append(line.strip())
                    line = w
                else:
                    line = (line + " " + w).strip()
            if line:
                lines.append(line.strip())
        else:
            lines = [title]

        y_start = 100
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=font_title)
            tw = bbox[2] - bbox[0]
            x = (1080 - tw) // 2
            # Black outline
            for dx in range(-3, 4):
                for dy in range(-3, 4):
                    draw.text((x + dx, y_start + dy), line,
                              font=font_title, fill="black")
            draw.text((x, y_start), line, font=font_title, fill="white")
            y_start += bbox[3] - bbox[1] + 15

        # --- Part number (bottom, gold) ---
        part_text = f"PART {part_num} / {total_parts}"
        bbox = draw.textbbox((0, 0), part_text, font=font_part)
        tw = bbox[2] - bbox[0]
        x = (1080 - tw) // 2
        y = 1740
        for dx in range(-2, 3):
            for dy in range(-2, 3):
                draw.text((x + dx, y + dy), part_text,
                          font=font_part, fill="black")
        draw.text((x, y), part_text, font=font_part, fill=(255, 215, 0))

        # --- Movie counter (below part number) ---
        counter_text = f"Movie {movie_num} of {total_movies}"
        bbox = draw.textbbox((0, 0), counter_text, font=font_info)
        tw = bbox[2] - bbox[0]
        x = (1080 - tw) // 2
        draw.text((x, 1810), counter_text, font=font_info, fill=(180, 180, 180))

        # --- Decorative line ---
        draw.rectangle([(200, 1720), (880, 1723)], fill=(255, 215, 0, 180))

        thumb.save(output_path, "JPEG", quality=95)
        log.info(f"🖼️ Thumbnail created: {output_path}")
        return True

    except Exception as e:
        log.error(f"Thumbnail creation failed: {e}")
        log.error(traceback.format_exc())
        # Ultimate fallback: plain dark image with text
        try:
            fb = Image.new("RGB", (1080, 1920), (20, 20, 40))
            d = ImageDraw.Draw(fb)
            fnt = get_font(60)
            d.text((100, 800), movie_name, font=fnt, fill="white")
            d.text((100, 900), f"Part {part_num}/{total_parts}",
                   font=fnt, fill=(255, 215, 0))
            fb.save(output_path, "JPEG")
            return True
        except Exception:
            return False


# ============================================================
#                 INSTAGRAM MANAGER
# ============================================================
def instagram_login():
    """Login to Instagram with session reuse and retry"""
    for attempt in range(1, 4):
        try:
            cl = Client()
            cl.delay_range = [2, 5]

            if os.path.exists(Config.SESSION_FILE):
                log.info(f"🔐 Login attempt {attempt} (using saved session)...")
                cl.load_settings(Config.SESSION_FILE)
                cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
                # Verify session works
                cl.get_timeline_feed()
                log.info("🔐 Logged in via saved session")
                return cl

            raise LoginRequired("No session file")

        except (LoginRequired, ChallengeRequired):
            try:
                log.info("🔐 Fresh login...")
                cl = Client()
                cl.delay_range = [2, 5]
                cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
                cl.dump_settings(Config.SESSION_FILE)
                log.info("🔐 Fresh login successful!")
                return cl
            except Exception as e:
                log.error(f"Fresh login failed (attempt {attempt}): {e}")
                if attempt < 3:
                    wait = 120 * attempt
                    log.info(f"⏳ Waiting {wait}s before retry...")
                    time.sleep(wait)

        except Exception as e:
            log.error(f"Login error (attempt {attempt}): {e}")
            # Delete corrupted session
            if os.path.exists(Config.SESSION_FILE):
                os.remove(Config.SESSION_FILE)
            if attempt < 3:
                time.sleep(120 * attempt)

    log.error("🔐 All login attempts failed!")
    return None


def upload_reel(cl, video_path, thumbnail_path, caption):
    """
    Upload a single reel with comprehensive error handling.
    Returns: True (success), False (failed), or "STOP" (fatal error)
    """
    for retry in range(1, 4):
        try:
            log.info(f"  📤 Upload attempt {retry}/3...")

            kwargs = {
                "path": video_path,
                "caption": caption,
            }

            # Add thumbnail if it exists
            if thumbnail_path and os.path.exists(thumbnail_path):
                kwargs["thumbnail"] = Path(thumbnail_path)

            cl.clip_upload(**kwargs)
            return True

        except PleaseWaitFewMinutes:
            wait = 600 * retry  # 10, 20, 30 min
            log.warn(f"Rate limited → waiting {wait // 60} minutes...")
            time.sleep(wait)

        except ClientThrottledError:
            wait = 900 * retry  # 15, 30, 45 min
            log.warn(f"Throttled → waiting {wait // 60} minutes...")
            time.sleep(wait)

        except FeedbackRequired as e:
            log.error(f"Instagram feedback required: {e}")
            log.error("⛔ Account may be flagged. Stopping all uploads.")
            return "STOP"

        except ChallengeRequired:
            log.error("⛔ Instagram challenge required. Manual action needed.")
            return "STOP"

        except LoginRequired:
            log.warn("Session expired during upload → re-logging...")
            try:
                cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
                cl.dump_settings(Config.SESSION_FILE)
                log.info("Re-login successful")
            except Exception:
                log.error("Re-login failed")
                return "STOP"

        except ConnectionError:
            wait = 180 * retry
            log.warn(f"Connection error → waiting {wait // 60} minutes...")
            time.sleep(wait)

        except Exception as e:
            log.error(f"Upload error (attempt {retry}): {e}")
            log.error(traceback.format_exc())
            if retry < 3:
                wait = 300 * retry
                log.info(f"⏳ Waiting {wait // 60}m before retry...")
                time.sleep(wait)

    return False


# ============================================================
#                   SMART DELAY
# ============================================================
def smart_delay(upload_number):
    """
    Wait ~2 hours with 2-3 minute random variation.
    Each upload gets a slightly different delay so the pattern
    looks natural to Instagram.
    Logs remaining time every 10 minutes.
    """
    base = random.randint(Config.DELAY_MIN, Config.DELAY_MAX)
    jitter = random.randint(-45, 45)
    total = max(60, base + jitter)  # minimum 1 minute

    mins = total // 60
    secs = total % 60
    next_time = datetime.now() + timedelta(seconds=total)

    log.info(f"⏳ Delay: {mins}m {secs}s  (next upload ≈ {next_time.strftime('%H:%M:%S')})")

    elapsed = 0
    while elapsed < total:
        chunk = min(60, total - elapsed)
        time.sleep(chunk)
        elapsed += chunk
        remaining = total - elapsed
        # Log every 10 minutes
        if remaining > 0 and elapsed % 600 < 60:
            log.info(f"  ⏳ {remaining // 60}m {remaining % 60}s remaining...")


# ============================================================
#                  MOVIE TRACKER
# ============================================================
def load_movies_log():
    default = {
        "movies": {},
        "current_movie": "",
        "total_movies_found": 0,
        "total_completed": 0,
        "total_reels_uploaded": 0,
        "last_run": "",
    }
    data = load_json(Config.LOG_FILE, default)
    # Ensure all keys exist
    for k, v in default.items():
        if k not in data:
            data[k] = v
    return data


def save_movies_log(data):
    data["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["total_completed"] = sum(
        1 for m in data["movies"].values() if m["status"] == "completed"
    )
    data["total_movies_found"] = len(data["movies"])
    data["total_reels_uploaded"] = sum(
        m.get("uploaded_parts", 0) for m in data["movies"].values()
    )
    save_json(Config.LOG_FILE, data)


def sync_with_drive(movies_log, drive_files):
    """
    Sync movies_log with what's actually in Drive.
    - New movies in Drive → added as 'pending'
    - Completed movies → never touched again
    - Deleted movies from Drive → kept in log (historical)
    """
    added = 0
    for f in drive_files:
        name = f["name"]
        if name not in movies_log["movies"]:
            movies_log["movies"][name] = {
                "drive_id": f["id"],
                "status": "pending",
                "total_parts": 0,
                "uploaded_parts": 0,
                "size_mb": round(f["size"] / (1024 * 1024), 1),
                "started_at": "",
                "completed_at": "",
                "last_uploaded_at": "",
                "errors": 0,
            }
            log.info(f"🆕 New movie detected: {name}")
            added += 1

    if added:
        log.info(f"🆕 {added} new movie(s) added to tracking")
    return movies_log


def get_next_movie(movies_log):
    """
    Pick the next movie to work on:
    1. First: any 'in_progress' movie (resume)
    2. Then: first 'pending' movie
    3. None if all done
    """
    # Resume in-progress first
    for name, info in movies_log["movies"].items():
        if info["status"] == "in_progress":
            log.info(f"▶️ Resuming: {name}")
            return name, info

    # Then pick next pending
    for name, info in movies_log["movies"].items():
        if info["status"] == "pending":
            log.info(f"🆕 Starting new: {name}")
            return name, info

    return None, None


def load_progress():
    return load_json(Config.PROGRESS_FILE, {
        "movie_name": "",
        "last_uploaded": 0,
        "total_parts": 0,
    })


def save_progress(data):
    save_json(Config.PROGRESS_FILE, data)


# ============================================================
#                    SUMMARY REPORT
# ============================================================
def print_summary(movies_log):
    """Print a nice summary of all movies and their status"""
    log.separator("=")
    print("📊 MOVIES STATUS REPORT")
    log.separator("-")

    status_emoji = {
        "pending": "⏳",
        "in_progress": "🔄",
        "completed": "✅",
        "error": "❌",
    }

    movie_num = 0
    for name, info in movies_log["movies"].items():
        movie_num += 1
        emoji = status_emoji.get(info["status"], "❓")
        display = movie_display_name(name)
        parts = f"{info.get('uploaded_parts', 0)}/{info.get('total_parts', '?')}"
        size = f"{info.get('size_mb', '?')} MB"

        print(f"  {emoji} #{movie_num} {display}")
        print(f"      Status: {info['status']} | Parts: {parts} | Size: {size}")

        if info.get("started_at"):
            print(f"      Started: {info['started_at']}")
        if info.get("completed_at"):
            print(f"      Completed: {info['completed_at']}")
        if info.get("errors", 0) > 0:
            print(f"      Errors: {info['errors']}")
        print()

    log.separator("-")
    total = len(movies_log["movies"])
    done = movies_log.get("total_completed", 0)
    reels = movies_log.get("total_reels_uploaded", 0)
    print(f"  📈 Movies: {done}/{total} completed")
    print(f"  📤 Total reels uploaded: {reels}")
    print(f"  🕐 Last run: {movies_log.get('last_run', 'N/A')}")
    log.separator("=")


# ============================================================
#                      MAIN
# ============================================================
def main():
    log.separator("=")
    print("🎬 FULLY AUTOMATED INSTAGRAM REEL UPLOADER")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.separator("=")

    # ---------- 1. Verify setup ----------
    if not verify_setup():
        log.error("Setup verification failed. Add missing secrets!")
        return

    # ---------- 2. Scan Google Drive ----------
    drive_files = list_drive_movies()
    if not drive_files:
        log.error("No video files found in Google Drive folder!")
        log.info("Make sure the folder is shared and contains .mp4 files")
        return

    # ---------- 3. Load & sync movie tracker ----------
    movies_log = load_movies_log()
    movies_log = sync_with_drive(movies_log, drive_files)
    save_movies_log(movies_log)

    # ---------- 4. Get next movie to process ----------
    movie_name, movie_info = get_next_movie(movies_log)

    if not movie_name:
        log.info("🎉 ALL MOVIES HAVE BEEN UPLOADED! Nothing to do.")
        print_summary(movies_log)
        return

    display_name = movie_display_name(movie_name)
    log.info(f"🎬 Current movie: {display_name}")
    log.info(f"📌 Status: {movie_info['status']}")

    # ---------- 5. Download movie ----------
    if not download_movie(movie_info["drive_id"], Config.MOVIE_FILE):
        log.error(f"Failed to download '{display_name}'. Will retry next run.")
        movie_info["errors"] = movie_info.get("errors", 0) + 1
        save_movies_log(movies_log)
        git_push()
        return

    # ---------- 6. Get video info ----------
    duration, total_parts = get_video_info(Config.MOVIE_FILE)

    if total_parts == 0:
        log.error(f"Could not read video '{display_name}'. Marking as error.")
        movie_info["status"] = "error"
        save_movies_log(movies_log)
        git_push()
        return

    movie_info["total_parts"] = total_parts
    mins = int(duration) // 60
    secs = int(duration) % 60
    log.info(f"📏 Duration: {mins}m {secs}s → {total_parts} parts")

    # ---------- 7. Update status to in_progress ----------
    if movie_info["status"] == "pending":
        movie_info["status"] = "in_progress"
        movie_info["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    movies_log["current_movie"] = movie_name
    save_movies_log(movies_log)

    # ---------- 8. Load upload progress ----------
    progress = load_progress()
    if progress.get("movie_name") != movie_name:
        # New movie → reset progress
        progress = {
            "movie_name": movie_name,
            "last_uploaded": 0,
            "total_parts": total_parts,
        }
    last_uploaded = progress["last_uploaded"]

    log.info(f"📊 Upload progress: {last_uploaded}/{total_parts}")
# ---------- 9. Select best thumbnail frame using Gemini ----------
gemini_bg = None

if last_uploaded == 0 or not os.path.exists(Config.GEMINI_BG):

    log.info("🎬 Extracting frames for thumbnail selection")

    frames = extract_frames_for_grid(Config.MOVIE_FILE)

    grid = create_frame_grid(frames)

    # Save debug image (optional)
    try:
        grid.save("thumbnail_candidates.jpg")
    except Exception:
        pass

    try:
        log.info("🤖 Asking Gemini to choose best frame")
        best_frame = choose_best_frame_with_gemini(grid, frames)
    except Exception as e:
        log.warn(f"Gemini selection failed: {e}")
        best_frame = frames[4]

    best_frame.save(Config.GEMINI_BG)
    gemini_bg = best_frame

else:
    try:
        gemini_bg = Image.open(Config.GEMINI_BG)
        log.info("🎨 Loaded cached thumbnail background")
    except:
        gemini_bg = None

    else:
        try:
            gemini_bg = Image.open(Config.GEMINI_BG)
            log.info("🎨 Loaded cached thumbnail background")
        except:
            gemini_bg = None

    # ---------- 10. Login to Instagram ----------
    cl = instagram_login()
    if cl is None:
        log.error("Cannot login to Instagram. Stopping.")
        save_progress(progress)
        save_movies_log(movies_log)
        git_push()
        return

    # ---------- 11. Create working directories ----------
    os.makedirs(Config.REELS_DIR, exist_ok=True)
    os.makedirs(Config.THUMBS_DIR, exist_ok=True)

    # Calculate movie number for thumbnail
    movie_names = list(movies_log["movies"].keys())
    movie_num = movie_names.index(movie_name) + 1
    total_movies = len(movie_names)

    # ---------- 12. UPLOAD LOOP ----------
    uploaded_this_run = 0
    stop_uploading = False

    for part_num in range(last_uploaded + 1, total_parts + 1):

        if stop_uploading:
            break

        if uploaded_this_run >= Config.MAX_UPLOADS_PER_RUN:
            log.info(f"🛑 Run limit reached ({Config.MAX_UPLOADS_PER_RUN} uploads)")
            break

        log.separator("-")
        log.info(f"📦 Processing Part {part_num}/{total_parts} of '{display_name}'")

        # ---- Extract clip ----
        clip_path = os.path.join(Config.REELS_DIR, f"part_{part_num}.mp4")
        success = extract_clip(Config.MOVIE_FILE, part_num, clip_path)

        if not success:
            log.warn(f"Skipping part {part_num}")
            progress["last_uploaded"] = part_num
            save_progress(progress)
            continue

        # ---- Create thumbnail ----
        thumb_path = os.path.join(Config.THUMBS_DIR, f"thumb_{part_num}.jpg")

        if gemini_bg:
            bg_image = gemini_bg.copy()
        else:
            # Fallback: extract frame from middle of this clip
            mid_time = ((part_num - 1) * Config.CLIP_LENGTH) + (Config.CLIP_LENGTH // 2)
            mid_time = min(mid_time, duration - 1)
            bg_image = extract_frame(Config.MOVIE_FILE, mid_time)

        create_thumbnail(
            bg_image, display_name, part_num, total_parts,
            movie_num, total_movies, thumb_path
        )

        # ---- Build caption ----
        caption = random.choice(Config.CAPTIONS).format(
            name=display_name, p=part_num, t=total_parts
        )

        # ---- Upload ----
        log.info(f"📤 Uploading Part {part_num}/{total_parts}...")
        result = upload_reel(cl, clip_path, thumb_path, caption)

        if result == "STOP":
            log.error("⛔ Fatal Instagram error → stopping all uploads")
            log.upload(display_name, part_num, total_parts, "FATAL_STOP")
            stop_uploading = True

        elif result is True:
            log.info(f"✅ Part {part_num}/{total_parts} uploaded successfully!")
            log.upload(display_name, part_num, total_parts, "SUCCESS")

            uploaded_this_run += 1
            progress["last_uploaded"] = part_num
            movie_info["uploaded_parts"] = part_num
            movie_info["last_uploaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Save progress immediately after each successful upload
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()

        else:
            log.error(f"❌ Part {part_num} failed after all retries")
            log.upload(display_name, part_num, total_parts, "FAILED")
            movie_info["errors"] = movie_info.get("errors", 0) + 1

            # Don't skip failed parts → retry next run
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()

            log.info("⏳ Waiting 10 minutes before next attempt...")
            time.sleep(600)
            continue

        # ---- Cleanup clip & thumbnail to save disk ----
        for f in [clip_path, thumb_path]:
            if os.path.exists(f):
                try: os.remove(f)
                except: pass

        # ---- Smart delay before next upload ----
        if (uploaded_this_run < Config.MAX_UPLOADS_PER_RUN
                and part_num < total_parts
                and not stop_uploading):
            smart_delay(uploaded_this_run)

    # ---------- 13. Check if movie is complete ----------
    if progress["last_uploaded"] >= total_parts:
        log.separator("*")
        log.info(f"🎉🎉🎉 Movie '{display_name}' FULLY UPLOADED! 🎉🎉🎉")
        log.separator("*")

        movie_info["status"] = "completed"
        movie_info["uploaded_parts"] = total_parts
        movie_info["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        movies_log["current_movie"] = ""

        # Reset progress for next movie
        progress = {"movie_name": "", "last_uploaded": 0, "total_parts": 0}

        # Clean up
        cleanup_temp()

    # ---------- 14. Final save ----------
    save_progress(progress)
    save_movies_log(movies_log)
    git_push()

    # ---------- 15. Print summary ----------
    print_summary(movies_log)

    log.separator("=")
    log.info(f"✅ Run complete! Uploaded {uploaded_this_run} reels this run.")
    log.separator("=")


# ============================================================
#                      ENTRY POINT
# ============================================================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warn("Interrupted by user")
        git_push()
    except Exception as e:
        log.error(f"💥 CRITICAL UNHANDLED ERROR: {e}")
        log.error(traceback.format_exc())
        git_push()
        sys.exit(1)
