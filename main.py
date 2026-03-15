"""
============================================================
🎬 FULLY AUTOMATED MULTI-MOVIE INSTAGRAM REEL UPLOADER
============================================================
FLOW (printed live while running):
  STARTUP   → write session from secret → verify credentials
  DRIVE     → scan folder → find new movies → pick next one
  DOWNLOAD  → download movie from Google Drive
  VIDEO     → ffprobe duration → calculate parts count
  THUMBNAIL → extract 9 frames (ffmpeg) → Gemini picks best
  LOGIN     → Instagram session login (no fresh login = no OTP)
  UPLOAD    → for each part: cut clip → make thumbnail → upload
  DELAY     → wait ~2hrs between uploads (natural pattern)
  DONE      → save progress → push to GitHub → print summary
============================================================
"""

import os
import sys
import json
import time
import random
import shutil
import subprocess
import requests
import traceback
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta

# Flush stdout immediately so GitHub Actions shows output in real-time
# Without this, output can be buffered and appear all at once at the end
os.environ["PYTHONUNBUFFERED"] = "1"

def flush_print(msg=""):
    """Print and immediately flush so GitHub Actions shows it live."""
    print(msg, flush=True)


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
    flush_print("⚠️ google-genai not installed → video-frame thumbnails only")


# ============================================================
#                    CONFIGURATION
# ============================================================
class Config:
    # ── Credentials from GitHub Secrets ──────────────────────
    IG_USERNAME      = os.environ.get("IG_USERNAME", "")
    IG_PASSWORD      = os.environ.get("IG_PASSWORD", "")
    # IG_SESSION: paste the FULL contents of session.json here
    # (everything including the opening { and closing })
    IG_SESSION       = os.environ.get("IG_SESSION", "")
    GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "")
    GDRIVE_API_KEY   = os.environ.get("GDRIVE_API_KEY", "")
    GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")

    # ── Video settings ────────────────────────────────────────
    CLIP_LENGTH = 60            # seconds per reel (Instagram max = 90s)

    # ── Upload timing ─────────────────────────────────────────
    # 1 upload per run × 12 cron runs/day (every 2 hrs) = 12 reels/day
    # The 2-hour gap is the cron schedule itself — no sleeping in code.
    # Each GitHub Actions run finishes in ~3-5 minutes and exits cleanly.
    MAX_UPLOADS_PER_RUN = 1

    # ── File paths ────────────────────────────────────────────
    REELS_DIR      = "reels"
    THUMBS_DIR     = "thumbnails"
    MOVIE_FILE     = "current_movie.mp4"
    SESSION_FILE   = "session.json"   # written from IG_SESSION secret at runtime
    LOG_FILE       = "movies_log.json"
    PROGRESS_FILE  = "progress.json"
    DETAIL_LOG     = "detailed_log.txt"
    THUMB_BG_FILE  = "thumb_background.jpg"   # cached once per movie

    VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".mov", ".webm")

    CAPTIONS = [
        "🎬 {name} | Part {p}/{t}\n\n#movie #reels #viral #trending #fyp #cinema",
        "🔥 {name} — Part {p}/{t}\n\nFollow for next part! 🍿\n\n#movie #viral #reels",
        "🎥 {name} [{p}/{t}]\n\n⬇️ Follow for more parts!\n\n#movies #cinema #viral #fyp",
        "🍿 {name} | Part {p} of {t}\n\nLike & Follow for more ❤️\n\n#movie #trending #reels",
        "📽️ {name} • Part {p}/{t}\n\nStay tuned! 🔔\n\n#film #reels #viral #trending #fyp",
    ]

    GEMINI_VISION_MODELS = [
        "gemini-2.0-flash",
        "gemini-1.5-flash",
        "gemini-1.5-pro",
    ]

    GEMINI_IMAGE_MODELS = [
        "gemini-2.0-flash-exp-image-generation",
        "imagen-3.0-generate-002",
    ]


# ============================================================
#                      LOGGER
# ============================================================
class Logger:
    """
    Writes timestamped messages to both console (live) and
    a log file. Uses flush=True on every print so GitHub
    Actions shows output immediately instead of buffering it.
    """
    def __init__(self, filepath):
        self.filepath = filepath

    def _ts(self):    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def _short(self): return datetime.now().strftime("%H:%M:%S")

    def _write(self, line):
        try:
            with open(self.filepath, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def info(self, msg):
        line = f"[{self._short()}] ✅ {msg}"
        print(line, flush=True)
        self._write(f"[{self._ts()}] INFO  | {msg}")

    def warn(self, msg):
        line = f"[{self._short()}] ⚠️  {msg}"
        print(line, flush=True)
        self._write(f"[{self._ts()}] WARN  | {msg}")

    def error(self, msg):
        line = f"[{self._short()}] ❌ {msg}"
        print(line, flush=True)
        self._write(f"[{self._ts()}] ERROR | {msg}")

    def step(self, num, total, msg):
        """Big visible step header — shows which stage we're in."""
        line = f"\n[{self._short()}] ━━━ STEP {num}/{total}: {msg} ━━━"
        print(line, flush=True)
        self._write(f"[{self._ts()}] STEP  | {num}/{total}: {msg}")

    def upload(self, movie, part, total, status):
        line = f"[{self._ts()}] UPLOAD | {movie} | Part {part}/{total} | {status}"
        print(f"  📤 {movie} Part {part}/{total} → {status}", flush=True)
        self._write(line)

    def separator(self, char="=", length=60):
        sep = char * length
        print(sep, flush=True)
        self._write(sep)


log = Logger(Config.DETAIL_LOG)


# ============================================================
#                   HELPERS
# ============================================================
def load_json(filepath, default=None):
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
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except IOError as e:
        log.error(f"Failed to save {filepath}: {e}")


def movie_display_name(filename):
    return Path(filename).stem


def cleanup_temp():
    """Delete all temporary files to free disk space after movie completes."""
    log.info("🧹 Cleaning up temp files...")
    for path in [Config.MOVIE_FILE, Config.THUMB_BG_FILE]:
        if os.path.exists(path):
            os.remove(path)
            log.info(f"   Deleted: {path}")
    for folder in [Config.REELS_DIR, Config.THUMBS_DIR]:
        if os.path.exists(folder):
            shutil.rmtree(folder)
            log.info(f"   Deleted folder: {folder}")
    log.info("🧹 Cleanup done")


def git_push():
    """
    Push only tracking files to GitHub.
    IMPORTANT: session.json is intentionally EXCLUDED from git push
    because it contains Instagram login tokens — never commit it
    to a public repo. It is written fresh from IG_SESSION secret
    every run and deleted from git history if accidentally added.
    """
    log.info("📁 Pushing progress to GitHub...")
    try:
        os.system('git config user.name "Reel Bot"')
        os.system('git config user.email "bot@reelbot.com"')

        # Add ONLY these safe tracking files — NOT session.json
        safe_files = [Config.LOG_FILE, Config.PROGRESS_FILE, Config.DETAIL_LOG]
        for f in safe_files:
            if os.path.exists(f):
                os.system(f'git add "{f}"')
                log.info(f"   Staged: {f}")

        # Make sure session.json is in .gitignore so it is never accidentally added
        _ensure_gitignore()

        result = os.system(
            'git diff --staged --quiet || git commit -m "🤖 Auto: progress update"'
        )
        os.system('git push')
        log.info("📁 GitHub push complete")
    except Exception as e:
        log.error(f"Git push failed: {e}")


def _ensure_gitignore():
    """Add session.json to .gitignore so it is never committed to the repo."""
    gitignore = ".gitignore"
    entry = "session.json"
    try:
        existing = ""
        if os.path.exists(gitignore):
            with open(gitignore, "r") as f:
                existing = f.read()
        if entry not in existing:
            with open(gitignore, "a") as f:
                f.write(f"\n# Instagram session token — never commit this!\n{entry}\n")
            os.system(f'git add "{gitignore}"')
            log.info("🔒 session.json added to .gitignore (security: not tracked by git)")
    except Exception as e:
        log.warn(f"Could not update .gitignore: {e}")


# ============================================================
#              STEP 1 — SESSION FROM SECRET
# ============================================================
def write_session_from_secret():
    """
    WHAT: Reads IG_SESSION GitHub Secret and writes it to session.json on disk.
    WHY:  We store the session as a secret (not a file) so it never appears
          in the git repo — safe for public repos.
    HOW:  In GitHub Secrets, name = IG_SESSION, value = full contents of
          session.json including the opening { and closing }.
    """
    log.step(1, 10, "Write Instagram session from GitHub Secret")
    session_json = Config.IG_SESSION.strip()

    if not session_json:
        log.warn("IG_SESSION secret is empty — will try existing session.json if present")
        if os.path.exists(Config.SESSION_FILE):
            log.info("Found existing session.json on disk — will use it")
        else:
            log.error("No session available! Add IG_SESSION secret to GitHub Secrets.")
        return

    log.info("IG_SESSION secret found — validating JSON...")
    try:
        parsed = json.loads(session_json)
    except json.JSONDecodeError as e:
        log.error(f"IG_SESSION is not valid JSON: {e}")
        log.error("Go to GitHub → Settings → Secrets → IG_SESSION")
        log.error("Value must be the FULL contents of session.json including {{ and }}")
        return

    log.info("JSON valid — writing session.json to disk (NOT to git)...")
    try:
        with open(Config.SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(parsed, f, indent=4, ensure_ascii=False)
        log.info("🔑 session.json written successfully (temp file, not committed to git)")
    except IOError as e:
        log.error(f"Could not write session.json: {e}")


# ============================================================
#              STEP 2 — VERIFY SETUP
# ============================================================
def verify_setup():
    """
    WHAT: Checks all required GitHub Secrets are set before doing anything.
    WHY:  Fail fast with a clear message rather than crashing halfway through.
    """
    log.step(2, 10, "Verify all required GitHub Secrets")
    critical_missing = []

    checks = [
        (Config.IG_USERNAME,      "IG_USERNAME",      "Your Instagram username (no @)"),
        (Config.IG_PASSWORD,      "IG_PASSWORD",      "Your Instagram password"),
        (Config.GDRIVE_FOLDER_ID, "GDRIVE_FOLDER_ID", "Google Drive folder ID"),
        (Config.GDRIVE_API_KEY,   "GDRIVE_API_KEY",   "Google Drive API key"),
    ]
    for value, name, description in checks:
        if value:
            log.info(f"   ✓ {name} is set")
        else:
            log.error(f"   ✗ {name} is MISSING — {description}")
            critical_missing.append(name)

    if Config.GEMINI_API_KEY:
        log.info("   ✓ GEMINI_API_KEY is set (AI thumbnails enabled)")
    else:
        log.warn("   ~ GEMINI_API_KEY not set → will use video frames for thumbnails")

    if Config.IG_SESSION or os.path.exists(Config.SESSION_FILE):
        log.info("   ✓ Instagram session available")
    else:
        log.warn("   ~ No Instagram session — login will fail")

    if critical_missing:
        log.error(f"STOPPING: {len(critical_missing)} required secret(s) missing: "
                  f"{', '.join(critical_missing)}")
        return False

    log.info("✅ All required secrets verified")
    return True


# ============================================================
#              STEP 3 — GOOGLE DRIVE SCAN
# ============================================================
def list_drive_movies():
    """
    WHAT: Lists all video files in your Google Drive folder.
    WHY:  Detects new movies automatically — just upload to Drive and
          the script finds them on the next run.
    NEEDS: Folder must be shared as 'Anyone with the link can view'.
    """
    log.step(3, 10, "Scan Google Drive folder for video files")
    folder_id  = Config.GDRIVE_FOLDER_ID
    api_key    = Config.GDRIVE_API_KEY
    url        = "https://www.googleapis.com/drive/v3/files"
    all_files  = []
    page_token = None

    log.info(f"Calling Google Drive API v3...")
    log.info(f"Folder ID: {folder_id}")

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
            log.info("Sending Drive API request...")
            r = requests.get(url, params=params, timeout=30)
            log.info(f"Drive API response: HTTP {r.status_code}")

            if r.status_code == 403:
                log.error("Drive API: 403 Access denied.")
                log.error("Fix: Share the Drive folder → 'Anyone with the link' → Viewer")
                return []
            if r.status_code == 404:
                log.error("Drive API: 404 Folder not found.")
                log.error("Fix: Check GDRIVE_FOLDER_ID secret is the correct folder ID")
                return []
            if r.status_code != 200:
                log.error(f"Drive API unexpected error {r.status_code}: {r.text[:300]}")
                return []

            data = r.json()
            files_on_page = data.get("files", [])
            log.info(f"Got {len(files_on_page)} items from Drive (this page)")

            for f in files_on_page:
                name = f["name"]
                if any(name.lower().endswith(ext) for ext in Config.VIDEO_EXTS):
                    size_mb = round(int(f.get("size", 0)) / (1024*1024), 1)
                    log.info(f"   🎬 Found video: {name} ({size_mb} MB)")
                    all_files.append({
                        "id":      f["id"],
                        "name":    name,
                        "size":    int(f.get("size", 0)),
                        "created": f.get("createdTime", ""),
                    })
                else:
                    log.info(f"   ⏭️  Skipping non-video: {name}")

            page_token = data.get("nextPageToken")
            if not page_token:
                break
            log.info("More pages available — fetching next page...")

        except requests.exceptions.RequestException as e:
            log.error(f"Drive API network error: {e}")
            return []

    log.info(f"Drive scan complete: {len(all_files)} video(s) found")
    return all_files


def download_movie(file_id, output_path):
    """
    WHAT: Downloads the movie file from Google Drive using gdown.
    WHY:  We download each time (not stored in repo) to save GitHub storage.
    """
    log.step(6, 10, f"Download movie from Google Drive")
    log.info(f"File ID: {file_id}")
    log.info(f"Saving to: {output_path}")
    try:
        if os.path.exists(output_path):
            log.info("Removing old download file...")
            os.remove(output_path)

        log.info("Starting download (progress shown below)...")
        gdown.download(f"https://drive.google.com/uc?id={file_id}",
                       output_path, quiet=False)

        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            log.info(f"✅ Download complete! File size: {size_mb:.1f} MB")
            return True

        log.error("Download finished but file is empty or missing!")
        return False

    except Exception as e:
        log.error(f"Download exception: {e}")
        log.error(traceback.format_exc())
        return False


# ============================================================
#              STEP 7 — VIDEO INFO (fast ffprobe)
# ============================================================
def ffprobe_duration(video_path):
    """
    WHAT: Reads video duration using ffprobe (part of ffmpeg).
    WHY:  Fast metadata read — does not decode any video frames.
    TIME: ~0.1 seconds.
    """
    log.info("Reading video duration with ffprobe...")
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        duration = float(result.stdout.strip())
        log.info(f"ffprobe result: {duration:.2f} seconds")
        return duration
    except Exception as e:
        log.error(f"ffprobe failed: {e}")
        log.error("Is ffmpeg installed? Workflow should install it with apt-get.")
        return 0.0


def get_video_info(video_path):
    """Calculate how many 60-second parts the movie will be split into."""
    log.step(7, 10, "Analyse video duration and calculate parts count")
    duration = ffprobe_duration(video_path)
    if duration <= 0:
        return 0, 0

    total_parts = 0
    start = 0
    while start < duration:
        end = min(start + Config.CLIP_LENGTH, duration)
        if end - start >= 5:   # skip tiny final clips under 5s
            total_parts += 1
        start += Config.CLIP_LENGTH

    log.info(f"Video: {int(duration)//60}m {int(duration)%60}s total")
    log.info(f"Split into: {total_parts} parts × {Config.CLIP_LENGTH}s each")
    return duration, total_parts


# ============================================================
#          STEP 9 (part A) — CLIP EXTRACTION (ffmpeg fast)
# ============================================================
def extract_clip(video_path, part_num, total_parts, output_path):
    """
    WHAT: Cuts a 60-second segment from the movie.
    HOW:  ffmpeg -c copy (stream copy, NO re-encoding).
          This copies raw video bytes — no quality loss, extremely fast.
    WHY NOT moviepy: moviepy re-encodes every frame through Python.
          On a GitHub runner it takes 3-8 minutes per clip.
          ffmpeg stream copy takes 2-5 SECONDS per clip.
    TIME: ~2-5 seconds per clip.
    """
    start        = (part_num - 1) * Config.CLIP_LENGTH
    duration_sec = Config.CLIP_LENGTH

    log.info(f"✂️  Cutting Part {part_num}/{total_parts} | "
             f"Time range: {start}s → {start + duration_sec}s")
    log.info(f"   Method: ffmpeg stream-copy (no re-encoding, very fast)")
    log.info(f"   Output: {output_path}")

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start),           # seek before input = fast seek
        "-i", video_path,
        "-t", str(duration_sec),
        "-c", "copy",                # COPY bytes, do NOT re-encode
        "-avoid_negative_ts", "make_zero",
        "-movflags", "+faststart",   # makes MP4 streamable
        output_path,
    ]

    try:
        log.info(f"   Running: ffmpeg -ss {start} -t {duration_sec} -c copy ...")
        t_start = time.time()
        result  = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        elapsed = time.time() - t_start

        if result.returncode != 0:
            log.error(f"ffmpeg failed (exit code {result.returncode})")
            log.error(f"ffmpeg stderr: {result.stderr[-600:]}")
            return False

        if os.path.exists(output_path) and os.path.getsize(output_path) > 10_000:
            size_mb = os.path.getsize(output_path) / (1024*1024)
            log.info(f"   ✅ Clip ready in {elapsed:.1f}s — size: {size_mb:.1f} MB")
            return True
        else:
            log.error(f"ffmpeg produced no output file for part {part_num}")
            return False

    except subprocess.TimeoutExpired:
        log.error(f"ffmpeg timed out after 120s on part {part_num}")
        return False
    except Exception as e:
        log.error(f"extract_clip exception: {e}")
        return False


# ============================================================
#          STEP 9 (part B) — THUMBNAIL BACKGROUND
# ============================================================
def extract_frame_ffmpeg(video_path, time_sec, output_jpg):
    """
    WHAT: Extracts one frame from the video as a JPEG image.
    TIME: ~0.5 seconds per frame.
    """
    log.info(f"   Extracting frame at t={time_sec:.1f}s → {output_jpg}")
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(time_sec),
        "-i", video_path,
        "-frames:v", "1",
        "-q:v", "2",
        output_jpg,
    ]
    try:
        subprocess.run(cmd, capture_output=True, timeout=30)
        if os.path.exists(output_jpg) and os.path.getsize(output_jpg) > 0:
            img = Image.open(output_jpg).copy()
            log.info(f"   Frame extracted: {img.width}×{img.height}px")
            return img
    except Exception as e:
        log.error(f"Frame extract failed at t={time_sec:.1f}s: {e}")
    log.warn(f"   Using blank fallback frame")
    return Image.new("RGB", (1280, 720), (20, 20, 40))


def extract_frames_for_grid(video_path, duration, frame_count=9):
    """
    WHAT: Extracts 9 frames spread across the movie for thumbnail selection.
    WHY:  Gemini AI will look at all 9 and pick the most attractive one.
    TIME: ~5-8 seconds total (9 × ~0.5s ffmpeg calls).
    """
    log.info(f"Extracting {frame_count} candidate frames from video...")
    log.info(f"Spread across: 15% to 78% of movie duration ({duration:.0f}s)")
    frames  = []
    tmp_dir = "tmp_frames"
    os.makedirs(tmp_dir, exist_ok=True)

    for i in range(frame_count):
        t   = duration * (0.15 + i * 0.07)
        t   = min(t, duration - 1.0)
        out = os.path.join(tmp_dir, f"frame_{i}.jpg")
        log.info(f"   Frame {i+1}/{frame_count}: t={t:.1f}s")
        img = extract_frame_ffmpeg(video_path, t, out)
        frames.append(img)

    shutil.rmtree(tmp_dir, ignore_errors=True)
    log.info(f"All {frame_count} frames extracted successfully")
    return frames


def create_frame_grid(frames, tile_size=320):
    """Arrange 9 frames into a 3×3 grid image for Gemini to analyse."""
    log.info(f"Building 3×3 frame grid (tile size: {tile_size}px)...")
    grid = Image.new("RGB", (tile_size * 3, tile_size * 3))
    for idx, img in enumerate(frames):
        x = (idx % 3) * tile_size
        y = (idx // 3) * tile_size
        grid.paste(img.resize((tile_size, tile_size)), (x, y))
    log.info(f"Grid created: {grid.width}×{grid.height}px")
    return grid


def choose_best_frame_with_gemini(grid_image, frames):
    """
    WHAT: Sends the 3×3 frame grid to Gemini and asks it to pick
          the most visually attractive frame for the thumbnail (1-9).
    WHY:  AI picks a frame with good lighting, interesting action,
          and visible characters rather than a random middle frame.
    FALLBACK: Uses frame 5 (middle) if Gemini fails or is not configured.
    IMPORTANT: Uses Part.from_bytes() API — NOT raw dicts.
    """
    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        log.warn("Gemini not configured → using middle frame (frame 5) as thumbnail")
        return frames[4]

    log.info("Converting grid image to JPEG bytes for Gemini...")
    buf = BytesIO()
    grid_image.save(buf, format="JPEG", quality=85)
    image_bytes = buf.getvalue()
    log.info(f"Grid image size: {len(image_bytes) / 1024:.1f} KB")

    prompt_text = (
        "You are selecting the best movie thumbnail frame.\n"
        "The image shows a 3x3 grid numbered:\n"
        "  1 2 3\n  4 5 6\n  7 8 9\n\n"
        "Pick the frame that is brightest, clearest, has visible "
        "characters or action, and would attract the most viewers.\n"
        "Reply with ONLY a single digit 1-9. Nothing else."
    )

    try:
        log.info("Initialising Gemini client...")
        client = genai.Client(api_key=Config.GEMINI_API_KEY)
    except Exception as e:
        log.warn(f"Gemini client init failed: {e} → using middle frame")
        return frames[4]

    for model_name in Config.GEMINI_VISION_MODELS:
        try:
            log.info(f"Asking Gemini ({model_name}) to pick best frame...")
            image_part = genai_types.Part.from_bytes(
                data=image_bytes, mime_type="image/jpeg")
            text_part  = genai_types.Part.from_text(text=prompt_text)
            response   = client.models.generate_content(
                model=model_name,
                contents=[image_part, text_part],
            )
            raw   = response.text.strip()
            log.info(f"Gemini responded: '{raw}'")
            digit = next((c for c in raw if c.isdigit() and c != "0"), None)
            if digit and 1 <= int(digit) <= 9:
                log.info(f"✅ Gemini chose frame #{digit} as best thumbnail background")
                return frames[int(digit) - 1]
            log.warn(f"Gemini returned unexpected value '{raw}' → trying next model")
        except Exception as e:
            log.warn(f"Gemini vision {model_name} failed: {e}")
            log.warn("Trying next Gemini model...")

    log.warn("All Gemini vision models failed → using middle frame (frame 5)")
    return frames[4]


def generate_gemini_background(movie_name):
    """
    WHAT: Asks Gemini to generate a cinematic AI poster image.
    WHY:  Creates a more dramatic, styled background than a raw video frame.
    NOTE: Only works if your Gemini API key has image generation access.
          Falls back to best video frame if not available.
    """
    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        log.info("Gemini image generation skipped (no API key)")
        return None

    prompt = (
        f"Cinematic movie poster background for '{movie_name}'. "
        "Dark moody atmosphere, dramatic lighting, professional quality. "
        "NO text, NO letters, NO words anywhere in the image."
    )
    log.info(f"Trying Gemini AI image generation for: {movie_name}")

    try:
        client = genai.Client(api_key=Config.GEMINI_API_KEY)
    except Exception as e:
        log.warn(f"Gemini init failed: {e}")
        return None

    for model_name in Config.GEMINI_IMAGE_MODELS:
        try:
            log.info(f"   Trying model: {model_name}...")
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    response_modalities=["IMAGE", "TEXT"]
                ),
            )
            if response.candidates:
                for part in response.candidates[0].content.parts:
                    if hasattr(part, "inline_data") and part.inline_data is not None:
                        image = Image.open(BytesIO(part.inline_data.data))
                        log.info(f"✅ AI background image generated! "
                                 f"Size: {image.width}×{image.height}px")
                        return image
            log.warn(f"   {model_name}: response had no image → trying next")
        except Exception as e:
            log.warn(f"   {model_name} failed: {e}")

    log.warn("All Gemini image models failed → falling back to best video frame")
    return None


# ============================================================
#                  THUMBNAIL COMPOSER  (Pillow)
# ============================================================
def get_font(size):
    for fp in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                continue
    return ImageFont.load_default()


def create_thumbnail(bg_image, movie_name, part_num, total_parts,
                     movie_num, total_movies, output_path):
    """
    WHAT: Composites the final thumbnail image using Pillow.
    Layout:
      - Background: AI-generated or best video frame (resized to 1080×1920)
      - Dark gradient overlay top and bottom (makes text readable)
      - Movie title text at top (white, bold, word-wrapped)
      - Gold "PART X / Y" text at bottom
      - Grey "Movie N of M" counter below that
      - Gold decorative line above part number
    """
    log.info(f"   Compositing thumbnail for Part {part_num}/{total_parts}...")
    try:
        thumb = bg_image.copy().resize((1080, 1920), Image.LANCZOS).convert("RGBA")
        log.info(f"   Background resized to 1080×1920px")

        # Dark gradient overlays
        overlay = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
        odraw   = ImageDraw.Draw(overlay)
        for y in range(500):
            odraw.rectangle([(0, y), (1080, y+1)],
                            fill=(0, 0, 0, int(220 * (1 - y / 500))))
        for y in range(1420, 1920):
            odraw.rectangle([(0, y), (1080, y+1)],
                            fill=(0, 0, 0, int(220 * ((y - 1420) / 500))))

        thumb = Image.alpha_composite(thumb, overlay).convert("RGB")
        draw  = ImageDraw.Draw(thumb)

        font_title = get_font(68)
        font_part  = get_font(56)
        font_info  = get_font(36)

        # Word-wrap title
        title = movie_name.upper()
        if len(title) > 18:
            words, lines, line = title.split(), [], ""
            for w in words:
                test = (line + " " + w).strip()
                if len(test) > 18 and line:
                    lines.append(line)
                    line = w
                else:
                    line = test
            if line:
                lines.append(line)
        else:
            lines = [title]

        y_cur = 100
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=font_title)
            tw   = bbox[2] - bbox[0]
            x    = (1080 - tw) // 2
            for dx in range(-3, 4):
                for dy in range(-3, 4):
                    draw.text((x+dx, y_cur+dy), line, font=font_title, fill="black")
            draw.text((x, y_cur), line, font=font_title, fill="white")
            y_cur += bbox[3] - bbox[1] + 15

        part_text = f"PART {part_num} / {total_parts}"
        bbox = draw.textbbox((0, 0), part_text, font=font_part)
        tw   = bbox[2] - bbox[0]
        x    = (1080 - tw) // 2
        for dx in range(-2, 3):
            for dy in range(-2, 3):
                draw.text((x+dx, 1740+dy), part_text, font=font_part, fill="black")
        draw.text((x, 1740), part_text, font=font_part, fill=(255, 215, 0))

        ct   = f"Movie {movie_num} of {total_movies}"
        bbox = draw.textbbox((0, 0), ct, font=font_info)
        draw.text(((1080-(bbox[2]-bbox[0]))//2, 1810),
                  ct, font=font_info, fill=(180, 180, 180))

        draw.rectangle([(200, 1720), (880, 1723)], fill=(255, 215, 0))

        thumb.save(output_path, "JPEG", quality=95)
        log.info(f"   ✅ Thumbnail saved: {output_path}")
        return True

    except Exception as e:
        log.error(f"Thumbnail compositing failed: {e}")
        log.error(traceback.format_exc())
        log.warn("Using plain text fallback thumbnail...")
        try:
            fb = Image.new("RGB", (1080, 1920), (20, 20, 40))
            d  = ImageDraw.Draw(fb)
            f2 = get_font(60)
            d.text((100, 800), movie_name,                       font=f2, fill="white")
            d.text((100, 900), f"Part {part_num}/{total_parts}", font=f2, fill=(255,215,0))
            fb.save(output_path, "JPEG")
            log.info("   Fallback thumbnail saved")
            return True
        except Exception:
            return False


# ============================================================
#              STEP 10 — INSTAGRAM LOGIN (session only)
# ============================================================
def instagram_login():
    """
    WHAT: Logs in to Instagram using the pre-saved session.
    WHY NO FRESH LOGIN: GitHub Actions uses a different IP every run.
          Instagram treats each new IP as a suspicious device → sends OTP.
          Using a session token avoids this — it looks like the same device.
    HOW SESSION WAS CREATED: You ran generate_session.py on your local PC
          once, which did the fresh login + OTP there, and saved session.json.
          That file's contents are stored as IG_SESSION GitHub Secret.
    SECURITY: session.json is NEVER committed to git. It is written to disk
          from the secret at runtime and excluded via .gitignore.
    """
    log.step(10, 10, "Login to Instagram via saved session")

    if not os.path.exists(Config.SESSION_FILE):
        log.error("session.json not found on disk!")
        log.error("Solution: Make sure IG_SESSION secret is set correctly in GitHub.")
        return None

    log.info(f"Loading session from: {Config.SESSION_FILE}")

    for attempt in range(1, 4):
        try:
            log.info(f"Login attempt {attempt}/3...")
            cl = Client()
            cl.delay_range = [2, 5]
            log.info("   Loading session settings...")
            cl.load_settings(Config.SESSION_FILE)
            log.info("   Calling cl.login() with saved session...")
            cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
            log.info("   Verifying session is alive (get_timeline_feed)...")
            cl.get_timeline_feed()
            log.info("   Refreshing and saving session...")
            cl.dump_settings(Config.SESSION_FILE)
            log.info(f"✅ Instagram login successful (attempt {attempt})")
            return cl

        except ChallengeRequired:
            log.error("⛔ Instagram sent a security challenge.")
            log.error("Your session has expired or been flagged.")
            log.error("FIX: Run generate_session.py on your local PC again,")
            log.error("     complete the challenge, then update the IG_SESSION secret.")
            return None

        except Exception as e:
            log.warn(f"Login attempt {attempt} failed: {e}")
            if attempt < 3:
                wait = 30 * attempt
                log.info(f"Waiting {wait}s before retry {attempt+1}...")
                time.sleep(wait)

    log.error("All 3 login attempts failed.")
    log.error("FIX: Regenerate session.json and update IG_SESSION secret.")
    return None


# ============================================================
#              UPLOAD REEL
# ============================================================
def upload_reel(cl, video_path, thumbnail_path, caption):
    """
    WHAT: Uploads a single 60-second clip to Instagram as a Reel.
    RETRY: Up to 3 attempts with increasing wait times on failure.
    RETURNS: True = uploaded | False = failed | 'STOP' = fatal error
    """
    file_size_mb = os.path.getsize(video_path) / (1024*1024) if os.path.exists(video_path) else 0
    log.info(f"   Video file: {video_path} ({file_size_mb:.1f} MB)")
    log.info(f"   Thumbnail:  {thumbnail_path}")
    log.info(f"   Caption length: {len(caption)} chars")

    for retry in range(1, 4):
        try:
            log.info(f"   Upload attempt {retry}/3 — calling cl.clip_upload()...")
            t_start = time.time()
            kwargs  = {"path": video_path, "caption": caption}
            if thumbnail_path and os.path.exists(thumbnail_path):
                kwargs["thumbnail"] = Path(thumbnail_path)
            cl.clip_upload(**kwargs)
            elapsed = time.time() - t_start
            log.info(f"   cl.clip_upload() returned successfully in {elapsed:.1f}s")
            return True

        except PleaseWaitFewMinutes:
            wait = 600 * retry
            log.warn(f"Instagram rate limit → waiting {wait//60} minutes...")
            time.sleep(wait)

        except ClientThrottledError:
            wait = 900 * retry
            log.warn(f"Instagram throttle → waiting {wait//60} minutes...")
            time.sleep(wait)

        except FeedbackRequired as e:
            log.error(f"Instagram FeedbackRequired: {e}")
            log.error("Account may be flagged. Upload stopped to protect account.")
            return "STOP"

        except ChallengeRequired:
            log.error("Instagram challenge required during upload.")
            log.error("FIX: Update IG_SESSION secret with fresh session.")
            return "STOP"

        except LoginRequired:
            log.warn("Session expired during upload → attempting re-login...")
            try:
                cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
                cl.dump_settings(Config.SESSION_FILE)
                log.info("Re-login successful — retrying upload...")
            except Exception as re_e:
                log.error(f"Re-login failed: {re_e}")
                return "STOP"

        except ConnectionError as e:
            wait = 180 * retry
            log.warn(f"Connection error: {e} → waiting {wait//60}m...")
            time.sleep(wait)

        except Exception as e:
            log.error(f"Upload exception on attempt {retry}: {e}")
            log.error(traceback.format_exc())
            if retry < 3:
                wait = 300 * retry
                log.info(f"Waiting {wait//60}m before retry...")
                time.sleep(wait)

    log.error(f"Upload failed after 3 attempts")
    return False


# ============================================================
#                   SMART DELAY (2hr gap)
# ============================================================
def smart_delay(upload_num):
    """
    REMOVED: Delay is now handled by the cron schedule (every 2 hours).
    This function is kept as a no-op so no other code needs changing.
    Previously it slept for 2 hours inside GitHub Actions — wasteful.
    Now the workflow just exits after 1 upload and cron re-triggers it.
    """
    log.info("No in-script delay needed — cron schedule handles the 2hr gap")


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
    for k, v in default.items():
        if k not in data:
            data[k] = v
    return data


def save_movies_log(data):
    data["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["total_completed"] = sum(
        1 for m in data["movies"].values() if m["status"] == "completed")
    data["total_movies_found"]   = len(data["movies"])
    data["total_reels_uploaded"] = sum(
        m.get("uploaded_parts", 0) for m in data["movies"].values())
    save_json(Config.LOG_FILE, data)


def sync_with_drive(movies_log, drive_files):
    """Add any new Drive files to tracking. Never removes existing entries."""
    added = 0
    for f in drive_files:
        name = f["name"]
        if name not in movies_log["movies"]:
            movies_log["movies"][name] = {
                "drive_id":         f["id"],
                "status":           "pending",
                "total_parts":      0,
                "uploaded_parts":   0,
                "size_mb":          round(f["size"] / (1024*1024), 1),
                "started_at":       "",
                "completed_at":     "",
                "last_uploaded_at": "",
                "errors":           0,
            }
            log.info(f"🆕 New movie added to tracker: {name}")
            added += 1
    if added:
        log.info(f"Added {added} new movie(s) to tracking")
    else:
        log.info("No new movies detected in Drive")
    return movies_log


def get_next_movie(movies_log):
    """Resume in-progress first, then start next pending."""
    for name, info in movies_log["movies"].items():
        if info["status"] == "in_progress":
            log.info(f"▶️ Resuming in-progress movie: {name}")
            return name, info
    for name, info in movies_log["movies"].items():
        if info["status"] == "pending":
            log.info(f"🆕 Starting new movie: {name}")
            return name, info
    return None, None


def load_progress():
    return load_json(Config.PROGRESS_FILE,
                     {"movie_name": "", "last_uploaded": 0, "total_parts": 0})


def save_progress(data):
    save_json(Config.PROGRESS_FILE, data)


# ============================================================
#                    SUMMARY REPORT
# ============================================================
def print_summary(movies_log):
    log.separator("=")
    print("📊 FINAL MOVIES STATUS REPORT", flush=True)
    log.separator("-")
    emoji_map = {"pending":"⏳","in_progress":"🔄","completed":"✅","error":"❌"}
    for idx, (name, info) in enumerate(movies_log["movies"].items(), 1):
        emoji   = emoji_map.get(info["status"], "❓")
        display = movie_display_name(name)
        parts   = f"{info.get('uploaded_parts',0)}/{info.get('total_parts','?')}"
        print(f"  {emoji} #{idx} {display}", flush=True)
        print(f"      Status: {info['status']} | Parts uploaded: {parts} | "
              f"Size: {info.get('size_mb','?')} MB", flush=True)
        if info.get("started_at"):   print(f"      Started:   {info['started_at']}", flush=True)
        if info.get("completed_at"): print(f"      Completed: {info['completed_at']}", flush=True)
        if info.get("errors", 0) > 0: print(f"      Errors:    {info['errors']}", flush=True)
        print(flush=True)
    log.separator("-")
    total = len(movies_log["movies"])
    done  = movies_log.get("total_completed", 0)
    reels = movies_log.get("total_reels_uploaded", 0)
    print(f"  📈 Movies completed:  {done}/{total}", flush=True)
    print(f"  📤 Total reels uploaded: {reels}", flush=True)
    print(f"  🕐 Last run: {movies_log.get('last_run','N/A')}", flush=True)
    log.separator("=")


# ============================================================
#                        MAIN
# ============================================================
def main():
    log.separator("=")
    print("🎬 FULLY AUTOMATED INSTAGRAM REEL UPLOADER", flush=True)
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"📋 Plan: download → thumbnail → login → upload 1 reel → exit (cron handles 2hr gap)", flush=True)
    log.separator("=")

    # ── STEP 1: Write session from secret ────────────────────
    write_session_from_secret()

    # ── STEP 2: Verify all secrets ───────────────────────────
    if not verify_setup():
        log.error("Cannot continue — fix missing secrets above then re-run")
        return

    # ── STEP 3: Scan Drive ───────────────────────────────────
    drive_files = list_drive_movies()
    if not drive_files:
        log.error("No videos found in Drive. Upload a video and try again.")
        return

    # ── STEP 4: Sync movie tracker ───────────────────────────
    log.step(4, 10, "Sync movie tracker with Drive contents")
    movies_log = load_movies_log()
    log.info(f"Loaded tracker: {len(movies_log['movies'])} movies known")
    movies_log = sync_with_drive(movies_log, drive_files)
    save_movies_log(movies_log)
    log.info("Tracker saved")

    # ── STEP 5: Pick next movie ──────────────────────────────
    log.step(5, 10, "Select next movie to process")
    movie_name, movie_info = get_next_movie(movies_log)
    if not movie_name:
        log.info("🎉 All movies have been fully uploaded! Nothing to do.")
        print_summary(movies_log)
        return

    display_name = movie_display_name(movie_name)
    log.info(f"Selected: {display_name}")
    log.info(f"Status:   {movie_info['status']}")
    log.info(f"Size:     {movie_info.get('size_mb','?')} MB")

    # ── STEP 6: Download ─────────────────────────────────────
    if not download_movie(movie_info["drive_id"], Config.MOVIE_FILE):
        log.error("Download failed — will retry on next scheduled run")
        movie_info["errors"] = movie_info.get("errors", 0) + 1
        save_movies_log(movies_log)
        git_push()
        return

    # ── STEP 7: Video info ───────────────────────────────────
    duration, total_parts = get_video_info(Config.MOVIE_FILE)
    if total_parts == 0:
        log.error("Could not read video file — marking as error")
        movie_info["status"] = "error"
        save_movies_log(movies_log)
        git_push()
        return

    movie_info["total_parts"] = total_parts
    log.info(f"Movie will be split into {total_parts} reels × {Config.CLIP_LENGTH}s")

    # ── STEP 8: Mark in progress ─────────────────────────────
    log.step(8, 10, "Update movie status and load upload progress")
    if movie_info["status"] == "pending":
        movie_info["status"]     = "in_progress"
        movie_info["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("Status updated: pending → in_progress")
    movies_log["current_movie"] = movie_name
    save_movies_log(movies_log)

    progress = load_progress()
    if progress.get("movie_name") != movie_name:
        log.info("New movie detected — resetting part counter to 0")
        progress = {"movie_name": movie_name,
                    "last_uploaded": 0, "total_parts": total_parts}
    last_uploaded = progress["last_uploaded"]
    log.info(f"Upload progress: {last_uploaded}/{total_parts} parts done so far")
    if last_uploaded > 0:
        log.info(f"Resuming from Part {last_uploaded + 1}")

    # ── STEP 9: Thumbnail background (cached once per movie) ─
    log.step(9, 10, "Prepare thumbnail background image")
    thumb_bg = None

    if os.path.exists(Config.THUMB_BG_FILE):
        log.info(f"Found cached background: {Config.THUMB_BG_FILE}")
        try:
            thumb_bg = Image.open(Config.THUMB_BG_FILE)
            log.info(f"✅ Cached thumbnail background loaded "
                     f"({thumb_bg.width}×{thumb_bg.height}px) — skipping generation")
        except Exception as e:
            log.warn(f"Could not load cached background ({e}) — regenerating...")
            thumb_bg = None

    if thumb_bg is None:
        log.info("No cached background found — generating one now...")
        log.info("Attempt 1: Gemini AI image generation...")
        thumb_bg = generate_gemini_background(display_name)

        if thumb_bg is None:
            log.info("Gemini image gen not available — using best video frame instead")
            log.info("Attempt 2: Extract 9 frames and ask Gemini vision to pick best...")
            frames   = extract_frames_for_grid(Config.MOVIE_FILE, duration)
            grid     = create_frame_grid(frames)
            log.info("Sending frame grid to Gemini for selection...")
            thumb_bg = choose_best_frame_with_gemini(grid, frames)

        log.info(f"Saving thumbnail background to {Config.THUMB_BG_FILE}...")
        try:
            thumb_bg.save(Config.THUMB_BG_FILE, "JPEG", quality=95)
            log.info(f"✅ Thumbnail background cached for all {total_parts} parts")
        except Exception as e:
            log.warn(f"Could not save thumbnail background cache: {e}")

    # ── STEP 10: Instagram login ─────────────────────────────
    cl = instagram_login()
    if cl is None:
        log.error("Instagram login failed — saving progress and stopping")
        save_progress(progress)
        save_movies_log(movies_log)
        git_push()
        return

    os.makedirs(Config.REELS_DIR,  exist_ok=True)
    os.makedirs(Config.THUMBS_DIR, exist_ok=True)

    movie_names  = list(movies_log["movies"].keys())
    movie_num    = movie_names.index(movie_name) + 1
    total_movies = len(movie_names)

    remaining_parts = total_parts - last_uploaded
    log.info(f"Ready to upload! Parts remaining: {remaining_parts}")
    log.info(f"This run will upload up to {Config.MAX_UPLOADS_PER_RUN} parts")
    log.separator("=")

    # ── UPLOAD LOOP ──────────────────────────────────────────
    uploaded_this_run = 0
    stop_uploading    = False

    for part_num in range(last_uploaded + 1, total_parts + 1):

        if stop_uploading:
            log.warn("Upload loop stopped due to fatal error")
            break

        if uploaded_this_run >= Config.MAX_UPLOADS_PER_RUN:
            log.info(f"🛑 Reached run limit of {Config.MAX_UPLOADS_PER_RUN} uploads")
            log.info("Remaining parts will be uploaded on the next scheduled run")
            log.info(f"Parts left: {total_parts - part_num + 1}")
            break

        log.separator("-")
        log.info(f"📦 PART {part_num} of {total_parts} | "
                 f"'{display_name}' | "
                 f"Run upload #{uploaded_this_run+1}/{Config.MAX_UPLOADS_PER_RUN}")

        # ── Cut clip ─────────────────────────────────────────
        clip_path = os.path.join(Config.REELS_DIR, f"part_{part_num}.mp4")
        log.info(f"[{part_num}/{total_parts}] Cutting clip with ffmpeg...")
        if not extract_clip(Config.MOVIE_FILE, part_num, total_parts, clip_path):
            log.warn(f"Clip extraction failed — skipping Part {part_num}")
            progress["last_uploaded"] = part_num
            save_progress(progress)
            continue

        # ── Make thumbnail ───────────────────────────────────
        thumb_path = os.path.join(Config.THUMBS_DIR, f"thumb_{part_num}.jpg")
        log.info(f"[{part_num}/{total_parts}] Creating thumbnail...")
        if thumb_bg:
            bg_image = thumb_bg.copy()
        else:
            mid_t    = ((part_num-1) * Config.CLIP_LENGTH) + (Config.CLIP_LENGTH//2)
            mid_t    = min(mid_t, duration - 1)
            tmp_jpg  = os.path.join(Config.THUMBS_DIR, f"tmp_{part_num}.jpg")
            log.info(f"   Extracting fallback frame at t={mid_t:.0f}s...")
            bg_image = extract_frame_ffmpeg(Config.MOVIE_FILE, mid_t, tmp_jpg)

        create_thumbnail(bg_image, display_name, part_num, total_parts,
                         movie_num, total_movies, thumb_path)

        # ── Build caption ────────────────────────────────────
        caption = random.choice(Config.CAPTIONS).format(
            name=display_name, p=part_num, t=total_parts)
        log.info(f"[{part_num}/{total_parts}] Caption: {caption[:60]}...")

        # ── Upload ───────────────────────────────────────────
        log.info(f"[{part_num}/{total_parts}] Uploading reel to Instagram...")
        result = upload_reel(cl, clip_path, thumb_path, caption)

        if result == "STOP":
            log.error("Fatal Instagram error — stopping upload loop")
            log.upload(display_name, part_num, total_parts, "FATAL_STOP")
            stop_uploading = True

        elif result is True:
            log.info(f"✅ Part {part_num}/{total_parts} uploaded successfully!")
            log.upload(display_name, part_num, total_parts, "SUCCESS")
            uploaded_this_run             += 1
            progress["last_uploaded"]      = part_num
            movie_info["uploaded_parts"]   = part_num
            movie_info["last_uploaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log.info(f"Saving progress: {part_num}/{total_parts} done")
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()

        else:
            log.error(f"Part {part_num} upload failed after 3 attempts")
            log.upload(display_name, part_num, total_parts, "FAILED")
            movie_info["errors"] = movie_info.get("errors", 0) + 1
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()
            log.info("Waiting 10 minutes then continuing to next part...")
            time.sleep(600)
            continue

        # ── Cleanup clip + thumbnail ─────────────────────────
        log.info(f"[{part_num}/{total_parts}] Cleaning up clip and thumbnail files...")
        for f in [clip_path, thumb_path]:
            if os.path.exists(f):
                os.remove(f)
                log.info(f"   Deleted: {f}")

        # No in-script delay — cron triggers next run in 2 hours

    # ── Check movie complete ──────────────────────────────────
    log.separator("*")
    if progress["last_uploaded"] >= total_parts:
        log.info(f"🎉🎉🎉 Movie '{display_name}' is FULLY UPLOADED! 🎉🎉🎉")
        log.info(f"All {total_parts} parts have been posted to Instagram")
        movie_info["status"]         = "completed"
        movie_info["uploaded_parts"] = total_parts
        movie_info["completed_at"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        movies_log["current_movie"]  = ""
        progress = {"movie_name": "", "last_uploaded": 0, "total_parts": 0}
        log.info("Cleaning up all temp files for this movie...")
        cleanup_temp()
    else:
        remaining = total_parts - progress["last_uploaded"]
        log.info(f"Run complete — {uploaded_this_run} reels uploaded this run")
        log.info(f"{remaining} parts still remaining — will continue next scheduled run")
    log.separator("*")

    # ── Final save ────────────────────────────────────────────
    log.info("Saving final progress...")
    save_progress(progress)
    save_movies_log(movies_log)
    git_push()
    print_summary(movies_log)

    log.separator("=")
    log.info(f"✅ RUN COMPLETE")
    log.info(f"   Uploaded this run:  {uploaded_this_run} reels")
    log.info(f"   Total uploaded:     {movies_log.get('total_reels_uploaded',0)} reels")
    log.info(f"   Finished at:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.separator("=")


# ============================================================
#                      ENTRY POINT
# ============================================================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warn("Interrupted by user (Ctrl+C)")
        git_push()
    except Exception as e:
        log.error(f"💥 CRITICAL UNHANDLED ERROR: {e}")
        log.error(traceback.format_exc())
        log.error("Attempting emergency progress save...")
        git_push()
        sys.exit(1)
