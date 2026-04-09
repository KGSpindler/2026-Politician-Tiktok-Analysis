"""
TiktokAccountStatFinder v3
All v2 improvements plus:

  CAPTCHA HANDLING (free — no API key required)
  ──────────────────────────────────────────────
  - Uses undetected-chromedriver (uc) instead of plain Selenium to reduce
    how often TikTok serves captchas in the first place.
  - Free local OpenCV solver handles puzzle/slide captchas when they do
    appear. Approach: Canny edge detection + template matching on the
    background image to locate the gap, then a human-like drag via
    ActionChains. Based on github.com/onurkun/puzzle-captcha-resolver (MIT).
  - Captcha detection runs at the top of every scroll iteration and after
    initial page load.
  - If auto-solve fails (rotate / 3D shapes / wrong selectors), waits up to
    CAPTCHA_MANUAL_TIMEOUT seconds so you can solve it manually.
  - Required:  pip install undetected-chromedriver opencv-python Pillow

  COLLECTION ACCURACY
  ───────────────────
  - Consecutive-old-scroll counter (MAX_CONSECUTIVE_OLD_SCROLLS=3) replaces
    the instant old_video_seen break, handling out-of-order timelines.
  - Pinned-video guard (PINNED_VIDEO_SKIP=3): first 3 fetched videos are
    exempt from the old-scroll counter (pinned videos predate the timeline).
  - MAX_SCROLLS 40→80, NO_NEW_LINKS_STOP 3→5, MAX_SMALL_GROWTH_ROUNDS 4→7.
  - Slow-scroll fallback: smaller 200-400 px scrolls when stagnating.
  - Explicit post-scroll wait polls link count instead of fixed sleep.

  BLOCK DETECTION
  ───────────────
  - Single recovery attempt per error: if one reload fails, RetryLaterError
    is raised immediately — rapid retries against a blocked IP never help.
  - EmptyAccountError distinguishes "account has no videos" from "blocked"
    by checking URL, error markers, and link count together.
"""

import argparse
import base64
import csv
import io
import json
import logging
import os
import random
import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

# ─────────────────────────── paths ───────────────────────────
_HERE = Path(__file__).parent
_ROOT = _HERE.parent
_RUN_ID = datetime.now().strftime("%Y-%m-%d_%H%M%S")

def _env_path(var, default: Path) -> Path:
    v = os.environ.get(var)
    return Path(v) if v else default

CANDIDATES_CSV    = _env_path("TIKTOK_CANDIDATES_CSV",  _ROOT / "Inputs" / "TikTok" / "current" / "candidates_tiktok_accounts.csv")
PARTIES_CSV       = _env_path("TIKTOK_PARTIES_CSV",     _ROOT / "Inputs" / "TikTok" / "current" / "party_tiktok_accounts_from_claude.csv")
OUTDIR            = _env_path("TIKTOK_OUTDIR",          _ROOT / "Outputs" / "TikTok" / f"run_{_RUN_ID}")
CHECKPOINT_FILE   = OUTDIR / "tiktok_collect_checkpoint.json"
RETRY_LATER_FILE  = OUTDIR / "tiktok_retry_later.json"

# ─────────────────────────── tunable constants ───────────────────────────
MONTHS_BACK       = int(os.environ.get("TIKTOK_MONTHS", "6"))
MAX_SCROLLS       = int(os.environ.get("TIKTOK_MAX_SCROLLS", "80"))

NO_NEW_LINKS_STOP           = 5
MIN_EXPECTED_NEW_LINKS      = 2
MAX_SMALL_GROWTH_ROUNDS     = 7
PROFILE_STATS_RETRIES       = 2
MAX_CONSECUTIVE_OLD_SCROLLS = 3
PINNED_VIDEO_SKIP           = 3   # exempt first N fetched videos from old-scroll counter

# Single recovery attempt — rapid retries against a blocked IP never help
MAX_RECOVERY_ATTEMPTS       = 1

BETWEEN_SCROLL_MIN          = 6.0
BETWEEN_SCROLL_MAX          = 10.0
BETWEEN_VIDEO_FETCH_MIN     = 1.2
BETWEEN_VIDEO_FETCH_MAX     = 2.8
BETWEEN_PROFILE_MIN         = 65.0
BETWEEN_PROFILE_MAX         = 130.0
NO_VIDEO_PROFILE_MIN        = 8.0
NO_VIDEO_PROFILE_MAX        = 20.0
COOLDOWN_EVERY_N_PROFILES   = 10
COOLDOWN_MIN_SECONDS        = 20
COOLDOWN_MAX_SECONDS        = 75

SCROLL_SETTLE_TIMEOUT       = 8
SLOW_SCROLL_THRESHOLD       = 2
MOUSE_MOVE_EVERY_N_SCROLLS  = 5

# Captcha constants
CAPTCHA_SOLVE_TIMEOUT       = 30    # seconds to wait for auto-solve confirmation
CAPTCHA_MANUAL_TIMEOUT      = 120   # seconds to wait for manual solve fallback
CAPTCHA_DRAG_STEPS          = 30    # smoothstep drag granularity
CAPTCHA_MIN_CONFIDENCE      = 0.25  # minimum template-match score to attempt drag

# ─────────────────────────── logging ───────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# Custom exceptions
# ═══════════════════════════════════════════════════════════════
class RetryLaterError(Exception):
    pass

class EmptyAccountError(Exception):
    """Profile loaded correctly but the account has no videos at all."""
    pass


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════
def parse_args():
    p = argparse.ArgumentParser(description="Hent TikTok-statistik for politikere og partier")
    p.add_argument("--candidates",    default=str(CANDIDATES_CSV))
    p.add_argument("--parties",       default=str(PARTIES_CSV))
    p.add_argument("--months",        type=int, default=MONTHS_BACK)
    p.add_argument("--reset",         action="store_true",  help="Start forfra (slet checkpoint)")
    p.add_argument("--max-profiles",  type=int, default=None)
    p.add_argument("--dry-run",       action="store_true",  help="Vis plan uden at åbne browser")
    p.add_argument("--only-handles",  default=None,         help="Kommasepareret liste af handles")
    p.add_argument("--chrome-profile", default=None,
                   help="Sti til Chrome User Data-mappe (overfører cookies/login-session)")
    return p.parse_args()


# ═══════════════════════════════════════════════════════════════
# Profile loading
# ═══════════════════════════════════════════════════════════════
def extract_handle(url):
    if not isinstance(url, str):
        return None
    m = re.search(r"tiktok\.com/@([^/?&#\s]+)", url)
    return m.group(1) if m else None


def load_profiles(candidates_path, parties_path):
    profiles = []

    c = pd.read_csv(candidates_path, encoding="utf-8-sig")
    c_ok = c[c["tiktok_url"].notna()].copy()
    c_ok["tiktok_handle"] = c_ok["tiktok_url"].apply(extract_handle)
    c_ok = c_ok[c_ok["tiktok_handle"].notna()]
    for _, row in c_ok.iterrows():
        profiles.append({
            "handle":       row["tiktok_handle"].strip(),
            "display_name": row["candidate_name"],
            "party_name":   row.get("party_name", ""),
            "storkreds":    row.get("storkreds", ""),
            "account_type": "kandidat",
        })

    p = pd.read_csv(parties_path, encoding="utf-8-sig")
    p_ok = p[p["tiktok_url"].notna() & p["tiktok_handle"].notna()].copy()
    for _, row in p_ok.iterrows():
        handle = str(row["tiktok_handle"]).strip()
        if not handle:
            continue
        profiles.append({
            "handle":       handle,
            "display_name": row.get("candidate_name", handle),
            "party_name":   row.get("party_name", ""),
            "storkreds":    row.get("storkreds", ""),
            "account_type": row.get("account_type", ""),
        })

    seen, unique = set(), []
    for prof in profiles:
        if prof["handle"] not in seen:
            seen.add(prof["handle"])
            unique.append(prof)
    return unique


# ═══════════════════════════════════════════════════════════════
# Checkpoint helpers
# ═══════════════════════════════════════════════════════════════
def _read_json_set(path: Path) -> set:
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()


def _write_json_set(path: Path, values):
    path.write_text(
        json.dumps(sorted(values), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_checkpoint(reset: bool):
    if reset:
        CHECKPOINT_FILE.unlink(missing_ok=True)
        RETRY_LATER_FILE.unlink(missing_ok=True)
        print("Reset: starter forfra.\n")
        return set(), set()
    done        = _read_json_set(CHECKPOINT_FILE)
    retry_later = _read_json_set(RETRY_LATER_FILE)
    if done:
        print(f"Genoptager: {len(done)} profiler allerede hentet.")
    if retry_later:
        print(f"Retry-later: {len(retry_later)} profiler markeret til ny kørsel.")
    if done or retry_later:
        print()
    return done, retry_later


def save_checkpoint(done, retry_later):
    _write_json_set(CHECKPOINT_FILE, done)
    _write_json_set(RETRY_LATER_FILE, retry_later)


# ═══════════════════════════════════════════════════════════════
# CSV output
# ═══════════════════════════════════════════════════════════════
def append_rows(rows, path: Path):
    if not rows:
        return
    cols = list(rows[0].keys())
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(rows)


def count_csv_rows(path: Path) -> int:
    """Count data rows in a CSV file without failing on malformed rows."""
    if not path.exists():
        return 0

    try:
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            next(reader, None)
            return sum(1 for _ in reader)
    except Exception as exc:
        log.warning("Kunne ikke læse %s til række-tælling: %s", path, exc)
        return 0


# ═══════════════════════════════════════════════════════════════
# Data-parsing helpers
# ═══════════════════════════════════════════════════════════════
def _safe_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def _parse_ts(ts):
    if ts in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(ts))
    except Exception:
        return None


def _get_item_struct(data) -> dict:
    if not isinstance(data, dict):
        return {}
    return (
        data.get("itemInfo", {}).get("itemStruct")
        or data.get("__DEFAULT_SCOPE__", {})
               .get("webapp.video-detail", {})
               .get("itemInfo", {})
               .get("itemStruct")
        or data.get("aweme_detail")
        or {}
    )


def _video_row_from_json(data, profile, video_url, run_id):
    item     = _get_item_struct(data)
    stats    = item.get("stats", {}) if isinstance(item, dict) else {}
    video_id = str(item.get("id", ""))
    ts       = _parse_ts(item.get("createTime"))
    date_str = ts.strftime("%Y-%m-%d") if ts else "UNKNOWN"

    return {
        "run_id":         run_id,
        "tiktok_handle":  profile["handle"],
        "display_name":   profile["display_name"],
        "party_name":     profile["party_name"],
        "storkreds":      profile["storkreds"],
        "account_type":   profile["account_type"],
        "video_id":       video_id,
        "upload_dato":    date_str,
        "beskrivelse":    str(item.get("desc", ""))[:250],
        "visninger":      _safe_int(stats.get("playCount")),
        "likes":          _safe_int(stats.get("diggCount")),
        "kommentarer":    _safe_int(stats.get("commentCount")),
        "shares":         _safe_int(stats.get("shareCount")),
        "gemmer":         _safe_int(stats.get("collectCount")),
        "video_url":      video_url,
    }, ts, item


def _extract_profile_stats_from_item(item, profile, run_id):
    author_stats = item.get("authorStats", {}) if isinstance(item, dict) else {}
    if not author_stats:
        return None
    return {
        "run_id":         run_id,
        "tiktok_handle":  profile["handle"],
        "display_name":   profile["display_name"],
        "party_name":     profile["party_name"],
        "account_type":   profile["account_type"],
        "følgere":        author_stats.get("followerCount", ""),
        "samlet_likes":   author_stats.get("heartCount", ""),
        "antal_videoer":  author_stats.get("videoCount", ""),
        "hentet_dato":    datetime.now().strftime("%Y-%m-%d"),
    }


def get_profile_stats(pyk, profile, run_id):
    url = f"https://www.tiktok.com/@{profile['handle']}"
    for attempt in range(1, PROFILE_STATS_RETRIES + 2):
        try:
            data = pyk.alt_get_tiktok_json(url)
            if not data:
                break
            stats = (
                data.get("userInfo", {}).get("stats")
                or data.get("stats")
                or {}
            )
            if not stats:
                user_module = data.get("__DEFAULT_SCOPE__", {}).get("webapp.user-detail", {})
                stats = user_module.get("userInfo", {}).get("stats") or {}
            if stats:
                return {
                    "run_id":        run_id,
                    "tiktok_handle": profile["handle"],
                    "display_name":  profile["display_name"],
                    "party_name":    profile["party_name"],
                    "account_type":  profile["account_type"],
                    "følgere":       stats.get("followerCount", ""),
                    "samlet_likes":  stats.get("heartCount", stats.get("diggCount", "")),
                    "antal_videoer": stats.get("videoCount", ""),
                    "hentet_dato":   datetime.now().strftime("%Y-%m-%d"),
                }
        except Exception as e:
            log.warning("get_profile_stats attempt %d/%d failed for @%s: %s",
                        attempt, PROFILE_STATS_RETRIES + 1, profile["handle"], e)
        if attempt <= PROFILE_STATS_RETRIES:
            time.sleep(2 ** attempt)
    return None


# ═══════════════════════════════════════════════════════════════
# Browser — undetected-chromedriver
# ═══════════════════════════════════════════════════════════════

def _detect_chrome_version() -> Optional[int]:
    """Detect installed Chrome version from Windows registry."""
    try:
        import winreg
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Google\Chrome\BLBeacon"
        )
        version_str, _ = winreg.QueryValueEx(key, "version")
        winreg.CloseKey(key)
        major_version = int(version_str.split(".")[0])
        log.info("Opdaget Chrome-version: %d", major_version)
        return major_version
    except Exception as e:
        log.warning("Kunne ikke opdage Chrome-version fra registry: %s", e)
        return None


def _clear_chromedriver_cache_if_mismatch(expected_version: int):
    """Delete cached ChromeDriver if it doesn't match the expected version."""
    try:
        cache_dir = Path(os.path.expanduser("~")) / "AppData" / "Roaming" / "undetected_chromedriver"
        if not cache_dir.exists():
            return
        
        # Check what version is cached
        driver_file = cache_dir / "undetected_chromedriver.exe"
        if not driver_file.exists():
            return
        
        log.info("Sletter gammel ChromeDriver-cache for version-opdatering...")
        import shutil
        try:
            shutil.rmtree(cache_dir)
            log.info("Cache slettet succesfuldt")
        except Exception as e:
            log.warning("Kunne ikke slette cache: %s", e)
    except Exception as e:
        log.debug("_clear_chromedriver_cache_if_mismatch: %s", e)


def build_driver(chrome_profile_path=None):
    """
    Build an undetected-chromedriver instance.

    uc patches Chrome at binary level to remove the navigator.webdriver flag
    and dozens of other Selenium/automation fingerprints that TikTok checks,
    significantly reducing captcha frequency vs plain Selenium.

    Install: pip install undetected-chromedriver
    """
    import undetected_chromedriver as uc

    # Detect Chrome version and clear cache if needed
    chrome_version = _detect_chrome_version()
    if chrome_version:
        _clear_chromedriver_cache_if_mismatch(chrome_version)

    opts = uc.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    if chrome_profile_path:
        opts.add_argument(f"--user-data-dir={Path(chrome_profile_path)}")
        log.info("Bruger Chrome-profil: %s", chrome_profile_path)

    # headless=False is intentional — TikTok JS actively detects headless
    log.info("Initialiserer undetected-chromedriver...")
    
    # If we detected Chrome version, use it explicitly; otherwise auto-detect
    if chrome_version:
        return uc.Chrome(options=opts, headless=False, version_main=chrome_version)
    else:
        return uc.Chrome(options=opts, headless=False, version_main=None)


def human_pause(min_s, max_s, reason=None):
    dur = random.uniform(min_s, max_s)
    if reason:
        print(f"    Pause {dur:.1f}s ({reason})")
    time.sleep(dur)


def cooldown_pause(reason, min_s=COOLDOWN_MIN_SECONDS, max_s=COOLDOWN_MAX_SECONDS):
    dur = random.uniform(min_s, max_s)
    print(f"\n⏸ {reason}. Venter {dur/60:.1f} min.\n")
    time.sleep(dur)


# ═══════════════════════════════════════════════════════════════
# Free captcha solver — OpenCV puzzle/slide
# ═══════════════════════════════════════════════════════════════
#
# TikTok's DOM class names change often. Each list below contains
# multiple selectors tried in order; the first match wins.

_CAPTCHA_PRESENCE_SELECTORS = [
    "[id*='captcha']", "[class*='captcha']",
    "[class*='secsdk']", "[class*='verify-wrap']",
]
_BG_SELECTORS = [
    "img.captcha_verify_img_path",
    "img[class*='captcha'][class*='bg']",
    "img[class*='verify'][class*='bg']",
    "img[class*='secsdk'][class*='bg']",
    "img[class*='captcha'][class*='image']",
]
_PIECE_SELECTORS = [
    "img.captcha_verify_img_path ~ img",
    "img[class*='captcha'][class*='piece']",
    "img[class*='captcha'][class*='fg']",
    "img[class*='verify'][class*='piece']",
    "img[class*='secsdk'][class*='fg']",
]
_SLIDER_SELECTORS = [
    ".secsdk-captcha-drag-icon__close-icon",
    ".captcha_verify_slide--btn",
    "[class*='captcha'][class*='drag']",
    "[class*='captcha'][class*='slider']",
    "[class*='verify'][class*='btn']",
    "[class*='captcha'][class*='btn']",
]


def _captcha_visible(driver) -> bool:
    """Return True if any captcha container is present in the DOM."""
    try:
        for sel in _CAPTCHA_PRESENCE_SELECTORS:
            if driver.find_elements("css selector", sel):
                return True
    except Exception:
        pass
    return False


def _src_to_cv2(src: str):
    """
    Load a <img> src (data URI or HTTPS URL) into an OpenCV BGR array.
    Returns None on failure.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image

        if src.startswith("data:"):
            _, b64 = src.split(",", 1)
            raw = base64.b64decode(b64)
        else:
            import urllib.request
            with urllib.request.urlopen(src, timeout=10) as r:
                raw = r.read()

        pil = Image.open(io.BytesIO(raw)).convert("RGB")
        return cv2.cvtColor(np.array(pil), cv2.COLOR_RGB2BGR)
    except Exception as e:
        log.debug("_src_to_cv2 failed: %s", e)
        return None


def _compute_puzzle_offset(bg, piece) -> Optional[int]:
    """
    Canny edge detection + normalized cross-correlation template matching.
    Returns the x-pixel offset where the piece fits, or None if confidence
    is below CAPTCHA_MIN_CONFIDENCE (avoids dragging to the wrong place).

    Reference: github.com/onurkun/puzzle-captcha-resolver (MIT licence).
    """
    try:
        import cv2

        bg_gray    = cv2.cvtColor(bg,    cv2.COLOR_BGR2GRAY)
        piece_gray = cv2.cvtColor(piece, cv2.COLOR_BGR2GRAY)

        # Resize piece to match background scale if wildly different
        if piece_gray.shape[0] > bg_gray.shape[0] * 0.9:
            scale = bg_gray.shape[0] / piece_gray.shape[0] * 0.5
            piece_gray = cv2.resize(
                piece_gray,
                (int(piece_gray.shape[1] * scale), int(piece_gray.shape[0] * scale))
            )

        bg_edges    = cv2.Canny(bg_gray,    100, 200)
        piece_edges = cv2.Canny(piece_gray, 100, 200)

        result = cv2.matchTemplate(bg_edges, piece_edges, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, max_loc = cv2.minMaxLoc(result)

        log.info("Captcha template match: confidence=%.3f  x=%d", max_val, max_loc[0])

        return max_loc[0] if max_val >= CAPTCHA_MIN_CONFIDENCE else None
    except Exception as e:
        log.debug("_compute_puzzle_offset failed: %s", e)
        return None


def _human_drag(driver, slider_el, x_pixels: int):
    """
    Drag slider_el `x_pixels` to the right with a smoothstep speed profile
    and small random vertical jitter to mimic a human hand.
    """
    from selenium.webdriver.common.action_chains import ActionChains

    chain = ActionChains(driver)
    chain.click_and_hold(slider_el).pause(random.uniform(0.1, 0.3))

    prev_ease = 0.0
    for step in range(1, CAPTCHA_DRAG_STEPS + 1):
        t    = step / CAPTCHA_DRAG_STEPS
        ease = t * t * (3 - 2 * t)   # smoothstep: slow → fast → slow
        dx   = max(1, int((ease - prev_ease) * x_pixels))
        dy   = random.randint(-2, 2)
        chain.move_by_offset(dx, dy).pause(random.uniform(0.005, 0.025))
        prev_ease = ease

    chain.pause(random.uniform(0.1, 0.35)).release().perform()


def _solve_puzzle_captcha(driver) -> bool:
    """
    Auto-solve a TikTok slide-puzzle captcha using OpenCV template matching.

    1. Extract background and puzzle-piece <img> elements from the DOM.
    2. Compute the drag offset with Canny + matchTemplate.
    3. Drag the slider with a human-like motion.
    4. Confirm the captcha disappeared.

    Returns True if solved, False if anything went wrong.
    """
    try:
        # ── locate background image ───────────────────────────────────────
        bg_el = None
        for sel in _BG_SELECTORS:
            els = driver.find_elements("css selector", sel)
            if els:
                bg_el = els[0]
                break

        # ── locate puzzle piece image ─────────────────────────────────────
        piece_el = None
        for sel in _PIECE_SELECTORS:
            els = driver.find_elements("css selector", sel)
            if els:
                piece_el = els[0]
                break

        # Fallback: find all <img> inside the captcha container; largest =
        # background, second largest = piece
        if not bg_el or not piece_el:
            for sel in _CAPTCHA_PRESENCE_SELECTORS:
                containers = driver.find_elements("css selector", sel)
                if containers:
                    imgs = containers[0].find_elements("tag name", "img")
                    if len(imgs) >= 2:
                        imgs.sort(
                            key=lambda el: el.size.get("width", 0) * el.size.get("height", 0),
                            reverse=True,
                        )
                        bg_el, piece_el = imgs[0], imgs[1]
                    break

        if not bg_el or not piece_el:
            log.warning("Captcha: billederne kunne ikke identificeres i DOM")
            return False

        bg_src    = bg_el.get_attribute("src") or ""
        piece_src = piece_el.get_attribute("src") or ""
        if not bg_src or not piece_src:
            log.warning("Captcha: src-attribut mangler på billederne")
            return False

        bg_cv2    = _src_to_cv2(bg_src)
        piece_cv2 = _src_to_cv2(piece_src)
        if bg_cv2 is None or piece_cv2 is None:
            log.warning("Captcha: kunne ikke konvertere billeder til OpenCV-format")
            return False

        # ── find offset ───────────────────────────────────────────────────
        x_offset = _compute_puzzle_offset(bg_cv2, piece_cv2)
        if x_offset is None:
            log.warning("Captcha: template matching confidence for lav — springer over")
            return False

        # ── scale offset to actual slider width on screen ─────────────────
        # The image may be displayed at a different pixel width than its
        # natural width, so we scale the computed offset proportionally.
        try:
            natural_w  = int(bg_el.get_attribute("naturalWidth") or bg_cv2.shape[1])
            display_w  = bg_el.size.get("width", natural_w) or natural_w
            x_offset   = int(x_offset * display_w / natural_w)
        except Exception:
            pass  # if scaling fails, use raw offset

        log.info("Captcha: skaleret x-offset = %d px → trækker slider", x_offset)

        # ── find slider ───────────────────────────────────────────────────
        slider_el = None
        for sel in _SLIDER_SELECTORS:
            els = driver.find_elements("css selector", sel)
            if els:
                slider_el = els[0]
                break

        if not slider_el:
            log.warning("Captcha: slider-element ikke fundet")
            return False

        _human_drag(driver, slider_el, x_offset)

        # ── confirm solve ─────────────────────────────────────────────────
        deadline = time.time() + CAPTCHA_SOLVE_TIMEOUT
        while time.time() < deadline:
            if not _captcha_visible(driver):
                log.info("Captcha løst automatisk ✓")
                return True
            time.sleep(0.5)

        return False

    except Exception as e:
        log.warning("_solve_puzzle_captcha fejl: %s", e)
        return False


def handle_captcha(driver) -> bool:
    """
    Top-level captcha handler called from the scroll loop.

    1. Try the free OpenCV puzzle solver.
    2. If that fails (rotate / 3D shapes / unrecognised layout), wait up to
       CAPTCHA_MANUAL_TIMEOUT seconds for manual resolution.
    3. Returns True if captcha gone, False if still present after timeout.
    """
    if not _captcha_visible(driver):
        return True

    print("    ⚠ Captcha opdaget — forsøger automatisk OpenCV-løsning...")
    solved = _solve_puzzle_captcha(driver)

    if solved:
        time.sleep(random.uniform(1.0, 2.0))
        return True

    # Auto-solve failed — prompt for manual intervention
    print(f"    ⚠ Automatisk løsning fejlede (muligvis rotate/3D-captcha).")
    print(f"    Løs venligst captchaen manuelt i browservinduet.")
    print(f"    Venter op til {CAPTCHA_MANUAL_TIMEOUT} sekunder...")

    deadline = time.time() + CAPTCHA_MANUAL_TIMEOUT
    while time.time() < deadline:
        if not _captcha_visible(driver):
            print("    ✓ Captcha løst manuelt")
            return True
        remaining = int(deadline - time.time())
        if remaining % 20 == 0 and remaining > 0:
            print(f"    ... {remaining}s tilbage ...")
        time.sleep(1)

    print("    ✗ Captcha stadig synlig efter timeout")
    return False


# ═══════════════════════════════════════════════════════════════
# Page-state helpers
# ═══════════════════════════════════════════════════════════════

def _extract_video_urls_from_page(driver, handle) -> list:
    anchors = driver.find_elements("css selector", "a[href*='/video/']")
    out, seen = [], set()
    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
        except Exception:
            continue
        if f"/@{handle}/video/" not in href:
            continue
        href = href.split("?")[0]
        if href not in seen:
            seen.add(href)
            out.append(href)
    return out


def tiktok_error_visible(driver, handle=None) -> bool:
    """True if the page shows an error state. Captchas are NOT counted as errors."""
    try:
        page = (driver.page_source or "").lower()
    except Exception:
        return True
    markers = [
        "something went wrong", "please try again later",
        "network issue", "too many attempts", "maximum number of attempts",
    ]
    if any(m in page for m in markers):
        if handle:
            try:
                if _extract_video_urls_from_page(driver, handle):
                    return False
            except Exception:
                pass
        return True
    return False


def _profile_is_empty(driver, handle) -> bool:
    """
    True when the profile loaded correctly but has no videos.
    All three must hold: correct URL, no error markers, zero video links.
    """
    try:
        if f"/@{handle.lower()}" not in (driver.current_url or "").lower():
            return False
        if tiktok_error_visible(driver, handle=handle):
            return False
        return len(_extract_video_urls_from_page(driver, handle)) == 0
    except Exception:
        return False


def wait_for_profile_content(driver, handle, timeout=25) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if _extract_video_urls_from_page(driver, handle):
                return True
        except Exception:
            pass
        if tiktok_error_visible(driver, handle=handle):
            return False
        time.sleep(1.0)
    return False


def wait_for_new_links(driver, handle, previous_count: int, timeout=8) -> int:
    """Poll until visible link count grows beyond previous_count, or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            current = len(_extract_video_urls_from_page(driver, handle))
            if current > previous_count:
                return current
        except Exception:
            pass
        time.sleep(0.8)
    try:
        return len(_extract_video_urls_from_page(driver, handle))
    except Exception:
        return previous_count


def recover_profile_page(driver, profile_url, handle) -> bool:
    """Single reload attempt. Caller raises RetryLaterError if this fails."""
    print("    Recovery-forsøg (enkelt reload)")
    human_pause(10, 16, "før refresh")
    try:
        driver.get(profile_url)
    except Exception:
        pass
    human_pause(5, 8, "efter refresh")
    if wait_for_profile_content(driver, handle, timeout=22):
        return True
    return not tiktok_error_visible(driver, handle=handle)


def _random_mouse_move(driver):
    try:
        from selenium.webdriver.common.action_chains import ActionChains
        vw = driver.execute_script("return window.innerWidth")
        vh = driver.execute_script("return window.innerHeight")
        x  = random.randint(max(1, vw // 4), min(vw - 10, 3 * vw // 4))
        y  = random.randint(max(1, vh // 4), min(vh - 10, 3 * vh // 4))
        ActionChains(driver).move_by_offset(x // 4, y // 4).perform()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# Core scraper
# ═══════════════════════════════════════════════════════════════
def scrape_profile(pyk, profile, cutoff, raw_dir, run_id,
                   global_seen_video_ids: set,
                   chrome_profile_path=None):
    """
    Scroll a TikTok profile page and collect video rows posted after `cutoff`.
    Returns (list_of_rows, first_item_dict_or_None).
    Side-effect: updates global_seen_video_ids.
    """
    handle    = profile["handle"]
    raw_path  = raw_dir / f"{handle}_raw.csv"
    seen_urls = set()
    all_rows  = []
    first_item = None

    stagnant_scrolls        = 0
    previous_link_count     = 0
    small_growth_rounds     = 0
    consecutive_old_scrolls = 0
    videos_fetched_total    = 0   # used for pinned-video guard

    driver = build_driver(chrome_profile_path)
    try:
        profile_url = f"https://www.tiktok.com/@{handle}"
        driver.get(profile_url)
        human_pause(7, 12, "efter profilåbning")

        # Handle any captcha on initial load
        if _captcha_visible(driver):
            if not handle_captcha(driver):
                raise RetryLaterError("Captcha kunne ikke løses ved profilåbning")
            human_pause(2, 4, "efter captcha-løsning")

        loaded_ok = wait_for_profile_content(driver, handle, timeout=25)
        if not loaded_ok:
            if tiktok_error_visible(driver, handle=handle):
                if not recover_profile_page(driver, profile_url, handle):
                    # Distinguish empty accounts from true transient blocks.
                    if _profile_is_empty(driver, handle):
                        raise EmptyAccountError(f"@{handle} har ingen videoer (tomt profilside)")
                    raise RetryLaterError("TikTok-side viste fejl fra start")
            elif _profile_is_empty(driver, handle):
                raise EmptyAccountError(f"@{handle} har ingen videoer (tomt profilside)")

        # ── main scroll loop ──────────────────────────────────────────────
        for scroll_idx in range(MAX_SCROLLS):

            # Captcha check — every iteration
            if _captcha_visible(driver):
                if not handle_captcha(driver):
                    raise RetryLaterError("Captcha kunne ikke løses under scrolling")
                human_pause(2, 4, "efter captcha-løsning")

            # Error guard
            if tiktok_error_visible(driver, handle=handle):
                if not recover_profile_page(driver, profile_url, handle):
                    if _profile_is_empty(driver, handle):
                        raise EmptyAccountError(
                            f"@{handle} har ingen videoer (tomt profilside efter recovery)"
                        )
                    raise RetryLaterError("TikTok-side viste fejl — recovery slog fejl")

            # ── collect new video URLs ────────────────────────────────────
            urls     = _extract_video_urls_from_page(driver, handle)
            new_urls = [u for u in urls if u not in seen_urls]
            scroll_had_new_in_period = False

            for url in new_urls:
                seen_urls.add(url)
                human_pause(BETWEEN_VIDEO_FETCH_MIN, BETWEEN_VIDEO_FETCH_MAX,
                            "mellem video-metadata")
                try:
                    data = pyk.alt_get_tiktok_json(url)
                except Exception as e:
                    print(f"    ⚠ Metadata-fejl for {url}: {e}")
                    continue

                row, ts, item = _video_row_from_json(data, profile, url, run_id)
                video_id = row["video_id"]

                if not video_id or video_id in global_seen_video_ids:
                    continue
                global_seen_video_ids.add(video_id)
                videos_fetched_total += 1

                if first_item is None and item:
                    first_item = item

                if ts is not None and ts < cutoff:
                    continue   # outside the requested period

                all_rows.append(row)
                scroll_had_new_in_period = True

            # ── consecutive-old-scroll logic with pinned-video guard ───────
            # Stop only after MAX_CONSECUTIVE_OLD_SCROLLS back-to-back scrolls
            # where ALL new videos are out-of-period.
            # The first PINNED_VIDEO_SKIP videos are exempt because TikTok may
            # pin old videos at the top of any profile.
            if new_urls and not scroll_had_new_in_period:
                if videos_fetched_total > PINNED_VIDEO_SKIP:
                    consecutive_old_scrolls += 1
                    if consecutive_old_scrolls >= MAX_CONSECUTIVE_OLD_SCROLLS:
                        print(f"    {MAX_CONSECUTIVE_OLD_SCROLLS} scrolls i træk kun"
                              f" med gamle videoer — stopper")
                        break
                else:
                    print(f"    Gammel video inden for de første {PINNED_VIDEO_SKIP}"
                          f" (muligvis pinned) — ignorerer")
            else:
                consecutive_old_scrolls = 0

            # Persist raw progress after each scroll
            if all_rows:
                pd.DataFrame(all_rows).to_csv(raw_path, index=False, encoding="utf-8-sig")

            print(
                f"    Scroll {scroll_idx + 1:>2}: "
                f"{len(new_urls):>3} nye links | "
                f"{len(all_rows):>3} i perioden | "
                f"hentet: {videos_fetched_total} | "
                f"old-streak: {consecutive_old_scrolls}"
            )

            # ── stagnation detection ──────────────────────────────────────
            current_link_count = len(seen_urls)
            stagnant_scrolls   = stagnant_scrolls + 1 if current_link_count == previous_link_count else 0
            previous_link_count = current_link_count

            small_growth_rounds = small_growth_rounds + 1 if len(new_urls) < MIN_EXPECTED_NEW_LINKS else 0

            if stagnant_scrolls >= NO_NEW_LINKS_STOP:
                if videos_fetched_total == 0 and _profile_is_empty(driver, handle):
                    raise EmptyAccountError(
                        f"@{handle} har ingen videoer "
                        f"(ingen links fundet efter {scroll_idx + 1} scrolls)"
                    )
                print("    Ingen nye links efter flere scrolls — stopper")
                break

            if small_growth_rounds >= MAX_SMALL_GROWTH_ROUNDS:
                print("    Meget lille vækst over flere scrolls — stopper")
                break

            # ── slow-scroll fallback when stagnating ──────────────────────
            if stagnant_scrolls >= SLOW_SCROLL_THRESHOLD:
                print("    Skifter til langsom scroll for at aktivere lazy-loader")
                for _ in range(4):
                    driver.execute_script("window.scrollBy(0, arguments[0]);",
                                          random.randint(200, 400))
                    time.sleep(random.uniform(1.2, 2.0))
                driver.execute_script("window.scrollBy(0, arguments[0]);",
                                      -random.randint(100, 250))
                time.sleep(random.uniform(0.8, 1.5))

            # ── stagnation recovery (one attempt only) ────────────────────
            if stagnant_scrolls == 2 and len(all_rows) >= 20:
                print("    Forsøger ét enkelt recovery-reload ved stagnation")
                if recover_profile_page(driver, profile_url, handle):
                    stagnant_scrolls = 0

            # ── random mouse move every N scrolls ─────────────────────────
            if (scroll_idx + 1) % MOUSE_MOVE_EVERY_N_SCROLLS == 0:
                _random_mouse_move(driver)

            # ── main scroll ───────────────────────────────────────────────
            scroll_px = random.randint(700, 1400)
            driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_px)
            if random.random() < 0.15:
                time.sleep(random.uniform(0.3, 0.7))
                driver.execute_script("window.scrollBy(0, arguments[0]);",
                                      -random.randint(40, 120))

            prev_visible = len(_extract_video_urls_from_page(driver, handle))
            wait_for_new_links(driver, handle, prev_visible, timeout=SCROLL_SETTLE_TIMEOUT)
            human_pause(BETWEEN_SCROLL_MIN, BETWEEN_SCROLL_MAX, "mellem scrolls")

        return all_rows, first_item

    finally:
        try:
            driver.quit()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
# Session run-log
# ═══════════════════════════════════════════════════════════════
def init_run_log(log_dir: Path, run_id: str, profiles_total: int):
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"run_{run_id}.jsonl"
    path.write_text(
        json.dumps({
            "run_id":         run_id,
            "started_at":     datetime.now().isoformat(),
            "profiles_total": profiles_total,
        }, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return path


def append_run_log(path: Path, record: dict):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════
def main():
    args = parse_args()

    OUTDIR.mkdir(parents=True, exist_ok=True)
    raw_dir = OUTDIR / "raw"
    raw_dir.mkdir(exist_ok=True)
    log_dir = OUTDIR / "run_logs"

    videos_out   = OUTDIR / "tiktok_videos.csv"
    profiles_out = OUTDIR / "tiktok_profiles.csv"

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

    try:
        import pyktok as pyk
    except ImportError:
        print("pyktok ikke installeret. Kør:\n"
              "  pip install pyktok pandas tqdm "
              "undetected-chromedriver opencv-python Pillow")
        return

    all_profiles = load_profiles(args.candidates, args.parties)

    if args.only_handles:
        wanted = {h.strip().lstrip("@") for h in args.only_handles.split(",")}
        all_profiles = [p for p in all_profiles if p["handle"] in wanted]
        print(f"--only-handles: filtreret til {len(all_profiles)} profil(er)\n")

    print(f"Profiler i alt: {len(all_profiles)}\n")

    cutoff = (datetime.now() - timedelta(days=30 * args.months)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    print(f"Henter videoer efter: {cutoff.strftime('%Y-%m-%d')}\n")

    if args.dry_run:
        print("─── DRY RUN ────────────────────────────────────────")
        for p in all_profiles[:20]:
            print(f"  @{p['handle']:<30}  {p['account_type']:<20}  {p['party_name']}")
        if len(all_profiles) > 20:
            print(f"  … og {len(all_profiles)-20} flere")
        print("────────────────────────────────────────────────────")
        return

    print("Initialiserer med Chrome-cookies...")
    try:
        pyk.specify_browser("chrome")
        print("OK\n")
    except Exception as e:
        print(f"⚠ Cookie-fejl ({e}) — forsøger alligevel\n")

    done, retry_later = load_checkpoint(args.reset)

    # Retry-only mode: process only accounts that were previously marked retry-later.
    todo = [p for p in all_profiles if p["handle"] in retry_later and p["handle"] not in done]

    if args.max_profiles is not None:
        todo = todo[: args.max_profiles]

    print(f"Tilbage: {len(todo)} | Hentet: {len(done)} | Retry-later: {len(retry_later)}\n")
    print("Captcha: gratis OpenCV-løser (puzzle/slide) + manuel fallback (rotate/3D)\n")

    run_log_path = init_run_log(log_dir, run_id, len(todo))

    global_seen_video_ids: set = set()
    session_video_total  = 0
    session_count        = 0
    consecutive_empty    = 0

    for profile in tqdm(todo, desc="Profiler", unit="profil"):
        handle = profile["handle"]
        profile_pause_min = BETWEEN_PROFILE_MIN
        profile_pause_max = BETWEEN_PROFILE_MAX
        print(f"\n{'─' * 54}")
        print(f"@{handle}  [{profile['account_type']}]  {profile['party_name']}")

        try:
            video_rows, first_item = scrape_profile(
                pyk, profile, cutoff, raw_dir,
                run_id, global_seen_video_ids,
                chrome_profile_path=args.chrome_profile,
            )
            append_rows(video_rows, videos_out)
            session_video_total += len(video_rows)
            print(f"  ✓ {len(video_rows)} videoer gemt  (total: {session_video_total})")

            consecutive_empty = 0

            stats = get_profile_stats(pyk, profile, run_id)
            if not stats and first_item:
                stats = _extract_profile_stats_from_item(first_item, profile, run_id)
            if stats:
                append_rows([stats], profiles_out)

            done.add(handle)
            retry_later.discard(handle)
            save_checkpoint(done, retry_later)
            append_run_log(run_log_path, {
                "handle": handle, "status": "ok",
                "videos": len(video_rows), "ts": datetime.now().isoformat(),
            })

        except KeyboardInterrupt:
            print("\n⚠ Afbrudt — gemmer checkpoint...")
            save_checkpoint(done, retry_later)
            print("Kør igen for at fortsætte.")
            return

        except EmptyAccountError as e:
            print(f"  — Ingen videoer: {e}")
            done.add(handle)
            retry_later.discard(handle)
            save_checkpoint(done, retry_later)
            # No-video profiles are expected outcomes; move on quickly.
            profile_pause_min = NO_VIDEO_PROFILE_MIN
            profile_pause_max = NO_VIDEO_PROFILE_MAX
            consecutive_empty = 0
            append_run_log(run_log_path, {"handle": handle, "status": "no_videos",
                                          "reason": str(e)})

        except RetryLaterError as e:
            print(f"  ↺ Retry-later: {e}")
            retry_later.add(handle)
            save_checkpoint(done, retry_later)
            cooldown_pause("Midlertidig TikTok-fejl")
            consecutive_empty += 1
            append_run_log(run_log_path, {"handle": handle, "status": "retry_later",
                                          "reason": str(e)})

        except Exception as e:
            print(f"  ✗ Uventet fejl: {e}")
            retry_later.add(handle)
            save_checkpoint(done, retry_later)
            cooldown_pause("Uventet fejl")
            consecutive_empty += 1
            append_run_log(run_log_path, {"handle": handle, "status": "error",
                                          "reason": str(e)})

        session_count += 1

        if consecutive_empty >= 3:
            cooldown_pause("Flere profiler i træk uden brugbart output", 600, 1200)
            consecutive_empty = 0

        if session_count % COOLDOWN_EVERY_N_PROFILES == 0:
            cooldown_pause("Planlagt cooldown")

        human_pause(profile_pause_min, profile_pause_max, "mellem profiler")

    print(f"\n{'═' * 54}")
    print(f"Færdig!  Run-id: {run_id}")
    if videos_out.exists():
        n = count_csv_rows(videos_out)
        print(f"  {n:>6} videoer  →  {videos_out}")
    if profiles_out.exists():
        n = count_csv_rows(profiles_out)
        print(f"  {n:>6} profiler →  {profiles_out}")
    print(f"  Kørsellog      →  {run_log_path}")

    if not retry_later and RETRY_LATER_FILE.exists():
        RETRY_LATER_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
