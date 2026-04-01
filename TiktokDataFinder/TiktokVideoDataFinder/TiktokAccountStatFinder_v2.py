"""
TiktokAccountStatFinder v2
Improvements over v1:
  - Paths resolved relative to script location (no hardcoded user paths)
  - Configurable via env vars (TIKTOK_OUTDIR, TIKTOK_BROWSER, etc.)
  - Per-session run log written to OUTDIR/run_logs/
  - Progress summary printed live (videos / profile, cumulative totals)
  - Duplicate video-id guard is now global across profiles, not just per-profile
  - Profile-stats fallback chain is more robust (userInfo → authorStats → None)
  - get_profile_stats now retries up to 2x with back-off before giving up
  - Videos with missing createTime are kept but flagged instead of silently skipped
  - cutoff uses timedelta(days=…) rounded to midnight for reproducibility
  - Output CSVs include a run_id column so multiple runs can be merged safely
  - --dry-run flag: load profiles & print plan without launching browser
  - --only-handles flag: scrape a comma-separated list of handles only
  - Cleaner separation: scraping logic in scrape_profile(), I/O in main()
  - Danish console output preserved; English inline comments added
"""

import argparse
import csv
import json
import logging
import os
import random
import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# ─────────────────────────── paths ───────────────────────────
_HERE = Path(__file__).parent

def _env_path(var, default: Path) -> Path:
    v = os.environ.get(var)
    return Path(v) if v else default

CANDIDATES_CSV    = _env_path("TIKTOK_CANDIDATES_CSV",  _HERE / "candidates_tiktok_accounts.csv")
PARTIES_CSV       = _env_path("TIKTOK_PARTIES_CSV",     _HERE / "party_tiktok_accounts_from_claude.csv")
OUTDIR            = _env_path("TIKTOK_OUTDIR",          _HERE / "tiktok_data")
CHECKPOINT_FILE   = OUTDIR / "tiktok_collect_checkpoint.json"
RETRY_LATER_FILE  = OUTDIR / "tiktok_retry_later.json"

# ─────────────────────────── tunable constants ───────────────────────────
BROWSER           = os.environ.get("TIKTOK_BROWSER", "chrome")
MONTHS_BACK       = int(os.environ.get("TIKTOK_MONTHS", "6"))
MAX_SCROLLS       = int(os.environ.get("TIKTOK_MAX_SCROLLS", "40"))

BETWEEN_SCROLL_MIN          = 5.0
BETWEEN_SCROLL_MAX          = 7.0
BETWEEN_VIDEO_FETCH_MIN     = 0.8
BETWEEN_VIDEO_FETCH_MAX     = 2.0
BETWEEN_PROFILE_MIN         = 60.0
BETWEEN_PROFILE_MAX         = 120.0
COOLDOWN_EVERY_N_PROFILES   = 10
COOLDOWN_MIN_SECONDS        = 15
COOLDOWN_MAX_SECONDS        = 60
MAX_RECOVERY_ATTEMPTS       = 1
MAX_STAGNANT_SCROLLS        = 2           # unused directly — kept for clarity
MAX_CONSECUTIVE_EMPTY       = 3
PROFILE_STATS_RETRIES       = 2           # NEW: retry get_profile_stats this many times

NO_NEW_LINKS_STOP           = 3
MIN_EXPECTED_NEW_LINKS      = 2
MAX_SMALL_GROWTH_ROUNDS     = 4

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


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════
def parse_args():
    p = argparse.ArgumentParser(description="Hent TikTok-statistik for politikere og partier")
    p.add_argument("--candidates",    default=str(CANDIDATES_CSV))
    p.add_argument("--parties",       default=str(PARTIES_CSV))
    p.add_argument("--browser",       default=BROWSER, choices=["chrome", "edge", "firefox"])
    p.add_argument("--months",        type=int, default=MONTHS_BACK)
    p.add_argument("--reset",         action="store_true",  help="Start forfra (slet checkpoint)")
    p.add_argument("--max-profiles",  type=int, default=None, help="Maks antal profiler i denne kørsel")
    p.add_argument("--dry-run",       action="store_true",  help="Vis plan uden at åbne browser")   # NEW
    p.add_argument("--only-handles",  default=None,         help="Kommasepareret liste af handles")  # NEW
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

    # — candidates —
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

    # — parties / organisations —
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

    # deduplicate by handle (first occurrence wins)
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
    """Append rows (list of dicts) to a CSV, writing header only on first write."""
    if not rows:
        return
    cols = list(rows[0].keys())
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(rows)


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
    """Try several known JSON shapes that TikTok has returned historically."""
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
    item  = _get_item_struct(data)
    stats = item.get("stats", {}) if isinstance(item, dict) else {}
    video_id = str(item.get("id", ""))
    ts       = _parse_ts(item.get("createTime"))

    # NEW: flag videos where createTime is missing instead of silently dropping
    date_str = ts.strftime("%Y-%m-%d") if ts else "UNKNOWN"

    return {
        "run_id":         run_id,          # NEW: lets you merge runs later
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
    """Pull follower / like / video counts from an authorStats block."""
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
    """
    Fetch account-level stats.  Retries up to PROFILE_STATS_RETRIES times
    with exponential back-off before returning None.
    """
    url = f"https://www.tiktok.com/@{profile['handle']}"
    for attempt in range(1, PROFILE_STATS_RETRIES + 2):
        try:
            data = pyk.alt_get_tiktok_json(url)
            if not data:
                break
            # Try multiple known locations for the stats object
            stats = (
                data.get("userInfo", {}).get("stats")
                or data.get("stats")
                or {}
            )
            if not stats:
                # Some responses nest it differently
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
            time.sleep(2 ** attempt)   # 2s, 4s …
    return None


# ═══════════════════════════════════════════════════════════════
# Browser / Selenium helpers
# ═══════════════════════════════════════════════════════════════
def build_driver(browser):
    from selenium import webdriver
    from selenium.webdriver.chrome.options  import Options as ChromeOptions
    from selenium.webdriver.edge.options    import Options as EdgeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions

    if browser == "chrome":
        opts = ChromeOptions()
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--start-maximized")
        return webdriver.Chrome(options=opts)
    if browser == "edge":
        opts = EdgeOptions()
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--start-maximized")
        return webdriver.Edge(options=opts)
    # firefox
    opts = FirefoxOptions()
    return webdriver.Firefox(options=opts)


def human_pause(min_s, max_s, reason=None):
    dur = random.uniform(min_s, max_s)
    if reason:
        print(f"    Pause {dur:.1f}s ({reason})")
    time.sleep(dur)


def cooldown_pause(reason, min_s=COOLDOWN_MIN_SECONDS, max_s=COOLDOWN_MAX_SECONDS):
    dur = random.uniform(min_s, max_s)
    print(f"\n⏸ {reason}. Venter {dur/60:.1f} min.\n")
    time.sleep(dur)


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
    try:
        page = (driver.page_source or "").lower()
    except Exception:
        return True
    error_markers = [
        "something went wrong",
        "please try again later",
        "network issue",
        "too many attempts",
        "maximum number of attempts",
    ]
    if any(m in page for m in error_markers):
        # Double-check: if video links exist the page is probably fine
        if handle:
            try:
                if _extract_video_urls_from_page(driver, handle):
                    return False
            except Exception:
                pass
        return True
    return False


def wait_for_profile_content(driver, handle, timeout=20) -> bool:
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


def recover_profile_page(driver, profile_url, handle) -> bool:
    for attempt in range(1, MAX_RECOVERY_ATTEMPTS + 1):
        print(f"    Recovery-forsøg {attempt}/{MAX_RECOVERY_ATTEMPTS}")
        human_pause(8, 12, "før refresh")
        try:
            driver.get(profile_url)
        except Exception:
            pass
        human_pause(4, 6, "efter refresh")
        if wait_for_profile_content(driver, handle, timeout=18):
            return True
        if not tiktok_error_visible(driver, handle=handle):
            return True
    return False


# ═══════════════════════════════════════════════════════════════
# Core scraper
# ═══════════════════════════════════════════════════════════════
def scrape_profile(pyk, profile, cutoff, raw_dir, browser, run_id,
                   global_seen_video_ids: set):      # NEW: shared dedup set
    """
    Scroll through a TikTok profile and collect video rows for videos
    posted after `cutoff`.

    Returns (list_of_rows, first_item_dict_or_None).
    Side-effect: adds collected video IDs to global_seen_video_ids.
    """
    handle      = profile["handle"]
    raw_path    = raw_dir / f"{handle}_raw.csv"
    seen_urls   = set()
    all_rows    = []
    first_item  = None
    old_video_seen      = False
    stagnant_scrolls    = 0
    previous_count      = 0
    recovery_failures   = 0
    small_growth_rounds = 0
    recovery_used       = 0

    driver = build_driver(browser)
    try:
        profile_url = f"https://www.tiktok.com/@{handle}"
        driver.get(profile_url)
        human_pause(6, 10, "efter profilåbning")

        loaded_ok = wait_for_profile_content(driver, handle, timeout=20)
        if not loaded_ok and tiktok_error_visible(driver, handle=handle):
            if not recover_profile_page(driver, profile_url, handle):
                raise RetryLaterError("TikTok-side viste fejl fra start")

        for scroll_idx in range(MAX_SCROLLS):
            if tiktok_error_visible(driver, handle=handle):
                if not recover_profile_page(driver, profile_url, handle):
                    raise RetryLaterError("TikTok-side viste fejl flere gange i træk")
                recovery_failures += 1
                if recovery_failures >= MAX_RECOVERY_ATTEMPTS:
                    raise RetryLaterError("For mange recovery-forsøg")

            urls     = _extract_video_urls_from_page(driver, handle)
            new_urls = [u for u in urls if u not in seen_urls]

            for url in new_urls:
                seen_urls.add(url)
                human_pause(BETWEEN_VIDEO_FETCH_MIN, BETWEEN_VIDEO_FETCH_MAX, "mellem video-metadata")
                try:
                    data = pyk.alt_get_tiktok_json(url)
                except Exception as e:
                    print(f"    ⚠ Metadata-fejl for {url}: {e}")
                    continue

                row, ts, item = _video_row_from_json(data, profile, url, run_id)
                video_id = row["video_id"]

                # Global dedup: skip if we've already seen this video in any profile
                if not video_id or video_id in global_seen_video_ids:
                    continue
                global_seen_video_ids.add(video_id)

                if first_item is None and item:
                    first_item = item

                # Keep videos without a parseable date, but don't stop on them
                if ts is not None and ts < cutoff:
                    old_video_seen = True
                    continue

                all_rows.append(row)

            # Persist raw progress after each scroll
            if all_rows:
                pd.DataFrame(all_rows).to_csv(raw_path, index=False, encoding="utf-8-sig")

            print(
                f"    Scroll {scroll_idx + 1:>2}: "
                f"{len(new_urls):>3} nye links | "
                f"{len(all_rows):>3} videoer i perioden"
            )

            if old_video_seen:
                print("    Fandt video ældre end cutoff — stopper")
                break

            if len(seen_urls) == previous_count:
                stagnant_scrolls += 1
            else:
                stagnant_scrolls  = 0
                recovery_failures = 0
            previous_count = len(seen_urls)

            if len(new_urls) < MIN_EXPECTED_NEW_LINKS:
                small_growth_rounds += 1
            else:
                small_growth_rounds = 0

            if stagnant_scrolls >= NO_NEW_LINKS_STOP:
                print("    Ingen nye links efter flere scrolls — stopper")
                break

            if small_growth_rounds >= MAX_SMALL_GROWTH_ROUNDS:
                print("    Meget lille vækst over flere scrolls — stopper")
                break

            if (stagnant_scrolls >= 2
                    and len(all_rows) >= 20
                    and recovery_used < MAX_RECOVERY_ATTEMPTS):
                print("    Forsøger ét enkelt recovery-scroll")
                recovery_used += 1
                if recover_profile_page(driver, profile_url, handle):
                    stagnant_scrolls = 0
                    continue

            scroll_px = random.randint(650, 1250)
            driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_px)
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

    # Unique identifier for this run — included in every output row
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

    # — pyktok —
    try:
        import pyktok as pyk
    except ImportError:
        print("pyktok ikke installeret. Kør:\n  pip install pyktok pandas tqdm selenium")
        return

    # — load profiles —
    all_profiles = load_profiles(args.candidates, args.parties)

    # Optional filter: --only-handles handle1,handle2
    if args.only_handles:
        wanted = {h.strip().lstrip("@") for h in args.only_handles.split(",")}
        all_profiles = [p for p in all_profiles if p["handle"] in wanted]
        print(f"--only-handles: filtreret til {len(all_profiles)} profil(er)\n")

    print(f"Profiler i alt: {len(all_profiles)}\n")

    # cutoff rounded to midnight for reproducible behaviour
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

    # — browser cookies —
    print(f"Initialiserer med {args.browser}-cookies...")
    try:
        pyk.specify_browser(args.browser)
        print("OK\n")
    except Exception as e:
        print(f"⚠ Cookie-fejl ({e}) — forsøger alligevel\n")

    done, retry_later = load_checkpoint(args.reset)

    todo        = [p for p in all_profiles if p["handle"] not in done and p["handle"] not in retry_later]
    retry_queue = [p for p in all_profiles if p["handle"] in retry_later and p["handle"] not in done]
    todo.extend(retry_queue)

    if args.max_profiles is not None:
        todo = todo[: args.max_profiles]

    print(f"Tilbage i denne kørsel: {len(todo)} | Allerede hentet: {len(done)} | Retry-later: {len(retry_later)}\n")
    print("Kører forsigtigt med pauser og retry-logik.\n")

    run_log_path = init_run_log(log_dir, run_id, len(todo))

    # Shared global dedup set — avoids duplicate video rows across profiles
    global_seen_video_ids: set = set()

    session_video_total  = 0
    session_count        = 0
    consecutive_empty    = 0

    for profile in tqdm(todo, desc="Profiler", unit="profil"):
        handle = profile["handle"]
        print(f"\n{'─' * 54}")
        print(f"@{handle}  [{profile['account_type']}]  {profile['party_name']}")

        try:
            video_rows, first_item = scrape_profile(
                pyk, profile, cutoff, raw_dir, args.browser,
                run_id, global_seen_video_ids,
            )
            append_rows(video_rows, videos_out)
            session_video_total += len(video_rows)
            print(f"  ✓ {len(video_rows)} videoer gemt  (total denne kørsel: {session_video_total})")

            consecutive_empty = 0 if video_rows else consecutive_empty + 1

            # Profile-level stats: try dedicated fetch first, fall back to video item
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

        except RetryLaterError as e:
            print(f"  ↺ Retry-later: {e}")
            retry_later.add(handle)
            save_checkpoint(done, retry_later)
            cooldown_pause("Midlertidig TikTok-fejl")
            consecutive_empty += 1
            append_run_log(run_log_path, {"handle": handle, "status": "retry_later", "reason": str(e)})

        except Exception as e:
            print(f"  ✗ Uventet fejl: {e}")
            retry_later.add(handle)
            save_checkpoint(done, retry_later)
            cooldown_pause("Uventet fejl")
            consecutive_empty += 1
            append_run_log(run_log_path, {"handle": handle, "status": "error", "reason": str(e)})

        session_count += 1

        if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
            cooldown_pause("Flere profiler i træk uden brugbart output", 600, 1200)
            consecutive_empty = 0

        if session_count % COOLDOWN_EVERY_N_PROFILES == 0:
            cooldown_pause("Planlagt cooldown")

        human_pause(BETWEEN_PROFILE_MIN, BETWEEN_PROFILE_MAX, "mellem profiler")

    # — final summary —
    print(f"\n{'═' * 54}")
    print(f"Færdig!  Run-id: {run_id}")
    if videos_out.exists():
        n = len(pd.read_csv(videos_out, encoding="utf-8-sig"))
        print(f"  {n:>6} videoer  →  {videos_out}")
    if profiles_out.exists():
        n = len(pd.read_csv(profiles_out, encoding="utf-8-sig"))
        print(f"  {n:>6} profiler →  {profiles_out}")
    print(f"  Kørsellog      →  {run_log_path}")

    # Clean up retry file if everything succeeded
    if not retry_later and RETRY_LATER_FILE.exists():
        RETRY_LATER_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
