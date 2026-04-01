import argparse
import csv
import json
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from tqdm import tqdm

CANDIDATES_CSV = r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\TiktokStatTracking\candidates_tiktok_accounts.csv"
PARTIES_CSV = r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\TiktokStatTracking\party_tiktok_accounts_from_claude.csv"

BROWSER = "chrome"
MONTHS_BACK = 6
MAX_SCROLLS = 40
OUTDIR = Path(r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\TiktokAccountData\tiktok_data")
CHECKPOINT_FILE = Path(r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\TiktokAccountData\itiktok_collect_checkpoint.json")
RETRY_LATER_FILE = Path(r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\TiktokAccountData\itiktok_retry_later.json")

# Conservative pacing / stability settings
BETWEEN_SCROLL_MIN = 5.0
BETWEEN_SCROLL_MAX = 7.0
BETWEEN_VIDEO_FETCH_MIN = 0.8
BETWEEN_VIDEO_FETCH_MAX = 2
BETWEEN_PROFILE_MIN = 60.0
BETWEEN_PROFILE_MAX = 120.0
COOLDOWN_EVERY_N_PROFILES = 10
COOLDOWN_MIN_SECONDS = 15
COOLDOWN_MAX_SECONDS = 60
MAX_RECOVERY_ATTEMPTS = 1
MAX_STAGNANT_SCROLLS = 2
MAX_CONSECUTIVE_EMPTY_PROFILES = 3

NO_NEW_LINKS_STOP = 3
MIN_EXPECTED_NEW_LINKS = 2
MAX_SMALL_GROWTH_ROUNDS = 4


class RetryLaterError(Exception):
    pass


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--candidates", default=CANDIDATES_CSV)
    p.add_argument("--parties", default=PARTIES_CSV)
    p.add_argument("--browser", default=BROWSER, choices=["chrome", "edge", "firefox"])
    p.add_argument("--months", type=int, default=MONTHS_BACK)
    p.add_argument("--reset", action="store_true", help="Start forfra")
    p.add_argument("--max-profiles", type=int, default=None, help="Valgfrit loft på antal profiler i denne kørsel")
    return p.parse_args()


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
        profiles.append(
            {
                "handle": row["tiktok_handle"].strip(),
                "display_name": row["candidate_name"],
                "party_name": row.get("party_name", ""),
                "storkreds": row.get("storkreds", ""),
                "account_type": "kandidat",
            }
        )

    p = pd.read_csv(parties_path, encoding="utf-8-sig")
    p_ok = p[p["tiktok_url"].notna() & p["tiktok_handle"].notna()].copy()

    for _, row in p_ok.iterrows():
        handle = str(row["tiktok_handle"]).strip()
        if not handle:
            continue
        profiles.append(
            {
                "handle": handle,
                "display_name": row.get("candidate_name", handle),
                "party_name": row.get("party_name", ""),
                "storkreds": row.get("storkreds", ""),
                "account_type": row.get("account_type", ""),
            }
        )

    seen, unique = set(), []
    for p in profiles:
        if p["handle"] not in seen:
            seen.add(p["handle"])
            unique.append(p)
    return unique


def _read_json_set(path):
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()


def _write_json_set(path, values):
    path.write_text(json.dumps(sorted(values), ensure_ascii=False, indent=2), encoding="utf-8")


def load_checkpoint(reset):
    if reset:
        CHECKPOINT_FILE.unlink(missing_ok=True)
        RETRY_LATER_FILE.unlink(missing_ok=True)
        print("Reset: starter forfra.\n")
        return set(), set()
    done = _read_json_set(CHECKPOINT_FILE)
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


def append_rows(rows, path):
    if not rows:
        return
    cols = list(rows[0].keys())
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if write_header:
            w.writeheader()
        w.writerows(rows)


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


def _get_item_struct(data):
    if not isinstance(data, dict):
        return {}
    return (
        data.get("itemInfo", {}).get("itemStruct")
        or data.get("__DEFAULT_SCOPE__", {}).get("webapp.video-detail", {}).get("itemInfo", {}).get("itemStruct")
        or data.get("aweme_detail")
        or {}
    )


def _video_row_from_json(data, profile, video_url):
    item = _get_item_struct(data)
    stats = item.get("stats", {}) if isinstance(item, dict) else {}
    video_id = str(item.get("id", ""))
    ts = _parse_ts(item.get("createTime"))
    return {
        "tiktok_handle": profile["handle"],
        "display_name": profile["display_name"],
        "party_name": profile["party_name"],
        "storkreds": profile["storkreds"],
        "account_type": profile["account_type"],
        "video_id": video_id,
        "upload_dato": ts.strftime("%Y-%m-%d") if ts else "",
        "beskrivelse": str(item.get("desc", ""))[:250],
        "visninger": _safe_int(stats.get("playCount")),
        "likes": _safe_int(stats.get("diggCount")),
        "kommentarer": _safe_int(stats.get("commentCount")),
        "shares": _safe_int(stats.get("shareCount")),
        "gemmer": _safe_int(stats.get("collectCount")),
        "video_url": video_url,
    }, ts, item


def get_profile_stats_from_video_item(item, profile):
    author_stats = item.get("authorStats", {}) if isinstance(item, dict) else {}
    return {
        "tiktok_handle": profile["handle"],
        "display_name": profile["display_name"],
        "party_name": profile["party_name"],
        "account_type": profile["account_type"],
        "følgere": author_stats.get("followerCount", ""),
        "samlet_likes": author_stats.get("heartCount", ""),
        "antal_videoer": author_stats.get("videoCount", ""),
        "hentet_dato": datetime.now().strftime("%Y-%m-%d"),
    }


def get_profile_stats(pyk, profile):
    try:
        data = pyk.alt_get_tiktok_json(f"https://www.tiktok.com/@{profile['handle']}")
        if not data:
            return None
        stats = data.get("userInfo", {}).get("stats") or data.get("stats") or {}
        return {
            "tiktok_handle": profile["handle"],
            "display_name": profile["display_name"],
            "party_name": profile["party_name"],
            "account_type": profile["account_type"],
            "følgere": stats.get("followerCount", ""),
            "samlet_likes": stats.get("heartCount", stats.get("diggCount", "")),
            "antal_videoer": stats.get("videoCount", ""),
            "hentet_dato": datetime.now().strftime("%Y-%m-%d"),
        }
    except Exception:
        return None


def build_driver(browser):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.edge.options import Options as EdgeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions

    if browser == "chrome":
        options = ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--start-maximized")
        return webdriver.Chrome(options=options)
    if browser == "edge":
        options = EdgeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--start-maximized")
        return webdriver.Edge(options=options)
    options = FirefoxOptions()
    return webdriver.Firefox(options=options)


def human_pause(min_seconds, max_seconds, reason=None):
    duration = random.uniform(min_seconds, max_seconds)
    if reason:
        print(f"    Pause {duration:.1f}s ({reason})")
    time.sleep(duration)


def cooldown_pause(reason, min_seconds=COOLDOWN_MIN_SECONDS, max_seconds=COOLDOWN_MAX_SECONDS):
    duration = random.uniform(min_seconds, max_seconds)
    print(f"\n⏸ {reason}. Venter {duration/60:.1f} minutter før fortsættelse.\n")
    time.sleep(duration)


def restart_driver(driver, browser, reason):
    print(f"    ↻ Genstarter browser ({reason})")
    try:
        if driver is not None:
            driver.quit()
    except Exception:
        pass
    human_pause(15, 30, "efter browser-genstart")
    return build_driver(browser)


def tiktok_error_visible(driver, handle=None):
    """
    Kun reelle TikTok-fejl skal trigge recovery. Den gamle version matchede også
    ordet "refresh", hvilket gav mange falske positiver.
    """
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
    marker_hit = any(marker in page for marker in error_markers)

    if handle:
        try:
            urls = _extract_video_urls_from_page(driver, handle)
        except Exception:
            urls = []
        if urls:
            return False

    return marker_hit


def wait_for_profile_content(driver, handle, timeout=20):
    """
    Giv profilen tid til at loade links. Returnerer True hvis vi ser mindst ét
    videolink, False hvis der dukker en reel fejl op eller timeout rammes.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urls = _extract_video_urls_from_page(driver, handle)
            if urls:
                return True
        except Exception:
            pass
        if tiktok_error_visible(driver, handle=handle):
            return False
        time.sleep(1.0)
    return False


def _extract_video_urls_from_page(driver, handle):
    anchors = driver.find_elements("css selector", "a[href*='/video/']")
    out = []
    seen = set()
    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
        except Exception:
            continue
        if not href:
            continue
        if f"/@{handle}/video/" not in href:
            continue
        href = href.split("?")[0]
        if href not in seen:
            seen.add(href)
            out.append(href)
    return out


def recover_profile_page(driver, profile_url, handle):
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
            # Siden er ikke en tydelig fejlside; giv den en chance til senere.
            return True
    return False


def scrape_profile(pyk, profile, cutoff, raw_dir, browser):
    handle = profile["handle"]
    raw_path = raw_dir / f"{handle}_raw.csv"
    seen_urls = set()
    seen_video_ids = set()
    all_rows = []
    first_item_for_profile_stats = None
    old_video_seen = False
    stagnant_scrolls = 0
    previous_count = 0
    recovery_failures = 0
    small_growth_rounds = 0
    recovery_used = 0

    driver = build_driver(browser)
    try:
        profile_url = f"https://www.tiktok.com/@{handle}"
        driver.get(profile_url)
        human_pause(6, 10, "efter profilåbning")

        loaded_ok = wait_for_profile_content(driver, handle, timeout=20)
        if not loaded_ok and tiktok_error_visible(driver, handle=handle):
            ok = recover_profile_page(driver, profile_url, handle)
            if not ok:
                raise RetryLaterError("TikTok-side viste fejl flere gange i træk")

        for scroll_idx in range(MAX_SCROLLS):
            if tiktok_error_visible(driver, handle=handle):
                ok = recover_profile_page(driver, profile_url, handle)
                if not ok:
                    raise RetryLaterError("TikTok-side viste fejl flere gange i træk")
                recovery_failures += 1
                if recovery_failures >= MAX_RECOVERY_ATTEMPTS:
                    raise RetryLaterError("For mange recovery-forsøg på samme profil")

            urls = _extract_video_urls_from_page(driver, handle)
            new_urls = [u for u in urls if u not in seen_urls]

            for url in new_urls:
                seen_urls.add(url)
                human_pause(BETWEEN_VIDEO_FETCH_MIN, BETWEEN_VIDEO_FETCH_MAX, "mellem video-metadata")
                try:
                    data = pyk.alt_get_tiktok_json(url)
                except Exception as e:
                    print(f"    ⚠ Kunne ikke hente metadata for {url}: {e}")
                    continue

                row, ts, item = _video_row_from_json(data, profile, url)
                video_id = row["video_id"]
                if not video_id or video_id in seen_video_ids:
                    continue
                seen_video_ids.add(video_id)

                if first_item_for_profile_stats is None and item:
                    first_item_for_profile_stats = item

                if ts and ts < cutoff:
                    old_video_seen = True
                    continue

                all_rows.append(row)

            if all_rows:
                pd.DataFrame(all_rows).to_csv(raw_path, index=False, encoding="utf-8-sig")

            print(
                f"    Scroll {scroll_idx + 1}: {len(new_urls)} nye links | {len(all_rows)} videoer inden for perioden"
            )

            if old_video_seen:
                print("    Fandt video ældre end cutoff — stopper")
                break

            if len(seen_urls) == previous_count:
                stagnant_scrolls += 1
            else:
                stagnant_scrolls = 0
                recovery_failures = 0
            previous_count = len(seen_urls)

            if len(new_urls) < MIN_EXPECTED_NEW_LINKS:
                small_growth_rounds += 1
            else:
                small_growth_rounds = 0

            if stagnant_scrolls >= NO_NEW_LINKS_STOP:
                print("    Ingen nye links efter flere scrolls — stopper tidligt")
                break

            if small_growth_rounds >= MAX_SMALL_GROWTH_ROUNDS:
                print("    Meget lille vækst over flere scrolls — stopper")
                break

            if stagnant_scrolls >= 2 and len(all_rows) >= 20 and recovery_used < MAX_RECOVERY_ATTEMPTS:
                print("    Forsøger ét enkelt recovery")
                recovery_used += 1
                ok = recover_profile_page(driver, profile_url, handle)
                if ok:
                    stagnant_scrolls = 0
                    continue

            scroll_px = random.randint(650, 1250)
            driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_px)
            human_pause(BETWEEN_SCROLL_MIN, BETWEEN_SCROLL_MAX, "mellem scrolls")

        return all_rows, first_item_for_profile_stats
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def main():
    args = parse_args()

    OUTDIR.mkdir(exist_ok=True)
    raw_dir = OUTDIR / "raw"
    raw_dir.mkdir(exist_ok=True)

    videos_out = OUTDIR / "tiktok_videos.csv"
    profiles_out = OUTDIR / "tiktok_profiles.csv"

    try:
        import pyktok as pyk
    except ImportError:
        print("pyktok ikke installeret. Kør:\n  pip install pyktok pandas tqdm selenium")
        return

    print(f"Initialiserer med {args.browser}-cookies...")
    try:
        pyk.specify_browser(args.browser)
        print("OK\n")
    except Exception as e:
        print(f"⚠ Cookie-fejl ({e}) — forsøger alligevel\n")

    profiles = load_profiles(args.candidates, args.parties)
    print(f"Profiler i alt: {len(profiles)}\n")

    cutoff = datetime.now() - timedelta(days=30 * args.months)
    print(f"Henter videoer efter: {cutoff.strftime('%Y-%m-%d')}\n")
    print("Denne version kører forsigtigt, laver pauser, markerer fejlprofiler til retry senere og stopper ved gentagne sidefejl.\n")

    done, retry_later = load_checkpoint(args.reset)
    todo = [p for p in profiles if p["handle"] not in done and p["handle"] not in retry_later]
    retry_queue = [p for p in profiles if p["handle"] in retry_later and p["handle"] not in done]
    todo.extend(retry_queue)

    if args.max_profiles is not None:
        todo = todo[: args.max_profiles]

    print(f"Tilbage i denne kørsel: {len(todo)} | Allerede hentet: {len(done)} | Retry-later: {len(retry_later)}\n")

    profiles_processed_this_session = 0
    consecutive_empty_profiles = 0

    for profile in tqdm(todo, desc="Profiler", unit="profil"):
        handle = profile["handle"]
        print(f"\n{'─' * 52}")
        print(f"@{handle}  [{profile['account_type']}]  {profile['party_name']}")

        try:
            video_rows, first_item = scrape_profile(pyk, profile, cutoff, raw_dir, args.browser)
            append_rows(video_rows, videos_out)
            print(f"  ✓ {len(video_rows)} videoer gemt")

            if len(video_rows) == 0:
                consecutive_empty_profiles += 1
            else:
                consecutive_empty_profiles = 0

            stats = get_profile_stats(pyk, profile)
            if not stats and first_item:
                stats = get_profile_stats_from_video_item(first_item, profile)
            if stats:
                append_rows([stats], profiles_out)

            done.add(handle)
            retry_later.discard(handle)
            save_checkpoint(done, retry_later)

        except KeyboardInterrupt:
            print("\n⚠ Afbrudt — gemmer checkpoint...")
            save_checkpoint(done, retry_later)
            print("Kør igen for at fortsætte.")
            return
        except RetryLaterError as e:
            print(f"  ↺ Markeret til retry senere: {e}")
            retry_later.add(handle)
            save_checkpoint(done, retry_later)
            cooldown_pause("Midlertidig TikTok-fejl")
            consecutive_empty_profiles += 1
        except Exception as e:
            print(f"  ✗ Uventet fejl: {e}")
            retry_later.add(handle)
            save_checkpoint(done, retry_later)
            cooldown_pause("Uventet fejl på profil")
            consecutive_empty_profiles += 1

        profiles_processed_this_session += 1

        if consecutive_empty_profiles >= MAX_CONSECUTIVE_EMPTY_PROFILES:
            cooldown_pause("Flere profiler i træk uden brugbart output", 600, 1200)
            consecutive_empty_profiles = 0

        if profiles_processed_this_session % COOLDOWN_EVERY_N_PROFILES == 0:
            cooldown_pause("Planlagt cooldown efter flere profiler")

        human_pause(BETWEEN_PROFILE_MIN, BETWEEN_PROFILE_MAX, "mellem profiler")

    print(f"\n{'═' * 52}")
    print("Færdig!")
    if videos_out.exists():
        n = len(pd.read_csv(videos_out, encoding="utf-8-sig"))
        print(f"  {n} videoer  →  {videos_out}")
    if profiles_out.exists():
        n = len(pd.read_csv(profiles_out, encoding="utf-8-sig"))
        print(f"  {n} profiler →  {profiles_out}")
    if CHECKPOINT_FILE.exists() and RETRY_LATER_FILE.exists() and not retry_later:
        RETRY_LATER_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
