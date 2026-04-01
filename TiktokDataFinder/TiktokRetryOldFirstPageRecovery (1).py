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

CANDIDATES_CSV = r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\TiktokDataFinder\TiktokVideoDataFinder\candidates_tiktok_accounts.csv"
PARTIES_CSV = r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\TiktokDataFinder\TiktokVideoDataFinder\party_tiktok_accounts_from_claude.csv"

BROWSER = "chrome"
MONTHS_BACK = 6
MAX_SCROLLS = 40
OUTDIR = Path(r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\TiktokDataFinder\TiktokVideoDataFinder\tiktok_data")
VIDEOS_OUT = OUTDIR / "tiktok_videos.csv"
PROFILES_OUT = OUTDIR / "tiktok_profiles.csv"
RAW_DIR = OUTDIR / "raw"
RECOVERY_LOG = OUTDIR / "retry_old_first_page_log.csv"

BETWEEN_SCROLL_MIN = 5.0
BETWEEN_SCROLL_MAX = 7.0
BETWEEN_VIDEO_FETCH_MIN = 0.8
BETWEEN_VIDEO_FETCH_MAX = 2.0
BETWEEN_PROFILE_MIN = 45.0
BETWEEN_PROFILE_MAX = 120.0

NO_NEW_LINKS_STOP = 3
MIN_EXPECTED_NEW_LINKS = 2
MAX_SMALL_GROWTH_ROUNDS = 4
DEFAULT_MAX_EXISTING_VIDEOS = 32
MIN_EXISTING_VIDEOS_TO_CHECK = 30


class RetryLaterError(Exception):
    pass


def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Retry only likely false-stopped accounts: accounts with a small number of saved videos, "
            "where the first visible page contains both recent and older videos."
        )
    )
    p.add_argument("--candidates", default=CANDIDATES_CSV)
    p.add_argument("--parties", default=PARTIES_CSV)
    p.add_argument("--browser", default=BROWSER, choices=["chrome", "edge", "firefox"])
    p.add_argument("--months", type=int, default=MONTHS_BACK)
    p.add_argument(
        "--max-existing-videos",
        type=int,
        default=DEFAULT_MAX_EXISTING_VIDEOS,
        help="Only consider accounts that currently have between 1 and this many saved videos.",
    )
    p.add_argument(
        "--probe-only",
        action="store_true",
        help="Only identify which accounts should be retried; do not rewrite datasets.",
    )
    p.add_argument(
        "--max-handles",
        type=int,
        default=None,
        help="Optional cap on how many flagged accounts to process.",
    )
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
    for profile in profiles:
        if profile["handle"] not in seen:
            seen.add(profile["handle"])
            unique.append(profile)
    return unique


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


def tiktok_error_visible(driver):
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
    return any(marker in page for marker in error_markers)


def wait_for_profile_content(driver, handle, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urls = _extract_video_urls_from_page(driver, handle)
            if urls:
                return True
        except Exception:
            pass
        if tiktok_error_visible(driver):
            return False
        time.sleep(1.0)
    return False


def load_existing_video_counts(videos_out):
    if not videos_out.exists():
        raise FileNotFoundError(f"Mangler samlet videofil: {videos_out}")
    df = pd.read_csv(videos_out, encoding="utf-8-sig")
    if "tiktok_handle" not in df.columns:
        raise ValueError(f"Kolonnen 'tiktok_handle' findes ikke i {videos_out}")
    counts = df.groupby("tiktok_handle").size().to_dict()
    return df, counts


def probe_first_page_old_vs_recent(pyk, profile, cutoff, browser):
    handle = profile["handle"]
    driver = build_driver(browser)
    try:
        profile_url = f"https://www.tiktok.com/@{handle}"
        driver.get(profile_url)
        human_pause(6, 10, "efter profilåbning")

        loaded_ok = wait_for_profile_content(driver, handle, timeout=20)
        if not loaded_ok:
            raise RetryLaterError("Kunne ikke loade profilen stabilt til first-page probe")

        urls = _extract_video_urls_from_page(driver, handle)
        if not urls:
            return {
                "should_retry": False,
                "reason": "Ingen videolinks på første side",
                "first_page_links": 0,
                "recent_first_page": 0,
                "old_first_page": 0,
            }

        recent_count = 0
        old_count = 0
        metadata_rows = []

        for url in urls:
            human_pause(0.8, 1.5, "mellem probe-metadata")
            try:
                data = pyk.alt_get_tiktok_json(url)
            except Exception as e:
                print(f"    ⚠ Probe kunne ikke hente metadata for {url}: {e}")
                continue

            row, ts, _ = _video_row_from_json(data, profile, url)
            if ts is None:
                continue

            metadata_rows.append((url, ts, row.get("upload_dato", "")))
            if ts < cutoff:
                old_count += 1
            else:
                recent_count += 1

        should_retry = recent_count > 0 and old_count > 0
        reason = "Både gamle og nye videoer på første side" if should_retry else "Ingen tydelig pinned-old situation"

        return {
            "should_retry": should_retry,
            "reason": reason,
            "first_page_links": len(urls),
            "recent_first_page": recent_count,
            "old_first_page": old_count,
            "first_page_examples": metadata_rows[:8],
        }
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def scrape_profile_ignore_old_first_page(pyk, profile, cutoff, raw_dir, browser):
    handle = profile["handle"]
    raw_path = raw_dir / f"{handle}_raw.csv"
    seen_urls = set()
    seen_video_ids = set()
    all_rows = []
    first_item_for_profile_stats = None
    old_video_seen_after_first_scroll = False
    stagnant_scrolls = 0
    previous_count = 0
    small_growth_rounds = 0

    driver = build_driver(browser)
    try:
        profile_url = f"https://www.tiktok.com/@{handle}"
        driver.get(profile_url)
        human_pause(6, 10, "efter profilåbning")

        loaded_ok = wait_for_profile_content(driver, handle, timeout=20)
        if not loaded_ok:
            raise RetryLaterError("Kunne ikke loade profilen stabilt til retry-scrape")

        for scroll_idx in range(MAX_SCROLLS):
            urls = _extract_video_urls_from_page(driver, handle)
            new_urls = [u for u in urls if u not in seen_urls]

            old_seen_this_round = False

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
                    if scroll_idx > 0:
                        old_seen_this_round = True
                    continue

                all_rows.append(row)

            if all_rows:
                pd.DataFrame(all_rows).to_csv(raw_path, index=False, encoding="utf-8-sig")

            print(
                f"    Scroll {scroll_idx + 1}: {len(new_urls)} nye links | {len(all_rows)} videoer inden for perioden"
            )

            if old_seen_this_round:
                old_video_seen_after_first_scroll = True

            if old_video_seen_after_first_scroll:
                print("    Fandt gammel video efter første side — stopper")
                break

            if len(seen_urls) == previous_count:
                stagnant_scrolls += 1
            else:
                stagnant_scrolls = 0
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

            scroll_px = random.randint(650, 1250)
            driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_px)
            human_pause(BETWEEN_SCROLL_MIN, BETWEEN_SCROLL_MAX, "mellem scrolls")

        return all_rows, first_item_for_profile_stats
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def replace_handle_rows(csv_path, handle, new_rows):
    if csv_path.exists():
        existing = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "tiktok_handle" in existing.columns:
            existing = existing[existing["tiktok_handle"] != handle].copy()
        else:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    new_df = pd.DataFrame(new_rows)
    if not existing.empty and not new_df.empty:
        all_cols = list(dict.fromkeys(list(existing.columns) + list(new_df.columns)))
        existing = existing.reindex(columns=all_cols)
        new_df = new_df.reindex(columns=all_cols)
        out = pd.concat([existing, new_df], ignore_index=True)
    elif not existing.empty:
        out = existing
    else:
        out = new_df

    out.to_csv(csv_path, index=False, encoding="utf-8-sig")


def upsert_profile_stats(csv_path, stats_row):
    handle = stats_row["tiktok_handle"]
    if csv_path.exists():
        existing = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "tiktok_handle" in existing.columns:
            existing = existing[existing["tiktok_handle"] != handle].copy()
        else:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    new_df = pd.DataFrame([stats_row])
    if not existing.empty:
        all_cols = list(dict.fromkeys(list(existing.columns) + list(new_df.columns)))
        existing = existing.reindex(columns=all_cols)
        new_df = new_df.reindex(columns=all_cols)
        out = pd.concat([existing, new_df], ignore_index=True)
    else:
        out = new_df

    out.to_csv(csv_path, index=False, encoding="utf-8-sig")


def append_log_row(path, row):
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def main():
    args = parse_args()

    OUTDIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(exist_ok=True)

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

    cutoff = datetime.now() - timedelta(days=30 * args.months)
    print(f"Cutoff: {cutoff.strftime('%Y-%m-%d')}\n")

    profiles = load_profiles(args.candidates, args.parties)
    by_handle = {p["handle"]: p for p in profiles}

    videos_df, counts = load_existing_video_counts(VIDEOS_OUT)
    candidate_handles = [
        handle
        for handle, count in counts.items()
        if MIN_EXISTING_VIDEOS_TO_CHECK <= count <= args.max_existing_videos and handle in by_handle
    ]
    candidate_handles = sorted(candidate_handles, key=lambda h: (counts[h], h))

    if args.max_handles is not None:
        candidate_handles = candidate_handles[: args.max_handles]

    print(
        f"Kandidater til probe: {len(candidate_handles)} profiler "
        f"(har mellem {MIN_EXISTING_VIDEOS_TO_CHECK} og {args.max_existing_videos} gemte videoer).\n"
    )

    flagged = []

    for handle in tqdm(candidate_handles, desc="Probe", unit="profil"):
        profile = by_handle[handle]
        existing_count = counts.get(handle, 0)

        print(f"\n{'─' * 52}")
        print(f"Prober @{handle} | nuværende videoer i datasæt: {existing_count}")

        try:
            probe = probe_first_page_old_vs_recent(pyk, profile, cutoff, args.browser)
            log_row = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "tiktok_handle": handle,
                "display_name": profile["display_name"],
                "existing_video_count": existing_count,
                "first_page_links": probe.get("first_page_links", 0),
                "recent_first_page": probe.get("recent_first_page", 0),
                "old_first_page": probe.get("old_first_page", 0),
                "should_retry": int(bool(probe.get("should_retry"))),
                "reason": probe.get("reason", ""),
            }
            append_log_row(RECOVERY_LOG, log_row)

            if probe.get("should_retry"):
                flagged.append(handle)
                print(
                    f"  ✓ Flagged: {probe['recent_first_page']} nye og {probe['old_first_page']} gamle på første side"
                )
            else:
                print(f"  - Ikke flagged: {probe['reason']}")

        except Exception as e:
            print(f"  ✗ Probe-fejl: {e}")
            append_log_row(
                RECOVERY_LOG,
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "tiktok_handle": handle,
                    "display_name": profile["display_name"],
                    "existing_video_count": existing_count,
                    "first_page_links": "",
                    "recent_first_page": "",
                    "old_first_page": "",
                    "should_retry": "",
                    "reason": f"PROBE_ERROR: {e}",
                },
            )

        human_pause(BETWEEN_PROFILE_MIN, BETWEEN_PROFILE_MAX, "mellem profiler")

    print(f"\nFlagged profiler: {len(flagged)}")
    if flagged:
        print(", ".join(f"@{h}" for h in flagged[:25]) + (" ..." if len(flagged) > 25 else ""))

    if args.probe_only:
        print("\nProbe-only valgt, så ingen filer er overskrevet.")
        return

    if not flagged:
        print("\nIngen profiler krævede retry.")
        return

    print("\nStarter retry-scrape for flagged profiler...\n")

    for handle in tqdm(flagged, desc="Retry", unit="profil"):
        profile = by_handle[handle]
        print(f"\n{'═' * 52}")
        print(f"Retry @{handle}")

        try:
            rows, first_item = scrape_profile_ignore_old_first_page(pyk, profile, cutoff, RAW_DIR, args.browser)
            replace_handle_rows(VIDEOS_OUT, handle, rows)
            print(f"  ✓ Overskrev videoer for @{handle}: {len(rows)} rækker")

            stats = get_profile_stats(pyk, profile)
            if not stats and first_item:
                stats = get_profile_stats_from_video_item(first_item, profile)
            if stats:
                upsert_profile_stats(PROFILES_OUT, stats)
                print(f"  ✓ Opdaterede profilstatistik for @{handle}")

            append_log_row(
                RECOVERY_LOG,
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "tiktok_handle": handle,
                    "display_name": profile["display_name"],
                    "existing_video_count": counts.get(handle, 0),
                    "first_page_links": "",
                    "recent_first_page": "",
                    "old_first_page": "",
                    "should_retry": 1,
                    "reason": f"RETRIED_OK new_count={len(rows)}",
                },
            )

        except Exception as e:
            print(f"  ✗ Retry-fejl for @{handle}: {e}")
            append_log_row(
                RECOVERY_LOG,
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "tiktok_handle": handle,
                    "display_name": profile["display_name"],
                    "existing_video_count": counts.get(handle, 0),
                    "first_page_links": "",
                    "recent_first_page": "",
                    "old_first_page": "",
                    "should_retry": 1,
                    "reason": f"RETRY_ERROR: {e}",
                },
            )

        human_pause(BETWEEN_PROFILE_MIN, BETWEEN_PROFILE_MAX, "mellem profiler")

    print("\nFærdig.")
    print(f"Log: {RECOVERY_LOG}")
    print(f"Video-output: {VIDEOS_OUT}")
    print(f"Profil-output: {PROFILES_OUT}")


if __name__ == "__main__":
    main()
