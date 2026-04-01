"""
tiktok_profile_stats.py
────────────────────────
Fetches follower count and total likes for a list of TikTok profiles.

Usage:
    python tiktok_profile_stats.py --input profiles.csv
    python tiktok_profile_stats.py --input profiles.csv --handle-col tiktok_handle

Input CSV must have a column with TikTok handles (with or without @).

Output:
    tiktok_profile_stats.csv

Requirements:
    pip install httpx pandas tqdm
"""

import argparse
import csv
import json
import random
import re
import time
from pathlib import Path

import httpx
import pandas as pd
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
OUTPUT_FILE  = Path("tiktok_profile_stats.csv")
SLEEP_MIN    = 1.0
SLEEP_MAX    = 2.5
TIMEOUT      = 12

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
}

# ─────────────────────────────────────────────────────────────────────────────
# FETCH STATS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_stats(client: httpx.Client, handle: str) -> dict:
    """
    Fetches follower count and total likes from a TikTok profile page.
    Parses the embedded JSON that TikTok includes in every profile page.
    """
    handle = handle.lstrip("@").strip()
    url    = f"https://www.tiktok.com/@{handle}"

    try:
        resp = client.get(url, timeout=TIMEOUT, follow_redirects=True)
        if resp.status_code == 404:
            return {"handle": handle, "followers": None, "total_likes": None, "status": "not_found"}
        resp.raise_for_status()
    except Exception as e:
        return {"handle": handle, "followers": None, "total_likes": None, "status": f"error: {e}"}

    # TikTok embeds stats in a <script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"> tag
    match = re.search(
        r'<script[^>]+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
        resp.text, re.DOTALL
    )
    if not match:
        # Fallback: look for stats in any JSON blob
        match = re.search(r'"followerCount":(\d+)', resp.text)
        if match:
            followers    = int(match.group(1))
            likes_match  = re.search(r'"heartCount":(\d+)', resp.text)
            total_likes  = int(likes_match.group(1)) if likes_match else None
            return {"handle": handle, "followers": followers, "total_likes": total_likes, "status": "ok"}
        return {"handle": handle, "followers": None, "total_likes": None, "status": "parse_error"}

    try:
        data  = json.loads(match.group(1))

        # Navigate the nested structure
        user_detail = (
            data.get("__DEFAULT_SCOPE__", {})
                .get("webapp.user-detail", {})
        )
        user_info = user_detail.get("userInfo", {})
        stats     = user_info.get("stats", {})

        followers   = stats.get("followerCount")
        total_likes = stats.get("heartCount", stats.get("diggCount"))

        if followers is None:
            return {"handle": handle, "followers": None, "total_likes": None, "status": "no_stats"}

        return {
            "handle":      handle,
            "followers":   followers,
            "total_likes": total_likes,
            "status":      "ok",
        }
    except Exception as e:
        return {"handle": handle, "followers": None, "total_likes": None, "status": f"parse_error: {e}"}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",      required=True, help="CSV file with TikTok handles")
    parser.add_argument("--handle-col", default=None,  help="Column name for handles (auto-detected if omitted)")
    args = parser.parse_args()

    df = pd.read_csv(args.input, encoding="utf-8-sig")

    # Auto-detect handle column
    if args.handle_col:
        handle_col = args.handle_col
    else:
        candidates = [c for c in df.columns if "handle" in c.lower() or "tiktok" in c.lower()]
        if not candidates:
            candidates = [df.columns[0]]
        handle_col = candidates[0]
        print(f"Using column: '{handle_col}'")

    handles = df[handle_col].dropna().tolist()
    handles = [str(h).strip().lstrip("@") for h in handles if str(h).strip()]
    print(f"Profiles to fetch: {len(handles)}\n")

    results = []

    with httpx.Client(headers=HEADERS) as client:
        for handle in tqdm(handles, desc="Fetching", unit="profile"):
            stats = fetch_stats(client, handle)

            # Merge with original row data if available
            orig = df[df[handle_col].astype(str).str.lstrip("@") == handle]
            if not orig.empty:
                row = orig.iloc[0].to_dict()
                row.update(stats)
            else:
                row = stats

            results.append(row)

            if stats["status"] == "ok":
                tqdm.write(
                    f"  @{handle:<30} "
                    f"followers={stats['followers']:,}  "
                    f"likes={stats['total_likes']:,}"
                )
            else:
                tqdm.write(f"  @{handle:<30} {stats['status']}")

            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    out_df = pd.DataFrame(results)

    # Move handle/followers/total_likes/status to front
    front = ["handle", "followers", "total_likes", "status"]
    cols  = front + [c for c in out_df.columns if c not in front]
    out_df = out_df[[c for c in cols if c in out_df.columns]]

    out_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\nSaved {len(out_df)} rows → {OUTPUT_FILE}")

    ok = out_df[out_df["status"] == "ok"]
    print(f"Successfully fetched: {len(ok)}/{len(out_df)}")


if __name__ == "__main__":
    main()
