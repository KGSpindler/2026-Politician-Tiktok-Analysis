#WRITTEN BY CLAUDE

"""
clean_tiktok_results.py
───────────────────────
Cleans candidate_tiktok_best.csv and outputs a lean, readable file with:
  - candidate_name, party_name, storkreds
  - tiktok_handle, tiktok_url
  - handle_score, name_score, party_score, context_score, total_score
  - status

Cleaning steps:
  1. Drop columns not needed (query, engine, snippet, title, etc.)
  2. Strip trailing underscores from handles (scraping artefact)
  3. Rebuild tiktok_url from cleaned handle
  4. Reclassify search_error rows with no result as no_match
  5. Sort: high_confidence first, then by total_score desc

Usage:
    python clean_tiktok_results.py
    python clean_tiktok_results.py --input my_best.csv --output my_clean.csv
"""

import argparse
import re
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean TikTok candidate match results")
    p.add_argument("--input",  default=r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\Candidate Tiktok Profile Matching + Quick Cleaning\candidate_tiktok_best.csv")
    p.add_argument("--output", default=r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\Candidate Tiktok Profile Matching + Quick Cleaning\candidate_tiktok_clean.csv")
    return p.parse_args()


# Status order for sorting
STATUS_ORDER = {
    "high_confidence": 0,
    "possible_match":  1,
    "manual_review":   2,
    "no_match":        3,
    "search_error":    4,
}


def clean(df: pd.DataFrame) -> pd.DataFrame:

    # ── 1. Keep only the columns we want ─────────────────────────────────────
    keep = [
        "candidate_name", "party_name", "storkreds",
        "profile_handle", "profile_url",
        "handle_score", "name_score", "party_score", "context_score", "total_score",
        "status",
    ]
    df = df[keep].copy()

    # ── 2. Rename for clarity ─────────────────────────────────────────────────
    df = df.rename(columns={
        "profile_handle": "tiktok_handle",
        "profile_url":    "tiktok_url",
    })

    # ── 3. Normalise handle: strip trailing underscores and whitespace ────────
    #    TikTok handles can't actually end in underscore — it's a scraping artefact.
    df["tiktok_handle"] = (
        df["tiktok_handle"]
        .fillna("")
        .str.strip()
        .str.rstrip("_")
        .str.lower()
    )

    # ── 4. Rebuild tiktok_url from cleaned handle ─────────────────────────────
    has_handle = df["tiktok_handle"] != ""
    df.loc[has_handle, "tiktok_url"] = (
        "https://www.tiktok.com/@" + df.loc[has_handle, "tiktok_handle"]
    )
    df.loc[~has_handle, "tiktok_url"] = ""

    # ── 5. Reclassify search_error rows that found nothing as no_match ────────
    #    search_error with an empty handle means the search engine failed,
    #    not that we found a wrong account — treat them the same as no_match.
    is_error_with_no_result = (df["status"] == "search_error") & (df["tiktok_handle"] == "")
    df.loc[is_error_with_no_result, "status"] = "no_match"

    # ── 6. Zero out scores for rows with no result ────────────────────────────
    score_cols = ["handle_score", "name_score", "party_score", "context_score", "total_score"]
    no_result  = df["tiktok_handle"] == ""
    df.loc[no_result, score_cols] = 0

    # ── 7. Sort: by status priority, then total_score descending ─────────────
    df["_sort"] = df["status"].map(STATUS_ORDER).fillna(9)
    df = df.sort_values(["_sort", "total_score"], ascending=[True, False])
    df = df.drop(columns=["_sort"]).reset_index(drop=True)

    return df


def print_summary(df: pd.DataFrame) -> None:
    print(f"\n{'─'*40}")
    print(f"Total candidates:  {len(df)}")
    print(f"\nStatus breakdown:")
    for status, cnt in df["status"].value_counts().items():
        pct = 100 * cnt / len(df)
        print(f"  {status:<20} {cnt:>4}  ({pct:.1f}%)")

    hc = df[df["status"] == "high_confidence"]
    if not hc.empty:
        print(f"\nTop 10 high-confidence matches:")
        for _, row in hc.head(10).iterrows():
            print(f"  {row['candidate_name']:<35} @{row['tiktok_handle']:<30} score={row['total_score']}")
    print(f"{'─'*40}\n")


def main() -> None:
    args   = parse_args()
    in_path  = Path(args.input)
    out_path = Path(args.output)

    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Reading:  {in_path}")
    df = pd.read_csv(in_path, encoding="utf-8-sig")
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    df = clean(df)

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Written:  {out_path}")
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    print_summary(df)


if __name__ == "__main__":
    main()
