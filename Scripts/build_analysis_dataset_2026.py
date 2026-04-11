#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build a clean 2026 analysis dataset for the Folketing + TikTok project.

What this script does
---------------------
1. Loads the official 2026 candidate list.
2. Loads candidate demographics from Altinget.
3. Loads curated candidate TikTok accounts and creates tiktok_exists.
4. Loads video-level TikTok data and builds pre-election activity measures.
5. Loads 2026 election results and aggregates them to candidate x municipality.
6. Optionally loads 2022 election results and builds prior-strength variables.
7. Loads municipality data (tidy long format) and widens the chosen pre-election year.
8. Saves two clean CSV files:
      - one row per candidate x municipality in 2026
      - one row per candidate in 2026

Important assumptions
---------------------
- This script expects the NEW file structure and the NEW tidy municipality format.
- Candidate matching is STRICT and exact on:
      candidate_name + party_name + storkreds
- TikTok existence is taken from the curated TikTok candidate list.
- Video windows exclude election day itself and end on the day before the election.
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
import warnings
from pathlib import Path
from typing import Iterable

import pandas as pd
import numpy as np


ELECTION_DAY_2026 = pd.Timestamp("2026-03-24")
PRE_ELECTION_END = ELECTION_DAY_2026 - pd.Timedelta(days=1)
PRE_ELECTION_YEAR = 2025  # municipality covariates are taken from the year before the election


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------

def norm_text(value: object) -> str:
    """Normalize text for exact matching while preserving Danish characters."""
    if pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    text = re.sub(r"\s+", " ", text)
    text = text.strip()
    return text


def norm_storkreds(value: object) -> str:
    """Normalize storkreds labels so 'Bornholm' and 'Bornholms Storkreds' align."""
    text = norm_text(value).lower()
    text = re.sub(r"\bstorkreds\b", "", text).strip()
    if text.endswith("s"):
        text = text[:-1]
    return text


def norm_kommune(value: object) -> str:
    """Normalize kommune labels across election and municipality sources."""
    text = norm_text(value)
    text = re.sub(r"Regionskommune$", "Kommune", text, flags=re.IGNORECASE)
    return text


def to_int_series(series: pd.Series) -> pd.Series:
    """Convert strings like '1.234' or '1,234' to integers."""
    cleaned = (
        series.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0).astype(int)


def sanitize_column_name(value: str) -> str:
    """Make municipality variable names safe as column names."""
    value = norm_text(value).lower()
    value = value.replace("%", "pct")
    value = value.replace("+", "plus")
    value = re.sub(r"[^0-9a-zæøå]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def parse_age_to_number(series: pd.Series) -> pd.Series:
    """Extract numeric age from strings like '40 år'."""
    extracted = series.astype(str).str.extract(r"(\d+)", expand=False)
    return pd.to_numeric(extracted, errors="coerce")


def require_columns(df: pd.DataFrame, required: Iterable[str], file_label: str) -> None:
    """Stop immediately if required columns are missing."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{file_label} is missing required columns: {missing}")


def parse_tiktok_handle(url: object) -> str:
    """Extract a TikTok handle from a TikTok URL.

    Examples:
    - https://www.tiktok.com/@example -> example
    - https://www.tiktok.com/@example?lang=da -> example
    - @example -> example
    """
    if pd.isna(url):
        return ""
    text = str(url).strip()
    if not text:
        return ""
    if text.startswith("@"):
        return text[1:].strip().lower()
    match = re.search(r"tiktok\.com/@([^/?#]+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().lower()
    return text.strip().lower()


# -----------------------------------------------------------------------------
# Load candidate master data
# -----------------------------------------------------------------------------

def load_candidate_master(candidate_list_path: Path, demographics_path: Path) -> pd.DataFrame:
    """Load the official candidate list and strictly merge candidate demographics."""
    candidate_list = pd.read_csv(candidate_list_path)
    demographics = pd.read_csv(demographics_path)

    require_columns(candidate_list, ["candidate_name", "party_name", "storkreds"], str(candidate_list_path))
    require_columns(demographics, ["candidate_name", "party", "age", "education"], str(demographics_path))

    # Keep only successful Altinget scrapes when the status column exists.
    if "status" in demographics.columns:
        demographics = demographics[demographics["status"].astype(str).str.lower().eq("ok")].copy()

    candidate_list = candidate_list.copy()
    candidate_list["candidate_name"] = candidate_list["candidate_name"].map(norm_text)
    candidate_list["party_name"] = candidate_list["party_name"].map(norm_text)
    candidate_list["storkreds"] = candidate_list["storkreds"].map(norm_storkreds)

    demographics = demographics.rename(columns={"party": "party_name"}).copy()
    demographics["candidate_name"] = demographics["candidate_name"].map(norm_text)
    demographics["party_name"] = demographics["party_name"].map(norm_text)

    # If storkreds is not in demographics, we merge through the official candidate list.
    # Here we require exact match on candidate_name + party_name.
    demo_dupes = demographics.duplicated(subset=["candidate_name", "party_name"], keep=False)
    if demo_dupes.any():
        dupes = demographics.loc[demo_dupes, ["candidate_name", "party_name"]].drop_duplicates()
        raise ValueError(
            "Candidate demographics contain duplicate candidate_name + party_name rows. "
            f"Examples: {dupes.head(10).to_dict(orient='records')}"
        )

    master = candidate_list.merge(
        demographics[["candidate_name", "party_name", "age", "education", "birthdate"] if "birthdate" in demographics.columns else ["candidate_name", "party_name", "age", "education"]],
        on=["candidate_name", "party_name"],
        how="left",
        validate="one_to_one",
    )

    if "birthdate" not in master.columns:
        master["birthdate"] = pd.NA

    unmatched = master[master["age"].isna() | master["education"].isna()][["candidate_name", "party_name", "storkreds"]]
    if not unmatched.empty:
        warnings.warn(
            "Candidate-demographics merge left missing values for some candidates. "
            f"Count={len(unmatched)}. Sample={unmatched.head(10).to_dict(orient='records')}",
            RuntimeWarning,
        )

    master["age_num"] = parse_age_to_number(master["age"])
    master["education"] = master["education"].fillna("Unknown")
    return master


# -----------------------------------------------------------------------------
# Load curated TikTok existence list
# -----------------------------------------------------------------------------

def load_tiktok_accounts(tiktok_accounts_path: Path, candidate_master: pd.DataFrame) -> pd.DataFrame:
    """Create a strict candidate-level tiktok_exists indicator and handle lookup."""
    accounts = pd.read_csv(tiktok_accounts_path)
    require_columns(accounts, ["candidate_name", "party_name", "storkreds"], str(tiktok_accounts_path))

    accounts = accounts.copy()
    for col in ["candidate_name", "party_name", "storkreds"]:
        accounts[col] = accounts[col].map(norm_text)
    accounts["storkreds"] = accounts["storkreds"].map(norm_storkreds)

    if "har_tiktok" in accounts.columns:
        values = accounts["har_tiktok"].astype(str).str.strip().str.lower()
        accounts["tiktok_exists"] = values.isin({"1", "true", "ja", "yes", "y"}).astype(int)
    else:
        accounts["tiktok_exists"] = 1

    if "tiktok_url" in accounts.columns:
        accounts["tiktok_handle_from_url"] = accounts["tiktok_url"].map(parse_tiktok_handle)
    else:
        accounts["tiktok_handle_from_url"] = ""

    # Keep one row per candidate key.
    accounts = accounts.sort_values(["candidate_name", "party_name", "storkreds"]).drop_duplicates(
        subset=["candidate_name", "party_name", "storkreds"], keep="first"
    )

    merged = candidate_master.merge(
        accounts[["candidate_name", "party_name", "storkreds", "tiktok_exists", "tiktok_handle_from_url"]],
        on=["candidate_name", "party_name", "storkreds"],
        how="left",
        validate="one_to_one",
    )

    merged["tiktok_exists"] = merged["tiktok_exists"].fillna(0).astype(int)
    merged["tiktok_handle_from_url"] = merged["tiktok_handle_from_url"].fillna("")
    return merged


# -----------------------------------------------------------------------------
# Load and aggregate TikTok videos
# -----------------------------------------------------------------------------

def load_video_measures(video_path: Path, candidate_master_with_tiktok: pd.DataFrame) -> pd.DataFrame:
    """Aggregate video-level TikTok data to the candidate level.

    The video file is linked to candidates through tiktok_handle.
    Video windows exclude election day itself and end on the day before election day.
    """
    videos = pd.read_csv(video_path)
    require_columns(videos, ["tiktok_handle", "video_id", "upload_dato"], str(video_path))

    videos = videos.copy()
    videos["tiktok_handle"] = videos["tiktok_handle"].astype(str).str.strip().str.lower()
    videos["upload_dato"] = pd.to_datetime(videos["upload_dato"], errors="coerce")

    # Drop rows without usable handle or upload date.
    videos = videos[videos["tiktok_handle"].ne("") & videos["upload_dato"].notna()].copy()

    lookup = candidate_master_with_tiktok[
        candidate_master_with_tiktok["tiktok_exists"].eq(1) & candidate_master_with_tiktok["tiktok_handle_from_url"].ne("")
    ][["candidate_name", "party_name", "storkreds", "tiktok_handle_from_url"]].copy()

    # Strict assumption: one handle belongs to one candidate.
    dupes = lookup.duplicated(subset=["tiktok_handle_from_url"], keep=False)
    if dupes.any():
        bad = lookup.loc[dupes].to_dict(orient="records")
        raise ValueError(f"Multiple candidates share the same TikTok handle: {bad[:20]}")

    lookup = lookup.rename(columns={"tiktok_handle_from_url": "tiktok_handle"})

    # Merge videos onto candidates through the curated handle list.
    videos = videos.merge(
        lookup,
        on="tiktok_handle",
        how="inner",
        validate="many_to_one",
        suffixes=("", "_candidate"),
    )
    for col in ["candidate_name", "party_name", "storkreds"]:
        candidate_col = f"{col}_candidate"
        if candidate_col in videos.columns:
            videos[col] = videos[candidate_col]

    def count_in_window(df: pd.DataFrame, days: int) -> int:
        start = PRE_ELECTION_END - pd.Timedelta(days=days - 1)
        mask = (df["upload_dato"] >= start) & (df["upload_dato"] <= PRE_ELECTION_END)
        return int(mask.sum())

    def count_total_pre_election(df: pd.DataFrame) -> int:
        return int((df["upload_dato"] <= PRE_ELECTION_END).sum())

    def count_total_2026_pre_election(df: pd.DataFrame) -> int:
        mask = (df["upload_dato"] >= pd.Timestamp("2026-01-01")) & (df["upload_dato"] <= PRE_ELECTION_END)
        return int(mask.sum())

    grouped = []
    for key, group in videos.groupby(["candidate_name", "party_name", "storkreds"], dropna=False):
        row = {
            "candidate_name": key[0],
            "party_name": key[1],
            "storkreds": key[2],
            "videos_total_pre_election": count_total_pre_election(group),
            "videos_total_2026_pre_election": count_total_2026_pre_election(group),
            "videos_last_90d": count_in_window(group, 90),
            "videos_last_30d": count_in_window(group, 30),
            "videos_last_14d": count_in_window(group, 14),
            "videos_last_7d": count_in_window(group, 7),
        }
        grouped.append(row)

    out = pd.DataFrame(grouped)
    if out.empty:
        out = pd.DataFrame(columns=[
            "candidate_name", "party_name", "storkreds",
            "videos_total_pre_election", "videos_total_2026_pre_election",
            "videos_last_90d", "videos_last_30d", "videos_last_14d", "videos_last_7d"
        ])

    # Add log transforms to make later modeling easier.
    count_cols = [
        "videos_total_pre_election",
        "videos_total_2026_pre_election",
        "videos_last_90d",
        "videos_last_30d",
        "videos_last_14d",
        "videos_last_7d",
    ]
    for col in count_cols:
        if col not in out.columns:
            out[col] = 0
        out[f"log1p_{col}"] = pd.to_numeric(out[col], errors="coerce").fillna(0).map(lambda x: np.log1p(x))

    return out


# -----------------------------------------------------------------------------
# Load election data and create outcomes
# -----------------------------------------------------------------------------

def aggregate_election_to_candidate_municipality(path: Path, year: int) -> pd.DataFrame:
    """Aggregate parsed valg.dk rows to candidate x municipality level."""
    df = pd.read_csv(path)
    require_columns(df, ["storkreds", "kommune", "Partinavn", "Navn", "Stemmetal"], str(path))

    df = df.copy()
    df["storkreds"] = df["storkreds"].map(norm_storkreds)
    df["kommune"] = df["kommune"].map(norm_kommune)
    df["party_name"] = df["Partinavn"].map(norm_text)
    df["candidate_name"] = df["Navn"].map(norm_text)
    df["votes"] = to_int_series(df["Stemmetal"])

    # Candidate votes in each municipality.
    cand_muni = (
        df.groupby(["candidate_name", "party_name", "storkreds", "kommune"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": f"candidate_votes_{year}_municipality"})
    )

    # Party votes in each municipality.
    party_muni = (
        df.groupby(["party_name", "kommune"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": f"party_votes_{year}_municipality"})
    )

    cand_muni = cand_muni.merge(party_muni, on=["party_name", "kommune"], how="left", validate="many_to_one")
    cand_muni[f"vote_share_of_party_{year}_municipality"] = (
        cand_muni[f"candidate_votes_{year}_municipality"] / cand_muni[f"party_votes_{year}_municipality"]
    )

    # Candidate totals across all municipalities.
    cand_total = (
        df.groupby(["candidate_name", "party_name", "storkreds"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": f"candidate_votes_{year}_total"})
    )

    # Party totals across all municipalities.
    party_total = (
        df.groupby(["party_name"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": f"party_votes_{year}_total"})
    )

    cand_total = cand_total.merge(party_total, on="party_name", how="left", validate="many_to_one")
    cand_total[f"vote_share_of_party_{year}_total"] = (
        cand_total[f"candidate_votes_{year}_total"] / cand_total[f"party_votes_{year}_total"]
    )

    return cand_muni.merge(cand_total, on=["candidate_name", "party_name", "storkreds"], how="left", validate="many_to_one")


# -----------------------------------------------------------------------------
# Load tidy municipality data
# -----------------------------------------------------------------------------

def load_municipality_covariates(path: Path, year: int) -> pd.DataFrame:
    """Load municipality data in tidy long format and pivot it to wide format."""
    muni = pd.read_csv(path)
    require_columns(muni, ["municipality", "municipality_code", "variable", "year", "value"], str(path))

    muni = muni.copy()
    muni["municipality"] = muni["municipality"].map(norm_kommune)
    muni["variable"] = muni["variable"].map(norm_text)
    muni["year"] = pd.to_numeric(muni["year"], errors="coerce")
    muni["value"] = pd.to_numeric(muni["value"], errors="coerce")

    muni = muni[muni["year"].eq(year)].copy()
    if muni.empty:
        raise ValueError(f"No municipality data found for year={year} in {path}")

    muni["variable_clean"] = muni["variable"].map(sanitize_column_name)

    wide = (
        muni.pivot_table(
            index=["municipality", "municipality_code"],
            columns="variable_clean",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    wide.columns.name = None
    return wide


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------

def build_analysis_datasets(
    repo_root: Path,
    candidate_list_path: Path,
    demographics_path: Path,
    tiktok_accounts_path: Path,
    video_path: Path,
    election_2026_path: Path,
    election_2022_path: Path | None,
    municipality_path: Path,
    outdir: Path,
) -> None:
    """Build and save the clean analysis datasets."""
    outdir.mkdir(parents=True, exist_ok=True)

    # 1. Candidate master table.
    candidate_master = load_candidate_master(candidate_list_path, demographics_path)
    candidate_master = load_tiktok_accounts(tiktok_accounts_path, candidate_master)
    video_measures = load_video_measures(video_path, candidate_master)

    candidate_master = candidate_master.merge(
        video_measures,
        on=["candidate_name", "party_name", "storkreds"],
        how="left",
        validate="one_to_one",
    )

    # Fill missing video measures with zeros for candidates without TikTok videos.
    for col in candidate_master.columns:
        if col.startswith("videos_") or col.startswith("log1p_videos_"):
            candidate_master[col] = pd.to_numeric(candidate_master[col], errors="coerce").fillna(0)

    # 2. 2026 election outcomes at candidate x municipality level.
    election_2026 = aggregate_election_to_candidate_municipality(election_2026_path, 2026)

    # Strictly keep only official candidates from the candidate list.
    analysis_muni = election_2026.merge(
        candidate_master,
        on=["candidate_name", "party_name", "storkreds"],
        how="left",
        validate="many_to_one",
    )

    unmatched_candidates = analysis_muni[analysis_muni["age"].isna()][["candidate_name", "party_name", "storkreds"]].drop_duplicates()
    if not unmatched_candidates.empty:
        warnings.warn(
            "Some election rows have missing demographics after matching candidate master. "
            f"Count={len(unmatched_candidates)}. Sample={unmatched_candidates.head(10).to_dict(orient='records')}",
            RuntimeWarning,
        )

    # Keep the build runnable when demographics are partially missing.
    if "age_num" in analysis_muni.columns:
        median_age = pd.to_numeric(analysis_muni["age_num"], errors="coerce").median()
        if pd.isna(median_age):
            median_age = 0
        analysis_muni["age_num"] = pd.to_numeric(analysis_muni["age_num"], errors="coerce").fillna(median_age)
    if "education" in analysis_muni.columns:
        analysis_muni["education"] = analysis_muni["education"].fillna("Unknown")

    # 3. Optional prior-strength variables from 2022.
    if election_2022_path is not None:
        election_2022 = aggregate_election_to_candidate_municipality(election_2022_path, 2022)
        prior_cols = [
            "candidate_name", "party_name", "storkreds", "kommune",
            "candidate_votes_2022_municipality", "party_votes_2022_municipality",
            "vote_share_of_party_2022_municipality",
            "candidate_votes_2022_total", "party_votes_2022_total",
            "vote_share_of_party_2022_total",
        ]
        analysis_muni = analysis_muni.merge(
            election_2022[prior_cols],
            on=["candidate_name", "party_name", "storkreds", "kommune"],
            how="left",
            validate="many_to_one",
        )

    # 4. Municipality covariates.
    municipality_covariates = load_municipality_covariates(municipality_path, PRE_ELECTION_YEAR)
    municipality_covariates = municipality_covariates.rename(columns={"municipality": "kommune"})
    municipality_covariates["kommune"] = municipality_covariates["kommune"].map(norm_kommune)
    analysis_muni["kommune"] = analysis_muni["kommune"].map(norm_kommune)

    analysis_muni = analysis_muni.merge(
        municipality_covariates,
        on="kommune",
        how="left",
        validate="many_to_one",
    )

    unmatched_kommuner = analysis_muni[analysis_muni["municipality_code"].isna()][["kommune"]].drop_duplicates()
    if not unmatched_kommuner.empty:
        warnings.warn(
            "Some municipalities did not match covariates; municipality columns will be NaN for those rows. "
            f"Count={len(unmatched_kommuner)}. Sample={unmatched_kommuner.head(10).to_dict(orient='records')}",
            RuntimeWarning,
        )

    # 5. Candidate-level version collapsed from municipality rows.
    candidate_level = analysis_muni[[
        "candidate_name", "party_name", "storkreds", "age", "age_num", "education", "birthdate",
        "tiktok_exists", "tiktok_handle_from_url",
        "videos_total_pre_election", "videos_total_2026_pre_election",
        "videos_last_90d", "videos_last_30d", "videos_last_14d", "videos_last_7d",
        "log1p_videos_total_pre_election", "log1p_videos_total_2026_pre_election",
        "log1p_videos_last_90d", "log1p_videos_last_30d", "log1p_videos_last_14d", "log1p_videos_last_7d",
        "candidate_votes_2026_total", "party_votes_2026_total", "vote_share_of_party_2026_total",
    ] + ([
        "candidate_votes_2022_total", "party_votes_2022_total", "vote_share_of_party_2022_total",
    ] if election_2022_path is not None else [])].drop_duplicates().copy()

    # 6. Save outputs.
    muni_path = outdir / "candidate_municipality_analysis_2026.csv"
    cand_path = outdir / "candidate_analysis_2026.csv"
    summary_path = outdir / "build_summary.json"

    analysis_muni.to_csv(muni_path, index=False, encoding="utf-8-sig")
    candidate_level.to_csv(cand_path, index=False, encoding="utf-8-sig")

    summary = {
        "repo_root": str(repo_root),
        "pre_election_end": str(PRE_ELECTION_END.date()),
        "municipality_covariate_year": PRE_ELECTION_YEAR,
        "n_candidate_municipality_rows": int(len(analysis_muni)),
        "n_candidate_rows": int(len(candidate_level)),
        "n_candidates_with_tiktok_exists": int(candidate_level["tiktok_exists"].sum()),
        "candidate_municipality_output": str(muni_path),
        "candidate_output": str(cand_path),
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"Saved municipality-level dataset: {muni_path}")
    print(f"Saved candidate-level dataset:    {cand_path}")
    print(f"Saved build summary:              {summary_path}")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build a clean 2026 analysis dataset.")
    parser.add_argument("--repo-root", type=Path, default=Path("."), help="Repository root folder.")
    parser.add_argument(
        "--candidate-list",
        type=Path,
        default=Path("Datasets/Candidates/candidates_list_2026.csv"),
        help="Path to candidates_list_2026.csv, relative to repo root unless absolute.",
    )
    parser.add_argument(
        "--candidate-demographics",
        type=Path,
        default=Path("Datasets/Candidates/altinget_ft26_candidates.csv"),
        help="Path to altinget_ft26_candidates.csv, relative to repo root unless absolute.",
    )
    parser.add_argument(
        "--tiktok-accounts",
        type=Path,
        default=Path("Inputs/TikTok/current/candidates_tiktok_accounts.csv"),
        help="Path to curated candidate TikTok accounts file.",
    )
    parser.add_argument(
        "--video-data",
        type=Path,
        default=Path("Datasets/Tiktok/video_data_full.csv"),
        help="Path to the video-level TikTok file.",
    )
    parser.add_argument(
        "--results-2026",
        type=Path,
        default=Path("Datasets/Election/parsed_export_rows_2026.csv"),
        help="Path to parsed 2026 election rows.",
    )
    parser.add_argument(
        "--results-2022",
        type=Path,
        default=Path("Datasets/Election/parsed_export_rows_2022.csv"),
        help="Path to parsed 2022 election rows.",
    )
    parser.add_argument(
        "--municipality-data",
        type=Path,
        default=Path("Datasets/Muncipality Data/Muncipality Data 2005-2026.csv"),
        help="Path to tidy municipality data.",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("Outputs/analysis_dataset_2026"),
        help="Output directory, relative to repo root unless absolute.",
    )
    args = parser.parse_args()

    if args.repo_root == Path("."):
        repo_root = Path(__file__).resolve().parents[1]
    else:
        repo_root = args.repo_root.resolve()

    def resolve(path: Path) -> Path:
        return path if path.is_absolute() else (repo_root / path)

    build_analysis_datasets(
        repo_root=repo_root,
        candidate_list_path=resolve(args.candidate_list),
        demographics_path=resolve(args.candidate_demographics),
        tiktok_accounts_path=resolve(args.tiktok_accounts),
        video_path=resolve(args.video_data),
        election_2026_path=resolve(args.results_2026),
        election_2022_path=resolve(args.results_2022),
        municipality_path=resolve(args.municipality_data),
        outdir=resolve(args.outdir),
    )


if __name__ == "__main__":
    main()
