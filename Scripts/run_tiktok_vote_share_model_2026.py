#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple TikTok / vote-share model for the Danish Folketing 2026 project.

Purpose
-------
Build one analysis-ready dataset at the candidate x municipality level and run
simple OLS models for whether TikTok presence and recent TikTok activity are
associated with a candidate capturing a larger share of their party's votes.

The script is designed around the repository structure documented in
FILESTRUCTURE.md.

Main outcome
------------
For each candidate i in municipality m:

    vote_share_of_party_muni = candidate_votes_2026 / party_votes_2026

This means the outcome is the candidate's share of own-party votes inside the
municipality.

Main regressors
---------------
- tiktok_exists: 1 if the candidate appears in the curated TikTok candidate list
- videos_last_7d / 14d / 30d / 90d: number of matched candidate videos posted in
  the final 7 / 14 / 30 / 90 days before election day
- age_num
- education
- party fixed effects
- optional municipality youth variable interaction if available
- optional prior electoral strength from 2022

Inputs expected by default
--------------------------
repo_root/
    Datasets/Candidates/altinget_ft26_candidates.csv
    Datasets/Candidates/candidates_list_2026.csv
    Datasets/Election/parsed_export_rows_2026.csv
    Datasets/Election/parsed_export_rows_2022.csv
    Datasets/Muncipality Data/Muncipality Data 2005-2026.csv
    Datasets/Tiktok/video_data_full.csv
    Inputs/TikTok/current/candidates_tiktok_accounts.csv

Outputs
-------
outdir/
    analysis_candidate_muni_2026.csv
    analysis_candidate_level_2026.csv
    model_1_tiktok_exists.txt
    model_2_recent_video_windows.txt
    model_3_with_prior_strength.txt
    model_4_young_area_interaction.txt
    summary_metrics.txt

Notes
-----
- Matching is strict where it should be strict:
    * election candidates -> candidate list
    * candidate list -> demographics
    * candidate list -> TikTok candidate list
- TikTok video matching uses the matched-candidate fields already present in the
  enriched video-level file. Candidates with no matched videos simply get zeros.
- Municipality data are already tidy according to the updated file structure.
"""

from __future__ import annotations

import argparse
import re
import unicodedata
import warnings
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


ELECTION_DAY = pd.Timestamp("2026-03-24")


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------

def clean_text(value: object) -> str:
    """Return a stripped string with repeated whitespace collapsed."""
    if pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKC", str(value)).replace("\xa0", " ")
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_storkreds(value: object) -> str:
    """Normalize storkreds labels across data sources."""
    text = clean_text(value).lower()
    text = re.sub(r"\bstorkreds\b", "", text).strip()
    if text.endswith("s"):
        text = text[:-1]
    return text


def normalize_party(value: object) -> str:
    """Normalize party labels across data sources."""
    text = clean_text(value)
    lowered = text.lower()
    if lowered.startswith("venstre,"):
        return "Venstre"
    if lowered == "(uden for parti)":
        return "Uden for partierne"
    return text


def normalize_kommune(value: object) -> str:
    """Normalize kommune labels across sources."""
    text = clean_text(value)
    text = re.sub(r"Regionskommune$", "Kommune", text, flags=re.IGNORECASE)
    return text


def strict_key(candidate_name: object, party_name: object, storkreds: object) -> str:
    """Build one exact join key after very light text cleaning."""
    return " || ".join(
        [clean_text(candidate_name), normalize_party(party_name), normalize_storkreds(storkreds)]
    )


def assert_no_duplicate_keys(df: pd.DataFrame, key_col: str, label: str) -> None:
    """Raise an error if a supposed unique key is duplicated."""
    dupes = df[df.duplicated(key_col, keep=False)].sort_values(key_col)
    if not dupes.empty:
        sample = dupes[[key_col]].drop_duplicates().head(10).to_dict("records")
        raise ValueError(
            f"{label} contains duplicate keys in column '{key_col}'. Sample: {sample}"
        )


def log1p_series(series: pd.Series) -> pd.Series:
    """Numerically stable log(1+x) transformation."""
    return np.log1p(series.astype(float))


def coerce_numeric(series: pd.Series) -> pd.Series:
    """Convert a column to numeric, stripping Danish thousands separators if needed."""
    cleaned = (
        series.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def first_existing(columns: Iterable[str], df: pd.DataFrame) -> Optional[str]:
    """Return the first column name that exists in df, else None."""
    for col in columns:
        if col in df.columns:
            return col
    return None


# -----------------------------------------------------------------------------
# Readers
# -----------------------------------------------------------------------------

def read_candidate_list(path: Path) -> pd.DataFrame:
    """Read the clean 2026 candidate list used as the bridge file."""
    df = pd.read_csv(path)
    required = {"candidate_name", "party_name", "storkreds"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Candidate list missing columns: {sorted(missing)}")

    df = df.copy()
    for col in ["candidate_name", "party_name", "storkreds"]:
        df[col] = df[col].map(clean_text)
    df["party_name"] = df["party_name"].map(normalize_party)
    df["storkreds"] = df["storkreds"].map(normalize_storkreds)

    df["candidate_key"] = df.apply(
        lambda row: strict_key(row["candidate_name"], row["party_name"], row["storkreds"]),
        axis=1,
    )
    assert_no_duplicate_keys(df, "candidate_key", "Candidate list")
    return df


def read_demographics(path: Path, candidate_list: pd.DataFrame) -> pd.DataFrame:
    """
    Read scraped candidate demographics and match them strictly through the
    candidate list.

    The Altinget file has columns:
        profile_id, url, status, candidate_name, party, age, education, birthdate, error
    """
    df = pd.read_csv(path)
    required = {"candidate_name", "party", "age", "education"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Demographics file missing columns: {sorted(missing)}")

    df = df.copy()
    if "status" in df.columns:
        df = df[df["status"].fillna("").eq("ok")].copy()

    df["candidate_name"] = df["candidate_name"].map(clean_text)
    df["party_name"] = df["party"].map(normalize_party)

    # Candidate list can contain duplicate (candidate_name, party_name) across storkreds.
    # We first match on exact name+party when unique, then fallback to unique candidate_name.
    bridge = candidate_list[["candidate_name", "party_name", "candidate_key"]].copy()
    pair_counts = bridge.groupby(["candidate_name", "party_name"]).size().rename("n_pair").reset_index()
    pair_unique = pair_counts[pair_counts["n_pair"].eq(1)][["candidate_name", "party_name"]]
    bridge_pair = bridge.merge(pair_unique, on=["candidate_name", "party_name"], how="inner")

    merged = df.merge(
        bridge_pair,
        on=["candidate_name", "party_name"],
        how="left",
        validate="m:1",
    )

    name_unique = bridge.groupby("candidate_name", as_index=False).agg(n_name=("candidate_key", "nunique"))
    name_unique = name_unique[name_unique["n_name"].eq(1)][["candidate_name"]]
    bridge_name = bridge.merge(name_unique, on="candidate_name", how="inner")

    missing_mask = merged["candidate_key"].isna()
    if missing_mask.any():
        fallback = merged.loc[missing_mask, ["candidate_name", "party_name", "age", "education", "birthdate"] if "birthdate" in merged.columns else ["candidate_name", "party_name", "age", "education"]].copy()
        fallback = fallback.merge(bridge_name, on="candidate_name", how="left", validate="m:1")
        merged.loc[missing_mask, "candidate_key"] = fallback["candidate_key"].values

    unmatched = merged[merged["candidate_key"].isna()][["candidate_name", "party_name"]].drop_duplicates()
    if not unmatched.empty:
        warnings.warn(
            "Some demographic rows did not match candidate list and will be ignored. "
            f"Count={len(unmatched)}. Sample={unmatched.head(10).to_dict('records')}",
            RuntimeWarning,
        )
        merged = merged[merged["candidate_key"].notna()].copy()

    merged["age_num"] = coerce_numeric(merged["age"])
    if "birthdate" not in merged.columns:
        merged["birthdate"] = np.nan
    keep = ["candidate_key", "age_num", "education", "birthdate"]
    out = merged[keep].drop_duplicates("candidate_key")
    assert_no_duplicate_keys(out, "candidate_key", "Demographics after matching")
    return out


def read_tiktok_candidate_list(path: Path, candidate_list: pd.DataFrame) -> pd.DataFrame:
    """
    Read the curated candidate TikTok list and create a strict candidate-level
    tiktok_exists indicator.
    """
    df = pd.read_csv(path)
    required = {"candidate_name", "party_name", "storkreds", "har_tiktok"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"TikTok candidate list missing columns: {sorted(missing)}")

    df = df.copy()
    for col in ["candidate_name", "party_name", "storkreds"]:
        df[col] = df[col].map(clean_text)
    df["party_name"] = df["party_name"].map(normalize_party)
    df["storkreds"] = df["storkreds"].map(normalize_storkreds)

    df["candidate_key"] = df.apply(
        lambda row: strict_key(row["candidate_name"], row["party_name"], row["storkreds"]),
        axis=1,
    )

    # Ensure the curated list only references known 2026 candidates.
    unknown = df.loc[~df["candidate_key"].isin(set(candidate_list["candidate_key"]))]
    if not unknown.empty:
        sample = unknown[["candidate_name", "party_name", "storkreds"]].head(10).to_dict("records")
        warnings.warn(
            "Some rows in candidates_tiktok_accounts.csv do not match candidate list and will be dropped. "
            f"Count={len(unknown)}. Sample={sample}",
            RuntimeWarning,
        )
        df = df[df["candidate_key"].isin(set(candidate_list["candidate_key"]))].copy()

    def to_binary(value: object) -> int:
        text = clean_text(value).lower()
        return 1 if text in {"1", "true", "ja", "yes", "y"} else 0

    df["tiktok_exists"] = df["har_tiktok"].map(to_binary)
    out = df.groupby("candidate_key", as_index=False)["tiktok_exists"].max()
    assert_no_duplicate_keys(out, "candidate_key", "TikTok candidate list after grouping")
    return out


def read_election_rows(path: Path, year: int) -> pd.DataFrame:
    """
    Read parsed election rows for one year.

    Row grain in the source file is one candidate-party vote row from one polling table.
    """
    df = pd.read_csv(path)
    required = {
        "storkreds",
        "kommune",
        "Partinavn",
        "Navn",
        "Stemmetal",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Election file for {year} missing columns: {sorted(missing)}")

    df = df.copy()
    df["election_year"] = year
    df["candidate_name"] = df["Navn"].map(clean_text)
    df["party_name"] = df["Partinavn"].map(normalize_party)
    df["storkreds"] = df["storkreds"].map(normalize_storkreds)
    df["kommune"] = df["kommune"].map(normalize_kommune)
    df["votes"] = coerce_numeric(df["Stemmetal"]).fillna(0)
    df = df[~df["candidate_name"].str.lower().eq("partiliste")].copy()

    # Build exact candidate key for strict join with candidate list.
    df["candidate_key"] = df.apply(
        lambda row: strict_key(row["candidate_name"], row["party_name"], row["storkreds"]),
        axis=1,
    )
    return df


def read_tiktok_videos(path: Path, candidate_list: pd.DataFrame) -> pd.DataFrame:
    """
    Read enriched video-level TikTok data.

    The updated file structure says video_data_full.csv is already a deduplicated,
    candidate-enriched video-level dataset. We use matched candidate fields where
    available and require exact matching to the 2026 candidate list.
    """
    df = pd.read_csv(path, on_bad_lines="skip")
    name_col = first_existing(["matched_candidate_name", "candidate_name_from_list", "candidate_name", "display_name"], df)
    party_col = first_existing(["candidate_party_from_list", "party_name"], df)
    storkreds_col = first_existing(["candidate_storkreds_from_list", "storkreds"], df)
    date_col = first_existing(["upload_dato", "upload_date"], df)

    if name_col is None or party_col is None or storkreds_col is None or date_col is None:
        raise ValueError(
            "TikTok video file does not contain the expected candidate-matching columns. "
            "Need candidate name, party, storkreds, and upload_dato."
        )

    df = df.copy()
    if "is_candidate_match" in df.columns:
        df = df[df["is_candidate_match"].astype(str).str.lower().isin(["1", "true", "yes"])].copy()

    df["candidate_name"] = df[name_col].map(clean_text)
    df["party_name"] = df[party_col].map(normalize_party)
    df["storkreds"] = df[storkreds_col].map(normalize_storkreds)
    df["candidate_key"] = df.apply(
        lambda row: strict_key(row["candidate_name"], row["party_name"], row["storkreds"]),
        axis=1,
    )
    df["upload_dato"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df["upload_dato"].notna()].copy()

    unknown = df.loc[~df["candidate_key"].isin(set(candidate_list["candidate_key"]))]
    if not unknown.empty:
        sample = unknown[["candidate_name", "party_name", "storkreds"]].drop_duplicates().head(10).to_dict("records")
        warnings.warn(
            "Some TikTok video rows do not map to candidate list and will be dropped. "
            f"Count={len(unknown)}. Sample={sample}",
            RuntimeWarning,
        )
        df = df[df["candidate_key"].isin(set(candidate_list["candidate_key"]))].copy()

    return df


def read_municipality_data(path: Path) -> pd.DataFrame:
    """
    Read tidy municipality-year-variable data.

    Expected columns:
        municipality, municipality_code, variable, year, value
    """
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [str(c).strip().lstrip("\ufeff") for c in df.columns]
    required = {"municipality", "municipality_code", "variable", "year", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Municipality data missing columns: {sorted(missing)}")

    df = df.copy()
    df["municipality"] = df["municipality"].map(normalize_kommune)
    df["variable"] = df["variable"].map(clean_text)
    df["year"] = coerce_numeric(df["year"])
    df["value"] = coerce_numeric(df["value"])
    return df


# -----------------------------------------------------------------------------
# Feature engineering
# -----------------------------------------------------------------------------

def build_2026_analysis_base(election_2026: pd.DataFrame, candidate_list: pd.DataFrame) -> pd.DataFrame:
    """
    Build the candidate x municipality outcome dataset for 2026.
    """
    # Strict check: all election candidate keys must exist in the 2026 candidate list.
    unknown = election_2026.loc[~election_2026["candidate_key"].isin(set(candidate_list["candidate_key"]))]
    if not unknown.empty:
        sample = unknown[["candidate_name", "party_name", "storkreds"]].drop_duplicates().head(10).to_dict("records")
        warnings.warn(
            "Some 2026 election rows do not match candidates_list_2026.csv and will be dropped. "
            f"Count={len(unknown)}. Sample={sample}",
            RuntimeWarning,
        )
        election_2026 = election_2026[election_2026["candidate_key"].isin(set(candidate_list["candidate_key"]))].copy()

    # Candidate votes by municipality.
    cand_muni = (
        election_2026.groupby(["candidate_key", "candidate_name", "party_name", "storkreds", "kommune"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "candidate_votes_muni_2026"})
    )

    # Party votes by municipality.
    party_muni = (
        election_2026.groupby(["party_name", "kommune"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "party_votes_muni_2026"})
    )

    # Candidate total votes nationally.
    cand_total = (
        election_2026.groupby(["candidate_key"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "candidate_votes_total_2026"})
    )

    # Party total votes nationally.
    party_total = (
        election_2026.groupby(["party_name"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "party_votes_total_2026"})
    )

    out = cand_muni.merge(party_muni, on=["party_name", "kommune"], how="left", validate="m:1")
    out = out.merge(cand_total, on="candidate_key", how="left", validate="m:1")
    out = out.merge(party_total, on="party_name", how="left", validate="m:1")

    out["vote_share_of_party_muni_2026"] = (
        out["candidate_votes_muni_2026"] / out["party_votes_muni_2026"]
    )
    out["vote_share_of_party_total_2026"] = (
        out["candidate_votes_total_2026"] / out["party_votes_total_2026"]
    )
    return out


def build_prior_strength_2022(election_2022: pd.DataFrame, candidate_list: pd.DataFrame) -> pd.DataFrame:
    """
    Build a simple prior strength variable from 2022.

    We use candidate total votes divided by party total votes in 2022 and a
    within-party national rank on that measure.
    """
    matched = election_2022[election_2022["candidate_key"].isin(set(candidate_list["candidate_key"]))].copy()
    if matched.empty:
        return pd.DataFrame(columns=["candidate_key", "prior_vote_share_of_party_2022", "prior_party_rank_2022"])

    cand_total = (
        matched.groupby(["candidate_key", "party_name"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "candidate_votes_total_2022"})
    )
    party_total = (
        matched.groupby("party_name", as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "party_votes_total_2022"})
    )
    out = cand_total.merge(party_total, on="party_name", how="left", validate="m:1")
    out["prior_vote_share_of_party_2022"] = out["candidate_votes_total_2022"] / out["party_votes_total_2022"]
    out["prior_party_rank_2022"] = out.groupby("party_name")["prior_vote_share_of_party_2022"].rank(
        ascending=False, method="dense"
    )
    return out[["candidate_key", "prior_vote_share_of_party_2022", "prior_party_rank_2022"]]


def build_video_windows(videos: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate candidate video activity in the final 7 / 14 / 30 / 90 days before
    election day.
    """
    df = videos.copy()
    days_before = (ELECTION_DAY - df["upload_dato"]).dt.days
    # Keep only videos posted on or before election day.
    df = df[(days_before >= 0)].copy()
    df["days_before_election"] = (ELECTION_DAY - df["upload_dato"]).dt.days

    if df.empty:
        return pd.DataFrame(columns=[
            "candidate_key",
            "videos_last_7d",
            "videos_last_14d",
            "videos_last_30d",
            "videos_last_90d",
            "videos_total_2026",
            "log_videos_last_7d",
            "log_videos_last_14d",
            "log_videos_last_30d",
            "log_videos_last_90d",
            "log_videos_total_2026",
        ])

    df["in_last_7d"] = df["days_before_election"] <= 7
    df["in_last_14d"] = df["days_before_election"] <= 14
    df["in_last_30d"] = df["days_before_election"] <= 30
    df["in_last_90d"] = df["days_before_election"] <= 90

    out = df.groupby("candidate_key", as_index=False).agg(
        videos_total_2026=("candidate_key", "size"),
        videos_last_7d=("in_last_7d", "sum"),
        videos_last_14d=("in_last_14d", "sum"),
        videos_last_30d=("in_last_30d", "sum"),
        videos_last_90d=("in_last_90d", "sum"),
    )

    for col in ["videos_last_7d", "videos_last_14d", "videos_last_30d", "videos_last_90d", "videos_total_2026"]:
        out[col] = out[col].fillna(0).astype(int)
        out[f"log_{col}"] = log1p_series(out[col])

    return out


def select_municipality_features(muni: pd.DataFrame, target_year: int = 2025) -> pd.DataFrame:
    """
    Pivot municipality data for one year into wide format.

    The function also tries to locate one youth-share variable automatically.
    """
    df = muni[muni["year"].eq(target_year)].copy()
    if df.empty:
        raise ValueError(f"No municipality observations found for year {target_year}.")

    wide = df.pivot_table(
        index=["municipality", "municipality_code"],
        columns="variable",
        values="value",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None

    # Find a likely youth-share variable for heterogeneity analysis.
    lower_map = {col.lower(): col for col in wide.columns}
    young_candidates = [
        col for col in wide.columns
        if isinstance(col, str)
        and ("18" in col and ("24" in col or "25" in col or "ung" in col.lower()))
    ]
    if young_candidates:
        wide["young_area_share"] = wide[young_candidates[0]]
        wide["young_area_variable_name"] = young_candidates[0]
    else:
        wide["young_area_share"] = np.nan
        wide["young_area_variable_name"] = ""

    # Normalize municipality field name for merging.
    wide = wide.rename(columns={"municipality": "kommune"})
    wide["kommune"] = wide["kommune"].map(normalize_kommune)
    return wide


# -----------------------------------------------------------------------------
# Modelling
# -----------------------------------------------------------------------------

def fit_ols(df: pd.DataFrame, formula: str):
    """Fit OLS with candidate-clustered standard errors."""
    model = smf.ols(formula=formula, data=df, missing="drop")
    used_index = model.data.row_labels
    groups = df.loc[used_index, "candidate_key"]
    try:
        return model.fit(cov_type="cluster", cov_kwds={"groups": groups})
    except Exception as exc:
        warnings.warn(
            f"Clustered covariance failed for formula '{formula}'. Falling back to HC1. Error: {exc}",
            RuntimeWarning,
        )
        return model.fit(cov_type="HC1")


def safe_formula_term(name: str) -> str:
    """Quote a column name for Patsy when it contains special characters."""
    return f'Q("{name}")'


# -----------------------------------------------------------------------------
# Main pipeline
# -----------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Simple TikTok vote-share model using updated repository structure.")
    parser.add_argument("--repo-root", type=Path, default=Path("."), help="Repository root directory.")
    parser.add_argument("--results-2026", type=Path, default=None)
    parser.add_argument("--results-2022", type=Path, default=None)
    parser.add_argument("--candidate-demographics", type=Path, default=None)
    parser.add_argument("--candidate-list", type=Path, default=None)
    parser.add_argument("--tiktok-candidate-list", type=Path, default=None)
    parser.add_argument("--tiktok-videos", type=Path, default=None)
    parser.add_argument("--municipality-data", type=Path, default=None)
    parser.add_argument("--outdir", type=Path, default=None)
    args = parser.parse_args()

    if args.repo_root == Path("."):
        repo_root = Path(__file__).resolve().parents[1]
    else:
        repo_root = args.repo_root.resolve()

    results_2026 = (args.results_2026 or repo_root / "Datasets" / "Election" / "parsed_export_rows_2026.csv").resolve()
    results_2022 = (args.results_2022 or repo_root / "Datasets" / "Election" / "parsed_export_rows_2022.csv").resolve()
    candidate_demographics = (args.candidate_demographics or repo_root / "Datasets" / "Candidates" / "altinget_ft26_candidates.csv").resolve()
    candidate_list_path = (args.candidate_list or repo_root / "Datasets" / "Candidates" / "candidates_list_2026.csv").resolve()
    tiktok_candidate_list = (args.tiktok_candidate_list or repo_root / "Inputs" / "TikTok" / "current" / "candidates_tiktok_accounts.csv").resolve()
    tiktok_videos = (args.tiktok_videos or repo_root / "Datasets" / "Tiktok" / "video_data_full.csv").resolve()
    municipality_data = (args.municipality_data or repo_root / "Datasets" / "Muncipality Data" / "Muncipality Data 2005-2026.csv").resolve()
    outdir = (args.outdir or repo_root / "Outputs" / "simple_tiktok_model_v6").resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    # 1. Read bridge / lookup files.
    candidates_2026 = read_candidate_list(candidate_list_path)
    demographics = read_demographics(candidate_demographics, candidates_2026)
    tiktok_exists = read_tiktok_candidate_list(tiktok_candidate_list, candidates_2026)

    # 2. Read election files.
    election_2026 = read_election_rows(results_2026, 2026)
    election_2022 = read_election_rows(results_2022, 2022) if results_2022.exists() else pd.DataFrame()

    # 3. Build base analysis dataset.
    analysis = build_2026_analysis_base(election_2026, candidates_2026)

    # 4. Merge candidate-level files strictly.
    analysis = analysis.merge(
        candidates_2026[["candidate_key"]],
        on="candidate_key",
        how="left",
        validate="m:1",
    )
    analysis = analysis.merge(demographics, on="candidate_key", how="left", validate="m:1")
    if analysis["age_num"].isna().any():
        missing_age = analysis[analysis["age_num"].isna()][["candidate_key", "candidate_name", "party_name", "storkreds"]].drop_duplicates()
        warnings.warn(
            "Some 2026 candidates are missing age and will be median-imputed. "
            f"Count={len(missing_age)}. Sample={missing_age.head(10).to_dict('records')}",
            RuntimeWarning,
        )
        non_null_age = analysis["age_num"].dropna()
        median_age = non_null_age.median() if not non_null_age.empty else 0
        analysis["age_num"] = analysis["age_num"].fillna(median_age)
    if analysis["education"].isna().any():
        analysis["education"] = analysis["education"].fillna("Unknown")

    analysis = analysis.merge(tiktok_exists, on="candidate_key", how="left", validate="m:1")
    analysis["tiktok_exists"] = analysis["tiktok_exists"].fillna(0).astype(int)

    # 5. Merge video windows.
    videos = read_tiktok_videos(tiktok_videos, candidates_2026)
    video_windows = build_video_windows(videos)
    analysis = analysis.merge(video_windows, on="candidate_key", how="left", validate="m:1")
    for col in [
        "videos_last_7d",
        "videos_last_14d",
        "videos_last_30d",
        "videos_last_90d",
        "videos_total_2026",
        "log_videos_last_7d",
        "log_videos_last_14d",
        "log_videos_last_30d",
        "log_videos_last_90d",
        "log_videos_total_2026",
    ]:
        if col in analysis.columns:
            analysis[col] = analysis[col].fillna(0)

    # 6. Merge municipality features.
    muni = read_municipality_data(municipality_data)
    muni_2025 = select_municipality_features(muni, target_year=2025)
    analysis = analysis.merge(muni_2025, on="kommune", how="left", validate="m:1")

    # 7. Add prior strength from 2022 if available.
    if not election_2022.empty:
        prior_strength = build_prior_strength_2022(election_2022, candidates_2026)
        analysis = analysis.merge(prior_strength, on="candidate_key", how="left", validate="m:1")
    else:
        analysis["prior_vote_share_of_party_2022"] = np.nan
        analysis["prior_party_rank_2022"] = np.nan

    # Candidate-level collapsed file for quick inspection.
    candidate_level = (
        analysis.sort_values(["candidate_key", "kommune"])
        .groupby("candidate_key", as_index=False)
        .agg(
            candidate_name=("candidate_name", "first"),
            party_name=("party_name", "first"),
            storkreds=("storkreds", "first"),
            age_num=("age_num", "first"),
            education=("education", "first"),
            tiktok_exists=("tiktok_exists", "first"),
            candidate_votes_total_2026=("candidate_votes_total_2026", "first"),
            party_votes_total_2026=("party_votes_total_2026", "first"),
            vote_share_of_party_total_2026=("vote_share_of_party_total_2026", "first"),
            videos_last_7d=("videos_last_7d", "first"),
            videos_last_14d=("videos_last_14d", "first"),
            videos_last_30d=("videos_last_30d", "first"),
            videos_last_90d=("videos_last_90d", "first"),
            videos_total_2026=("videos_total_2026", "first"),
            prior_vote_share_of_party_2022=("prior_vote_share_of_party_2022", "first"),
            prior_party_rank_2022=("prior_party_rank_2022", "first"),
        )
    )

    # 8. Save datasets.
    analysis.to_csv(outdir / "analysis_candidate_muni_2026.csv", index=False)
    candidate_level.to_csv(outdir / "analysis_candidate_level_2026.csv", index=False)

    # 9. Fit simple models.
    # Use the municipality-level file to keep municipality features available.
    model_df = analysis.copy()

    formulas = {
        "model_1_tiktok_exists": "vote_share_of_party_muni_2026 ~ tiktok_exists + age_num + C(education) + C(party_name)",
        "model_2_recent_video_windows": (
            "vote_share_of_party_muni_2026 ~ tiktok_exists + log_videos_last_7d + "
            "log_videos_last_14d + log_videos_last_30d + log_videos_last_90d + "
            "age_num + C(education) + C(party_name)"
        ),
        "model_3_with_prior_strength": (
            "vote_share_of_party_muni_2026 ~ tiktok_exists + log_videos_last_14d + "
            "prior_vote_share_of_party_2022 + age_num + C(education) + C(party_name)"
        ),
    }

    # Optional heterogeneity model if a youth variable was found.
    young_available = model_df["young_area_share"].notna().any()
    if young_available:
        formulas["model_4_young_area_interaction"] = (
            "vote_share_of_party_muni_2026 ~ tiktok_exists + young_area_share + "
            "tiktok_exists:young_area_share + age_num + C(education) + C(party_name)"
        )

    for name, formula in formulas.items():
        res = fit_ols(model_df.dropna(subset=["vote_share_of_party_muni_2026", "age_num"]), formula)
        with open(outdir / f"{name}.txt", "w", encoding="utf-8") as fh:
            fh.write(res.summary().as_text())

    # 10. Save a small text summary.
    young_var_name = ""
    if "young_area_variable_name" in analysis.columns and analysis["young_area_variable_name"].notna().any():
        names = [x for x in analysis["young_area_variable_name"].dropna().unique().tolist() if x]
        young_var_name = names[0] if names else ""

    summary_lines = [
        "Simple TikTok vote-share model summary",
        f"2026 candidate x municipality rows: {len(analysis):,}",
        f"Unique 2026 candidates: {analysis['candidate_key'].nunique():,}",
        f"Candidates with TikTok account list match: {int(candidate_level['tiktok_exists'].sum()):,}",
        f"Matched candidate-level TikTok videos: {len(videos):,}",
        f"Mean vote_share_of_party_muni_2026: {analysis['vote_share_of_party_muni_2026'].mean():.6f}",
        f"Municipality heterogeneity variable used: {young_var_name or 'none detected'}",
        "Saved files:",
        "- analysis_candidate_muni_2026.csv",
        "- analysis_candidate_level_2026.csv",
        "- model_1_tiktok_exists.txt",
        "- model_2_recent_video_windows.txt",
        "- model_3_with_prior_strength.txt",
    ]
    if young_available:
        summary_lines.append("- model_4_young_area_interaction.txt")

    with open(outdir / "summary_metrics.txt", "w", encoding="utf-8") as fh:
        fh.write("\n".join(summary_lines))

    print(f"Done. Outputs written to: {outdir}")


if __name__ == "__main__":
    main()
