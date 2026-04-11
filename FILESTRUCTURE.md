# File Structure and Data Dictionary

This document is an AI-ready map of the repository datasets.
For each key file, it includes:

- row grain (what one row represents)
- key columns
- analysis intent

## Project Scope

The repository analyzes Danish Folketing candidates, election outcomes (2005-2026), and TikTok activity linked to candidates and parties.

## Root Overview

- `README.md`: narrative project description and outputs.
- `FILESTRUCTURE.md`: this technical data dictionary.
- `Datasets/`: primary data lake (raw + processed).
- `Datasummary/`: aggregate tables for reporting.
- `Inputs/`: curated source inputs for collection/matching.
- `Scripts/`: scraping and parsing pipelines.

## Datasets

### `Datasets/Candidates/`

Purpose: candidate master lists and election candidate outcomes.

1. `altinget_ft26_candidates.csv`
- Row grain: one scraped Altinget candidate profile URL.
- Columns: `profile_id, url, status, candidate_name, party, age, education, birthdate, error`.

2. `candidates_list_2026.csv`
- Row grain: one candidate.
- Columns: `candidate_name, party_name, storkreds`.

3. `folketing_all_candidates_with_votes.csv`
- Row grain: one candidate-list placement/result entry.
- Columns: `candidate_name, party_name, storkreds, list_type, is_alphabetical, list_position, raw_position, opstillet_i_kreds, prioriteret_i_kreds, personal_votes, total_votes, valgt_token, elected_marker, valgt_nr, stedfortraeder_nr, is_elected, beat_someone_above, n_above, n_above_beaten`.

4. `folketing_all_elected_candidates.csv`
- Row grain: one elected candidate record.
- Columns: `candidate_name, party_name_results, storkreds, landsdel, opstillet_i_kreds, prioriteret_i_kreds, total_votes, personal_votes, valgt_token, valgt_nr, stedfortraeder_nr, elected_marker, is_elected`.

5. `Candidate PDFs/`
- Source files used by extraction scripts (not directly tabular for AI analysis).

### `Datasets/Election/`

Purpose: compact election result rows used directly in analysis scripts.

Files in folder:

1. `parsed_export_rows_2005.csv`
2. `parsed_export_rows_2007.csv`
3. `parsed_export_rows_2011.csv`
4. `parsed_export_rows_2015.csv`
5. `parsed_export_rows_2019.csv`
6. `parsed_export_rows_2022.csv`
7. `parsed_export_rows_2026.csv`

Shared schema:

- Row grain: one candidate-party vote row parsed from one exported polling table row.
- Columns: `ordinal, storkreds, kommune, nomination_label, nomination_url, polling_label, polling_url, election_id, nomination_district_id, polling_district_id, election_year, start_url, download_filename, source_table, source_row_number, Opstillingskreds, Afstemningsområde, Partibogstav, Partinavn, Navn, Stemmetal`.

Note:

- URL columns (`nomination_url`, `polling_url`, `start_url`) are intentionally blanked in this repository copy to reduce dataset size.

### `Datasets/Muncipality Data/`

Note: folder spelling is intentionally `Muncipality Data` in this repository.

Purpose: municipality context indicators in long (tidy) format.

1. `Muncipality Data 2005-2026.csv`
2. `Muncipality Data 2026.csv`

- Row grain: one municipality-variable-year observation.
- Columns: `municipality, municipality_code, variable, year, value`.

### `Datasets/Tiktok/`

Purpose: TikTok raw scrape runs plus processed/merged candidate video datasets.

Top-level in folder:

1. `video_data_full.csv`
- Row grain: one unique candidate video (deduplicated by `video_id`, oldest datapoint retained).
- Columns: `tiktok_handle, display_name, party_name, storkreds, account_type, video_id, upload_dato, beskrivelse, video_url, visninger, likes, kommentarer, shares, gemmer, source_file, source_rank, source_row_index`.

Run folders:

1. `scrape_2026-04-05/`
2. `scrape_2026-04-09/`

Common scrape files:

- `tiktok_profiles.csv`
  - Row grain: one scraped TikTok account profile.
  - Columns: `tiktok_handle, display_name, party_name, account_type, følgere, samlet_likes, antal_videoer, hentet_dato`.
- `tiktok_profile_stats.csv`
  - Row grain: one account stats fetch result.
  - Columns: `handle, followers, total_likes, status, tiktok_handle, display_name, party_name, account_type, følgere, samlet_likes, antal_videoer, hentet_dato`.
- `tiktok_videos.csv`
  - Row grain: one TikTok video record.
  - Columns: `tiktok_handle, display_name, party_name, storkreds, account_type, video_id, upload_dato, beskrivelse, visninger, likes, kommentarer, shares, gemmer, video_url`.
- `tiktok_collect_checkpoint.json`, `tiktok_retry_later.json`, `run_logs/`, `raw/` (run metadata and logs).

Processed folder:

1. `analysis_2026-04-09/`

Files:

- `tiktok_videos_raw.csv`
  - Row grain: one raw video row before enrichment.
  - Columns: same as scrape `tiktok_videos.csv`.
- `tiktok_videos_amended.csv`
  - Row grain: one enriched/matched video row.
  - Columns include raw video fields plus candidate match, normalized text, profile stats, engagement metrics, and run metadata:
  - `run_id, tiktok_handle, display_name, party_name, storkreds, account_type, video_id, upload_dato, beskrivelse, visninger, likes, kommentarer, shares, gemmer, video_url, matched_candidate_name, is_candidate_match, candidate_match_type, profile_display_name_final, profile_party_name_final, profile_account_type_final, profile_followers, profile_total_likes, profile_antal_videoer, profile_hentet_dato, stats_status, stats_hentet_dato, engagements, engagement_rate, period_relative_to_2026_02_26, profile_tiktok_handle, profile_display_name, profile_party_name, profile_account_type, profile_følgere, profile_samlet_likes, stats_tiktok_handle, stats_followers, stats_total_likes, stats_display_name, stats_party_name, stats_account_type, display_name_norm, party_name_norm, storkreds_norm, candidate_name_from_list, candidate_party_from_list, candidate_storkreds_from_list, candidate_name_norm`.
- `tiktok_profile_stats.csv`
  - Same schema as scrape `tiktok_profile_stats.csv`.
- `candidates_list_2026.csv`
  - Candidate lookup used in analysis run.
- `video_data_full.csv`
  - Analysis-run combined file (historical snapshot).

## Datasummary

Purpose: compact aggregates for reporting and dashboards.

1. `all_video_data_oldest_by_video_id.csv`
- Row grain: one deduplicated video across source runs.
- Columns: video-level and enrichment fields (superset of amended video schema), including provenance (`source_file`, `source_rank`, `source_row_index`) and matching/engagement columns.

2. `video_totals_by_account.csv`
- Row grain: one TikTok account aggregate.
- Columns: `tiktok_handle, display_name, party_name, videos, views, likes, comments, shares, saves`.

3. `video_totals_by_party.csv`
- Row grain: one party aggregate.
- Columns: `party_name, videos, views, likes, comments, shares, saves`.

4. `video_totals_overall.csv`
- Row grain: one global summary row.
- Columns: `total_videos, total_views, total_likes, total_comments, total_shares, total_saves, unique_accounts, unique_parties`.

5. `election_2019_party_summary.csv`, `election_2022_party_summary.csv`, `election_2026_party_summary.csv`
- Row grain: one party in one election year.
- Columns: `year, party_name, total_votes, candidate_rows, unique_candidates`.

6. `election_overview_2019_2022_2026.csv`
- Row grain: one election year overview.
- Columns: `year, total_candidate_rows, unique_candidates, unique_parties, total_votes`.

## Inputs

Purpose: curated source-of-truth inputs for TikTok target selection.

1. `Inputs/TikTok/current/candidates_tiktok_accounts.csv`
- Row grain: one candidate and known TikTok presence status.
- Columns: `candidate_name, party_name, storkreds, har_tiktok, tiktok_url`.

2. `Inputs/TikTok/current/party_tiktok_accounts_from_claude.csv`
- Row grain: one party account target.
- Columns: `candidate_name, party_name, storkreds, tiktok_handle, tiktok_url, account_type`.

## Scripts

Main automation scripts in `Scripts/`:

- `altinget_ft26_scraper.py`
- `build_analysis_dataset_2026.py`
- `pdf_extractor.py`
- `run_tiktok_vote_share_model_2026.py`
- `tiktok_scraper.py`
- `valgdk_scraper.py`

## Practical Data Lineage

1. Build candidate targets from `Datasets/Candidates/` and `Inputs/TikTok/current/`.
2. Collect TikTok raw runs in `Datasets/Tiktok/scrape_<date>/`.
3. Produce enriched video outputs in `Datasets/Tiktok/analysis_<date>/`.
4. Create final deduplicated candidate video file in `Datasets/Tiktok/video_data_full.csv`.
5. Build downstream aggregate tables in `Datasummary/`.

## Conventions

- Election snapshots: one flat file per year as `parsed_export_rows_<year>.csv`.
- TikTok run snapshots: `scrape_<YYYY-MM-DD>/`.
- Processed TikTok snapshots: `analysis_<YYYY-MM-DD>/`.
- Preserve historical snapshots; avoid deleting old runs unless intentional.