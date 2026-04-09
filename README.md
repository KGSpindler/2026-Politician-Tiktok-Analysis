# Tiktok Project - Cleaned Structure

This folder has been cleaned to keep important data while reducing script clutter.

## Scripts (only 3)
- Scripts/TiktokAccountStatFinder_v3 (1).py
- Scripts/valgdk_folketing_scraper.py
- Scripts/pdfextracter.py

## Dataset Layout
All datasets are now grouped by source and run/snapshot date.

- Datasets/TikTok/run_2026-04-09_current/
- Datasets/TikTok/run_2026-04-05_222139/
- Datasets/Election/recorded_2019/
- Datasets/Election/recorded_2022/
- Datasets/Election/recorded_2026/
- Datasets/Candidates/recorded_2026-04-09/
- Datasets/Candidates/Candidate PDFs/
- Datasets/Analysis/recorded_2026-04-09/

## Inputs And Outputs
The scripts now use generic input and output roots instead of source-specific paths.

- Inputs/TikTok/current/
- Outputs/TikTok/run_<timestamp>/
- Outputs/Valgdk/run_<timestamp>/
- Outputs/Candidates/run_<timestamp>/

## Datasummary
The Datasummary folder contains election and video summary outputs:

- Datasummary/election_2019_party_summary.csv
- Datasummary/election_2022_party_summary.csv
- Datasummary/election_2026_party_summary.csv
- Datasummary/election_overview_2019_2022_2026.csv
- Datasummary/all_video_data_oldest_by_video_id.csv
- Datasummary/video_totals_overall.csv
- Datasummary/video_totals_by_party.csv
- Datasummary/video_totals_by_account.csv

The combined video file keeps the oldest row for each `video_id` across all source video datasets.

## Script Names
The scripts in Scripts/ are the cleaned names to use going forward:

- tiktok_scraper.py
- valgdk_scraper.py
- pdf_extractor.py

## Important Data Check
The copied folder has been checked against the original source for key files.

- Core election files for 2019, 2022, and 2026 are present and matching.
- Core TikTok aggregate files are present and matching.
- Raw TikTok account files are complete (same file count and names).
- Added missing important analysis file: Datasets/Analysis/recorded_2026-04-09/tiktok_videos_amended.csv



