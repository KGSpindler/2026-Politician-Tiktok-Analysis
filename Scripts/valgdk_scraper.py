#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
valg.dk Folketing export scraper

What changed
------------
- Uses the "Eksporter data" button on each polling-district page instead of scraping the
  results table from the DOM.
- Captures both:
    * storkreds
    * opstillingskreds / nomination_label
    * kommune
    * polling district / afstemningsområde
- Keeps the raw exported files, plus a merged CSV with metadata and parsed export rows
  whenever the downloaded file can be read as CSV or Excel.

Requirements
------------
pip install playwright pandas beautifulsoup4 lxml openpyxl
python -m playwright install chromium

Run
---
python valgdk_export_scraper.py
python valgdk_export_scraper.py --start-url "https://valg.dk/fv/47b883ef-d0d3-4cb6-9da5-91963f0e9ba0"

Outputs
-------
- nomination_district_links.csv
- polling_district_links.csv
- export_inventory.csv
- parsed_export_rows.csv
- exports/<raw downloaded files>
- parsed_tables/<one CSV per parsed export sheet or CSV file>

Notes
-----
- The scraper still opens each polling-district page, but it no longer scrapes the result
  tables from the HTML. It clicks the export button and parses the downloaded file instead.
- If a downloaded file cannot be parsed automatically, it is still saved in exports/.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import Error as PlaywrightError  # type: ignore[import-not-found]
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError  # type: ignore[import-not-found]
    from playwright.sync_api import Download, Locator, Page, sync_playwright  # type: ignore[import-not-found]
    _PLAYWRIGHT_IMPORT_ERROR = None
except ImportError as exc:
    PlaywrightError = Exception
    PlaywrightTimeoutError = TimeoutError
    sync_playwright = None
    Download = object  # type: ignore
    Locator = object  # type: ignore
    Page = object  # type: ignore
    _PLAYWRIGHT_IMPORT_ERROR = exc


START_URL = "https://valg.dk/fv/47b883ef-d0d3-4cb6-9da5-91963f0e9ba0"
DEFAULT_ELECTIONS: List[Tuple[int, str]] = [
    (2005, "https://valg.dk/fv/a0fdd185-c290-43c0-a323-6f93743cc074"),
    (2007, "https://valg.dk/fv/103a7710-f6e1-4330-a4dc-267dffc8643c"),
    (2011, "https://valg.dk/fv/da30d080-70f3-4c6e-9933-caa61fad85b4"),
    (2015, "https://valg.dk/fv/8b9462dd-2bb6-40f7-b160-2a625665124b"),
    (2019, "https://valg.dk/fv/847cec19-05ff-4eff-b433-f0e886b24b7d"),
    (2022, "https://valg.dk/fv/987875fe-0dae-42ac-be5b-62cf0bd5d65e"),
    (2026, "https://valg.dk/fv/47b883ef-d0d3-4cb6-9da5-91963f0e9ba0"),
]
DEFAULT_ELECTION_URLS = [url for _, url in DEFAULT_ELECTIONS]
URL_TO_YEAR = {url: year for year, url in DEFAULT_ELECTIONS}
HEADLESS = True
NAVIGATION_ATTEMPTS = 3
NAVIGATION_TIMEOUT_MS = 120000
NAVIGATION_RETRY_SLEEP_MS = 2500
DOWNLOAD_TIMEOUT_MS = 20000

RUN_ID = datetime.now().strftime("%Y-%m-%d_%H%M%S")
ROOT = Path(__file__).resolve().parents[1]
OUTDIR = Path(os.environ.get("VALGDK_OUTDIR", str(ROOT / "Datasets" / "Election")))

NOMINATION_LINKS_CSV = "nomination_district_links.csv"
POLLING_LINKS_CSV = "polling_district_links.csv"
EXPORT_INVENTORY_CSV = "export_inventory.csv"
PARSED_EXPORT_ROWS_CSV = "parsed_export_rows.csv"
RUN_SUMMARY_CSV = "run_summary.csv"


def clean_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def safe_slug(value: object) -> str:
    text = clean_text(value)
    text = re.sub(r"[^\w\-]+", "_", text, flags=re.UNICODE)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "value"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: List[Dict], field_order: Optional[List[str]] = None) -> None:
    ensure_dir(path.parent)

    if not rows:
        cols = field_order or ["empty"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
        return

    keys: List[str] = []
    seen = set()

    if field_order:
        for key in field_order:
            if key not in seen:
                seen.add(key)
                keys.append(key)

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                keys.append(key)

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def append_csv(path: Path, rows: List[Dict]) -> None:
    ensure_dir(path.parent)
    if not rows:
        return

    if not path.exists():
        write_csv(path, rows)
        return

    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

    seen = set(fieldnames)
    extra = []
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                extra.append(key)

    if extra:
        # Re-write file when new columns appear.
        existing_rows = load_csv(path)
        write_csv(path, existing_rows + rows, field_order=fieldnames + extra)
        return

    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerows(rows)


def load_csv(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def parse_url_meta(url: str) -> Dict[str, str]:
    parts = urlparse(url).path.strip("/").split("/")
    out = {
        "election_id": "",
        "nomination_district_id": "",
        "polling_district_id": "",
    }
    try:
        if len(parts) >= 2 and parts[0] == "fv":
            out["election_id"] = parts[1]
        if "nomination-district" in parts:
            i = parts.index("nomination-district")
            if i + 1 < len(parts):
                out["nomination_district_id"] = parts[i + 1]
        if "polling-district" in parts:
            i = parts.index("polling-district")
            if i + 1 < len(parts):
                out["polling_district_id"] = parts[i + 1]
    except Exception:
        pass
    return out


def to_abs(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def wait_settle(page: Page, ms: int = 1200) -> None:
    page.wait_for_timeout(ms)


def goto_with_retry(page: Page, url: str, attempts: int = NAVIGATION_ATTEMPTS) -> None:
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
            return
        except (PlaywrightTimeoutError, PlaywrightError) as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            page.wait_for_timeout(NAVIGATION_RETRY_SLEEP_MS * attempt)
    if last_exc is not None:
        raise last_exc


def save_html(page: Page, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(page.content(), encoding="utf-8")


def find_sidenav_component(soup: BeautifulSoup, heading_text: str):
    target = heading_text.lower()
    for component in soup.find_all("valg-sidenavigation"):
        heading = component.find(["h1", "h2", "h3", "h4"])
        if heading and target in clean_text(heading.get_text(" ", strip=True)).lower():
            return component
    return None


def parse_overview_html(html: str, base_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    component = find_sidenav_component(soup, "Gå til opstillingskreds")
    if component is None:
        return []

    nav = component.find("nav")
    if nav is None:
        return []

    root_ul = nav.find("ul")
    if root_ul is None:
        return []

    rows: List[Dict] = []
    seen = set()

    for li in root_ul.find_all("li", recursive=False):
        storkreds_button = li.find("button")
        storkreds = clean_text(storkreds_button.get_text(" ", strip=True)) if storkreds_button else ""

        nested_ul = li.find("ul")
        if nested_ul is None:
            continue

        for child in nested_ul.find_all("li", recursive=False):
            a = child.find("a", href=True)
            if a is None:
                continue

            href = a["href"]
            if "/nomination-district/" not in href:
                continue

            url = to_abs(base_url, href)
            if url in seen:
                continue
            seen.add(url)

            row = {
                "storkreds": storkreds,
                "nomination_label": clean_text(a.get_text(" ", strip=True)),
                "nomination_url": url,
                **parse_url_meta(url),
            }
            rows.append(row)

    rows.sort(key=lambda r: (r["storkreds"], r["nomination_label"]))
    return rows


def parse_nomination_html(html: str, nomination_url: str, nomination_label: str, storkreds: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    component = find_sidenav_component(soup, "Gå til afstemningsområde")
    if component is None:
        return []

    nav = component.find("nav")
    if nav is None:
        return []

    root_ul = nav.find("ul")
    if root_ul is None:
        return []

    rows: List[Dict] = []
    seen = set()

    for municipality_li in root_ul.find_all("li", recursive=False):
        municipality_button = municipality_li.find("button")
        kommune = clean_text(municipality_button.get_text(" ", strip=True)) if municipality_button else ""

        nested_ul = municipality_li.find("ul")
        if nested_ul is None:
            continue

        for child in nested_ul.find_all("li", recursive=False):
            a = child.find("a", href=True)
            if a is None:
                continue

            href = a["href"]
            if "/polling-district/" not in href:
                continue

            url = to_abs(nomination_url, href)
            if url in seen:
                continue
            seen.add(url)

            row = {
                "storkreds": storkreds,
                "kommune": kommune,
                "nomination_label": nomination_label,
                "nomination_url": nomination_url,
                "polling_label": clean_text(a.get_text(" ", strip=True)),
                "polling_url": url,
                **parse_url_meta(url),
            }
            rows.append(row)

    rows.sort(key=lambda r: (r["kommune"], r["polling_label"]))
    return rows


def locator_exists(locator) -> bool:
    try:
        return locator.count() > 0
    except Exception:
        return False


def click_export_and_download(page: Page) -> Download:
    """
    Try the obvious export button first. If that only opens a menu, try common follow-up
    entries such as CSV / Excel.
    """
    export_button_candidates = [
        page.get_by_role("button", name=re.compile(r"Eksporter data", re.I)),
        page.locator("button").filter(has_text=re.compile(r"Eksporter data", re.I)),
        page.locator("button").filter(has_text=re.compile(r"Export", re.I)),
    ]

    last_exc: Optional[Exception] = None

    for locator in export_button_candidates:
        if not locator_exists(locator):
            continue
        try:
            with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as download_info:
                locator.first.click(timeout=5000)
            return download_info.value
        except Exception as exc:
            last_exc = exc

    # Maybe the first click opens a menu, popover, or secondary action.
    for locator in export_button_candidates:
        if not locator_exists(locator):
            continue
        try:
            locator.first.click(timeout=5000)
            page.wait_for_timeout(400)
        except Exception:
            continue

        secondary_candidates = [
            page.get_by_role("menuitem", name=re.compile(r"csv", re.I)),
            page.get_by_role("menuitem", name=re.compile(r"excel|xlsx", re.I)),
            page.get_by_role("button", name=re.compile(r"csv", re.I)),
            page.get_by_role("button", name=re.compile(r"excel|xlsx", re.I)),
            page.get_by_text(re.compile(r"csv", re.I)),
            page.get_by_text(re.compile(r"excel|xlsx", re.I)),
        ]
        for secondary in secondary_candidates:
            if not locator_exists(secondary):
                continue
            try:
                with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as download_info:
                    secondary.first.click(timeout=5000)
                return download_info.value
            except Exception as exc:
                last_exc = exc

    if last_exc is not None:
        raise RuntimeError(f"Could not trigger export download: {type(last_exc).__name__}: {last_exc}")
    raise RuntimeError("Could not find a usable export button on the polling page.")


def save_download(download: Download, dest_dir: Path, polling_row: Dict, ordinal: int) -> Path:
    ensure_dir(dest_dir)

    suggested = clean_text(download.suggested_filename)
    suffix = Path(suggested).suffix if suggested else ""
    if not suffix:
        suffix = ".bin"

    filename = (
        f"{ordinal:04d}_"
        f"{safe_slug(polling_row.get('storkreds'))}__"
        f"{safe_slug(polling_row.get('nomination_label'))}__"
        f"{safe_slug(polling_row.get('kommune'))}__"
        f"{safe_slug(polling_row.get('polling_label'))}{suffix}"
    )
    dest = dest_dir / filename
    download.save_as(str(dest))
    return dest


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    seen = set()
    for i, col in enumerate(df.columns, start=1):
        name = clean_text(col)
        if not name or name.lower().startswith("unnamed:"):
            name = f"column_{i}"
        base = name
        n = 2
        while name in seen:
            name = f"{base}_{n}"
            n += 1
        seen.add(name)
        cols.append(name)
    out = df.copy()
    out.columns = cols
    out = out.dropna(axis=1, how="all").dropna(axis=0, how="all")
    out = out.reset_index(drop=True)
    return out


def sniff_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        # Danish exports often use semicolon.
        return ";" if sample.count(";") >= sample.count(",") else ","


def read_csv_flex(path: Path) -> pd.DataFrame:
    raw = path.read_bytes()
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
    text = None
    for enc in encodings:
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = raw.decode("latin-1", errors="replace")

    sample = text[:10000]
    sep = sniff_delimiter(sample)

    return pd.read_csv(
        io.StringIO(text),
        sep=sep,
        engine="python",
        dtype=str,
    )


def parse_export_file(path: Path) -> List[Tuple[str, pd.DataFrame]]:
    suffix = path.suffix.lower()

    if suffix in {".xlsx", ".xls", ".xlsm"}:
        sheets = pd.read_excel(path, sheet_name=None, dtype=str)
        return [(clean_text(name) or "Sheet1", normalise_columns(df)) for name, df in sheets.items()]

    if suffix in {".csv", ".txt"}:
        df = normalise_columns(read_csv_flex(path))
        return [("data", df)]

    # Best effort fallbacks
    try:
        sheets = pd.read_excel(path, sheet_name=None, dtype=str)
        return [(clean_text(name) or "Sheet1", normalise_columns(df)) for name, df in sheets.items()]
    except Exception:
        pass

    try:
        df = normalise_columns(read_csv_flex(path))
        return [("data", df)]
    except Exception:
        pass

    raise RuntimeError(f"Unsupported or unreadable export format: {path.name}")


def table_to_rows(df: pd.DataFrame, metadata: Dict, source_table: str) -> List[Dict]:
    rows: List[Dict] = []
    if df.empty:
        return rows

    df = df.fillna("")

    for i, record in enumerate(df.to_dict(orient="records"), start=1):
        row = dict(metadata)
        row["source_table"] = source_table
        row["source_row_number"] = i
        for key, value in record.items():
            row[key] = clean_text(value)
        rows.append(row)

    return rows


def save_parsed_tables(
    parsed_tables: List[Tuple[str, pd.DataFrame]],
    parsed_dir: Path,
    polling_row: Dict,
    ordinal: int,
) -> List[Path]:
    ensure_dir(parsed_dir)
    saved_paths = []

    for sheet_name, df in parsed_tables:
        filename = (
            f"{ordinal:04d}_"
            f"{safe_slug(polling_row.get('storkreds'))}__"
            f"{safe_slug(polling_row.get('nomination_label'))}__"
            f"{safe_slug(polling_row.get('kommune'))}__"
            f"{safe_slug(polling_row.get('polling_label'))}__"
            f"{safe_slug(sheet_name)}.csv"
        )
        path = parsed_dir / filename
        df.to_csv(path, index=False, encoding="utf-8-sig")
        saved_paths.append(path)

    return saved_paths


def scrape_one_election(
    start_url: str,
    outdir: Path,
    headless: bool = HEADLESS,
    election_year: Optional[int] = None,
) -> Dict[str, object]:
    if sync_playwright is None:
        raise RuntimeError(
            "Missing dependency: playwright. Install with `pip install playwright` and "
            "run `python -m playwright install chromium`."
        ) from _PLAYWRIGHT_IMPORT_ERROR

    election_meta = parse_url_meta(start_url)
    election_id = election_meta.get("election_id", "")

    ensure_dir(outdir)
    raw_html_dir = outdir / "raw_html"
    exports_dir = outdir / "exports"
    parsed_dir = outdir / "parsed_tables"
    ensure_dir(raw_html_dir)
    ensure_dir(exports_dir)
    ensure_dir(parsed_dir)

    nomination_path = outdir / NOMINATION_LINKS_CSV
    polling_path = outdir / POLLING_LINKS_CSV
    inventory_path = outdir / EXPORT_INVENTORY_CSV
    parsed_rows_path = outdir / PARSED_EXPORT_ROWS_CSV

    nomination_rows: List[Dict] = []
    polling_rows: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1440, "height": 2200},
        )

        # Step 1: overview -> nomination districts
        page = context.new_page()
        goto_with_retry(page, start_url)
        wait_settle(page, 1200)
        save_html(page, raw_html_dir / "overview.html")
        nomination_rows = parse_overview_html(page.content(), start_url)
        page.close()

        if not nomination_rows:
            raise RuntimeError("No nomination-district links found on the overview page.")

        write_csv(
            nomination_path,
            nomination_rows,
            [
                "election_year",
                "start_url",
                "storkreds",
                "nomination_label",
                "nomination_url",
                "election_id",
                "nomination_district_id",
            ],
        )

        # Step 2: nomination districts -> kommune + polling districts
        for i, nomination in enumerate(nomination_rows, start=1):
            nd_page = context.new_page()
            try:
                goto_with_retry(nd_page, nomination["nomination_url"])
                wait_settle(nd_page, 1000)
                save_html(
                    nd_page,
                    raw_html_dir / f"nomination_{i:03d}_{safe_slug(nomination['nomination_label'])}.html",
                )
                rows = parse_nomination_html(
                    html=nd_page.content(),
                    nomination_url=nomination["nomination_url"],
                    nomination_label=nomination["nomination_label"],
                    storkreds=nomination["storkreds"],
                )
                for row in rows:
                    row["election_year"] = election_year if election_year is not None else ""
                    row["start_url"] = start_url
                polling_rows.extend(rows)
            finally:
                nd_page.close()

        if not polling_rows:
            raise RuntimeError("No polling-district links found on the nomination pages.")

        write_csv(
            polling_path,
            polling_rows,
            [
                "election_year",
                "start_url",
                "storkreds",
                "kommune",
                "nomination_label",
                "nomination_url",
                "polling_label",
                "polling_url",
                "election_id",
                "nomination_district_id",
                "polling_district_id",
            ],
        )

        # Step 3: polling district page -> export download
        inventory_rows: List[Dict] = []
        parsed_export_rows: List[Dict] = []

        for i, polling in enumerate(polling_rows, start=1):
            polling_page = context.new_page()
            inventory_row = {
                "ordinal": i,
                **polling,
                "downloaded": 0,
                "parsed": 0,
                "download_path": "",
                "parse_tables": 0,
                "parse_rows": 0,
                "error": "",
            }

            try:
                goto_with_retry(polling_page, polling["polling_url"])
                wait_settle(polling_page, 1200)
                save_html(
                    polling_page,
                    raw_html_dir / f"polling_{i:04d}_{safe_slug(polling['polling_label'])}.html",
                )

                download = click_export_and_download(polling_page)
                download_path = save_download(download, exports_dir, polling, i)
                inventory_row["downloaded"] = 1
                inventory_row["download_path"] = str(download_path)

                parsed_tables = parse_export_file(download_path)
                inventory_row["parsed"] = 1
                inventory_row["parse_tables"] = len(parsed_tables)

                save_parsed_tables(parsed_tables, parsed_dir, polling, i)

                metadata = {
                    "ordinal": i,
                    **polling,
                    "download_filename": download_path.name,
                }

                row_count = 0
                for source_table, df in parsed_tables:
                    rows = table_to_rows(df, metadata, source_table)
                    row_count += len(rows)
                    parsed_export_rows.extend(rows)

                inventory_row["parse_rows"] = row_count

            except Exception as exc:
                inventory_row["error"] = f"{type(exc).__name__}: {exc}"
            finally:
                inventory_rows.append(inventory_row)
                polling_page.close()

        browser.close()

    write_csv(inventory_path, inventory_rows)
    write_csv(parsed_rows_path, parsed_export_rows)

    downloaded_ok = sum(int(r["downloaded"]) for r in inventory_rows)
    parsed_ok = sum(int(r["parsed"]) for r in inventory_rows)

    print("Done election run.")
    print("Election year:", election_year if election_year is not None else "")
    print("Election id:", election_id)
    print("Nomination districts:", len(nomination_rows))
    print("Polling districts:", len(polling_rows))
    print("Downloads attempted:", len(inventory_rows))
    print("Downloaded OK:", downloaded_ok)
    print("Parsed OK:", parsed_ok)
    print("Output folder:", outdir.resolve())

    return {
        "election_year": election_year if election_year is not None else "",
        "start_url": start_url,
        "election_id": election_id,
        "nomination_districts": len(nomination_rows),
        "polling_districts": len(polling_rows),
        "downloads_attempted": len(inventory_rows),
        "downloaded_ok": downloaded_ok,
        "parsed_ok": parsed_ok,
        "outdir": str(outdir.resolve()),
    }


def scrape_all(start_urls: List[str], outdir: Path, headless: bool = HEADLESS) -> None:
    ensure_dir(outdir)
    summary_rows: List[Dict] = []

    for index, start_url in enumerate(start_urls, start=1):
        meta = parse_url_meta(start_url)
        election_id = meta.get("election_id", "")
        election_year = URL_TO_YEAR.get(start_url)

        if election_year is not None:
            election_dir_name = f"recorded_{election_year}"
        elif election_id:
            election_dir_name = f"recorded_{election_id}"
        else:
            election_dir_name = f"recorded_{index:02d}"

        election_outdir = outdir / election_dir_name

        print("=" * 80)
        print(f"[{index}/{len(start_urls)}] Running election scrape")
        print("Year:", election_year if election_year is not None else "")
        print("URL:", start_url)
        print("Output:", election_outdir.resolve())

        try:
            summary = scrape_one_election(
                start_url=start_url,
                outdir=election_outdir,
                headless=headless,
                election_year=election_year,
            )
            summary["status"] = "ok"
            summary["error"] = ""
        except Exception as exc:
            summary = {
                "election_year": election_year if election_year is not None else "",
                "start_url": start_url,
                "election_id": election_id,
                "nomination_districts": 0,
                "polling_districts": 0,
                "downloads_attempted": 0,
                "downloaded_ok": 0,
                "parsed_ok": 0,
                "outdir": str(election_outdir.resolve()),
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }

        summary_rows.append(summary)
        write_csv(outdir / RUN_SUMMARY_CSV, summary_rows)

    print("=" * 80)
    print("All election runs completed.")
    print("Summary CSV:", (outdir / RUN_SUMMARY_CSV).resolve())
    print("Total elections attempted:", len(summary_rows))
    print("Succeeded:", sum(1 for row in summary_rows if row.get("status") == "ok"))
    print("Failed:", sum(1 for row in summary_rows if row.get("status") == "failed"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="valg.dk export scraper (2005-2026)")
    parser.add_argument(
        "--start-url",
        action="append",
        default=None,
        help="Election overview URL (repeat flag for multiple URLs). Defaults to all known 2005-2026 URLs.",
    )
    parser.add_argument("--outdir", default=str(OUTDIR), help="Output directory")
    parser.add_argument("--headed", action="store_true", help="Run browser non-headless")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_urls = args.start_url or list(DEFAULT_ELECTION_URLS)
    scrape_all(
        start_urls=start_urls,
        outdir=Path(args.outdir),
        headless=not args.headed,
    )


if __name__ == "__main__":
    main()
